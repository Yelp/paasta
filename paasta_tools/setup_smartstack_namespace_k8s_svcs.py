#!/usr/bin/env python
# Copyright 2015-2018 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Usage: ./setup_smartstack_namespace_k8s_svcs.py [options]

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -v, --verbose: Verbose output
- -n <NAMESPACE>, --namespace <NAMESPACE>: Filter to specific namespace(s)
- --dry-run: Preview changes without applying them
"""
import argparse
import logging
import os
import sys
from collections import defaultdict
from typing import AbstractSet
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple

import kubernetes.client as k8s
from kubernetes.client.exceptions import ApiException

from paasta_tools import yaml_tools as yaml
from paasta_tools.kubernetes_tools import CONTAINER_PORT_NAME
from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import limit_size_with_hash
from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.kubernetes_tools import registration_label
from paasta_tools.kubernetes_tools import sanitise_kubernetes_name
from paasta_tools.utils import DEFAULT_SOA_DIR


log = logging.getLogger(__name__)

FIELD_MANAGER = "paasta-namespace-services"
MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
SERVICE_LABELS = {
    MANAGED_BY_LABEL: FIELD_MANAGER,
}
SERVICE_ANNOTATIONS = {
    paasta_prefixed("managed_by"): FIELD_MANAGER,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Creates one Kubernetes Service per Smartstack namespace inside the service's Kubernetes namespace."
        )
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        default=False,
        help="Log intended Kubernetes mutations but do not execute them.",
    )
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        default=DEFAULT_SOA_DIR,
        metavar="PATH",
        type=str,
        help=f"Directory with service declarations. Default is {DEFAULT_SOA_DIR}",
    )
    parser.add_argument(
        "-n",
        "--namespace",
        dest="namespaces",
        action="append",
        metavar="SERVICE.NAMESPACE",
        help=(
            "Only manage the specified Smartstack namespace(s). May be provided multiple times. "
            "Defaults to all namespaces with routing.external enabled."
        ),
    )
    return parser.parse_args()


def get_services_from_namespaces(
    target_namespaces: Optional[Sequence[str]],
) -> Optional[Set[str]]:
    """Extract unique service names from target namespaces.

    Args:
        target_namespaces: List of "service.namespace" strings

    Returns:
        Set of service names, or None if no filter specified
    """
    if not target_namespaces:
        return None

    services = set()
    for namespace in target_namespaces:
        try:
            service, _ = namespace.split(".", 1)
            services.add(service)
        except ValueError:
            log.warning(f"Skipping invalid namespace format '{namespace}'")
            continue
    return services


def load_smartstack_namespaces_for_service(
    service: str, soa_dir: str = DEFAULT_SOA_DIR
) -> Dict[str, Mapping]:
    """Load smartstack namespace configurations for a specific service.

    Args:
        service: Service name
        soa_dir: Directory containing service configurations

    Returns:
        Mapping of "service.namespace" to configuration dict
    """
    namespaces: Dict[str, Mapping] = {}
    file_path = os.path.join(soa_dir, service, "smartstack.yaml")

    if not os.path.isfile(file_path):
        return namespaces

    try:
        with open(file_path) as f:
            svc_namespaces = yaml.safe_load(f)
            if not isinstance(svc_namespaces, Mapping):
                log.warning(f"smartstack.yaml for {service} is not a mapping, skipping")
                return namespaces

            for (ns, details) in svc_namespaces.items():
                if details is None:
                    log.warning(
                        f"Namespace {ns} in {service}/smartstack.yaml is empty, skipping"
                    )
                    continue
                if not isinstance(details, Mapping):
                    log.warning(
                        f"Namespace {ns} in {service}/smartstack.yaml is not a mapping, skipping"
                    )
                    continue
                namespaces[f"{service}.{ns}"] = details
    except Exception as err:
        log.warning(f"Failed to load namespaces for {service}: {err}")

    return namespaces


def load_smartstack_namespaces(
    soa_dir: str = DEFAULT_SOA_DIR,
    services: Optional[Set[str]] = None,
) -> Mapping[str, Mapping]:
    """Load smartstack namespace configurations from soa_dir.

    Args:
        soa_dir: Directory containing service configurations
        services: Optional set of service names to load. If None, loads all services.

    Returns:
        Mapping of "service.namespace" to configuration dict
    """
    namespaces: Dict[str, Mapping] = {}

    if services is not None:
        # Load only specified services
        for service in services:
            namespaces.update(load_smartstack_namespaces_for_service(service, soa_dir))
    else:
        # Load all services
        _, dirs, _ = next(os.walk(soa_dir))
        for service_dir in dirs:
            namespaces.update(
                load_smartstack_namespaces_for_service(service_dir, soa_dir)
            )

    return namespaces


def sanitise_kubernetes_service_name(name: str) -> str:
    return limit_size_with_hash(sanitise_kubernetes_name(name).replace(".", "---"))


def get_existing_kubernetes_service_names(
    kube_client: KubeClient, kube_namespace: str
) -> Set[str]:
    try:
        service_objects = kube_client.core.list_namespaced_service(
            kube_namespace,
            label_selector=f"{MANAGED_BY_LABEL}={FIELD_MANAGER}",
        )
    except ApiException as e:
        if e.status == 404:
            log.debug(
                f"Namespace {kube_namespace} does not exist yet; assuming no managed Services"
            )
            return set()
        raise

    return {
        item.metadata.name
        for item in service_objects.items
        if item.metadata and item.metadata.name
    }


def get_service_kube_namespace(service: str) -> str:
    return f"paastasvc-{sanitise_kubernetes_name(service)}"


def is_external_routing_enabled(config: Mapping) -> bool:
    """Check if external routing is enabled for a namespace config.

    Args:
        config: Namespace configuration dict

    Returns:
        True if routing.external is set to True
    """
    routing_cfg = config.get("routing", {})
    if not isinstance(routing_cfg, Mapping):
        return False
    return bool(routing_cfg.get("external"))


def group_namespaces_by_service(
    namespaces: Mapping[str, Mapping],
) -> Dict[str, Set[str]]:
    """Group smartstack namespaces by service name, filtering for external routing.

    Args:
        namespaces: Mapping of "service.namespace" to config dict

    Returns:
        Dict mapping service name to set of namespace registrations
    """
    grouped: Dict[str, Set[str]] = defaultdict(set)
    for registration, config in namespaces.items():
        try:
            service, _ = registration.split(".", 1)
        except ValueError:
            log.warning(f"Skipping invalid smartstack namespace '{registration}'")
            continue
        if not is_external_routing_enabled(config):
            continue
        grouped[service].add(registration)
    return grouped


def filter_grouped_namespaces(
    grouped_namespaces: Mapping[str, Set[str]],
    target_namespaces: Optional[Sequence[str]],
) -> Dict[str, Set[str]]:
    """Filter grouped namespaces to only those explicitly requested.

    Args:
        grouped_namespaces: Dict mapping service to set of namespaces
        target_namespaces: Optional list of "service.namespace" to filter to

    Returns:
        Filtered dict mapping service to set of namespaces
    """
    if not target_namespaces:
        return dict(grouped_namespaces)

    filtered: Dict[str, Set[str]] = defaultdict(set)
    requested = set(target_namespaces)
    for namespace in requested:
        try:
            service, smartstack_ns = namespace.split(".", 1)
        except ValueError:
            log.warning(f"Skipping invalid namespace filter '{namespace}'")
            continue

        available = grouped_namespaces.get(service)
        if not available:
            log.warning(
                f"Requested namespace {namespace} but no routable namespaces found for service {service}"
            )
            continue
        if namespace not in available:
            log.warning(
                f"Requested namespace {namespace} but it is not routable (missing routing.external)"
            )
            continue
        filtered[service].add(namespace)

    return filtered


def build_namespace_service(
    kube_namespace: str,
    smartstack_namespace: str,
) -> k8s.V1Service:
    """Build a Kubernetes Service object for a smartstack namespace.

    Args:
        kube_namespace: Kubernetes namespace to create service in
        smartstack_namespace: Smartstack namespace (e.g. "service.main")

    Returns:
        V1Service object ready to be applied
    """
    service_name = sanitise_kubernetes_service_name(smartstack_namespace)
    metadata = k8s.V1ObjectMeta(
        name=service_name,
        namespace=kube_namespace,
        labels=SERVICE_LABELS.copy(),
        annotations=SERVICE_ANNOTATIONS.copy(),
    )
    port_spec = k8s.V1ServicePort(
        name="application",
        port=80,  # Required by k8s API but not used for headless services
        target_port=CONTAINER_PORT_NAME,
        protocol="TCP",
        app_protocol="http",
    )
    spec = k8s.V1ServiceSpec(
        cluster_ip="None",  # Headless service - no virtual IP, just DNS
        selector={registration_label(smartstack_namespace): "true"},
        ports=[port_spec],
    )
    return k8s.V1Service(
        api_version="v1",
        kind="Service",
        metadata=metadata,
        spec=spec,
    )


def server_side_apply_service(
    kube_client: KubeClient,
    kube_namespace: str,
    service: k8s.V1Service,
    dry_run: bool = False,
) -> None:
    log.debug(f"Reconciling k8s Service {kube_namespace}/{service.metadata.name}")
    serialized = kube_client.core.api_client.sanitize_for_serialization(service)

    patch_kwargs = dict(
        name=service.metadata.name,
        namespace=kube_namespace,
        body=serialized,
        field_manager=FIELD_MANAGER,
        force=True,
        content_type="application/apply-patch+yaml",
    )
    if dry_run:
        patch_kwargs["dry_run"] = "All"

    try:
        kube_client.core.patch_namespaced_service(**patch_kwargs)
    except ApiException as e:
        if e.status != 404:
            raise

        create_kwargs = dict(namespace=kube_namespace, body=service)
        if dry_run:
            create_kwargs["dry_run"] = "All"

        created = False
        try:
            kube_client.core.create_namespaced_service(**create_kwargs)
            created = True
            if not dry_run:
                kube_client.core.patch_namespaced_service(**patch_kwargs)
        except Exception:
            if created and not dry_run:
                log.warning(
                    f"Rolling back Service {kube_namespace}/{service.metadata.name} after apply error"
                )
                try:
                    kube_client.core.delete_namespaced_service(
                        name=service.metadata.name,
                        namespace=kube_namespace,
                    )
                except Exception:
                    log.exception(
                        f"Failed to rollback Service {kube_namespace}/{service.metadata.name} after apply error"
                    )
            raise


def delete_namespace_service(
    kube_client: KubeClient,
    kube_namespace: str,
    service_name: str,
    dry_run: bool = False,
) -> None:
    log.debug(f"Deleting k8s Service {kube_namespace}/{service_name}")
    delete_kwargs = {}
    if dry_run:
        delete_kwargs["dry_run"] = "All"
    kube_client.core.delete_namespaced_service(
        name=service_name,
        namespace=kube_namespace,
        **delete_kwargs,
    )


def setup_namespace_services_for_kube_namespace(
    kube_client: KubeClient,
    kube_namespace: str,
    smartstack_namespaces: Iterable[str],
    dry_run: bool = False,
) -> Iterator[Tuple[bool, str]]:
    """Reconcile Services for smartstack namespaces.

    Args:
        kube_client: Kubernetes client
        kube_namespace: Kubernetes namespace to create services in
        smartstack_namespaces: Iterable of smartstack namespace names
        dry_run: If True, preview changes without applying

    Yields:
        Tuples of (success: bool, message: str) for each operation
    """
    for namespace in smartstack_namespaces:
        svc = build_namespace_service(kube_namespace, namespace)
        try:
            server_side_apply_service(kube_client, kube_namespace, svc, dry_run)
            yield (True, f"Reconciled Service {kube_namespace}/{svc.metadata.name}")
        except Exception as e:
            yield (
                False,
                f"Failed to reconcile Service {kube_namespace}/{svc.metadata.name}: {e}",
            )


def cleanup_namespace_services(
    kube_client: KubeClient,
    kube_namespace: str,
    smartstack_namespaces: Iterable[str],
    existing_namespace_services: AbstractSet[str],
    dry_run: bool = False,
) -> Iterator[Tuple[bool, str]]:
    """Delete Services no longer in configuration.

    Args:
        kube_client: Kubernetes client
        kube_namespace: Kubernetes namespace to clean up
        smartstack_namespaces: Currently declared smartstack namespaces
        existing_namespace_services: Services currently in cluster
        dry_run: If True, preview changes without applying

    Yields:
        Tuples of (success: bool, message: str) for each operation
    """
    declared_services = {
        sanitise_kubernetes_service_name(ns) for ns in smartstack_namespaces
    }
    for service in existing_namespace_services:
        if service in declared_services:
            continue
        try:
            delete_namespace_service(kube_client, kube_namespace, service, dry_run)
            yield (True, f"Deleted Service {kube_namespace}/{service}")
        except Exception as e:
            yield (False, f"Failed to delete Service {kube_namespace}/{service}: {e}")


def setup_namespace_services(
    kube_client: KubeClient,
    target_namespaces: Optional[Sequence[str]] = None,
    soa_dir: str = DEFAULT_SOA_DIR,
    dry_run: bool = False,
) -> bool:
    """Reconcile all namespace services.

    Args:
        kube_client: Kubernetes client
        target_namespaces: Optional filter for specific namespaces
        soa_dir: Directory containing service configurations
        dry_run: If True, preview changes without applying

    Returns:
        True if all operations succeeded, False otherwise
    """
    services = get_services_from_namespaces(target_namespaces)
    smartstack_namespaces = load_smartstack_namespaces(soa_dir, services=services)
    grouped_namespaces = group_namespaces_by_service(smartstack_namespaces)
    grouped_namespaces = filter_grouped_namespaces(
        grouped_namespaces, target_namespaces
    )
    if not grouped_namespaces:
        log.info(
            "No Smartstack namespaces matched the provided filters; nothing to do."
        )
        return True

    success = True
    for service, registrations in grouped_namespaces.items():
        kube_namespace = get_service_kube_namespace(service)
        if dry_run:
            log.info(f"Dry run enabled: would ensure namespace {kube_namespace} exists")
        else:
            ensure_namespace(
                kube_client,
                namespace=kube_namespace,
            )
        log.info(
            f"Reconciling Smartstack Services for {service} in namespace {kube_namespace}"
        )
        existing_namespace_services = get_existing_kubernetes_service_names(
            kube_client, kube_namespace
        )

        # Reconcile services
        for op_success, message in setup_namespace_services_for_kube_namespace(
            kube_client,
            kube_namespace,
            sorted(registrations),
            dry_run=dry_run,
        ):
            if op_success:
                log.info(message)
            else:
                log.error(message)
                success = False

        # Cleanup stale services
        for op_success, message in cleanup_namespace_services(
            kube_client,
            kube_namespace,
            sorted(registrations),
            existing_namespace_services,
            dry_run=dry_run,
        ):
            if op_success:
                log.info(message)
            else:
                log.error(message)
                success = False

    return success


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    kube_client = KubeClient()
    success = setup_namespace_services(
        kube_client,
        args.namespaces,
        args.soa_dir,
        args.dry_run,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
