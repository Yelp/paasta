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
Usage: ./setup_kubernetes_cr.py [options]

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -v, --verbose: Verbose output
"""
import argparse
import logging
import sys
from typing import Any
from typing import Mapping
from typing import Optional
from typing import Sequence

import yaml
from kubernetes.client.exceptions import ApiException

from paasta_tools.cli.utils import LONG_RUNNING_INSTANCE_TYPE_HANDLERS
from paasta_tools.flink_tools import get_flink_ingress_url_root
from paasta_tools.kubernetes_tools import create_custom_resource
from paasta_tools.kubernetes_tools import CustomResourceDefinition
from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubeCustomResource
from paasta_tools.kubernetes_tools import KubeKind
from paasta_tools.kubernetes_tools import list_custom_resources
from paasta_tools.kubernetes_tools import load_custom_resource_definitions
from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.kubernetes_tools import sanitise_kubernetes_name
from paasta_tools.kubernetes_tools import update_custom_resource
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import get_git_sha_from_dockerurl
from paasta_tools.utils import load_all_configs
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.vitesscluster_tools import load_vitess_service_instance_configs


log = logging.getLogger(__name__)


INSTANCE_TYPE_TO_CONFIG_LOADER = {"vitesscluster": load_vitess_service_instance_configs}


class StdoutKubeClient:
    """Replace all destructive operations in Kubernetes APIs with
    writing out YAML to stdout."""

    class StdoutWrapper:
        def __init__(self, target) -> None:
            self.target = target

        def __getattr__(self, attr):
            if attr.startswith("create") or attr.startswith("replace"):
                return self.yaml_dump
            return getattr(self.target, attr)

        def yaml_dump(self, **kwargs):
            body = kwargs.get("body")
            if not body:
                return
            ns = kwargs.get("namespace")
            if ns:
                if "metadata" not in body:
                    body["metadata"] = {}
                body["metadata"]["namespace"] = ns
            yaml.safe_dump(body, sys.stdout, indent=4, explicit_start=True)

    def __init__(self, kube_client) -> None:
        self.deployments = StdoutKubeClient.StdoutWrapper(kube_client.deployments)
        self.core = StdoutKubeClient.StdoutWrapper(kube_client.core)
        self.policy = StdoutKubeClient.StdoutWrapper(kube_client.policy)
        self.apiextensions = StdoutKubeClient.StdoutWrapper(kube_client.apiextensions)
        self.custom = StdoutKubeClient.StdoutWrapper(kube_client.custom)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Creates custom_resources.")
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", dest="verbose", default=False
    )
    parser.add_argument(
        "-s", "--service", default=None, help="Service to setup CRs for"
    )
    parser.add_argument(
        "-i", "--instance", default=None, help="Service instance to setup CR for"
    )
    parser.add_argument(
        "-D",
        "--dry-run",
        action="store_true",
        default=False,
        help="Output kubernetes configuration instead of applying it",
    )
    parser.add_argument(
        "-c", "--cluster", default=None, help="Cluster to setup CRs for"
    )
    args = parser.parse_args()
    return args


def main() -> None:
    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    kube_client: Any = KubeClient()
    if args.dry_run:
        kube_client = StdoutKubeClient(kube_client)

    system_paasta_config = load_system_paasta_config()
    cluster = args.cluster or system_paasta_config.get_cluster()
    custom_resource_definitions = load_custom_resource_definitions(system_paasta_config)
    setup_kube_succeeded = setup_all_custom_resources(
        kube_client=kube_client,
        soa_dir=soa_dir,
        cluster=cluster,
        custom_resource_definitions=custom_resource_definitions,
        service=args.service,
        instance=args.instance,
    )
    sys.exit(0 if setup_kube_succeeded else 1)


def setup_all_custom_resources(
    kube_client: KubeClient,
    soa_dir: str,
    cluster: str,
    custom_resource_definitions: Sequence[CustomResourceDefinition],
    service: str = None,
    instance: str = None,
) -> bool:

    got_results = False
    succeeded = False
    # We support two versions due to our upgrade to 1.22
    # this functions runs succefully when any of the two apiextensions
    # succeed to update the CRDs as the cluster could be in any version
    # we need to try both possibilities
    for apiextension in [
        kube_client.apiextensions,
        kube_client.apiextensions_v1_beta1,
    ]:

        try:
            crds_list = apiextension.list_custom_resource_definition(
                label_selector=paasta_prefixed("service")
            ).items
        except ApiException:
            log.debug(
                "Listing CRDs with apiextensions/v1 not supported on this cluster, falling back to v1beta1"
            )
            crds_list = []

        cluster_crds = {crd.spec.names.kind for crd in crds_list}
        log.debug(f"CRDs found: {cluster_crds}")
        results = []
        for crd in custom_resource_definitions:
            if crd.kube_kind.singular not in cluster_crds:
                # TODO: kube_kind.singular seems to correspond to `crd.names.kind`
                # and not `crd.names.singular`
                log.warning(f"CRD {crd.kube_kind.singular} " f"not found in {cluster}")
                continue

            # by convention, entries where key begins with _ are used as templates
            # and will be filter out here
            config_dicts = load_all_configs(
                cluster=cluster, file_prefix=crd.file_prefix, soa_dir=soa_dir
            )

            ensure_namespace(
                kube_client=kube_client, namespace=f"paasta-{crd.kube_kind.plural}"
            )
            results.append(
                setup_custom_resources(
                    kube_client=kube_client,
                    kind=crd.kube_kind,
                    crd=crd,
                    config_dicts=config_dicts,
                    version=crd.version,
                    group=crd.group,
                    cluster=cluster,
                    service=service,
                    instance=instance,
                )
            )
        if results:
            got_results = True
            if any(results):
                succeeded = True
    # we want to return True if we never called `setup_custom_resources`
    # (i.e., we noop'd) or if any call to `setup_custom_resources`
    # succeed (handled above) - otherwise, we want to return False
    return succeeded or not got_results


def setup_custom_resources(
    kube_client: KubeClient,
    kind: KubeKind,
    version: str,
    crd: CustomResourceDefinition,
    config_dicts: Mapping[str, Mapping[str, Any]],
    group: str,
    cluster: str,
    service: str = None,
    instance: str = None,
) -> bool:
    succeded = True
    if config_dicts:
        crs = list_custom_resources(
            kube_client=kube_client, kind=kind, version=version, group=group
        )
    for svc, config in config_dicts.items():
        if service is not None and service != svc:
            continue
        if not reconcile_kubernetes_resource(
            kube_client=kube_client,
            service=svc,
            instance=instance,
            instance_configs=config,
            kind=kind,
            custom_resources=crs,
            version=version,
            group=group,
            cluster=cluster,
            crd=crd,
        ):
            succeded = False
    return succeded


def get_dashboard_base_url(kind: str, cluster: str, is_eks: bool) -> Optional[str]:
    system_paasta_config = load_system_paasta_config()
    dashboard_links = system_paasta_config.get_dashboard_links()
    if kind.lower() == "flink":
        flink_link = dashboard_links.get(cluster, {}).get("Flink")
        if flink_link is None:
            flink_link = get_flink_ingress_url_root(cluster, is_eks)
        if flink_link[-1:] != "/":
            flink_link += "/"
        return flink_link
    return None


def get_cr_owner(kind: str) -> Optional[str]:
    system_paasta_config = load_system_paasta_config()
    owners = system_paasta_config.get_cr_owners()
    return owners.get(kind.lower())


def format_custom_resource(
    instance_config: Mapping[str, Any],
    service: str,
    instance: str,
    cluster: str,
    kind: str,
    version: str,
    group: str,
    namespace: str,
    git_sha: str,
    is_eks: bool,
) -> Mapping[str, Any]:
    sanitised_service = sanitise_kubernetes_name(service)
    sanitised_instance = sanitise_kubernetes_name(instance)
    resource: Mapping[str, Any] = {
        "apiVersion": f"{group}/{version}",
        "kind": kind,
        "metadata": {
            "name": f"{sanitised_service}-{sanitised_instance}",
            "namespace": namespace,
            "labels": {
                "yelp.com/paasta_service": service,
                "yelp.com/paasta_instance": instance,
                "yelp.com/paasta_cluster": cluster,
                paasta_prefixed("service"): service,
                paasta_prefixed("instance"): instance,
                paasta_prefixed("cluster"): cluster,
            },
            "annotations": {},
        },
        "spec": instance_config,
    }
    if is_eks:
        resource["metadata"]["labels"][paasta_prefixed("eks")] = str(is_eks)

    url = get_dashboard_base_url(kind, cluster, is_eks)
    if url:
        resource["metadata"]["annotations"][paasta_prefixed("dashboard_base_url")] = url
    owner = get_cr_owner(kind)
    if owner:
        resource["metadata"]["labels"]["yelp.com/owner"] = owner
    config_hash = get_config_hash(resource)

    resource["metadata"]["annotations"]["yelp.com/desired_state"] = "running"
    resource["metadata"]["annotations"][paasta_prefixed("desired_state")] = "running"
    resource["metadata"]["labels"]["yelp.com/paasta_config_sha"] = config_hash
    resource["metadata"]["labels"][paasta_prefixed("config_sha")] = config_hash
    resource["metadata"]["labels"][paasta_prefixed("git_sha")] = git_sha
    return resource


def reconcile_kubernetes_resource(
    kube_client: KubeClient,
    service: str,
    instance_configs: Mapping[str, Any],
    custom_resources: Sequence[KubeCustomResource],
    kind: KubeKind,
    version: str,
    group: str,
    crd: CustomResourceDefinition,
    cluster: str,
    instance: str = None,
) -> bool:
    succeeded = True
    config_handler = LONG_RUNNING_INSTANCE_TYPE_HANDLERS[crd.file_prefix]

    is_eks = False
    if crd.file_prefix.endswith("eks"):
        is_eks = True

    for inst, config in instance_configs.items():
        if instance is not None and instance != inst:
            continue
        try:
            soa_config = config_handler.loader(
                service=service,
                instance=inst,
                cluster=cluster,
                load_deployments=True,
                soa_dir=DEFAULT_SOA_DIR,
            )
            if crd.file_prefix in INSTANCE_TYPE_TO_CONFIG_LOADER:
                config = INSTANCE_TYPE_TO_CONFIG_LOADER[crd.file_prefix](
                    service=service,
                    instance=inst,
                    cluster=cluster,
                    soa_dir=DEFAULT_SOA_DIR,
                )
            git_sha = get_git_sha_from_dockerurl(soa_config.get_docker_url(), long=True)
            formatted_resource = format_custom_resource(
                instance_config=config,
                service=service,
                instance=inst,
                cluster=cluster,
                kind=kind.singular,
                version=version,
                group=group,
                namespace=f"paasta-{kind.plural}",
                git_sha=git_sha,
                is_eks=is_eks,
            )
            desired_resource = KubeCustomResource(
                service=service,
                instance=inst,
                config_sha=formatted_resource["metadata"]["labels"][
                    paasta_prefixed("config_sha")
                ],
                git_sha=formatted_resource["metadata"]["labels"].get(
                    paasta_prefixed("git_sha")
                ),
                kind=kind.singular,
                name=formatted_resource["metadata"]["name"],
                namespace=f"paasta-{kind.plural}",
            )
            if not (service, inst, kind.singular) in [
                (c.service, c.instance, c.kind) for c in custom_resources
            ]:
                log.info(f"{desired_resource} does not exist so creating")
                create_custom_resource(
                    kube_client=kube_client,
                    version=version,
                    kind=kind,
                    formatted_resource=formatted_resource,
                    group=group,
                )
            elif desired_resource not in custom_resources:
                sanitised_service = sanitise_kubernetes_name(service)
                sanitised_instance = sanitise_kubernetes_name(inst)
                log.info(f"{desired_resource} exists but config_sha doesn't match")
                update_custom_resource(
                    kube_client=kube_client,
                    name=f"{sanitised_service}-{sanitised_instance}",
                    version=version,
                    kind=kind,
                    formatted_resource=formatted_resource,
                    group=group,
                )
            else:
                log.info(f"{desired_resource} is up to date, no action taken")
        except Exception as e:
            log.error(str(e))
            succeeded = False
    return succeeded


if __name__ == "__main__":
    main()
