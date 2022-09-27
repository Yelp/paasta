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
Usage: ./setup_kubernetes_services.py <service.instance>

Command line options:

- -c, --cluster: PAASTA cluster
- -v, --verbose: Verbose output
"""
import argparse
import logging
import os
import sys
import time
from functools import partial
from typing import AbstractSet
from typing import Iterator
from typing import Mapping
from typing import Set

import kubernetes.client as k8s
import yaml

from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import limit_size_with_hash
from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.kubernetes_tools import registration_label
from paasta_tools.kubernetes_tools import sanitise_kubernetes_name
from paasta_tools.utils import DEFAULT_SOA_DIR


log = logging.getLogger(__name__)

UNIFIED_K8S_SVC_NAME = "paasta-routing"
UNIFIED_SVC_PORT = 1337
PAASTA_SVC_PORT = 8888
PAASTA_NAMESPACE = "paasta"
ANNOTATIONS = {paasta_prefixed("managed_by"): "setup_istio_mesh"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Creates Kubernetes services.")
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
    )
    parser.add_argument(
        "-l",
        "--rate-limit",
        dest="rate_limit",
        default=0,
        metavar="LIMIT",
        type=float,
        help="Maximum number of write calls to k8s per second. Default is 0 (no limit).",
    )
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        default=DEFAULT_SOA_DIR,
        metavar="LIMIT",
        type=str,
        help=f"Directory with service declarations. Default is {DEFAULT_SOA_DIR}",
    )
    args = parser.parse_args()
    return args


def load_smartstack_namespaces(soa_dir: str = DEFAULT_SOA_DIR) -> Mapping:
    namespaces = {}

    _, dirs, _ = next(os.walk(soa_dir))
    for dir in dirs:
        file_path = f"{soa_dir}/{dir}/smartstack.yaml"
        if not os.path.isfile(file_path):
            continue
        try:
            with open(file_path) as f:
                svc_namespaces = yaml.load(f, Loader=yaml.CSafeLoader)
                for (ns, details) in svc_namespaces.items():
                    namespaces[f"{dir}.{ns}"] = details
        except Exception as err:
            log.warn(f"Failed to load namespaces for {dir}: {err}")

    return namespaces


def sanitise_kubernetes_service_name(name: str) -> str:
    return limit_size_with_hash(sanitise_kubernetes_name(name).replace(".", "---"))


def get_existing_kubernetes_service_names(kube_client: KubeClient) -> Set[str]:
    service_objects = kube_client.core.list_namespaced_service(PAASTA_NAMESPACE)

    return {
        item.metadata.name
        for item in service_objects.items
        if item.metadata.annotations
        if item.metadata.annotations.get(paasta_prefixed("managed_by"))
        == "setup_istio_mesh"
    }


def get_existing_kubernetes_virtual_services(kube_client: KubeClient) -> Set[str]:
    virtual_service_objects = kube_client.custom.list_namespaced_custom_object(
        "networking.istio.io", "v1beta1", PAASTA_NAMESPACE, "virtualservices"
    )

    return {item["metadata"]["name"] for item in virtual_service_objects["items"]}


def setup_paasta_routing(kube_client: KubeClient, namespaces: Mapping) -> Iterator:
    # Add smartstack ports for routing, Clients can connect to this
    # Directly without need of setting x-yelp-svc header
    # Add port 1337 for envoy unified listener.
    # Clients can connect to this listenner and set x-yelp-svc header for routing
    port_list = sorted(
        val["proxy_port"] for val in namespaces.values() if val.get("proxy_port")
    )
    ports = [
        k8s.V1ServicePort(
            name=f"p{port}",
            port=port,
            protocol="TCP",
            target_port=PAASTA_SVC_PORT,
            app_protocol="http",
        )
        for port in [1337, *port_list]
    ]

    service_meta = k8s.V1ObjectMeta(name=UNIFIED_K8S_SVC_NAME, annotations=ANNOTATIONS)
    service_spec = k8s.V1ServiceSpec(ports=ports)
    service_object = k8s.V1APIService(metadata=service_meta, spec=service_spec)
    yield partial(
        kube_client.core.create_namespaced_service, PAASTA_NAMESPACE, service_object
    )

    sorted_namespaces = sorted(namespaces.keys())
    x_yelp_svc_routes = [
        dict(
            match=[dict(headers={"x-yelp-svc": dict(exact=mesh_ns)})],
            delegate=dict(
                name=sanitise_kubernetes_service_name(mesh_ns),
                namespace=PAASTA_NAMESPACE,
            ),
        )
        for mesh_ns in sorted_namespaces
    ]
    port_routes = [
        dict(
            match=[dict(port=namespaces[mesh_ns]["proxy_port"])],
            delegate=dict(
                name=sanitise_kubernetes_service_name(mesh_ns),
                namespace=PAASTA_NAMESPACE,
            ),
        )
        for mesh_ns in sorted_namespaces
        if namespaces[mesh_ns].get("proxy_port")
    ]
    virtual_service = dict(
        apiVersion="networking.istio.io/v1alpha3",
        kind="VirtualService",
        metadata=dict(
            name="paasta-routing",
            namespace=PAASTA_NAMESPACE,
        ),
        spec=dict(
            hosts=["paasta-routing", "169.254.255.254"],
            http=x_yelp_svc_routes + port_routes,
        ),
    )

    yield partial(
        kube_client.custom.create_namespaced_custom_object,
        "networking.istio.io",
        "v1alpha3",
        PAASTA_NAMESPACE,
        "virtualservices",
        virtual_service,
    )


def setup_paasta_namespace_services(
    kube_client: KubeClient,
    paasta_namespaces: AbstractSet,
    existing_namespace_services: Set[str],
    existing_virtual_services: Set[str],
) -> Iterator:
    for namespace in paasta_namespaces:
        service = sanitise_kubernetes_service_name(namespace)
        if service not in existing_namespace_services:
            log.info(f"Creating k8s service {service} because it does not exist yet.")

            service_meta = k8s.V1ObjectMeta(name=service, annotations=ANNOTATIONS)
            port_spec = k8s.V1ServicePort(
                name="http", port=PAASTA_SVC_PORT, protocol="TCP", app_protocol="http"
            )
            service_spec = k8s.V1ServiceSpec(
                selector={registration_label(namespace): "true"}, ports=[port_spec]
            )
            service_object = k8s.V1APIService(metadata=service_meta, spec=service_spec)
            yield partial(
                kube_client.core.create_namespaced_service,
                PAASTA_NAMESPACE,
                service_object,
            )

        if service not in existing_virtual_services:
            log.info(
                f"Creating istio virtualservice {service} because it does not exist yet."
            )

            route = dict(
                destination=dict(host=service, port=dict(number=PAASTA_SVC_PORT))
            )
            virtual_service = dict(
                apiVersion="networking.istio.io/v1alpha3",
                kind="VirtualService",
                metadata=dict(name=service, namespace=PAASTA_NAMESPACE),
                spec=dict(http=[dict(route=[route])]),
            )
            yield partial(
                kube_client.custom.create_namespaced_custom_object,
                "networking.istio.io",
                "v1alpha3",
                PAASTA_NAMESPACE,
                "virtualservices",
                virtual_service,
            )


def cleanup_paasta_namespace_services(
    kube_client: KubeClient,
    paasta_namespaces: AbstractSet,
    existing_namespace_services: Set[str],
    existing_virtual_services: Set[str],
) -> Iterator:
    declared_services = {
        sanitise_kubernetes_service_name(ns) for ns in paasta_namespaces
    }
    for service in existing_namespace_services:
        if service == UNIFIED_K8S_SVC_NAME or service in declared_services:
            continue
        log.info(f"Garbage collecting K8s Service {service}")
        yield partial(
            kube_client.core.delete_namespaced_service, service, PAASTA_NAMESPACE
        )
    for service in existing_virtual_services:
        if service == UNIFIED_K8S_SVC_NAME or service in declared_services:
            continue
        log.info(f"Garbage collecting Istio VS {service}")
        yield partial(
            kube_client.custom.delete_namespaced_custom_object,
            "networking.istio.io",
            "v1beta1",
            PAASTA_NAMESPACE,
            "virtualservices",
            service,
        )


def process_kube_services(
    kube_client: KubeClient, soa_dir: str = DEFAULT_SOA_DIR
) -> Iterator:

    existing_namespace_services = get_existing_kubernetes_service_names(kube_client)
    existing_virtual_services = get_existing_kubernetes_virtual_services(kube_client)

    namespaces = load_smartstack_namespaces(soa_dir)

    should_setup_unified = (
        UNIFIED_K8S_SVC_NAME not in existing_namespace_services
        or UNIFIED_K8S_SVC_NAME not in existing_virtual_services
    )

    if should_setup_unified:
        log.info(f"Creating {UNIFIED_K8S_SVC_NAME} because it does not exist yet.")
        yield from setup_paasta_routing(
            kube_client=kube_client,
            namespaces=namespaces,
        )

    yield from setup_paasta_namespace_services(
        kube_client,
        namespaces.keys(),
        existing_namespace_services,
        existing_virtual_services,
    )

    yield from cleanup_paasta_namespace_services(
        kube_client,
        namespaces.keys(),
        existing_namespace_services,
        existing_virtual_services,
    )


def setup_istio_mesh(
    kube_client: KubeClient,
    rate_limit: int = 0,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> bool:
    delay = 0 if rate_limit == 0 else 1.0 / float(rate_limit)
    took = delay
    success = True
    for fn in process_kube_services(kube_client=kube_client, soa_dir=soa_dir):
        time.sleep(max(0, delay - took))
        try:
            log.debug(f"Calling yielded {fn.func}({fn.args})")
            start = time.time()
            result = fn()
            took = time.time() - start
            log.debug(f"Result: {result}, took {took}s")
        except Exception:
            success = False
            log.exception(f"Failed calling {fn.func}({fn.args})")
    return success


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    kube_client = KubeClient()
    ensure_namespace(kube_client, namespace=PAASTA_NAMESPACE)
    success = setup_istio_mesh(kube_client, args.rate_limit, args.soa_dir)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
