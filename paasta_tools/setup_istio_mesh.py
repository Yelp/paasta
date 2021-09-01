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
from typing import AbstractSet
from typing import Iterator
from typing import List
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
        "-v", "--verbose", action="store_true", dest="verbose", default=False,
    )
    parser.add_argument(
        "--dry-run", action="store_true", dest="dry_run", default=False,
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
    if not service_objects:
        raise RuntimeError("Error retrieving services list from k8s api")

    return {
        item.metadata.name
        for item in service_objects.items
        if item.metadata.annotations
        if item.metadata.annotations.get(paasta_prefixed("managed_by"))
        == "setup_istio_mesh"
    }


def setup_unified_service(kube_client: KubeClient, port_list: List) -> Iterator:
    # Add smartstack ports for routing, Clients can connect to this
    # Directly without need of setting x-yelp-svc header
    # Add port 1337 for envoy unified listener.
    # Clients can connect to this listenner and set x-yelp-svc header for routing
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
    yield kube_client.core.create_namespaced_service, (PAASTA_NAMESPACE, service_object)


def setup_paasta_namespace_services(
    kube_client: KubeClient,
    paasta_namespaces: AbstractSet,
    existing_namespace_services: Set[str] = set(),
) -> Iterator:
    for namespace in paasta_namespaces:
        service = sanitise_kubernetes_service_name(namespace)

        if service in existing_namespace_services:
            log.debug(f"Service {service} alredy exists, skipping")
            continue

        log.info(f"Creating {service} because it does not exist yet.")

        service_meta = k8s.V1ObjectMeta(name=service, annotations=ANNOTATIONS)
        port_spec = k8s.V1ServicePort(
            name="http", port=PAASTA_SVC_PORT, protocol="TCP", app_protocol="http"
        )
        service_spec = k8s.V1ServiceSpec(
            selector={registration_label(namespace): "true"}, ports=[port_spec]
        )
        service_object = k8s.V1APIService(metadata=service_meta, spec=service_spec)
        yield kube_client.core.create_namespaced_service, (
            PAASTA_NAMESPACE,
            service_object,
        )


def cleanup_paasta_namespace_services(
    kube_client: KubeClient,
    paasta_namespaces: AbstractSet,
    existing_namespace_services: Set[str],
) -> Iterator:
    declared_services = {
        sanitise_kubernetes_service_name(ns) for ns in paasta_namespaces
    }
    for service in existing_namespace_services:
        if service == UNIFIED_K8S_SVC_NAME or service in declared_services:
            continue
        log.info(
            f"Garbage collecting {service} since there is no reference in services.yaml"
        )
        yield kube_client.core.delete_namespaced_service, (service, PAASTA_NAMESPACE)


def process_kube_services(
    kube_client: KubeClient, soa_dir: str = DEFAULT_SOA_DIR
) -> Iterator:
    existing_namespace_services = get_existing_kubernetes_service_names(kube_client)
    namespaces = load_smartstack_namespaces(soa_dir)

    if UNIFIED_K8S_SVC_NAME not in existing_namespace_services:
        log.info(f"Creating {UNIFIED_K8S_SVC_NAME} because it does not exist yet.")
        yield from setup_unified_service(
            kube_client=kube_client,
            port_list=sorted(
                val["proxy_port"]
                for val in namespaces.values()
                if val.get("proxy_port")
            ),
        )

    yield from setup_paasta_namespace_services(
        kube_client, namespaces.keys(), existing_namespace_services
    )

    yield from cleanup_paasta_namespace_services(
        kube_client, namespaces.keys(), existing_namespace_services
    )


def main() -> None:
    args = parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    kube_client = KubeClient()
    ensure_namespace(kube_client, namespace="paasta")
    delay = 0 if args.rate_limit == 0 else 1.0 / float(args.rate_limit)
    success = True
    for (fn, args) in process_kube_services(
        kube_client=kube_client, soa_dir=args.soa_dir
    ):
        time.sleep(delay)
        try:
            fn(*args)
        except Exception:
            success = False
            log.exception(f"Failed setting up {fn}({args})")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
