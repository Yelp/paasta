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
import base64
import hashlib
import json
import logging
import sys
from typing import AbstractSet
from typing import List
from typing import Mapping
from typing import Set

import kubernetes.client as k8s

from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.kubernetes_tools import registration_prefixed
from paasta_tools.kubernetes_tools import sanitise_kubernetes_name


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
        type=int,
        help="Update or create up to this number of service instances. Default is 0 (no limit).",
    )
    parser.add_argument(
        "-f",
        "--file-path",
        dest="file_path",
        default="/nail/etc/services/services.json",
        metavar="LIMIT",
        type=str,
        help="services file to use for smartstack namespace. Default file is /nail/etc/services/services.json",
    )
    args = parser.parse_args()
    return args


def load_smartstack_namespaces(file_path: str) -> Mapping:
    with open(file_path) as service_file:
        return json.load(service_file)


def sanitise_kubernetes_service_name(name: str) -> str:
    name = sanitise_kubernetes_name(name)
    name = name.replace(".", "---")
    if len(name) > 63:
        digest = hashlib.md5(name.encode("utf-8")).digest()
        hash = base64.b64encode(digest, altchars=b"ps")[0:6].decode("utf-8")
        hash.replace("=", "")
        name = f"{name[0:56]}-{hash}"
    return name


def get_existing_kubernetes_service_names(kube_client: KubeClient) -> Set[str]:
    service_objects = kube_client.core.list_namespaced_service("paasta")
    if not service_objects:
        raise RuntimeError("Error retrieving services list from k8s api")

    return {
        item.metadata.name
        for item in service_objects.items
        if item.metadata.annotations
        if item.metadata.annotations.get(paasta_prefixed("managed_by"))
        == "setup_istio_mesh"
    }


def setup_unified_service(kube_client: KubeClient, port_list: List) -> k8s.V1Service:
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
    return kube_client.core.create_namespaced_service(PAASTA_NAMESPACE, service_object)


def setup_paasta_namespace_services(
    kube_client: KubeClient,
    paasta_namespaces: AbstractSet,
    existing_kube_services_names: Set[str] = set(),
    rate_limit: int = 0,
) -> bool:
    api_updates = 0
    status = True

    for namespace in paasta_namespaces:
        service = sanitise_kubernetes_service_name(namespace)
        if rate_limit > 0 and api_updates >= rate_limit:
            log.info(
                f"Not doing any further updates as we reached the limit ({api_updates})"
            )
            break

        if service in existing_kube_services_names:
            log.info(f"Service {service} alredy exists, skipping")
            continue

        log.info(f"Creating {service} because it does not exist yet.")

        service_meta = k8s.V1ObjectMeta(name=service, annotations=ANNOTATIONS)
        port_spec = k8s.V1ServicePort(
            name="http", port=PAASTA_SVC_PORT, protocol="TCP", app_protocol="http"
        )
        service_spec = k8s.V1ServiceSpec(
            selector={registration_prefixed(namespace): "true"}, ports=[port_spec]
        )
        service_object = k8s.V1APIService(metadata=service_meta, spec=service_spec)
        try:
            kube_client.core.create_namespaced_service(PAASTA_NAMESPACE, service_object)
            api_updates += 1
        except Exception as err:
            log.warning(f"{err} while setting up k8s service for {namespace}")
            status = False

    return status


def setup_kube_services(
    kube_client: KubeClient,
    rate_limit: int = 0,
    file_path: str = "/nail/etc/services/services.json",
) -> bool:
    existing_kube_services_names = get_existing_kubernetes_service_names(kube_client)

    namespaces = load_smartstack_namespaces(file_path)
    if UNIFIED_K8S_SVC_NAME not in existing_kube_services_names:
        try:
            setup_unified_service(
                kube_client=kube_client,
                port_list=sorted(val["port"] for val in namespaces.values()),
            )
        except Exception as err:
            log.error(f"{err} while setting up unified service")
            return False

    return setup_paasta_namespace_services(
        kube_client, namespaces.keys(), existing_kube_services_names, rate_limit
    )


def main() -> None:
    args = parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    kube_client = KubeClient()
    ensure_namespace(kube_client, namespace="paasta")

    setup_kube_succeeded = setup_kube_services(
        kube_client=kube_client, rate_limit=args.rate_limit, file_path=args.file_path,
    )

    sys.exit(0 if setup_kube_succeeded else 1)


if __name__ == "__main__":
    main()
