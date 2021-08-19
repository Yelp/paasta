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
import json
import logging
import sys
from typing import Mapping
from typing import Sequence

import kubernetes.client as k8s
from mypy_extensions import TypedDict

from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import sanitise_kubernetes_name


log = logging.getLogger(__name__)

UNIFIED_K8S_SVC_NAME = "paasta-routing"
UNIFIED_SVC_PORT = 1337
PAASTA_SVC_PORT = 8888
PAATA_REGISTRATION_PREFIX = "registrations.paasta.yelp.com"
PAASTA_SERVICE_FILE = "/nail/etc/"
PAASTA_NAMESPACE = "paasta"

KubeSvcLabels = TypedDict(
    "KubeSvcLabels",
    {"paasta.yelp.com/owner": str, "paasta.yelp.com/unified_service": str},
)


class ErrorCreatingUnifiedService(Exception):
    pass


class ErrorGettingServiceList(Exception):
    pass


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


def build_port_namespace_mapping(file_path: str) -> Mapping:
    paasta_namespaces = {}
    with open(file_path) as service_file:
        smartstack_namespaces = json.load(service_file)
        for namespace, endpoint in smartstack_namespaces.items():
            paasta_namespaces[endpoint["port"]] = namespace
    return paasta_namespaces


def sanitise_kubernetes_service_name(name: str) -> str:
    name = sanitise_kubernetes_name(name)
    return name.replace(".", "--")


def get_existing_kubernetes_service_names(kube_client: KubeClient) -> Sequence:
    label_selector = "paasta.yelp.com/owner=istio"
    service_objects = kube_client.core.list_namespaced_service(
        "paasta", label_selector=label_selector
    )
    if not service_objects:
        raise ErrorGettingServiceList("Error retrieving services list from k8s api")

    return {item.metadata.name for item in service_objects.items}


def setup_unified_service(
    kube_client: KubeClient, port_list: Sequence
) -> k8s.V1Service:

    # Add port 1337 for envoy unified listener.
    # Clients can connect to this listenner and set x-yelp-svc header for routing
    ports = [
        k8s.V1ServicePort(
            name=f"p{UNIFIED_SVC_PORT}",
            port=UNIFIED_SVC_PORT,
            protocol="TCP",
            target_port=PAASTA_SVC_PORT,
            appProtocol="http",
        )
    ]

    # Add smartstack ports for routing, Clients can connect to this
    # Directly without need of setting x-yelp-svc header
    for port in port_list:
        port_spec = k8s.V1ServicePort(
            name=f"p{port}",
            port=port,
            protocol="TCP",
            target_port=PAASTA_SVC_PORT,
            appProtocol="http",
        )
        ports.append(port_spec)

    service_labels: KubeSvcLabels = {
        "paasta.yelp.com/owner": "istio",
        "paasta.yelp.com/unified_service": "true",
    }

    service_meta = k8s.V1ObjectMeta(name=UNIFIED_K8S_SVC_NAME, labels=service_labels)
    service_spec = k8s.V1ServiceSpec(ports=ports)
    service_object = k8s.V1APIService(metadata=service_meta, spec=service_spec)
    return kube_client.core.create_namespaced_service(PAASTA_NAMESPACE, service_object)


def setup_paasta_namespace_service(
    kube_client: KubeClient,
    paasta_namespaces: Mapping,
    existing_kube_services_names: Sequence = set(),
    rate_limit: int = 0,
) -> bool:
    api_updates = 0
    status = True

    for namespace in paasta_namespaces.values():
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

        service_labels: KubeSvcLabels = {"paasta.yelp.com/owner": "istio"}
        service_meta = k8s.V1ObjectMeta(name=service, labels=service_labels)
        service_spec = k8s.V1ServiceSpec(
            selector={f"{PAATA_REGISTRATION_PREFIX}/{namespace}": "true"},
            ports=[
                k8s.V1ServicePort(name="http", port=PAASTA_SVC_PORT, protocol="TCP")
            ],
        )
        service_object = k8s.V1APIService(metadata=service_meta, spec=service_spec)
        try:
            kube_client.core.create_namespaced_service(PAASTA_NAMESPACE, service_object)
        except k8s.ApiException as Error:
            log.warning(
                f"""got {Error} error  while setting up
                        k8s service for {namespace}"""
            )
            status = False

        api_updates += 1
    return status


def setup_kube_services(
    kube_client: KubeClient,
    rate_limit: int = 0,
    file_path: str = "/nail/etc/services/services.json",
) -> bool:
    existing_kube_services_names = get_existing_kubernetes_service_names(kube_client)

    paasta_namespaces: Mapping = build_port_namespace_mapping(file_path)
    if UNIFIED_K8S_SVC_NAME not in existing_kube_services_names:
        try:
            setup_unified_service(
                kube_client=kube_client, port_list=paasta_namespaces.keys()
            )
        except k8s.ApiException as Error:
            log.error(f"""got {Error} while setting up unified service""")
            return False

    return setup_paasta_namespace_service(
        kube_client, paasta_namespaces, existing_kube_services_names, rate_limit
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
