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
import sys
from typing import Sequence

from kubernetes.client import V1APIService
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1ServiceSpec

from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import list_kubernetes_services
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import SPACER
from paasta_tools.utils import validate_registration_name

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Creates Kubernetes services.")
    parser.add_argument(
        "service_instance_list",
        nargs="+",
        help="The list of Kubernetes service instances to create or update services for",
        metavar="SERVICE%sINSTANCE" % SPACER,
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", dest="verbose", default=False,
    )
    args = parser.parse_args()
    return args


def setup_kube_services(
    kube_client: KubeClient, service_instances: Sequence[str], rate_limit: int = 0,
) -> bool:
    if service_instances:
        existing_kube_services = list_kubernetes_services(kube_client)
        existing_kube_services_names = {
            decompose_job_id(item.name) for item in existing_kube_services
        }

    service_instances_with_valid_names = [
        decompose_job_id(service_instance)
        for service_instance in service_instances
        if validate_registration_name(service_instance)
    ]

    if len(service_instances) != len(service_instances_with_valid_names):
        return False

    api_updates = 0

    for service in service_instances_with_valid_names:
        service_name = f"{service[0]}.{service[1]}"

        if rate_limit > 0 and api_updates >= rate_limit:
            log.info(
                f"Not doing any further updates as we reached the limit ({api_updates})"
            )
            break

        if service_name in existing_kube_services_names:
            log.info(f"Service {service_name} alredy exists, skipping")
            continue

        log.info(f"Creating {service_name} because it does not exist yet.")

        service_meta = V1ObjectMeta(name=service_name)

        service_spec = V1ServiceSpec(
            selector={f"registrations.paasta.yelp.com/{service_name}": True}
        )

        service_instance = V1APIService(metadata=service_meta, spec=service_spec)

        kube_client.core.create_namespaced_service("paasta", service_instance)

        api_updates += 1

    return True


def main() -> None:
    args = parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    kube_client = KubeClient()
    ensure_namespace(kube_client, namespace="paasta")

    setup_kube_succeeded = setup_kube_services(
        kube_client=kube_client,
        service_instances=args.service_instance_list,
        rate_limit=args.rate_limit,
    )

    sys.exit(0 if setup_kube_succeeded else 1)


if __name__ == "__main__":
    main()
