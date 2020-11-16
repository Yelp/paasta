#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
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
Usage: ./delete_paasta_contract_monitor.py <service.instance> [options]

The following script is a setup on a cron job in k8s masters. This is responsible for deleting
paasta-contract-monitor deployments and its services. By deleting the deployment itself,
setup_kubernetes_job.py will be able to reschedule the deployment and its pods on different nodes.
"""
import argparse
import logging
import sys

from paasta_tools.kubernetes_tools import delete_deployment
from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import get_kubernetes_app_name
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import SPACER

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deletes list of deployments.")
    parser.add_argument(
        "service_instance_list",
        nargs="+",
        help="The list of service instances to delete",
        metavar=f"SERVICE{SPACER}INSTANCE",
    )
    args = parser.parse_args()
    return args


def get_deployment_names_from_list(service_instance_list):
    app_names = []
    for service_instance in service_instance_list:
        try:
            service, instance, _, __ = decompose_job_id(service_instance)
            app_name = get_kubernetes_app_name(service, instance)
            app_names.append(app_name)
        except InvalidJobNameError:
            log.error(
                f"Invalid service instance specified. Format is service{SPACER}instance."
            )
            sys.exit(1)
    return app_names


def main() -> None:
    args = parse_args()
    service_instance_list = args.service_instance_list
    deployment_names = get_deployment_names_from_list(service_instance_list)

    log.debug(f"Deleting deployments: {deployment_names}")
    kube_client = KubeClient()
    ensure_namespace(kube_client=kube_client, namespace="paasta")

    for deployment_name in deployment_names:
        try:
            log.debug(f"Deleting {deployment_name}")
            delete_deployment(kube_client=kube_client, deployment_name=deployment_name)
        except Exception as err:
            log.error(f"Unable to delete {deployment_name}: {err}")
            sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
