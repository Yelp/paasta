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
Usage: ./delete_paasta_contract_monitor.py

The following script is a setup on a cron job in k8s masters. This is responsible for deleting
paasta-contract-monitor deployments and its services. By deleting the deployment itself,
setup_kubernetes_job.py will be able to reschedule the deployment and its pods on different nodes.
"""
import logging
import subprocess
import sys
from subprocess import PIPE

from paasta_tools.kubernetes_tools import delete_deployment
from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import KubeClient

log = logging.getLogger(__name__)


def get_paasta_contract_monitor_deployment_names():
    p1 = subprocess.Popen(
        [
            "python",
            "-m",
            "paasta_tools.list_kubernetes_service_instances",
            "--sanitise",
        ],
        stdout=PIPE,
    )
    p2 = subprocess.Popen(
        ["grep", "paasta-contract-monitor"], stdin=p1.stdout, stdout=PIPE
    )
    p1.stdout.close()
    output = p2.communicate()[0].decode("utf-8").split()
    return output


def main() -> None:
    pcm_deployment_names = get_paasta_contract_monitor_deployment_names()
    log.debug(f"Deleting deployments: {pcm_deployment_names}")
    kube_client = KubeClient()
    ensure_namespace(kube_client, namespace="paasta")

    for deployment_name in pcm_deployment_names:
        try:
            delete_deployment(kube_client, deployment_name)
        except Exception as err:
            log.error(f"Unable to delete {deployment_name}: {err}")

    sys.exit(0)


if __name__ == "__main__":
    main()
