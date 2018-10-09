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
Usage: ./check_kubernetes_services_replication.py [options]

This is a script that checks the number of HAProxy backends via Synapse against
the expected amount that should've been deployed via Kubernetes.

Basically, the script checks smartstack.yaml for listed namespaces, and then queries
Synapse for the number of available backends for that namespace. It then goes through
the Kubernetes service configuration file for that cluster, and sees how many instances
are expected to be available for that namespace based on the number of instances deployed
on that namespace.

After retrieving that information, a fraction of available instances is calculated
(available/expected), and then compared against a threshold. The default threshold is
50, meaning if less than 50% of a service's backends are available, the script sends
CRITICAL. If replication_threshold is defined in the yelpsoa config for a service
instance then it will be used instead.
"""
import argparse
import logging

from paasta_tools import kubernetes_tools
from paasta_tools import monitoring_tools
from paasta_tools.kubernetes_tools import filter_pods_by_service_instance
from paasta_tools.kubernetes_tools import get_all_nodes
from paasta_tools.kubernetes_tools import get_all_pods
from paasta_tools.kubernetes_tools import is_pod_ready
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.long_running_service_tools import get_proxy_port_for_instance
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.smartstack_tools import KubeSmartstackReplicationChecker
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config


log = logging.getLogger(__name__)


def parse_args():
    epilog = "PERCENTAGE is an integer value representing the percentage of available to expected instances"
    parser = argparse.ArgumentParser(epilog=epilog)

    parser.add_argument(
        '-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        dest="verbose", default=False,
    )
    options = parser.parse_args()

    return options


def check_healthy_kubernetes_tasks_for_service_instance(
    instance_config,
    expected_count,
    all_pods,
):
    si_pods = filter_pods_by_service_instance(
        pod_list=all_pods,
        service=instance_config.service,
        instance=instance_config.instance,
    )
    num_healthy_tasks = len([pod for pod in si_pods if is_pod_ready(pod)])
    log.info(f"Checking {instance_config.service}.{instance_config.instance} in kubernetes as it is not in smartstack")
    monitoring_tools.send_replication_event_if_under_replication(
        instance_config=instance_config,
        expected_count=expected_count,
        num_available=num_healthy_tasks,
    )


def check_service_replication(
    instance_config: KubernetesDeploymentConfig,
    all_pods,
    smartstack_replication_checker: KubeSmartstackReplicationChecker,
) -> None:
    """Checks a service's replication levels based on how the service's replication
    should be monitored. (smartstack or k8s)

    :param instance_config: an instance of KubernetesDeploymentConfig
    :param smartstack_replication_checker: an instance of KubeSmartstackReplicationChecker
    """
    expected_count = instance_config.get_instances()
    log.info("Expecting %d total tasks for %s" % (expected_count, instance_config.job_id))
    proxy_port = get_proxy_port_for_instance(instance_config)

    registrations = instance_config.get_registrations()
    # if the primary registration does not match the service_instance name then
    # the best we can do is check k8s for replication (for now).
    if proxy_port is not None and registrations[0] == instance_config.job_id:
        monitoring_tools.check_smartstack_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_count,
            smartstack_replication_checker=smartstack_replication_checker,
        )
    else:
        check_healthy_kubernetes_tasks_for_service_instance(
            instance_config=instance_config,
            expected_count=expected_count,
            all_pods=all_pods,
        )


def check_all_kubernetes_services_replication(soa_dir: str) -> None:
    kube_client = KubeClient()
    all_pods = get_all_pods(kube_client)
    all_nodes = get_all_nodes(kube_client)
    system_paasta_config = load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()
    smartstack_replication_checker = KubeSmartstackReplicationChecker(
        nodes=all_nodes,
        system_paasta_config=system_paasta_config,
    )

    for service in list_services(soa_dir=soa_dir):
        service_config = PaastaServiceConfigLoader(service=service, soa_dir=soa_dir)
        for instance_config in service_config.instance_configs(
            cluster=cluster,
            instance_type_class=kubernetes_tools.KubernetesDeploymentConfig,
        ):
            if instance_config.get_docker_image():
                check_service_replication(
                    instance_config=instance_config,
                    all_pods=all_pods,
                    smartstack_replication_checker=smartstack_replication_checker,
                )
            else:
                log.debug(
                    '%s is not deployed. Skipping replication monitoring.' %
                    instance_config.job_id,
                )


def main() -> None:
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)
    check_all_kubernetes_services_replication(soa_dir=args.soa_dir)


if __name__ == "__main__":
    main()
