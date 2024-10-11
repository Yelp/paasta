#!/usr/bin/env python
# Copyright 2015-2019 Yelp Inc.
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
import logging
from typing import Optional
from typing import Sequence
from typing import Union

from paasta_tools import eks_tools
from paasta_tools import kubernetes_tools
from paasta_tools import monitoring_tools
from paasta_tools.check_services_replication_tools import main
from paasta_tools.check_services_replication_tools import parse_args
from paasta_tools.eks_tools import EksDeploymentConfig
from paasta_tools.kubernetes_tools import filter_pods_by_service_instance
from paasta_tools.kubernetes_tools import is_pod_ready
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import V1Pod
from paasta_tools.long_running_service_tools import get_proxy_port_for_instance
from paasta_tools.smartstack_tools import KubeSmartstackEnvoyReplicationChecker


log = logging.getLogger(__name__)
DEFAULT_ALERT_AFTER = "10m"


def check_healthy_kubernetes_tasks_for_service_instance(
    instance_config: Union[KubernetesDeploymentConfig, EksDeploymentConfig],
    expected_count: int,
    all_pods: Sequence[V1Pod],
    dry_run: bool = False,
) -> None:
    si_pods = filter_pods_by_service_instance(
        pod_list=all_pods,
        service=instance_config.service,
        instance=instance_config.instance,
    )
    num_healthy_tasks = len([pod for pod in si_pods if is_pod_ready(pod)])
    log.info(
        f"Checking {instance_config.service}.{instance_config.instance} in kubernetes as it is not in smartstack"
    )
    monitoring_tools.send_replication_event_if_under_replication(
        instance_config=instance_config,
        expected_count=expected_count,
        num_available=num_healthy_tasks,
        dry_run=dry_run,
    )


def check_kubernetes_pod_replication(
    instance_config: Union[KubernetesDeploymentConfig, EksDeploymentConfig],
    all_pods: Sequence[V1Pod],
    replication_checker: KubeSmartstackEnvoyReplicationChecker,
    dry_run: bool = False,
) -> Optional[bool]:
    """Checks a service's replication levels based on how the service's replication
    should be monitored. (smartstack/envoy or k8s)

    :param instance_config: an instance of KubernetesDeploymentConfig or EksDeploymentConfig
    :param replication_checker: an instance of KubeSmartstackEnvoyReplicationChecker
    """
    default_alert_after = DEFAULT_ALERT_AFTER
    expected_count = instance_config.get_instances()
    log.info(
        "Expecting %d total tasks for %s" % (expected_count, instance_config.job_id)
    )
    proxy_port = get_proxy_port_for_instance(instance_config)

    registrations = instance_config.get_registrations()

    # If this instance does not autoscale and only has 1 instance, set alert after to 20m.
    # Otherwise, set it to 10 min.
    if (
        not instance_config.is_autoscaling_enabled()
        and instance_config.get_instances() == 1
    ):
        default_alert_after = "20m"
    if "monitoring" not in instance_config.config_dict:
        instance_config.config_dict["monitoring"] = {}
    instance_config.config_dict["monitoring"][
        "alert_after"
    ] = instance_config.config_dict["monitoring"].get(
        "alert_after", default_alert_after
    )

    # if the primary registration does not match the service_instance name then
    # the best we can do is check k8s for replication (for now).
    if proxy_port is not None and registrations[0] == instance_config.job_id:
        is_well_replicated = monitoring_tools.check_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_count,
            replication_checker=replication_checker,
            dry_run=dry_run,
        )
        return is_well_replicated
    else:
        check_healthy_kubernetes_tasks_for_service_instance(
            instance_config=instance_config,
            expected_count=expected_count,
            all_pods=all_pods,
            dry_run=dry_run,
        )
        return None


if __name__ == "__main__":
    args = parse_args()
    main(
        instance_type_class=eks_tools.EksDeploymentConfig
        if args.eks
        else kubernetes_tools.KubernetesDeploymentConfig,
        check_service_replication=check_kubernetes_pod_replication,
    )
