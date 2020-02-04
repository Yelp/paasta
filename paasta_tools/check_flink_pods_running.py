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
Usage: ./check_flink_pods_running.py [options]
"""
import logging
from typing import Sequence

import pysensu_yelp

from paasta_tools import flink_tools
from paasta_tools import kubernetes_tools
from paasta_tools.check_services_replication_tools import main
from paasta_tools.flink_tools import FlinkDeploymentConfig
from paasta_tools.kubernetes_tools import filter_pods_by_service_instance
from paasta_tools.kubernetes_tools import PodStatus
from paasta_tools.kubernetes_tools import V1Pod
from paasta_tools.monitoring_tools import check_under_replication
from paasta_tools.monitoring_tools import send_replication_event
from paasta_tools.smartstack_tools import KubeSmartstackReplicationChecker

log = logging.getLogger(__name__)
TEAM = "stream-processing-flink"


def running_flink_pods_cnt(si_pods: Sequence[V1Pod]) -> int:
    """Return count of running pods
    """
    return len(
        [
            pod
            for pod in si_pods
            if kubernetes_tools.get_pod_status(pod) == PodStatus.RUNNING
        ]
    )


def check_flink_pods_running(
    instance_config: FlinkDeploymentConfig,
    all_tasks_or_pods: Sequence[V1Pod],
    smartstack_replication_checker: KubeSmartstackReplicationChecker,
) -> None:
    si_pods = filter_pods_by_service_instance(
        pod_list=all_tasks_or_pods,
        service=instance_config.service,
        instance=instance_config.instance,
    )
    taskmanager_expected_running_cnt = instance_config.config_dict.get(
        "taskmanager", {"instances": 10}
    ).get("instances", 10)
    # Total expected running count:
    # +1 for supervisor
    # +1 for jobmanager
    # + number of configured taskmanagers, defaults to 10
    total_expected_running_cnt = 1 + 1 + taskmanager_expected_running_cnt
    result = check_under_replication(
        instance_config=instance_config,
        expected_count=total_expected_running_cnt,
        num_available=running_flink_pods_cnt(si_pods),
    )
    pod_not_running, output = result
    if pod_not_running:
        log.error(output)
        status = pysensu_yelp.Status.CRITICAL
    else:
        log.info(output)
        status = pysensu_yelp.Status.OK
    send_replication_event(
        instance_config=instance_config,
        status=status,
        output=output,
        team_override=TEAM,
    )


if __name__ == "__main__":
    main(
        flink_tools.FlinkDeploymentConfig,
        check_flink_pods_running,
        namespace="paasta-flinks",
    )
