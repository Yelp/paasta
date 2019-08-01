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
Usage: ./check_flink_services_replication.py [options]
"""
import datetime
import logging
from typing import Sequence

from paasta_tools import flink_tools
from paasta_tools import monitoring_tools
from paasta_tools.check_services_replication_tools import main
from paasta_tools.flink_tools import FlinkDeploymentConfig
from paasta_tools.kubernetes_tools import filter_pods_by_service_instance
from paasta_tools.kubernetes_tools import is_pod_ready
from paasta_tools.kubernetes_tools import V1Pod
from paasta_tools.smartstack_tools import KubeSmartstackReplicationChecker


log = logging.getLogger(__name__)


def container_lifetime(pod: V1Pod,) -> datetime.timedelta:
    """Return a time duration for how long the pod is alive
    """
    st = pod.status.start_time
    return datetime.datetime.now(st.tzinfo) - st


def healthy_flink_containers_cnt(si_pods: Sequence[V1Pod], container_type: str) -> int:
    """Return count of healthy Flink containers with given type
    """
    return len(
        [
            pod
            for pod in si_pods
            if pod.metadata.labels["flink-container-type"] == container_type
            and is_pod_ready(pod)
            and container_lifetime(pod).total_seconds() > 60
        ]
    )


def check_flink_service_replication(
    instance_config: FlinkDeploymentConfig,
    all_pods: Sequence[V1Pod],
    smartstack_replication_checker: KubeSmartstackReplicationChecker,
) -> None:
    si_pods = filter_pods_by_service_instance(
        pod_list=all_pods,
        service=instance_config.service,
        instance=instance_config.instance,
    )
    taskmanagers_expected_cnt = instance_config.config_dict.get(
        "taskmanager", {"instances": 10}
    ).get("instances", 10)
    num_healthy_supervisors = healthy_flink_containers_cnt(si_pods, "supervisor")
    num_healthy_jobmanagers = healthy_flink_containers_cnt(si_pods, "jobmanager")
    num_healthy_taskmanagers = healthy_flink_containers_cnt(si_pods, "taskmanager")

    # TBD: check cnt according to Flink

    monitoring_tools.send_replication_event_if_under_replication(
        instance_config=instance_config,
        expected_count=1,
        num_available=num_healthy_supervisors,
        sub_component="supervisor",
    )
    monitoring_tools.send_replication_event_if_under_replication(
        instance_config=instance_config,
        expected_count=1,
        num_available=num_healthy_jobmanagers,
        sub_component="jobmanager",
    )
    monitoring_tools.send_replication_event_if_under_replication(
        instance_config=instance_config,
        expected_count=taskmanagers_expected_cnt,
        num_available=num_healthy_taskmanagers,
        sub_component="taskmanager",
    )


if __name__ == "__main__":
    main(
        flink_tools.FlinkDeploymentConfig,
        check_flink_service_replication,
        namespace="paasta-flinks",
    )
