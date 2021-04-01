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
Usage: ./check_marathon_services_replication.py [options]

This is a script that checks the number of HAProxy backends via Synapse against
the expected amount that should've been deployed via Marathon in a mesos cluster.

Basically, the script checks smartstack.yaml for listed namespaces, and then queries
Synapse for the number of available backends for that namespace. It then goes through
the Marathon service configuration file for that cluster, and sees how many instances
are expected to be available for that namespace based on the number of instances deployed
on that namespace.

After retrieving that information, a fraction of available instances is calculated
(available/expected), and then compared against a threshold. The default threshold is
50, meaning if less than 50% of a service's backends are available, the script sends
CRITICAL. If replication_threshold is defined in the yelpsoa config for a service
instance then it will be used instead.
"""
import logging
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Optional
from typing import Sequence

from marathon.models.task import MarathonTask

from paasta_tools import marathon_tools
from paasta_tools import monitoring_tools
from paasta_tools.check_services_replication_tools import main
from paasta_tools.long_running_service_tools import get_proxy_port_for_instance
from paasta_tools.marathon_tools import format_job_id
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.smartstack_tools import MesosSmartstackEnvoyReplicationChecker

log = logging.getLogger(__name__)


def filter_healthy_marathon_instances_for_short_app_id(all_tasks, app_id):
    tasks_for_app = [
        task for task in all_tasks if task.app_id.startswith("/%s" % app_id)
    ]
    one_minute_ago = datetime.now(timezone.utc) - timedelta(minutes=1)

    healthy_tasks = []
    for task in tasks_for_app:
        if (
            marathon_tools.is_task_healthy(task, default_healthy=True)
            and task.started_at is not None
            and task.started_at < one_minute_ago
        ):
            healthy_tasks.append(task)
    return len(healthy_tasks)


def check_healthy_marathon_tasks_for_service_instance(
    instance_config, expected_count, all_tasks, dry_run=False,
):
    app_id = format_job_id(instance_config.service, instance_config.instance)
    num_healthy_tasks = filter_healthy_marathon_instances_for_short_app_id(
        all_tasks=all_tasks, app_id=app_id
    )
    log.info("Checking %s in marathon as it is not in smartstack" % app_id)
    monitoring_tools.send_replication_event_if_under_replication(
        instance_config=instance_config,
        expected_count=expected_count,
        num_available=num_healthy_tasks,
        dry_run=dry_run,
    )


def check_service_replication(
    instance_config: MarathonServiceConfig,
    all_tasks_or_pods: Sequence[MarathonTask],
    replication_checker: MesosSmartstackEnvoyReplicationChecker,
    dry_run: bool = False,
) -> Optional[bool]:
    """Checks a service's replication levels based on how the service's replication
    should be monitored. (smartstack/envoy or mesos)

    :param instance_config: an instance of MarathonServiceConfig
    :param replication_checker: an instance of MesosSmartstackEnvoyReplicationChecker
    """
    expected_count = instance_config.get_instances()
    log.info(
        "Expecting %d total tasks for %s" % (expected_count, instance_config.job_id)
    )
    proxy_port = get_proxy_port_for_instance(instance_config)

    registrations = instance_config.get_registrations()
    # if the primary registration does not match the service_instance name then
    # the best we can do is check marathon for replication (for now).
    if proxy_port is not None and registrations[0] == instance_config.job_id:
        is_well_replicated = monitoring_tools.check_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_count,
            replication_checker=replication_checker,
            dry_run=dry_run,
        )
        return is_well_replicated
    else:
        check_healthy_marathon_tasks_for_service_instance(
            instance_config=instance_config,
            expected_count=expected_count,
            all_tasks=all_tasks_or_pods,
            dry_run=dry_run,
        )
        return None


if __name__ == "__main__":
    main(
        instance_type_class=marathon_tools.MarathonServiceConfig,
        check_service_replication=check_service_replication,
        namespace=None,  # not relevant for mesos
        mesos=True,
    )
