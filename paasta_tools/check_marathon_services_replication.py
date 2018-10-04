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
import argparse
import logging
from datetime import datetime
from datetime import timedelta

import a_sync

from paasta_tools import marathon_tools
from paasta_tools import monitoring_tools
from paasta_tools.long_running_service_tools import get_proxy_port_for_instance
from paasta_tools.marathon_tools import format_job_id
from paasta_tools.mesos_tools import get_slaves
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.smartstack_tools import MesosSmartstackReplicationChecker
from paasta_tools.utils import datetime_from_utc_to_local
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


def filter_healthy_marathon_instances_for_short_app_id(all_tasks, app_id):
    tasks_for_app = [task for task in all_tasks if task.app_id.startswith('/%s' % app_id)]
    one_minute_ago = datetime.now() - timedelta(minutes=1)

    healthy_tasks = []
    for task in tasks_for_app:
        if marathon_tools.is_task_healthy(task, default_healthy=True) \
                and task.started_at is not None \
                and datetime_from_utc_to_local(task.started_at) < one_minute_ago:
            healthy_tasks.append(task)
    return len(healthy_tasks)


def check_healthy_marathon_tasks_for_service_instance(
    instance_config,
    expected_count,
    all_tasks,
):
    app_id = format_job_id(instance_config.service, instance_config.instance)
    num_healthy_tasks = filter_healthy_marathon_instances_for_short_app_id(
        all_tasks=all_tasks,
        app_id=app_id,
    )
    log.info("Checking %s in marathon as it is not in smartstack" % app_id)
    monitoring_tools.send_replication_event_if_under_replication(
        instance_config=instance_config,
        expected_count=expected_count,
        num_available=num_healthy_tasks,
    )


def check_service_replication(
    instance_config,
    all_tasks,
    smartstack_replication_checker,
):
    """Checks a service's replication levels based on how the service's replication
    should be monitored. (smartstack or mesos)

    :param instance_config: an instance of MarathonServiceConfig
    :param smartstack_replication_checker: an instance of MesosSmartstackReplicationChecker
    """
    expected_count = instance_config.get_instances()
    log.info("Expecting %d total tasks for %s" % (expected_count, instance_config.job_id))
    proxy_port = get_proxy_port_for_instance(instance_config)

    registrations = instance_config.get_registrations()
    # if the primary registration does not match the service_instance name then
    # the best we can do is check marathon for replication (for now).
    if proxy_port is not None and registrations[0] == instance_config.job_id:
        monitoring_tools.check_smartstack_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_count,
            smartstack_replication_checker=smartstack_replication_checker,
        )
    else:
        check_healthy_marathon_tasks_for_service_instance(
            instance_config=instance_config,
            expected_count=expected_count,
            all_tasks=all_tasks,
        )


def main():
    args = parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    system_paasta_config = load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()

    clients = marathon_tools.get_marathon_clients(marathon_tools.get_marathon_servers(system_paasta_config))
    all_clients = clients.get_all_clients()
    all_tasks = []
    for client in all_clients:
        all_tasks.extend(client.list_tasks())
    mesos_slaves = a_sync.block(get_slaves)
    smartstack_replication_checker = MesosSmartstackReplicationChecker(mesos_slaves, system_paasta_config)

    for service in list_services(soa_dir=args.soa_dir):
        service_config = PaastaServiceConfigLoader(service=service, soa_dir=args.soa_dir)
        for instance_config in service_config.instance_configs(
            cluster=cluster,
            instance_type_class=marathon_tools.MarathonServiceConfig,
        ):
            if instance_config.get_docker_image():
                check_service_replication(
                    instance_config=instance_config,
                    all_tasks=all_tasks,
                    smartstack_replication_checker=smartstack_replication_checker,
                )
            else:
                log.debug(
                    '%s is not deployed. Skipping replication monitoring.' %
                    instance_config.job_id,
                )


if __name__ == "__main__":
    main()
