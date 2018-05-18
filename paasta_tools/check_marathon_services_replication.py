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
import os
from datetime import datetime
from datetime import timedelta

import a_sync
import pysensu_yelp

from paasta_tools import marathon_tools
from paasta_tools import monitoring_tools
from paasta_tools.marathon_tools import format_job_id
from paasta_tools.mesos_tools import get_slaves
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.smartstack_tools import SmartstackReplicationChecker
from paasta_tools.utils import _log
from paasta_tools.utils import datetime_from_utc_to_local
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import is_under_replicated
from paasta_tools.utils import load_system_paasta_config


log = logging.getLogger(__name__)


def send_event(instance_config, status, output):
    """Send an event to sensu via pysensu_yelp with the given information.

    :param instance_config: an instance of MarathonServiceConfig
    :param status: The status to emit for this event
    :param output: The output to emit for this event"""
    # This function assumes the input is a string like "mumble.main"
    monitoring_overrides = instance_config.get_monitoring()
    if 'alert_after' not in monitoring_overrides:
        monitoring_overrides['alert_after'] = '2m'
    monitoring_overrides['check_every'] = '1m'
    monitoring_overrides['runbook'] = monitoring_tools.get_runbook(
        monitoring_overrides,
        instance_config.service, soa_dir=instance_config.soa_dir,
    )

    check_name = (
        'check_marathon_services_replication.%s' %
        instance_config.job_id
    )
    monitoring_tools.send_event(
        service=instance_config.service,
        check_name=check_name,
        overrides=monitoring_overrides,
        status=status,
        output=output,
        soa_dir=instance_config.soa_dir,
        cluster=instance_config.cluster,
    )
    _log(
        service=instance_config.service,
        line='Replication: %s' % output,
        component='monitoring',
        level='debug',
        cluster=instance_config.cluster,
        instance=instance_config.instance,
    )


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


def check_smartstack_replication_for_instance(
    instance_config,
    expected_count,
    smartstack_replication_checker,
):
    """Check a set of namespaces to see if their number of available backends is too low,
    emitting events to Sensu based on the fraction available and the thresholds defined in
    the corresponding yelpsoa config.

    :param instance_config: an instance of MarathonServiceConfig
    :param smartstack_replication_checker: an instance of SmartstackReplicationChecker
    """

    crit_threshold = instance_config.get_replication_crit_percentage()

    log.info('Checking instance %s in smartstack', instance_config.job_id)
    smartstack_replication_info = \
        smartstack_replication_checker.get_replication_for_instance(instance_config)

    log.debug('Got smartstack replication info for %s: %s' %
              (instance_config.job_id, smartstack_replication_info))

    if len(smartstack_replication_info) == 0:
        status = pysensu_yelp.Status.CRITICAL
        output = (
            'Service %s has no Smartstack replication info. Make sure the discover key in your smartstack.yaml '
            'is valid!\n'
        ) % instance_config.job_id
        log.error(output)
    else:
        expected_count_per_location = int(expected_count / len(smartstack_replication_info))
        output = ''
        output_critical = ''
        output_ok = ''
        under_replication_per_location = []

        for location, available_backends in sorted(smartstack_replication_info.items()):
            num_available_in_location = available_backends.get(instance_config.job_id, 0)
            under_replicated, ratio = is_under_replicated(
                num_available_in_location, expected_count_per_location, crit_threshold,
            )
            if under_replicated:
                output_critical += '- Service %s has %d out of %d expected instances in %s (CRITICAL: %d%%)\n' % (
                    instance_config.job_id, num_available_in_location, expected_count_per_location, location, ratio,
                )
            else:
                output_ok += '- Service %s has %d out of %d expected instances in %s (OK: %d%%)\n' % (
                    instance_config.job_id, num_available_in_location, expected_count_per_location, location, ratio,
                )
            under_replication_per_location.append(under_replicated)

        output += output_critical
        if output_critical and output_ok:
            output += '\n\n'
            output += 'The following locations are OK:\n'
        output += output_ok

        if any(under_replication_per_location):
            status = pysensu_yelp.Status.CRITICAL
            output += (
                "\n\n"
                "What this alert means:\n"
                "\n"
                "  This replication alert means that a SmartStack powered loadbalancer (haproxy)\n"
                "  doesn't have enough healthy backends. Not having enough healthy backends\n"
                "  means that clients of that service will get 503s (http) or connection refused\n"
                "  (tcp) when trying to connect to it.\n"
                "\n"
                "Reasons this might be happening:\n"
                "\n"
                "  The service may simply not have enough copies or it could simply be\n"
                "  unhealthy in that location. There also may not be enough resources\n"
                "  in the cluster to support the requested instance count.\n"
                "\n"
                "Things you can do:\n"
                "\n"
                "  * You can view the logs for the job with:\n"
                "      paasta logs -s %(service)s -i %(instance)s -c %(cluster)s\n"
                "\n"
                "  * Fix the cause of the unhealthy service. Try running:\n"
                "\n"
                "      paasta status -s %(service)s -i %(instance)s -c %(cluster)s -vv\n"
                "\n"
                "  * Widen SmartStack discovery settings\n"
                "  * Increase the instance count\n"
                "\n"
            ) % {
                'service': instance_config.service,
                'instance': instance_config.instance,
                'cluster': instance_config.cluster,
            }
            log.error(output)
        else:
            status = pysensu_yelp.Status.OK
            log.info(output)
    send_event(instance_config=instance_config, status=status, output=output)


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
    send_event_if_under_replication(
        instance_config=instance_config,
        expected_count=expected_count,
        num_available=num_healthy_tasks,
    )


def send_event_if_under_replication(
    instance_config,
    expected_count,
    num_available,
):
    crit_threshold = instance_config.get_replication_crit_percentage()
    output = (
        'Service %s has %d out of %d expected instances available!\n' +
        '(threshold: %d%%)'
    ) % (instance_config.job_id, num_available, expected_count, crit_threshold)
    under_replicated, _ = is_under_replicated(num_available, expected_count, crit_threshold)
    if under_replicated:
        output += (
            "\n\n"
            "What this alert means:\n"
            "\n"
            "  This replication alert means that the service PaaSTA can't keep the\n"
            "  requested number of copies up and healthy in the cluster.\n"
            "\n"
            "Reasons this might be happening:\n"
            "\n"
            "  The service may simply unhealthy. There also may not be enough resources\n"
            "  in the cluster to support the requested instance count.\n"
            "\n"
            "Things you can do:\n"
            "\n"
            "  * Increase the instance count\n"
            "  * Fix the cause of the unhealthy service. Try running:\n"
            "\n"
            "      paasta status -s %(service)s -i %(instance)s -c %(cluster)s -vv\n"
        ) % {
            'service': instance_config.service,
            'instance': instance_config.instance,
            'cluster': instance_config.cluster,
        }
        log.error(output)
        status = pysensu_yelp.Status.CRITICAL
    else:
        log.info(output)
        status = pysensu_yelp.Status.OK
    send_event(
        instance_config=instance_config,
        status=status,
        output=output,
    )


def check_service_replication(
    instance_config,
    all_tasks,
    smartstack_replication_checker,
):
    """Checks a service's replication levels based on how the service's replication
    should be monitored. (smartstack or mesos)

    :param instance_config: an instance of MarathonServiceConfig
    :param smartstack_replication_checker: an instance of SmartstackReplicationChecker
    """
    expected_count = instance_config.get_instances()
    log.info("Expecting %d total tasks for %s" % (expected_count, instance_config.job_id))
    proxy_port = marathon_tools.get_proxy_port_for_instance(
        name=instance_config.service,
        instance=instance_config.instance,
        cluster=instance_config.cluster,
        soa_dir=instance_config.soa_dir,
    )

    registrations = instance_config.get_registrations()
    # if the primary registration does not match the service_instance name then
    # the best we can do is check marathon for replication (for now).
    if proxy_port is not None and registrations[0] == instance_config.job_id:
        check_smartstack_replication_for_instance(
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


def list_services(soa_dir):
    rootdir = os.path.abspath(soa_dir)
    return os.listdir(rootdir)


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
    smartstack_replication_checker = SmartstackReplicationChecker(mesos_slaves, system_paasta_config)

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
