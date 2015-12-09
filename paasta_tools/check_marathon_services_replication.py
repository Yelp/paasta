#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
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
(available/expected), and then compared against a threshold. The default threshold
is .50, meaning if fewer than 50 of a service's backends are available, the script
sends CRITICAL.
"""

import argparse
import logging
import pysensu_yelp
import service_configuration_lib

from paasta_tools import marathon_tools
from paasta_tools import mesos_tools
from paasta_tools import monitoring_tools
from paasta_tools import smartstack_tools
from paasta_tools.marathon_tools import format_job_id
from paasta_tools.monitoring import replication_utils
from paasta_tools.utils import _log
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import is_under_replicated
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoDeploymentsAvailable


log = logging.getLogger('__main__')
logging.basicConfig()


def send_event(service, namespace, cluster, soa_dir, status, output):
    """Send an event to sensu via pysensu_yelp with the given information.

    :param service: The service name the event is about
    :param namespace: The namespace of the service the event is about
    :param soa_dir: The service directory to read monitoring information from
    :param status: The status to emit for this event
    :param output: The output to emit for this event"""
    # This function assumes the input is a string like "mumble.main"
    monitoring_overrides = marathon_tools.load_marathon_service_config(
        service, namespace, cluster).get_monitoring()
    if 'alert_after' not in monitoring_overrides:
        monitoring_overrides['alert_after'] = '2m'
    monitoring_overrides['check_every'] = '1m'
    monitoring_overrides['runbook'] = monitoring_tools.get_runbook(monitoring_overrides, service, soa_dir=soa_dir)

    check_name = 'check_marathon_services_replication.%s' % compose_job_id(service, namespace)
    monitoring_tools.send_event(service, check_name, monitoring_overrides, status, output, soa_dir)
    _log(
        service=service,
        line='Replication: %s' % output,
        component='monitoring',
        level='debug',
        cluster=cluster,
        instance=namespace
    )


def parse_args():
    epilog = "PERCENTAGE is an integer value representing the percentage of available to expected instances"
    parser = argparse.ArgumentParser(epilog=epilog)

    parser.add_argument('-c', '--critical', dest='crit', type=int,
                        metavar='PERCENTAGE', default=50,
                        help="Generate critical state if fraction of instances \
                        available is less than this percentage")
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest="verbose", default=False)
    options = parser.parse_args()

    return options


def check_smartstack_replication_for_instance(
    service,
    instance,
    cluster,
    soa_dir,
    crit_threshold,
    expected_count,
):
    """Check a set of namespaces to see if their number of available backends is too low,
    emitting events to Sensu based on the fraction available and the thresholds given.

    :param service: A string like example_service
    :param namespace: A nerve namespace, like "main"
    :param cluster: name of the cluster
    :param soa_dir: The SOA configuration directory to read from
    :param crit_threshold: The fraction of instances that need to be up to avoid a CRITICAL event
    """
    namespace = marathon_tools.read_namespace_for_service_instance(service, instance, soa_dir=soa_dir)
    if namespace != instance:
        log.debug("Instance %s is announced under namespace: %s. "
                  "Not checking replication for it" % (instance, namespace))
        return
    full_name = compose_job_id(service, instance)
    job_config = marathon_tools.load_marathon_service_config(service, instance, cluster)
    monitoring_blacklist = job_config.get_monitoring_blacklist()
    log.info('Checking instance %s in smartstack', full_name)
    smartstack_replication_info = load_smartstack_info_for_service(
        service=service, namespace=namespace, soa_dir=soa_dir, blacklist=monitoring_blacklist)
    log.debug('Got smartstack replication info for %s: %s' % (full_name, smartstack_replication_info))

    if len(smartstack_replication_info) == 0:
        status = pysensu_yelp.Status.CRITICAL
        output = ('Service %s has no Smartstack replication info. Make sure the discover key in your smartstack.yaml '
                  'is valid!\n') % full_name
        log.error(output)
    else:
        expected_count_per_location = int(expected_count / len(smartstack_replication_info))
        output = ''
        under_replication_per_location = []

        for location, available_backends in sorted(smartstack_replication_info.iteritems()):
            num_available_in_location = available_backends.get(full_name, 0)
            under_replicated, ratio = is_under_replicated(
                num_available_in_location, expected_count_per_location, crit_threshold)
            if under_replicated:
                output += '- Service %s has %d out of %d expected instances in %s (CRITICAL: %d%%)\n' % (
                    full_name, num_available_in_location, expected_count_per_location, location, ratio)
            else:
                output += '- Service %s has %d out of %d expected instances in %s (OK: %d%%)\n' % (
                    full_name, num_available_in_location, expected_count_per_location, location, ratio)
            under_replication_per_location.append(under_replicated)

        if any(under_replication_per_location):
            status = pysensu_yelp.Status.CRITICAL
            log.error(output)
        else:
            status = pysensu_yelp.Status.OK
            log.info(output)
    send_event(service=service, namespace=instance, cluster=cluster, soa_dir=soa_dir, status=status, output=output)


def get_healthy_marathon_instances_for_short_app_id(client, app_id):
    tasks = client.list_tasks()
    tasks_for_app = [task for task in tasks if task.app_id.startswith('/%s' % app_id)]

    healthy_tasks = []
    for task in tasks_for_app:
        if all([health_check_result.alive for health_check_result in task.health_check_results]):
            healthy_tasks.append(task)
    return len(healthy_tasks)


def check_healthy_marathon_tasks_for_service_instance(client, service, instance, cluster,
                                                      soa_dir, crit_threshold, expected_count):
    app_id = format_job_id(service, instance)
    log.info("Checking %s in marathon as it is not in smartstack" % app_id)
    num_healthy_tasks = get_healthy_marathon_instances_for_short_app_id(client, app_id)
    send_event_if_under_replication(
        service=service,
        instance=instance,
        cluster=cluster,
        crit_threshold=crit_threshold,
        expected_count=expected_count,
        num_available=num_healthy_tasks,
        soa_dir=soa_dir,
    )


def send_event_if_under_replication(
    service,
    instance,
    cluster,
    crit_threshold,
    expected_count,
    num_available,
    soa_dir,
):
    full_name = compose_job_id(service, instance)
    output = ('Service %s has %d out of %d expected instances available!\n' +
              '(threshold: %d%%)') % (full_name, num_available, expected_count, crit_threshold)
    under_replicated, _ = is_under_replicated(num_available, expected_count, crit_threshold)
    if under_replicated:
        log.error(output)
        status = pysensu_yelp.Status.CRITICAL
    else:
        log.info(output)
        status = pysensu_yelp.Status.OK
    send_event(
        service=service,
        namespace=instance,
        cluster=cluster,
        soa_dir=soa_dir,
        status=status,
        output=output)


def check_service_replication(client, service, instance, cluster, crit_threshold, soa_dir):
    """Checks a service's replication levels based on how the service's replication
    should be monitored. (smartstack or mesos)

    :param service: Service name, like "example_service"
    :param instance: Instance name, like "main" or "canary"
    :param cluster: name of the cluster
    :param crit_threshold: an int from 0-100 representing the percentage threshold for triggering an alert
    :param soa_dir: The SOA configuration directory to read from
    """
    job_id = compose_job_id(service, instance)
    try:
        expected_count = marathon_tools.get_expected_instance_count_for_namespace(service, instance, soa_dir=soa_dir)
    except NoDeploymentsAvailable:
        log.info('deployments.json missing for %s. Skipping replication monitoring.' % job_id)
        return
    if expected_count is None:
        return
    log.info("Expecting %d total tasks for %s" % (expected_count, job_id))
    proxy_port = marathon_tools.get_proxy_port_for_instance(service, instance, soa_dir=soa_dir)
    if proxy_port is not None:
        check_smartstack_replication_for_instance(
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            crit_threshold=crit_threshold,
            expected_count=expected_count)
    else:
        check_healthy_marathon_tasks_for_service_instance(
            client=client,
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            crit_threshold=crit_threshold,
            expected_count=expected_count,
        )


def load_smartstack_info_for_service(service, namespace, soa_dir, blacklist):
    """Retrives number of available backends for given services

    :param service_instances: A list of tuples of (service, instance)
    :param namespaces: list of Smartstack namespaces
    :param blacklist: A list of blacklisted location tuples in the form (location, value)
    :returns: a dictionary of the form

    ::

        {
          'location_type': {
              'unique_location_name': {
                  'service.instance': <# ofavailable backends>
              },
              'other_unique_location_name': ...
          }
        }

    """
    service_namespace_config = marathon_tools.load_service_namespace_config(service, namespace,
                                                                            soa_dir=soa_dir)
    discover_location_type = service_namespace_config.get_discover()
    return get_smartstack_replication_for_attribute(
        attribute=discover_location_type,
        service=service,
        namespace=namespace,
        blacklist=blacklist)


def get_smartstack_replication_for_attribute(attribute, service, namespace, blacklist):
    """Loads smartstack replication from a host with the specified attribute

    :param attribute: a Mesos attribute
    :param service: A service name, like 'example_service'
    :param namespace: A particular smartstack namespace to inspect, like 'main'
    :param constraints: A list of Marathon constraints to restrict which synapse hosts to query
    :param blacklist: A list of blacklisted location tuples in the form of (location, value)
    :returns: a dictionary of the form {'<unique_attribute_value>': <smartstack replication hash>}
              (the dictionary will contain keys for unique all attribute values)
    """
    replication_info = {}
    unique_values = mesos_tools.get_mesos_slaves_grouped_by_attribute(attribute=attribute, blacklist=blacklist)
    full_name = compose_job_id(service, namespace)

    for value, hosts in unique_values.iteritems():
        # arbitrarily choose the first host with a given attribute to query for replication stats
        synapse_host = hosts[0]
        repl_info = replication_utils.get_replication_for_services(
            synapse_host=synapse_host,
            synapse_port=smartstack_tools.DEFAULT_SYNAPSE_PORT,
            services=[full_name],
        )
        replication_info[value] = repl_info

    return replication_info


def main():
    args = parse_args()
    soa_dir = args.soa_dir
    crit_threshold = args.crit
    logging.basicConfig()
    if args.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.WARNING)
    cluster = load_system_paasta_config().get_cluster()
    service_instances = get_services_for_cluster(
        cluster=cluster, instance_type='marathon', soa_dir=args.soa_dir)

    config = marathon_tools.load_marathon_config()
    client = marathon_tools.get_marathon_client(config.get_url(), config.get_username(), config.get_password())
    for service, instance in service_instances:
        check_service_replication(
            client=client,
            service=service,
            instance=instance,
            cluster=cluster,
            crit_threshold=crit_threshold,
            soa_dir=soa_dir,
        )


if __name__ == "__main__":
    if mesos_tools.is_mesos_leader():
        main()
