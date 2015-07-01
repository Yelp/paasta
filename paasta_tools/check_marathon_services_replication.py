#!/usr/bin/env python
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
import sys

from paasta_tools.monitoring import replication_utils
from paasta_tools.monitoring.context import get_context
from paasta_tools import marathon_tools
from paasta_tools import monitoring_tools
from paasta_tools.utils import _log


ID_SPACER = marathon_tools.ID_SPACER
log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler(sys.stdout))


def send_event(service_name, namespace, soa_dir, status, output):
    """Send an event to sensu via pysensu_yelp with the given information.

    :param service_name: The service name the event is about
    :param namespace: The namespace of the service the event is about
    :param soa_dir: The service directory to read monitoring information from
    :param status: The status to emit for this event
    :param output: The output to emit for this event"""
    # This function assumes the input is a string like "mumble.main"
    framework = 'marathon'
    check_name = 'check_marathon_services_replication.%s%s%s' % (service_name, ID_SPACER, namespace)
    team = monitoring_tools.get_team(framework, service_name, soa_dir=soa_dir)
    if not team:
        return
    runbook = monitoring_tools.get_runbook(framework, service_name, soa_dir=soa_dir)
    cluster = marathon_tools.get_cluster()
    result_dict = {
        'tip': monitoring_tools.get_tip(framework, service_name, soa_dir=soa_dir),
        'notification_email': monitoring_tools.get_notification_email(framework, service_name, soa_dir=soa_dir),
        'page': monitoring_tools.get_page(framework, service_name, soa_dir=soa_dir),
        'irc_channels': monitoring_tools.get_irc_channels(framework, service_name, soa_dir=soa_dir),
        'alert_after': '2m',
        'check_every': '1m',
        'realert_every': -1,
        'source': 'paasta-%s' % cluster
    }
    pysensu_yelp.send_event(check_name, runbook, status, output, team, **result_dict)
    _log(
        service_name=service_name,
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


def split_id(fid):
    """Split a service_name.namespace id into a tuple of
    (service_name, namespace).

    :param fid: The full id to split
    :returns: A tuple of (service_name, namespace)"""
    return (fid.split(ID_SPACER)[0], fid.split(ID_SPACER)[1])


def get_expected_count(service, instance, soa_dir):
    expected_count = marathon_tools.get_expected_instance_count_for_namespace(
        service,
        instance,
        soa_dir=soa_dir
    )
    if expected_count == 0:
        log.info('%s.%s doesn\'t have any expected instances' % (service, instance))
        return None
    return expected_count


def check_smartstack_replication_for_namespace(
        service,
        namespace,
        available_backends,
        soa_dir,
        crit_threshold,
        expected_count):
    """Check a set of namespaces to see if their number of available backends is too low,
    emitting events to Sensu based on the fraction available and the thresholds given.

    :param service: A string like example_service
    :param namespace: A nerve namespace, like "main"
    :param available_backends: A dictionary mapping namespaces to the number of available_backends
    :param soa_dir: The SOA configuration directory to read from
    :param crit_threshold: The fraction of instances that need to be up to avoid a CRITICAL event
    """
    full_name = "%s%s%s" % (service, ID_SPACER, namespace)
    log.info('Checking namespace %s', full_name)
    if full_name not in available_backends:
        output = 'Service namespace entry %s not found! No instances available!' % full_name
        log.error(output)
        context = get_context(service, namespace)
        output = '%s\n%s' % (output, context)
        send_event(service, namespace, soa_dir, pysensu_yelp.Status.CRITICAL, output)
        return
    num_available = available_backends[full_name]
    ratio = (num_available / float(expected_count)) * 100
    output = ('Service %s has %d out of %d expected instances available!\n' +
              '(threshold: %d%%)') % (full_name, num_available, expected_count, crit_threshold)
    if ratio < crit_threshold:
        log.error(output)
        status = pysensu_yelp.Status.CRITICAL
        status_str = 'CRITICAL'
    else:
        log.info(output)
        status = pysensu_yelp.Status.OK
        status_str = 'OK'
    output = '%s: %s' % (status_str, output)
    if status_str != 'OK':
        context = get_context(service, namespace)
        output = '%s\n%s' % (output, context)
    send_event(service, namespace, soa_dir, status, output)


def check_mesos_replication_for_service(service, instance, soa_dir, crit_threshold):
    pass


def check_service_replication(service, instance, crit_threshold, available_smartstack_replication, soa_dir):
    """Checks a service's replication levels based on how the service's replication
    should be monitored. (smartstack or mesos)

    :param service: Service name, like "example_service"
    :param instance: Instance name, like "main" or "canary"
    :param crit_threshold: an int from 0-100 representing the percentage threshold for triggering an alert
    :param soa_dir: The SOA configuration directory to read from
    :available_smartstack_replication: a special hash that represents the current replication levels in smartstack
    """
    expected_count = get_expected_count(service, instance, soa_dir)
    if expected_count is None:
        return
    proxy_port = marathon_tools.get_proxy_port_for_instance(service, instance, soa_dir=soa_dir)
    if proxy_port is not None:
        namespace = marathon_tools.read_namespace_for_service_instance(service, instance, soa_dir=soa_dir)
        if namespace == instance:
            check_smartstack_replication_for_namespace(
                service, namespace, available_smartstack_replication, soa_dir, crit_threshold, expected_count
            )
        else:
            log.debug(
                "Instance %s is announced under namespace: %s, not checking replication for it" % (instance, namespace)
            )
    else:
        check_mesos_replication_for_service(service, instance, soa_dir, crit_threshold)


def main():
    args = parse_args()
    soa_dir = args.soa_dir
    crit_threshold = args.crit
    logging.basicConfig()
    if args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)
    service_instances = marathon_tools.get_marathon_services_for_cluster(cluster=args.cluster, soa_dir=args.soa_dir)
    available_smartstack_replication = replication_utils.get_replication_for_services(
        'localhost:3212', service_instances)
    for service, instance in service_instances:
        check_service_replication(service, instance, crit_threshold, available_smartstack_replication, soa_dir)


if __name__ == "__main__":
    if marathon_tools.is_mesos_leader():
        main()
