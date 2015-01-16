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
(available/expected), and then compared against two thresholds- one for emitting a
warning status to sensu, and one for emitting a critical status to sensu. The
default thresholds are .75/.50, meaning if fewer than 75% of a service's backends are
available, the script sends WARNING, and if fewer than 50% are available, it sends
CRITICAL.
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
    result_dict = {
        'tip': monitoring_tools.get_tip(framework, service_name, soa_dir=soa_dir),
        'notification_email': monitoring_tools.get_notification_email(framework, service_name, soa_dir=soa_dir),
        'page': monitoring_tools.get_page(framework, service_name, soa_dir=soa_dir),
        'irc_channels': monitoring_tools.get_irc_channels(framework, service_name, soa_dir=soa_dir),
        'alert_after': '2m',
        'check_every': '1m',
        'realert_every': -1,
        'source': 'mesos-%s' % marathon_tools.get_cluster()
    }
    pysensu_yelp.send_event(check_name, runbook, status, output, team, **result_dict)


def parse_args():
    epilog = "PERCENTAGE is an integer value representing the percentage of available to expected instances"
    parser = argparse.ArgumentParser(epilog=epilog)

    parser.add_argument('-s', '--synapse-host-port',
                        dest='synapse_host_port', type=str,
                        help='The host and port to check synapse on',
                        default='localhost:3212')
    parser.add_argument('-w', '--warn', dest='warn', type=int,
                        metavar='PERCENTAGE', default=75,
                        help="Generate warning state if fraction of instances \
                        available is less than this percentage")
    parser.add_argument('-c', '--critcal', dest='crit', type=int,
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


def check_namespaces(all_namespaces, available_backends, soa_dir, crit_threshold, warn_threshold):
    """Check a set of namespaces to see if their number of available backends is too low,
    emitting events to Sensu based on the fraction available and the thresholds given.

    :param all_namespaces: A list of all namespaces to check
    :param available_backends: A dictionary mapping namespaces to the number of available_backends
    :param soa_dir: The SOA configuration directory to read from
    :param crit_threshold: The fraction of instances that need to be up to avoid a CRITICAL event
    :param warn_threshold: The fraction of instances that need to be up to avoid a WARNING event"""
    for full_name in all_namespaces:
        log.info('Checking namespace %s', full_name)
        try:
            service_name, namespace = split_id(full_name)
            num_expected = marathon_tools.get_expected_instances(service_name, namespace, soa_dir)
        except (IndexError, KeyError, OSError, ValueError, AttributeError):
            log.info('Namespace %s isn\'t a marathon service', full_name)
            continue  # This isn't a Marathon service
        if num_expected == 0:
            log.info('Namespace %s doesn\'t have any expected instances', full_name)
            continue  # This namespace isn't in this cluster
        # We want to make sure that we're expecting some # of backends first!
        if full_name not in available_backends:
            output = 'Service namespace entry %s not found! No instances available!' % full_name
            log.error(output)
            context = get_context(service_name, namespace)
            output = '%s\n%s' % (output, context)
            send_event(service_name, namespace, soa_dir, pysensu_yelp.Status.CRITICAL, output)
            continue
        num_available = available_backends[full_name]
        ratio = (num_available / float(num_expected)) * 100
        output = ('Service %s has %d out of %d expected instances available!\n' +
                  'Thresholds are WARN <= %d%%, CRITICAL <= %d%%') % (full_name,
                                                                      num_available,
                                                                      num_expected,
                                                                      warn_threshold,
                                                                      crit_threshold)
        if ratio <= crit_threshold:
            log.error(output)
            status = pysensu_yelp.Status.CRITICAL
            status_str = 'CRITICAL'
        elif ratio <= warn_threshold:
            log.warning(output)
            status = pysensu_yelp.Status.WARNING
            status_str = 'WARNING'
        else:
            log.info(output)
            status = pysensu_yelp.Status.OK
            status_str = 'OK'
        output = '%s: %s' % (status_str, output)
        if status_str != 'OK':
            context = get_context(service_name, namespace)
            output = '%s\n%s' % (output, context)
        send_event(service_name, namespace, soa_dir, status, output)


def main():
    args = parse_args()
    soa_dir = args.soa_dir
    warn_threshold = args.warn
    crit_threshold = args.crit
    synapse_host = args.synapse_host_port
    logging.basicConfig()
    if args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)
    all_namespaces = [name for name, config in marathon_tools.get_all_namespaces()]
    all_available = replication_utils.get_replication_for_services(synapse_host, all_namespaces)
    check_namespaces(all_namespaces, all_available, soa_dir, crit_threshold, warn_threshold)


if __name__ == "__main__":
    if marathon_tools.is_mesos_leader():
        main()
