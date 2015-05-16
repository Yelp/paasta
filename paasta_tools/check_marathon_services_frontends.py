#!/usr/bin/env python
"""
Usage: ./check_marathon_services_frontends.py [options]

Get the http frontends of deployed services and see if they're up.
Loads services from marathon_tools.get_marathon_services_for_cluster
and then uses nagios' check_http plugin to query HAProxy on the given
proxy_port defined in a service instance's smartstack.yaml namespace.

Can only be run on the current mesos-master leader- if this host
isn't the leader, the script exits immediately.

Sends a sensu event for each service that it checks.
"""

import argparse
import logging
import sys

import service_configuration_lib
from paasta_tools import marathon_tools
from paasta_tools import monitoring_tools
from paasta_tools.utils import _run
import pysensu_yelp


log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='Cleans up stale marathon jobs.')
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest="verbose", default=False)
    args = parser.parse_args()
    return args


def send_event(service_name, instance_name, check_name, soa_dir, status, output):
    """Send an event to sensu via pysensu_yelp with the given information.

    :param service_name: The service name the event is about
    :param instance_name: The instance of the service the event is about
    :param check_name: The actual name of the check
    :param status: The status to emit for this event
    :param output: The output to emit for this event"""
    # This function assumes the input is a string like "mumble.main"
    framework = 'marathon'
    team = monitoring_tools.get_team(framework, service_name, instance_name, soa_dir)
    if not team:
        return
    runbook = monitoring_tools.get_runbook(framework, service_name, instance_name, soa_dir)
    result_dict = {
        'tip': monitoring_tools.get_tip(framework, service_name, instance_name, soa_dir),
        'notification_email': monitoring_tools.get_notification_email(framework, service_name,
                                                                      instance_name, soa_dir),
        'page': monitoring_tools.get_page(framework, service_name, instance_name, soa_dir),
        'irc_channels': monitoring_tools.get_irc_channels(framework, service_name,
                                                          instance_name, soa_dir),
        'alert_after': '2m',
        'check_every': '1m',
        'realert_every': -1,
        'source': 'mesos-%s' % marathon_tools.load_marathon_config().get_cluster()
    }
    pysensu_yelp.send_event(check_name, runbook, status, output, team, **result_dict)


def build_check_command(port, mode):
    return '/usr/lib/nagios/plugins/check_%s -H localhost -p %d' % (mode, port)


def check_service(port, mode):
    """Check the proxy_port exposed by HAProxy via the check_${mode} plugin provided
    by nagios.

    :param mode: The mode to check the service with; one of 'tcp' or 'http'
    :param port: The proxy_port defined in a service's smartstack namespace
    :returns: A tuple of (status, output) to be used with send_event"""
    command = build_check_command(port, mode)
    # Nagios checks use the default (10 seconds) timeout
    status, output = _run(command, timeout=11)
    return (status, output)


def check_service_instance(service_name, instance_name, soa_dir):
    """Check a service instance and emit a Sensu event about it.

    Finds out whether the service is TCP or HTTP and then runs the
    corresponding check.

    :param service_name: The service name to check
    :param instance_name: The instance of the service to check
    :returns: The output field returned by check_http or check_tcp"""
    # TODO: Skip checking a service if it is under the namespace of another instance
    port = marathon_tools.get_proxy_port_for_instance(service_name, instance_name, soa_dir=soa_dir)
    mode = marathon_tools.get_mode_for_instance(service_name, instance_name, soa_dir=soa_dir)
    check_name = "check_marathon_services_frontends.%s.%s" % (service_name, instance_name)
    if mode == 'tcp' or mode == 'http':
        status, output = check_service(port, mode)
    else:
        status = pysensu_yelp.Status.CRITICAL
        output = 'Mode not recognized for instance %s.%s: %s' % (service_name, instance_name, mode)
        log.error(output)
    send_event(service_name, instance_name, check_name, soa_dir, status, output)
    return output


def main():
    """Check every service instance for this cluster and emit a sensu event about it."""
    args = parse_args()
    # Do this check after parsing args so we can emit a useful --help message
    # even if we're not the mesos leader.
    is_leader = False
    try:
        is_leader = marathon_tools.is_mesos_leader()
    except marathon_tools.MesosMasterConnectionException as exc:
        log.debug(repr(exc))
    if not is_leader:
        log.warning("You must run this command from a mesos master.")
        log.warning("http://y/zookeeper-discovery explains how to check if you are.")
        log.warning("")
        log.warning("If you're sure you're on a master, maybe the master is dead! :(")
        log.warning("http://y/rb-mesos-master explains how to fix it.")
        sys.exit(1)

    soa_dir = args.soa_dir
    if args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)
    all_checked_services = []
    for service_name, instance_name in marathon_tools.get_marathon_services_for_cluster():
        all_checked_services.append("%s.%s" % (service_name, instance_name))
        log.info("Checking %s.%s" % (service_name, instance_name))
        output = check_service_instance(service_name, instance_name, soa_dir)
        log.info("Got output: %s" % output)

    # If we got here, it is ok to return OK
    # Otherwise an exception would blow us up earlier.
    log.info("Finished checking all services: %s" % ' '.join(all_checked_services))


if __name__ == "__main__":
    main()
