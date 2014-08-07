#!/usr/bin/env python
"""
Usage: ./check_marathon_services_http_frontends.py [-d]

Get the http frontends of deployed services and see if they're up.
Loads services from marathon_tools.get_marathon_services_for_cluster
and then uses nagios' check_http plugin to query HAProxy on the given
proxy_port defined in a service instance's smartstack.yaml namespace.

Can only be run on the current mesos-master leader- if this host
isn't the leader, the script exits immediately.

Sends a sensu event for each service that it checks.

Run with -d or --debug to get verbose output.
"""

import logging
import subprocess
import sys

from sensu_plugin import SensuPluginCheck

from service_deployment_tools import marathon_tools
from service_deployment_tools import monitoring_tools
import pysensu_yelp


def check_http(port):
    """Check the proxy_port exposed by HAProxy via the check_http plugin provided
    by nagios.

    :param port: The proxy_port defined in a service's smartstack namespace
    :returns: A tuple of (status, output) to be used with send_event"""
    command = build_check_http_command(port)
    child = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output, _ = child.communicate()
    status = child.returncode
    return (status, output)


def send_event(service_name, instance_name, check_name, status, output):
    """Send an event to sensu via pysensu_yelp. With the given information.

    :param service_name: The service name the event is about
    :param instance_name: The instance of the service the event is about
    :param check_name: The actual name of the check
    :param status: The status to emit for this event
    :param output: The output to emit for this event"""
    # This function assumes the input is a string like "mumble.main"
    framework = 'marathon'
    team = monitoring_tools.get_team(framework, service_name, instance_name)
    runbook = monitoring_tools.get_runbook(framework, service_name, instance_name)
    result_dict = {
        'tip': monitoring_tools.get_tip(framework, service_name, instance_name),
        'notification_email': monitoring_tools.get_notification_email(framework, service_name, instance_name),
        'page': monitoring_tools.get_page(framework, service_name, instance_name),
        'alert_after': monitoring_tools.get_alert_after(framework, service_name, instance_name),
        'realert_every': -1
    }
    if team:
        pysensu_yelp.send_event(check_name, runbook, status, output, team, **result_dict)


def build_check_http_command(port):
    return '/usr/lib/nagios/plugins/check_http -H localhost -p %d' % port


def check_service_instance(service_name, instance_name):
    """Check a service instance and emit a Sensu event about it.

    :param service_name: The service name to check
    :param instance_name: The instance of the service to check
    :returns: The output field returned by check_http"""
    # TODO: Skip checking a service if it is under the namespace of another instance
    port = marathon_tools.get_proxy_port_for_instance(service_name, instance_name)
    status, output = check_http(port)
    check_name = "soa_%s.%s_http_frontends" % (service_name, instance_name)
    send_event(service_name, instance_name, check_name, status, output)
    return output


class MarathonServicesHttpFrontends(SensuPluginCheck):
    """
    A SensuPluginCheck for checking http frontends of services.
    Individual sensu events are emitted for each service instance,
    but having this class as a check means we also emit a general
    sensu event about this script's status.
    """

    log = logging.getLogger(__name__)
    log.addHandler(logging.StreamHandler(sys.stdout))

    def setup_logging(self):
        if self.options.debug:
            self.log.setLevel(logging.DEBUG)
        else:
            self.log.setLevel(logging.WARN)

    def setup(self):
        self.parser.add_argument(
            '-d',
            '--debug',
            default=False,
            action='store_true',
            help='Turn on debug output')

    def run(self):
        """Check every service instance for this cluster via http and emit a sensu
        event about it."""
        self.setup_logging()
        all_checked_services = []
        for service_name, instance_name in marathon_tools.get_marathon_services_for_cluster():
            all_checked_services.append("%s.%s" % (service_name, instance_name))
            self.log.debug("Checking %s.%s" % (service_name, instance_name))
            output = check_service_instance(service_name, instance_name)
            self.log.debug("Got output: %s" % output)

        # If we got here, it is ok to return OK
        # Otherwise an exception would blow us up earlier.
        self.ok("Finished checking all services: %s" % ' '.join(all_checked_services))


if __name__ == "__main__":
    # The act of making the object calls ends up calling the run method via
    # SensuPluginCheck
    if marathon_tools.is_mesos_leader():
        check = MarathonServicesHttpFrontends()
