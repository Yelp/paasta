#!/usr/bin/env python

import logging
import subprocess
import sys

from sensu_plugin import SensuPluginCheck

from service_deployment_tools import marathon_tools
from service_deployment_tools import monitoring_tools
import pysensu_yelp


def check_http(port):
    """Actually exec the check_http command and return the output"""
    command = build_check_http_command(port)
    child = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output, _ = child.communicate()
    status = child.returncode
    return (output, status)


def send_event(service_name, instance_name, check_name, status, output):
    """Take the raw results and emit a valid hash to the sensu library"""
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
    """Actually check a service instance and emit Sensu events if needed"""
    # TODO: Skip checking a service if it is under the namespace of another instance
    port = marathon_tools.get_proxy_port_for_instance(service_name, instance_name)
    output, status = check_http(port)
    check_name = "soa_%s.%s_http_frontends" % (service_name, instance_name)
    send_event(service_name, instance_name, check_name, status, output)
    return output


class MarathonServicesHttpFrontends(SensuPluginCheck):

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
    check = MarathonServicesHttpFrontends()
