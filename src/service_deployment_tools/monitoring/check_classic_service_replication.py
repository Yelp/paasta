#!/usr/bin/env python
import sys
import logging

from service_configuration_lib import read_services_configuration
from service_deployment_tools.monitoring.replication_utils import (
    get_replication_for_services
)
from service_deployment_tools.monitoring.check_synapse_replication import (
    check_replication,
)
from service_deployment_tools.monitoring.config_providers import (
    extract_monitoring_info
)

from sensu_plugin import SensuPluginCheck
import pysensu_yelp


SYNAPSE_HOST_PORT = "localhost:3212"


def read_key(key):
    with open("/nail/etc/{0}".format(key)) as fd:
        return fd.read().strip()


def report_event(event_dict):
    assert event_dict['team'] is not None
    pysensu_yelp.send_event(**event_dict)


def do_replication_check(service_name, monitoring_config, service_replication):
    """Do a replication check on the provided service and generate
    notification events based on the information in monitoring_config and
    service_replication. Note that the caller must provide replication data

    :param service_name: The name of the service to send an event for
    :param monitoring_config: A dictionary conforming to the mandatory
        monitoring keys (as defined by extract_replication_info) and
        optionally providing additional keys:

        runbook ("no runbook"): The runbook to refer oncall members to
        tip ("no tip"): A tip for oncall members
        page (false): Whether to page the provided team on failure
        alert_after ("0s"): How many minutes before going critical
        realert_every (-1): How many events before you trigger a realert
                            -1 indicates an exponential backoff

        extra.replication.key ("habitat"): The file in /nail/etc to inspect
            to figure out which value to lookup in map
        extra.replication.default (1): The default number of instances to
            check for
        extra.replication.map ({}): A lookup that maps the replication keys to
            the appropriate minimum replication value
    :param service_replication: An int that represents the present replication

    The default behaviour is to send emails to a team if their service
    reaches 0 replicas, although teams can fine tune this to their needs

    :returns A dictionary that conforms to the expected sensu event API
             Note that this function does NOT send it to sensu
    """
    replication_config = monitoring_config.get('extra', {}).get(
        'replication', {})
    replication_key = replication_config.get('key', 'habitat')
    replication_default = replication_config.get('default', 1)
    replication_map = replication_config.get('map', {})

    try:
        goal_replication = replication_map[read_key(replication_key)]
    except (IOError, KeyError):
        # Either the /nail/etc/{key} file didn't exist or the result didn't
        # appear in the replication_map, either way use the default
        goal_replication = replication_default

    warn_range = (goal_replication, sys.maxint)
    crit_range = warn_range

    status_code, message = check_replication(service_name,
                                             service_replication,
                                             warn_range, crit_range)
    return {
        'name': "replication_{0}".format(service_name),
        'status': status_code,
        'output': message,
        'team': monitoring_config['team'],
        'notification_email': monitoring_config['notification_email'],
        'runbook': monitoring_config['runbook'] or 'no runbook',
        'tip': monitoring_config['tip'] or 'no tip',
        'page': monitoring_config['page'] or False,
        'alert_after': monitoring_config['alert_after'] or '0s',
        'realert_every': monitoring_config['realert_every'] or -1
    }


def extract_replication_info(service_config):
    """Extract monitoring information from yelpsoa-configs

    To be monitored a service *must* supply a team.

    Mandatory keys:
        team: The team to send pages to
        notification_email: The email to send emails to
        service_type: Must be "classic" for this check to run

    :param service_config: The configuration dictionary for the service

    :returns (do_monitoring, monitoring_config): Which is a tuple of a bool
        and a monitoring dictionary that has keys specified by
        config_providers.monitoring_keys
    """
    monitoring_config = extract_monitoring_info('classic', service_config)

    # If we do not meet required information, do nothing
    if not (monitoring_config['team'] and
            monitoring_config.get('service_type') == 'classic'):
        return False, {}
    return True, monitoring_config


class ClassicServiceReplicationCheck(SensuPluginCheck):
    log = logging.getLogger(__name__)
    log.addHandler(logging.StreamHandler(sys.stdout))

    def setup_logging(self):
        if self.options.debug:
            self.log.setLevel(logging.DEBUG)
        else:
            self.log.setLevel(logging.WARN)

    def setup(self):
        self.parser.add_argument('-d', '--debug', default=False,
                                 action='store_true',
                                 help='Turn on debug output')

    def run(self):
        self.setup_logging()
        all_service_config = read_services_configuration()

        # Get the replication data once for performance
        service_replication = get_replication_for_services(
            SYNAPSE_HOST_PORT, ['%s.main' % name for name in all_service_config.keys()]
        )

        checked_services = []
        for service_name, service_config in all_service_config.iteritems():
            do_monitoring, monitoring_config = extract_replication_info(
                service_config
            )

            if do_monitoring:
                self.log.debug("Checking {0}".format(service_name))
                replication = service_replication.get('%s.main' % service_name, 0)
                event = do_replication_check(service_name, monitoring_config,
                                             replication)
                checked_services.append(service_name)
                self.log.debug("Result for {0}: {1}".format(service_name,
                                                            event['output']))
                report_event(event)
            else:
                self.log.debug("Not checking {0}".format(service_name))

        self.ok("Finished checking services: {0}".format(checked_services))


if __name__ == "__main__":
    # The act of making the object calls ends up calling the run method via
    # SensuPluginCheck
    check = ClassicServiceReplicationCheck()
