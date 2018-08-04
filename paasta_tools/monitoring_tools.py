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
Getters for deriving monitoring parameters for mesos-deployed stuff.
This leaves a place for sane defaults that might change depending
on the framework that is asking, and still allows you to set your team
*once* for a service in the general config.

Everything in here is private, and you shouldn't worry about it.
"""
import json
import logging
import os

import pysensu_yelp
import service_configuration_lib

from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaNotConfiguredError


log = logging.getLogger(__name__)


def monitoring_defaults(key):
    defaults = {
        'runbook': 'Please set a `runbook` field in your monitoring.yaml. Like "y/rb-mesos". Docs: '
                   'https://paasta.readthedocs.io/en/latest/yelpsoa_configs.html#monitoring-yaml',
        'tip': 'Please set a `tip` field in your monitoring.yaml. Docs: '
               'https://paasta.readthedocs.io/en/latest/yelpsoa_configs.html#monitoring-yaml',
        'ticket': False,
        'project': None,
        'realert_every': -1,
    }
    return defaults.get(key, None)


def get_team(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('team', overrides, service, soa_dir)


def get_runbook(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('runbook', overrides, service, soa_dir)


def get_tip(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('tip', overrides, service, soa_dir)


def get_notification_email(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('notification_email', overrides, service, soa_dir)


def get_page(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('page', overrides, service, soa_dir)


def get_alert_after(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('alert_after', overrides, service, soa_dir)


def get_realert_every(
    overrides, service, soa_dir=DEFAULT_SOA_DIR,
    monitoring_defaults=monitoring_defaults,
):
    return __get_monitoring_config_value(
        'realert_every',
        overrides=overrides,
        service=service,
        soa_dir=soa_dir,
        monitoring_defaults=monitoring_defaults,
    )


def get_check_every(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('check_every', overrides, service, soa_dir)


def get_irc_channels(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('irc_channels', overrides, service, soa_dir)


def get_slack_channels(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('slack_channels', overrides, service, soa_dir)


def get_dependencies(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('dependencies', overrides, service, soa_dir)


def get_ticket(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('ticket', overrides, service, soa_dir)


def get_project(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('project', overrides, service, soa_dir)


def __get_monitoring_config_value(
    key, overrides, service,
    soa_dir=DEFAULT_SOA_DIR,
    monitoring_defaults=monitoring_defaults,
):
    general_config = service_configuration_lib.read_service_configuration(service, soa_dir=soa_dir)
    monitor_config = read_monitoring_config(service, soa_dir=soa_dir)
    service_default = general_config.get(key, monitoring_defaults(key))
    service_default = general_config.get('monitoring', {key: service_default}).get(key, service_default)
    service_default = monitor_config.get(key, service_default)
    return overrides.get(key, service_default)


def get_team_email_address(service, overrides=None, soa_dir=DEFAULT_SOA_DIR):
    """Looks up the team email address from specific marathon or chronos config
    (most specific) to monitoring.yaml, or the global Sensu team_data.json.
    (least specific). Returns None if nothing is available.

    This function is most useful for when you *really* need an email address to use
    for non-Sensu applications. (chronos, jenkins, etc)

    This function should *not* be used with Sensu stuff. Instead you should
    leave `notification_email` absent and just let Sensu do its thing."""
    if overrides is None:
        overrides = {}
    email_address = __get_monitoring_config_value(
        'notification_email', overrides=overrides, service=service, soa_dir=soa_dir,
    )
    if not email_address:
        team = get_team(overrides=overrides, service=service)
        email_address = get_sensu_team_data(team).get('notification_email', None)
    return email_address


def get_sensu_team_data(team):
    """Takes a team and returns the dictionary of Sensu configuration
    settings for that team. The data is in this format:
    https://github.com/Yelp/sensu_handlers#teams
    Returns an empty dictionary if there is nothing to return.

    Not all teams specify all the different types of configuration settings.
    for example, a team may not specify a `notification_email`. It is up
    to the caller of this function to handle that case.
    """
    global_team_data = _load_sensu_team_data()['team_data']
    return global_team_data.get(team, {})


def _load_sensu_team_data():
    try:
        with open('/etc/sensu/team_data.json') as f:
            team_data = json.load(f)
    except IOError:
        log.warning("No Sensu Team data (/etc/sensu/team_data.json) available. Using empty defaults")
        team_data = {}
    return team_data


def send_event(service, check_name, overrides, status, output, soa_dir, ttl=None, cluster=None):
    """Send an event to sensu via pysensu_yelp with the given information.

    :param service: The service name the event is about
    :param check_name: The name of the check as it appears in Sensu
    :param overrides: A dictionary containing overrides for monitoring options
                      (e.g. notification_email, ticket, page)
    :param status: The status to emit for this event
    :param output: The output to emit for this event
    :param soa_dir: The service directory to read monitoring information from
    :param cluster: The cluster name (optional)
    """
    # This function assumes the input is a string like "mumble.main"
    team = get_team(overrides, service, soa_dir)
    if not team:
        return

    runbook = overrides.get('runbook', 'http://y/paasta-troubleshooting')
    system_paasta_config = load_system_paasta_config()
    if cluster is None:
        try:
            cluster = system_paasta_config.get_cluster()
        except PaastaNotConfiguredError:
            cluster = 'localhost'
    result_dict = {
        'tip': get_tip(overrides, service, soa_dir),
        'notification_email': get_notification_email(overrides, service, soa_dir),
        'irc_channels': get_irc_channels(overrides, service, soa_dir),
        'slack_channels': get_slack_channels(overrides, service, soa_dir),
        'ticket': get_ticket(overrides, service, soa_dir),
        'project': get_project(overrides, service, soa_dir),
        'page': get_page(overrides, service, soa_dir),
        'alert_after': overrides.get('alert_after', '5m'),
        'check_every': overrides.get('check_every', '1m'),
        'realert_every': overrides.get('realert_every', monitoring_defaults('realert_every')),
        'source': 'paasta-%s' % cluster,
        'ttl': ttl,
    }

    sensu_host = system_paasta_config.get_sensu_host()
    sensu_port = system_paasta_config.get_sensu_port()

    if sensu_host is not None:
        pysensu_yelp.send_event(
            check_name, runbook, status, output, team, sensu_host=sensu_host, sensu_port=sensu_port,
            **result_dict,
        )


def read_monitoring_config(service, soa_dir=DEFAULT_SOA_DIR):
    """Read a service's monitoring.yaml file.

    :param service: The service name
    :param soa_dir: THe SOA configuration directory to read from
    :returns: A dictionary of whatever was in soa_dir/name/monitoring.yaml"""
    rootdir = os.path.abspath(soa_dir)
    monitoring_file = os.path.join(rootdir, service, "monitoring.yaml")
    monitor_conf = service_configuration_lib.read_monitoring(monitoring_file)
    return monitor_conf


def list_teams(**kwargs):
    """Loads team data from the system. Returns a set of team names (or empty
    set).
    """
    team_data = _load_sensu_team_data()
    teams = set(team_data.get('team_data', {}).keys())
    return teams
