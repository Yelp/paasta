#!/usr/bin/env python
"""
Getters for deriving monitoring parameters for mesos-deployed stuff.
This leaves a place for sane defaults that might change depending
on the framework that is asking, and still allows you to set your team
*once* for a service in the general config.

Everything in here is private, and you shouldn't worry about it.
"""

import service_configuration_lib
import marathon_tools
from paasta_tools.paasta_cli.utils import _load_sensu_team_data


def get_team(framework, service_name, instance_name=None,
             soa_dir=service_configuration_lib.DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('team', framework, service_name, instance_name, soa_dir)


def get_runbook(framework, service_name, instance_name=None,
                soa_dir=service_configuration_lib.DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('runbook', framework, service_name, instance_name, soa_dir)


def get_tip(framework, service_name, instance_name=None,
            soa_dir=service_configuration_lib.DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('tip', framework, service_name, instance_name, soa_dir)


def get_notification_email(framework, service_name, instance_name=None,
                           soa_dir=service_configuration_lib.DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('notification_email', framework, service_name, instance_name, soa_dir)


def get_page(framework, service_name, instance_name=None,
             soa_dir=service_configuration_lib.DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('page', framework, service_name, instance_name, soa_dir)


def get_alert_after(framework, service_name, instance_name=None,
                    soa_dir=service_configuration_lib.DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('alert_after', framework, service_name, instance_name, soa_dir)


def get_realert_every(framework, service_name, instance_name=None,
                      soa_dir=service_configuration_lib.DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('realert_every', framework, service_name, instance_name, soa_dir)


def get_check_every(framework, service_name, instance_name=None,
                    soa_dir=service_configuration_lib.DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('check_every', framework, service_name, instance_name, soa_dir)


def get_irc_channels(framework, service_name, instance_name=None,
                     soa_dir=service_configuration_lib.DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('irc_channels', framework, service_name, instance_name, soa_dir)


def get_dependencies(framework, service_name, instance_name=None,
                     soa_dir=service_configuration_lib.DEFAULT_SOA_DIR):
    return __get_monitoring_config_value('dependencies', framework, service_name, instance_name, soa_dir)


def __get_monitoring_config_value(key, framework, service_name, instance_name=None,
                                  soa_dir=service_configuration_lib.DEFAULT_SOA_DIR):
    general_config = service_configuration_lib.read_service_configuration(service_name, soa_dir=soa_dir)
    if instance_name:
        cluster = marathon_tools.get_cluster()
        job_config = marathon_tools.load_marathon_service_config(service_name, instance_name, cluster, soa_dir=soa_dir)
    else:
        job_config = marathon_tools.MarathonServiceConfig(service_name, instance_name, {}, {})
    monitor_config = marathon_tools.read_monitoring_config(service_name, soa_dir=soa_dir)
    service_default = general_config.get(key, monitoring_defaults(key))
    service_default = general_config.get('monitoring', {key: service_default}).get(key, service_default)
    service_default = monitor_config.get(key, service_default)
    return job_config.get(key, service_default)


def monitoring_defaults(key):
    defaults = {
        'runbook': 'Please set a `runbook` field in your monitoring.yaml. Like "y/rb-mesos". Docs: '
                   'https://trac.yelpcorp.com/wiki/HowToService/Monitoring/monitoring.yaml',
        'tip': 'Please set a `tip` field in your monitoring.yaml. Docs: '
               'https://trac.yelpcorp.com/wiki/HowToService/Monitoring/monitoring.yaml',
    }
    return defaults.get(key, False)


def get_team_email_address(service, framework=None, instance=None):
    """ Looks up the team email address from the most specific to least specific order
    going from the specific marathon or chronos config, to monitoring.yaml, then
    to the global Sensu team_data.json. Returns None if nothing is available.

    This function is most useful for when you *really* need an email address to use
    for non-Sensu applications. (chronos, jenkins, etc)

    This function should *not* be used with Sensu stuff. Instead you should
    leave `notification_email` absent and just let Sensu do its thing."""

    email_address = __get_monitoring_config_value('notification_email', framework=framework,
                                                  service_name=service, instance_name=instance)
    if not email_address:
        team = get_team(framework=framework, service_name=service, instance_name=instance)
        email_address = get_sensu_team_data(team).get('notification_email', None)
    return email_address


def get_sensu_team_data(team):
    """Takes a team and returns the dictionary of Sensu configuration
    settings for that team. The data is in this format:
    https://github.com/Yelp/sensu_handlers#teams
    Returns an empty dictionary if there is nothing to return.

    Not all teams specify all the different types of configuration settings.
    for example a team may not specify a `nofitication_email`. It is up
    to the caller of this function to handle that case.
    """
    global_team_data = _load_sensu_team_data()['team_data']
    return global_team_data.get(team, {})
