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
        job_config = marathon_tools.read_service_config(service_name, instance_name, soa_dir=soa_dir)
    else:
        job_config = {}
    monitor_config = marathon_tools.read_monitoring_config(service_name, soa_dir=soa_dir)
    service_default = general_config.get(key, monitoring_defaults(key))
    service_default = general_config.get('monitoring', {key: service_default}).get(key, service_default)
    service_default = monitor_config.get(key, service_default)
    return job_config.get(key, service_default)


def monitoring_defaults(key):
    defaults = {
        'runbook': 'Please set a `runbook` field in your monitoring.yaml. Like "y/rb-mesos". Docs: https://trac.yelpcorp.com/wiki/HowToService/Monitoring/monitoring.yaml',
        'tip': 'Please set a `tip` field in your monitoring.yaml. Like "Tip: Frob the psi-trust modulator." Docs: https://trac.yelpcorp.com/wiki/HowToService/Monitoring/monitoring.yaml',
        'command': 'N/A',
    }
    return defaults.get(key, False)
