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


def get_team(framework, service_name, instance_name=None):
    return __get_monitoring_config_value('team', framework, service_name, instance_name)


def get_runbook(framework, service_name, instance_name=None):
    return __get_monitoring_config_value('runbook', framework, service_name, instance_name)


def get_tip(framework, service_name, instance_name=None):
    return __get_monitoring_config_value('tip', framework, service_name, instance_name)


def get_notification_email(framework, service_name, instance_name=None):
    return __get_monitoring_config_value('notification_email', framework, service_name, instance_name)


def get_page(framework, service_name, instance_name=None):
    return __get_monitoring_config_value('page', framework, service_name, instance_name)


def get_alert_after(framework, service_name, instance_name=None):
    return __get_monitoring_config_value('alert_after', framework, service_name, instance_name)


def __get_monitoring_config_value(key, framework, service_name, instance_name=None):
    general_config = service_configuration_lib.read_service_configuration(service_name)
    if instance_name:
        job_config = marathon_tools.read_service_config(service_name, instance_name)
    else:
        job_config = {}
    monitor_config = marathon_tools.read_monitoring_config(service_name)
    service_default = general_config.get(key, False)
    service_default = general_config.get('monitoring', {key: service_default}).get(key, service_default)
    service_default = monitor_config.get(key, service_default)
    return job_config.get(key, service_default)
