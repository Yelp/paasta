#!/usr/bin/env python
#
# Getters for deriving monitoring parameters for mesos-deployed stuff.
# This leaves a place for sane defaults that might change depending
# on the framework that is asking, and still allows you to set your team
# *once* for a service in the general config.

import service_configuration_lib
import marathon_tools


def get_team(framework, service_name, instance_name):
    return __get_monitoring_config_value('team', framework, service_name, instance_name)


def get_runbook(framework, service_name, instance_name):
    return __get_monitoring_config_value('runbook', framework, service_name, instance_name)


def get_tip(framework, service_name, instance_name):
    return __get_monitoring_config_value('tip', framework, service_name, instance_name)


def get_notification_email(framework, service_name, instance_name):
    return __get_monitoring_config_value('notification_email', framework, service_name, instance_name)


def get_page(framework, service_name, instance_name):
    return __get_monitoring_config_value('page', framework, service_name, instance_name)


def get_alert_after(framework, service_name, instance_name):
    value = __get_monitoring_config_value('alert_after', framework, service_name, instance_name)
    return value if value is not False else -1  # need != False as alert_after could be 0?


def __get_monitoring_config_value(key, framework, service_name, instance_name):
    general_config = service_configuration_lib.read_service_configuration(service_name)
    job_config = marathon_tools.read_service_config(service_name, instance_name)
    service_default = general_config.get(key, False)
    service_default = general_config.get('monitoring', {key: service_default}).get(key, service_default)
    return job_config.get(key, service_default)
