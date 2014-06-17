#!/usr/bin/env python

import isodate
import service_configuration_lib


DEFAULT_EXECUTOR = 'some sort of default - look in to this'
DEFAULT_EXECUTOR_FLAGS = 'flags that will be determined'
DEFAULT_EPSILON = 'PT60S'
DEFAULT_RETRIES = 2
DEFAULT_CPUS = 0.1
DEFAULT_MEMORY = 100
DEFAULT_DISK = 100
DEFAULT_URIS = []


class InvalidChronosException(Exception):
    pass


def main():
    for service in service_configuration_lib.read_services_configuration():
        print 'hello'


def parse_job_config(service_name, job_config):
    return {
        'name': get_name(service_name),
        'command': get_command(job_config),
        'epsilon': get_epsilon(job_config),
        'executor': get_executor(),
        'executorFlags': get_executor_flags(),
        'retries': get_retries(job_config),
        'owner': get_owner(job_config),
        'async': get_async(),
        'cpus': get_cpus(job_config),
        'memory': get_memory(job_config),
        'disk': get_disk(job_config),
        'disabled': get_disabled(),
        'uris': get_uris(),
        'schedule': get_schedule(job_config),
    }


def get_name(service_name):
    return service_name


def get_epsilon(job_config):
    epsilon = job_config.get('epsilon', DEFAULT_EPSILON)
    isodate.parse_duration(epsilon)  # throws isodate.ISO8601Error

    return epsilon


def get_command(job_config):
    return job_config['command']


def get_executor():
    return DEFAULT_EXECUTOR


def get_executor_flags():
    return DEFAULT_EXECUTOR_FLAGS


def get_retries(job_config):
    return int(job_config.get('retries', DEFAULT_RETRIES))


def get_owner(job_config):
    return job_config['failure_contact_email']


def get_async():
    """Async Chronos jobs seem like the sort of thing that we should
    explore and have a framework for before we allow them"""
    return False


def get_cpus(job_config):
    return float(job_config.get('mesos_cpus', DEFAULT_CPUS))


def get_memory(job_config):
    return int(job_config.get('mesos_memory', DEFAULT_MEMORY))


def get_disk(job_config):
    return int(job_config.get('mesos_disk', DEFAULT_DISK))


def get_disabled():
    return False


def get_uris():
    return DEFAULT_URIS


def get_schedule(job_config):
    schedule = job_config['schedule']
    # TODO isodate does not have intervals
    return schedule
