#!/usr/bin/env python

import argparse
import isodate
import os
import service_configuration_lib
import yaml


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
    args = parse_args()

    write_chronos_configs(args.soa_dir, args.chronos_dir, args.ecosystem)


def write_chronos_configs(soa_dir, chronos_dir, ecosystem):
    services = service_configuration_lib.read_services_configuration(soa_dir)
    for service_name, service_configuration in services.iteritems():
        chronos_jobs = service_configuration_lib.read_extra_service_information(service_name, 'chronos-%s' % ecosystem, soa_dir=soa_dir)
        for job in chronos_jobs:
            job_config = parse_job_config(job['name'], job)
            with open(os.path.join(chronos_dir, 'scheduled', '%s_%s.yaml' % (service_name, job['name'])), 'w') as f:
                print 'Writing config for %s/%s' % (service_name, job['name'])
                f.write(yaml.dump(job_config))


def parse_args():
    parser = argparse.ArgumentParser(description='Creates chronos configurations from yelpsoa-configs')
    parser.add_argument('--chronos-dir', dest='chronos_dir', metavar='CHRONOS_DIR',
                        help='chronos configuration directory')
    parser.add_argument('--ecosystem', dest='ecosystem', metavar='ECOSYSTEM',
                        help='ecosystem to generate configuration for')
    parser.add_argument('-d', '--soa-dir', dest='soa_dir', metavar='SOA_DIR',
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    args = parser.parse_args()
    return args


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

if __name__ == '__main__':
    main()
