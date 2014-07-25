#!/usr/bin/env python

import argparse
import isodate
import os
import service_configuration_lib
import setup_marathon_job
import json
import yaml


DEFAULT_EXECUTOR = ''
DEFAULT_EPSILON = 'PT60S'
DEFAULT_RETRIES = 2
DEFAULT_CPUS = 0.1
DEFAULT_MEM = 100
DEFAULT_DISK = 100
DEFAULT_URIS = []


class InvalidChronosException(Exception):
    pass


def main():
    args = parse_args()
    write_chronos_configs(args.soa_dir, args.chronos_dir, args.ecosystem)


def write_chronos_configs(soa_dir, chronos_dir, ecosystem):
    services = service_configuration_lib.read_services_configuration(soa_dir)

    chronos_jobs = extract_chronos_jobs(services, ecosystem, soa_dir)
    generated_jobs = map(parse_job_config, chronos_jobs)
    write_chronos_yaml_files(generated_jobs, chronos_dir)


def extract_chronos_jobs(services, ecosystem, soa_dir):
    jobs = []
    for service_name in services.iterkeys():
        jobs = jobs + read_chronos_soa_configs(service_name, ecosystem, soa_dir)

    return jobs


def read_chronos_soa_configs(service_name, ecosystem, soa_dir):
    service_chronos_config = service_configuration_lib.read_extra_service_information(service_name, 'chronos-%s' % ecosystem, soa_dir=soa_dir)

    # keeps the function returning lists when the yaml file is empty
    if service_chronos_config == {}:
        service_chronos_config = []

    return service_chronos_config


def write_chronos_yaml_files(chronos_jobs, chronos_dir):
    for job in chronos_jobs:
        with open(get_job_output_file_path(chronos_dir, job), 'w') as f:
            f.write(yaml.safe_dump(job))


def get_job_output_file_path(chronos_dir, job):
    """The chronos-sync.rb directory structure is made of two dirs:
    'scheduled', with a yaml file for each job (whose name corresponds to the name field),
    and dependent (same), with each dir respectively containing those types of jobs."""

    job_dir = 'scheduled'  # TODO this is silly to hardcode - we just haven't implemented dependent jobs yet
    return os.path.join(chronos_dir, job_dir, '%s.yaml' % job['name'])


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


def parse_job_config(job_config):
    return {
        'name': get_name(job_config),
        'command': get_command(job_config),
        'epsilon': get_epsilon(job_config),
        'executor': get_executor(),
        'executorFlags': get_executor_flags(job_config),
        'retries': get_retries(job_config),
        'owner': get_owner(job_config),
        'async': get_async(),
        'cpus': get_cpus(job_config),
        'mem': get_mem(job_config),
        'disk': get_disk(job_config),
        'disabled': get_disabled(),
        'uris': get_uris(),
        'schedule': get_schedule(job_config),
    }


def get_name(job_config):
    return job_config['name']


def get_epsilon(job_config):
    epsilon = job_config.get('epsilon', DEFAULT_EPSILON)
    isodate.parse_duration(epsilon)  # throws isodate.ISO8601Error

    return epsilon


def get_command(job_config):
    return job_config['command']


def get_executor():
    return DEFAULT_EXECUTOR


def get_executor_flags(job_config):
    flags = {
        'container': {
            'image': get_docker_url_for_image(job_config['docker_image']),
            'options': job_config.get('docker_options', [])
        }
    }
    return json.dumps(flags)


def get_docker_url_for_image(docker_image):
    marathon_config = setup_marathon_job.get_main_marathon_config()
    return setup_marathon_job.get_docker_url(marathon_config['docker_registry'], docker_image)


def get_retries(job_config):
    return int(job_config.get('retries', DEFAULT_RETRIES))


def get_owner(job_config):
    return job_config['failure_contact_email']


def get_async():
    """Async Chronos jobs seem like the sort of thing that we should
    explore and have a framework for before we allow them"""
    return False


def get_cpus(job_config):
    """Python likes to output floats with as much precision as possible.
    The chronos API seems to round, so be aware that some difference may
    occur"""
    return float(job_config.get('cpus', DEFAULT_CPUS))


def get_mem(job_config):
    return int(job_config.get('mem', DEFAULT_MEM))


def get_disk(job_config):
    return int(job_config.get('disk', DEFAULT_DISK))


def get_disabled():
    return False


def get_uris():
    return DEFAULT_URIS


def get_schedule(job_config):
    schedule = job_config['schedule']
    # TODO isodate does not have intervals, so we can't currently validate these
    return schedule

if __name__ == '__main__':
    main()
