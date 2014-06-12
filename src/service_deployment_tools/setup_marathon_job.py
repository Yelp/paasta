#!/usr/bin/env python

import argparse
import service_configuration_lib
from marathon import MarathonClient


def get_marathon_config():
    #TODO read from a config file
    config = {
        'cluster': 'devc',
        'url': 'http://dev5-devc.dev.yelpcorp.com:5052',
        'user': 'admin',
        'pass': '***REMOVED***',
        'docker_registry': 'docker-dev.yelpcorp.com',
    }
    return config


def parse_args():
    parser = argparse.ArgumentParser(description='Creates marathon jobs.')
    parser.add_argument('service_name',
                        help="The service name to create or update")
    args = parser.parse_args()
    return args


def get_docker_url(docker_registry, docker_image):
    return docker_registry + '/' + docker_image


def get_docker_registry(marathon_config):
    return marathon_config['docker_registry']


def get_docker_image(service_config):
    return service_config['docker_image']


def main():
    args = parse_args()
    service_name = args.service_name

    marathon_config = get_marathon_config()

    service_general_config = service_configuration_lib.read_service_configuration(service_name)
    service_marathon_config = service_configuration_lib.get_service_dict(service_name,
                                                                         "marathon-" + marathon_config['cluster'])
    client = MarathonClient(marathon_config['url'], marathon_config['user'], marathon_config['pass'])

    docker_url = get_docker_url(get_docker_registry(marathon_config), get_docker_image(service_marathon_config))

    # https://github.com/mesosphere/marathon/blob/master/REST.md#post-v2apps
    client.create_app(
        id=service_name,
        cmd=docker_url,
        constraints=service_marathon_config['constraints'],
        container=None,
        cpus=service_marathon_config['cpus'],
        env=service_marathon_config['env'],
        executor='/var/lib/mesos/executors/docker',
        healthchecks=service_marathon_config['healthchecks'],
        mem=service_marathon_config['mem'],
        ports=service_general_config['port'],
        uris=[])


if __name__ == "__main__":
    main()
