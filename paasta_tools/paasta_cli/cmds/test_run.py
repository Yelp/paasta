#!/usr/bin/env python
import json
import sys
import yaml

from docker import Client
from docker import errors


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'test-run',
        description='Test run service Docker container (not implemented)',
        help='Test run service Docker container (not implemented)')

    list_parser.add_argument('-s', '--service',
                             help='Name of service for which you wish to '
                             + 'upload a docker image. Leading "services-'
                             + '", as included in a Jenkins job name, will'
                             + ' be stripped.',
                             required=True,
                             )
    list_parser.add_argument('-p', '--path',
                             help='Path to Dockerfile which you want to test-run',
                             required=False,
                             )
    list_parser.add_argument('-t', '--tty',
                             help='Run Docker container with --tty=true',
                             required=False,
                             )
    list_parser.add_argument('-c', '--cmd',
                             help='Run Docker container with particular command, o'
                             + 'for instance bash',
                             required=False,
                             )
    list_parser.add_argument('-C', '--canary',
                             help='Run Docker container with CPU/memory limit set '
                             + 'for canary cluster',
                             required=False,
                             )
    list_parser.add_argument('-v', '--verbose',
                             help='Show Docker commands output',
                             required=False,
                             )
    list_parser.add_argument('-e', '--cluster',
                             help='Specify Marathon cluster',
                             required=False,
                             )

    list_parser.set_defaults(command=paasta_test_run)


def read_marathon_config():
    """
    Read Marathon configs to get cluster info and volumes
    that we need to bind when runngin a container.
    """
    config_path = '/etc/paasta_tools/paasta.json'

    config = json.loads(open(config_path).read())

    volumes = list()

    for volume in config['docker_volumes']:
        volumes.append(volume['hostPath'] + ':' + volume['containerPath'] + ':' + volume['mode'].lower())

    result = dict()

    result['cluster'] = config['cluster']
    result['volumes'] = volumes
    return result


def read_service_manifest(service, cluster, canary):
    """
    Read service manifest to get info about the environment.
    """
    path = '/nail/etc/services/' + service + '/marathon-' + cluster + '.yaml'

    manifest = yaml.load(file(path, 'r'))

    if canary:
        return manifest['main']
    else:
        return manifest['main']


def run_docker_container(docker_client, docker_hash, args):
    """
    Run Docker container by image hash with args set in command line.
    Function prints the output of run command in stdout.
    """
    marathon_config = read_marathon_config()

    service_manifest = read_service_manifest(args.service, marathon_config['cluster'], args.canary)

    print docker_hash

    command = ''
    if args.cmd:
        command = args.cmd

    tty = False
    if args.tty:
        tty = args.tty

    create_result = docker_client.create_container(image=docker_hash,
                                                   command=command,
                                                   tty=tty,
                                                   volumes=marathon_config['volumes'],
                                                   mem_limit=str(service_manifest['mem']) + 'm',
                                                   cpu_shares=service_manifest['cpus'],
                                                   ports=[8888],
                                                   stdin_open=True,
                                                   )

    docker_client.start(create_result['Id'], port_bindings={8888: None})

    for line in docker_client.attach(create_result['Id'], stream=True, logs=True):
        sys.stdout.write(line)


def build_docker_container(docker_client, args):
    """
    Build Docker container from Dockerfile in the current directory or
    specified in command line args. Returns result image hash.
    """
    result = ''

    dockerfile_path = './'

    if args.path:
        dockerfile_path = args.path

    for line in docker_client.build(path=dockerfile_path, tag='latest'):
        line_dict = json.loads(line)

        try:
            stream_line = line_dict['stream']

            if args.verbose:
                sys.stdout.write(stream_line)

            if stream_line.startswith('Successfully built '):
                """Strip the beginning of a string and \n in the end."""
                result = stream_line[len('Successfully built '):]
                result = result[:len(result) - 1]
        except:
            pass

    return result


def paasta_test_run(args):
    docker_client = Client(base_url='unix://var/run/docker.sock')

    try:
        docker_hash = build_docker_container(docker_client, args)
    except errors.APIError as e:
        sys.stderr.write('Can\'t build Docker container with message: ' + str(e) + '\n')
        sys.exit(1)

    try:
        run_docker_container(docker_client, docker_hash, args)
    except errors.APIError as e:
        sys.stderr.write('Can\'t run Docker container with message: ' + str(e) + '\n')
        sys.exit(1)
