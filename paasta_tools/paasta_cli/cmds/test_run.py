#!/usr/bin/env python
import json
import os
import sys

from docker import Client
from docker import errors

from paasta_tools.marathon_tools import get_args
from paasta_tools.marathon_tools import get_cpus
from paasta_tools.marathon_tools import get_mem
from paasta_tools.marathon_tools import read_service_config
from paasta_tools.paasta_cli.utils import figure_out_service_name
from paasta_tools.paasta_cli.utils import lazy_choices_completer
from paasta_tools.paasta_cli.utils import list_instances
from paasta_tools.utils import read_marathon_config


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'test-run',
        description='Test run service Docker container',
        help='Test run service Docker container',
    )

    list_parser.add_argument(
        '-s', '--service',
        help=(
            'Name of service for which you wish to '
            'upload a docker image. Leading "services-'
            '", as included in a Jenkins job name, will'
            ' be stripped.'
        ),
    )
    list_parser.add_argument(
        '-p', '--path',
        help='Path to directory with Dockerfile which you want to test-run',
        required=False,
    )
    list_parser.add_argument(
        '-t', '--tty',
        help='Run Docker container with --tty=true',
        action='store_true',
        required=False,
        default=False,
    )
    list_parser.add_argument(
        '-C', '--cmd',
        help=(
            'Run Docker container with particular command, '
            'for instance bash'
        ),
        required=False,
        default='',
    )
    list_parser.add_argument(
        '-i', '--instance',
        help='Run Docker container with environment set for particular instance',
        required=False,
        default='main',
    ).completer = lazy_choices_completer(list_instances)
    list_parser.add_argument(
        '-v', '--verbose',
        help='Show Docker commands output',
        action='store_true',
        required=False,
        default=False,
    )

    list_parser.set_defaults(command=paasta_test_run)


def run_docker_container(docker_client, docker_hash, args):
    """
    Run Docker container by image hash with args set in command line.
    Function prints the output of run command in stdout.
    """
    marathon_config_raw = read_marathon_config()

    volumes = list()

    for volume in marathon_config_raw['docker_volumes']:
        volumes.append('%s:%s:%s', volume['hostPath'], volume['containerPath'], volume['mode'].lower())

    marathon_config = dict()

    marathon_config['cluster'] = marathon_config_raw['cluster']
    marathon_config['volumes'] = volumes

    service = figure_out_service_name(args)

    service_manifest = read_service_config(service, args.instance, marathon_config['cluster'])

    create_result = docker_client.create_container(
        image=docker_hash,
        command=args.cmd + get_args(service_manifest),
        tty=args.tty,
        volumes=marathon_config['volumes'],
        mem_limit=get_mem(service_manifest),
        cpu_shares=get_cpus(service_manifest),
        ports=[8888],
        stdin_open=True,
    )

    docker_client.start(create_result['Id'], port_bindings={8888: None})

    for line in docker_client.attach(create_result['Id'], stream=True, logs=True):
        sys.stdout.write(line)


def build_docker_container(docker_client, args):
    """
    Build Docker container from Dockerfile in the current directory or
    specified in command line args. Resulting image hash.
    """
    result = ''

    dockerfile_path = './Dockerfile'

    if args.path:
        dockerfile_path = args.path + dockerfile_path

    for line in docker_client.build(path=dockerfile_path, tag='latest'):
        line_dict = json.loads(line)

        stream_line = line_dict.get('stream')

        if args.verbose:
            sys.stdout.write(stream_line)

        if stream_line.startswith('Successfully built '):
            """Strip the beginning of a string and \n in the end."""
            result = stream_line[len('Successfully built '):]
            result = result[:len(result) - 1]

    return result


def paasta_test_run(args):
    base_docker_url = os.environ.get('DOCKER_HOST', 'unix://var/run/docker.sock')

    docker_client = Client(base_url=base_docker_url)

    try:
        docker_hash = build_docker_container(docker_client, args)
    except errors.APIError as e:
        sys.stderr.write('Can\'t build Docker container. Error:\n%s' % str(e))
        sys.exit(1)

    try:
        run_docker_container(docker_client, docker_hash, args)
    except errors.APIError as e:
        sys.stderr.write('Can\'t run Docker container. Error:\n%s' % str(e))
        sys.exit(1)
