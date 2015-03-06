#!/usr/bin/env python
import json
import os
import shlex
import socket
import sys

from docker import Client
from docker import errors

from paasta_tools.marathon_tools import CONTAINER_PORT
from paasta_tools.marathon_tools import get_args
from paasta_tools.marathon_tools import get_mem
from paasta_tools.marathon_tools import read_service_config
from paasta_tools.marathon_tools import read_service_namespace_config
from paasta_tools.paasta_cli.utils import figure_out_service_name
from paasta_tools.paasta_cli.utils import lazy_choices_completer
from paasta_tools.paasta_cli.utils import list_instances
from paasta_tools.paasta_cli.utils import list_services
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import read_marathon_config


def pick_random_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    addr, port = s.getsockname()
    s.shutdown()
    s.close()
    return port


def get_healthcheck(service, instance, random_port):
    smartstack_config = read_service_namespace_config(service, instance)
    mode = smartstack_config.get('mode', 'http')
    hostname = socket.getfqdn()

    uri = PaastaColors.cyan('%s://%s:%d\n' % (mode, hostname, random_port))

    return 'Mesos will make sure that your service is healthy via\n%s\n' % uri


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'test-run',
        description='Test run service Docker container',
        help='Test run service Docker container',
    )
    list_parser.add_argument(
        '-s', '--service',
        help='The name of the service you wish to inspect',
    ).completer = lazy_choices_completer(list_services)
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
        help='Simulate a docker run for a particular instance of the service, like "main" or "canary"',
        required=False,
        default='main',
    ).completer = lazy_choices_completer(list_instances)
    list_parser.add_argument(
        '-v', '--verbose',
        help='Show Docker commands output',
        action='store_true',
        required=False,
        default=True,
    )
    list_parser.add_argument(
        '-I', '--interactive',
        help='Run container in interactive mode',
        action='store_true',
        required=False,
        default=False,
    )

    list_parser.set_defaults(command=paasta_test_run)


def run_docker_container(docker_client, docker_hash, service, args):
    """
    Run Docker container by image hash with args set in command line.
    Function prints the output of run command in stdout.
    """
    marathon_config_raw = read_marathon_config()

    volumes = list()

    for volume in marathon_config_raw['docker_volumes']:
        volumes.append('%s:%s:%s' % (volume['hostPath'], volume['containerPath'], volume['mode'].lower()))

    marathon_config = dict()

    marathon_config['cluster'] = marathon_config_raw['cluster']
    marathon_config['volumes'] = volumes

    service_manifest = read_service_config(service, args.instance, marathon_config['cluster'])

    if args.cmd:
        command = shlex.split(args.cmd)
    else:
        command = get_args(service_manifest)

    if args.interactive:
        interactive_string = 'true'
        sys.stderr.write(
            'Warning! You\'re running a container in interactive mode.\n'
            'This is not how Mesos runs containers. To run container exactly\n'
            'like Mesos does don\'t use the -I flag.\n\n'
        )
    else:
        interactive_string = 'false'
        sys.stderr.write(
            'Warning! You\'re running a container in non-interactive mode.\n'
            'This is how Mesos runs containers. Some programs behave differently\n'
            'with no tty attach.\n\n'
        )

    run_args = ['docker', 'run']

    for volume in volumes:
        run_args.append('--volume=%s' % volume)

    random_port = pick_random_port()

    run_args.append('--tty=%s' % interactive_string)
    run_args.append('--interactive=%s' % interactive_string)
    run_args.append('--memory=%s' % (str(get_mem(service_manifest)) + 'm'))
    run_args.append('--publish=%d:%d' % (random_port, CONTAINER_PORT))
    run_args.append('%s' % docker_hash)
    run_args.extend(command)

    sys.stdout.write('Running docker command:\n%s\n' % ' '.join(run_args))

    if args.interactive:
        healthcheck_string = get_healthcheck(service, args.instance, random_port)
        sys.stdout.write(healthcheck_string)

        os.execlp('/usr/bin/docker', *run_args)
    else:
        sys.stderr.write('Not implemented yet.\n')


def build_docker_container(docker_client, args):
    """
    Build Docker container from Dockerfile in the current directory or
    specified in command line args. Resulting image hash.
    """
    result = ''

    dockerfile_path = os.getcwd()

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
    service = figure_out_service_name(args)

    base_docker_url = os.environ.get('DOCKER_HOST', 'unix://var/run/docker.sock')

    docker_client = Client(base_url=base_docker_url)

    try:
        docker_hash = build_docker_container(docker_client, args)
    except errors.APIError as e:
        sys.stderr.write('Can\'t build Docker container. Error: %s\n' % str(e))
        sys.exit(1)

    try:
        run_docker_container(docker_client, docker_hash, service, args)
    except errors.APIError as e:
        sys.stderr.write('Can\'t run Docker container. Error: %s\n' % str(e))
        sys.exit(1)
