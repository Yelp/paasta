#!/usr/bin/env python
import json
import os
import shlex
import socket
import sys

from docker import Client
from docker import errors

from paasta_tools.marathon_tools import CONTAINER_PORT
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.marathon_tools import load_service_namespace_config
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
    s.close()
    return port


def get_healthcheck(service, instance, random_port):
    smartstack_config = load_service_namespace_config(service, instance)
    mode = smartstack_config.get('mode', 'http')
    hostname = socket.getfqdn()

    uri = PaastaColors.cyan('%s://%s:%d\n' % (mode, hostname, random_port))

    return 'Mesos would have healthchecked your service via\n%s\n' % uri


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


def run_docker_container_interactive(service, instance, docker_hash, volumes, command, service_manifest):
    """
    Since docker-py has some issues with running a container with TTY attached we're just executing
    docker run command for interactive mode. For non-interactive mode we're using docker-py.
    """
    sys.stderr.write(PaastaColors.yellow(
        'Warning! You\'re running a container in interactive mode.\n'
        'This is not how Mesos runs containers. To run container exactly\n'
        'like Mesos does don\'t use the -I flag.\n\n'
    ))

    run_args = ['docker', 'run']

    for volume in volumes:
        run_args.append('--volume=%s' % volume)

    random_port = pick_random_port()

    run_args.append('--tty=true')
    run_args.append('--interactive=true')
    # We inject a BAD_PORT as the PORT variable, as marathon injects the externally
    # assigned port like this. That allows this test run to catch services that might
    # be using this variable in surprising ways.
    run_args.append('--env=PORT=BAD_PORT')
    run_args.append('--memory=%dm' % service_manifest.get_mem())
    run_args.append('--publish=%d:%d' % (random_port, CONTAINER_PORT))
    run_args.append('%s' % docker_hash)
    run_args.extend(command)

    sys.stdout.write('Running docker command:\n%s\n' % ' '.join(run_args))

    healthcheck_string = get_healthcheck(service, instance, random_port)
    sys.stdout.write(healthcheck_string)

    os.execlp('/usr/bin/docker', *run_args)


def run_docker_container_non_interactive(
    docker_client,
    service,
    instance,
    docker_hash,
    volumes,
    command,
    service_manifest
):
    """
    Using docker-py for non-interactive run of a container. In the end of function it stops the container
    and removes it.
    """
    sys.stderr.write(PaastaColors.yellow(
        'Warning! You\'re running a container in non-interactive mode.\n'
        'This is how Mesos runs containers. Some programs behave differently\n'
        'with no tty attach.\n\n'
    ))

    create_result = docker_client.create_container(
        image=docker_hash,
        command=command,
        tty=False,
        volumes=volumes,
        mem_limit='%dm' % service_manifest.get_mem(),
        cpu_shares=service_manifest.get_cpus(),
        ports=[CONTAINER_PORT],
        stdin_open=False,
    )

    container_started = False
    try:
        docker_client.start(create_result['Id'], port_bindings={CONTAINER_PORT: None})

        container_started = True

        smartstack_config = load_service_namespace_config(service, instance)
        port = smartstack_config.get('proxy_port', 0)

        healthcheck_string = get_healthcheck(service, instance, port)
        sys.stdout.write(healthcheck_string)

        for line in docker_client.attach(create_result['Id'], stream=True, logs=True):
            sys.stdout.write(line)

    except KeyboardInterrupt:
        if container_started:
            docker_client.stop(create_result['Id'])
            docker_client.remove_container(create_result['Id'])
            raise

    docker_client.stop(create_result['Id'])
    docker_client.remove_container(create_result['Id'])


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

    service_manifest = load_marathon_service_config(service, args.instance, marathon_config['cluster'])

    if args.cmd:
        command = shlex.split(args.cmd)
    else:
        command = service_manifest.get_args()

    if args.interactive:
        run_docker_container_interactive(
            service,
            args.instance,
            docker_hash,
            volumes,
            command,
            service_manifest
        )
    else:
        run_docker_container_non_interactive(
            docker_client,
            service,
            args.instance,
            docker_hash,
            volumes,
            command,
            service_manifest
        )


def build_docker_container(docker_client, args):
    """
    Build Docker container from Dockerfile in the current directory or
    specified in command line args. Resulting image hash.
    """
    result = ''

    dockerfile_path = os.getcwd()

    sys.stdout.write('Building container from Dockerfile in %s\n' % dockerfile_path)

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
