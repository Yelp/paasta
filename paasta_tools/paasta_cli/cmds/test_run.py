#!/usr/bin/env python
import json
import os
from os import execlp
from random import randint
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
from paasta_tools.paasta_cli.utils import validate_service_name
from paasta_tools.utils import get_username
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import _run


BAD_PORT_WARNING = 'This_service_is_listening_on_the_PORT_variable__You_must_use_8888__see_y/paasta_deploy'


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

    return '\nMesos would have healthchecked your service via\n%s\n' % uri


def read_local_dockerfile_lines():
    dockerfile = os.path.join(os.getcwd(), 'Dockerfile')
    return open(dockerfile).readlines()


def get_cmd():
    """Returns first CMD line from Dockerfile"""
    for line in read_local_dockerfile_lines():
        if line.startswith('CMD'):
            return line.lstrip('CMD ')
    return "Unknown. Is there a CMD line in the Dockerfile?"


def get_cmd_string():
    """Returns get_cmd() with some formatting and explanation."""
    cmd = get_cmd()
    return ('You are in interactive mode, which may not run the exact command\n'
            'that PaaSTA would have run. Run this command yourself to simulate\n'
            'PaaSTA:\n%s\n' % PaastaColors.yellow(cmd))


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


def get_container_name():
    return 'paasta_test_run_%s_%s' % (get_username(), randint(1, 999999))


def get_docker_run_cmd(memory, random_port, container_name, volumes, interactive, docker_hash, command):
    cmd = ['docker', 'run']
    # We inject an invalid port as the PORT variable, as marathon injects the externally
    # assigned port like this. That allows this test run to catch services that might
    # be using this variable in surprising ways. See PAASTA-267 for more context.
    cmd.append('--env=PORT=%s' % BAD_PORT_WARNING)
    cmd.append('--memory=%dm' % memory)
    cmd.append('--publish=%d:%d' % (random_port, CONTAINER_PORT))
    cmd.append('--name=%s' % container_name)
    for volume in volumes:
        cmd.append('--volume=%s' % volume)
    if interactive:
        cmd.append('--interactive=true')
        cmd.append('--tty=true')
    else:
        cmd.append('--detach=true')
    cmd.append('%s' % docker_hash)
    cmd.extend(command)
    return cmd


class LostContainerException(Exception):
    pass


def get_container_id(docker_client, container_name):
    """Use 'docker_client' to find the container we started, identifiable by
    its 'container_name'. If we can't find the id, raise
    LostContainerException.
    """
    containers = docker_client.containers()
    for container in containers:
        if '/%s' % container_name in container.get('Names', []):
            return container.get('Id')
    raise LostContainerException(
        "Can't find the container I just launched so I can't do anything else.\n"
        "Try docker 'ps --all | grep %s' to see where it went.\n"
        "Here were all the containers:\n"
        "%s" % (container_name, containers)
    )


def _cleanup_container(docker_client, container_id):
    docker_client.stop(container_id)
    docker_client.remove_container(container_id)


def run_docker_container(
    docker_client,
    service,
    instance,
    docker_hash,
    volumes,
    interactive,
    command,
    service_manifest
):
    """docker-py has issues running a container with a TTY attached, so for
    consistency we execute 'docker run' directly in both interactive and
    non-interactive modes.

    In non-interactive mode when the run is complete, stop the container and
    remove it (with docker-py).
    """
    if interactive:
        sys.stderr.write(PaastaColors.yellow(
            "Warning! You're running a container in interactive mode.\n"
            "This is *NOT* how Mesos runs containers.\n"
            "To run the container exactly as Mesos does, omit the -I flag.\n\n"
        ))
    else:
        sys.stderr.write(PaastaColors.yellow(
            "You're running a container in non-interactive mode.\n"
            "This is how Mesos runs containers.\n"
            "Note that some programs behave differently with no tty attached.\n\n"
        ))

    memory = service_manifest.get_mem()
    random_port = pick_random_port()
    container_name = get_container_name()
    docker_run_cmd = get_docker_run_cmd(memory, random_port, container_name, volumes, interactive, docker_hash, command)
    sys.stdout.write('Running docker command:\n%s\n' % ' '.join(docker_run_cmd))
    container_started = False

    healthcheck_string = get_healthcheck(service, instance, random_port)
    sys.stdout.write(healthcheck_string)
    if interactive:
        sys.stdout.write(get_cmd_string())

    if interactive:
        # NOTE: This immediately replaces us with the docker run cmd. Docker
        # run knows how to clean up the running container in this situation.
        execlp('docker', *docker_run_cmd)
        # For testing, when execlp is patched out and doesn't replace us, we
        # still want to bail out.
        return

    container_id = None
    # This isn't technically correct/space-safe but I don't care right now.
    # Do this instead:
    # http://stackoverflow.com/questions/4748344/whats-the-reverse-of-shlex-split
    # ya lol this is actually broken i think when i try to -C "bash -c '...'"
    joined_cmd = ' '.join(docker_run_cmd)
    try:
        (returncode, output) = _run(joined_cmd)
        sys.stdout.write('got returncode %d and output %s\n' % (returncode, output))
        if returncode != 0:
            # Container failed to start so no need to cleanup; just bail.
            return
        container_started = True
        container_id = get_container_id(docker_client, container_name)
        sys.stdout.write('got container_id %s\n' % container_id)
        for line in docker_client.attach(container_id, stderr=True, stream=True, logs=True):
            sys.stdout.write(line)
    except KeyboardInterrupt:
        if container_started:
            _cleanup_container(docker_client, container_id)
            raise
    # Also cleanup if the container exits on its own.
    # ### if container_started: ???
    _cleanup_container(docker_client, container_id)


def configure_and_run_docker_container(docker_client, docker_hash, service, args):
    """
    Run Docker container by image hash with args set in command line.
    Function prints the output of run command in stdout.
    """
    system_paasta_config = load_system_paasta_config()

    volumes = list()

    for volume in system_paasta_config['docker_volumes']:
        volumes.append('%s:%s:%s' % (volume['hostPath'], volume['containerPath'], volume['mode'].lower()))

    marathon_config = dict()

    marathon_config['cluster'] = system_paasta_config['cluster']
    marathon_config['volumes'] = volumes

    service_manifest = load_marathon_service_config(service, args.instance, marathon_config['cluster'])

    if args.cmd:
        command = shlex.split(args.cmd)
    else:
        command = service_manifest.get_args()

    run_docker_container(
        docker_client,
        service,
        args.instance,
        docker_hash,
        volumes,
        args.interactive,
        command,
        service_manifest
    )


def build_docker_container(docker_client, args):
    """
    Build Docker container from Dockerfile in the current directory or
    specified in command line args. Resulting image id.
    """
    image_id = None
    dockerfile_path = os.getcwd()
    sys.stdout.write('Building container from Dockerfile in %s\n' % dockerfile_path)

    for line in docker_client.build(path=dockerfile_path, tag='latest'):
        line_dict = json.loads(line)

        stream_line = line_dict.get('stream')

        if args.verbose and stream_line:
            sys.stdout.write(stream_line)

        if stream_line and stream_line.startswith('Successfully built '):
            # Strip the beginning of a string and \n in the end.
            image_id = stream_line[len('Successfully built '):].strip()

    if image_id:
        return image_id
    else:
        sys.stderr.write("Error: Failed to build docker image")
        sys.exit(1)


def validate_environment():
    """Validates whether the current directory is good for running
    paasta test_run"""
    if os.getcwd() == os.path.expanduser("~"):
        sys.stderr.write(
            'ERROR: Don\'t run this command from your home directory.\n'
            'Try changing to the root of your working copy of the service.\n'
        )
        sys.exit(1)
    if not os.path.isfile(os.path.join(os.getcwd(), 'Dockerfile')):
        sys.stderr.write(
            'ERROR: No Dockerfile in the current directory.\n'
            'Are you in the root folder of the service directory? Does a Dockerfile exist?'
        )
        sys.exit(1)


def paasta_test_run(args):
    validate_environment()

    service = figure_out_service_name(args)
    validate_service_name(service)

    base_docker_url = os.environ.get('DOCKER_HOST', 'unix://var/run/docker.sock')

    docker_client = Client(base_url=base_docker_url)

    try:
        docker_hash = build_docker_container(docker_client, args)
    except errors.APIError as e:
        sys.stderr.write('Can\'t build Docker container. Error: %s\n' % str(e))
        sys.exit(1)

    try:
        configure_and_run_docker_container(docker_client, docker_hash, service, args)
    except errors.APIError as e:
        sys.stderr.write('Can\'t run Docker container. Error: %s\n' % str(e))
        sys.exit(1)
