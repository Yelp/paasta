#!/usr/bin/env python
import json
import os
from os import execlp
import pipes
from random import randint
import shlex
import socket
import sys
import time

from docker import Client
from docker import errors
import requests
from urlparse import urlparse

import service_configuration_lib
from paasta_tools.marathon_tools import CONTAINER_PORT
from paasta_tools.marathon_tools import get_healthcheck_for_instance
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.paasta_execute_docker_command import execute_in_container
from paasta_tools.paasta_cli.utils import figure_out_service_name
from paasta_tools.paasta_cli.utils import lazy_choices_completer
from paasta_tools.paasta_cli.utils import list_instances
from paasta_tools.paasta_cli.utils import list_services
from paasta_tools.utils import get_username
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import list_clusters
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import get_default_cluster_for_service
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import _run
from paasta_tools.utils import get_docker_host
from paasta_tools.utils import Timeout
from paasta_tools.utils import TimeoutError


BAD_PORT_WARNING = 'This_service_is_listening_on_the_PORT_variable__You_must_use_8888__see_y/paasta_deploy'


def pick_random_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    addr, port = s.getsockname()
    s.close()
    return port


def perform_http_healthcheck(url, timeout):
    """Returns true if healthcheck on url succeeds, false otherwise

    :param url: the healthcheck url
    :param timeout: timeout in seconds
    :returns: True if healthcheck succeeds within number of seconds specified by timeout, false otherwise
    """
    try:
        with Timeout(seconds=timeout):
            try:
                res = requests.head(url)
            except requests.ConnectionError:
                return False
    except TimeoutError:
        return False

    if 'content-type' in res.headers and ',' in res.headers['content-type']:
        sys.stdout.write(PaastaColors.yellow(
            "Multiple content-type headers detected in response."
            " The Mesos healthcheck system will treat this as a failure!"))
        return False
    # check if response code is valid per https://mesosphere.github.io/marathon/docs/health-checks.html
    elif res.status_code >= 200 and res.status_code < 400:
        return True


def perform_tcp_healthcheck(url, timeout):
    """Returns true if successfully connests to host and port, false otherwise

    :param url: the healthcheck url (in the form tcp://host:port)
    :param timeout: timeout in seconds
    :returns: True if healthcheck succeeds within number of seconds specified by timeout, false otherwise
    """
    url_elem = urlparse(url)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    result = sock.connect_ex((url_elem.hostname, url_elem.port))
    sock.close()
    if result == 0:
        return True
    else:
        return False


def perform_cmd_healthcheck(docker_client, container_id, command, timeout):
    """Returns true if return code of command is 0 when executed inside container, false otherwise

    :param docker_client: Docker client object
    :param container_id: Docker container id
    :param command: command to execute
    :param timeout: timeout in seconds
    :returns: True if command exits with return code 0, false otherwise
    """
    (_, return_code) = execute_in_container(docker_client, container_id, command, timeout)
    if return_code == 0:
        return True
    else:
        return False


def run_healthcheck_on_container(
    docker_client,
    container_id,
    healthcheck_mode,
    healthcheck_data,
    timeout
):
    """Performs healthcheck on a container

    :param container_id: Docker container id
    :param healthcheck_mode: one of 'http', 'tcp', or 'cmd'
    :param healthcheck_data: a URL when healthcheck_mode is 'http' or 'tcp', a command if healthcheck_mode is 'cmd'
    :param timeout: timeout in seconds for individual check
    :returns: true if healthcheck succeeds, false otherwise
    """
    healthcheck_result = False
    if healthcheck_mode == 'cmd':
        healthcheck_result = perform_cmd_healthcheck(docker_client, container_id, healthcheck_data, timeout)
    elif healthcheck_mode == 'http':
        healthcheck_result = perform_http_healthcheck(healthcheck_data, timeout)
    elif healthcheck_mode == 'tcp':
        healthcheck_result = perform_tcp_healthcheck(healthcheck_data, timeout)
    else:
        sys.stdout.write(PaastaColors.yellow(
            "Healthcheck mode '%s' is not currently supported!\n" % healthcheck_mode))
    return healthcheck_result


def simulate_healthcheck_on_service(
    service_manifest,
    docker_client,
    container_id,
    healthcheck_mode,
    healthcheck_data,
    healthcheck_enabled
):
    """Simulates Marathon-style healthcheck on given service if healthcheck is enabled

    :param service_manifest: service manifest
    :param docker_client: Docker client object
    :param container_id: Docker container id
    :param healthcheck_data: tuple url to healthcheck
    :param healthcheck_enabled: boolean
    :returns: if healthcheck_enabled is true, then returns output of healthcheck, otherwise simply returns true
    """
    healthcheck_link = PaastaColors.cyan(healthcheck_data)
    if healthcheck_enabled:
        grace_period = service_manifest.get_healthcheck_grace_period_seconds()
        timeout = service_manifest.get_healthcheck_timeout_seconds()
        interval = service_manifest.get_healthcheck_interval_seconds()
        max_failures = service_manifest.get_healthcheck_max_consecutive_failures()

        sys.stdout.write('\nStarting health check via %s (waiting %s seconds before '
                         'considering failures due to grace period):\n' % (healthcheck_link, grace_period))

        # silenty start performing health checks until grace period ends or first check succeeds
        graceperiod_end_time = time.time() + grace_period
        while True:
            healthcheck_succeeded = run_healthcheck_on_container(
                docker_client, container_id, healthcheck_mode, healthcheck_data, timeout)
            if healthcheck_succeeded or time.time() > graceperiod_end_time:
                break
            else:
                sys.stdout.write("%s\n" % PaastaColors.grey("Healthcheck failed (disregarded due to grace period)"))
            time.sleep(interval)

        failure = False
        for attempt in range(1, max_failures + 1):
            healthcheck_succeeded = run_healthcheck_on_container(
                docker_client, container_id, healthcheck_mode, healthcheck_data, timeout)
            if healthcheck_succeeded:
                sys.stdout.write("%s (via: %s)\n" %
                                 (PaastaColors.green("Healthcheck succeeded!"), healthcheck_link))
                failure = False
                break
            else:
                sys.stdout.write("%s (via: %s)\n" %
                                 (PaastaColors.red("Healthcheck failed! (Attempt %d of %d)" % (attempt, max_failures)),
                                  healthcheck_link))
                failure = True
            time.sleep(interval)

        if failure:
            healthcheck_status = False
        else:
            healthcheck_status = True
    else:
        sys.stdout.write('\nMesos would have healthchecked your service via\n%s\n' % healthcheck_link)
        healthcheck_status = True
    return healthcheck_status


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
        'local-run',
        description="Build and run a service's Docker container locally",
        help='Test run service Docker container',
    )
    list_parser.add_argument(
        '-s', '--service',
        help='The name of the service you wish to inspect',
    ).completer = lazy_choices_completer(list_services)
    list_parser.add_argument(
        '-c', '--cluster',
        help='The name of the cluster you wish to simulate. If omitted, attempts to guess a cluster to simulate',
    ).completer = lazy_choices_completer(list_clusters)
    list_parser.add_argument(
        '-y', '--yelpsoa-root',
        dest='soaconfig_root',
        help='A directory from which yelpsoa-configs should be read from',
        default=service_configuration_lib.DEFAULT_SOA_DIR
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
    list_parser.add_argument(
        '-k', '--no-healthcheck',
        help='Disable simulated healthcheck',
        dest='healthcheck',
        action='store_false',
        required=False,
        default=True,
    )
    list_parser.add_argument(
        '-t', '--healthcheck-only',
        help='Terminates container after healthcheck (exits with status code 0 on success, 1 otherwise)',
        dest='healthcheck_only',
        action='store_true',
        required=False,
        default=False,
    )

    list_parser.set_defaults(command=paasta_local_run)


def get_container_name():
    return 'paasta_local_run_%s_%s' % (get_username(), randint(1, 999999))


def get_docker_run_cmd(memory, random_port, container_name, volumes, env, interactive, docker_hash, command):
    cmd = ['docker', 'run']
    for k, v in env.iteritems():
        cmd.append('--env=\"%s=%s\"' % (k, v))
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
    if command:
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
    sys.stdout.write("\nTerminating container...\n")
    sys.stdout.write("(Please wait or you may leave an orphaned container.)\n")
    sys.stdout.flush()
    try:
        docker_client.stop(container_id)
        docker_client.remove_container(container_id)
        sys.stdout.write("...terminated\n")
    except errors.APIError:
        sys.stdout.write(PaastaColors.yellow(
            "Could not clean up container! You should stop and remove container '%s' manually.\n" % container_id))


def run_docker_container(
    docker_client,
    service,
    instance,
    docker_hash,
    volumes,
    interactive,
    command,
    healthcheck,
    healthcheck_only,
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
            "Note that some programs behave differently when running with no\n"
            "tty attached (as your program is about to run).\n\n"
        ))
    environment = service_manifest.get_env()
    memory = service_manifest.get_mem()
    random_port = pick_random_port()
    container_name = get_container_name()
    docker_run_cmd = get_docker_run_cmd(
        memory=memory,
        random_port=random_port,
        container_name=container_name,
        volumes=volumes,
        env=environment,
        interactive=interactive,
        docker_hash=docker_hash,
        command=command,
    )
    # http://stackoverflow.com/questions/4748344/whats-the-reverse-of-shlex-split
    joined_docker_run_cmd = ' '.join(pipes.quote(word) for word in docker_run_cmd)
    healthcheck_mode, healthcheck_data = get_healthcheck_for_instance(service, instance, service_manifest, random_port)

    sys.stdout.write('Running docker command:\n%s\n' % PaastaColors.grey(joined_docker_run_cmd))
    if interactive:
        sys.stdout.write(get_cmd_string())
        # NOTE: This immediately replaces us with the docker run cmd. Docker
        # run knows how to clean up the running container in this situation.
        execlp('docker', *docker_run_cmd)
        # For testing, when execlp is patched out and doesn't replace us, we
        # still want to bail out.
        return

    container_started = False
    container_id = None
    try:
        (returncode, output) = _run(joined_docker_run_cmd)
        if returncode != 0:
            sys.stdout.write(
                'Failure trying to start your container!\n'
                'Returncode: %d\n'
                'Output:\n'
                '%s\n'
                '\n'
                'Fix that problem and try again.\n'
                'http://y/paasta-troubleshooting\n'
                % (returncode, output)
            )
            # Container failed to start so no need to cleanup; just bail.
            sys.exit(1)
        container_started = True
        container_id = get_container_id(docker_client, container_name)
        sys.stdout.write('Found our container running with CID %s\n' % container_id)

        # If the service has a healthcheck, simulate it
        if healthcheck_mode:
            status = simulate_healthcheck_on_service(
                service_manifest, docker_client, container_id, healthcheck_mode, healthcheck_data, healthcheck)
        else:
            status = True
            sys.stdout.write(PaastaColors.yellow(
                'Your service does not have a healthcheck configured (it is optional, but recommended).\n'))

        if healthcheck_only:
            sys.stdout.write('Detected --healthcheck-only flag, exiting now.\n')
            if container_started:
                _cleanup_container(docker_client, container_id)
            if status:
                sys.exit(0)
            else:
                sys.exit(1)

        sys.stdout.write('Your service is now running! Tailing stdout and stderr:\n')
        for line in docker_client.attach(container_id, stderr=True, stream=True, logs=True):
            sys.stdout.write(PaastaColors.grey(line))

    except KeyboardInterrupt:
        pass

    # Cleanup if the container exits on its own or interrupted.
    if container_started:
        returncode = docker_client.inspect_container(container_id)['State']['ExitCode']
        _cleanup_container(docker_client, container_id)
    sys.exit(returncode)


def configure_and_run_docker_container(docker_client, docker_hash, service, args):
    """
    Run Docker container by image hash with args set in command line.
    Function prints the output of run command in stdout.
    """
    system_paasta_config = load_system_paasta_config()

    volumes = list()

    for volume in system_paasta_config.get_volumes():
        volumes.append('%s:%s:%s' % (volume['hostPath'], volume['containerPath'], volume['mode'].lower()))

    if args.cluster:
        cluster = args.cluster
    else:
        try:
            cluster = get_default_cluster_for_service(service)
        except NoConfigurationForServiceError:
            sys.stdout.write(PaastaColors.red(
                'Could not automatically detect cluster to emulate. Please specify one with the --cluster option.\n'))
            sys.exit(2)
        sys.stdout.write(PaastaColors.yellow(
            'Using cluster configuration for %s. To override, use the --cluster option.\n\n' % cluster))

    service_manifest = load_marathon_service_config(
        service,
        args.instance,
        cluster,
        load_deployments=False,
        soa_dir=args.soaconfig_root
    )

    if args.cmd:
        command = shlex.split(args.cmd)
    else:
        command_from_config = service_manifest.get_cmd()
        if command_from_config:
            command = shlex.split(command_from_config)
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
        args.healthcheck,
        args.healthcheck_only,
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
            sys.stdout.write(PaastaColors.grey(stream_line))

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
    paasta local_run"""
    if os.getcwd() == os.path.expanduser("~"):
        sys.stderr.write(
            'ERROR: Don\'t run this command from your home directory.\n'
            'Try changing to the root of your working copy of the service.\n'
        )
        sys.exit(1)
    if not os.path.isfile(os.path.join(os.getcwd(), 'Dockerfile')):
        sys.stderr.write(
            'ERROR: No Dockerfile in the current directory.\n'
            'Are you in the root folder of the service directory? Does a Dockerfile exist?\n'
        )
        sys.exit(1)


def paasta_local_run(args):
    validate_environment()

    service = figure_out_service_name(args, soa_dir=args.soaconfig_root)

    base_docker_url = get_docker_host()

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
