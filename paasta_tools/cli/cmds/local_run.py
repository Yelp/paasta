#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
from paasta_tools.chronos_tools import load_chronos_job_config
from paasta_tools.marathon_tools import CONTAINER_PORT
from paasta_tools.marathon_tools import get_healthcheck_for_instance
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.paasta_execute_docker_command import execute_in_container
from paasta_tools.cli.cmds.cook_image import paasta_cook_image
from paasta_tools.cli.cmds.check import makefile_responds_to
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import guess_instance
from paasta_tools.cli.utils import guess_cluster
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_instances
from paasta_tools.cli.utils import list_services
from paasta_tools.utils import get_docker_host
from paasta_tools.utils import get_docker_url
from paasta_tools.utils import get_username
from paasta_tools.utils import list_clusters
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import _run
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import Timeout
from paasta_tools.utils import TimeoutError
from paasta_tools.utils import validate_service_instance

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
    instance_config,
    docker_client,
    container_id,
    healthcheck_mode,
    healthcheck_data,
    healthcheck_enabled
):
    """Simulates Marathon-style healthcheck on given service if healthcheck is enabled

    :param instance_config: service manifest
    :param docker_client: Docker client object
    :param container_id: Docker container id
    :param healthcheck_data: tuple url to healthcheck
    :param healthcheck_enabled: boolean
    :returns: if healthcheck_enabled is true, then returns output of healthcheck, otherwise simply returns true
    """
    healthcheck_link = PaastaColors.cyan(healthcheck_data)
    if healthcheck_enabled:
        grace_period = instance_config.get_healthcheck_grace_period_seconds()
        timeout = instance_config.get_healthcheck_timeout_seconds()
        interval = instance_config.get_healthcheck_interval_seconds()
        max_failures = instance_config.get_healthcheck_max_consecutive_failures()

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


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'local-run',
        help="Run service's Docker image locally",
        description=(
            "'paasta local-run' is useful for simulating how a PaaSTA service would be "
            "executed on a real cluster. It analyzes the local soa-configs and constructs "
            "a 'docker run' invocation to match. This is useful as a type of end-to-end "
            "test, ensuring that a service will work inside the docker container as expected. "
            "Additionally, 'local-run' can healthcheck a service per the configured healthcheck.\n\n"
            "Alternatively, 'local-run' can be used with --pull, which will pull the currently "
            "deployed docker image and use it, instead of building one."
        ),
        epilog=(
            "Note: 'paasta local-run' uses docker commands, which may require elevated privileges "
            "to run (sudo)."
        ),
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
        '-y', '--yelpsoa-config-root',
        dest='yelpsoa_config_root',
        help='A directory from which yelpsoa-configs should be read from',
        default=service_configuration_lib.DEFAULT_SOA_DIR,
    )
    build_pull_group = list_parser.add_mutually_exclusive_group()
    build_pull_group.add_argument(
        '-b', '--build',
        help=(
            "Build the docker image to run from scratch using the local Makefile's "
            "'cook-image' target. Defaults to try to use the local Makefile if present. "
            "otherwise local-run will pull and run the Docker image that is marked for "
            "deployment in the Docker registry. Mutually exclusive with '--pull'."
        ),
        required=False,
        action='store_true',
        default=None,
    )
    build_pull_group.add_argument(
        '-p', '--pull',
        help=(
            "Pull the docker image marked for deployment from the Docker registry and "
            "use that for the local-run. This is the opposite of --build. Defaults to "
            "autodetect a Makefile, if present will not pull, and instead assume that "
            "a local build is desired. Mutally exclusive with '--build'"
        ),
        required=False,
        action='store_true',
        default=None,
    )
    list_parser.add_argument(
        '-C', '--cmd',
        help=('Run Docker container with particular command, '
              'for example: "bash". By default will use the command or args specified by the '
              'soa-configs or what was specified in the Dockefile'),
        required=False,
        default=None,
    )
    list_parser.add_argument(
        '-i', '--instance',
        help='Simulate a docker run for a particular instance of the service, like "main" or "canary"',
        required=False,
        default=None,
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
        help=('Run container in interactive mode. If interactive is set the default command will be "bash" '
              'unless otherwise set by the "--cmd" flag'),
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


def docker_pull_image(docker_url):
    """Pull an image via ``docker pull``. Uses the actual pull command instead of the python
    bindings due to the docker auth/registry transition. Once we are past Docker 1.6
    we can use better credential management, but for now this function assumes the
    user running the command has already been authorized for the registry"""
    sys.stderr.write("Please wait while the image (%s) is pulled...\n" % docker_url)
    DEVNULL = open(os.devnull, 'wb')
    ret, output = _run('docker pull %s' % docker_url, stream=True, stdin=DEVNULL)
    if ret != 0:
        sys.stderr.write("\nPull failed. Are you authorized to run docker commands?\n")
        sys.exit(ret)


def get_container_id(docker_client, container_name):
    """Use 'docker_client' to find the container we started, identifiable by
    its 'container_name'. If we can't find the id, raise
    LostContainerException.
    """
    containers = docker_client.containers(all=False)
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
    sys.stdout.write("\nStopping and removing the old container %s...\n" % container_id)
    sys.stdout.write("(Please wait or you may leave an orphaned container.)\n")
    sys.stdout.flush()
    try:
        docker_client.stop(container_id)
        docker_client.remove_container(container_id)
        sys.stdout.write("...done\n")
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
    instance_config
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
    environment = instance_config.get_unformatted_env()
    memory = instance_config.get_mem()
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
    healthcheck_mode, healthcheck_data = get_healthcheck_for_instance(service, instance, instance_config, random_port)

    sys.stdout.write('Running docker command:\n%s\n' % PaastaColors.grey(joined_docker_run_cmd))
    if interactive:
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
                instance_config, docker_client, container_id, healthcheck_mode, healthcheck_data, healthcheck)
        else:
            status = True

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
        returncode = 3
        pass

    # Cleanup if the container exits on its own or interrupted.
    if container_started:
        returncode = docker_client.inspect_container(container_id)['State']['ExitCode']
        _cleanup_container(docker_client, container_id)
    sys.exit(returncode)


def get_instance_config(service, instance, cluster, soa_dir, load_deployments=False):
    """ Returns the InstanceConfig object for whatever type of instance
    it is. (chronos or marathon) """
    instance_type = validate_service_instance(
        service=service,
        instance=instance,
        cluster=cluster,
        soa_dir=soa_dir,
    )
    if instance_type == 'marathon':
        instance_config_load_function = load_marathon_service_config
    elif instance_type == 'chronos':
        instance_config_load_function = load_chronos_job_config
    else:
        raise NotImplementedError(
            "instance is %s of type %s which is not supported by local-run"
            % (instance, instance_type)
        )
    return instance_config_load_function(
        service=service,
        instance=instance,
        cluster=cluster,
        load_deployments=load_deployments,
        soa_dir=soa_dir
    )


def configure_and_run_docker_container(docker_client, docker_hash, service, instance, cluster, args, pull_image=False):
    """
    Run Docker container by image hash with args set in command line.
    Function prints the output of run command in stdout.
    """
    try:
        system_paasta_config = load_system_paasta_config()
    except PaastaNotConfiguredError:
        sys.stdout.write(PaastaColors.yellow(
            "Warning: Couldn't load config files from '/etc/paasta'. This indicates\n"
            "PaaSTA is not configured locally on this host, and local-run may not behave\n"
            "the same way it would behave on a server configured for PaaSTA.\n"
        ))
        system_paasta_config = SystemPaastaConfig({"volumes": []}, '/etc/paasta')

    volumes = list()
    instance_config = get_instance_config(
        service=service,
        instance=instance,
        cluster=cluster,
        load_deployments=pull_image,
        soa_dir=args.yelpsoa_config_root,
    )

    if pull_image:
        docker_url = get_docker_url(
            system_paasta_config.get_docker_registry(), instance_config.get_docker_image())
        docker_pull_image(docker_url)

        docker_hash = docker_url

    # if only one volume specified, extra_volumes should be converted to a list
    extra_volumes = instance_config.get_extra_volumes()
    if type(extra_volumes) == dict:
        extra_volumes = [extra_volumes]

    for volume in system_paasta_config.get_volumes() + extra_volumes:
        volumes.append('%s:%s:%s' % (volume['hostPath'], volume['containerPath'], volume['mode'].lower()))

    if args.interactive is True and args.cmd is None:
        command = ['bash']
    elif args.cmd:
        command = shlex.split(args.cmd)
    else:
        command_from_config = instance_config.get_cmd()
        if command_from_config:
            command = shlex.split(command_from_config)
        else:
            command = instance_config.get_args()

    run_docker_container(
        docker_client=docker_client,
        service=service,
        instance=args.instance,
        docker_hash=docker_hash,
        volumes=volumes,
        interactive=args.interactive,
        command=command,
        healthcheck=args.healthcheck,
        healthcheck_only=args.healthcheck_only,
        instance_config=instance_config,
    )


def local_makefile_present():
    if makefile_responds_to('cook-image'):
        sys.stderr.write("Local Makefile with 'cook-image' target deteced. Assuming --build\n")
        return True
    else:
        sys.stderr.write("No Makefile with 'cook-image' target detected. Assuming --pull\n")
        return False


def paasta_local_run(args):
    if args.pull:
        build = False
    elif args.build:
        build = True
    else:
        build = local_makefile_present()

    service = figure_out_service_name(args, soa_dir=args.yelpsoa_config_root)
    cluster = guess_cluster(service=service, args=args)
    instance = guess_instance(service=service, cluster=cluster, args=args)
    base_docker_url = get_docker_host()
    docker_client = Client(base_url=base_docker_url)

    if build:
        default_tag = 'paasta-local-run-%s-%s' % (service, get_username())
        tag = os.environ.get('DOCKER_TAG', default_tag)
        os.environ['DOCKER_TAG'] = tag
        pull_image = False
        paasta_cook_image(args=None, service=service, soa_dir=args.yelpsoa_config_root)
    else:
        pull_image = True
        tag = None

    try:
        configure_and_run_docker_container(
            docker_client=docker_client,
            docker_hash=tag,
            service=service,
            instance=instance,
            cluster=cluster,
            args=args,
            pull_image=pull_image,
        )
    except errors.APIError as e:
        sys.stderr.write('Can\'t run Docker container. Error: %s\n' % str(e))
        sys.exit(1)
