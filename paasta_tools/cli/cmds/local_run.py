#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
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
import datetime
import json
import math
import os
import pipes
import shlex
import socket
import sys
import time
from os import execlp
from random import randint
from urlparse import urlparse

import requests
from docker import errors

from paasta_tools.chronos_tools import parse_time_variables
from paasta_tools.cli.cmds.check import makefile_responds_to
from paasta_tools.cli.cmds.cook_image import paasta_cook_image
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.cli.utils import guess_cluster
from paasta_tools.cli.utils import guess_instance
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_instances
from paasta_tools.cli.utils import list_services
from paasta_tools.marathon_tools import CONTAINER_PORT
from paasta_tools.marathon_tools import get_healthcheck_for_instance
from paasta_tools.paasta_execute_docker_command import execute_in_container
from paasta_tools.utils import _run
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_docker_client
from paasta_tools.utils import get_docker_url
from paasta_tools.utils import get_username
from paasta_tools.utils import list_clusters
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import Timeout
from paasta_tools.utils import TimeoutError
from paasta_tools.utils import validate_service_instance


def pick_random_port():
    """Bind to an ephemeral port, force it into the TIME_WAIT state, and
    unbind it.

    This means that further ephemeral port alloctions won't pick this
    "reserved" port, but subprocesses can still bind to it explicitly, given
    that they use SO_REUSEADDR.

    By default on linux you have a grace period of 60 seconds to reuse this
    port.

    To check your own particular value:
    $ cat /proc/sys/net/ipv4/tcp_fin_timeout
    60
    """
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', 0))
    s.listen(0)

    sockname = s.getsockname()

    # these three are necessary just to get the port into a TIME_WAIT state
    s2 = socket.socket()
    s2.connect(sockname)
    s.accept()

    return sockname[1]


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
                return (False, "http request failed: connection failed")
    except TimeoutError:
        return (False, "http request timed out after %d seconds" % timeout)

    if 'content-type' in res.headers and ',' in res.headers['content-type']:
        sys.stdout.write(PaastaColors.yellow(
            "Multiple content-type headers detected in response."
            " The Mesos healthcheck system will treat this as a failure!"))
        return (False, "http request succeeded, code %d" % res.status_code)
    # check if response code is valid per https://mesosphere.github.io/marathon/docs/health-checks.html
    elif res.status_code >= 200 and res.status_code < 400:
        return (True, "http request succeeded, code %d" % res.status_code)
    elif res.status_code >= 400:
        return (False, "http request failed, code %d" % res.status_code)


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
        return (True, "tcp connection succeeded")
    else:
        return (False, "%s (timeout %d seconds)" % (os.strerror(result), timeout))


def perform_cmd_healthcheck(docker_client, container_id, command, timeout):
    """Returns true if return code of command is 0 when executed inside container, false otherwise

    :param docker_client: Docker client object
    :param container_id: Docker container id
    :param command: command to execute
    :param timeout: timeout in seconds
    :returns: True if command exits with return code 0, false otherwise
    """
    (output, return_code) = execute_in_container(docker_client, container_id, command, timeout)
    if return_code == 0:
        return (True, output)
    else:
        return (False, output)


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
    :returns: a tuple of (bool, output string)
    """
    healthcheck_result = (False, "unknown")
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
    :returns: a 2-tuple of (healthcheck_passed_bool, healthcheck_output_string)
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
        after_grace_period_attempts = 0
        while True:
            # First inspect the container for early exits
            container_state = docker_client.inspect_container(container_id)
            if not container_state['State']['Running']:
                sys.stdout.write(
                    PaastaColors.red('Container exited with code {}'.format(
                        container_state['State']['ExitCode'],
                    )) + '\n'
                )
                healthcheck_result = (False, "Aborted by the user")
                break

            healthcheck_result = run_healthcheck_on_container(
                docker_client, container_id, healthcheck_mode, healthcheck_data, timeout,
            )

            # Yay, we passed the healthcheck
            if healthcheck_result[0]:
                sys.stdout.write("{}'{}' (via {})\n".format(
                    PaastaColors.green("Healthcheck succeeded!: "),
                    healthcheck_result[1],
                    healthcheck_link,
                ))
                break

            # Otherwise, print why we failed
            if time.time() < graceperiod_end_time:
                color = PaastaColors.grey
                msg = '(disregarded due to grace period)'
                extra_msg = ''
            else:
                # If we've exceeded the grace period, we start incrementing attempts
                after_grace_period_attempts += 1
                color = PaastaColors.red
                msg = '(Attempt {} of {})'.format(
                    after_grace_period_attempts, max_failures,
                )
                extra_msg = ' (via: {})'.format(healthcheck_link)

            sys.stdout.write('{}{}\n'.format(
                color('Healthcheck failed! {}'.format(msg)),
                extra_msg,
            ))

            if after_grace_period_attempts == max_failures:
                break

            time.sleep(interval)
    else:
        sys.stdout.write('\nMesos would have healthchecked your service via\n%s\n' % healthcheck_link)
        healthcheck_result = (True, "No healthcheck enabled")
    return healthcheck_result


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
        default=DEFAULT_SOA_DIR,
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
              'soa-configs or what was specified in the Dockerfile'),
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
    list_parser.add_argument(
        '-d', '--dry-run',
        help='Shows the arguments in json format supplied to docker',
        action='store_true',
    )
    list_parser.set_defaults(command=paasta_local_run)


def get_container_name():
    return 'paasta_local_run_%s_%s' % (get_username(), randint(1, 999999))


def get_docker_run_cmd(memory, random_port, container_name, volumes, env, interactive,
                       docker_hash, command, hostname, net):
    cmd = ['docker', 'run']
    for k, v in env.iteritems():
        cmd.append('--env=\"%s=%s\"' % (k, v))
    cmd.append('--env=MARATHON_PORT=%s' % random_port)
    cmd.append('--env=HOST=%s' % hostname)
    cmd.append('--env=MESOS_SANDBOX=/mnt/mesos/sandbox')
    cmd.append('--memory=%dm' % memory)
    cmd.append('--memory-swap=%dm' % int(math.ceil(memory)))
    if net == 'bridge':
        cmd.append('--publish=%d:%d' % (random_port, CONTAINER_PORT))
    elif net == 'host':
        cmd.append('--net=host')
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
        cmd.extend((
            'sh', '-c',
            ' '.join(pipes.quote(part) for part in command)
        ))
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
    if docker_client.inspect_container(container_id)['State'].get('OOMKilled', False):
        sys.stderr.write(PaastaColors.red("Your service was killed by the OOM Killer!\n"))
        sys.stderr.write(PaastaColors.red(
            "You've exceeded the memory limit, try increasing the mem parameter in your soa_configs\n"))
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
    hostname,
    healthcheck,
    healthcheck_only,
    instance_config,
    soa_dir=DEFAULT_SOA_DIR,
    dry_run=False
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
    environment = instance_config.get_env_dictionary()
    net = instance_config.get_net()
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
        hostname=hostname,
        net=net,
    )
    # http://stackoverflow.com/questions/4748344/whats-the-reverse-of-shlex-split
    joined_docker_run_cmd = ' '.join(pipes.quote(word) for word in docker_run_cmd)
    healthcheck_mode, healthcheck_data = get_healthcheck_for_instance(
        service, instance, instance_config, random_port, soa_dir=soa_dir)

    if dry_run:
        sys.stdout.write(json.dumps(docker_run_cmd) + '\n')
        sys.exit(0)
    else:
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
            status, _ = simulate_healthcheck_on_service(
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

        running = docker_client.inspect_container(container_id)['State']['Running']
        if running:
            sys.stdout.write('Your service is now running! Tailing stdout and stderr:\n')
            for line in docker_client.attach(container_id, stderr=True, stream=True, logs=True):
                sys.stdout.write(PaastaColors.grey(line))
        else:
            sys.stdout.write('Your service failed to start, here is the stdout and stderr\n')
            sys.stdout.write(PaastaColors.grey(
                docker_client.attach(container_id, stderr=True, stream=False, logs=True)
            ))
            raise KeyboardInterrupt

    except KeyboardInterrupt:
        returncode = 3

    # Cleanup if the container exits on its own or interrupted.
    if container_started:
        returncode = docker_client.inspect_container(container_id)['State']['ExitCode']
        _cleanup_container(docker_client, container_id)
    sys.exit(returncode)


def command_function_for_framework(framework):
    """
    Given a framework, return a function that appropriately formats
    the command to be run.
    """
    def format_marathon_command(cmd):
        return cmd

    def format_chronos_command(cmd):
        interpolated_command = parse_time_variables(cmd, datetime.datetime.now())
        return interpolated_command

    if framework == 'chronos':
        return format_chronos_command
    elif framework == 'marathon':
        return format_marathon_command
    else:
        raise ValueError("Invalid Framework")


def configure_and_run_docker_container(
        docker_client,
        docker_hash,
        service,
        instance,
        cluster,
        args,
        pull_image=False,
        dry_run=False
):
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

    soa_dir = args.yelpsoa_config_root

    volumes = list()

    instance_type = validate_service_instance(service, instance, cluster, soa_dir)

    try:
        instance_config = get_instance_config(
            service=service,
            instance=instance,
            cluster=cluster,
            load_deployments=pull_image,
            soa_dir=soa_dir,
        )
    except NoDeploymentsAvailable:
        sys.stderr.write(PaastaColors.red(
            "Error: No deployments.json found in %(soa_dir)s/%(service)s.\n"
            "You can generate this by running:\n"
            "generate_deployments_for_service -d %(soa_dir)s -s %(service)s\n" % {
                'soa_dir': soa_dir, 'service': service}))
        return

    if pull_image:
        try:
            docker_url = get_docker_url(
                system_paasta_config.get_docker_registry(), instance_config.get_docker_image())
        except NoDockerImageError:
            sys.stderr.write(PaastaColors.red(
                "Error: No sha has been marked for deployment for the %s deploy group.\n"
                "Please ensure this service has either run through a jenkins pipeline "
                "or paasta mark-for-deployment has been run for %s" % (instance_config.get_deploy_group(), service)))
            return
        docker_hash = docker_url
        docker_pull_image(docker_url)

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
            command_modifier = command_function_for_framework(instance_type)
            command = shlex.split(command_modifier(command_from_config))
        else:
            command = instance_config.get_args()

    hostname = socket.getfqdn()

    run_docker_container(
        docker_client=docker_client,
        service=service,
        instance=instance,
        docker_hash=docker_hash,
        volumes=volumes,
        interactive=args.interactive,
        command=command,
        hostname=hostname,
        healthcheck=args.healthcheck,
        healthcheck_only=args.healthcheck_only,
        instance_config=instance_config,
        soa_dir=args.yelpsoa_config_root,
        dry_run=dry_run,
    )


def local_makefile_present():
    if makefile_responds_to('cook-image'):
        sys.stderr.write("Local Makefile with 'cook-image' target detected. Assuming --build\n")
        return True
    else:
        sys.stderr.write("No Makefile with 'cook-image' target detected. Assuming --pull\n")
        return False


def paasta_local_run(args):
    if args.pull or args.dry_run:
        build = False
    elif args.build:
        build = True
    else:
        build = local_makefile_present()

    service = figure_out_service_name(args, soa_dir=args.yelpsoa_config_root)
    cluster = guess_cluster(service=service, args=args)
    instance = guess_instance(service=service, cluster=cluster, args=args)
    docker_client = get_docker_client()

    if build:
        default_tag = 'paasta-local-run-%s-%s' % (service, get_username())
        tag = os.environ.get('DOCKER_TAG', default_tag)
        os.environ['DOCKER_TAG'] = tag
        pull_image = False
        cook_return = paasta_cook_image(args=None, service=service, soa_dir=args.yelpsoa_config_root)
        if cook_return != 0:
            return cook_return
    elif args.dry_run:
        pull_image = False
        tag = None
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
            dry_run=args.dry_run,
        )
    except errors.APIError as e:
        sys.stderr.write('Can\'t run Docker container. Error: %s\n' % str(e))
        return 1
