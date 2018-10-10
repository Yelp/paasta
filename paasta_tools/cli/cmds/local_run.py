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
import os
import socket
import sys
import time
import uuid
from os import execlpe
from random import randint
from urllib.parse import urlparse

import requests
from docker import errors

from paasta_tools.adhoc_tools import get_default_interactive_config
from paasta_tools.chronos_tools import parse_time_variables
from paasta_tools.cli.cmds.check import makefile_responds_to
from paasta_tools.cli.cmds.cook_image import paasta_cook_image
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_instances
from paasta_tools.cli.utils import pick_random_port
from paasta_tools.long_running_service_tools import get_healthcheck_for_instance
from paasta_tools.paasta_execute_docker_command import execute_in_container
from paasta_tools.secret_tools import get_secret_provider
from paasta_tools.secret_tools import is_secret_ref
from paasta_tools.utils import _run
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_docker_client
from paasta_tools.utils import get_username
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import timed_flock
from paasta_tools.utils import Timeout
from paasta_tools.utils import TimeoutError
from paasta_tools.utils import validate_service_instance


def parse_date(date_string):
    return datetime.datetime.strptime(date_string, "%Y-%m-%d")


def perform_http_healthcheck(url, timeout):
    """Returns true if healthcheck on url succeeds, false otherwise

    :param url: the healthcheck url
    :param timeout: timeout in seconds
    :returns: True if healthcheck succeeds within number of seconds specified by timeout, false otherwise
    """
    try:
        with Timeout(seconds=timeout):
            try:
                res = requests.get(url, verify=False)
            except requests.ConnectionError:
                return (False, "http request failed: connection failed")
    except TimeoutError:
        return (False, "http request timed out after %d seconds" % timeout)

    if 'content-type' in res.headers and ',' in res.headers['content-type']:
        paasta_print(PaastaColors.yellow(
            "Multiple content-type headers detected in response."
            " The Mesos healthcheck system will treat this as a failure!",
        ))
        return (False, "http request succeeded, code %d" % res.status_code)
    # check if response code is valid per https://mesosphere.github.io/marathon/docs/health-checks.html
    elif res.status_code >= 200 and res.status_code < 400:
        return (True, "http request succeeded, code %d" % res.status_code)
    else:
        return (False, "http request failed, code %s" % str(res.status_code))


def perform_tcp_healthcheck(url, timeout):
    """Returns true if successfully connects to host and port, false otherwise

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
    timeout,
):
    """Performs healthcheck on a container

    :param container_id: Docker container id
    :param healthcheck_mode: one of 'http', 'https', 'tcp', or 'cmd'
    :param healthcheck_data: a URL when healthcheck_mode is 'http[s]' or 'tcp', a command if healthcheck_mode is 'cmd'
    :param timeout: timeout in seconds for individual check
    :returns: a tuple of (bool, output string)
    """
    healthcheck_result = (False, "unknown")
    if healthcheck_mode == 'cmd':
        healthcheck_result = perform_cmd_healthcheck(docker_client, container_id, healthcheck_data, timeout)
    elif healthcheck_mode == 'http' or healthcheck_mode == 'https':
        healthcheck_result = perform_http_healthcheck(healthcheck_data, timeout)
    elif healthcheck_mode == 'tcp':
        healthcheck_result = perform_tcp_healthcheck(healthcheck_data, timeout)
    else:
        paasta_print(PaastaColors.yellow(
            "Healthcheck mode '%s' is not currently supported!" % healthcheck_mode,
        ))
        sys.exit(1)
    return healthcheck_result


def simulate_healthcheck_on_service(
    instance_config,
    docker_client,
    container_id,
    healthcheck_mode,
    healthcheck_data,
    healthcheck_enabled,
):
    """Simulates Marathon-style healthcheck on given service if healthcheck is enabled

    :param instance_config: service manifest
    :param docker_client: Docker client object
    :param container_id: Docker container id
    :param healthcheck_data: tuple url to healthcheck
    :param healthcheck_enabled: boolean
    :returns: healthcheck_passed: boolean
    """
    healthcheck_link = PaastaColors.cyan(healthcheck_data)
    if healthcheck_enabled:
        grace_period = instance_config.get_healthcheck_grace_period_seconds()
        timeout = instance_config.get_healthcheck_timeout_seconds()
        interval = instance_config.get_healthcheck_interval_seconds()
        max_failures = instance_config.get_healthcheck_max_consecutive_failures()

        paasta_print('\nStarting health check via %s (waiting %s seconds before '
                     'considering failures due to grace period):' % (healthcheck_link, grace_period))

        # silently start performing health checks until grace period ends or first check succeeds
        graceperiod_end_time = time.time() + grace_period
        after_grace_period_attempts = 0
        while True:
            # First inspect the container for early exits
            container_state = docker_client.inspect_container(container_id)
            if not container_state['State']['Running']:
                paasta_print(
                    PaastaColors.red('Container exited with code {}'.format(
                        container_state['State']['ExitCode'],
                    )),
                )
                healthcheck_passed = False
                break

            healthcheck_passed, healthcheck_output = run_healthcheck_on_container(
                docker_client, container_id, healthcheck_mode, healthcheck_data, timeout,
            )

            # Yay, we passed the healthcheck
            if healthcheck_passed:
                paasta_print("{}'{}' (via {})".format(
                    PaastaColors.green("Healthcheck succeeded!: "),
                    healthcheck_output,
                    healthcheck_link,
                ))
                break

            # Otherwise, print why we failed
            if time.time() < graceperiod_end_time:
                color = PaastaColors.grey
                msg = '(disregarded due to grace period)'
                extra_msg = f' (via: {healthcheck_link}. Output: {healthcheck_output})'
            else:
                # If we've exceeded the grace period, we start incrementing attempts
                after_grace_period_attempts += 1
                color = PaastaColors.red
                msg = '(Attempt {} of {})'.format(
                    after_grace_period_attempts, max_failures,
                )
                extra_msg = f' (via: {healthcheck_link}. Output: {healthcheck_output})'

            paasta_print('{}{}'.format(
                color(f'Healthcheck failed! {msg}'),
                extra_msg,
            ))

            if after_grace_period_attempts == max_failures:
                break

            time.sleep(interval)
    else:
        paasta_print('\nPaaSTA would have healthchecked your service via\n%s' % healthcheck_link)
        healthcheck_passed = True
    return healthcheck_passed


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
        help=(
            "The name of the cluster you wish to simulate. "
            "If omitted, uses the default cluster defined in the paasta local-run configs"
        ),
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
            "'cook-image' target. Defaults to try to use the local Makefile if present."
        ),
        action='store_const',
        const='build',
        dest='action',
    )
    build_pull_group.add_argument(
        '-p', '--pull',
        help=(
            "Pull the docker image marked for deployment from the Docker registry and "
            "use that for the local-run. This is the opposite of --build."
        ),
        action='store_const',
        const='pull',
        dest='action',
    )
    build_pull_group.add_argument(
        '-d', '--dry-run',
        help='Shows the arguments supplied to docker as json.',
        action='store_const',
        const='dry_run',
        dest='action',
    )
    build_pull_group.set_defaults(action='build')
    list_parser.add_argument(
        '--json-dict',
        help='When running dry run, output the arguments as a json dict',
        action='store_true',
        dest='dry_run_json_dict',
    )
    list_parser.add_argument(
        '-C', '--cmd',
        help=(
            'Run Docker container with particular command, '
            'for example: "bash". By default will use the command or args specified by the '
            'soa-configs or what was specified in the Dockerfile'
        ),
        required=False,
        default=None,
    )
    list_parser.add_argument(
        '-i', '--instance',
        help=(
            "Simulate a docker run for a particular instance of the service, like 'main' or 'canary'"
            "NOTE: if you don't specify an instance, PaaSTA will run in interactive mode"
        ),
        required=False,
        default=None,
    ).completer = lazy_choices_completer(list_instances)
    list_parser.add_argument(
        '--date',
        default=datetime.datetime.today().strftime('%Y-%m-%d'),
        help="Date to use for interpolating date variables in a job. Defaults to use %(default)s.",
        type=parse_date,
    )
    list_parser.add_argument(
        '-v', '--verbose',
        help='Show Docker commands output',
        action='store_true',
        required=False,
        default=True,
    )
    list_parser.add_argument(
        '-I', '--interactive',
        help=(
            'Run container in interactive mode. If interactive is set the default command will be "bash" '
            'unless otherwise set by the "--cmd" flag'
        ),
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
        '-o', '--port',
        help='Specify a port number to use. If not set, a random non-conflicting port will be found.',
        type=int,
        dest='user_port',
        required=False,
        default=False,
    )
    list_parser.add_argument(
        '--vault-auth-method',
        help='Override how we auth with vault, defaults to token if not present',
        type=str,
        dest='vault_auth_method',
        required=False,
        default='token',
        choices=['token', 'ldap'],
    )
    list_parser.add_argument(
        '--vault-token-file',
        help='Override vault token file, defaults to /root/.vault-token',
        type=str,
        dest='vault_token_file',
        required=False,
        default='/var/spool/.paasta_vault_token',
    )
    list_parser.add_argument(
        '--skip-secrets',
        help='Skip decrypting secrets, useful if running non-interactively',
        dest='skip_secrets',
        required=False,
        action='store_true',
        default=False,
    )
    list_parser.set_defaults(command=paasta_local_run)


def get_container_name():
    return 'paasta_local_run_{}_{}'.format(get_username(), randint(1, 999999))


def get_docker_run_cmd(
    memory, chosen_port, container_port, container_name, volumes, env, interactive,
    docker_hash, command, net, docker_params, detach,
):
    cmd = ['paasta_docker_wrapper', 'run']
    for k in env.keys():
        cmd.append('--env')
        cmd.append(f'{k}')
    cmd.append('--memory=%dm' % memory)
    for i in docker_params:
        cmd.append(f"--{i['key']}={i['value']}")
    if net == 'bridge' and container_port is not None:
        cmd.append('--publish=%d:%d' % (chosen_port, container_port))
    elif net == 'host':
        cmd.append('--net=host')
    cmd.append('--name=%s' % container_name)
    for volume in volumes:
        cmd.append('--volume=%s' % volume)
    if interactive:
        cmd.append('--interactive=true')
        if sys.stdout.isatty():
            cmd.append('--tty=true')
    else:
        if detach:
            cmd.append('--detach=true')
    cmd.append('%s' % docker_hash)
    if command:
        if isinstance(command, str):
            cmd.extend(('sh', '-c', command))
        else:
            cmd.extend(command)
    return cmd


class LostContainerException(Exception):
    pass


def docker_pull_image(docker_url):
    """Pull an image via ``docker pull``. Uses the actual pull command instead of the python
    bindings due to the docker auth/registry transition. Once we are past Docker 1.6
    we can use better credential management, but for now this function assumes the
    user running the command has already been authorized for the registry"""
    paasta_print("Please wait while the image (%s) is pulled (times out after 30m)..." % docker_url, file=sys.stderr)
    DEVNULL = open(os.devnull, 'wb')
    with open('/tmp/paasta-local-run-pull.lock', 'w') as f:
        with timed_flock(f, seconds=1800):
            ret, output = _run('docker pull %s' % docker_url, stream=True, stdin=DEVNULL)
            if ret != 0:
                paasta_print(
                    "\nPull failed. Are you authorized to run docker commands?",
                    file=sys.stderr,
                )
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
        "%s" % (container_name, containers),
    )


def _cleanup_container(docker_client, container_id):
    if docker_client.inspect_container(container_id)['State'].get('OOMKilled', False):
        paasta_print(
            PaastaColors.red(
                "Your service was killed by the OOM Killer!\n"
                "You've exceeded the memory limit, try increasing the mem parameter in your soa_configs",
            ),
            file=sys.stderr,
        )
    paasta_print("\nStopping and removing the old container %s..." % container_id)
    paasta_print("(Please wait or you may leave an orphaned container.)")
    try:
        docker_client.stop(container_id)
        docker_client.remove_container(container_id)
        paasta_print("...done")
    except errors.APIError:
        paasta_print(PaastaColors.yellow(
            "Could not clean up container! You should stop and remove container '%s' manually." % container_id,
        ))


def get_local_run_environment_vars(instance_config, port0, framework):
    """Returns a dictionary of environment variables to simulate what would be available to
    a paasta service running in a container"""
    hostname = socket.getfqdn()
    docker_image = instance_config.get_docker_image()
    if docker_image == '':
        # In a local_run environment, the docker_image may not be available
        # so we can fall-back to the injected DOCKER_TAG per the paasta contract
        docker_image = os.environ['DOCKER_TAG']
    fake_taskid = uuid.uuid4()
    env = {
        'HOST': hostname,
        'MESOS_SANDBOX': '/mnt/mesos/sandbox',
        'MESOS_CONTAINER_NAME': 'localrun-%s' % fake_taskid,
        'MESOS_TASK_ID': str(fake_taskid),
        'PAASTA_DOCKER_IMAGE': docker_image,
    }
    if framework == 'marathon':
        env['MARATHON_PORT'] = str(port0)
        env['MARATHON_PORT0'] = str(port0)
        env['MARATHON_PORTS'] = str(port0)
        env['MARATHON_PORT_%d' % instance_config.get_container_port()] = str(port0)
        env['MARATHON_APP_VERSION'] = 'simulated_marathon_app_version'
        env['MARATHON_APP_RESOURCE_CPUS'] = str(instance_config.get_cpus())
        env['MARATHON_APP_DOCKER_IMAGE'] = docker_image
        env['MARATHON_APP_RESOURCE_MEM'] = str(instance_config.get_mem())
        env['MARATHON_APP_RESOURCE_DISK'] = str(instance_config.get_disk())
        env['MARATHON_APP_LABELS'] = ""
        env['MARATHON_APP_ID'] = '/simulated_marathon_app_id'
        env['MARATHON_HOST'] = hostname
    elif framework == 'chronos':
        env['CHRONOS_RESOURCE_DISK'] = str(instance_config.get_disk())
        env['CHRONOS_RESOURCE_CPU'] = str(instance_config.get_cpus())
        env['CHRONOS_RESOURCE_MEM'] = str(instance_config.get_mem())
        env['CHRONOS_JOB_OWNER'] = 'simulated-owner'
        env['CHRONOS_JOB_RUN_TIME'] = str(int(time.time()))
        env['CHRONOS_JOB_NAME'] = "{} {}".format(instance_config.get_service(), instance_config.get_instance())
        env['CHRONOS_JOB_RUN_ATTEMPT'] = str(0)
        env['mesos_task_id'] = 'ct:simulated-task-id'
    return env


def check_if_port_free(port):
    temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        temp_socket.bind(("127.0.0.1", port))
    except socket.error:
        return False
    finally:
        temp_socket.close()
    return True


def decrypt_secret_environment_variables(
    secret_provider_name,
    environment,
    soa_dir,
    service_name,
    cluster_name,
    secret_provider_kwargs,
):
    secret_environment = {}
    secret_env_vars = {k: v for k, v in environment.items() if is_secret_ref(v)}
    if secret_env_vars:
        secret_provider = get_secret_provider(
            secret_provider_name=secret_provider_name,
            soa_dir=soa_dir,
            service_name=service_name,
            cluster_names=[cluster_name],
            secret_provider_kwargs=secret_provider_kwargs,
        )
        try:
            secret_environment = secret_provider.decrypt_environment(
                secret_env_vars,
            )
        except Exception as e:
            paasta_print(f"Failed to retrieve secrets with {e.__class__.__name__}: {e}")
            paasta_print("If you don't need the secrets for local-run, you can add --skip-secrets")
            sys.exit(1)
    return secret_environment


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
    user_port,
    instance_config,
    secret_provider_name,
    soa_dir=DEFAULT_SOA_DIR,
    dry_run=False,
    json_dict=False,
    framework=None,
    secret_provider_kwargs={},
    skip_secrets=False,
):
    """docker-py has issues running a container with a TTY attached, so for
    consistency we execute 'docker run' directly in both interactive and
    non-interactive modes.

    In non-interactive mode when the run is complete, stop the container and
    remove it (with docker-py).
    """
    if user_port:
        if check_if_port_free(user_port):
            chosen_port = user_port
        else:
            paasta_print(
                PaastaColors.red(
                    "The chosen port is already in use!\n"
                    "Try specifying another one, or omit (--port|-o) and paasta will find a free one for you",
                ),
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        chosen_port = pick_random_port(service)
    environment = instance_config.get_env_dictionary()
    if not skip_secrets:
        secret_environment = decrypt_secret_environment_variables(
            secret_provider_name=secret_provider_name,
            environment=environment,
            soa_dir=soa_dir,
            service_name=service,
            cluster_name=instance_config.cluster,
            secret_provider_kwargs=secret_provider_kwargs,
        )
        environment.update(secret_environment)
    local_run_environment = get_local_run_environment_vars(
        instance_config=instance_config,
        port0=chosen_port,
        framework=framework,
    )
    environment.update(local_run_environment)
    net = instance_config.get_net()
    memory = instance_config.get_mem()
    container_name = get_container_name()
    docker_params = instance_config.format_docker_parameters()

    healthcheck_mode, healthcheck_data = get_healthcheck_for_instance(
        service, instance, instance_config, chosen_port, soa_dir=soa_dir,
    )
    if healthcheck_mode is None:
        container_port = None
        interactive = True
    elif not user_port and not healthcheck and not healthcheck_only:
        container_port = None
    else:
        try:
            container_port = instance_config.get_container_port()
        except AttributeError:
            container_port = None

    simulate_healthcheck = (healthcheck_only or healthcheck) and healthcheck_mode is not None

    docker_run_args = dict(
        memory=memory,
        chosen_port=chosen_port,
        container_port=container_port,
        container_name=container_name,
        volumes=volumes,
        env=environment,
        interactive=interactive,
        detach=simulate_healthcheck,
        docker_hash=docker_hash,
        command=command,
        net=net,
        docker_params=docker_params,
    )
    docker_run_cmd = get_docker_run_cmd(**docker_run_args)
    joined_docker_run_cmd = ' '.join(docker_run_cmd)

    if dry_run:
        if json_dict:
            paasta_print(json.dumps(docker_run_args))
        else:
            paasta_print(json.dumps(docker_run_cmd))
        return 0
    else:
        paasta_print('Running docker command:\n%s' % PaastaColors.grey(joined_docker_run_cmd))

    merged_env = {**os.environ, **environment}

    if interactive or not simulate_healthcheck:
        # NOTE: This immediately replaces us with the docker run cmd. Docker
        # run knows how to clean up the running container in this situation.
        execlpe('paasta_docker_wrapper', *docker_run_cmd, merged_env)
        # For testing, when execlpe is patched out and doesn't replace us, we
        # still want to bail out.
        return 0

    container_started = False
    container_id = None
    try:
        (returncode, output) = _run(docker_run_cmd, env=merged_env)
        if returncode != 0:
            paasta_print(
                'Failure trying to start your container!'
                'Returncode: %d'
                'Output:'
                '%s'
                ''
                'Fix that problem and try again.'
                'http://y/paasta-troubleshooting'
                % (returncode, output),
                sep='\n',
            )
            # Container failed to start so no need to cleanup; just bail.
            sys.exit(1)
        container_started = True
        container_id = get_container_id(docker_client, container_name)
        paasta_print('Found our container running with CID %s' % container_id)

        if simulate_healthcheck:
            healthcheck_result = simulate_healthcheck_on_service(
                instance_config=instance_config,
                docker_client=docker_client,
                container_id=container_id,
                healthcheck_mode=healthcheck_mode,
                healthcheck_data=healthcheck_data,
                healthcheck_enabled=healthcheck,
            )

        def _output_stdout_and_exit_code():
            returncode = docker_client.inspect_container(container_id)['State']['ExitCode']
            paasta_print('Container exited: %d)' % returncode)
            paasta_print('Here is the stdout and stderr:\n\n')
            paasta_print(
                docker_client.attach(container_id, stderr=True, stream=False, logs=True),
            )

        if healthcheck_only:
            if container_started:
                _output_stdout_and_exit_code()
                _cleanup_container(docker_client, container_id)
            if healthcheck_mode is None:
                paasta_print('--healthcheck-only, but no healthcheck is defined for this instance!')
                sys.exit(1)
            elif healthcheck_result is True:
                sys.exit(0)
            else:
                sys.exit(1)

        running = docker_client.inspect_container(container_id)['State']['Running']
        if running:
            paasta_print('Your service is now running! Tailing stdout and stderr:')
            for line in docker_client.attach(container_id, stderr=True, stream=True, logs=True):
                paasta_print(line)
        else:
            _output_stdout_and_exit_code()
            returncode = 3

    except KeyboardInterrupt:
        returncode = 3

    # Cleanup if the container exits on its own or interrupted.
    if container_started:
        returncode = docker_client.inspect_container(container_id)['State']['ExitCode']
        _cleanup_container(docker_client, container_id)
    return returncode


def command_function_for_framework(framework, date):
    """
    Given a framework, return a function that appropriately formats
    the command to be run.
    """
    def format_marathon_command(cmd):
        return cmd

    def format_chronos_command(cmd):
        interpolated_command = parse_time_variables(cmd, date)
        return interpolated_command

    def format_tron_command(cmd: str) -> str:
        interpolated_command = parse_time_variables(cmd, date)
        return interpolated_command

    def format_adhoc_command(cmd):
        return cmd

    if framework == 'chronos':
        return format_chronos_command
    elif framework == 'marathon':
        return format_marathon_command
    elif framework == 'adhoc':
        return format_adhoc_command
    elif framework == 'tron':
        return format_tron_command
    else:
        raise ValueError("Invalid Framework")


def configure_and_run_docker_container(
        docker_client,
        docker_hash,
        service,
        instance,
        cluster,
        system_paasta_config,
        args,
        pull_image=False,
        dry_run=False,
):
    """
    Run Docker container by image hash with args set in command line.
    Function prints the output of run command in stdout.
    """

    if instance is None and args.healthcheck_only:
        paasta_print(
            "With --healthcheck-only, --instance MUST be provided!",
            file=sys.stderr,
        )
        return 1
    if instance is None and not sys.stdin.isatty():
        paasta_print(
            "--instance and --cluster must be specified when using paasta local-run without a tty!",
            file=sys.stderr,
        )
        return 1

    soa_dir = args.yelpsoa_config_root
    volumes = list()
    load_deployments = docker_hash is None or pull_image
    interactive = args.interactive

    try:
        if instance is None:
            instance_type = 'adhoc'
            instance = 'interactive'
            instance_config = get_default_interactive_config(
                service=service,
                cluster=cluster,
                soa_dir=soa_dir,
                load_deployments=load_deployments,
            )
            interactive = True
        else:
            instance_type = validate_service_instance(service, instance, cluster, soa_dir)
            instance_config = get_instance_config(
                service=service,
                instance=instance,
                cluster=cluster,
                load_deployments=load_deployments,
                soa_dir=soa_dir,
            )
    except NoConfigurationForServiceError as e:
        paasta_print(str(e), file=sys.stderr)
        return 1
    except NoDeploymentsAvailable:
        paasta_print(
            PaastaColors.red(
                "Error: No deployments.json found in %(soa_dir)s/%(service)s."
                "You can generate this by running:"
                "generate_deployments_for_service -d %(soa_dir)s -s %(service)s" % {
                    'soa_dir': soa_dir,
                    'service': service,
                },
            ),
            sep='\n',
            file=sys.stderr,
        )
        return 1

    if docker_hash is None:
        try:
            docker_url = instance_config.get_docker_url()
        except NoDockerImageError:
            if instance_config.get_deploy_group() is None:
                paasta_print(
                    PaastaColors.red(
                        f"Error: {service}.{instance} has no 'deploy_group' set. Please set one so "
                        "the proper image can be used to run for this service.",
                    ),
                    sep='',
                    file=sys.stderr,
                )
            else:
                paasta_print(
                    PaastaColors.red(
                        "Error: No sha has been marked for deployment for the %s deploy group.\n"
                        "Please ensure this service has either run through a jenkins pipeline "
                        "or paasta mark-for-deployment has been run for %s\n" % (
                            instance_config.get_deploy_group(), service,
                        ),
                    ),
                    sep='',
                    file=sys.stderr,
                )
            return 1
        docker_hash = docker_url

    if pull_image:
        docker_pull_image(docker_url)

    for volume in instance_config.get_volumes(system_paasta_config.get_volumes()):
        if os.path.exists(volume['hostPath']):
            volumes.append('{}:{}:{}'.format(volume['hostPath'], volume['containerPath'], volume['mode'].lower()))
        else:
            paasta_print(
                PaastaColors.yellow(
                    "Warning: Path %s does not exist on this host. Skipping this binding." % volume['hostPath'],
                ),
            )

    if interactive is True and args.cmd is None:
        command = 'bash'
    elif args.cmd:
        command = args.cmd
    else:
        command_from_config = instance_config.get_cmd()
        if command_from_config:
            command_modifier = command_function_for_framework(instance_type, args.date)
            command = command_modifier(command_from_config)
        else:
            command = instance_config.get_args()

    secret_provider_kwargs = {
        'vault_cluster_config': system_paasta_config.get_vault_cluster_config(),
        'vault_auth_method': args.vault_auth_method,
        'vault_token_file': args.vault_token_file,
    }

    return run_docker_container(
        docker_client=docker_client,
        service=service,
        instance=instance,
        docker_hash=docker_hash,
        volumes=volumes,
        interactive=interactive,
        command=command,
        healthcheck=args.healthcheck,
        healthcheck_only=args.healthcheck_only,
        user_port=args.user_port,
        instance_config=instance_config,
        soa_dir=args.yelpsoa_config_root,
        dry_run=dry_run,
        json_dict=args.dry_run_json_dict,
        framework=instance_type,
        secret_provider_name=system_paasta_config.get_secret_provider_name(),
        secret_provider_kwargs=secret_provider_kwargs,
        skip_secrets=args.skip_secrets,
    )


def docker_config_available():
    home = os.path.expanduser('~')
    oldconfig = os.path.join(home, ".dockercfg")
    newconfig = os.path.join(home, ".docker", "config.json")
    return (os.path.isfile(oldconfig) and os.access(oldconfig, os.R_OK)) or \
        (os.path.isfile(newconfig) and os.access(newconfig, os.R_OK))


def paasta_local_run(args):
    if args.action == 'pull' and os.geteuid() != 0 and not docker_config_available():
        paasta_print("Re-executing paasta local-run --pull with sudo..")
        os.execvp("sudo", ["sudo", "-H"] + sys.argv)
    if args.action == 'build' and not makefile_responds_to('cook-image'):
        paasta_print("A local Makefile with a 'cook-image' target is required for --build", file=sys.stderr)
        paasta_print("If you meant to pull the docker image from the registry, explicitly pass --pull", file=sys.stderr)
        return 1

    try:
        system_paasta_config = load_system_paasta_config()
    except PaastaNotConfiguredError:
        paasta_print(
            PaastaColors.yellow(
                "Warning: Couldn't load config files from '/etc/paasta'. This indicates"
                "PaaSTA is not configured locally on this host, and local-run may not behave"
                "the same way it would behave on a server configured for PaaSTA.",
            ),
            sep='\n',
        )
        system_paasta_config = SystemPaastaConfig({"volumes": []}, '/etc/paasta')

    local_run_config = system_paasta_config.get_local_run_config()

    service = figure_out_service_name(args, soa_dir=args.yelpsoa_config_root)
    if args.cluster:
        cluster = args.cluster
    else:
        try:
            cluster = local_run_config['default_cluster']
        except KeyError:
            paasta_print(
                PaastaColors.red(
                    "PaaSTA on this machine has not been configured with a default cluster."
                    "Please pass one to local-run using '-c'.",
                ),
                sep='\n',
                file=sys.stderr,
            )
            return 1
    instance = args.instance
    docker_client = get_docker_client()

    if args.action == 'build':
        default_tag = 'paasta-local-run-{}-{}'.format(service, get_username())
        tag = os.environ.get('DOCKER_TAG', default_tag)
        os.environ['DOCKER_TAG'] = tag
        pull_image = False
        cook_return = paasta_cook_image(args=None, service=service, soa_dir=args.yelpsoa_config_root)
        if cook_return != 0:
            return cook_return
    elif args.action == 'dry_run':
        pull_image = False
        tag = None
    else:
        pull_image = True
        tag = None

    try:
        return configure_and_run_docker_container(
            docker_client=docker_client,
            docker_hash=tag,
            service=service,
            instance=instance,
            cluster=cluster,
            args=args,
            pull_image=pull_image,
            system_paasta_config=system_paasta_config,
            dry_run=args.action == 'dry_run',
        )
    except errors.APIError as e:
        paasta_print(
            'Can\'t run Docker container. Error: %s' % str(e),
            file=sys.stderr,
        )
        return 1
