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
import base64
import datetime
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from os import execlpe
from random import randint
from typing import Any
from typing import cast
from typing import Mapping
from typing import NewType
from typing import Optional
from typing import TypedDict
from typing import Union
from urllib.parse import urlparse

import boto3
import requests
from docker import errors
from docker.api.client import APIClient
from service_configuration_lib import read_service_configuration

from paasta_tools.adhoc_tools import get_default_interactive_config
from paasta_tools.cli.authentication import get_service_auth_token
from paasta_tools.cli.authentication import get_sso_auth_token
from paasta_tools.cli.cmds.check import makefile_responds_to
from paasta_tools.cli.cmds.cook_image import paasta_cook_image
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_instances
from paasta_tools.cli.utils import pick_random_port
from paasta_tools.generate_deployments_for_service import build_docker_image_name
from paasta_tools.kubernetes_tools import get_kubernetes_secret_env_variables
from paasta_tools.kubernetes_tools import get_kubernetes_secret_volumes
from paasta_tools.kubernetes_tools import KUBE_CONFIG_USER_PATH
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.long_running_service_tools import get_healthcheck_for_instance
from paasta_tools.paasta_execute_docker_command import execute_in_container
from paasta_tools.secret_tools import decrypt_secret_environment_variables
from paasta_tools.secret_tools import decrypt_secret_volumes
from paasta_tools.secret_tools import is_secret_ref
from paasta_tools.tron_tools import parse_time_variables
from paasta_tools.utils import _run
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_docker_binary
from paasta_tools.utils import get_docker_client
from paasta_tools.utils import get_possible_launched_by_user_variable_from_env
from paasta_tools.utils import get_username
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import is_secrets_for_teams_enabled
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import Timeout
from paasta_tools.utils import TimeoutError
from paasta_tools.utils import validate_service_instance


MAX_DOCKER_HOSTNAME_LENGTH = 63

# A validated, Kubernetes-style hostname. Use _pod_style_hostname() to create.
PodHostname = NewType("PodHostname", str)

SKIP_SECRETS_HINT = (
    "If you don't need the secrets for local-run, you can add --skip-secrets"
)


class AWSSessionCreds(TypedDict):
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_SESSION_TOKEN: str
    AWS_SECURITY_TOKEN: str


class BaseEnvironment(TypedDict):
    """Environment variables that local-run always provides."""

    HOSTNAME: str
    PAASTA_DOCKER_IMAGE: str
    PAASTA_LAUNCHED_BY: str
    PAASTA_HOST: str
    PAASTA_CLUSTER: str


class InstanceRequiredEnvironment(TypedDict):
    """Required environment variables provided by instance_config.get_env()."""

    PAASTA_SERVICE: str
    PAASTA_INSTANCE: str
    PAASTA_DEPLOY_GROUP: str


class RequiredEnvironment(BaseEnvironment, InstanceRequiredEnvironment):
    """Required environment variables in the final container environment."""

    pass


BASE_ENVIRONMENT_KEYS: frozenset[str] = BaseEnvironment.__required_keys__


@dataclass(frozen=True)
class EnvLayers:
    """Container environment split into required keys vs everything else.

    TypedDicts can't express "these required keys + arbitrary other keys", so we
    carry the required mapping separately and merge only at the boundary.
    """

    required: RequiredEnvironment
    extras: dict[str, str]

    def to_dict(self) -> dict[str, str]:
        # mypy treats TypedDict unpacking/iteration as having `object` values, so
        # `{**self.required, **self.extras}` / `dict(self.required) | self.extras`
        # get inferred as `dict[str, object]`. Casting escapes the TypedDict so we
        # can merge into a normal `dict[str, str]`.
        return cast(dict[str, str], self.required) | self.extras

    def with_extras(self, patch: Mapping[str, str]) -> "EnvLayers":
        forbidden = BASE_ENVIRONMENT_KEYS.intersection(patch)
        if forbidden:
            raise ValueError(
                f"Refusing to overwrite base env keys: {', '.join(sorted(forbidden))}"
            )
        updated_extras = dict(self.extras)
        updated_extras.update(patch)
        return EnvLayers(required=self.required, extras=updated_extras)


def parse_date(date_string):
    return datetime.datetime.strptime(date_string, "%Y-%m-%d")


def perform_http_healthcheck(url, timeout):
    """Returns true if healthcheck on url succeeds, false otherwise.

    Kubernetes treats HTTP probes as success for 200 <= status < 400.
    """
    try:
        with Timeout(seconds=timeout):
            try:
                res = requests.get(url, verify=False)
            except requests.ConnectionError:
                return (False, "http request failed: connection failed")
    except TimeoutError:
        return (False, "http request timed out after %d seconds" % timeout)

    if res.status_code >= 200 and res.status_code < 400:
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
    (output, return_code) = execute_in_container(
        docker_client, container_id, command, timeout
    )
    if return_code == 0:
        return (True, output)
    else:
        return (False, output)


def run_healthcheck_on_container(
    docker_client, container_id, healthcheck_mode, healthcheck_data, timeout
):
    """Performs healthcheck on a container

    :param container_id: Docker container id
    :param healthcheck_mode: one of 'http', 'https', 'tcp', or 'cmd'
    :param healthcheck_data: a URL when healthcheck_mode is 'http[s]' or 'tcp', a command if healthcheck_mode is 'cmd'
    :param timeout: timeout in seconds for individual check
    :returns: a tuple of (bool, output string)
    """
    healthcheck_result = (False, "unknown")
    if healthcheck_mode == "cmd":
        healthcheck_result = perform_cmd_healthcheck(
            docker_client, container_id, healthcheck_data, timeout
        )
    elif healthcheck_mode == "http" or healthcheck_mode == "https":
        healthcheck_result = perform_http_healthcheck(healthcheck_data, timeout)
    elif healthcheck_mode == "tcp":
        healthcheck_result = perform_tcp_healthcheck(healthcheck_data, timeout)
    else:
        print(
            PaastaColors.yellow(
                "Healthcheck mode '%s' is not currently supported!" % healthcheck_mode
            )
        )
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
    """Simulates Kubernetes-style healthcheck locally if enabled.

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

        print(
            "\nStarting health check via %s (waiting %s seconds before "
            "considering failures due to grace period):"
            % (healthcheck_link, grace_period)
        )

        # silently start performing health checks until grace period ends or first check succeeds
        graceperiod_end_time = time.time() + grace_period
        after_grace_period_attempts = 0
        healthchecking = True

        def _stream_docker_logs(container_id, generator):
            while healthchecking:
                try:
                    # the generator will block until another log line is available
                    log_line = next(generator).decode("utf-8").rstrip("\n")
                    if healthchecking:
                        print(f"container [{container_id[:12]}]: {log_line}")
                    else:
                        # stop streaming at first opportunity, since generator.close()
                        # cant be used until the container is dead
                        break
                except StopIteration:  # natural end of logs
                    break

        docker_logs_generator = docker_client.logs(
            container_id, stderr=True, stream=True
        )
        threading.Thread(
            target=_stream_docker_logs,
            daemon=True,
            args=(container_id, docker_logs_generator),
        ).start()

        while True:
            # First inspect the container for early exits
            container_state = docker_client.inspect_container(container_id)
            if not container_state["State"]["Running"]:
                print(
                    PaastaColors.red(
                        "Container exited with code {}".format(
                            container_state["State"]["ExitCode"]
                        )
                    )
                )
                healthcheck_passed = False
                break

            healthcheck_passed, healthcheck_output = run_healthcheck_on_container(
                docker_client, container_id, healthcheck_mode, healthcheck_data, timeout
            )

            # Yay, we passed the healthcheck
            if healthcheck_passed:
                print(
                    "{}'{}' (via {})".format(
                        PaastaColors.green("Healthcheck succeeded!: "),
                        healthcheck_output,
                        healthcheck_link,
                    )
                )
                break

            # Otherwise, print why we failed
            if time.time() < graceperiod_end_time:
                color = PaastaColors.grey
                msg = "(disregarded due to grace period)"
                extra_msg = f" (via: {healthcheck_link}. Output: {healthcheck_output})"
            else:
                # If we've exceeded the grace period, we start incrementing attempts
                after_grace_period_attempts += 1
                color = PaastaColors.red
                msg = "(Attempt {} of {})".format(
                    after_grace_period_attempts, max_failures
                )
                extra_msg = f" (via: {healthcheck_link}. Output: {healthcheck_output})"

            print("{}{}".format(color(f"Healthcheck failed! {msg}"), extra_msg))

            if after_grace_period_attempts == max_failures:
                break

            time.sleep(interval)
        healthchecking = False  # end docker logs stream
    else:
        print(
            "\nPaaSTA would have healthchecked your service via\n%s" % healthcheck_link
        )
        healthcheck_passed = True
    return healthcheck_passed


def read_local_dockerfile_lines():
    dockerfile = os.path.join(os.getcwd(), "Dockerfile")
    return open(dockerfile).readlines()


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        "local-run",
        help="Run service's Docker image locally",
        description=(
            "'paasta local-run' is useful for simulating how a PaaSTA service would be "
            "executed on a real cluster. It analyzes the local soa-configs and constructs "
            "a 'docker run' invocation to match. This is useful as a type of end-to-end "
            "test, ensuring that a service will work inside the docker container as expected. "
            "Additionally, 'local-run' can healthcheck a service per the configured healthcheck.\n\n"
            "Alternatively, 'local-run' can be used with --pull, which will pull the currently "
            "deployed docker image and use it, instead of building one.\n\n"
            "While we've tried to make 'local-run' match the real PaaSTA environment, "
            "there are some differences/limitations: e.g., certain features like `boto_keys` "
            "are not supported."
        ),
        epilog=(
            "Note: 'paasta local-run' uses docker commands, which may require elevated privileges "
            "to run (sudo)."
        ),
    )
    list_parser.add_argument(
        "-s",
        "--service",
        help="The name of the service you wish to inspect",
        # strip any potential trailing / for folks tab-completing directories
        type=lambda x: x.rstrip("/"),
    ).completer = lazy_choices_completer(list_services)
    list_parser.add_argument(
        "-c",
        "--cluster",
        help=(
            "The name of the cluster you wish to simulate. "
            "If omitted, uses the default cluster defined in the paasta local-run configs"
        ),
    ).completer = lazy_choices_completer(list_clusters)
    list_parser.add_argument(
        "-y",
        "--yelpsoa-config-root",
        dest="yelpsoa_config_root",
        help="A directory from which yelpsoa-configs should be read from",
        default=DEFAULT_SOA_DIR,
    )
    build_pull_group = list_parser.add_mutually_exclusive_group()
    build_pull_group.add_argument(
        "-b",
        "--build",
        help=(
            "Build the docker image to run from scratch using the local Makefile's "
            "'cook-image' target. Defaults to try to use the local Makefile if present."
        ),
        action="store_const",
        const="build",
        dest="action",
    )
    build_pull_group.add_argument(
        "-p",
        "--pull",
        help=(
            "Pull the docker image marked for deployment from the Docker registry and "
            "use that for the local-run. This is the opposite of --build."
        ),
        action="store_const",
        const="pull",
        dest="action",
    )
    build_pull_group.add_argument(
        "-d",
        "--dry-run",
        help="Shows the arguments supplied to docker as json.",
        action="store_const",
        const="dry_run",
        dest="action",
    )
    build_pull_group.set_defaults(action="build")
    list_parser.add_argument(
        "--json-dict",
        help="When running dry run, output the arguments as a json dict",
        action="store_true",
        dest="dry_run_json_dict",
    )
    list_parser.add_argument(
        "-C",
        "--cmd",
        help=(
            "Run Docker container with particular command, "
            'for example: "bash". By default will use the command or args specified by the '
            "soa-configs or what was specified in the Dockerfile"
        ),
        required=False,
        default=None,
    )
    list_parser.add_argument(
        "-i",
        "--instance",
        help=(
            "Simulate a docker run for a particular instance of the service, like 'main' or 'canary'. "
            "NOTE: if you don't specify an instance, PaaSTA will run in interactive mode"
        ),
        required=False,
        default=None,
    ).completer = lazy_choices_completer(list_instances)
    list_parser.add_argument(
        "--date",
        default=datetime.datetime.today().strftime("%Y-%m-%d"),
        help="Date to use for interpolating date variables in a job. Defaults to use %(default)s.",
        type=parse_date,
    )
    list_parser.add_argument(
        "-v",
        "--verbose",
        help="Show Docker commands output",
        action="store_true",
        required=False,
        default=True,
    )
    list_parser.add_argument(
        "-I",
        "--interactive",
        help=(
            'Run container in interactive mode. If interactive is set the default command will be "bash" '
            'unless otherwise set by the "--cmd" flag'
        ),
        action="store_true",
        required=False,
        default=False,
    )
    list_parser.add_argument(
        "-k",
        "--no-healthcheck",
        help="Disable simulated healthcheck",
        dest="healthcheck",
        action="store_false",
        required=False,
        default=True,
    )
    list_parser.add_argument(
        "-t",
        "--healthcheck-only",
        help="Terminates container after healthcheck (exits with status code 0 on success, 1 otherwise)",
        dest="healthcheck_only",
        action="store_true",
        required=False,
        default=False,
    )
    list_parser.add_argument(
        "-o",
        "--port",
        help="Specify a port number to use. If not set, a random non-conflicting port will be found.",
        type=int,
        dest="user_port",
        required=False,
        default=False,
    )
    list_parser.add_argument(
        "--vault-auth-method",
        help="Override how we auth with vault, defaults to token if not present",
        type=str,
        dest="vault_auth_method",
        required=False,
        default="token",
        choices=["token", "ldap"],
    )
    list_parser.add_argument(
        "--vault-token-file",
        help="Override vault token file, defaults to %(default)s",
        type=str,
        dest="vault_token_file",
        required=False,
        default="/var/spool/.paasta_vault_token",
    )
    list_parser.add_argument(
        "--skip-secrets",
        help="Skip decrypting secrets, useful if running non-interactively",
        dest="skip_secrets",
        required=False,
        action="store_true",
        default=False,
    )
    list_parser.add_argument(
        "--assume-role-aws-account",
        "--aws-account",
        "-a",
        help="Specify AWS account from which to source credentials",
    )
    list_parser.add_argument(
        "--assume-role-arn",
        help=(
            "role ARN to assume before launching the service. "
            "Example format: arn:aws:iam::01234567890:role/rolename"
        ),
        type=str,
        dest="assume_role_arn",
        required=False,
        default="",
    )
    list_parser.add_argument(
        "--assume-pod-identity",
        help="If pod identity is set via yelpsoa-configs, attempt to assume it",
        action="store_true",
        dest="assume_pod_identity",
        required=False,
        default=False,
    )
    list_parser.add_argument(
        "--use-okta-role",
        help="Call aws-okta and run the service within the context of the returned credentials",
        dest="use_okta_role",
        action="store_true",
        required=False,
        default=False,
    )
    list_parser.add_argument(
        "--sha",
        help=(
            "SHA to run instead of the currently marked-for-deployment SHA. Ignored when used with --build."
            " Must be a version that exists in the registry, i.e. it has been built by Jenkins."
        ),
        type=str,
        dest="sha",
        required=False,
        default=None,
    )
    list_parser.add_argument(
        "--volume",
        dest="volumes",
        action="append",
        type=str,
        default=[],
        required=False,
        help=(
            "Same as the -v / --volume parameter to docker run: hostPath:containerPath[:mode]"
        ),
    )
    service_auth_group = list_parser.add_mutually_exclusive_group()
    service_auth_group.add_argument(
        "--use-service-auth-token",
        help=(
            "Acquire service authentication token for the underlying instance,"
            " and set it in the container environment"
        ),
        action="store_true",
        dest="use_service_auth_token",
        required=False,
        default=False,
    )
    service_auth_group.add_argument(
        "--use-sso-service-auth-token",
        help=(
            "Acquire service authentication token from SSO provider,"
            " and set it in the container environment"
        ),
        action="store_true",
        dest="use_sso_service_auth_token",
        required=False,
        default=False,
    )

    list_parser.set_defaults(command=paasta_local_run)


def get_container_name():
    return "paasta_local_run_{}_{}".format(get_username(), randint(1, 999999))


def _pod_style_hostname(candidate: str) -> PodHostname:
    """Return a validated, Kubernetes-style hostname derived from the container name.

    Raises ValueError if the candidate cannot be converted to a valid hostname.
    """
    hostname = candidate.lower()
    hostname = re.sub("[^a-z0-9-]", "-", hostname)
    hostname = hostname.strip("-")
    hostname = hostname[:MAX_DOCKER_HOSTNAME_LENGTH].rstrip("-")
    if not hostname:
        raise ValueError(
            f"Unable to derive a valid hostname from container name '{candidate}'"
        )
    return PodHostname(hostname)


def get_docker_run_cmd(
    memory: int,
    chosen_port: int,
    container_port: Optional[int],
    container_name: str,
    container_hostname: PodHostname,
    volumes: list,
    env: dict,
    interactive: bool,
    docker_hash: str,
    command: Optional[str | list],
    net: str,
    docker_params: list,
    detach: bool,
) -> list[str]:
    cmd = [get_docker_binary(), "run"]
    for k in env.keys():
        cmd.append("--env")
        cmd.append(f"{k}")
    cmd.append(f"--hostname={container_hostname}")
    cmd.append("--memory=%dm" % memory)
    for i in docker_params:
        cmd.append(f"--{i['key']}={i['value']}")
    if net == "bridge" and container_port is not None:
        cmd.append("--publish=%d:%d" % (chosen_port, container_port))
    elif net == "host":
        cmd.append("--net=host")
    cmd.append("--name=%s" % container_name)
    for volume in volumes:
        cmd.append("--volume=%s" % volume)
    if interactive:
        cmd.append("--interactive=true")
        if sys.stdin.isatty():
            cmd.append("--tty=true")
    else:
        if detach:
            cmd.append("--detach=true")
    cmd.append("%s" % docker_hash)
    if command:
        if isinstance(command, str):
            cmd.extend(("sh", "-c", command))
        else:
            cmd.extend(command)
    return cmd


class LostContainerException(Exception):
    pass


class LocalRunError(Exception):
    def __init__(self, message: str, *, hint: str | None = None) -> None:
        super().__init__(message)
        self.hint = hint


class SecretResolutionError(LocalRunError):
    pass


class AWSAssumeRoleConfigError(LocalRunError):
    pass


class DockerAuthConfig(TypedDict):
    username: str
    password: str


def get_readonly_docker_registry_auth_config(
    docker_url: str,
) -> DockerAuthConfig | None:
    system_paasta_config = load_system_paasta_config()
    config_path = system_paasta_config.get_readonly_docker_registry_auth_file()

    try:
        with open(config_path) as f:
            docker_config = json.load(f)
    except Exception:
        print(
            PaastaColors.yellow(
                "Warning: unable to load read-only docker registry credentials."
            ),
            file=sys.stderr,
        )
        # the best we can do is try to pull with whatever auth the user has configured locally
        # i.e., root-owned docker config in /root/.docker/config.json
        return None
    registry = docker_url.split("/")[0]

    # find matching auth config - our usual ro config will have at least two entries
    # at the time this comment was written
    auths = docker_config
    for auth_url, auth_data in auths.items():
        if registry in auth_url:
            # Decode the base64 auth string if present
            if "auth" in auth_data:
                auth_string = base64.b64decode(auth_data["auth"]).decode("utf-8")
                username, password = auth_string.split(":", 1)
                return {"username": username, "password": password}

    # we'll hit this for registries like docker-dev or extra-private internal registries
    return None


def docker_pull_image(docker_client: APIClient, docker_url: str) -> None:
    """Pull an image using the docker-py library with read-only registry credentials"""
    print(
        f"Please wait while the image ({docker_url}) is pulled (times out after 30m)...",
        file=sys.stderr,
    )

    auth_config = get_readonly_docker_registry_auth_config(docker_url)
    if not auth_config:
        print(
            PaastaColors.yellow(
                "Warning: No read-only docker registry credentials found, attempting to pull without them."
            ),
            file=sys.stderr,
        )

    try:
        with Timeout(
            seconds=1800,
            error_message=f"Timed out pulling docker image from {docker_url}",
        ):
            # this is slightly funky since pull() returns the output line-by-line, but as a dict
            # ...that we then need to format back to the usual `docker pull` output
            # :p
            for line in docker_client.pull(
                docker_url, auth_config=auth_config, stream=True, decode=True
            ):
                # not all lines have an 'id' key :(
                id_prefix = f"{line['id']}: " if "id" in line else ""
                print(f"{id_prefix}{line['status']}", file=sys.stderr)
    except Exception as e:
        print(
            f"\nPull failed. Error: {e}",
            file=sys.stderr,
        )
        sys.exit(1)


def get_container_id(docker_client, container_name):
    """Return the Docker container ID for the given container name."""
    containers = docker_client.containers(all=False)
    for container in containers:
        if "/%s" % container_name in container.get("Names", []):
            return container.get("Id")
    raise LostContainerException(
        "Can't find the container I just launched so I can't do anything else.\n"
        "Try docker 'ps --all | grep %s' to see where it went.\n"
        "Here were all the containers:\n"
        "%s" % (container_name, containers)
    )


def _cleanup_container(docker_client, container_id):
    if docker_client.inspect_container(container_id)["State"].get("OOMKilled", False):
        print(
            PaastaColors.red(
                "Your service was killed by the OOM Killer!\n"
                "You've exceeded the memory limit, try increasing the mem parameter in your soa_configs"
            ),
            file=sys.stderr,
        )
    print("\nStopping and removing the old container %s..." % container_id)
    print("(Please wait or you may leave an orphaned container.)")
    try:
        docker_client.stop(container_id)
        docker_client.remove_container(container_id)
        print("...done")
    except errors.APIError:
        print(
            PaastaColors.yellow(
                "Could not clean up container! You should stop and remove container '%s' manually."
                % container_id
            )
        )


def build_base_environment(
    instance_config: InstanceConfig, container_hostname: PodHostname
) -> BaseEnvironment:
    """Return env vars that local-run provides to simulate a PaaSTA container."""

    hostname = socket.getfqdn()
    docker_image = instance_config.get_docker_image()
    if docker_image == "":
        # In a local_run environment, the docker_image may not be available so
        # we can fall-back to the injected DOCKER_TAG per the paasta contract.
        docker_image = os.environ["DOCKER_TAG"]

    env: BaseEnvironment = {
        "HOSTNAME": container_hostname,
        "PAASTA_DOCKER_IMAGE": docker_image,
        "PAASTA_LAUNCHED_BY": get_possible_launched_by_user_variable_from_env(),
        "PAASTA_HOST": hostname,
        "PAASTA_CLUSTER": instance_config.get_cluster(),
    }
    return env


def build_required_environment(
    instance_config: InstanceConfig,
    container_hostname: PodHostname,
    service: str,
    instance: str,
) -> RequiredEnvironment:
    """Return the required env keys in the final container environment."""

    base_env = build_base_environment(
        instance_config=instance_config, container_hostname=container_hostname
    )

    env: RequiredEnvironment = {
        **base_env,
        "PAASTA_SERVICE": service,
        "PAASTA_INSTANCE": instance,
        "PAASTA_DEPLOY_GROUP": str(instance_config.get_deploy_group()),
    }
    return env


def build_env_layers(
    instance_config: InstanceConfig,
    container_hostname: PodHostname,
    service: str,
    instance: str,
) -> EnvLayers:
    instance_env = dict(instance_config.get_env())
    required = build_required_environment(
        instance_config=instance_config,
        container_hostname=container_hostname,
        service=service,
        instance=instance,
    )

    extras = {k: v for k, v in instance_env.items() if k not in BASE_ENVIRONMENT_KEYS}
    return EnvLayers(required=required, extras=extras)


def apply_secret_env_and_volumes(
    env: EnvLayers,
    instance_config: InstanceConfig,
    service: str,
    soa_dir: str,
    secret_provider_name: str,
    secret_provider_kwargs: Mapping[str, Any],
    skip_secrets: bool,
    assume_role_arn: str,
    assume_pod_identity: bool,
    use_okta_role: bool,
    assume_role_aws_account: Optional[str],
    use_service_auth_token: bool,
    use_sso_service_auth_token: bool,
) -> tuple[EnvLayers, dict[str, Union[str, bytes]]]:
    """Resolve secrets and other runtime-only env, returning an updated EnvLayers."""

    secret_provider_kwargs = dict(secret_provider_kwargs)
    secret_env: dict[str, str] = {}
    secret_volumes: dict[str, Union[str, bytes]] = {}

    if not skip_secrets:
        env_dict = env.to_dict()
        secret_volumes_config = instance_config.get_secret_volumes()
        needs_secret_env = any(is_secret_ref(v) for v in env_dict.values())
        needs_secret_volumes = bool(secret_volumes_config)

        if needs_secret_env or needs_secret_volumes:
            if is_secrets_for_teams_enabled(service, soa_dir):
                try:
                    kube_client = KubeClient(
                        config_file=KUBE_CONFIG_USER_PATH,
                        context=instance_config.cluster,
                    )
                    decrypted_secrets = (
                        get_kubernetes_secret_env_variables(
                            kube_client,
                            env_dict,
                            service,
                            instance_config.get_namespace(),
                        )
                        if needs_secret_env
                        else {}
                    )
                    secret_volumes = (
                        get_kubernetes_secret_volumes(
                            kube_client,
                            secret_volumes_config,
                            service,
                            instance_config.get_namespace(),
                        )
                        if needs_secret_volumes
                        else {}
                    )
                except Exception as e:
                    raise SecretResolutionError(
                        f"Failed to retrieve kubernetes secrets with {e.__class__.__name__}: {e}",
                        hint=SKIP_SECRETS_HINT,
                    ) from e
            else:
                try:
                    decrypted_secrets = (
                        decrypt_secret_environment_variables(
                            secret_provider_name=secret_provider_name,
                            environment=env_dict,
                            soa_dir=soa_dir,
                            service_name=instance_config.get_service(),
                            cluster_name=instance_config.cluster,
                            secret_provider_kwargs=secret_provider_kwargs,
                        )
                        if needs_secret_env
                        else {}
                    )
                    secret_volumes = (
                        decrypt_secret_volumes(
                            secret_provider_name=secret_provider_name,
                            secret_volumes_config=secret_volumes_config,
                            soa_dir=soa_dir,
                            service_name=instance_config.get_service(),
                            cluster_name=instance_config.cluster,
                            secret_provider_kwargs=secret_provider_kwargs,
                        )
                        if needs_secret_volumes
                        else {}
                    )
                except Exception as e:
                    raise SecretResolutionError(
                        f"Failed to decrypt secrets with {e.__class__.__name__}: {e}",
                        hint=SKIP_SECRETS_HINT,
                    ) from e
            secret_env = dict(decrypted_secrets)

    aws_env: dict[str, str] = {}
    if (
        assume_role_arn
        or assume_pod_identity
        or use_okta_role
        or "AWS_WEB_IDENTITY_TOKEN_FILE" in os.environ
    ):
        if (assume_role_arn or assume_pod_identity or use_okta_role) and (
            not assume_role_aws_account
        ):
            raise AWSAssumeRoleConfigError(
                "Error: AWS role assumption requested but no AWS account name was provided.",
                hint="Pass --assume-role-aws-account.",
            )
        aws_account = (
            assume_role_aws_account
            if assume_role_aws_account is not None
            else resolve_aws_account_from_runtimeenv()
        )
        aws_creds = assume_aws_role(
            instance_config,
            service,
            assume_role_arn,
            assume_pod_identity,
            use_okta_role,
            aws_account,
        )
        aws_env = {
            "AWS_ACCESS_KEY_ID": aws_creds["AWS_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY": aws_creds["AWS_SECRET_ACCESS_KEY"],
            "AWS_SESSION_TOKEN": aws_creds["AWS_SESSION_TOKEN"],
            "AWS_SECURITY_TOKEN": aws_creds["AWS_SECURITY_TOKEN"],
        }

    service_auth_env: dict[str, str] = {}
    if use_service_auth_token:
        service_auth_env["YELP_SVC_AUTHZ_TOKEN"] = get_service_auth_token()
    elif use_sso_service_auth_token:
        service_auth_env["YELP_SVC_AUTHZ_TOKEN"] = get_sso_auth_token()

    env_patch = {**secret_env, **aws_env, **service_auth_env}
    updated_env = env.with_extras(env_patch)
    return updated_env, secret_volumes


def check_if_port_free(port):
    temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        temp_socket.bind(("127.0.0.1", port))
    except socket.error:
        return False
    finally:
        temp_socket.close()
    return True


def resolve_aws_account_from_runtimeenv() -> str:
    try:
        with open("/nail/etc/runtimeenv") as runtimeenv_file:
            runtimeenv = runtimeenv_file.read()
    except FileNotFoundError:
        print(
            "Unable to determine environment for AWS account name. Using 'dev'",
            file=sys.stderr,
        )
        runtimeenv = "dev"

    runtimeenv_to_account_overrides = {
        "stage": "dev",
        "corp": "corpprod",
    }
    return runtimeenv_to_account_overrides.get(runtimeenv, runtimeenv)


def assume_aws_role(
    instance_config: InstanceConfig,
    service: str,
    assume_role_arn: str,
    assume_pod_identity: bool,
    use_okta_role: bool,
    aws_account: str,
) -> AWSSessionCreds:
    """Runs AWS cli to assume into the correct role, then extract and return the ENV variables from that session"""
    pod_identity = instance_config.get_iam_role()
    if assume_role_arn:
        pod_identity = assume_role_arn
    if assume_pod_identity and not pod_identity:
        print(
            f"Error: --assume-pod-identity passed but no pod identity was found for this instance ({instance_config.instance})",
            file=sys.stderr,
        )
        sys.exit(1)

    if pod_identity and (assume_pod_identity or assume_role_arn):
        print(
            "Calling aws-okta to assume role {} using account {}".format(
                pod_identity, aws_account
            )
        )
    elif use_okta_role:
        print(f"Calling aws-okta using account {aws_account}")
    elif "AWS_ROLE_ARN" in os.environ and "AWS_WEB_IDENTITY_TOKEN_FILE" in os.environ:
        # Get a session using the current pod identity
        print(
            f"Found Pod Identity token in env. Assuming into role {os.environ['AWS_ROLE_ARN']}."
        )
        boto_session = boto3.Session()
        credentials = boto_session.get_credentials()
        assumed_creds_dict: AWSSessionCreds = {
            "AWS_ACCESS_KEY_ID": credentials.access_key,
            "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
            "AWS_SESSION_TOKEN": credentials.token,
            "AWS_SECURITY_TOKEN": credentials.token,
        }
        return assumed_creds_dict
    else:
        # use_okta_role, assume_pod_identity, and assume_role are all empty, and there's no
        # pod identity (web identity token) in the env. This shouldn't happen
        print(
            "Error: assume_aws_role called without required arguments and no pod identity env",
            file=sys.stderr,
        )
        sys.exit(1)
    # local-run will sometimes run as root - make sure that we get the actual
    # users AWS credentials instead of looking for non-existent root AWS
    # credentials
    if os.getuid() == 0:
        aws_okta_cmd = [
            "sudo",
            "-u",
            get_username(),
            f"HOME=/nail/home/{get_username()}",
            "aws-okta",
            "-a",
            aws_account,
            "-o",
            "json",
        ]
    else:
        aws_okta_cmd = ["aws-okta", "-a", aws_account, "-o", "json"]
    cmd = subprocess.run(aws_okta_cmd, stdout=subprocess.PIPE)
    if cmd.returncode != 0:
        print(
            "Error calling aws-okta. Remove --assume-pod-identity to run without pod identity role",
            file=sys.stderr,
        )
        sys.exit(1)
    cmd_output = json.loads(cmd.stdout.decode("utf-8"))

    if not use_okta_role:
        boto_session = boto3.Session(
            aws_access_key_id=cmd_output["AccessKeyId"],
            aws_secret_access_key=cmd_output["SecretAccessKey"],
            aws_session_token=cmd_output["SessionToken"],
        )
        sts_client = boto_session.client("sts")
        assumed_role = sts_client.assume_role(
            RoleArn=pod_identity, RoleSessionName=f"{get_username()}-local-run"
        )
        # The contents of "Credentials" key from assume_role is the same as from aws-okta
        cmd_output = assumed_role["Credentials"]

    creds_dict: AWSSessionCreds = {
        "AWS_ACCESS_KEY_ID": cmd_output["AccessKeyId"],
        "AWS_SECRET_ACCESS_KEY": cmd_output["SecretAccessKey"],
        "AWS_SESSION_TOKEN": cmd_output["SessionToken"],
        "AWS_SECURITY_TOKEN": cmd_output["SessionToken"],
    }
    return creds_dict


def run_docker_container(
    docker_client,
    service,
    instance,
    docker_url,
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
    assume_pod_identity=False,
    assume_role_arn="",
    use_okta_role=False,
    assume_role_aws_account: Optional[str] = None,
    use_service_auth_token: bool = False,
    use_sso_service_auth_token: bool = False,
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
            print(
                PaastaColors.red(
                    "The chosen port is already in use!\n"
                    "Try specifying another one, or omit (--port|-o) and paasta will find a free one for you"
                ),
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        chosen_port = pick_random_port(service)

    container_name = get_container_name()
    container_hostname = _pod_style_hostname(container_name)

    healthcheck_mode, healthcheck_data = get_healthcheck_for_instance(
        service, instance, instance_config, chosen_port, soa_dir=soa_dir
    )
    if healthcheck_only and healthcheck_mode is None:
        print("--healthcheck-only, but no healthcheck is defined for this instance!")
        sys.exit(1)

    env = build_env_layers(
        instance_config=instance_config,
        container_hostname=container_hostname,
        service=service,
        instance=instance,
    )
    env_after_secrets, secret_volumes = apply_secret_env_and_volumes(
        instance_config=instance_config,
        env=env,
        service=service,
        soa_dir=soa_dir,
        secret_provider_name=secret_provider_name,
        secret_provider_kwargs=secret_provider_kwargs,
        skip_secrets=skip_secrets,
        assume_role_arn=assume_role_arn,
        assume_pod_identity=assume_pod_identity,
        use_okta_role=use_okta_role,
        assume_role_aws_account=assume_role_aws_account,
        use_service_auth_token=use_service_auth_token,
        use_sso_service_auth_token=use_sso_service_auth_token,
    )
    environment = env_after_secrets.to_dict()
    net = instance_config.get_net()
    memory = instance_config.get_mem()
    docker_params = instance_config.format_docker_parameters()

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

    simulate_healthcheck = (
        healthcheck_only or healthcheck
    ) and healthcheck_mode is not None

    for container_mount_path, secret_content in secret_volumes.items():
        temp_secret_folder = tempfile.mktemp(dir=os.environ.get("TMPDIR", "/nail/tmp"))
        os.makedirs(temp_secret_folder, exist_ok=True)
        temp_secret_filename = os.path.join(temp_secret_folder, str(uuid.uuid4()))
        # write the secret contents
        # Permissions will automatically be set to readable by "users" group
        # TODO: Make this readable only by "nobody" user? What about other non-standard users that people sometimes use inside the container?
        # -rw-r--r-- 1 dpopes users 3.2K Nov 28 19:16 854bdbad-30b8-4681-ae4e-854cb28075c5
        try:
            # First try to write the file as a string
            # This is for text like config files
            with open(temp_secret_filename, "w") as f:
                f.write(secret_content)  # type: ignore  # TODO: make this type-safe rather than rely on exceptions
        except TypeError:
            # If that fails, try to write it as bytes
            # This is for binary files like TLS keys
            with open(temp_secret_filename, "wb") as fb:
                fb.write(secret_content)  # type: ignore  # TODO: make this type-safe rather than rely on exceptions

        # Append this to the list of volumes passed to docker run
        volumes.append(f"{temp_secret_filename}:{container_mount_path}:ro")

    docker_run_args = dict(
        memory=memory,
        chosen_port=chosen_port,
        container_port=container_port,
        container_name=container_name,
        container_hostname=container_hostname,
        volumes=volumes,
        env=environment,
        interactive=interactive,
        detach=simulate_healthcheck,
        docker_hash=docker_url,
        command=command,
        net=net,
        docker_params=docker_params,
    )
    docker_run_cmd = get_docker_run_cmd(**docker_run_args)
    joined_docker_run_cmd = " ".join(docker_run_cmd)

    if dry_run:
        if json_dict:
            print(json.dumps(docker_run_args))
        else:
            print(json.dumps(docker_run_cmd))
        return 0
    else:
        print("Running docker command:\n%s" % PaastaColors.grey(joined_docker_run_cmd))

    merged_env = {**os.environ, **environment}

    if interactive or not simulate_healthcheck:
        execlpe(get_docker_binary(), *docker_run_cmd, merged_env)
        # For testing, when execlpe is patched out and doesn't replace us, we
        # still want to bail out.
        return 0

    container_started = False
    container_id = None
    try:
        (returncode, output) = _run(docker_run_cmd, env=merged_env)
        if returncode != 0:
            print(
                "Failure trying to start your container!"
                "Returncode: %d"
                "Output:"
                "%s"
                ""
                "Fix that problem and try again."
                "http://y/paasta-troubleshooting" % (returncode, output),
                sep="\n",
            )
            # Container failed to start so no need to cleanup; just bail.
            sys.exit(1)
        container_started = True
        container_id = get_container_id(docker_client, container_name)
        print("Found our container running with CID %s" % container_id)

        if simulate_healthcheck:
            healthcheck_result = simulate_healthcheck_on_service(
                instance_config=instance_config,
                docker_client=docker_client,
                container_id=container_id,
                healthcheck_mode=healthcheck_mode,
                healthcheck_data=healthcheck_data,
                healthcheck_enabled=healthcheck,
            )

        def _output_exit_code():
            returncode = docker_client.inspect_container(container_id)["State"][
                "ExitCode"
            ]
            print(f"Container exited: {returncode}")

        if healthcheck_only:
            if container_started:
                _output_exit_code()
                _cleanup_container(docker_client, container_id)
            if healthcheck_result is True:
                sys.exit(0)
            else:
                sys.exit(1)

        running = docker_client.inspect_container(container_id)["State"]["Running"]
        if running:
            print("Your service is now running! Tailing stdout and stderr:")
            for line in docker_client.logs(
                container_id,
                stderr=True,
                stream=True,
            ):
                # writing to sys.stdout.buffer lets us write the raw bytes we
                # get from the docker client without having to convert them to
                # a utf-8 string
                sys.stdout.buffer.write(line)
                sys.stdout.flush()
        else:
            _output_exit_code()
            returncode = 3

    except KeyboardInterrupt:
        returncode = 3

    # Cleanup if the container exits on its own or interrupted.
    if container_started:
        returncode = docker_client.inspect_container(container_id)["State"]["ExitCode"]
        _cleanup_container(docker_client, container_id)
    return returncode


def format_command_for_type(command, instance_type, date):
    """Return the command formatted appropriately for the instance type."""
    if instance_type == "tron":
        interpolated_command = parse_time_variables(command, date)
        return interpolated_command
    else:
        return command


def configure_and_run_docker_container(
    docker_client,
    docker_url,
    docker_sha,
    service,
    instance,
    cluster,
    system_paasta_config,
    args,
    assume_role_aws_account,
    pull_image=False,
    dry_run=False,
):

    if instance is None and args.healthcheck_only:
        print("With --healthcheck-only, --instance MUST be provided!", file=sys.stderr)
        return 1
    if instance is None and not sys.stdin.isatty():
        print(
            "--instance and --cluster must be specified when using paasta local-run without a tty!",
            file=sys.stderr,
        )
        return 1

    soa_dir = args.yelpsoa_config_root
    volumes = args.volumes
    load_deployments = (docker_url is None or pull_image) and not docker_sha
    interactive = args.interactive

    try:
        if instance is None:
            instance_type = "adhoc"
            instance = "interactive"
            instance_config = get_default_interactive_config(
                service=service,
                cluster=cluster,
                soa_dir=soa_dir,
                load_deployments=load_deployments,
            )
            interactive = True
        else:
            instance_type = validate_service_instance(
                service, instance, cluster, soa_dir
            )
            instance_config = get_instance_config(
                service=service,
                instance=instance,
                cluster=cluster,
                load_deployments=load_deployments,
                soa_dir=soa_dir,
            )
    except NoConfigurationForServiceError as e:
        print(str(e), file=sys.stderr)
        return 1
    except NoDeploymentsAvailable:
        print(
            PaastaColors.red(
                "Error: No deployments.json found in %(soa_dir)s/%(service)s. "
                "You can generate this by running: "
                "generate_deployments_for_service -d %(soa_dir)s -s %(service)s"
                % {"soa_dir": soa_dir, "service": service}
            ),
            sep="\n",
            file=sys.stderr,
        )
        return 1

    if docker_sha is not None:
        instance_config.branch_dict = {
            "git_sha": docker_sha,
            "docker_image": build_docker_image_name(service=service, sha=docker_sha),
            "desired_state": "start",
            "force_bounce": None,
        }

    if docker_url is None:
        try:
            docker_url = instance_config.get_docker_url()
        except NoDockerImageError:
            if instance_config.get_deploy_group() is None:
                print(
                    PaastaColors.red(
                        f"Error: {service}.{instance} has no 'deploy_group' set. Please set one so "
                        "the proper image can be used to run for this service."
                    ),
                    sep="",
                    file=sys.stderr,
                )
            else:
                print(
                    PaastaColors.red(
                        "Error: No sha has been marked for deployment for the %s deploy group.\n"
                        "Please ensure this service has either run through a jenkins pipeline "
                        "or paasta mark-for-deployment has been run for %s\n"
                        % (instance_config.get_deploy_group(), service)
                    ),
                    sep="",
                    file=sys.stderr,
                )
            return 1

    if pull_image:
        docker_pull_image(docker_client, docker_url)

    for volume in instance_config.get_volumes(
        system_paasta_config.get_volumes(),
    ):
        if os.path.exists(volume["hostPath"]):
            volumes.append(
                "{}:{}:{}".format(
                    volume["hostPath"], volume["containerPath"], volume["mode"].lower()
                )
            )
        else:
            print(
                PaastaColors.yellow(
                    "Warning: Path %s does not exist on this host. Skipping this binding."
                    % volume["hostPath"]
                ),
                file=sys.stderr,
            )

    if interactive is True and args.cmd is None:
        command = "bash"
    elif args.cmd:
        command = args.cmd
    else:
        command_from_config = instance_config.get_cmd()
        if command_from_config:
            command = format_command_for_type(
                command=command_from_config, instance_type=instance_type, date=args.date
            )
        else:
            command = instance_config.get_args()

    secret_provider_kwargs = {
        "vault_cluster_config": system_paasta_config.get_vault_cluster_config(),
        "vault_auth_method": args.vault_auth_method,
        "vault_token_file": args.vault_token_file,
    }

    return run_docker_container(
        docker_client=docker_client,
        service=service,
        instance=instance,
        docker_url=docker_url,
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
        assume_pod_identity=args.assume_pod_identity,
        assume_role_arn=args.assume_role_arn,
        assume_role_aws_account=assume_role_aws_account,
        use_okta_role=args.use_okta_role,
        use_service_auth_token=args.use_service_auth_token,
        use_sso_service_auth_token=args.use_sso_service_auth_token,
    )


def should_reexec_as_root(
    service: str, skip_secrets: bool, action: str, soa_dir: str = DEFAULT_SOA_DIR
) -> bool:
    # local-run can't pull secrets from Vault in prod without a root-owned token
    need_vault_token = not skip_secrets and action == "pull"

    # there are some special teams with their own private docker registries and no ro creds
    # however, we don't know what registry is to be used without loading the service config
    service_info = read_service_configuration(service, soa_dir)
    # technically folks can set the standard registry as a value here, but atm no one is doing that :p
    registry_override = service_info.get("docker_registry")
    # note: we could also have a list of registries that have ro creds, but this seems fine for now
    uses_private_registry = (
        registry_override
        and registry_override
        in load_system_paasta_config().get_private_docker_registries()
    )
    need_docker_config = uses_private_registry and action == "pull"

    return (need_vault_token or need_docker_config) and os.geteuid() != 0


def paasta_local_run(args):
    service = figure_out_service_name(args, soa_dir=args.yelpsoa_config_root)
    if should_reexec_as_root(
        service, args.skip_secrets, args.action, args.yelpsoa_config_root
    ):
        # XXX: we should re-architect this to not need sudo, but for now,
        # re-exec ourselves with sudo to get access to the paasta vault token
        # NOTE: once we do that, we can also remove the venv check above :)
        print(
            "Re-executing paasta local-run --pull with sudo for Vault/Docker registry access..."
        )
        os.execvp("sudo", ["sudo", "-H", "/usr/bin/paasta"] + sys.argv[1:])
    if args.action == "build" and not makefile_responds_to("cook-image"):
        print(
            "A local Makefile with a 'cook-image' target is required for --build",
            file=sys.stderr,
        )
        print(
            "If you meant to pull the docker image from the registry, explicitly pass --pull",
            file=sys.stderr,
        )
        return 1

    try:
        system_paasta_config = load_system_paasta_config()
    except PaastaNotConfiguredError:
        print(
            PaastaColors.yellow(
                "Warning: Couldn't load config files from '/etc/paasta'. This indicates"
                "PaaSTA is not configured locally on this host, and local-run may not behave"
                "the same way it would behave on a server configured for PaaSTA."
            ),
            sep="\n",
        )
        system_paasta_config = SystemPaastaConfig({"volumes": []}, "/etc/paasta")

    local_run_config = system_paasta_config.get_local_run_config()

    if args.cluster:
        cluster = args.cluster
    else:
        try:
            cluster = local_run_config["default_cluster"]
        except KeyError:
            print(
                PaastaColors.red(
                    "PaaSTA on this machine has not been configured with a default cluster."
                    "Please pass one to local-run using '-c'."
                ),
                sep="\n",
                file=sys.stderr,
            )
            return 1
    assume_role_aws_account = args.assume_role_aws_account or (
        system_paasta_config.get_kube_clusters()
        .get(cluster, {})
        .get("aws_account", resolve_aws_account_from_runtimeenv())
    )

    instance = args.instance
    docker_client = get_docker_client()

    docker_sha = None
    docker_url = None

    if args.action == "build":
        default_tag = "paasta-local-run-{}-{}".format(service, get_username())
        docker_url = os.environ.get("DOCKER_TAG", default_tag)
        os.environ["DOCKER_TAG"] = docker_url
        pull_image = False
        cook_return = paasta_cook_image(
            args=None, service=service, soa_dir=args.yelpsoa_config_root
        )
        if cook_return != 0:
            return cook_return
    elif args.action == "dry_run":
        pull_image = False
        docker_url = None
        docker_sha = args.sha
    else:
        pull_image = True
        docker_url = None
        docker_sha = args.sha

    try:
        return configure_and_run_docker_container(
            docker_client=docker_client,
            docker_url=docker_url,
            docker_sha=docker_sha,
            service=service,
            instance=instance,
            cluster=cluster,
            args=args,
            pull_image=pull_image,
            system_paasta_config=system_paasta_config,
            dry_run=args.action == "dry_run",
            assume_role_aws_account=assume_role_aws_account,
        )
    except LocalRunError as e:
        print(str(e), file=sys.stderr)
        if e.hint:
            print(e.hint, file=sys.stderr)
        return 1
    except errors.APIError as e:
        print("Can't run Docker container. Error: %s" % str(e), file=sys.stderr)
        return 1
