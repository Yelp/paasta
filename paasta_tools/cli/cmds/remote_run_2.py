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
import json
import os
import pty
import sys
import time

from paasta_tools.api.client import get_paasta_oapi_client
from paasta_tools.cli.cmds.check import makefile_responds_to
from paasta_tools.cli.cmds.cook_image import paasta_cook_image
from paasta_tools.cli.utils import get_paasta_oapi_api_clustername
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_username
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import SystemPaastaConfig


def add_common_args_to_parser(parser):
    parser.add_argument(
        "-s",
        "--service",
        help="The name of the service you wish to inspect. Required.",
        required=True,
    ).completer = lazy_choices_completer(list_services)
    parser.add_argument(
        "-i",
        "--instance",
        help=(
            "Simulate a docker run for a particular instance of the "
            "service, like 'main' or 'canary'. Required."
        ),
        required=True,
    )
    parser.add_argument(
        "-c",
        "--cluster",
        help=(
            "The name of the cluster you wish to run your task on. "
            "If omitted, uses the default cluster defined in the paasta "
            f"remote-run configs."
        ),
    )


def add_subparser(
    subparsers,
) -> None:
    remote_run_parser = subparsers.add_parser(
        "remote-run-2",
        help="Run stuff remotely.",
        description=("'paasta remote-run' runs stuff remotely "),
    )
    subparsers = remote_run_parser.add_subparsers(dest="remote_run_command")
    start_parser = subparsers.add_parser(
        "start",
        help="Start or connect to a remote-run job",
        description=("Starts or connects to a remote-run-job"),
    )
    start_parser.add_argument(
        "-b",
        "--build",
        dest="build",
        help="Build the image from current directory",
        action="store_true",
    )
    start_parser.add_argument(
        "-y",
        "--yelpsoa-config-root",
        dest="yelpsoa_config_root",
        help="A directory from which yelpsoa-configs should be read from",
        default=DEFAULT_SOA_DIR,
    )
    start_parser.add_argument(
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
    stop_parser = subparsers.add_parser(
        "stop",
        help="Stop! In the name of Paasta",
        description="Stop your remote-run job if it exists",
    )
    add_common_args_to_parser(start_parser)
    add_common_args_to_parser(stop_parser)
    remote_run_parser.set_defaults(command=remote_run)


def paasta_remote_run_start(
    args,
    system_paasta_config: SystemPaastaConfig,
    verbose: int = 0,
    is_eks: bool = True,
) -> int:

    cluster = args.cluster
    service = args.service
    instance = args.instance
    build = args.build
    output = []
    ret_code = 0

    # TODO: Build
    if build and not makefile_responds_to("cook-image"):
        print(
            "A local Makefile with a 'cook-image' target is required for --build",
            file=sys.stderr,
        )
        default_tag = "paasta-remote-run-{}-{}".format(service, get_username())
        os.environ["DOCKER_TAG"] = default_tag
        paasta_cook_image(args=None, service=service, soa_dir=soa_dir)
        # TODO Actually push the image

    client = get_paasta_oapi_client(
        cluster=get_paasta_oapi_api_clustername(cluster=cluster, is_eks=is_eks),
        system_paasta_config=system_paasta_config,
    )
    if not client:
        print("Cannot get a paasta-api client")
        exit(1)

    # TODO add image argument if build
    response = client.remote_run.remote_run_start(
        service,
        instance,
        {"user": get_username(), "interactive": True},
    )
    try:
        response = json.loads(response)
    except client.api_error as exc:
        print(exc, file=sys.stderr)
        output.append(PaastaColors.red(exc.reason))
        ret_code = exc.status
    except (client.connection_error, client.timeout_error) as exc:
        output.append(
            PaastaColors.red(f"Could not connect to API: {exc.__class__.__name__}")
        )
        ret_code = 1
    except Exception as e:
        output.append(PaastaColors.red(f"Exception when talking to the API:"))
        output.append(str(e))
        ret_code = 1

    if ret_code:
        print("\n".join(output))
        return ret_code

    if response["status"] == 409:
        print(
            "A remote-run container already exists. Run remote-run stop first if you'd like a new one."
        )
        attach = input("Would you like to attach to it? y/n ")
        if attach == "n":
            return 0
    print(response)
    pod_name, namespace = response["pod_name"], response["namespace"]

    try:
        token = client.remote_run.remote_run_token(
            service=service, instance=instance, user="qlo"
        )
        token = json.loads(token)["token"]
    except:
        raise

    exec_command_tmpl = "kubectl{eks}-{cluster} --token {token} exec -it -n {namespace} {pod} -- /bin/bash"
    exec_command = exec_command_tmpl.format(
        eks="-eks" if is_eks else "",
        cluster=cluster,
        namespace=namespace,
        pod=pod_name,
        token=token,
    )
    cmd = pty.spawn(exec_command.split(" "))

    return ret_code


def paasta_remote_run_stop(
    args,
    system_paasta_config: SystemPaastaConfig,
    verbose: int = 0,
    is_eks: bool = True,
) -> int:

    cluster = args.cluster
    service = args.service
    instance = args.instance

    ret_code = 0

    client = get_paasta_oapi_client(
        cluster=get_paasta_oapi_api_clustername(cluster=cluster, is_eks=is_eks),
        system_paasta_config=system_paasta_config,
    )
    if not client:
        print("Cannot get a paasta-api client")
        exit(1)
    response = client.remote_run.remote_run_stop(
        service,
        instance,
        {"user": get_username()},
    )
    print(response)
    return ret_code


def remote_run(args) -> int:
    """Run stuff, but remotely!"""
    system_paasta_config = load_system_paasta_config(
        "/nail/home/qlo/paasta_config/paasta/"
    )
    if args.remote_run_command == "start":
        return paasta_remote_run_start(args, system_paasta_config)
    elif args.remote_run_command == "stop":
        return paasta_remote_run_stop(args, system_paasta_config)
