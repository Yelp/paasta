#!/usr/bin/env python
# Copyright 2015-2017 Yelp Inc.
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
import argparse
import shutil
import time

from paasta_tools.cli.utils import get_paasta_oapi_api_clustername
from paasta_tools.cli.utils import get_paasta_oapi_client_with_auth
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import run_interactive_cli
from paasta_tools.paastaapi.model.remote_run_start import RemoteRunStart
from paasta_tools.paastaapi.model.remote_run_stop import RemoteRunStop
from paasta_tools.utils import get_username
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import SystemPaastaConfig


KUBECTL_CMD_TEMPLATE = (
    "{kubectl_wrapper} --token {token} exec -it -n {namespace} {pod} -- /bin/bash"
)


def paasta_remote_run_start(
    args: argparse.Namespace,
    system_paasta_config: SystemPaastaConfig,
) -> int:
    client = get_paasta_oapi_client_with_auth(
        cluster=get_paasta_oapi_api_clustername(cluster=args.cluster, is_eks=True),
        system_paasta_config=system_paasta_config,
    )
    if not client:
        print("Cannot get a paasta-api client")
        return 1

    user = get_username()
    start_response = client.remote_run.remote_run_start(
        args.service,
        args.instance,
        RemoteRunStart(
            user=user,
            interactive=args.interactive,
            recreate=args.recreate,
            max_duration=args.max_duration,
        ),
    )
    if start_response.status >= 300:
        print(f"Error from PaaSTA APIs while starting job: {start_response.message}")
        return 1

    print(
        f"Triggered remote-run job for {args.service}. Waiting for pod to come online..."
    )
    start_time = time.time()
    while time.time() - start_time < args.timeout:
        poll_response = client.remote_run.remote_run_poll(
            args.service,
            args.instance,
            start_response.job_name,
        )
        if poll_response.status == 200:
            print("")
            break
        print(f"\rStatus: {poll_response.message}", end="")
        time.sleep(10)
    else:
        print("Timed out while waiting for job to start")
        return 1

    if not args.interactive:
        print("Successfully started remote-run job")
        return 0

    print("Pod ready, establishing interactive session...")

    token_response = client.remote_run.remote_run_token(
        args.service, args.instance, user
    )

    kubectl_wrapper = f"kubectl-eks-{args.cluster}"
    if not shutil.which(kubectl_wrapper):
        kubectl_wrapper = f"kubectl-{args.cluster}"
    exec_command = KUBECTL_CMD_TEMPLATE.format(
        kubectl_wrapper=kubectl_wrapper,
        namespace=poll_response.namespace,
        pod=poll_response.pod_name,
        token=token_response.token,
    )
    run_interactive_cli(exec_command)
    return 0


def paasta_remote_run_stop(
    args: argparse.Namespace,
    system_paasta_config: SystemPaastaConfig,
) -> int:
    client = get_paasta_oapi_client_with_auth(
        cluster=get_paasta_oapi_api_clustername(cluster=args.cluster, is_eks=True),
        system_paasta_config=system_paasta_config,
    )
    if not client:
        print("Cannot get a paasta-api client")
        return 1
    response = client.remote_run.remote_run_stop(
        args.service, args.instance, RemoteRunStop(user=get_username())
    )
    print(response.message)
    return 0 if response.status < 300 else 1


def add_common_args_to_parser(parser: argparse.ArgumentParser):
    service_arg = parser.add_argument(
        "-s",
        "--service",
        help="The name of the service you wish to inspect. Required.",
        required=True,
    )
    service_arg.completer = lazy_choices_completer(list_services)  # type: ignore
    parser.add_argument(
        "-i",
        "--instance",
        help=(
            "Simulate a docker run for a particular instance of the "
            "service, like 'main' or 'canary'. Required."
        ),
        required=True,
    )
    cluster_arg = parser.add_argument(
        "-c",
        "--cluster",
        help="The name of the cluster you wish to run your task on. Required.",
        required=True,
    )
    cluster_arg.completer = lazy_choices_completer(list_clusters)  # type: ignore


def add_subparser(subparsers: argparse._SubParsersAction) -> None:
    remote_run_parser = subparsers.add_parser(
        "remote-run",
        help="Run services / jobs remotely",
        description="'paasta remote-run' runs services / jobs remotely",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = remote_run_parser.add_subparsers(dest="remote_run_command")
    start_parser = subparsers.add_parser(
        "start",
        help="Start or connect to a remote-run job",
        description="Starts or connects to a remote-run-job",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    start_parser.add_argument(
        "-I",
        "--interactive",
        help=(
            "Run container in interactive mode. If interactive is set the "
            'default command will be "bash" unless otherwise set by the "--cmd" flag'
        ),
        action="store_true",
        default=False,
    )
    start_parser.add_argument(
        "-m",
        "--max-duration",
        help=(
            "Amount of time in seconds after which the job is "
            "automatically stopped (capped by the API backend)"
        ),
        type=int,
        default=1800,
    )
    start_parser.add_argument(
        "-r",
        "--recreate",
        help="Recreate remote-run job if already existing",
        action="store_true",
        default=False,
    )
    start_parser.add_argument(
        "-t",
        "--timeout",
        help="Maximum time to wait for a job to start, in seconds",
        type=int,
        default=600,
    )
    stop_parser = subparsers.add_parser(
        "stop",
        help="Stop your remote-run job if it exists",
        description="Stop your remote-run job if it exists",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_common_args_to_parser(start_parser)
    add_common_args_to_parser(stop_parser)
    remote_run_parser.set_defaults(command=paasta_remote_run)


def paasta_remote_run(args: argparse.Namespace) -> int:
    system_paasta_config = load_system_paasta_config()
    if args.remote_run_command == "start":
        return paasta_remote_run_start(args, system_paasta_config)
    elif args.remote_run_command == "stop":
        return paasta_remote_run_stop(args, system_paasta_config)
    raise ValueError(f"Unsupported subcommand: {args.remote_run_command}")
