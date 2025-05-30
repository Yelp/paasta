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
import subprocess
import sys
import time
from typing import List

from paasta_tools.cli.utils import get_paasta_oapi_api_clustername
from paasta_tools.cli.utils import get_paasta_oapi_client_with_auth
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import run_interactive_cli
from paasta_tools.kubernetes.remote_run import TOOLBOX_MOCK_SERVICE
from paasta_tools.paastaapi.model.remote_run_start import RemoteRunStart
from paasta_tools.paastaapi.model.remote_run_stop import RemoteRunStop
from paasta_tools.utils import get_username
from paasta_tools.utils import list_all_instances_for_service
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import SystemPaastaConfig


KUBECTL_EXEC_CMD_TEMPLATE = (
    "{kubectl_wrapper} --token {token} exec -it -n {namespace} {pod} -- /bin/bash"
)
KUBECTL_CP_CMD_TEMPLATE = (
    "{kubectl_wrapper} --token {token} -n {namespace} cp {filename} {pod}:/tmp/"
)


def _list_services_and_toolboxes() -> List[str]:
    try:
        toolbox_instances = list_all_instances_for_service(
            TOOLBOX_MOCK_SERVICE, instance_type="adhoc"
        )
    except Exception:
        toolbox_instances = set()
    # NOTE: API authorization is enforced by service, and we want different rules
    # for each toolbox, so we combine service and instance in this case to properly
    # allow that to happen.
    return list(list_services()) + sorted(
        f"{TOOLBOX_MOCK_SERVICE}-{instance}" for instance in toolbox_instances
    )


def paasta_remote_run_start(
    args: argparse.Namespace,
    system_paasta_config: SystemPaastaConfig,
    recursed: bool = False,
) -> int:
    status_prefix = "\x1b[2K\r"  # Clear line, carriage return
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
            toolbox=args.toolbox,
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
            service=args.service,
            instance=args.instance,
            job_name=start_response.job_name,
            user=user,
            toolbox=args.toolbox,
        )
        if poll_response.status == 200:
            print("")
            break
        print(f"{status_prefix}Status: {poll_response.message}", end="")
        if poll_response.status == 404:
            # Probably indicates a pod was terminating. Now that its gone, retry the whole process
            if not recursed:
                print("\nPod finished terminating. Rerunning")
                return paasta_remote_run_start(args, system_paasta_config, True)
            else:
                print("\nSomething went wrong. Pod still not found.")
                return 1
        time.sleep(10)
    else:
        print(f"{status_prefix}Timed out while waiting for job to start")
        return 1

    if not args.interactive and not args.toolbox:
        print("Successfully started remote-run job")
        return 0

    print("Pod ready, establishing interactive session...")

    if args.toolbox:
        # NOTE: we only do this for toolbox containers since those images are built with interactive
        # access in mind, and SSH sessions provide better auditability of user actions.
        # I.e., being `nobody` is fine in a normal remote-run, but in toolbox containers
        # we will require knowing the real user (and some tools may need that too).
        exec_command = f"ssh -A {poll_response.pod_address}"
    else:
        token_response = client.remote_run.remote_run_token(
            args.service, args.instance, user
        )
        kubectl_wrapper = f"kubectl-eks-{args.cluster}"
        if not shutil.which(kubectl_wrapper):
            kubectl_wrapper = f"kubectl-{args.cluster}"
        exec_command = KUBECTL_EXEC_CMD_TEMPLATE.format(
            kubectl_wrapper=kubectl_wrapper,
            namespace=poll_response.namespace,
            pod=poll_response.pod_name,
            token=token_response.token,
        )

    if args.copy_file:
        for filename in args.copy_file:
            cp_command = KUBECTL_CP_CMD_TEMPLATE.format(
                kubectl_wrapper=kubectl_wrapper,
                namespace=poll_response.namespace,
                pod=poll_response.pod_name,
                filename=filename,
                token=token_response.token,
            ).split(" ")
            call = subprocess.run(cp_command, capture_output=True)
            if call.returncode != 0:
                print("Error copying file to remote-run pod: ", file=sys.stderr)
                print(call.stderr.decode("utf-8"), file=sys.stderr)
                return 1

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
        args.service,
        args.instance,
        RemoteRunStop(user=get_username(), toolbox=args.toolbox),
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
    service_arg.completer = lazy_choices_completer(_list_services_and_toolboxes)  # type: ignore
    instance_or_toolbox = parser.add_mutually_exclusive_group()
    instance_or_toolbox.add_argument(
        "-i",
        "--instance",
        help=(
            "Simulate a docker run for a particular instance of the "
            "service, like 'main' or 'canary'. Required."
        ),
        default="main",
    )
    instance_or_toolbox.add_argument(
        "--toolbox",
        help="The selected service is a 'toolbox' container",
        action="store_true",
        default=False,
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
    start_parser.add_argument(
        "--copy-file",
        help="Adds a local file to /tmp inside the pod",
        type=str,
        action="append",
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
