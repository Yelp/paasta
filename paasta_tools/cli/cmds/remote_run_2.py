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
from paasta_tools.api.client import get_paasta_oapi_client
from paasta_tools.cli.utils import get_paasta_oapi_api_clustername
from paasta_tools.cli.utils import lazy_choices_completer
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
    add_common_args_to_parser(remote_run_parser)
    remote_run_parser.set_defaults(command=remote_run)


def paasta_remote_run(
    cluster: str,
    service: str,
    instance: str,
    system_paasta_config: SystemPaastaConfig,
    verbose: int,
    is_eks: bool = False,
) -> int:
    output = []
    ret_code = 0
    client = get_paasta_oapi_client(
        cluster=get_paasta_oapi_api_clustername(cluster=cluster, is_eks=is_eks),
        system_paasta_config=system_paasta_config,
    )
    if not client:
        print("Cannot get a paasta-api client")
        exit(1)

    try:
        response = client.service.remote_run(
            service=service, instance=instance, user="qlo"
        )
        print(response)
    except client.api_error as exc:
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

    print("\n".join(output))

    return ret_code


def remote_run(args) -> int:
    """Run stuff, but remotely!"""
    system_paasta_config = load_system_paasta_config()
    paasta_remote_run(
        args.cluster, args.service, args.instance, system_paasta_config, 1, False
    )
