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
import sys

from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import PaastaColors
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.deployment_utils import get_currently_deployed_version
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_services


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        "get-latest-deployment",
        help="Gets the Git SHA for the latest deployment of a service",
    )
    list_parser.add_argument(
        "-s",
        "--service",
        help="Name of the service which you want to get the latest deployment for.",
        required=True,
    ).completer = lazy_choices_completer(list_services)
    list_parser.add_argument(
        "-i",
        "-l",
        "--deploy-group",
        help="Name of the deploy group which you want to get the latest deployment for.",
        required=True,
    ).completer = lazy_choices_completer(list_deploy_groups)
    list_parser.add_argument(
        "-d",
        "--soa-dir",
        help="A directory from which soa-configs should be read from",
        default=DEFAULT_SOA_DIR,
    )
    format_group = list_parser.add_mutually_exclusive_group()
    format_group.add_argument(
        "--sha-only",
        help="Return only the latest sha for this deploy group, not the full deployed version",
        action="store_true",
        default=False,
    )
    format_group.add_argument(
        "-j",
        "--json",
        help="Return result in json format instead of raw string",
        action="store_true",
        default=False,
    )

    list_parser.set_defaults(command=paasta_get_latest_deployment)


def paasta_get_latest_deployment(args):
    service = args.service
    deploy_group = args.deploy_group
    soa_dir = args.soa_dir
    validate_service_name(service, soa_dir)

    version = get_currently_deployed_version(
        service=service, deploy_group=deploy_group, soa_dir=soa_dir
    )
    if not version:
        print(
            PaastaColors.red(
                f"A deployment could not be found for {deploy_group} in {service}"
            ),
            file=sys.stderr,
        )
        return 1
    else:
        if args.sha_only:
            print(version.sha)
        else:
            if args.json:
                print(version.json())
            else:
                print(version)
        return 0
