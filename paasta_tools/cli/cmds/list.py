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
from service_configuration_lib import DEFAULT_SOA_DIR

from paasta_tools.cli.utils import list_paasta_services
from paasta_tools.cli.utils import list_service_instances
from paasta_tools.utils import list_services
from paasta_tools.utils import SPACER


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        "list",
        help="Display a list of PaaSTA services",
        description=(
            "'paasta list' inspects the soa-configs directory and lists all of the "
            "PaaSTA services that are declared."
        ),
    )
    list_parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="Display all services, even if not on PaaSTA.",
    )
    list_parser.add_argument(
        "-i",
        "--print-instances",
        action="store_true",
        help="Display all service%sinstance values, which only PaaSTA services have."
        % SPACER,
    )
    list_parser.add_argument(
        "-y",
        "--yelpsoa-config-root",
        dest="soa_dir",
        help="A directory from which yelpsoa-configs should be read from",
        default=DEFAULT_SOA_DIR,
    )
    list_parser.set_defaults(command=paasta_list)


def paasta_list(args):
    """Print a list of Yelp services currently running
    :param args: argparse.Namespace obj created from sys.args by cli"""
    if args.print_instances:
        services = list_service_instances(args.soa_dir)
    elif args.all:
        services = list_services(args.soa_dir)
    else:
        services = list_paasta_services(args.soa_dir)

    for service in services:
        print(service)
