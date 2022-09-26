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
import argparse
from typing import Collection
from typing import Dict
from typing import Mapping
from typing import Tuple

from humanize import naturaltime

from paasta_tools.cli.cmds.mark_for_deployment import can_user_deploy_service
from paasta_tools.cli.cmds.mark_for_deployment import get_deploy_info
from paasta_tools.cli.utils import extract_tags
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import validate_given_deploy_groups
from paasta_tools.remote_git import list_remote_refs
from paasta_tools.utils import datetime_from_utc_to_local
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import DeploymentVersion
from paasta_tools.utils import format_table
from paasta_tools.utils import get_git_url
from paasta_tools.utils import list_services
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import parse_timestamp


def add_subparser(subparsers: argparse._SubParsersAction) -> None:
    list_parser = subparsers.add_parser(
        "get-deploygroup-history",
        help="Lists the history of a deploy group",
        description="",  # todo
    )

    arg_service = list_parser.add_argument(
        "-s",
        "--service",
        help='Name of the service to view its deploy group history (e.g. "service1")',
        required=True,
    )
    arg_service.completer = lazy_choices_completer(list_services)  # type: ignore

    arg_deploy_group = list_parser.add_argument(
        "-l",
        "--deploy-groups",
        help="Mark one or more deploy groups to view their history (e.g. "
        '"all.main", "all.main,all.canary").'
        "Use the flag -a or --all-deploy-groups",
        default="",
        required=False,
    )
    arg_deploy_group.completer = lazy_choices_completer(list_deploy_groups)  # type: ignore

    list_parser.add_argument(
        "-a",
        "--all-deploy-groups",
        help="View history of all deploy groups for the service",
        action="store_true",
        required=False,
    )

    list_parser.add_argument(
        "-y",
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )

    list_parser.set_defaults(command=paasta_get_deploygroup_history)


def get_versions_for_service(
    service: str, deploy_groups: Collection[str], soa_dir: str
) -> Mapping[DeploymentVersion, Tuple[str, str]]:
    """Returns a dictionary of 2-tuples of the form (timestamp, deploy_group) for each version tuple of (deploy sha, image_version)"""
    if service is None:
        return {}
    git_url = get_git_url(service=service, soa_dir=soa_dir)
    all_deploy_groups = list_deploy_groups(service=service, soa_dir=soa_dir)
    deploy_groups, _ = validate_given_deploy_groups(all_deploy_groups, deploy_groups)
    previously_deployed_versions: Dict[DeploymentVersion, Tuple[str, str]] = {}

    for ref, sha in list_remote_refs(git_url).items():
        regex_match = extract_tags(ref)
        try:
            deploy_group = regex_match["deploy_group"]
            tstamp = regex_match["tstamp"]
            image_version = regex_match["image_version"]
        except KeyError:
            pass
        else:
            if deploy_group in deploy_groups:
                version = DeploymentVersion(sha=sha, image_version=image_version)
                previously_deployed_versions[version] = (tstamp, deploy_group)
    return previously_deployed_versions


def list_previous_versions(
    service: str,
    deploy_groups: Collection[str],
    any_given_deploy_groups: bool,
    versions: Mapping[DeploymentVersion, Tuple],
) -> None:
    def format_timestamp(tstamp: str) -> str:
        return naturaltime(datetime_from_utc_to_local(parse_timestamp(tstamp)))

    print("Below is a list of deploy_groups History:")
    # All versions sorted by deployment time
    list_of_versions = sorted(versions.items(), key=lambda x: x[1], reverse=True)
    rows = [("Timestamp -- UTC", "Human time", "deploy_group", "Version")]
    for version, (timestamp, deploy_group) in list_of_versions:
        rows.extend(
            [(timestamp, format_timestamp(timestamp), deploy_group, repr(version))]
        )
    for line in format_table(rows):
        print(line)


def paasta_get_deploygroup_history(args: argparse.Namespace) -> int:
    soa_dir = args.soa_dir
    service = figure_out_service_name(args, soa_dir)

    deploy_info = get_deploy_info(service=service, soa_dir=args.soa_dir)
    if not can_user_deploy_service(deploy_info, service):
        return 1

    if args.all_deploy_groups:
        given_deploy_groups = list_deploy_groups(service=service, soa_dir=soa_dir)
    else:
        given_deploy_groups = {
            deploy_group
            for deploy_group in args.deploy_groups.split(",")
            if deploy_group
        }

    all_deploy_groups = list_deploy_groups(service=service, soa_dir=soa_dir)
    deploy_groups, invalid = validate_given_deploy_groups(
        all_deploy_groups, given_deploy_groups
    )

    if len(invalid) > 0:
        print(
            PaastaColors.yellow(
                "These deploy groups are not valid and will be skipped: %s.\n"
                % (",").join(invalid)
            )
        )

    if len(deploy_groups) == 0 and not args.all_deploy_groups:
        print(
            PaastaColors.red(
                "ERROR: No valid deploy groups specified for %s.\n Use the flag -a to view history of all valid deploy groups for this service"
                % (service)
            )
        )
        return 1

    versions = get_versions_for_service(service, deploy_groups, soa_dir)
    list_previous_versions(service, deploy_groups, bool(given_deploy_groups), versions)

    return 0
