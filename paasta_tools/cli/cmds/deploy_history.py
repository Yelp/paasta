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
from typing import List
from typing import Tuple

from dulwich.errors import GitProtocolError
from humanize import naturaltime

from paasta_tools.cli.utils import extract_tags
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import validate_given_deploy_groups
from paasta_tools.remote_git import LSRemoteException
from paasta_tools.remote_git import list_remote_refs
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import DeploymentVersion
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import datetime_from_utc_to_local
from paasta_tools.utils import format_table
from paasta_tools.utils import get_git_url
from paasta_tools.utils import list_services
from paasta_tools.utils import parse_timestamp

DEFAULT_LIMIT = 10

# (timestamp, deploy_group, version)
DeployHistoryEntry = Tuple[str, str, DeploymentVersion]


def add_subparser(subparsers: argparse._SubParsersAction) -> None:
    list_parser = subparsers.add_parser(
        "deploy-history",
        help="Show the deploy history of a service's deploy groups",
        description=(
            "'paasta deploy-history' lists past deployments of a service, newest first, "
            "based on the deploy tags PaaSTA pushes to the service's Git repo. "
            "Every deploy is shown as its own row, so redeploys of the same version "
            "appear multiple times. This command is read-only."
        ),
        epilog=(
            "This command reads tags from the service's Git repo, which requires "
            "network connectivity as well as read access to the Git repo."
        ),
    )
    arg_service = list_parser.add_argument(
        "-s",
        "--service",
        help='Name of the service to show deploy history for (e.g. "service1"). '
        "Defaults to the name of the current directory.",
        # strip any potential trailing / for folks tab-completing directories
        type=lambda x: x.rstrip("/"),
    )
    arg_service.completer = lazy_choices_completer(list_services)  # type: ignore
    arg_deploy_group = list_parser.add_argument(
        "-l",
        "--deploy-groups",
        help="Comma-separated list of deploy groups to show history for "
        '(e.g. "prod.main" or "prod.main,prod.canary"). '
        "Defaults to all deploy groups of the service.",
        default="",
        required=False,
    )
    arg_deploy_group.completer = lazy_choices_completer(list_deploy_groups)  # type: ignore
    list_parser.add_argument(
        "-n",
        "--limit",
        help=f"Maximum number of deploys to show (default: {DEFAULT_LIMIT}). "
        "Use 0 to show all of them.",
        type=int,
        default=DEFAULT_LIMIT,
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
    list_parser.set_defaults(command=paasta_deploy_history)


def get_deploy_history(
    service: str, deploy_groups: Collection[str], soa_dir: str
) -> List[DeployHistoryEntry]:
    """Returns every deploy of the given deploy groups as (timestamp, deploy_group, version),
    sorted newest first.

    Unlike rollback.get_versions_for_service, this does not dedupe by version: a version
    deployed several times (or to several deploy groups) shows up once per deploy.
    """
    git_url = get_git_url(service=service, soa_dir=soa_dir)
    history: List[DeployHistoryEntry] = []
    for ref, sha in list_remote_refs(git_url).items():
        tag_info = extract_tags(ref)
        if not tag_info:
            # not a paasta tag, e.g. refs/heads/master
            continue
        if tag_info["tag"] != "deploy":
            # skip noise like rollback tags
            continue
        if tag_info["deploy_group"] not in deploy_groups:
            continue
        history.append(
            (
                tag_info["tstamp"],
                tag_info["deploy_group"],
                DeploymentVersion(sha=sha, image_version=tag_info["image_version"]),
            )
        )
    # timestamps are YYYYMMDDTHHMMSS, so string order is chronological order
    return sorted(history, key=lambda entry: (entry[0], entry[1]), reverse=True)


def print_deploy_history(history: List[DeployHistoryEntry], limit: int) -> None:
    if not history:
        print("No deploys found for the given deploy groups.")
        return

    shown = history[:limit] if limit > 0 else history
    rows = [("Timestamp -- UTC", "Human time", "deploy_group", "Version")]
    for timestamp, deploy_group, version in shown:
        human_time = naturaltime(datetime_from_utc_to_local(parse_timestamp(timestamp)))
        rows.append((timestamp, human_time, deploy_group, repr(version)))
    for line in format_table(rows):
        print(line)

    if len(shown) < len(history):
        print(
            f"\nShowing the {len(shown)} most recent of {len(history)} deploys. "
            "Use -n 0 to show all of them."
        )


def paasta_deploy_history(args: argparse.Namespace) -> int:
    """Print the deploy history for one or more deploy groups of a service."""
    soa_dir = args.soa_dir
    service = figure_out_service_name(args, soa_dir)

    all_deploy_groups = list_deploy_groups(service=service, soa_dir=soa_dir)
    given_deploy_groups = {
        deploy_group for deploy_group in args.deploy_groups.split(",") if deploy_group
    }
    if not given_deploy_groups:
        given_deploy_groups = set(all_deploy_groups)

    deploy_groups, invalid = validate_given_deploy_groups(
        all_deploy_groups, given_deploy_groups
    )

    if invalid:
        print(
            PaastaColors.yellow(
                "These deploy groups are not valid and will be skipped: %s.\n"
                % ",".join(sorted(invalid))
            )
        )

    if not deploy_groups:
        print(
            PaastaColors.red(
                f"ERROR: No valid deploy groups found for {service}. "
                f"Valid deploy groups are: {', '.join(sorted(all_deploy_groups)) or '(none)'}"
            )
        )
        return 1

    try:
        history = get_deploy_history(service, deploy_groups, soa_dir)
    except (LSRemoteException, GitProtocolError) as e:
        print(
            PaastaColors.red(
                f"ERROR: Could not read deploy tags from the Git repo for {service}: {e}\n"
                "Check that you have network connectivity and read access to the repo."
            )
        )
        return 1
    print_deploy_history(history, args.limit)
    return 0
