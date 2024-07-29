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
from typing import Generator
from typing import Mapping
from typing import Tuple

from humanize import naturaltime

from paasta_tools.cli.cmds.mark_for_deployment import can_user_deploy_service
from paasta_tools.cli.cmds.mark_for_deployment import get_deploy_info
from paasta_tools.cli.cmds.mark_for_deployment import mark_for_deployment
from paasta_tools.cli.utils import extract_tags
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import validate_full_git_sha
from paasta_tools.cli.utils import validate_given_deploy_groups
from paasta_tools.deployment_utils import get_currently_deployed_version
from paasta_tools.remote_git import list_remote_refs
from paasta_tools.utils import _log_audit
from paasta_tools.utils import datetime_from_utc_to_local
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import DeploymentVersion
from paasta_tools.utils import format_table
from paasta_tools.utils import get_git_url
from paasta_tools.utils import list_services
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import parse_timestamp
from paasta_tools.utils import RollbackTypes


def add_subparser(subparsers: argparse._SubParsersAction) -> None:
    list_parser = subparsers.add_parser(
        "rollback",
        help="Rollback a docker image to a previous deploy",
        description=(
            "'paasta rollback' is a human-friendly tool for marking a particular "
            "docker image for deployment, which invokes a bounce. While the command "
            "is called 'rollback', it can be used to roll forward or back, as long "
            "as there is a docker image available for the input Git SHA."
        ),
        epilog=(
            "This rollback command uses the Git control plane, which requires network "
            "connectivity as well as authorization to the Git repo.\n\n"
            + PaastaColors.yellow(
                "WARNING: You MUST manually revert changes in Git and go through the normal push process after using this command.\n"
            )
            + PaastaColors.yellow(
                "WARNING: Failing to do so means that Jenkins will redeploy the latest code on the next scheduled build!"
            )
        ),
        # we manually format the epilog to add newlines + give it an attention-grabbing color
        # re: reverting changes in Git post-rollback
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    arg_commit = list_parser.add_argument(
        "-k",
        "--commit",
        help="Git SHA to mark for rollback. "
        "A commit to rollback to is required for paasta rollback to run. However if one is not provided, "
        "paasta rollback will instead output a list of valid git shas to rollback to.",
        required=False,
    )
    arg_commit.completer = lazy_choices_completer(list_previously_deployed_shas)  # type: ignore
    arg_version = list_parser.add_argument(
        "-i",
        "--image-version",
        help="Extra version metadata to mark for rollback. "
        "If your service has enabled no-commit redeploys, both a commit and the extra metadata is required for paasta rollback to run. However if one is not provided, "
        "paasta rollback will instead output a list of valid versions to rollback to.",
        required=False,
        default=None,
    )
    arg_version.completer = lazy_choices_completer(list_previously_deployed_image_versions)  # type: ignore
    arg_deploy_group = list_parser.add_argument(
        "-l",
        "--deploy-groups",
        help="Mark one or more deploy groups to roll back (e.g. "
        '"all.main", "all.main,all.canary"). If no deploy groups specified,'
        "no deploy groups for that service are rolled back. To rollback all deploy groups "
        "use the flag -a or --all-deploy-groups",
        default="",
        required=False,
    )
    arg_deploy_group.completer = lazy_choices_completer(list_deploy_groups)  # type: ignore
    list_parser.add_argument(
        "-a",
        "--all-deploy-groups",
        help="Rollback all deploy groups for the service",
        action="store_true",
        required=False,
    )

    arg_service = list_parser.add_argument(
        "-s", "--service", help='Name of the service to rollback (e.g. "service1")'
    )
    arg_service.completer = lazy_choices_completer(list_services)  # type: ignore
    list_parser.add_argument(
        "-y",
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    list_parser.add_argument(
        "-f",
        "--force",
        help=("Do not check if Git SHA was marked for deployment previously."),
        action="store_true",
    )
    list_parser.set_defaults(command=paasta_rollback)


def list_previously_deployed_shas(
    parsed_args: argparse.Namespace, **kwargs: None
) -> Generator[str, None, None]:
    service = parsed_args.service
    soa_dir = parsed_args.soa_dir
    deploy_groups = {
        deploy_group
        for deploy_group in parsed_args.deploy_groups.split(",")
        if deploy_group
    }
    return (
        version.sha
        for version in get_versions_for_service(service, deploy_groups, soa_dir)
    )


def list_previously_deployed_image_versions(
    parsed_args: argparse.Namespace, **kwargs: None
) -> Generator[str, None, None]:
    service = parsed_args.service
    soa_dir = parsed_args.soa_dir
    deploy_groups = {
        deploy_group
        for deploy_group in parsed_args.deploy_groups.split(",")
        if deploy_group
    }
    return (
        version.image_version
        for version in get_versions_for_service(service, deploy_groups, soa_dir)
    )


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
            # Now we filter and dedup by picking the most recent sha for a deploy group
            # Note that all strings are greater than ''
            if deploy_group in deploy_groups:
                version = DeploymentVersion(sha=sha, image_version=image_version)
                tstamp_so_far = previously_deployed_versions.get(version, ("all", ""))[
                    1
                ]
                if tstamp > tstamp_so_far:
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

    print("Below is a list of recent commits:")
    # Latest 10 versions sorted by deployment time
    list_of_versions = sorted(versions.items(), key=lambda x: x[1], reverse=True)[:10]
    rows = [("Timestamp -- UTC", "Human time", "deploy_group", "Version")]
    for version, (timestamp, deploy_group) in list_of_versions:
        rows.extend(
            [(timestamp, format_timestamp(timestamp), deploy_group, repr(version))]
        )
    for line in format_table(rows):
        print(line)
    if len(list_of_versions) >= 2:
        version, (timestamp, deploy_group) = list_of_versions[1]
        deploy_groups_arg_line = (
            "-l %s " % ",".join(deploy_groups) if any_given_deploy_groups else ""
        )
        version_arg = (
            f" --image-version {version.image_version}" if version.image_version else ""
        )
        print(
            "\nFor example, to use the second to last version from {} used on {}, run:".format(
                format_timestamp(timestamp), PaastaColors.bold(deploy_group)
            )
        )
        print(
            PaastaColors.bold(
                f"    paasta rollback -s {service} {deploy_groups_arg_line}-k {version.sha}{version_arg}"
            )
        )


def paasta_rollback(args: argparse.Namespace) -> int:
    """Call mark_for_deployment with rollback parameters
    :param args: contains all the arguments passed onto the script: service,
    deploy groups and sha. These arguments will be verified and passed onto
    mark_for_deployment.
    """

    soa_dir = args.soa_dir
    service = figure_out_service_name(args, soa_dir)

    deploy_info = get_deploy_info(service=service, soa_dir=args.soa_dir)
    if not can_user_deploy_service(deploy_info, service):
        return 1

    git_url = get_git_url(service, soa_dir)

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
                "ERROR: No valid deploy groups specified for %s.\n Use the flag -a to rollback all valid deploy groups for this service"
                % (service)
            )
        )
        return 1

    versions = get_versions_for_service(service, deploy_groups, soa_dir)
    commit = args.commit
    image_version = args.image_version
    new_version = DeploymentVersion(sha=commit, image_version=image_version)
    if not commit:
        print("Please specify a commit to mark for rollback (-k, --commit).")
        list_previous_versions(
            service, deploy_groups, bool(given_deploy_groups), versions
        )
        return 1
    elif new_version not in versions and not args.force:
        print(
            PaastaColors.red(
                f"This version {new_version} has never been deployed before."
            )
        )
        print("Please double check it or use --force to skip this verification.\n")
        list_previous_versions(
            service, deploy_groups, bool(given_deploy_groups), versions
        )
        return 1

    try:
        validate_full_git_sha(args.commit)
    except argparse.ArgumentTypeError as e:
        print(PaastaColors.red(f"Error: {e}"))
        return 1

    # TODO: Add similar check for when image_version is empty and no-commit redeploys is enforced for requested deploy_group

    returncode = 0

    for deploy_group in deploy_groups:
        rolled_back_from = get_currently_deployed_version(service, deploy_group)
        returncode |= mark_for_deployment(
            git_url=git_url,
            service=service,
            deploy_group=deploy_group,
            commit=commit,
            image_version=image_version,
        )

        # we could also gate this by the return code from m-f-d, but we probably care more about someone wanting to
        # rollback than we care about if the underlying machinery was successfully able to complete the request
        if rolled_back_from != new_version:
            audit_action_details = {
                "rolled_back_from": str(rolled_back_from),
                "rolled_back_to": str(new_version),
                "rollback_type": RollbackTypes.USER_INITIATED_ROLLBACK.value,
                "deploy_group": deploy_group,
            }
            _log_audit(
                action="rollback", action_details=audit_action_details, service=service
            )

    if returncode == 0:
        print(
            PaastaColors.yellow(
                f"WARNING: You MUST manually revert changes in Git! Use 'git revert {rolled_back_from.sha}', and go through the normal push process. "
            )
        )
        print(
            PaastaColors.yellow(
                f"WARNING: Failing to do so means that Jenkins will redeploy the latest code on the next scheduled build!"
            )
        )

    return returncode
