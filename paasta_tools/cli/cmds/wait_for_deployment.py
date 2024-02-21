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
"""Contains methods used by the paasta client to wait for deployment
of a docker image to a cluster.instance.
"""
import asyncio
import logging
from typing import Optional

from paasta_tools.cli.cmds.mark_for_deployment import NoSuchCluster
from paasta_tools.cli.cmds.mark_for_deployment import report_waiting_aborted
from paasta_tools.cli.cmds.mark_for_deployment import wait_for_deployment
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import NoSuchService
from paasta_tools.cli.utils import validate_git_sha
from paasta_tools.cli.utils import validate_given_deploy_groups
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.cli.utils import validate_short_git_sha
from paasta_tools.remote_git import list_remote_refs
from paasta_tools.remote_git import LSRemoteException
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import DeploymentVersion
from paasta_tools.utils import get_git_url
from paasta_tools.utils import get_latest_deployment_tag
from paasta_tools.utils import list_services
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import TimeoutError

DEFAULT_DEPLOYMENT_TIMEOUT = 3600  # seconds


log = logging.getLogger(__name__)


class VersionError(Exception):
    pass


class DeployGroupError(Exception):
    pass


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        "wait-for-deployment",
        help="Wait a service to be deployed to deploy_group",
        description=(
            "'paasta wait-for-deployment' waits for a previously marked for "
            "deployment service to be deployed to deploy_group."
        ),
        epilog=(
            "Note: Access and credentials to the Git repo of a service "
            "are required for this command to work."
        ),
    )
    list_parser.add_argument(
        "-u",
        "--git-url",
        help=(
            "Git url for service. Defaults to the normal git URL for " "the service."
        ),
        default=None,
    )
    list_parser.add_argument(
        "-c",
        "-k",
        "--commit",
        help="Git sha to wait for deployment",
        required=True,
        type=validate_short_git_sha,
    )
    list_parser.add_argument(
        "-i",
        "--image-version",
        help="Extra version metadata to mark for deployment",
        required=False,
        default=None,
    )
    list_parser.add_argument(
        "-l",
        "--deploy-group",
        help="deploy group (e.g. cluster1.canary, cluster2.main).",
        required=True,
    ).completer = lazy_choices_completer(list_deploy_groups)
    list_parser.add_argument(
        "-s",
        "--service",
        help="Name of the service which you wish to wait for deployment. "
        'Leading "services-" will be stripped.',
        required=True,
    ).completer = lazy_choices_completer(list_services)
    list_parser.add_argument(
        "-t",
        "--timeout",
        dest="timeout",
        type=int,
        default=DEFAULT_DEPLOYMENT_TIMEOUT,
        help=(
            "Time in seconds to wait for paasta to deploy the service. "
            "If the timeout is exceeded we return 1. "
            "Default is %(default)s seconds."
        ),
    )
    list_parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    list_parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        dest="verbose",
        default=0,
        help="Print out more output.",
    )
    list_parser.add_argument(
        "--polling-interval",
        dest="polling_interval",
        type=float,
        default=None,
        help="How long to wait between each time we check to see if an instance is done deploying.",
    )
    list_parser.add_argument(
        "--diagnosis-interval",
        dest="diagnosis_interval",
        type=float,
        default=None,
        help="How long to wait between diagnoses of why the bounce isn't done.",
    )
    list_parser.add_argument(
        "--time-before-first-diagnosis",
        dest="time_before_first_diagnosis",
        type=float,
        default=None,
        help="Wait this long before trying to diagnose why the bounce isn't done.",
    )

    list_parser.set_defaults(command=paasta_wait_for_deployment)


def get_latest_marked_version(
    git_url: str, deploy_group: str
) -> Optional[DeploymentVersion]:
    """Return the latest marked for deployment version or None"""
    # TODO: correct this function for new tag format
    refs = list_remote_refs(git_url)
    _, sha, image_version = get_latest_deployment_tag(refs, deploy_group)
    if sha:
        return DeploymentVersion(sha=sha, image_version=image_version)
    # We did not find a ref for this deploy group
    return None


def validate_version_is_latest(
    version: DeploymentVersion, git_url: str, deploy_group: str, service: str
):
    """Verify if the requested version  is the latest marked for deployment.

    Raise exception when the provided version is not the latest
    marked for deployment in 'deploy_group' for 'service'.
    """
    try:
        marked_version = get_latest_marked_version(git_url, deploy_group)
    except LSRemoteException as e:
        print(
            "Error talking to the git server: {}\n"
            "It is not possible to verify that {} is marked for deployment in {}, "
            "but I assume that it is marked and will continue waiting..".format(
                e, version, deploy_group
            )
        )
        return
    if marked_version is None:
        raise VersionError(
            "ERROR: Nothing is marked for deployment "
            "in {} for {}".format(deploy_group, service)
        )
    if version != marked_version:
        raise VersionError(
            "ERROR: The latest version marked for "
            "deployment in {} is {}".format(deploy_group, marked_version)
        )


def validate_deploy_group(deploy_group: str, service: str, soa_dir: str):
    """Validate deploy_group.

    Raise exception if the specified deploy group is not used anywhere.
    """
    in_use_deploy_groups = list_deploy_groups(service=service, soa_dir=soa_dir)
    _, invalid_deploy_groups = validate_given_deploy_groups(
        in_use_deploy_groups, [deploy_group]
    )

    if len(invalid_deploy_groups) == 1:
        raise DeployGroupError(
            "ERROR: These deploy groups are not currently "
            "used anywhere: {}.\n"
            "You probably need one of these in-use deploy "
            "groups?:\n   {}".format(
                ",".join(invalid_deploy_groups), ",".join(in_use_deploy_groups)
            )
        )


def paasta_wait_for_deployment(args):
    """Wrapping wait_for_deployment"""
    if args.verbose:
        log.setLevel(level=logging.DEBUG)
    else:
        log.setLevel(level=logging.INFO)

    service = args.service
    if service and service.startswith("services-"):
        service = service.split("services-", 1)[1]

    if args.git_url is None:
        args.git_url = get_git_url(service=service, soa_dir=args.soa_dir)

    args.commit = validate_git_sha(sha=args.commit, git_url=args.git_url)

    version = DeploymentVersion(sha=args.commit, image_version=args.image_version)

    try:
        validate_service_name(service, soa_dir=args.soa_dir)
        validate_deploy_group(args.deploy_group, service, args.soa_dir)
        validate_version_is_latest(version, args.git_url, args.deploy_group, service)
    except (VersionError, DeployGroupError, NoSuchService) as e:
        print(PaastaColors.red(f"{e}"))
        return 1

    try:
        asyncio.run(
            wait_for_deployment(
                service=service,
                deploy_group=args.deploy_group,
                git_sha=args.commit,
                image_version=args.image_version,
                soa_dir=args.soa_dir,
                timeout=args.timeout,
                polling_interval=args.polling_interval,
                diagnosis_interval=args.diagnosis_interval,
                time_before_first_diagnosis=args.time_before_first_diagnosis,
            )
        )
        _log(
            service=service,
            component="deploy",
            line=(f"Deployment of {version} for {args.deploy_group} complete"),
            level="event",
        )

    except (KeyboardInterrupt, TimeoutError, NoSuchCluster):
        report_waiting_aborted(service, args.deploy_group)
        return 1

    return 0
