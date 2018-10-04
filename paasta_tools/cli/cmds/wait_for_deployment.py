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
import logging

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
from paasta_tools.utils import get_git_url
from paasta_tools.utils import list_services
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import TimeoutError


DEFAULT_DEPLOYMENT_TIMEOUT = 3600  # seconds


log = logging.getLogger(__name__)


class GitShaError(Exception):
    pass


class DeployGroupError(Exception):
    pass


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'wait-for-deployment',
        help='Wait a service to be deployed to deploy_group',
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
        '-u', '--git-url',
        help=(
            'Git url for service. Defaults to the normal git URL for '
            'the service.'
        ),
        default=None,
    )
    list_parser.add_argument(
        '-c', '-k', '--commit',
        help='Git sha to wait for deployment',
        required=True,
        type=validate_short_git_sha,
    )
    list_parser.add_argument(
        '-l', '--deploy-group',
        help='deploy group (e.g. cluster1.canary, cluster2.main).',
        required=True,
    ).completer = lazy_choices_completer(list_deploy_groups)
    list_parser.add_argument(
        '-s', '--service',
        help='Name of the service which you wish to wait for deployment. '
        'Leading "services-" will be stripped.',
        required=True,
    ).completer = lazy_choices_completer(list_services)
    list_parser.add_argument(
        '-t', '--timeout',
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
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    list_parser.add_argument(
        '-v', '--verbose',
        action='count',
        dest="verbose",
        default=0,
        help="Print out more output.",
    )

    list_parser.set_defaults(command=paasta_wait_for_deployment)


def get_latest_marked_sha(git_url, deploy_group):
    """Return the latest marked for deployment git sha or ''"""
    refs = list_remote_refs(git_url)
    last_ref = ''
    for ref in refs:
        if (
            ref.startswith(f'refs/tags/paasta-{deploy_group}-') and
            ref.endswith('-deploy') and
            ref > last_ref
        ):
            last_ref = ref
    return refs[last_ref] if last_ref else ''


def validate_git_sha_is_latest(git_sha, git_url, deploy_group, service):
    """Verify if git_sha is the latest sha marked for deployment.

    Raise exception when the provided git_sha is not the latest
    marked for deployment in 'deploy_group' for 'service'.
    """
    try:
        marked_sha = get_latest_marked_sha(git_url, deploy_group)
    except LSRemoteException as e:
        paasta_print("Error talking to the git server: {}\n"
                     "It is not possible to verify that {} is marked for deployment in {}, "
                     "but I assume that it is marked and will continue waiting.."
                     .format(e, git_sha, deploy_group))
        return
    if marked_sha == '':
        raise GitShaError("ERROR: Nothing is marked for deployment "
                          "in {} for {}"
                          .format(deploy_group, service))
    if git_sha != marked_sha:
        raise GitShaError("ERROR: The latest git SHA marked for "
                          "deployment in {} is {}"
                          .format(deploy_group, marked_sha))


def validate_deploy_group(deploy_group, service, soa_dir):
    """Validate deploy_group.

    Raise exception if the specified deploy group is not used anywhere.
    """
    in_use_deploy_groups = list_deploy_groups(
        service=service,
        soa_dir=soa_dir,
    )
    _, invalid_deploy_groups = \
        validate_given_deploy_groups(in_use_deploy_groups, [deploy_group])

    if len(invalid_deploy_groups) == 1:
        raise DeployGroupError("ERROR: These deploy groups are not currently "
                               "used anywhere: {}.\n"
                               "You probably need one of these in-use deploy "
                               "groups?:\n   {}"
                               .format(
                                   ",".join(invalid_deploy_groups),
                                   ",".join(in_use_deploy_groups),
                               ))


def paasta_wait_for_deployment(args):
    """Wrapping wait_for_deployment"""
    if args.verbose:
        log.setLevel(level=logging.DEBUG)
    else:
        log.setLevel(level=logging.INFO)

    service = args.service
    if service and service.startswith('services-'):
        service = service.split('services-', 1)[1]

    if args.git_url is None:
        args.git_url = get_git_url(service=service, soa_dir=args.soa_dir)

    args.commit = validate_git_sha(sha=args.commit, git_url=args.git_url)

    try:
        validate_service_name(service, soa_dir=args.soa_dir)
        validate_deploy_group(args.deploy_group, service, args.soa_dir)
        validate_git_sha_is_latest(
            args.commit, args.git_url,
            args.deploy_group, service,
        )
    except (GitShaError, DeployGroupError, NoSuchService) as e:
        paasta_print(PaastaColors.red(f'{e}'))
        return 1

    try:
        wait_for_deployment(
            service=service,
            deploy_group=args.deploy_group,
            git_sha=args.commit,
            soa_dir=args.soa_dir,
            timeout=args.timeout,
        )
        _log(
            service=service,
            component='deploy',
            line=("Deployment of {} for {} complete".
                  format(args.commit, args.deploy_group)),
            level='event',
        )

    except (KeyboardInterrupt, TimeoutError, NoSuchCluster):
        report_waiting_aborted(service, args.deploy_group)
        return 1

    return 0
