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
"""Contains methods used by the paasta client to mark a docker image for
deployment to a cluster.instance.
"""
import logging

from paasta_tools import remote_git
from paasta_tools.cli.cmds.wait_for_deployment import wait_for_deployment
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import list_services
from paasta_tools.cli.utils import validate_full_git_sha
from paasta_tools.cli.utils import validate_given_deploy_groups
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.deployment_utils import get_currently_deployed_sha
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import format_tag
from paasta_tools.utils import get_git_url
from paasta_tools.utils import get_paasta_tag_from_deploy_group
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import TimeoutError


DEFAULT_DEPLOYMENT_TIMEOUT = 3600  # seconds


log = logging.getLogger(__name__)


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'mark-for-deployment',
        help='Mark a docker image for deployment in git',
        description=(
            "'paasta mark-for-deployment' uses Git as the control-plane, to "
            "signal to other PaaSTA components that a particular docker image "
            "is ready to be deployed."
        ),
        epilog=(
            "Note: Access and credentials to the Git repo of a service are required "
            "for this command to work."
        )
    )
    list_parser.add_argument(
        '-u', '--git-url',
        help=(
            'Git url for service -- where magic mark-for-deployment tags are pushed. '
            'Defaults to the normal git URL for the service.'),
        default=None
    )
    list_parser.add_argument(
        '-c', '-k', '--commit',
        help='Git sha to mark for deployment',
        required=True,
        type=validate_full_git_sha,
    )
    list_parser.add_argument(
        '-l', '--deploy-group', '--clusterinstance',
        help='Mark the service ready for deployment in this deploy group (e.g. '
             'cluster1.canary, cluster2.main). --clusterinstance is deprecated and '
             'should be replaced with --deploy-group',
        required=True,
    ).completer = lazy_choices_completer(list_deploy_groups)
    list_parser.add_argument(
        '-s', '--service',
        help='Name of the service which you wish to mark for deployment. Leading '
        '"services-" will be stripped.',
        required=True,
    ).completer = lazy_choices_completer(list_services)
    list_parser.add_argument(
        '--wait-for-deployment',
        help='Set to poll paasta and wait for the deployment to finish, '
             'the default strategy is to mark for deployment and exit straightaway',
        dest='block',
        action='store_true',
        default=False
    )
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
        '--auto-rollback',
        help='Automatically roll back to the previously deployed sha if the deployment '
             'times out or is canceled (ctrl-c). Only applicable with --wait-for-deployment. '
             'Defaults to false.',
        dest='auto_rollback',
        action='store_true',
        default=False
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
        help="Print out more output."
    )

    list_parser.set_defaults(command=paasta_mark_for_deployment)


def mark_for_deployment(git_url, deploy_group, service, commit):
    """Mark a docker image for deployment"""
    tag = get_paasta_tag_from_deploy_group(identifier=deploy_group, desired_state='deploy')
    remote_tag = format_tag(tag)
    ref_mutator = remote_git.make_force_push_mutate_refs_func(
        targets=[remote_tag],
        sha=commit,
    )
    try:
        remote_git.create_remote_refs(git_url=git_url, ref_mutator=ref_mutator, force=True)
    except Exception as e:
        loglines = ["Failed to mark %s for deployment in deploy group %s!" % (commit, deploy_group)]
        for line in str(e).split('\n'):
            loglines.append(line)
        return_code = 1
    else:
        loglines = ["Marked %s for deployment in deploy group %s" % (commit, deploy_group)]
        return_code = 0

    for logline in loglines:
        _log(
            service=service,
            line=logline,
            component='deploy',
            level='event',
        )
    return return_code


def paasta_mark_for_deployment(args):
    """Wrapping mark_for_deployment"""
    if args.verbose:
        log.setLevel(level=logging.DEBUG)
    else:
        log.setLevel(level=logging.INFO)

    service = args.service
    if service and service.startswith('services-'):
        service = service.split('services-', 1)[1]
    validate_service_name(service, soa_dir=args.soa_dir)

    in_use_deploy_groups = list_deploy_groups(
        service=service,
        soa_dir=args.soa_dir,
    )
    _, invalid_deploy_groups = validate_given_deploy_groups(in_use_deploy_groups, [args.deploy_group])

    if len(invalid_deploy_groups) == 1:
        print PaastaColors.red(
            "ERROR: These deploy groups are not currently used anywhere: %s.\n" % (",").join(invalid_deploy_groups))
        print PaastaColors.red(
            "This isn't technically wrong because you can mark-for-deployment before deploying there")
        print PaastaColors.red("but this is probably a typo. Did you mean one of these in-use deploy groups?:")
        print PaastaColors.red("   %s" % (",").join(in_use_deploy_groups))
        print ""
        print PaastaColors.red("Continuing regardless...")

    if args.git_url is None:
        args.git_url = get_git_url(service=service, soa_dir=args.soa_dir)

    old_git_sha = get_currently_deployed_sha(service=service, deploy_group=args.deploy_group)
    if old_git_sha == args.commit:
        print "Warning: The sha asked to be deployed already matches what is set to be deployed:"
        print old_git_sha
        print "Continuing anyway."

    ret = mark_for_deployment(
        git_url=args.git_url,
        deploy_group=args.deploy_group,
        service=service,
        commit=args.commit,
    )
    if args.block:
        try:
            wait_for_deployment(service=service,
                                deploy_group=args.deploy_group,
                                git_sha=args.commit,
                                soa_dir=args.soa_dir,
                                timeout=args.timeout)
            line = "Deployment of {0} for {1} complete".format(args.commit, args.deploy_group)
            _log(
                service=service,
                component='deploy',
                line=line,
                level='event'
            )
        except (KeyboardInterrupt, TimeoutError):
            if args.auto_rollback is True:
                if old_git_sha == args.commit:
                    print "Error: --auto-rollback was requested, but the previous sha"
                    print "is the same that was requested with --commit. Can't rollback"
                    print "automatically."
                else:
                    print "Auto-Rollback requested. Marking the previous sha"
                    print "(%s) for %s as desired." % (args.deploy_group, old_git_sha)
                    mark_for_deployment(
                        git_url=args.git_url,
                        deploy_group=args.deploy_group,
                        service=service,
                        commit=old_git_sha,
                    )
            else:
                print "Waiting for deployment aborted. PaaSTA will continue to try to deploy this code."
                print "If you wish to see the status, run:"
                print ""
                print "    paasta status -s %s -v" % service
                print ""
            ret = 1
        except NoInstancesFound:
            return 1
    if old_git_sha is not None and old_git_sha != args.commit:
        print ""
        print "If you wish to roll back, you can run:"
        print ""
        print PaastaColors.bold("    paasta rollback --service %s --deploy-group %s --commit %s " % (
            service, args.deploy_group, old_git_sha))
    return ret
