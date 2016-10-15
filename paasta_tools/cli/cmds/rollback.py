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

from humanize import naturaltime

from paasta_tools.cli.cmds.mark_for_deployment import mark_for_deployment
from paasta_tools.cli.cmds.mark_for_deployment import wait_for_deployment
from paasta_tools.cli.utils import extract_tags
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import list_services
from paasta_tools.cli.utils import validate_full_git_sha
from paasta_tools.remote_git import list_remote_refs
from paasta_tools.utils import _log
from paasta_tools.utils import datetime_from_utc_to_local
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import format_table
from paasta_tools.utils import get_git_url
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import parse_timestamp
from paasta_tools.utils import TimeoutError


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'rollback',
        help='Rollback a docker image to a previous deploy',
        description=(
            "'paasta rollback' is a human-friendly tool for marking a particular "
            "docker image for deployment, which invokes a bounce. While the command "
            "is called 'rollback', it can be used to roll forward or back, as long "
            "as there is a docker image available for the input git SHA."
        ),
        epilog=(
            "This rollback command uses the Git control plane, which requires network "
            "connectivity as well as authorization to the git repo."
        ),
    )
    list_parser.add_argument(
        '-k', '--commit',
        help="Git SHA to mark for rollback. "
        "A commit to rollback to is required for paasta rollback to run. However if one is not provided, "
        "paasta rollback will instead output a list of valid git shas to rollback to.",
        required=False,
        type=validate_full_git_sha,
    ).completer = lazy_choices_completer(list_previously_deployed_shas)
    list_parser.add_argument(
        '-l', '--deploy-group',
        help='A deploy groups to roll back (e.g. "all.main", "all.main,all.canary") '
        'If no deploy groups specified the some will be suggested.',
        default='',
        required=False,
    ).completer = lazy_choices_completer(list_deploy_groups)
    list_parser.add_argument(
        '-s', '--service',
        help='Name of the service to rollback (e.g. "service1")',
    ).completer = lazy_choices_completer(list_services)
    list_parser.add_argument(
        '-y', '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    list_parser.set_defaults(command=paasta_rollback)


def list_previously_deployed_shas(parsed_args, **kwargs):
    service = parsed_args.service
    soa_dir = parsed_args.soa_dir
    return (sha for sha, _ in get_git_shas_for_service(service, parsed_args.deploy_group, soa_dir))


def get_git_shas_for_service(service, deploy_group, soa_dir):
    """Returns a dictionary of 2-tuples of the form (timestamp, deploy_group) for each deploy sha"""
    if service is None:
        return []
    git_url = get_git_url(service=service, soa_dir=soa_dir)
    previously_deployed_shas = {}
    for ref, sha in list_remote_refs(git_url).items():
        regex_match = extract_tags(ref)
        try:
            dg = regex_match['deploy_group']
            tstamp = regex_match['tstamp']
        except KeyError:
            pass
        else:
            # note that all strings are greater than ''
            if dg == deploy_group and tstamp > previously_deployed_shas.get(sha, ''):
                previously_deployed_shas[sha] = (tstamp, deploy_group)
    return previously_deployed_shas.items()


def list_previous_commits(service, deploy_group, soa_dir):
    def format_timestamp(tstamp):
        return naturaltime(datetime_from_utc_to_local(parse_timestamp(tstamp)))

    print "Please specify a commit to mark for rollback (-k, --commit). Below is a list of recent commits:"
    git_shas = sorted(get_git_shas_for_service(service, deploy_group, soa_dir), key=lambda x: x[1], reverse=True)[:10]
    rows = [('Timestamp -- UTC', 'Human time', 'deploy_group', 'Git SHA')]
    for sha, (timestamp, deploy_group) in git_shas:
        rows.extend([(timestamp, format_timestamp(timestamp), deploy_group, sha)])
    for line in format_table(rows):
        print line
    if len(git_shas) >= 2:
        print ""
        sha, (timestamp, deploy_group) = git_shas[1]
        print "For example, to use the second to last commit from %s used on %s, run:" % (
            format_timestamp(timestamp), PaastaColors.bold(deploy_group))
        print PaastaColors.bold("    paasta rollback --service %s --deploy-group %s --commit %s" % (
            service, deploy_group, sha))


def print_deploy_group_suggestions(all_deploy_groups):
    print ""
    for deploy_group in all_deploy_groups:
        print PaastaColors.bold("    " + " ".join(sys.argv[1:]) + " --deploy-group " + deploy_group)
    print ""


def paasta_rollback(args):
    """Call mark_for_deployment with rollback parameters
    :param args: contains all the arguments passed onto the script: service,
    deploy groups and sha. These arguments will be verified and passed onto
    mark_for_deployment.
    """
    soa_dir = args.soa_dir
    service = figure_out_service_name(args, soa_dir)

    git_url = get_git_url(service, soa_dir)

    all_deploy_groups = list_deploy_groups(service=service, soa_dir=soa_dir)
    deploy_group = args.deploy_group
    if deploy_group is None:
        print "A deploy group was not specified. Please pick one of the following:"
        print_deploy_group_suggestions(all_deploy_groups)
        return 1
    elif deploy_group not in all_deploy_groups:
        print "%s is not a valid deploy group. Please try a different invocation:" % deploy_group
        print_deploy_group_suggestions(all_deploy_groups)
        return 1

    commit = args.commit
    if not commit:
        list_previous_commits(service, deploy_group, soa_dir)
        return 1

    returncode = mark_for_deployment(
        git_url=git_url,
        service=service,
        deploy_group=deploy_group,
        commit=commit,
    )
    if returncode != 0:
        return returncode

    try:
        print "Waiting for deployment of {0} to {1} complete...".format(commit, deploy_group)
        wait_for_deployment(service=service,
                            deploy_group=deploy_group,
                            git_sha=commit,
                            soa_dir=args.soa_dir,
                            timeout=120)
        line = "Deployment of {0} to {1} complete".format(commit, args.deploy_group)
        _log(
            service=service,
            component='deploy',
            line=line,
            level='event'
        )
    except (KeyboardInterrupt, TimeoutError):
        print "Waiting for deployment aborted. PaaSTA will continue to try to deploy this code."
        print "If you wish to see the status, run:"
        print ""
        print "    paasta status -s %s -v" % service
        print ""
        returncode = 1

    return returncode
