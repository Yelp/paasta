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
from humanize import naturaltime

from paasta_tools.cli.cmds.mark_for_deployment import mark_for_deployment
from paasta_tools.cli.utils import extract_tags
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import list_services
from paasta_tools.cli.utils import validate_full_git_sha
from paasta_tools.cli.utils import validate_given_deploy_groups
from paasta_tools.remote_git import list_remote_refs
from paasta_tools.utils import datetime_from_utc_to_local
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import format_table
from paasta_tools.utils import get_git_url
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import parse_timestamp


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
        '-l', '--deploy-groups',
        help='Mark one or more deploy groups to roll back (e.g. '
        '"all.main", "all.main,all.canary"). If no deploy groups specified,'
        ' all deploy groups for that service are rolled back',
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
    list_parser.add_argument(
        '-f', '--force',
        help=('Do not check if Git SHA was marked for deployment previously.'),
        action='store_true',
    )
    list_parser.set_defaults(command=paasta_rollback)


def list_previously_deployed_shas(parsed_args, **kwargs):
    service = parsed_args.service
    soa_dir = parsed_args.soa_dir
    deploy_groups = {deploy_group for deploy_group in parsed_args.deploy_groups.split(',') if deploy_group}
    return (sha for sha in get_git_shas_for_service(service, deploy_groups, soa_dir))


def get_git_shas_for_service(service, deploy_groups, soa_dir):
    """Returns a dictionary of 2-tuples of the form (timestamp, deploy_group) for each deploy sha"""
    if service is None:
        return []
    git_url = get_git_url(service=service, soa_dir=soa_dir)
    all_deploy_groups = list_deploy_groups(
        service=service,
        soa_dir=soa_dir,
    )
    deploy_groups, _ = validate_given_deploy_groups(all_deploy_groups, deploy_groups)
    previously_deployed_shas = {}
    for ref, sha in list_remote_refs(git_url).items():
        regex_match = extract_tags(ref)
        try:
            deploy_group = regex_match['deploy_group']
            tstamp = regex_match['tstamp']
        except KeyError:
            pass
        else:
            # Now we filter and dedup by picking the most recent sha for a deploy group
            # Note that all strings are greater than ''
            if deploy_group in deploy_groups:
                tstamp_so_far = previously_deployed_shas.get(sha, ('all', ''))[1]
                if tstamp > tstamp_so_far:
                    previously_deployed_shas[sha] = (tstamp, deploy_group)
    return previously_deployed_shas


def list_previous_commits(service, deploy_groups, any_given_deploy_groups, git_shas):
    def format_timestamp(tstamp):
        return naturaltime(datetime_from_utc_to_local(parse_timestamp(tstamp)))

    paasta_print('Below is a list of recent commits:')
    git_shas = sorted(git_shas.items(), key=lambda x: x[1], reverse=True)[:10]
    rows = [('Timestamp -- UTC', 'Human time', 'deploy_group', 'Git SHA')]
    for sha, (timestamp, deploy_group) in git_shas:
        rows.extend([(timestamp, format_timestamp(timestamp), deploy_group, sha)])
    for line in format_table(rows):
        paasta_print(line)
    if len(git_shas) >= 2:
        sha, (timestamp, deploy_group) = git_shas[1]
        deploy_groups_arg_line = '-l %s ' % ','.join(deploy_groups) if any_given_deploy_groups else ''
        paasta_print("\nFor example, to use the second to last commit from {} used on {}, run:".format(
            format_timestamp(timestamp), PaastaColors.bold(deploy_group),
        ))
        paasta_print(PaastaColors.bold(f"    paasta rollback -s {service} {deploy_groups_arg_line}-k {sha}"))


def paasta_rollback(args):
    """Call mark_for_deployment with rollback parameters
    :param args: contains all the arguments passed onto the script: service,
    deploy groups and sha. These arguments will be verified and passed onto
    mark_for_deployment.
    """
    soa_dir = args.soa_dir
    service = figure_out_service_name(args, soa_dir)

    git_url = get_git_url(service, soa_dir)
    given_deploy_groups = {deploy_group for deploy_group in args.deploy_groups.split(",") if deploy_group}

    all_deploy_groups = list_deploy_groups(service=service, soa_dir=soa_dir)
    deploy_groups, invalid = validate_given_deploy_groups(all_deploy_groups, given_deploy_groups)

    if len(invalid) > 0:
        paasta_print(
            PaastaColors.yellow(
                "These deploy groups are not valid and will be skipped: %s.\n" % (",").join(invalid),
            ),
        )

    if len(deploy_groups) == 0:
        paasta_print(PaastaColors.red("ERROR: No valid deploy groups specified for %s.\n" % (service)))
        return 1

    git_shas = get_git_shas_for_service(service, deploy_groups, soa_dir)
    commit = args.commit
    if not commit:
        paasta_print("Please specify a commit to mark for rollback (-k, --commit).")
        list_previous_commits(service, deploy_groups, bool(given_deploy_groups), git_shas)
        return 1
    elif commit not in git_shas and not args.force:
        paasta_print(PaastaColors.red("This Git SHA has never been deployed before."))
        paasta_print("Please double check it or use --force to skip this verification.\n")
        list_previous_commits(service, deploy_groups, bool(given_deploy_groups), git_shas)
        return 1

    returncode = 0

    for deploy_group in deploy_groups:
        returncode = max(
            mark_for_deployment(
                git_url=git_url,
                service=service,
                deploy_group=deploy_group,
                commit=commit,
            ),
            returncode,
        )

    return returncode
