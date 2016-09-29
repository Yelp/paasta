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
from paasta_tools.cli.utils import list_services
from paasta_tools.cli.utils import validate_given_deploy_groups
from paasta_tools.generate_deployments_for_service import get_instance_config_for_service
from paasta_tools.remote_git import list_remote_refs
from paasta_tools.utils import datetime_from_utc_to_local
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import format_table
from paasta_tools.utils import get_git_url
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
    ).completer = lazy_choices_completer(list_previously_deployed_shas)
    list_parser.add_argument(
        '-d', '--deploy-groups',
        help='Mark one or more deploy groups to roll back (e.g. '
        '"all.main", "all.main,all.canary"). If no deploy groups specified,'
        ' all deploy groups for that service are rolled back',
        default='',
        required=False,
    )
    list_parser.add_argument(
        '-s', '--service',
        help='Name of the service to rollback (e.g. "service1")',
    ).completer = lazy_choices_completer(list_services)
    list_parser.add_argument(
        '-y', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    list_parser.set_defaults(command=paasta_rollback)


def list_previously_deployed_shas(parsed_args, **kwargs):
    service = parsed_args.service
    soa_dir = parsed_args.soa_dir
    deploy_groups = {deploy_group for deploy_group in parsed_args.deploy_groups.split(',') if deploy_group}
    return (sha for sha, _ in get_git_shas_for_service(service, deploy_groups, soa_dir))


def get_git_shas_for_service(service, deploy_groups, soa_dir):
    """Returns a list of 2-tuples of the form (sha, timestamp) for each deploy tag in a service's git
    repository"""
    if service is None:
        return []
    git_url = get_git_url(service=service, soa_dir=soa_dir)
    all_deploy_groups = {config.get_deploy_group() for config in get_instance_config_for_service(
        service=service,
        soa_dir=soa_dir,
    )}
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
            # note that all strings are greater than ''
            if deploy_group in deploy_groups and tstamp > previously_deployed_shas.get(sha, ''):
                previously_deployed_shas[sha] = tstamp
    return previously_deployed_shas.items()


def list_previous_commits(service, deploy_groups, any_given_deploy_groups, soa_dir):
    def format_timestamp(tstamp):
        return naturaltime(datetime_from_utc_to_local(parse_timestamp(tstamp)))

    print "Please specify a commit to mark for rollback (-k, --commit). Below is a list of recent commits:"
    git_shas = sorted(get_git_shas_for_service(service, deploy_groups, soa_dir), key=lambda x: x[1], reverse=True)[:10]
    rows = [('Timestamp -- UTC', 'Git SHA')]
    rows.extend([('%s (%s)' % (timestamp, format_timestamp(timestamp)), sha) for sha, timestamp in git_shas])
    for line in format_table(rows):
        print line
    if len(git_shas) >= 2:
        sha, tstamp = git_shas[1]
        deploy_groups_arg_line = '-d %s ' % ','.join(deploy_groups) if any_given_deploy_groups else ''
        print "For example, to roll back to the second to last commit from %s, run:" % format_timestamp(tstamp)
        print PaastaColors.bold("    paasta rollback -s %s %s-k %s" % (service, deploy_groups_arg_line, sha))


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

    service_deploy_groups = {config.get_deploy_group() for config in get_instance_config_for_service(
        service=service,
        soa_dir=soa_dir,
    )}
    deploy_groups, invalid = validate_given_deploy_groups(service_deploy_groups, given_deploy_groups)

    if len(invalid) > 0:
        print PaastaColors.yellow("These deploy groups are not valid and will be skipped: %s.\n" % (",").join(invalid))

    if len(deploy_groups) == 0:
        print PaastaColors.red("ERROR: No valid deploy groups specified for %s.\n" % (service))
        return 1

    commit = args.commit
    if not commit:
        list_previous_commits(service, deploy_groups, bool(given_deploy_groups), soa_dir)
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
