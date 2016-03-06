#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
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

from paasta_tools.cli.cmds.mark_for_deployment import mark_for_deployment
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_services
from paasta_tools.generate_deployments_for_service import get_instance_config_for_service
from paasta_tools.utils import get_git_url
from paasta_tools.utils import PaastaColors


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
        help='Git SHA to mark for rollback',
        required=True,
    )
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
    list_parser.set_defaults(command=paasta_rollback)


def validate_given_deploy_groups(service_deploy_groups, args_deploy_groups):
    """Given two lists of deploy groups, return the intersection and difference between them.

    :param service_deploy_groups: instances actually belonging to a service
    :param args_deploy_groups: the desired instances
    :returns: a tuple with (common, difference) indicating deploy groups common in both
    lists and those only in args_deploy_groups
    """
    if len(args_deploy_groups) is 0:
        valid_deploy_groups = set(service_deploy_groups)
        invalid_deploy_groups = set([])
    else:
        valid_deploy_groups = set(args_deploy_groups).intersection(service_deploy_groups)
        invalid_deploy_groups = set(args_deploy_groups).difference(service_deploy_groups)

    return valid_deploy_groups, invalid_deploy_groups


def paasta_rollback(args):
    """Call mark_for_deployment with rollback parameters
    :param args: contains all the arguments passed onto the script: service,
    deploy groups and sha. These arguments will be verified and passed onto
    mark_for_deployment.
    """
    service = figure_out_service_name(args)
    git_url = get_git_url(service)
    commit = args.commit
    given_deploy_groups = [deploy_group for deploy_group in args.deploy_groups.split(",") if deploy_group]

    service_deploy_groups = set(config.get_deploy_group() for config in get_instance_config_for_service(
        soa_dir=DEFAULT_SOA_DIR,
        service=service,
    ))
    deploy_groups, invalid = validate_given_deploy_groups(service_deploy_groups, given_deploy_groups)
    if len(invalid) > 0:
        print PaastaColors.yellow("These deploy groups are not valid and will be skipped: %s.\n" % (",").join(invalid))

    if len(deploy_groups) == 0:
        print PaastaColors.red("ERROR: No valid deploy groups specified for %s.\n" % (service))
        returncode = 1

    for deploy_group in deploy_groups:
        returncode = mark_for_deployment(
            git_url=git_url,
            service=service,
            deploy_group=deploy_group,
            commit=commit,
        )

    return returncode
