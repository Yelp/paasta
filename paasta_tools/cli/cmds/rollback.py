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

"""Mechanism to rollback to a previous deployed version.
"""
import sys

from paasta_tools.utils import get_git_url
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_services
from paasta_tools.cli.utils import list_instances
from paasta_tools.cli.cmds.mark_for_deployment import mark_for_deployment
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_all_instances_for_service
from paasta_tools.utils import PaastaColors


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'rollback',
        description='Rollback a docker image to a previous deploy',
        help='Rollback a docker image to a previous deploy')

    list_parser.add_argument('-k', '--commit',
                             help='Git SHA to mark for rollback',
                             required=True,
                             )
    list_parser.add_argument('-i', '--instances',
                             help='Mark one or more instances to roll back (e.g. '
                             '"canary", "canary,main"). If no instances specified,'
                             ' all instances for that service are rolled back',
                             default='',
                             required=False,
                             ).completer = lazy_choices_completer(list_instances)
    list_parser.add_argument('-c', '--cluster',
                             help='Mark the cluster to rollback (e.g. '
                             'cluster1)',
                             required=True,
                             ).completer = lazy_choices_completer(list_clusters)
    list_parser.add_argument('-s', '--service',
                             help='Name of the service to rollback (e.g. '
                             'service1)'
                             ).completer = lazy_choices_completer(list_services)

    list_parser.set_defaults(command=paasta_rollback)


def validate_given_instances(service_instances, args_instances):
    """Given two lists of instances, return the intersection and difference between them.

    :param service_instances: instances actually belonging to a service
    :param args_instances: the desired instances
    :returns: a tuple with (common, difference) indicating instances common in both
    lists and those only in args_instances
    """
    if len(args_instances) is 0:
        valid_instances = set(service_instances)
        invalid_instances = set([])
    else:
        valid_instances = set(args_instances).intersection(service_instances)
        invalid_instances = set(args_instances).difference(service_instances)

    return valid_instances, invalid_instances


def paasta_rollback(args):
    """Call mark_for_deployment with rollback parameters
    :param args: contains all the arguments passed onto the script: service,
    cluster, instance and sha. These arguments will be verified and passed onto
    mark_for_deployment.
    """
    service = figure_out_service_name(args)
    cluster = args.cluster
    git_url = get_git_url(service)
    commit = args.commit
    given_instances = args.instances.split(",")

    if cluster in list_clusters(service):
        service_instances = list_all_instances_for_service(service)
        instances, invalid = validate_given_instances(service_instances, given_instances)

        if len(invalid) > 0:
            print PaastaColors.yellow("These instances are not valid and will be skipped: %s.\n" % (",").join(invalid))

        if len(instances) is 0:
            print PaastaColors.red("ERROR: No valid instances specified for %s.\n" % (service))
            returncode = 1

        for instance in instances:
            returncode = mark_for_deployment(
                git_url=git_url,
                cluster=cluster,
                instance=instance,
                service=service,
                commit=commit,
            )
    else:
        print PaastaColors.red("ERROR: The service %s is not deployed into cluster %s.\n" % (service, cluster))
        returncode = 1

    sys.exit(returncode)
