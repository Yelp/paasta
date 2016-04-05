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
import argparse
import datetime

from paasta_tools.cli.cmds.status import get_actual_deployments
from paasta_tools.cli.cmds.status import get_planned_deployments
from paasta_tools.cli.cmds.status import list_deployed_clusters
from paasta_tools.cli.utils import execute_chronos_rerun_on_remote_master
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_instances
from paasta_tools.cli.utils import list_services
from paasta_tools.utils import list_clusters
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import SPACER


EXECUTION_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def type_execution_date(value):
    try:
        parsed = datetime.datetime.strptime(value, EXECUTION_DATE_FORMAT)
    except ValueError:
        raise argparse.ArgumentTypeError('must be in the format "%s"' % EXECUTION_DATE_FORMAT)
    return parsed


def add_subparser(subparsers):
    rerun_parser = subparsers.add_parser(
        'rerun',
        help="Re-run a scheduled PaaSTA job",
        description=(
            "'paasta rerun' creates a copy of the specified PaaSTA scheduled job and executes it immediately. "
            "Parent-dependent relationships are ignored: 'pasta rerun' only executes individual jobs."
        ),
        epilog=(
            "Note: This command requires SSH and sudo privileges on the remote PaaSTA "
            "masters."
        ),
    )
    rerun_parser.add_argument(
        '-v', '--verbose',
        action='count',
        dest="verbose",
        default=0,
        help="Print out more output regarding the operation."
    )
    rerun_parser.add_argument(
        '-s', '--service',
        help='The name of the service you wish to operate on.',
        required=True,
    ).completer = lazy_choices_completer(list_services)
    rerun_parser.add_argument(
        '-i', '--instance',
        help='Name of the scheduled job (instance) that you want to rerun.',
        required=True,
    ).completer = lazy_choices_completer(list_instances)
    rerun_parser.add_argument(
        '-c', '--clusters',
        help="A comma-separated list of clusters to rerun the job on. Defaults to rerun on all clusters.\n"
             "For example: --clusters norcal-prod,nova-prod"
    ).completer = lazy_choices_completer(list_clusters)
    rerun_parser.add_argument(
        '-d', '--execution_date',
        help="The date the job should be rerun for. Expected in the format %%Y-%%m-%%dT%%H:%%M:%%S .",
        required=True,
        type=type_execution_date
    )
    rerun_parser.set_defaults(command=paasta_rerun)


def _get_cluster_instance(cluster_dot_instance_list):
    """given a list of cluster.instance, returns
    a nested dict of ['cluster']['instance'] = True"""
    cluster_instance_dict = {}
    for namespace in cluster_dot_instance_list:
        cluster_in_pipeline, instance = namespace.split(SPACER, 1)
        instance_dict = cluster_instance_dict.get(cluster_in_pipeline, {})
        instance_dict[instance] = True
        cluster_instance_dict[cluster_in_pipeline] = instance_dict
    return cluster_instance_dict


def paasta_rerun(args):
    """Reruns a Chronos job.
    :param args: argparse.Namespace obj created from sys.args by cli"""
    service = figure_out_service_name(args)  # exit with an error if the service doesn't exist
    execution_date = args.execution_date.strftime(EXECUTION_DATE_FORMAT)

    all_clusters = list_clusters()
    actual_deployments = get_actual_deployments(service)  # cluster.instance: sha
    if actual_deployments:
        deploy_pipeline = list(get_planned_deployments(service))  # cluster.instance
        deployed_clusters = list_deployed_clusters(deploy_pipeline, actual_deployments)
        deployed_cluster_instance = _get_cluster_instance(actual_deployments.keys())

    if args.clusters is not None:
        clusters = args.clusters.split(",")
    else:
        clusters = deployed_clusters

    for cluster in clusters:
        print "cluster: %s" % cluster

        if cluster not in all_clusters:
            print "  Warning: \"%s\" does not look like a valid cluster..." % cluster
            continue
        if cluster not in deployed_clusters:
            print "  Warning: service \"%s\" has not been deployed to \"%s\" yet..." % (service, cluster)
            continue
        if not deployed_cluster_instance[cluster].get(args.instance, False):
            print ("  Warning: instance \"%s\" is either invalid "
                   "or has not been deployed to \"%s\" yet..." % (args.instance, cluster))
            continue

        rc, output = execute_chronos_rerun_on_remote_master(service,
                                                            args.instance,
                                                            cluster,
                                                            verbose=args.verbose,
                                                            execution_date=execution_date)
        if rc == 0:
            print PaastaColors.green('  success')
        else:
            print PaastaColors.red('  error')
            print output
