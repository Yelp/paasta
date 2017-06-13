# !/usr/bin/env python
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
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
from argparse import RawTextHelpFormatter

from paasta_tools import chronos_tools
from paasta_tools.chronos_tools import get_related_jobs_configs
from paasta_tools.cli.cmds.status import get_actual_deployments
from paasta_tools.cli.cmds.status import get_planned_deployments
from paasta_tools.cli.cmds.status import list_deployed_clusters
from paasta_tools.cli.utils import execute_chronos_rerun_on_remote_master
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_instances
from paasta_tools.cli.utils import list_services
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_clusters
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import SPACER


def _get_default_execution_date():
    return datetime.datetime.utcnow()


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
        formatter_class=RawTextHelpFormatter,
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
        type=chronos_tools.parse_execution_date
    )
    rerun_parser.add_argument(
        '-y', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    rerun_parser.add_argument(
        '-t', '--rerun-type',
        dest="rerun_type",
        choices=['instance', 'graph'],
        help="Specify how to rerun jobs that have parent-dependencies.\n"
             "  - instance: rerun, as soon as possible, the required instance ONLY\n"
             "  - graph: will rerun, as soon as possible, ALL the instances related to the required instance\n"
             "    NOTE: the jobs rerun will respect the parents dependencies (topological order).\n"
             "    WARNING: it could be expensive in terms of resources and of time. Use it carefully.\n"
             "\n"
             "Example: Assume that we have 4 jobs (j1, j2, j3 and j4) with the following relations\n"
             "    j1 -> j2, j1 -> j3, j2 -> j3, j2 -> j4\n"
             "\n"
             "    Rerunning j2 wih --rerun-type=instance will rerun ONLY j2, j3 and j4 will not be re-ran\n"
             "    Rerunning j2 wih --rerun-type=graph will rerun j1, j2, j3 and j4 respecting the dependency order\n",
    )
    rerun_parser.add_argument(
        '-f', '--force-disabled',
        dest="force_disabled",
        action="store_true",
        default=False,
        help="Ignore the 'disabled' configuration of the service.\n"
             "If this is set, disabled services will still be run.\n"
             "If specified with '--rerun-type=graph', will also rerun disabled dependencies.\n",
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
    system_paasta_config = load_system_paasta_config()
    soa_dir = args.soa_dir
    service = figure_out_service_name(args, soa_dir)  # exit with an error if the service doesn't exist
    if args.execution_date:
        execution_date = args.execution_date
    else:
        execution_date = None

    all_clusters = list_clusters(soa_dir=soa_dir)
    actual_deployments = get_actual_deployments(service, soa_dir)  # cluster.instance: sha
    if actual_deployments:
        deploy_pipeline = list(get_planned_deployments(service, soa_dir))  # cluster.instance
        deployed_clusters = list_deployed_clusters(deploy_pipeline, actual_deployments)
        deployed_cluster_instance = _get_cluster_instance(actual_deployments.keys())

    if args.clusters is not None:
        clusters = args.clusters.split(",")
    else:
        clusters = deployed_clusters

    for cluster in clusters:
        paasta_print("cluster: %s" % cluster)

        if cluster not in all_clusters:
            paasta_print("  Warning: \"%s\" does not look like a valid cluster." % cluster)
            continue
        if cluster not in deployed_clusters:
            paasta_print("  Warning: service \"%s\" has not been deployed to \"%s\" yet." % (service, cluster))
            continue
        if not deployed_cluster_instance[cluster].get(args.instance, False):
            paasta_print(("  Warning: instance \"%s\" is either invalid "
                          "or has not been deployed to \"%s\" yet." % (args.instance, cluster)))
            continue

        try:
            chronos_job_config = chronos_tools.load_chronos_job_config(
                service, args.instance, cluster, load_deployments=False, soa_dir=soa_dir)
            if chronos_tools.uses_time_variables(chronos_job_config) and execution_date is None:
                paasta_print(("  Warning: \"%s\" uses time variables interpolation, "
                              "please supply a `--execution_date` argument." % args.instance))
                continue
        except NoConfigurationForServiceError as e:
            paasta_print("  Warning: %s" % e)
            continue
        if execution_date is None:
            execution_date = _get_default_execution_date()

        related_job_configs = get_related_jobs_configs(cluster, service, args.instance)

        if not args.rerun_type and len(related_job_configs) > 1:
            instance_names = sorted([
                '- {}{}{}'.format(srv, chronos_tools.INTERNAL_SPACER, inst)
                for srv, inst in related_job_configs
                if srv != service or inst != args.instance
            ])
            paasta_print(PaastaColors.red('  error'))
            paasta_print(
                'Instance {instance} has dependency relations with the following jobs:\n'
                '{relations}\n'
                '\n'
                'Please specify the rerun policy via --rerun-type argument'.format(
                    instance=args.instance,
                    relations='\n'.join(instance_names),
                ),
            )
            return

        rc, output = execute_chronos_rerun_on_remote_master(
            service=service,
            instancename=args.instance,
            cluster=cluster,
            verbose=args.verbose,
            execution_date=execution_date.strftime(chronos_tools.EXECUTION_DATE_FORMAT),
            system_paasta_config=system_paasta_config,
            run_all_related_jobs=args.rerun_type == 'graph',
            force_disabled=args.force_disabled,
        )
        if rc == 0:
            paasta_print(PaastaColors.green('  successfully created job'))
        else:
            paasta_print(PaastaColors.red('  error'))
            paasta_print(output)
