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

from service_configuration_lib import read_deploy

from paasta_tools.cli.utils import execute_paasta_serviceinit_on_remote_master
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import get_pipeline_url
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_services
from paasta_tools.cli.utils import PaastaCheckMessages
from paasta_tools.cli.utils import x_mark
from paasta_tools.marathon_tools import load_deployments_json
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_soa_cluster_deploy_files
from paasta_tools.utils import list_clusters
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors


def add_subparser(subparsers):
    status_parser = subparsers.add_parser(
        'status',
        help="Display the status of a PaaSTA service.",
        description=(
            "'paasta status' works by SSH'ing to remote PaaSTA masters and "
            "inspecting the local APIs, and reports on the overal health "
            "of a service."
        ),
        epilog=(
            "Note: This command requires SSH and sudo privileges on the remote PaaSTA "
            "masters."
        ),
    )
    status_parser.add_argument(
        '-v', '--verbose',
        action='count',
        dest="verbose",
        default=0,
        help="Print out more output regarding the state of the service. "
             "A second -v will also print the stdout/stderr tail.")
    status_parser.add_argument(
        '-s', '--service',
        help='The name of the service you wish to inspect'
    ).completer = lazy_choices_completer(list_services)
    status_parser.add_argument(
        '-c', '--clusters',
        help="A comma-separated list of clusters to view. Defaults to view all clusters.\n"
             "For example: --clusters norcal-prod,nova-prod"
    ).completer = lazy_choices_completer(list_clusters)
    status_parser.add_argument(
        '-i', '--instances',
        help="A comma-separated list of instances to view. Defaults to view all instances.\n"
             "For example: --instances canary,main"
    )  # No completer because we need to know service first and we can't until some other stuff has happened
    status_parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    status_parser.set_defaults(command=paasta_status)


def missing_deployments_message(service):
    jenkins_url = PaastaColors.cyan(
        'https://jenkins.yelpcorp.com/view/services-%s' % service)
    message = "%s No deployments in deployments.json yet.\n  " \
              "Has Jenkins run?\n  " \
              "Check: %s" % (x_mark(), jenkins_url)
    return message


def get_deploy_info(deploy_file_path):
    deploy_info = read_deploy(deploy_file_path)
    if not deploy_info:
        print PaastaCheckMessages.DEPLOY_YAML_MISSING
        exit(1)
    return deploy_info


def get_planned_deployments(service, soa_dir):
    for cluster, cluster_deploy_file in get_soa_cluster_deploy_files(
        service=service,
        soa_dir=soa_dir,
    ):
        for instance in get_deploy_info(cluster_deploy_file):
            yield '%s.%s' % (cluster, instance)


def list_deployed_clusters(pipeline, actual_deployments):
    """Returns a list of clusters that a service is deployed to given
    an input deploy pipeline and the actual deployments"""
    deployed_clusters = []
    for namespace in pipeline:
        cluster, instance = namespace.split('.')
        if namespace in actual_deployments:
            if cluster not in deployed_clusters:
                deployed_clusters.append(cluster)
    return deployed_clusters


def get_actual_deployments(service, soa_dir):
    deployments_json = load_deployments_json(service, soa_dir)
    if not deployments_json:
        sys.stderr.write("Warning: it looks like %s has not been deployed anywhere yet!\n" % service)
    # Create a dictionary of actual $service Jenkins deployments
    actual_deployments = {}
    for key in deployments_json:
        service, namespace = key.encode('utf8').split(':')
        if service == service:
            value = deployments_json[key]['docker_image'].encode('utf8')
            sha = value[value.rfind('-') + 1:]
            actual_deployments[namespace.replace('paasta-', '', 1)] = sha
    return actual_deployments


def report_status_for_cluster(service, cluster, deploy_pipeline, actual_deployments, instance_whitelist,
                              system_paasta_config, verbose=0):
    """With a given service and cluster, prints the status of the instances
    in that cluster"""
    print
    print "cluster: %s" % cluster
    seen_instances = []
    deployed_instances = []

    for namespace in deploy_pipeline:
        cluster_in_pipeline, instance = namespace.split('.')
        seen_instances.append(instance)

        if cluster_in_pipeline != cluster:
            continue
        if instance_whitelist and instance not in instance_whitelist:
            continue

        # Case: service deployed to cluster.instance
        if namespace in actual_deployments:
            deployed_instances.append(instance)

        # Case: service NOT deployed to cluster.instance
        else:
            print '  instance: %s' % PaastaColors.red(instance)
            print '    Git sha:    None (not deployed yet)'

    if len(deployed_instances) > 0:
        status = execute_paasta_serviceinit_on_remote_master('status', cluster, service, ','.join(deployed_instances),
                                                             system_paasta_config, stream=True, verbose=verbose)
        # Status results are streamed. This print is for possible error messages.
        if status is not None:
            for line in status.rstrip().split('\n'):
                print '    %s' % line

    print report_invalid_whitelist_values(instance_whitelist, seen_instances, 'instance')


def report_invalid_whitelist_values(whitelist, items, item_type):
    """Warns the user if there are entries in ``whitelist`` which don't
    correspond to any item in ``items``. Helps highlight typos.
    """
    return_string = ""
    bogus_entries = []
    for entry in whitelist:
        if entry not in items:
            bogus_entries.append(entry)
    if len(bogus_entries) > 0:
        return_string = (
            "\n"
            "Warning: This service does not have any %s matching these names:\n%s"
        ) % (item_type, ",".join(bogus_entries))
    return return_string


def report_status(service, deploy_pipeline, actual_deployments, cluster_whitelist, instance_whitelist,
                  system_paasta_config, verbose=0):
    pipeline_url = get_pipeline_url(service)
    print "Pipeline: %s" % pipeline_url

    deployed_clusters = list_deployed_clusters(deploy_pipeline, actual_deployments)
    for cluster in deployed_clusters:
        if not cluster_whitelist or cluster in cluster_whitelist:
            report_status_for_cluster(
                service=service,
                cluster=cluster,
                deploy_pipeline=deploy_pipeline,
                actual_deployments=actual_deployments,
                instance_whitelist=instance_whitelist,
                system_paasta_config=system_paasta_config,
                verbose=verbose,
            )

    print report_invalid_whitelist_values(cluster_whitelist, deployed_clusters, 'cluster')


def paasta_status(args):
    """Print the status of a Yelp service running on PaaSTA.
    :param args: argparse.Namespace obj created from sys.args by cli"""
    soa_dir = args.soa_dir
    service = figure_out_service_name(args, soa_dir)
    actual_deployments = get_actual_deployments(service, soa_dir)
    system_paasta_config = load_system_paasta_config()

    if args.clusters is not None:
        cluster_whitelist = args.clusters.split(",")
    else:
        cluster_whitelist = []
    if args.instances is not None:
        instance_whitelist = args.instances.split(",")
    else:
        instance_whitelist = []

    if actual_deployments:
        deploy_pipeline = list(get_planned_deployments(service, soa_dir))
        report_status(
            service=service,
            deploy_pipeline=deploy_pipeline,
            actual_deployments=actual_deployments,
            cluster_whitelist=cluster_whitelist,
            instance_whitelist=instance_whitelist,
            system_paasta_config=system_paasta_config,
            verbose=args.verbose,
        )
    else:
        print missing_deployments_message(service)
