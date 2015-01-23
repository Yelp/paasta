#!/usr/bin/env python
"""Contains methods used by the paasta client to check the status of the service
on the PaaSTA stack"""
from ordereddict import OrderedDict
from os.path import join

from argcomplete.completers import ChoicesCompleter

from service_configuration_lib import read_deploy
from paasta_tools.marathon_tools import \
    DEFAULT_SOA_DIR, _get_deployments_json
from paasta_tools.marathon_tools import list_clusters
from paasta_tools.paasta_cli.utils import execute_paasta_serviceinit_on_remote_master
from paasta_tools.paasta_cli.utils import guess_service_name
from paasta_tools.paasta_cli.utils import list_services
from paasta_tools.paasta_cli.utils import NoSuchService
from paasta_tools.utils import DEPLOY_PIPELINE_NON_DEPLOY_STEPS
from paasta_tools.paasta_cli.utils import PaastaCheckMessages
from paasta_tools.paasta_cli.utils import PaastaColors
from paasta_tools.paasta_cli.utils import validate_service_name
from paasta_tools.paasta_cli.utils import x_mark


def add_subparser(subparsers):
    status_parser = subparsers.add_parser(
        'status',
        description="PaaSTA client will attempt to deduce the SERVICE option if"
                    " none is provided.",
        help="Display the status of a Yelp service running on PaaSTA.")
    status_parser.add_argument(
        '-s', '--service',
        help='The name of the service you wish to inspect'
    ).completer = ChoicesCompleter(list_services())
    clusters_help = (
        'A comma separated list of clusters to view. Defaults to view all clusters. '
        'Try: --clusters norcal-prod,nova-prod'
    )
    status_parser.add_argument(
        '-c', '--clusters',
        help=clusters_help,
    ).completer = ChoicesCompleter(list_clusters())
    status_parser.set_defaults(command=paasta_status)


def missing_deployments_message(service_name):
    jenkins_url = PaastaColors.cyan(
        'https://jenkins.yelpcorp.com/view/services-%s' % service_name)
    message = "%s No deployments in deployments.json yet.\n  " \
              "Has Jenkins run?\n  " \
              "Check: %s" % (x_mark(), jenkins_url)
    return message


def get_deploy_info(service_name):
    deploy_file_path = join(DEFAULT_SOA_DIR, service_name, "deploy.yaml")
    deploy_info = read_deploy(deploy_file_path)
    if not deploy_info:
        print PaastaCheckMessages.DEPLOY_YAML_MISSING
        exit(1)
    return deploy_info


def get_planned_deployments(deploy_info):
    """Yield deployment environments in the form 'cluster.instance' in the order
    they appear in the deploy.yaml file for service service_name.
    :param service_name : name of the service for we wish to inspect
    :return : a series of strings of the form: 'cluster.instance', exits on
    error if deploy.yaml is not found"""
    cluster_dict = OrderedDict()

    # Store cluster names in the order in which they are read
    # Clusters map to an ordered list of instances
    for entry in deploy_info['pipeline']:
        namespace = entry['instancename']
        if namespace not in DEPLOY_PIPELINE_NON_DEPLOY_STEPS:
            cluster, instance = namespace.split('.')
            cluster_dict.setdefault(cluster, []).append(instance)

    # Yield deployment environments in the form of 'cluster.instance'
    for cluster in cluster_dict:
        for instance in cluster_dict[cluster]:
            yield "%s.%s" % (cluster, instance)


def figure_out_service_name(args):
    """Figures out and validates the input service name"""
    service_name = args.service or guess_service_name()
    try:
        validate_service_name(service_name)
    except NoSuchService as service_not_found:
        print service_not_found
        exit(1)
    return service_name


def list_deployed_clusters(pipeline, actual_deployments):
    """Returns a list of clusters that a service is deployed to given
    an input deploy pipeline and the actual deployments"""
    deployed_clusters = []
    # Get cluster.instance in the order in which they appear in deploy.yaml
    for namespace in pipeline:
        cluster_name, instance = namespace.split('.')
        if namespace in actual_deployments:
            if cluster_name not in deployed_clusters:
                deployed_clusters.append(cluster_name)
    return deployed_clusters


def get_actual_deployments(service_name):
    deployments_json = _get_deployments_json(DEFAULT_SOA_DIR)
    if not deployments_json:
        print 'Failed to locate deployments.json in default SOA directory'
        exit(1)
    # Create a dictionary of actual $service_name Jenkins deployments
    actual_deployments = {}
    for key in deployments_json['v1']:
        service, namespace = key.encode('utf8').split(':')
        if service == service_name:
            value = deployments_json['v1'][key]['docker_image'].encode('utf8')
            sha = value[value.rfind('-') + 1:]
            actual_deployments[namespace.replace('paasta-', '', 1)] = sha
    return actual_deployments


def report_status_for_cluster(service, cluster, deploy_pipeline, actual_deployments):
    """With a given service and cluster, prints the status of the instances
    in that cluster"""
    # Get cluster.instance in the order in which they appear in deploy.yaml
    print
    print "cluster: %s" % cluster
    for namespace in deploy_pipeline:
        cluster_in_pipeline, instance = namespace.split('.')

        if cluster_in_pipeline != cluster:
            # This function only prints things that are relevant to cluster_name
            # We skip anything not in this cluster
            continue

        # Case: service deployed to cluster.instance
        if namespace in actual_deployments:
            unformatted_instance = instance
            instance = PaastaColors.blue(instance)
            version = actual_deployments[namespace]
            # TODO: Perform sanity checks once per cluster instead of for each namespace
            status = execute_paasta_serviceinit_on_remote_master('status', cluster, service, unformatted_instance)

        # Case: service NOT deployed to cluster.instance
        else:
            instance = PaastaColors.red(instance)
            version = 'None'
            status = None

        print '\tinstance: %s' % instance
        print '\t\tversion: %s' % version
        if status is not None:
            for line in status.rstrip().split('\n'):
                print '\t\t%s' % line


def report_bogus_filters(cluster_filter, deployed_clusters):
    """Warns the user if the filter used is not even in the deployed
    list. Helps pick up typos"""
    return_string = ""
    if cluster_filter is not None:
        bogus_clusters = []
        for c in cluster_filter:
            if c not in deployed_clusters:
                bogus_clusters.append(c)
        if len(bogus_clusters) > 0:
            return_string = (
                "\n"
                "Warning: The following clusters in the filter look bogus, this service\n"
                "is not deployed to the following cluster(s):\n%s"
            ) % ",".join(bogus_clusters)
    return return_string


def report_status(service_name, deploy_pipeline, actual_deployments, cluster_filter):
    jenkins_url = PaastaColors.cyan(
        'https://jenkins.yelpcorp.com/view/%s' % service_name)

    print "Pipeline: %s" % jenkins_url

    deployed_clusters = list_deployed_clusters(deploy_pipeline, actual_deployments)
    for cluster in deployed_clusters:
        if cluster_filter is None or cluster in cluster_filter:
            report_status_for_cluster(service_name, cluster, deploy_pipeline, actual_deployments)

    print report_bogus_filters(cluster_filter, deployed_clusters)


def paasta_status(args):
    """Print the status of a Yelp service running on PaaSTA.
    :param args: argparse.Namespace obj created from sys.args by paasta_cli"""
    service_name = figure_out_service_name(args)
    actual_deployments = get_actual_deployments(service_name)
    deploy_info = get_deploy_info(service_name)
    if args.clusters is not None:
        cluster_filter = args.clusters.split(",")
    else:
        cluster_filter = None

    if actual_deployments:
        deploy_pipeline = list(get_planned_deployments(deploy_info))
        report_status(service_name, deploy_pipeline, actual_deployments, cluster_filter)
    else:
        print missing_deployments_message(service_name)
