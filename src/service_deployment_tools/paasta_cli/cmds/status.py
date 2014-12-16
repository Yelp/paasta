#!/usr/bin/env python
"""
Contains methods used by the paasta client to check the status of the service
on the PaaSTA stack
"""
from ordereddict import OrderedDict
import os

from service_configuration_lib import read_deploy
from service_deployment_tools.marathon_tools import \
    DEFAULT_SOA_DIR, _get_deployments_json
from service_deployment_tools.paasta_cli.utils import \
    guess_service_name, NoSuchService, PaastaColors, PaastaCheckMessages, \
    validate_service_name


def add_subparser(subparsers):
    status_parser = subparsers.add_parser(
        'status',
        description="PaaSTA client will attempt to deduce the SERVICE option if"
                    " none is provided.",
        help="Display the status of a Yelp service running on PaaSTA.")
    status_parser.add_argument('-s', '--service', help='The name of the service '
                                                       'you wish to inspect')
    status_parser.set_defaults(command=paasta_status)


def get_deploy_yaml(service_name):
    deploy_file_path = os.path.join(DEFAULT_SOA_DIR, service_name, "deploy.yaml")
    deploy_file = read_deploy(deploy_file_path)
    return deploy_file


def planned_deployments(deploy_file):
    """
    Yield deployment environments in the form 'cluster.instance' in the order
    they appear in the deploy.yaml file for service service_name
    :param service_name : name of the service for we wish to inspect
    :return : a series of strings of the form: 'cluster.instance', exits on
    error if deploy.yaml is not found
    """

    cluster_dict = OrderedDict()

    # Store cluster names in the order in which they are read
    # Clusters map to an ordered list of instances
    for entry in deploy_file['pipeline']:
        namespace = entry['instance_name']
        if (namespace != 'itest') and (namespace != 'registry'):
            cluster, instance = namespace.split('.')
            cluster_dict.setdefault(cluster, []).append(instance)

    # Yield deployment environments in the form of 'cluster.instance'
    for cluster in cluster_dict:
        for instance in cluster_dict[cluster]:
            yield "%s.%s" % (cluster, instance)


def paasta_status(args):
    """
    Print the status of a Yelp service running on PaaSTA
    """
    service_name = args.service or guess_service_name()
    try:
        validate_service_name(service_name)
    except NoSuchService as service_not_found:
        print service_not_found
        exit(1)

    deployments_json = _get_deployments_json(DEFAULT_SOA_DIR)
    if not deployments_json:
        print 'Failed to locate deployments.json in default SOA directory'
        exit(1)

    deploy_file = get_deploy_yaml(service_name)
    if not deploy_file:
        print PaastaCheckMessages.DEPLOY_YAML_MISSING
        exit(1)

    # Create a dictionary of actual $service_name Jenkins deployments
    actual_deployments = {}
    for key in deployments_json:
        service, namespace = key.encode('utf8').split(':')
        if service == service_name:
            value = deployments_json[key].encode('utf8')
            sha = value[value.rfind('-') + 1:]
            actual_deployments[namespace.replace('paasta-', '', 1)] = sha

    if actual_deployments:

        previous_cluster = ''

        # Get cluster.instance in the order in which they appear in deploy.yaml
        for namespace in planned_deployments(deploy_file):
            cluster_name, instance = namespace.split('.')

            # Previous deploy cluster printed isn't this, so print the name
            if cluster_name != previous_cluster:
                print "cluster: %s" % cluster_name
                previous_cluster = cluster_name

            # Case: service deployed to cluster.instance
            if namespace in actual_deployments:
                instance = PaastaColors.green(instance)
                version = actual_deployments[namespace]

            # Case: service NOT deployed to cluster.instance
            else:
                instance = PaastaColors.red(instance)
                version = 'None'

            print '\tinstance: %s' % instance
            print '\t\tversion: %s\n' % version
