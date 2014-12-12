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
    guess_service_name, NoSuchService, PaastaColors


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


def yelp_sort(service_name):
    deploy_file = get_deploy_yaml(service_name)
    cluster_dict = OrderedDict()
    for namespace in deploy_file['pipeline']:
        namespace = namespace['instance_name']
        if (namespace != 'itest') and (namespace != 'registry'):
            cluster, instance = namespace.split('.')
            cluster_dict.setdefault(cluster, []).append(instance)

    for cluster in cluster_dict:
        for instance in cluster_dict[cluster]:
            yield "%s.%s" % (cluster, instance)


def paasta_status(args):
    """
    Print the status of a Yelp service running on PaaSTA
    """
    try:
        service_name = args.service or guess_service_name()
    except NoSuchService as service_not_found:
        print service_not_found
        exit(1)

    deployments_json = _get_deployments_json(DEFAULT_SOA_DIR)
    cluster_dict = {}
    for key in deployments_json:
        service, namespace = key.encode('utf8').split(':')
        if service == service_name:
            value = deployments_json[key].encode('utf8')
            sha = value[value.rfind('-') + 1:]
            cluster_dict[namespace[7:]] = sha

    clusters_seen = []

    if cluster_dict:
        for namespace in yelp_sort(service_name):
            if namespace in cluster_dict:
                cluster_name, instance = namespace.split('.')
                if cluster_name not in clusters_seen:
                    print "cluster: %s" % PaastaColors.green(cluster_name)
                    clusters_seen.append(cluster_name)
                print "\tinstance: %s" % instance
                print "\t\tversion: %s\n" % cluster_dict[namespace]
