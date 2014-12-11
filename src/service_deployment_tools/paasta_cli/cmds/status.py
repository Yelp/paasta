#!/usr/bin/env python
"""
Contains methods used by the paasta client to check the status of the service
on the PaaSTA stack
"""
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
        service, deployed_to = key.encode('utf8').split(':')
        if service == service_name:
            cluster, instance = deployed_to.split('.')
            value = deployments_json[key].encode('utf8')
            sha = value[value.rfind('-') + 1:]
            cluster_dict.setdefault(cluster, []).append(
                {'instance': instance, 'version': sha})

    if cluster_dict:
        print "\nRunning instance(s) of %s:\n" \
              % PaastaColors.cyan(service_name)
        for cluster_instance in sorted(cluster_dict):
            print "cluster: %s" % PaastaColors.green(cluster_instance)
            for service_instance in sorted(cluster_dict[cluster_instance]):
                print "\tinstance: %s" % service_instance['instance']
                print "\t\tversion: %s\n" % service_instance['version']
