#!/usr/bin/env python
"""Contains methods used by the paasta client to check the status of the service
on the PaaSTA stack"""
from ordereddict import OrderedDict
from os.path import join

from argcomplete.completers import ChoicesCompleter

from service_configuration_lib import read_deploy
from paasta_tools.marathon_tools import \
    DEFAULT_SOA_DIR, _get_deployments_json
from paasta_tools.paasta_cli.utils import guess_service_name
from paasta_tools.paasta_cli.utils import list_services
from paasta_tools.paasta_cli.utils import NoSuchService
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
        if (namespace != 'itest') and (namespace != 'registry'):
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


def get_actual_deployments(service_name):
    deployments_json = _get_deployments_json(DEFAULT_SOA_DIR)
    if not deployments_json:
        print 'Failed to locate deployments.json in default SOA directory'
        exit(1)
    # Create a dictionary of actual $service_name Jenkins deployments
    actual_deployments = {}
    for key in deployments_json:
        service, namespace = key.encode('utf8').split(':')
        if service == service_name:
            value = deployments_json[key].encode('utf8')
            sha = value[value.rfind('-') + 1:]
            actual_deployments[namespace.replace('paasta-', '', 1)] = sha
    return actual_deployments


def report_status(service_name, deploy_pipeline, actual_deployments):
    jenkins_url = PaastaColors.cyan(
        'https://jenkins.yelpcorp.com/view/%s' % service_name)

    print "Pipeline: %s" % jenkins_url

    previous_cluster = ''

    # Get cluster.instance in the order in which they appear in deploy.yaml
    for namespace in deploy_pipeline:
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


def paasta_status(args):
    """Print the status of a Yelp service running on PaaSTA.
    :param args: argparse.Namespace obj created from sys.args by paasta_cli"""
    service_name = figure_out_service_name(args)
    actual_deployments = get_actual_deployments(service_name)
    deploy_info = get_deploy_info(service_name)

    if actual_deployments:
        deploy_pipeline = get_planned_deployments(deploy_info)
        report_status(service_name, deploy_pipeline, actual_deployments)
    else:
        print missing_deployments_message(service_name)
