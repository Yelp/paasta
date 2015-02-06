#!/usskr/bin/env python
"""PaaSTA log reader for humans"""
import sys
import argparse
import logging

from argcomplete.completers import ChoicesCompleter

from paasta_tools.marathon_tools import list_clusters
from paasta_tools.paasta_cli.utils import figure_out_service_name
from paasta_tools.paasta_cli.utils import figure_out_cluster
from paasta_tools.paasta_cli.utils import list_services
from paasta_tools.paasta_cli.utils import PaastaColors

DEFAULT_COMPONENTS = ['build', 'deploy', 'app_output', 'lb_errors', 'monitoring']
LOG_COMPONENTS = {
    'build': {
        'color': PaastaColors.blue,
        'help': 'Jenkins build jobs output, like the itest, promotion, security checks, etc.',
        'command': 'NA - TODO: tee jenkins build steps into scribe PAASTA-201',
        'source_env': 'devc',
    },
    'deploy': {
        'color': PaastaColors.cyan,
        'help': 'Output from the paasta deploy code. (setup_marathon_job, bounces, etc)',
        'command': 'NA - TODO: tee deploy logs into scribe PAASTA-201',
    },
    'app_output': {
        'color': PaastaColors.bold,
        'help': 'Stderr and stdout of the actual process spawned by Mesos',
        'command': 'NA - PAASTA-78',
    },
    'app_request': {
        'color': PaastaColors.bold,
        'help': 'The request log for the service. Defaults to "service_NAME_requests"',
        'command': 'scribe_reader -e ENV -f service_example_happyhour_requests',
    },
    'app_errors': {
        'color': PaastaColors.red,
        'help': 'Application error log, defaults to "stream_service_NAME_errors"',
        'command': 'scribe_reader -e ENV -f stream_service_SERVICE_errors',
    },
    'lb_requests': {
        'color': PaastaColors.bold,
        'help': 'All requests from Smartstack haproxy',
        'command': 'NA - TODO: SRV-1130',
    },
    'lb_errors': {
        'color': PaastaColors.red,
        'help': 'Logs from Smartstack haproxy that have 400-500 error codes',
        'command': 'scribereader -e ENV -f stream_service_errors | grep SERVICE.instance',
    },
    'monitoring': {
        'color': PaastaColors.green,
        'help': 'Logs from Sensu checks for the service',
        'command': 'NA - TODO log mesos healthcheck and sensu stuff.',
    },
}


def add_subparser(subparsers):
    status_parser = subparsers.add_parser(
        'logs',
        description="Gets logs relevant to a service across the PaaSTA components",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help="Gets logs relevant to a service across the PaaSTA components.")
    status_parser.add_argument(
        '-s', '--service',
        help='The name of the service you wish to inspect. Defaults to autodetect.'
    ).completer = ChoicesCompleter(list_services())
    components_help = 'A comma separated list of the components you want logs for.'
    status_parser.add_argument(
        '-c', '--components',
        help=components_help,
    ).completer = ChoicesCompleter(LOG_COMPONENTS.keys())
    cluster_help = 'The cluster to see relevant logs for. Defaults to the local cluster.'
    status_parser.add_argument(
        '-l', '--cluster',
        help=cluster_help,
    ).completer = ChoicesCompleter(list_clusters())
    status_parser.add_argument(
        '-f', '-F', '--tail', dest='tail', action='store_true', default=True,
        help='Stream the logs and follow it for more data',
    )
    status_parser.add_argument('-d', '--debug', action='store_true',
                               dest='debug', default=False,
                               help='Enable debug logging')
    default_component_string = ','.join(DEFAULT_COMPONENTS)
    component_descriptions = build_component_descriptions(LOG_COMPONENTS)
    epilog = 'COMPONENTS\n' \
             'There are many possible components of Paasta logs that you might be interested in:\n' \
             'Run --list-components to see all available log components.\n' \
             'If unset, the default components are:\n\t%s\n' \
             'So the default behavior of `paasta logs` will be to tail those logs.\n\n' \
             'Here is a list of all components and what they are:\n%s\n\n' \
             % (default_component_string, component_descriptions)
    status_parser.epilog = epilog
    status_parser.set_defaults(command=paasta_logs)


def build_component_descriptions(components):
    """Returns a colored description string for every log component
    based on its help attribute"""
    output = []
    for k, v in components.iteritems():
        output.append("     %s: %s" % (v['color'](k), v['help']))
    return '\n'.join(output)


def prefix(input_string, component):
    """Returns a colored string with the right colored prefix with a given component"""
    return "%s: %s" % (LOG_COMPONENTS[component]['color'](component), input_string)


def determine_scribereader_envs(components, cluster):
    """Returns a list of environments that scribereader needs to connect
    to based on a given list of components and the cluster involved.

    Some components are in certain environments, regardless of the cluster.
    Some clusters do not match up with the scribe environment names, so
    we figure that out here"""
    envs = []
    for component in components:
        # If a component has a 'source_env', we use that
        # otherwise we lookup what scribe env is associated with a given cluster
        env = LOG_COMPONENTS[component].get('source_env', cluster_to_scribe_env(cluster))
        envs.append(env)
    return set(envs)


def cluster_to_scribe_env(cluster):
    """Looks up the particular scribe env associated with a given paasta
    cluster."""
    lookup_map = {
        'paasta-cluster': 'scribe-environment',
    }
    env = lookup_map.get(cluster, None)
    if env is None:
        print "I don't know where scribe logs for %s live?" % cluster
        sys.exit(1)
    else:
        return env


def scribe_tail(env, service, components, cluster):
    """Calls scribetailer for a particular environment.
    outputs lines that match for the requested cluster and components
    in a pretty way"""
    log.info("Going to scribetail in %s" % env)
    # TODO: Replace with real scribe-tailer
    for component in components:
        command_string = "Command: %s" % LOG_COMPONENTS[component]['command']
        print prefix(command_string, component)


def tail_paasta_logs(service, components, cluster):
    """Sergeant function for spawning off all the right scribe tailing functions"""
    envs = determine_scribereader_envs(components, cluster)
    log.info("Would connect to these envs to tail scribe logs: %s" % envs)
    for env in envs:
        # TODO: do this in parallel
        scribe_tail(env, service, components, cluster)


def paasta_logs(args):
    """Print the logs for as Paasta service.
    :param args: argparse.Namespace obj created from sys.args by paasta_cli"""
    log = logging.getLogger('__main__')
    service_name = figure_out_service_name(args)
    cluster = figure_out_cluster(args)
    if args.components is not None:
        components = args.components.split(",")
    else:
        components = DEFAULT_COMPONENTS

    if args.debug:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.WARN)

    log.info("Going to get logs for %s on cluster %s" % (service_name, cluster))
    if args.tail:
        tail_paasta_logs(service_name, components, cluster)
    else:
        print "Non-tailing actions are not yet supported"
