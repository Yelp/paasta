#!/usskr/bin/env python
"""PaaSTA log reader for humans"""
import argparse

from argcomplete.completers import ChoicesCompleter

from paasta_tools.paasta_cli.utils import figure_out_service_name
from paasta_tools.paasta_cli.utils import list_services
from paasta_tools.paasta_cli.utils import PaastaColors

DEFAULT_COMPONENTS = ['build', 'deploy', 'app_output', 'lb_errors', 'monitoring']
LOG_COMPONENTS = {
    'build': {
        'color': PaastaColors.blue,
        'help': 'Jenkins build jobs output, like the itest, promotion, security checks, etc.',
        'command': 'NA - TODO: tee jenkins build steps into scribe PAASTA-201',
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
    status_parser.add_argument(
        '-e', '--environment',
        help="The scribereader 'environment'. TODO: list envs from scribreader",
    )
    status_parser.add_argument(
        '-f', '-F', '--tail', dest='tail', action='store_true', default=True,
        help='Stream the logs and follow it for more data',
    )
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


def paasta_logs(args):
    """Print the logs for as Paasta service.
    :param args: argparse.Namespace obj created from sys.args by paasta_cli"""
    service_name = figure_out_service_name(args)
    if args.components is not None:
        components_list = args.clusters.split(",")
    else:
        components_list = DEFAULT_COMPONENTS

    print "Getting logs for %s" % service_name
    for component in components_list:
        command_string = "Command: %s" % LOG_COMPONENTS[component]['command']
        print prefix(command_string, component)
