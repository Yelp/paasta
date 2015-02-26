#!/usskr/bin/env python
"""PaaSTA log reader for humans"""
import argparse
import logging
import Queue
import sys

from argcomplete.completers import ChoicesCompleter
from scribereader import scribereader

from paasta_tools.marathon_tools import list_clusters
from paasta_tools.paasta_cli.utils import figure_out_service_name
from paasta_tools.paasta_cli.utils import figure_out_cluster
from paasta_tools.paasta_cli.utils import list_services
from paasta_tools.utils import LOG_COMPONENTS
from paasta_tools.utils import get_log_name_for_service


DEFAULT_COMPONENTS = ['build', 'deploy', 'app_output', 'lb_errors', 'monitoring']

log = logging.getLogger('__main__')


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


def line_passes_filter(line, levels, components, cluster):
    pass
    # return line.component in components and (line.cluster == cluster (alias like "prod"?) or line.cluster == 'N/A')


def scribe_tail(env, service, levels, components, cluster, queue):
    """Calls scribetailer for a particular environment.
    outputs lines that match for the requested cluster and components
    in a pretty way

    UPDATE ME!!!
    """
    # This is the code that runs in the thread spawned by
    # tail_paasta_logs.
    log.debug("Going to tail scribe in %s" % env)
    stream_name = get_log_name_for_service(service)
    host, port = scribereader.get_env_scribe_host(env, True)
    tailer = scribereader.get_stream_tailer(stream_name, host, port)
    for line in tailer:
        if line_passes_filter(line, levels, components, cluster):
            queue.put(line)


def tail_paasta_logs(service, levels, components, cluster):
    """Sergeant function for spawning off all the right scribe tailing functions"""
    envs = determine_scribereader_envs(components, cluster)
    log.info("Would connect to these envs to tail scribe logs: %s" % envs)
    queue = Queue.Queue()
    # establish ioloop Queue
    for env in envs:
        # start a thread that tails scribe for env, passing in reference to ioloop Queue
        scribe_tail(env, service, levels, components, cluster, queue)
        # kwargs = { env=env, service=service, levels=levels, components=components, cluster=cluster, queue=queue }
        # t = Thread(target=scribe_tail, kwargs=**kwargs)
        # t.start()
    # start pulling things off the queue and output them
    # while True:
    #     print Queue.get()
    #     Queue.task_done()


def paasta_logs(args):
    """Print the logs for as Paasta service.
    :param args: argparse.Namespace obj created from sys.args by paasta_cli"""
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
