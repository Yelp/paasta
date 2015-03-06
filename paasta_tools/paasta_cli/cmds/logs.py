#!/usskr/bin/env python
"""PaaSTA log reader for humans"""
import argparse
import json
import logging
import Queue
import sys
import threading

from argcomplete.completers import ChoicesCompleter
from scribereader import scribereader

from paasta_tools.marathon_tools import list_clusters
from paasta_tools.paasta_cli.utils import figure_out_service_name
from paasta_tools.paasta_cli.utils import figure_out_cluster
from paasta_tools.paasta_cli.utils import list_services
from paasta_tools.utils import DEFAULT_LOGLEVEL
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
    """UPDATE ME!!!"""
    parsed_line = json.loads(line)
    return (
        parsed_line.get('level') in levels
        and parsed_line.get('component') in components
        and (
            parsed_line.get('cluster') == cluster
            # TODO: Use ANY_CLUSTER when tripy ships the branch that
            # introduces that constant instead of hardcoded 'N/A'
            or parsed_line.get('cluster') == 'N/A'
        )
    )


def scribe_tail(scribe_env, service, levels, components, cluster, queue):
    """Calls scribetailer for a particular environment.
    outputs lines that match for the requested cluster and components
    in a pretty way

    UPDATE ME!!!
    """
    # This is the code that runs in the thread spawned by
    # tail_paasta_logs().
    log.debug("Going to tail scribe in %s" % scribe_env)
    stream_name = get_log_name_for_service(service)
    host, port = scribereader.get_env_scribe_host(scribe_env, True)
    tailer = scribereader.get_stream_tailer(stream_name, host, port)
    for line in tailer:
        if line_passes_filter(line, levels, components, cluster):
            queue.put(line)


def print_log(line):
    """Mostly a stub to ease testing. Eventually this may do some formatting or
    something.
    """
    print line


def tail_paasta_logs(service, levels, components, cluster):
    """Sergeant function for spawning off all the right scribe tailing functions.

    NOTE: This function creates threads and doesn't really clean them up!
    That's because we expect to just exit the process when this function
    returns (as main() does). Someone calling this function directly with
    something like "while True: tail_paasta_logs()" will be very sad.
    """
    scribe_envs = determine_scribereader_envs(components, cluster)
    log.info("Would connect to these envs to tail scribe logs: %s" % scribe_envs)
    queue = Queue.Queue()
    for scribe_env in scribe_envs:
        # Start a thread that tails scribe in this env
        kw = {
            'scribe_env': scribe_env,
            'service': service,
            'levels': levels,
            'components': components,
            'cluster': cluster,
            'queue': queue,
        }
        t = threading.Thread(target=scribe_tail, kwargs=kw)
        t.start()
    # Pull things off the queue and output them. If any thread dies we are no longer
    # presenting the user with the full picture so we quit.
    #
    # This is convenient for testing, where a fake scribe_tail() can emit a
    # fake log and exit. Without the thread count check, we would just sit here
    # forever even though the threads doing the tailing are all gone.
    #
    # TODO???: A noisy tailer in one scribe_env (such that the queue never gets
    # empty) will prevent us from ever noticing that another tailer has died.
    expected_thread_count = len(scribe_envs) + 1  # main thread is included
    while True:
        try:
            # This is a blocking call with a timeout for a couple reasons:
            #
            # * If the queue is empty and we get_nowait(), we loop very tightly
            # and accomplish nothing.
            #
            # * Testing revealed a race condition where print_log() is called
            # and even prints its message, but this action isn't recorded on
            # the patched-in print_log(). This resulted in test flakes. The
            # short timeout seems to soothe this behavior: running this test 10
            # times with a timeout of 0.0 resulted in 5 failures; running it
            # with a timeout of 0.1 resulted in 0 failures.
            #
            # * There's still a race because if thread1 emits its log line and
            # exits before thread2 has a chance to do anything, we bail out
            # (via Queue.Empty and one of our expected threads having quit) and
            # thread2 never does its work.
            #
            # It's possible this approach is fundamentally broken and will
            # never work, and we'll need to e.g. put more responsibility on the
            # individual threads and have them raise an exception or use
            # notify() when they die and let the main thread take it from
            # there.
            #
            # OR, don't use threads at all. Fork processes, monitor them, kill
            # them when necessary, etc.
            #
            # (notes from end of #levchins discussion with wtimoney)
            # [05.17:48:42] <        troscoe> | anyway, | your proposed
            # strategy would be: individual tailer threads monitor some
            # sentinel or global flag or get notified when they're supposed to
            # shut down. then instead of just aborting, the sargeant would set
            # the sentinel or flag or do the notify() of all the running
            # threads, let them shut down, and clear out the queue?
            # [05.17:49:34] <              |> | first half yes but clog needs
            # to play nice with that strat
            # [05.17:49:40] <              |> | which it doesnt so itd be a clog change
            # [05.17:49:45] <        troscoe> | :\
            # [05.17:50:03] <        troscoe> | how about the death process is
            # sleep for a while and then kill?
            # [05.17:50:21] <        troscoe> | or uh, something with a timer
            # thread that just murders everyone who hasn't come back yet?
            # [05.17:50:35] <              |> | you cant murder cpy threads
            # [05.17:50:43] <        troscoe> | really?
            # [05.17:50:47] <              |> | they can be in gil-yielded native code
            # [05.17:50:54] <              |> | at which point their held
            # refcounts are fucking hosed
            # [05.17:51:00] <              |> | similarly you cant kill java threads
            # [05.17:51:04] <        troscoe> | currently, there are no
            # priorities, no thread groups, and threads cannot be destroyed,
            # stopped, suspended, resumed, or interrupted
            # [05.17:51:06] <              |> | because vm guarantees
            # [05.17:51:22] <              |> | you can nuke child processes
            # [05.17:51:50] <        PBERENS> | FORK-it
            # [05.17:51:51] <        troscoe> | hence your suggestion seven
            # hours ago to just use fork()? :)
            # [05.17:52:12] <              |> | troof
            # [05.17:52:16] <              |> | cpy loves processes
            print_log(queue.get(True, 0.1))
            queue.task_done()
        except Queue.Empty:
            # If there's nothing in the queue, take this opportunity to make
            # sure all the tailers are still running.
            running_threads = threading.enumerate()
            if len(running_threads) != expected_thread_count:
                print "@@@@@@@@@@@@@@@@@@@@@@"
                print "Quitting because I expected %s threads but there are %s" % (
                    expected_thread_count, running_threads)
                break


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
        levels = [DEFAULT_LOGLEVEL, 'debug']
    else:
        log.setLevel(logging.WARN)
        levels = [DEFAULT_LOGLEVEL]

    log.info("Going to get logs for %s on cluster %s" % (service_name, cluster))
    if args.tail:
        tail_paasta_logs(service_name, levels, components, cluster)
    else:
        print "Non-tailing actions are not yet supported"
