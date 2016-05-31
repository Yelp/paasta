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
"""PaaSTA log reader for humans"""
import argparse
import datetime
import json
import logging
import re
import sys
from multiprocessing import Process
from multiprocessing import Queue
from Queue import Empty

import dateutil
import isodate

try:
    from scribereader import scribereader
    from scribereader.scribereader import StreamTailerSetupError
except ImportError:
    scribereader = None

from paasta_tools import chronos_tools
from paasta_tools.marathon_tools import format_job_id
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import guess_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_services
from paasta_tools.utils import ANY_CLUSTER
from paasta_tools.utils import datetime_convert_timezone
from paasta_tools.utils import datetime_from_utc_to_local
from paasta_tools.utils import DEFAULT_LOGLEVEL
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import format_log_line
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import list_clusters
from paasta_tools.utils import LOG_COMPONENTS
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import get_log_name_for_service


DEFAULT_COMPONENTS = ['build', 'deploy', 'monitoring']

log = logging.getLogger(__name__)


def add_subparser(subparsers):
    status_parser = subparsers.add_parser(
        'logs',
        help="Streams logs relevant to a service across the PaaSTA components",
        description=(
            "'paasta logs' works by streaming PaaSTA-related event messages "
            "in a human-readable way."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    status_parser.add_argument(
        '-s', '--service',
        help='The name of the service you wish to inspect. Defaults to autodetect.'
    ).completer = lazy_choices_completer(list_services)
    components_help = 'A comma separated list of the components you want logs for.'
    status_parser.add_argument(
        '-C', '--components',
        help=components_help,
    ).completer = lazy_choices_completer(LOG_COMPONENTS.keys)
    cluster_help = 'The clusters to see relevant logs for. Defaults to all clusters to which this service is deployed.'
    status_parser.add_argument(
        '-c', '--clusters',
        help=cluster_help,
    ).completer = completer_clusters
    status_parser.add_argument(
        '-f', '-F', '--tail', dest='tail', action='store_true', default=True,
        help='Stream the logs and follow it for more data',
    )
    status_parser.add_argument(
        '-v', '--verbose', action='store_true', dest='verbose', default=False,
        help='Enable verbose logging',
    )
    status_parser.add_argument(
        '-r', '--raw-mode', action='store_true',
        dest='raw_mode', default=False,
        help="Don't pretty-print logs; emit them exactly as they are in scribe."
    )
    status_parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
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


def completer_clusters(prefix, parsed_args, **kwargs):
    service = parsed_args.service or guess_service_name()
    if service in list_services():
        return list_clusters(service)
    else:
        return list_clusters()


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


def paasta_log_line_passes_filter(line, levels, service, components, clusters):
    """Given a (JSON-formatted) log line, return True if the line should be
    displayed given the provided levels, components, and clusters; return False
    otherwise.
    """
    try:
        parsed_line = json.loads(line)
    except ValueError:
        log.debug('Trouble parsing line as json. Skipping. Line: %r' % line)
        return False
    return (
        parsed_line.get('level') in levels and
        parsed_line.get('component') in components and (
            parsed_line.get('cluster') in clusters or
            parsed_line.get('cluster') == ANY_CLUSTER
        )
    )


def extract_utc_timestamp_from_log_line(line):
    """
    Extracts the timestamp from a log line of the format "<timestamp> <other data>" and returns a UTC datetime object
    or None if it could not parse the line
    """
    # Extract ISO 8601 date per http://www.pelagodesign.com/blog/2009/05/20/iso-8601-date-validation-that-doesnt-suck/
    iso_re = r'^([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?|' \
        r'(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)([\.,]\d+' \
        r'(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)? '

    tokens = re.match(iso_re, line)

    if not tokens:
        # Could not parse line
        return None
    timestamp = tokens.group(0).strip()
    dt = isodate.parse_datetime(timestamp)
    utc_timestamp = datetime_convert_timezone(dt, dt.tzinfo, dateutil.tz.tzutc())
    return utc_timestamp


def parse_marathon_log_line(line, clusters, service):
    utc_timestamp = extract_utc_timestamp_from_log_line(line)
    if not utc_timestamp:
        return ''
    else:
        return format_log_line(
            level='event',
            cluster=clusters[0],
            service=service,
            instance='ALL',
            component='marathon',
            line=line.strip(),
            timestamp=utc_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        )


def parse_chronos_log_line(line, clusters, service):
    utc_timestamp = extract_utc_timestamp_from_log_line(line)
    if not utc_timestamp:
        return ''
    else:
        return format_log_line(
            level='event',
            cluster=clusters[0],
            service=service,
            instance='ALL',
            component='chronos',
            line=line.strip(),
            timestamp=utc_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        )


def marathon_log_line_passes_filter(line, levels, service, components, clusters):
    """Given a (JSON-formatted) log line where the message is a Marathon log line,
    return True if the line should be displayed given the provided service; return False
    otherwise."""
    try:
        parsed_line = json.loads(line)
    except ValueError:
        log.debug('Trouble parsing line as json. Skipping. Line: %r' % line)
        return False
    return format_job_id(service, '') in parsed_line.get('message', '')


def chronos_log_line_passes_filter(line, levels, service, components, clusters):
    """Given a (JSON-formatted) log line where the message is a Marathon log line,
    return True if the line should be displayed given the provided service; return False
    otherwise."""
    try:
        parsed_line = json.loads(line)
    except ValueError:
        log.debug('Trouble parsing line as json. Skipping. Line: %r' % line)
        return False
    return chronos_tools.compose_job_id(service, '') in parsed_line.get('message', '')


def print_log(line, requested_levels, raw_mode=False):
    """Mostly a stub to ease testing. Eventually this may do some formatting or
    something.
    """
    if raw_mode:
        print line,  # suppress trailing newline since scribereader already attached one
    else:
        print prettify_log_line(line, requested_levels)


def prettify_timestamp(timestamp):
    """Returns more human-friendly form of 'timestamp' without microseconds and
    in local time.
    """
    dt = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
    pretty_timestamp = datetime_from_utc_to_local(dt)
    return pretty_timestamp.strftime("%Y-%m-%d %H:%M:%S")


def prettify_component(component):
    try:
        return LOG_COMPONENTS[component]['color']('[%s]' % component)
    except KeyError:
        return "UNPRETTIFIABLE COMPONENT %s" % component


def prettify_level(level, requested_levels):
    """Colorize level. 'event' is special and gets bolded; everything else gets
    lightened.

    requested_levels is an iterable of levels that will be displayed. If only
    one level will be displayed, don't bother to print it (return empty string).
    If multiple levels will be displayed, emit the (prettified) level so the
    resulting log output is not ambiguous.
    """
    pretty_level = ''
    if len(requested_levels) > 1:
        if level == 'event':
            pretty_level = PaastaColors.bold('[%s]' % level)
        else:
            pretty_level = PaastaColors.grey('[%s]' % level)
    return pretty_level


def prettify_log_line(line, requested_levels):
    """Given a line from the log, which is expected to be JSON and have all the
    things we expect, return a pretty formatted string containing relevant values.
    """
    pretty_line = ''
    try:
        parsed_line = json.loads(line)
        pretty_level = prettify_level(parsed_line['level'], requested_levels)
        pretty_line = "%(timestamp)s %(component)s %(cluster)s %(instance)s - %(level)s%(message)s" % ({
            'timestamp': prettify_timestamp(parsed_line['timestamp']),
            'component': prettify_component(parsed_line['component']),
            'cluster': '[%s]' % parsed_line['cluster'],
            'instance': '[%s]' % parsed_line['instance'],
            'level': '%s ' % pretty_level,
            'message': parsed_line['message'],
        })
    except ValueError:
        log.debug('Trouble parsing line as json. Skipping. Line: %r' % line)
        pretty_line = "Invalid JSON: %s" % line
    except KeyError:
        log.debug('JSON parsed correctly but was missing a key. Skipping. Line: %r' % line)
        pretty_line = "JSON missing keys: %s" % line
    return pretty_line


# The map of name -> LogReader subclasses, used by configure_log.
_log_reader_classes = {}


def register_log_reader(name):
    """Returns a decorator that registers a log reader class at a given name
    so get_log_reader_classes can find it."""
    def outer(log_reader_class):
        _log_reader_classes[name] = log_reader_class
        return log_reader_class
    return outer


def get_log_reader_class(name):
    return _log_reader_classes[name]


def list_log_readers():
    return _log_reader_classes.keys()


def get_log_reader():
    log_reader_config = load_system_paasta_config().get_log_reader()
    log_reader_class = get_log_reader_class(log_reader_config['driver'])
    return log_reader_class(**log_reader_config.get('options', {}))


class LogReader(object):
    def __init__(self, **kwargs):
        pass

    def tail_logs(service, levels, components, clusters, raw_mode=False):
        raise NotImplementedError("tail_logs is not implemented")


@register_log_reader('scribereader')
class ScribeLogReader(LogReader):
    def __init__(self, cluster_map, **kwargs):
        if scribereader is None:
            raise Exception("scribereader package must be available to use scribereader log reading backend")
        self.cluster_map = cluster_map

    def tail_logs(self, service, levels, components, clusters, raw_mode=False):
        """Sergeant function for spawning off all the right log tailing functions.

        NOTE: This function spawns concurrent processes and doesn't necessarily
        worry about cleaning them up! That's because we expect to just exit the
        main process when this function returns (as main() does). Someone calling
        this function directly with something like "while True: tail_paasta_logs()"
        may be very sad.

        NOTE: We try pretty hard to supress KeyboardInterrupts to prevent big
        useless stack traces, but it turns out to be non-trivial and we fail ~10%
        of the time. We decided we could live with it and we're shipping this to
        see how it fares in real world testing.

        Here are some things we read about this problem:
        * http://stackoverflow.com/questions/1408356/keyboard-interrupts-with-pythons-multiprocessing-pool
        * http://jtushman.github.io/blog/2014/01/14/python-%7C-multiprocessing-and-interrupts/
        * http://bryceboe.com/2010/08/26/python-multiprocessing-and-keyboardinterrupt/

        We could also try harder to terminate processes from more places. We could
        use process.join() to ensure things have a chance to die. We punted these
        things.

        It's possible this whole multiprocessing strategy is wrong-headed. If you
        are reading this code to curse whoever wrote it, see discussion in
        PAASTA-214 and https://reviewboard.yelpcorp.com/r/87320/ and feel free to
        implement one of the other options.
        """
        scribe_envs = set([])
        for cluster in clusters:
            scribe_envs.update(self.determine_scribereader_envs(components, cluster))
        log.info("Would connect to these envs to tail scribe logs: %s" % scribe_envs)
        queue = Queue()
        spawned_processes = []
        for scribe_env in scribe_envs:
            # Tail stream_paasta_<service> for build or deploy components
            if any([component in components for component in DEFAULT_COMPONENTS]):
                # Start a thread that tails scribe in this env
                kw = {
                    'scribe_env': scribe_env,
                    'stream_name': get_log_name_for_service(service),
                    'service': service,
                    'levels': levels,
                    'components': components,
                    'clusters': clusters,
                    'queue': queue,
                    'filter_fn': paasta_log_line_passes_filter,
                }
                process = Process(target=self.scribe_tail, kwargs=kw)
                spawned_processes.append(process)
                process.start()

            # Tail Marathon logs for the relevant clusters for this service
            if 'marathon' in components:
                for cluster in clusters:
                    kw = {
                        'scribe_env': scribe_env,
                        'stream_name': 'stream_marathon_%s' % cluster,
                        'service': service,
                        'levels': levels,
                        'components': components,
                        'clusters': [cluster],
                        'queue': queue,
                        'parse_fn': parse_marathon_log_line,
                        'filter_fn': marathon_log_line_passes_filter,
                    }
                    process = Process(target=self.scribe_tail, kwargs=kw)
                    spawned_processes.append(process)
                    process.start()

            # Tail Chronos logs for the relevant clusters for this service
            if 'chronos' in components:
                for cluster in clusters:
                    kw = {
                        'scribe_env': scribe_env,
                        'stream_name': 'stream_chronos_%s' % cluster,
                        'service': service,
                        'levels': levels,
                        'components': components,
                        'clusters': [cluster],
                        'queue': queue,
                        'parse_fn': parse_chronos_log_line,
                        'filter_fn': chronos_log_line_passes_filter,
                    }
                    process = Process(target=self.scribe_tail, kwargs=kw)
                    spawned_processes.append(process)
                    process.start()
        # Pull things off the queue and output them. If any thread dies we are no
        # longer presenting the user with the full picture so we quit.
        #
        # This is convenient for testing, where a fake scribe_tail() can emit a
        # fake log and exit. Without the thread aliveness check, we would just sit
        # here forever even though the threads doing the tailing are all gone.
        #
        # NOTE: A noisy tailer in one scribe_env (such that the queue never gets
        # empty) will prevent us from ever noticing that another tailer has died.
        while True:
            try:
                # This is a blocking call with a timeout for a couple reasons:
                #
                # * If the queue is empty and we get_nowait(), we loop very tightly
                # and accomplish nothing.
                #
                # * Testing revealed a race condition where print_log() is called
                # and even prints its message, but this action isn't recorded on
                # the patched-in print_log(). This resulted in test flakes. A short
                # timeout seems to soothe this behavior: running this test 10 times
                # with a timeout of 0.0 resulted in 2 failures; running it with a
                # timeout of 0.1 resulted in 0 failures.
                #
                # * There's a race where thread1 emits its log line and exits
                # before thread2 has a chance to do anything, causing us to bail
                # out via the Queue Empty and thread aliveness check.
                #
                # We've decided to live with this for now and see if it's really a
                # problem. The threads in test code exit pretty much immediately
                # and a short timeout has been enough to ensure correct behavior
                # there, so IRL with longer start-up times for each thread this
                # will surely be fine.
                #
                # UPDATE: Actually this is leading to a test failure rate of about
                # 1/10 even with timeout of 1s. I'm adding a sleep to the threads
                # in test code to smooth this out, then pulling the trigger on
                # moving that test to integration land where it belongs.
                line = queue.get(True, 0.1)
                print_log(line, levels, raw_mode)
            except Empty:
                try:
                    # If there's nothing in the queue, take this opportunity to make
                    # sure all the tailers are still running.
                    running_processes = [tt.is_alive() for tt in spawned_processes]
                    if not running_processes or not all(running_processes):
                        log.warn('Quitting because I expected %d log tailers to be alive but only %d are alive.' % (
                            len(spawned_processes),
                            running_processes.count(True),
                        ))
                        for process in spawned_processes:
                            if process.is_alive():
                                process.terminate()
                        break
                except KeyboardInterrupt:
                    # Die peacefully rather than printing N threads worth of stack
                    # traces.
                    #
                    # This extra nested catch is because it's pretty easy to be in
                    # the above try block when the user hits Ctrl-C which otherwise
                    # dumps a stack trace.
                    log.warn('Terminating.')
                    break
            except KeyboardInterrupt:
                # Die peacefully rather than printing N threads worth of stack
                # traces.
                log.warn('Terminating.')
                break

    def scribe_tail(self, scribe_env, stream_name, service, levels, components, clusters, queue, filter_fn,
                    parse_fn=None):
        """Creates a scribetailer for a particular environment.

        When it encounters a line that it should report, it sticks it into the
        provided queue.

        This code is designed to run in a thread as spawned by tail_paasta_logs().
        """
        try:
            log.debug("Going to tail %s scribe stream in %s" % (stream_name, scribe_env))
            host_and_port = scribereader.get_env_scribe_host(scribe_env, True)
            host = host_and_port['host']
            port = host_and_port['port']
            tailer = scribereader.get_stream_tailer(stream_name, host, port)
            for line in tailer:
                if parse_fn:
                    line = parse_fn(line, clusters, service)
                if filter_fn(line, levels, service, components, clusters):
                    queue.put(line)
        except KeyboardInterrupt:
            # Die peacefully rather than printing N threads worth of stack
            # traces.
            pass
        except StreamTailerSetupError:
            log.error("Failed to setup stream tailing for %s in %s" % (stream_name, scribe_env))
            log.error("Don't Panic! This can happen the first time a service is deployed because the log")
            log.error("doesn't exist yet. Please wait for the service to be deployed in %s and try again." % scribe_env)
            raise

    def determine_scribereader_envs(self, components, cluster):
        """Returns a list of environments that scribereader needs to connect
        to based on a given list of components and the cluster involved.

        Some components are in certain environments, regardless of the cluster.
        Some clusters do not match up with the scribe environment names, so
        we figure that out here"""
        envs = []
        for component in components:
            # If a component has a 'source_env', we use that
            # otherwise we lookup what scribe env is associated with a given cluster
            env = LOG_COMPONENTS[component].get('source_env', self.cluster_to_scribe_env(cluster))
            envs.append(env)
        return set(envs)

    def cluster_to_scribe_env(self, cluster):
        """Looks up the particular scribe env associated with a given paasta cluster.

        Scribe has its own "environment" key, which doesn't always map 1:1 with our
        cluster names, so we have to maintain a manual mapping.

        This mapping is deployed as a config file via puppet as part of the public
        config deployed to every server.
        """
        env = self.cluster_map.get(cluster, None)
        if env is None:
            print "I don't know where scribe logs for %s live?" % cluster
            sys.exit(1)
        else:
            return env


def paasta_logs(args):
    """Print the logs for as Paasta service.
    :param args: argparse.Namespace obj created from sys.args by cli"""
    soa_dir = args.soa_dir
    service = figure_out_service_name(args, soa_dir)

    if args.clusters is None:
        clusters = list_clusters(service, soa_dir=soa_dir)
    else:
        clusters = args.clusters.split(",")

    if args.components is not None:
        components = args.components.split(",")
    else:
        components = DEFAULT_COMPONENTS

    if args.verbose:
        log.setLevel(logging.DEBUG)
        levels = [DEFAULT_LOGLEVEL, 'debug']
    else:
        log.setLevel(logging.WARNING)
        levels = [DEFAULT_LOGLEVEL]

    log.info("Going to get logs for %s on clusters %s" % (service, clusters))

    log_reader = get_log_reader()

    if args.tail:
        log_reader.tail_logs(service, levels, components, clusters, raw_mode=args.raw_mode)
    else:
        print "Non-tailing actions are not yet supported"
