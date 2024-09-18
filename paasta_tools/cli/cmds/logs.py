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
from collections import namedtuple
from contextlib import contextmanager
from multiprocessing import Process
from multiprocessing import Queue
from queue import Empty
from time import sleep
from typing import Any
from typing import Callable
from typing import ContextManager
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import MutableSequence
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple
from typing import Type
from typing import Union

import a_sync
import isodate
import nats
import pytz
from dateutil import tz

try:
    from scribereader import scribereader
    from scribereader.scribereader import StreamTailerSetupError
except ImportError:
    scribereader = None

try:
    from clog.readers import S3LogsReader
except ImportError:
    S3LogsReader = None

from pytimeparse.timeparse import timeparse

from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import guess_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import verify_instances
from paasta_tools.utils import list_services
from paasta_tools.utils import ANY_CLUSTER
from paasta_tools.utils import datetime_convert_timezone
from paasta_tools.utils import datetime_from_utc_to_local
from paasta_tools.utils import DEFAULT_LOGLEVEL
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import list_clusters
from paasta_tools.utils import LOG_COMPONENTS
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import get_log_name_for_service


DEFAULT_COMPONENTS = ["stdout", "stderr"]

log = logging.getLogger(__name__)


def add_subparser(subparsers) -> None:
    status_parser = subparsers.add_parser(
        "logs",
        help="Streams logs relevant to a service across the PaaSTA components",
        description=(
            "'paasta logs' works by streaming PaaSTA-related event messages "
            "in a human-readable way."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    status_parser.add_argument(
        "-s",
        "--service",
        help="The name of the service you wish to inspect. Defaults to autodetect.",
    ).completer = lazy_choices_completer(list_services)
    status_parser.add_argument(
        "-c",
        "--cluster",
        help="The cluster to see relevant logs for.",
        nargs=1,
    ).completer = completer_clusters
    status_parser.add_argument(
        "-i",
        "--instance",
        help="The instance to see relevant logs for. Defaults to all instances for this service.",
        type=str,
    ).completer = completer_clusters
    pod_help = (
        "The pods to see relevant logs for. Defaults to all pods for this service."
    )
    status_parser.add_argument("-p", "--pods", help=pod_help)
    status_parser.add_argument(
        "-C",
        "--components",
        type=lambda s: set(s.split(",")),
        default=set(DEFAULT_COMPONENTS),
        help=(
            "A comma-separated list of the components you want logs for. "
            "PaaSTA consists of 'components' such as builds and deployments, "
            "for each of which we collect logs for per service. "
            "See below for a list of components. "
            "Defaults to %(default)s."
        ),
    ).completer = lazy_choices_completer(LOG_COMPONENTS.keys)
    status_parser.add_argument(
        "-f",
        "-F",
        "--tail",
        dest="tail",
        action="store_true",
        default=False,
        help="Stream the logs and follow it for more data",
    )
    status_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="Enable verbose logging",
    )
    status_parser.add_argument(
        "-r",
        "--raw-mode",
        action="store_true",
        dest="raw_mode",
        default=False,
        help="Don't pretty-print logs; emit them exactly as they are in scribe.",
    )
    status_parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help=f"Define a different soa config directory. Defaults to %(default)s.",
    )

    status_parser.add_argument(
        "-a",
        "--from",
        "--after",
        dest="time_from",
        help=(
            "The time to start getting logs from. "
            'This can be an ISO-8601 timestamp or a human readable duration parsable by pytimeparse such as "5m", "1d3h" etc. '
            'For example: --from "3m" would start retrieving logs from 3 minutes ago. '
            "Incompatible with --line-offset and --lines."
        ),
    )
    status_parser.add_argument(
        "-t",
        "--to",
        dest="time_to",
        help=(
            "The time to get logs up to. "
            'This can be an ISO-8601 timestamp or a human readable duration parsable by pytimeparse such as "5m", "1d3h" etc. '
            "Incompatiable with --line-offset and --lines. "
            "Defaults to now."
        ),
    )
    status_parser.add_argument(
        "-l",
        "-n",
        "--lines",
        dest="line_count",
        help=(
            "The number of lines to retrieve from the specified offset. "
            'May optionally be prefixed with a "+" or "-" to specify which direction from the offset. '
            "Incompatiable with --from and --to. "
            'Defaults to "-100".'
        ),
        type=int,
    )
    status_parser.add_argument(
        "-o",
        "--line-offset",
        dest="line_offset",
        help=(
            "The offset at which line to start grabbing logs from. "
            "For example, --line-offset 1 would be the first line. "
            "Paired with --lines, --line-offset +100 would give you the first 100 lines of logs. "
            "Some logging backends may not support line offsetting by time or lines. "
            "Incompatiable with --from and --to. "
            "Defaults to the latest line's offset."
        ),
        type=int,
    )
    status_parser.add_argument(
        "-S",
        "--strip-headers",
        dest="strip_headers",
        help="Print log lines without header information.",
        action="store_true",
    )
    status_parser.epilog = (
        "COMPONENTS\n"
        "Here is a list of all components and what they are:\n"
        f"{build_component_descriptions(LOG_COMPONENTS)}"
    )
    status_parser.set_defaults(command=paasta_logs)


def completer_clusters(prefix, parsed_args, **kwargs):
    service = parsed_args.service or guess_service_name()
    if service in list_services():
        return list_clusters(service)
    else:
        return list_clusters()


def build_component_descriptions(components: Mapping[str, Mapping[str, Any]]) -> str:
    """Returns a colored description string for every log component
    based on its help attribute"""
    output = []
    for k, v in components.items():
        output.append("    {}: {}".format(v["color"](k), v["help"]))
    return "\n".join(output)


def prefix(input_string: str, component: str) -> str:
    """Returns a colored string with the right colored prefix with a given component"""
    return "{}: {}".format(LOG_COMPONENTS[component]["color"](component), input_string)


# The reason this returns true if start_time or end_time are None is because
# filtering by time is optional, and it allows us to simply do
# if not check_timestamp_in_range(...): return False
# The default arguments for start_time and end_time are None when we aren't
# filtering by time
def check_timestamp_in_range(
    timestamp: datetime.datetime,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
) -> bool:
    """A convenience function to check if a datetime.datetime timestamp is within the given start and end times,
    returns true if start_time or end_time is None

    :param timestamp: The timestamp to check
    :param start_time: The start of the interval
    :param end_time: The end of the interval
    :return: True if timestamp is within start_time and end_time range, False otherwise
    """
    if timestamp is not None and start_time is not None and end_time is not None:
        if timestamp.tzinfo is None:
            timestamp = pytz.utc.localize(timestamp)
        return start_time < timestamp < end_time
    else:
        return True


def paasta_log_line_passes_filter(
    line: str,
    levels: Sequence[str],
    service: str,
    components: Iterable[str],
    clusters: Sequence[str],
    instances: List[str],
    pods: Iterable[str] = None,
    start_time: datetime.datetime = None,
    end_time: datetime.datetime = None,
) -> bool:
    """Given a (JSON-formatted) log line, return True if the line should be
    displayed given the provided levels, components, and clusters; return False
    otherwise.

    NOTE: Pods are optional as services that use Mesos do not operate with pods.
    """
    try:
        parsed_line = json.loads(line)
    except ValueError:
        log.debug("Trouble parsing line as json. Skipping. Line: %r" % line)
        return False
    timestamp = isodate.parse_datetime(parsed_line.get("timestamp"))
    if not check_timestamp_in_range(timestamp, start_time, end_time):
        return False
    return (
        (parsed_line.get("level") is None or parsed_line.get("level") in levels)
        and parsed_line.get("component") in components
        and (
            parsed_line.get("cluster") in clusters
            or parsed_line.get("cluster") == ANY_CLUSTER
        )
        and (instances is None or parsed_line.get("instance") in instances)
    )


def paasta_app_output_passes_filter(
    line: str,
    levels: Sequence[str],
    service: str,
    components: Iterable[str],
    clusters: Sequence[str],
    instances: List[str],
    pods: Iterable[str] = None,
    start_time: datetime.datetime = None,
    end_time: datetime.datetime = None,
) -> bool:
    try:
        parsed_line = json.loads(line)
    except ValueError:
        log.debug("Trouble parsing line as json. Skipping. Line: %r" % line)
        return False
    try:
        timestamp = isodate.parse_datetime(parsed_line.get("timestamp"))
    # https://github.com/gweis/isodate/issues/53
    except ValueError:
        return True
    if not check_timestamp_in_range(timestamp, start_time, end_time):
        return False
    return (
        parsed_line.get("component") in components
        and (
            parsed_line.get("cluster") in clusters
            or parsed_line.get("cluster") == ANY_CLUSTER
        )
        and (instances is None or parsed_line.get("instance") in instances)
        and (pods is None or parsed_line.get("pod_name") in pods)
    )


def extract_utc_timestamp_from_log_line(line: str) -> datetime.datetime:
    """
    Extracts the timestamp from a log line of the format "<timestamp> <other data>" and returns a UTC datetime object
    or None if it could not parse the line
    """
    # Extract ISO 8601 date per http://www.pelagodesign.com/blog/2009/05/20/iso-8601-date-validation-that-doesnt-suck/
    iso_re = (
        r"^([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?|"
        r"(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)([\.,]\d+"
        r"(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)? "
    )

    tokens = re.match(iso_re, line)

    if not tokens:
        # Could not parse line
        return None
    timestamp = tokens.group(0).strip()
    dt = isodate.parse_datetime(timestamp)
    utc_timestamp = datetime_convert_timezone(dt, dt.tzinfo, tz.tzutc())
    return utc_timestamp


def print_log(
    line: str,
    requested_levels: Sequence[str],
    raw_mode: bool = False,
    strip_headers: bool = False,
) -> None:
    """Mostly a stub to ease testing. Eventually this may do some formatting or
    something.
    """
    if raw_mode:
        # suppress trailing newline since scribereader already attached one
        print(line, end=" ", flush=True)
    else:
        print(
            prettify_log_line(line, requested_levels, strip_headers),
            flush=True,
        )


def prettify_timestamp(timestamp: datetime.datetime) -> str:
    """Returns more human-friendly form of 'timestamp' without microseconds and
    in local time.
    """
    dt = isodate.parse_datetime(timestamp)
    pretty_timestamp = datetime_from_utc_to_local(dt)
    return pretty_timestamp.strftime("%Y-%m-%d %H:%M:%S")


def prettify_component(component: str) -> str:
    try:
        return LOG_COMPONENTS[component]["color"]("[%s]" % component)
    except KeyError:
        return "UNPRETTIFIABLE COMPONENT %s" % component


def prettify_level(level: str, requested_levels: Sequence[str]) -> str:
    """Colorize level. 'event' is special and gets bolded; everything else gets
    lightened.

    requested_levels is an iterable of levels that will be displayed. If only
    one level will be displayed, don't bother to print it (return empty string).
    If multiple levels will be displayed, emit the (prettified) level so the
    resulting log output is not ambiguous.
    """
    pretty_level = ""
    if len(requested_levels) > 1:
        if level == "event":
            pretty_level = PaastaColors.bold("[%s] " % level)
        else:
            pretty_level = PaastaColors.grey("[%s] " % level)
    return pretty_level


def prettify_log_line(
    line: str, requested_levels: Sequence[str], strip_headers: bool
) -> str:
    """Given a line from the log, which is expected to be JSON and have all the
    things we expect, return a pretty formatted string containing relevant values.
    """
    try:
        parsed_line = json.loads(line)
    except ValueError:
        log.debug("Trouble parsing line as json. Skipping. Line: %r" % line)
        return "Invalid JSON: %s" % line

    try:
        if strip_headers:
            return "%(timestamp)s %(message)s" % (
                {
                    "timestamp": prettify_timestamp(parsed_line["timestamp"]),
                    "message": parsed_line["message"],
                }
            )
        else:
            return "%(timestamp)s %(component)s - %(message)s" % (
                {
                    "timestamp": prettify_timestamp(parsed_line["timestamp"]),
                    "component": prettify_component(parsed_line["component"]),
                    "message": parsed_line["message"],
                }
            )
    except KeyError:
        log.debug(
            "JSON parsed correctly but was missing a key. Skipping. Line: %r" % line
        )
        return "JSON missing keys: %s" % line


# The map of name -> LogReader subclasses, used by configure_log.
_log_reader_classes = {}


def register_log_reader(name):
    """Returns a decorator that registers a log reader class at a given name
    so get_log_reader_classes can find it."""

    def outer(log_reader_class):
        _log_reader_classes[name] = log_reader_class
        return log_reader_class

    return outer


def get_log_reader_class(
    name: str,
) -> Union[Type["ScribeLogReader"], Type["VectorLogsReader"]]:
    return _log_reader_classes[name]


def list_log_readers() -> Iterable[str]:
    return _log_reader_classes.keys()


def get_default_log_reader() -> "LogReader":
    log_reader_config = load_system_paasta_config().get_log_reader()
    log_reader_class = get_log_reader_class(log_reader_config["driver"])
    return log_reader_class(**log_reader_config.get("options", {}))


def get_log_reader(components: Set[str]) -> "LogReader":
    log_readers_config = load_system_paasta_config().get_log_readers()
    # ideally we should use a single "driver" for all components, but in cases where more than one is used for different components,
    # we should use the first one that supports all requested components
    # otherwise signal an error and suggest to provide a different list of components
    components_lists = []
    for log_reader_config in log_readers_config:
        supported_components = log_reader_config.get("components", [])
        components_lists.append(supported_components)
        if components.issubset(supported_components):
            log_reader_class = get_log_reader_class(log_reader_config["driver"])
            print(
                PaastaColors.cyan(
                    "Using '{}' driver to fetch logs...".format(
                        log_reader_config["driver"]
                    )
                ),
                file=sys.stderr,
            )
            return log_reader_class(**log_reader_config.get("options", {}))
    print(
        PaastaColors.cyan(
            "Supplied list of components '{}' is not supported by any log reader. Supported components are:\n\t{}".format(
                ", ".join(components),
                "\n\tor ".join([",".join(comp_list) for comp_list in components_lists]),
            )
        ),
        file=sys.stderr,
    )
    sys.exit(1)


class LogReader:
    # Tailing, i.e actively viewing logs as they come in
    SUPPORTS_TAILING = False
    # Getting the last n lines of logs
    SUPPORTS_LINE_COUNT = False
    # Getting the last/prev n lines of logs from line #34013 for example
    SUPPORTS_LINE_OFFSET = False
    # Getting the logs between two given times
    SUPPORTS_TIME = False
    # Supporting at least one of these log retrieval modes is required

    def tail_logs(
        self,
        service,
        levels,
        components,
        clusters,
        instances,
        pods,
        raw_mode=False,
        strip_headers=False,
    ):
        raise NotImplementedError("tail_logs is not implemented")

    def print_logs_by_time(
        self,
        service,
        start_time,
        end_time,
        levels,
        components,
        clusters,
        instances,
        pods,
        raw_mode,
        strip_headers,
    ):
        raise NotImplementedError("print_logs_by_time is not implemented")

    def print_last_n_logs(
        self,
        service,
        line_count,
        levels,
        components,
        clusters,
        instances,
        pods,
        raw_mode,
        strip_headers,
    ):
        raise NotImplementedError("print_last_n_logs is not implemented")

    def print_logs_by_offset(
        self,
        service,
        line_count,
        line_offset,
        levels,
        components,
        clusters,
        instances,
        pods,
        raw_mode,
        strip_headers,
    ):
        raise NotImplementedError("print_logs_by_offset is not implemented")


ScribeComponentStreamInfo = namedtuple(
    "ScribeComponentStreamInfo", "per_cluster, stream_name_fn, filter_fn, parse_fn"
)


@register_log_reader("scribereader")
class ScribeLogReader(LogReader):
    SUPPORTS_TAILING = True
    SUPPORTS_LINE_COUNT = True
    SUPPORTS_TIME = True

    COMPONENT_STREAM_INFO = {
        "default": ScribeComponentStreamInfo(
            per_cluster=False,
            stream_name_fn=get_log_name_for_service,
            filter_fn=paasta_log_line_passes_filter,
            parse_fn=None,
        ),
        "stdout": ScribeComponentStreamInfo(
            per_cluster=False,
            stream_name_fn=lambda service: get_log_name_for_service(
                service, prefix="app_output"
            ),
            filter_fn=paasta_app_output_passes_filter,
            parse_fn=None,
        ),
        "stderr": ScribeComponentStreamInfo(
            per_cluster=False,
            stream_name_fn=lambda service: get_log_name_for_service(
                service, prefix="app_output"
            ),
            filter_fn=paasta_app_output_passes_filter,
            parse_fn=None,
        ),
    }

    def __init__(self, cluster_map: Mapping[str, Any]) -> None:
        super().__init__()

        if scribereader is None:
            raise Exception(
                "scribereader package must be available to use scribereader log reading backend"
            )
        self.cluster_map = cluster_map

    def get_scribereader_selector(self, scribe_env: str) -> str:
        # this is kinda silly, but until the scribereader cli becomes more ergonomic
        # we'll need to do a little bit of string munging to let humans use scribereader
        # in the same way we are (tl;dr: scribereader has sorta confusing behavior between
        # what can be use for --ecosystem, --region, and --superregion and the fastest/least
        # hacky thing to figure out which we wanna use is that any env with a - in it is a region
        # and any without one is an ecosystem)
        return "-e" if "-" in scribe_env else "-r"

    def run_code_over_scribe_envs(
        self,
        clusters: Sequence[str],
        components: Iterable[str],
        callback: Callable[..., None],
    ) -> None:
        """Iterates over the scribe environments for a given set of clusters and components, executing
        functions for each component

        :param clusters: The set of clusters
        :param components: The set of components
        :param callback: The callback function. Gets called with (component_name, stream_info, scribe_env, cluster)
                         The cluster field will only be set if the component is set to per_cluster
        """
        scribe_envs: Set[str] = set()
        for cluster in clusters:
            scribe_envs.update(self.determine_scribereader_envs(components, cluster))
        log.debug("Connect to these scribe envs to tail scribe logs: %s" % scribe_envs)

        for scribe_env in scribe_envs:
            # These components all get grouped in one call for backwards compatibility
            grouped_components = {"build", "deploy", "monitoring"}

            if any([component in components for component in grouped_components]):
                stream_info = self.get_stream_info("default")
                callback(components, stream_info, scribe_env, cluster=None)

            non_defaults = set(components) - grouped_components
            for component in non_defaults:
                stream_info = self.get_stream_info(component)

                if stream_info.per_cluster:
                    for cluster in clusters:
                        callback([component], stream_info, scribe_env, cluster=cluster)
                else:
                    callback([component], stream_info, scribe_env, cluster=None)

    def get_stream_info(self, component: str) -> ScribeComponentStreamInfo:
        if component in self.COMPONENT_STREAM_INFO:
            return self.COMPONENT_STREAM_INFO[component]
        else:
            return self.COMPONENT_STREAM_INFO["default"]

    def tail_logs(
        self,
        service: str,
        levels: Sequence[str],
        components: Iterable[str],
        clusters: Sequence[str],
        instances: List[str],
        pods: Iterable[str] = None,
        raw_mode: bool = False,
        strip_headers: bool = False,
    ) -> None:
        """Sergeant function for spawning off all the right log tailing functions.

        NOTE: This function spawns concurrent processes and doesn't necessarily
        worry about cleaning them up! That's because we expect to just exit the
        main process when this function returns (as main() does). Someone calling
        this function directly with something like "while True: tail_paasta_logs()"
        may be very sad.

        NOTE: We try pretty hard to suppress KeyboardInterrupts to prevent big
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
        queue: Queue = Queue()
        spawned_processes = []

        def callback(
            components: Iterable[str],
            stream_info: ScribeComponentStreamInfo,
            scribe_env: str,
            cluster: str,
        ) -> None:
            kw = {
                "scribe_env": scribe_env,
                "service": service,
                "levels": levels,
                "components": components,
                "clusters": clusters,
                "instances": instances,
                "pods": pods,
                "queue": queue,
                "filter_fn": stream_info.filter_fn,
            }

            if stream_info.per_cluster:
                kw["stream_name"] = stream_info.stream_name_fn(service, cluster)
                kw["clusters"] = [cluster]
            else:
                kw["stream_name"] = stream_info.stream_name_fn(service)
            log.debug(
                "Running the equivalent of 'scribereader {} {} {}'".format(
                    self.get_scribereader_selector(scribe_env),
                    scribe_env,
                    kw["stream_name"],
                )
            )
            process = Process(target=self.scribe_tail, kwargs=kw)
            spawned_processes.append(process)
            process.start()

        self.run_code_over_scribe_envs(
            clusters=clusters, components=components, callback=callback
        )

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
                line = queue.get(block=True, timeout=0.1)
                print_log(line, levels, raw_mode, strip_headers)
            except Empty:
                try:
                    # If there's nothing in the queue, take this opportunity to make
                    # sure all the tailers are still running.
                    running_processes = [tt.is_alive() for tt in spawned_processes]
                    if not running_processes or not all(running_processes):
                        log.warn(
                            "Quitting because I expected %d log tailers to be alive but only %d are alive."
                            % (len(spawned_processes), running_processes.count(True))
                        )
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
                    log.warn("Terminating.")
                    break
            except KeyboardInterrupt:
                # Die peacefully rather than printing N threads worth of stack
                # traces.
                log.warn("Terminating.")
                break

    def print_logs_by_time(
        self,
        service: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        levels: Sequence[str],
        components: Iterable[str],
        clusters: Sequence[str],
        instances: List[str],
        pods: Iterable[str],
        raw_mode: bool,
        strip_headers: bool,
    ) -> None:
        aggregated_logs: List[Dict[str, Any]] = []

        def callback(
            components: Iterable[str],
            stream_info: ScribeComponentStreamInfo,
            scribe_env: str,
            cluster: str,
        ) -> None:
            if stream_info.per_cluster:
                stream_name = stream_info.stream_name_fn(service, cluster)
            else:
                stream_name = stream_info.stream_name_fn(service)

            ctx = self.scribe_get_from_time(
                scribe_env, stream_name, start_time, end_time
            )
            self.filter_and_aggregate_scribe_logs(
                scribe_reader_ctx=ctx,
                scribe_env=scribe_env,
                stream_name=stream_name,
                levels=levels,
                service=service,
                components=components,
                clusters=clusters,
                instances=instances,
                aggregated_logs=aggregated_logs,
                pods=pods,
                filter_fn=stream_info.filter_fn,
                parser_fn=stream_info.parse_fn,
                start_time=start_time,
                end_time=end_time,
            )

        self.run_code_over_scribe_envs(
            clusters=clusters, components=components, callback=callback
        )

        aggregated_logs = list(
            {line["raw_line"]: line for line in aggregated_logs}.values()
        )
        aggregated_logs.sort(key=lambda log_line: log_line["sort_key"])

        for line in aggregated_logs:
            print_log(line["raw_line"], levels, raw_mode, strip_headers)

    def print_last_n_logs(
        self,
        service: str,
        line_count: int,
        levels: Sequence[str],
        components: Iterable[str],
        clusters: Sequence[str],
        instances: List[str],
        pods: Iterable[str],
        raw_mode: bool,
        strip_headers: bool,
    ) -> None:
        aggregated_logs: List[Dict[str, Any]] = []

        def callback(
            components: Iterable[str],
            stream_info: ScribeComponentStreamInfo,
            scribe_env: str,
            cluster: str,
        ) -> None:

            if stream_info.per_cluster:
                stream_name = stream_info.stream_name_fn(service, cluster)
            else:
                stream_name = stream_info.stream_name_fn(service)

            ctx = self.scribe_get_last_n_lines(scribe_env, stream_name, line_count)
            self.filter_and_aggregate_scribe_logs(
                scribe_reader_ctx=ctx,
                scribe_env=scribe_env,
                stream_name=stream_name,
                levels=levels,
                service=service,
                components=components,
                clusters=clusters,
                instances=instances,
                aggregated_logs=aggregated_logs,
                pods=pods,
                filter_fn=stream_info.filter_fn,
                parser_fn=stream_info.parse_fn,
            )

        self.run_code_over_scribe_envs(
            clusters=clusters, components=components, callback=callback
        )
        aggregated_logs = list(
            {line["raw_line"]: line for line in aggregated_logs}.values()
        )
        aggregated_logs.sort(key=lambda log_line: log_line["sort_key"])

        for line in aggregated_logs:
            print_log(line["raw_line"], levels, raw_mode, strip_headers)

    def filter_and_aggregate_scribe_logs(
        self,
        scribe_reader_ctx: ContextManager,
        scribe_env: str,
        stream_name: str,
        levels: Sequence[str],
        service: str,
        components: Iterable[str],
        clusters: Sequence[str],
        instances: List[str],
        aggregated_logs: MutableSequence[Dict[str, Any]],
        pods: Iterable[str] = None,
        parser_fn: Callable = None,
        filter_fn: Callable = None,
        start_time: datetime.datetime = None,
        end_time: datetime.datetime = None,
    ) -> None:
        with scribe_reader_ctx as scribe_reader:
            try:
                for line in scribe_reader:
                    # temporary until all log lines are strings not byte strings
                    if isinstance(line, bytes):
                        line = line.decode("utf-8")
                    if parser_fn:
                        line = parser_fn(line, clusters, service)
                    if filter_fn:
                        if filter_fn(
                            line,
                            levels,
                            service,
                            components,
                            clusters,
                            instances,
                            pods,
                            start_time=start_time,
                            end_time=end_time,
                        ):
                            try:
                                parsed_line = json.loads(line)
                                timestamp = isodate.parse_datetime(
                                    parsed_line.get("timestamp")
                                )
                                if not timestamp.tzinfo:
                                    timestamp = pytz.utc.localize(timestamp)
                            except ValueError:
                                timestamp = pytz.utc.localize(datetime.datetime.min)

                            line = {"raw_line": line, "sort_key": timestamp}
                            aggregated_logs.append(line)
            except StreamTailerSetupError as e:
                if "No data in stream" in str(e):
                    log.warning(f"Scribe stream {stream_name} is empty on {scribe_env}")
                    log.warning(
                        "Don't Panic! This may or may not be a problem depending on if you expect there to be"
                    )
                    log.warning("output within this stream.")
                elif "Failed to connect" in str(e):
                    log.warning(
                        f"Couldn't connect to Scribe to tail {stream_name} in {scribe_env}"
                    )
                    log.warning(f"Please be in {scribe_env} to tail this log.")
                else:
                    raise

    def scribe_get_from_time(
        self,
        scribe_env: str,
        stream_name: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
    ) -> ContextManager:
        # Scribe connection details
        host, port = scribereader.get_tail_host_and_port(
            **scribe_env_to_locations(scribe_env),
        )

        # Recent logs might not be archived yet. Log warning message.
        warning_end_time = datetime.datetime.utcnow().replace(
            tzinfo=pytz.utc
        ) - datetime.timedelta(hours=4)
        if end_time > warning_end_time:
            log.warn("Recent logs might be incomplete. Consider tailing instead.")

        # scribereader, sadly, is not based on UTC timestamps. It uses YST
        # dates instead.
        start_date_yst = start_time.astimezone(
            pytz.timezone("America/Los_Angeles")
        ).date()
        end_date_yst = end_time.astimezone(pytz.timezone("America/Los_Angeles")).date()

        log.debug(
            "Running the equivalent of 'scribereader %s %s %s --min-date %s --max-date %s"
            % (
                self.get_scribereader_selector(scribe_env),
                scribe_env,
                stream_name,
                start_date_yst,
                end_date_yst,
            )
        )
        return scribereader.get_stream_reader(
            stream_name=stream_name,
            reader_host=host,
            reader_port=port,
            min_date=start_date_yst,
            max_date=end_date_yst,
        )

    def scribe_get_last_n_lines(
        self, scribe_env: str, stream_name: str, line_count: int
    ) -> ContextManager:
        # Scribe connection details
        host, port = scribereader.get_tail_host_and_port(
            **scribe_env_to_locations(scribe_env),
        )

        # The reason we need a fake context here is because scribereader is a bit inconsistent in its
        # returns. get_stream_reader returns a context that needs to be acquired for cleanup code but
        # get_stream_tailer simply returns an object that can be iterated over. We'd still like to have
        # the cleanup code for get_stream_reader to be executed by this function's caller and this is
        # one of the simpler ways to achieve it without having 2 if statements everywhere that calls
        # this method
        @contextmanager
        def fake_context():
            log.debug(
                f"Running the equivalent of 'scribereader -n {line_count} {self.get_scribereader_selector(scribe_env)} {scribe_env} {stream_name}'"
            )
            yield scribereader.get_stream_tailer(
                stream_name=stream_name,
                tailing_host=host,
                tailing_port=port,
                lines=line_count,
            )

        return fake_context()

    def scribe_tail(
        self,
        scribe_env: str,
        stream_name: str,
        service: str,
        levels: Sequence[str],
        components: Iterable[str],
        clusters: Sequence[str],
        instances: List[str],
        pods: Iterable[str],
        queue: Queue,
        filter_fn: Callable,
        parse_fn: Callable = None,
    ) -> None:
        """Creates a scribetailer for a particular environment.

        When it encounters a line that it should report, it sticks it into the
        provided queue.

        This code is designed to run in a thread as spawned by tail_paasta_logs().
        """
        try:
            log.debug(f"Going to tail {stream_name} scribe stream in {scribe_env}")
            host, port = scribereader.get_tail_host_and_port(
                **scribe_env_to_locations(scribe_env),
            )
            tailer = scribereader.get_stream_tailer(stream_name, host, port)
            for line in tailer:
                if parse_fn:
                    line = parse_fn(line, clusters, service)
                if filter_fn(
                    line, levels, service, components, clusters, instances, pods
                ):
                    queue.put(line)
        except KeyboardInterrupt:
            # Die peacefully rather than printing N threads worth of stack
            # traces.
            pass
        except StreamTailerSetupError as e:
            if "No data in stream" in str(e):
                log.warning(f"Scribe stream {stream_name} is empty on {scribe_env}")
                log.warning(
                    "Don't Panic! This may or may not be a problem depending on if you expect there to be"
                )
                log.warning("output within this stream.")
                # Enter a wait so the process isn't considered dead.
                # This is just a large number, since apparently some python interpreters
                # don't like being passed sys.maxsize.
                sleep(2**16)
            else:
                raise

    def determine_scribereader_envs(
        self, components: Iterable[str], cluster: str
    ) -> Set[str]:
        """Returns a list of environments that scribereader needs to connect
        to based on a given list of components and the cluster involved.

        Some components are in certain environments, regardless of the cluster.
        Some clusters do not match up with the scribe environment names, so
        we figure that out here"""
        envs: List[str] = []
        for component in components:
            # If a component has a 'source_env', we use that
            # otherwise we lookup what scribe env is associated with a given cluster
            env = LOG_COMPONENTS[component].get(
                "source_env", self.cluster_to_scribe_env(cluster)
            )
            if "additional_source_envs" in LOG_COMPONENTS[component]:
                envs += LOG_COMPONENTS[component]["additional_source_envs"]
            envs.append(env)
        return set(envs)

    def cluster_to_scribe_env(self, cluster: str) -> str:
        """Looks up the particular scribe env associated with a given paasta cluster.

        Scribe has its own "environment" key, which doesn't always map 1:1 with our
        cluster names, so we have to maintain a manual mapping.

        This mapping is deployed as a config file via puppet as part of the public
        config deployed to every server.
        """
        env = self.cluster_map.get(cluster, None)
        if env is None:
            print("I don't know where scribe logs for %s live?" % cluster)
            sys.exit(1)
        else:
            return env


@register_log_reader("vector-logs")
class VectorLogsReader(LogReader):
    SUPPORTS_TAILING = True
    SUPPORTS_TIME = True

    def __init__(
        self, cluster_map: Mapping[str, Any], nats_endpoint_map: Mapping[str, Any]
    ) -> None:
        super().__init__()

        if S3LogsReader is None:
            raise Exception("yelp_clog package must be available to use S3LogsReader")

        self.cluster_map = cluster_map
        self.nats_endpoint_map = nats_endpoint_map

    def get_superregion_for_cluster(self, cluster: str) -> Optional[str]:
        return self.cluster_map.get(cluster, None)

    def get_nats_endpoint_for_cluster(self, cluster: str) -> Optional[str]:
        return self.nats_endpoint_map.get(cluster, None)

    def print_logs_by_time(
        self,
        service,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        levels,
        components: Iterable[str],
        clusters,
        instances,
        pods,
        raw_mode,
        strip_headers,
    ) -> None:
        stream_name = get_log_name_for_service(service, prefix="app_output")
        superregion = self.get_superregion_for_cluster(clusters[0])
        reader = S3LogsReader(superregion)
        aggregated_logs: List[Dict[str, Any]] = []

        for line in reader.get_log_reader(
            log_name=stream_name, start_datetime=start_time, end_datetime=end_time
        ):
            if paasta_app_output_passes_filter(
                line,
                levels,
                service,
                components,
                clusters,
                instances,
                pods,
                start_time=start_time,
                end_time=end_time,
            ):
                try:
                    parsed_line = json.loads(line)
                    timestamp = isodate.parse_datetime(parsed_line.get("timestamp"))
                    if not timestamp.tzinfo:
                        timestamp = pytz.utc.localize(timestamp)
                except ValueError:
                    timestamp = pytz.utc.localize(datetime.datetime.min)

                line = {"raw_line": line, "sort_key": timestamp}
                aggregated_logs.append(line)

        aggregated_logs = list(
            {line["raw_line"]: line for line in aggregated_logs}.values()
        )
        aggregated_logs.sort(key=lambda log_line: log_line["sort_key"])

        for line in aggregated_logs:
            print_log(line["raw_line"], levels, raw_mode, strip_headers)

    def tail_logs(
        self,
        service: str,
        levels: Sequence[str],
        components: Iterable[str],
        clusters: Sequence[str],
        instances: List[str],
        pods: Iterable[str] = None,
        raw_mode: bool = False,
        strip_headers: bool = False,
    ) -> None:
        stream_name = get_log_name_for_service(service, prefix="app_output")
        endpoint = self.get_nats_endpoint_for_cluster(clusters[0])
        if not endpoint:
            raise NotImplementedError(
                "Tailing logs is not supported in this cluster yet, sorry"
            )

        async def tail_logs_from_nats() -> None:
            nc = await nats.connect(f"nats://{endpoint}")
            sub = await nc.subscribe(stream_name)

            while True:
                # Wait indefinitely for a new message (no timeout)
                msg = await sub.next_msg(timeout=None)
                decoded_data = msg.data.decode("utf-8")

                if paasta_app_output_passes_filter(
                    decoded_data,
                    levels,
                    service,
                    components,
                    clusters,
                    instances,
                    pods,
                ):
                    await a_sync.run(
                        print_log, decoded_data, levels, raw_mode, strip_headers
                    )

        a_sync.block(tail_logs_from_nats)


def scribe_env_to_locations(scribe_env) -> Mapping[str, Any]:
    """Converts a scribe environment to a dictionary of locations. The
    return value is meant to be used as kwargs for `scribereader.get_tail_host_and_port`.
    """
    locations = {"ecosystem": None, "region": None, "superregion": None}
    if scribe_env in scribereader.PROD_REGIONS:
        locations["region"] = scribe_env
    elif scribe_env in scribereader.PROD_SUPERREGIONS:
        locations["superregion"] = scribe_env
    else:  # non-prod envs are expressed as ecosystems
        locations["ecosystem"] = scribe_env
    return locations


def generate_start_end_time(
    from_string: str = "30m", to_string: str = None
) -> Tuple[datetime.datetime, datetime.datetime]:
    """Parses the --from and --to command line arguments to create python
    datetime objects representing the start and end times for log retrieval

    :param from_string: The --from argument, defaults to 30 minutes
    :param to_string: The --to argument, defaults to the time right now
    :return: A tuple containing start_time, end_time, which specify the interval of log retrieval
    """
    if to_string is None:
        end_time = datetime.datetime.utcnow()
    else:
        # Try parsing as a a natural time duration first, if that fails move on to
        # parsing as an ISO-8601 timestamp
        to_duration = timeparse(to_string)

        if to_duration is not None:
            end_time = datetime.datetime.utcnow() - datetime.timedelta(
                seconds=to_duration
            )
        else:
            end_time = isodate.parse_datetime(to_string)
            if not end_time:
                raise ValueError(
                    "--to argument not in ISO8601 format and not a valid pytimeparse duration"
                )

    from_duration = timeparse(from_string)
    if from_duration is not None:
        start_time = datetime.datetime.utcnow() - datetime.timedelta(
            seconds=from_duration
        )
    else:
        start_time = isodate.parse_datetime(from_string)

        if not start_time:
            raise ValueError(
                "--from argument not in ISO8601 format and not a valid pytimeparse duration"
            )

    # Covert the timestamps to something timezone aware
    start_time = pytz.utc.localize(start_time)
    end_time = pytz.utc.localize(end_time)

    if start_time > end_time:
        raise ValueError("Start time bigger than end time")

    return start_time, end_time


def validate_filtering_args(args: argparse.Namespace, log_reader: LogReader) -> bool:
    if not log_reader.SUPPORTS_LINE_OFFSET and args.line_offset is not None:
        print(
            PaastaColors.red(
                log_reader.__class__.__name__ + " does not support line based offsets"
            ),
            file=sys.stderr,
        )
        return False
    if not log_reader.SUPPORTS_LINE_COUNT and args.line_count is not None:
        print(
            PaastaColors.red(
                log_reader.__class__.__name__
                + " does not support line count based log retrieval"
            ),
            file=sys.stderr,
        )
        return False
    if not log_reader.SUPPORTS_TAILING and args.tail:
        print(
            PaastaColors.red(
                log_reader.__class__.__name__ + " does not support tailing"
            ),
            file=sys.stderr,
        )
        return False
    if not log_reader.SUPPORTS_TIME and (
        args.time_from is not None or args.time_to is not None
    ):
        print(
            PaastaColors.red(
                log_reader.__class__.__name__ + " does not support time based offsets"
            ),
            file=sys.stderr,
        )
        return False

    if args.tail and (
        args.line_count is not None
        or args.time_from is not None
        or args.time_to is not None
        or args.line_offset is not None
    ):
        print(
            PaastaColors.red(
                "You cannot specify line/time based filtering parameters when tailing"
            ),
            file=sys.stderr,
        )
        return False

    # Can't have both
    if args.line_count is not None and args.time_from is not None:
        print(
            PaastaColors.red("You cannot filter based on both line counts and time"),
            file=sys.stderr,
        )
        return False

    return True


def pick_default_log_mode(
    args: argparse.Namespace,
    log_reader: LogReader,
    service: str,
    levels: Sequence[str],
    components: Iterable[str],
    clusters: Sequence[str],
    instances: List[str],
    pods: Iterable[str],
) -> int:
    if log_reader.SUPPORTS_LINE_COUNT:
        print(
            PaastaColors.cyan(
                "Fetching 100 lines and applying filters. Try -n 1000 for more lines..."
            ),
            file=sys.stderr,
        )
        log_reader.print_last_n_logs(
            service=service,
            line_count=100,
            levels=levels,
            components=components,
            clusters=clusters,
            instances=instances,
            pods=pods,
            raw_mode=args.raw_mode,
            strip_headers=args.strip_headers,
        )
        return 0
    elif log_reader.SUPPORTS_TIME:
        start_time, end_time = generate_start_end_time()
        print(
            PaastaColors.cyan(
                "Fetching a specific time period and applying filters..."
            ),
            file=sys.stderr,
        )
        log_reader.print_logs_by_time(
            service=service,
            start_time=start_time,
            end_time=end_time,
            levels=levels,
            components=components,
            clusters=clusters,
            instances=instances,
            pods=pods,
            raw_mode=args.raw_mode,
            strip_headers=args.strip_headers,
        )
        return 0
    elif log_reader.SUPPORTS_TAILING:
        print(
            PaastaColors.cyan("Tailing logs and applying filters..."), file=sys.stderr
        )
        log_reader.tail_logs(
            service=service,
            levels=levels,
            components=components,
            clusters=clusters,
            instances=instances,
            pods=pods,
            raw_mode=args.raw_mode,
            strip_headers=args.strip_headers,
        )
        return 0
    return 1


def pick_log_reader(cluster: str, components: Set[str]) -> LogReader:
    uses_log_readers = load_system_paasta_config().use_multiple_log_readers()
    if uses_log_readers and cluster in uses_log_readers:
        return get_log_reader(components)
    else:
        return get_default_log_reader()


def paasta_logs(args: argparse.Namespace) -> int:
    """Print the logs for as Paasta service.
    :param args: argparse.Namespace obj created from sys.args by cli"""
    soa_dir = args.soa_dir

    service = figure_out_service_name(args, soa_dir)

    clusters = args.cluster
    if (
        args.cluster is None
        or args.instance is None
        or len(args.instance.split(",")) > 2
    ):
        print(
            PaastaColors.red("You must specify one cluster and one instance."),
            file=sys.stderr,
        )
        return 1

    if verify_instances(args.instance, service, clusters, soa_dir):
        return 1

    instance = args.instance

    if args.pods is None:
        pods = None
    else:
        pods = args.pods.split(",")

    components = args.components
    if "app_output" in args.components:
        components.remove("app_output")
        components.add("stdout")
        components.add("stderr")

    if args.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    levels = [DEFAULT_LOGLEVEL, "debug"]

    log.debug(f"Going to get logs for {service} on cluster {clusters}")

    log_reader = pick_log_reader(clusters[0], components)

    if not validate_filtering_args(args, log_reader):
        return 1
    # They haven't specified what kind of filtering they want, decide for them
    if args.line_count is None and args.time_from is None and not args.tail:
        return pick_default_log_mode(
            args, log_reader, service, levels, components, clusters, instance, pods
        )
    if args.tail:
        print(
            PaastaColors.cyan("Tailing logs and applying filters..."), file=sys.stderr
        )
        log_reader.tail_logs(
            service=service,
            levels=levels,
            components=components,
            clusters=clusters,
            instances=[instance],
            pods=pods,
            raw_mode=args.raw_mode,
            strip_headers=args.strip_headers,
        )
        return 0

    # If the logger doesn't support offsetting the number of lines by a particular line number
    # there is no point in distinguishing between a positive/negative number of lines since it
    # can only get the last N lines
    if not log_reader.SUPPORTS_LINE_OFFSET and args.line_count is not None:
        args.line_count = abs(args.line_count)

    # Handle line based filtering
    if args.line_count is not None and args.line_offset is None:
        log_reader.print_last_n_logs(
            service=service,
            line_count=args.line_count,
            levels=levels,
            components=components,
            clusters=clusters,
            instances=[instance],
            pods=pods,
            raw_mode=args.raw_mode,
            strip_headers=args.strip_headers,
        )
        return 0
    elif args.line_count is not None and args.line_offset is not None:
        log_reader.print_logs_by_offset(
            service=service,
            line_count=args.line_count,
            line_offset=args.line_offset,
            levels=levels,
            components=components,
            clusters=clusters,
            instances=[instance],
            pods=pods,
            raw_mode=args.raw_mode,
            strip_headers=args.strip_headers,
        )
        return 0

    # Handle time based filtering
    try:
        start_time, end_time = generate_start_end_time(args.time_from, args.time_to)
    except ValueError as e:
        print(PaastaColors.red(str(e)), file=sys.stderr)
        return 1

    log_reader.print_logs_by_time(
        service=service,
        start_time=start_time,
        end_time=end_time,
        levels=levels,
        components=components,
        clusters=clusters,
        instances=[instance],
        pods=pods,
        raw_mode=args.raw_mode,
        strip_headers=args.strip_headers,
    )
    return 0
