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
import argparse
import json
import sys
import time

from pysensu_yelp import Status

from paasta_tools import monitoring_tools
from paasta_tools.cli.cmds.logs import scribe_env_to_locations
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config

try:
    from scribereader import scribereader
    from scribereader.clog.readers import StreamTailerSetupError
except ImportError:
    scribereader = None


OOM_EVENTS_STREAM = "tmp_paasta_oom_events"


def compose_check_name_for_service_instance(check_name, service, instance):
    return f"{check_name}.{service}.{instance}"


def parse_args(args):
    parser = argparse.ArgumentParser(
        description=(
            "Check the %s stream and report to Sensu if"
            " there are any OOM events." % OOM_EVENTS_STREAM
        )
    )
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        "-r",
        "--realert-every",
        dest="realert_every",
        type=int,
        default=1,
        help="Sensu 'realert_every' to use.",
    )
    parser.add_argument(
        "--check-interval",
        dest="check_interval",
        type=int,
        default=1,
        help="How often this check runs, in minutes.",
    )
    parser.add_argument(
        "--alert-threshold",
        dest="alert_threshold",
        type=int,
        default=1,
        help="Number of OOM kills required in the check interval to send an alert.",
    )
    parser.add_argument(
        "-s",
        "--superregion",
        dest="superregion",
        required=True,
        help="The superregion to read OOM events from.",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="Print Sensu alert events instead of sending them",
    )
    return parser.parse_args(args)


def read_oom_events_from_scribe(cluster, superregion, num_lines=1000):
    """Read the latest 'num_lines' lines from OOM_EVENTS_STREAM and iterate over them."""
    # paasta configs incls a map for cluster -> env that is expected by scribe
    log_reader_config = load_system_paasta_config().get_log_reader()
    cluster_map = log_reader_config["options"]["cluster_map"]
    scribe_env = cluster_map[cluster]

    # `scribe_env_to_locations` slightly mutates the scribe env based on whether
    # or not it is in dev or prod
    host, port = scribereader.get_tail_host_and_port(
        **scribe_env_to_locations(scribe_env),
    )
    stream = scribereader.get_stream_tailer(
        stream_name=OOM_EVENTS_STREAM,
        tailing_host=host,
        tailing_port=port,
        lines=num_lines,
        superregion=superregion,
    )
    try:
        for line in stream:
            try:
                j = json.loads(line)
                if j.get("cluster", "") == cluster:
                    yield j
            except json.decoder.JSONDecodeError:
                pass
    except StreamTailerSetupError as e:
        if "No data in stream" in str(e):
            pass
        else:
            raise e


def latest_oom_events(cluster, superregion, interval=60):
    """
    :returns: {(service, instance): [OOMEvent, OOMEvent,...] }
              if the number of events > 0
    """
    start_timestamp = int(time.time()) - interval
    res = {}
    for e in read_oom_events_from_scribe(cluster, superregion):
        if e["timestamp"] > start_timestamp:
            key = (e["service"], e["instance"])
            res.setdefault(key, set()).add(e.get("container_id", ""))
    return res


def compose_sensu_status(
    instance, oom_events, is_check_enabled, alert_threshold, check_interval
):
    """
    :param instance: InstanceConfig
    :param oom_events: a list of OOMEvents
    :param is_check_enabled: boolean to indicate whether the check enabled for the instance
    """
    interval_string = f"{check_interval} minute(s)"
    instance_name = f"{instance.service}.{instance.instance}"
    if not is_check_enabled:
        return (Status.OK, f"This check is disabled for {instance_name}.")
    if not oom_events:
        return (
            Status.OK,
            f"No oom events for {instance_name} in the last {interval_string}.",
        )
    elif len(oom_events) >= alert_threshold:
        return (
            Status.CRITICAL,
            f"The Out Of Memory killer killed processes for {instance_name} "
            f"in the last {interval_string}.",
        )
    else:
        # If the number of OOM kills isn't above the alert threshold,
        # don't send anything. This will keep an alert open if it's already open,
        # but won't start a new alert if there wasn't one yet
        return None


def send_sensu_event(instance, oom_events, args):
    """
    :param instance: InstanceConfig
    :param oom_events: a list of OOMEvents
    """
    check_name = compose_check_name_for_service_instance(
        "oom-killer", instance.service, instance.instance
    )
    monitoring_overrides = instance.get_monitoring()
    status = compose_sensu_status(
        instance=instance,
        oom_events=oom_events,
        is_check_enabled=monitoring_overrides.get("check_oom_events", True),
        alert_threshold=args.alert_threshold,
        check_interval=args.check_interval,
    )
    if not status:
        return

    memory_limit = instance.get_mem()
    try:
        memory_limit_str = f"{int(memory_limit)}MB"
    except ValueError:
        memory_limit_str = memory_limit

    monitoring_overrides.update(
        {
            "page": False,
            "alert_after": "0m",
            "realert_every": args.realert_every,
            "runbook": "y/check-oom-events",
            "tip": (
                "Follow the runbook to investigate and rightsize memory usage "
                f"(curr: {memory_limit_str})"
            ),
        }
    )
    return monitoring_tools.send_event(
        service=instance.service,
        check_name=check_name,
        overrides=monitoring_overrides,
        status=status[0],
        output=status[1],
        soa_dir=instance.soa_dir,
        dry_run=args.dry_run,
    )


def main(sys_argv):
    args = parse_args(sys_argv[1:])
    cluster = load_system_paasta_config().get_cluster()
    victims = latest_oom_events(
        cluster, args.superregion, interval=(60 * args.check_interval)
    )

    for (service, instance) in get_services_for_cluster(cluster, soa_dir=args.soa_dir):
        try:
            instance_config = get_instance_config(
                service=service,
                instance=instance,
                cluster=cluster,
                load_deployments=False,
                soa_dir=args.soa_dir,
            )
            oom_events = victims.get((service, instance), [])
            send_sensu_event(instance_config, oom_events, args)
        except NotImplementedError:  # When instance_type is not supported by get_instance_config
            pass


if __name__ == "__main__":
    main(sys.argv)
