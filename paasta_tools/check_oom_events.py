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
from random import choice

from pysensu_yelp import Status

from paasta_tools import monitoring_tools
from paasta_tools.chronos_tools import compose_check_name_for_service_instance
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config

try:
    from scribereader import scribereader
except ImportError:
    scribereader = None


OOM_EVENTS_STREAM = 'tmp_paasta_oom_events'


def parse_args(args):
    parser = argparse.ArgumentParser(description=(
        'Check the %s stream and report to Sensu if'
        ' there are any OOM events.' % OOM_EVENTS_STREAM
    ))
    parser.add_argument(
        '-d', '--soa-dir', dest="soa_dir", default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        '-r', '--realert-every', dest="realert_every", type=int, default=1,
        help="Sensu 'realert_every' to use.",
    )
    parser.add_argument(
        '-s', '--superregion', dest="superregion", required=True,
        help="The superregion to read OOM events from.",
    )
    return parser.parse_args(args)


def oom_events(cluster, superregion, num_lines=1000):
    """Read the latest 'num_lines' lines from OOM_EVENTS_STREAM and iterate over them."""
    host_port = choice(scribereader.get_default_scribe_hosts(tail=True))
    stream = scribereader.get_stream_tailer(
        OOM_EVENTS_STREAM, host_port['host'], host_port['port'], True,
        num_lines, superregion=superregion,
    )
    for line in stream:
        try:
            j = json.loads(line)
            if j.get('cluster', '') == cluster:
                yield j
        except json.decoder.JSONDecodeError:
            pass


def latest_oom_events(cluster, superregion, interval=60):
    """
    :returns: {(service, instance): [(hostname, container_id, process_name), ...] }
              if the number of events > 0
    """
    start_timestamp = int(time.time()) - interval
    res = {}
    for e in oom_events(cluster, superregion):
        if e['timestamp'] > start_timestamp:
            key = (e['service'], e['instance'])
            res.setdefault(key, []).append((
                e.get('hostname', ''),
                e.get('container_id', ''),
                e.get('process_name', ''),
            ))
    return res


def compose_sensu_status(service_instance, events):
    """
    :param service_instance: a tuple (service, instance)
    :param events: a list of tuples (hostname, container_id, process_name)
    """
    if len(events) == 0:
        return (Status.OK, 'oom-killer is calm.')
    elif len(events) == 1:
        return (Status.CRITICAL, 'killing 1 process/min (%s).' % events[0][2])
    else:
        return (
            Status.CRITICAL, 'killing %d processes/min (%s).' %
            (len(events), ','.join(sorted(e[2] for e in events if e[2]))),
        )


def send_sensu_event(instance, status, args):
    check_name = compose_check_name_for_service_instance(
        'oom-killer',
        instance.service,
        instance.instance,
    )
    monitoring_overrides = instance.get_monitoring()
    monitoring_overrides['page'] = False
    monitoring_overrides['ticket'] = False
    monitoring_overrides['alert_after'] = '1m'
    monitoring_overrides['runbook'] = ['http://y/none']  # TODO: needs a link
    monitoring_overrides['realert_every'] = args.realert_every
    monitoring_overrides['tip'] = 'Increase memory limit.'
    if monitoring_overrides['team'] == 'operations':  # TODO: remove after initial testing
        return monitoring_tools.send_event(
            service=instance.service,
            check_name=check_name,
            overrides=monitoring_overrides,
            status=status[0],
            output=status[1],
            soa_dir=instance.soa_dir,
        )


def main(sys_argv):
    args = parse_args(sys_argv[1:])
    cluster = load_system_paasta_config().get_cluster()
    victims = latest_oom_events(cluster, args.superregion)
    for s_i in get_services_for_cluster(cluster, soa_dir=args.soa_dir):
        instance = get_instance_config(
            s_i[0], s_i[1], cluster,
            load_deployments=False, soa_dir=args.soa_dir,
        )
        send_sensu_event(instance, compose_sensu_status(s_i, victims.get(s_i, [])), args)


if __name__ == '__main__':
    main(sys.argv)
