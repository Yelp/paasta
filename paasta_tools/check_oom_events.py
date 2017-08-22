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
from collections import namedtuple
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

OOMEvent = namedtuple('OOMEvent', ['hostname', 'container_id', 'process_name'])


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


def read_oom_events_from_scribe(cluster, superregion, num_lines=1000):
    """Read the latest 'num_lines' lines from OOM_EVENTS_STREAM and iterate over them."""
    host_port = choice(scribereader.get_default_scribe_hosts(tail=True))
    stream = scribereader.get_stream_tailer(
        stream_name=OOM_EVENTS_STREAM,
        tailing_host=host_port['host'],
        tailing_port=host_port['port'],
        use_kafka=True,
        lines=num_lines,
        superregion=superregion,
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
    :returns: {(service, instance): [OOMEvent, OOMEvent,...] }
              if the number of events > 0
    """
    start_timestamp = int(time.time()) - interval
    res = {}
    for e in read_oom_events_from_scribe(cluster, superregion):
        if e['timestamp'] > start_timestamp:
            key = (e['service'], e['instance'])
            res.setdefault(key, []).append(
                OOMEvent(
                    hostname=e.get('hostname', ''),
                    container_id=e.get('container_id', ''),
                    process_name=e.get('process_name', ''),
                ),
            )
    return res


def compose_sensu_status(instance, events):
    """
    :param instance: InstanceConfig
    :param events: a list of OOMEvents
    """
    if len(events) == 0:
        return (
            Status.OK, 'No oom events for %s.%s in the last minute.' %
            (instance.service, instance.instance),
        )
    else:
        return (
            Status.CRITICAL, 'The Out Of Memory killer killed %d processes (%s) '
            'in the last minute in %s.%s containers.' % (
                len(events),
                ','.join(sorted({e.process_name for e in events if e.process_name})),
                instance.service,
                instance.instance,
            ),
        )


def send_sensu_event(instance, status, args):
    """
    :param instance: InstanceConfig
    :patam status: a Sensu status tuple
    """
    check_name = compose_check_name_for_service_instance(
        'oom-killer',
        instance.service,
        instance.instance,
    )
    monitoring_overrides = instance.get_monitoring()
    monitoring_overrides['team'] = 'noop'  # TODO: remove after testing
    monitoring_overrides['page'] = False
    monitoring_overrides['ticket'] = False
    monitoring_overrides['notification_email'] = False  # TODO: remove after testing
    monitoring_overrides['irc_channels'] = ['#oom-test']  # TODO: remove after testing
    monitoring_overrides['alert_after'] = '0m'
    monitoring_overrides['runbook'] = ['http://y/none']  # TODO: needs a link
    monitoring_overrides['realert_every'] = args.realert_every
    monitoring_overrides['tip'] = 'Try bumping the memory limit past %dMB' % instance.get_mem()
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
    for (service, instance) in get_services_for_cluster(cluster, soa_dir=args.soa_dir):
        instance_config = get_instance_config(
            service=service,
            instance=instance,
            cluster=cluster,
            load_deployments=False,
            soa_dir=args.soa_dir,
        )
        status = compose_sensu_status(instance_config, victims.get((service, instance), []))
        send_sensu_event(instance_config, status, args)


if __name__ == '__main__':
    main(sys.argv)
