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
import time
from random import choice

import pysensu_yelp
from scribereader import scribereader

from paasta_tools import monitoring_tools
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config


def parse_args():
    parser = argparse.ArgumentParser(description=(
        'Check the tmp_paasta_oom_events stream and report to Sensu '
        'if there are any OOM events.',
    ))
    parser.add_argument(
        '-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    return parser.parse_args()


def get_superregion_from_file():
    with open("/nail/etc/superregion") as f:
        return f.read().strip()


def oom_events(cluster, num_lines=1000):
    """Iterate over latest 'num_lines' lines in the tmp_paasta_oom_events stream"""
    host_and_port = choice(scribereader.get_default_scribe_hosts(tail=True))
    host = host_and_port['host']
    port = host_and_port['port']
    stream = scribereader.get_stream_tailer(
        'tmp_paasta_oom_events', host, port, True,
        num_lines, superregion=get_superregion_from_file(),
    )
    for line in stream:
        try:
            j = json.loads(line)
            if j.get('cluster', '') == cluster:
                yield j
        except json.decoder.JSONDecodeError:
            pass


def latest_oom_events(cluster, interval=60):
    """
    :returns: {(service, instance): number_of_oom_events, ... } if number_of_oom_events > 0
    """
    start_timestamp = int(time.time()) - interval
    res = {}
    for e in oom_events(cluster):
        if e['timestamp'] > start_timestamp:
            key = (e['service'], e['instance'])
            res[key] = res.get(key, 0) + 1
    return res


def compose_sensu_status(service_instance, num_oom_events):
    if num_oom_events == 0:
        return (
            pysensu_yelp.Status.OK,
            'oom-killer is not killing processes in %s.%s containers.' %
            (service_instance[0], service_instance[1]),
        )
    else:
        return (
            pysensu_yelp.Status.CRITICAL,
            'oom-killer is killing %d processes a minute in %s.%s containers.' %
            (num_oom_events, service_instance[0], service_instance[1]),
        )


def send_sensu_event(instance, status):
    monitoring_overrides = instance.get_monitoring()
    monitoring_overrides['page'] = False
    monitoring_overrides['ticket'] = False
    monitoring_overrides['team'] = 'noop'
    monitoring_overrides['irc_channels'] = ['#adudkotest']
    monitoring_overrides['runbook'] = ['http://y/none']
    monitoring_tools.send_event(
        service=instance.service,
        check_name='check_oom_events',
        overrides=monitoring_overrides,
        status=status[0],
        output=status[1],
        soa_dir=instance.soa_dir,
    )


def main():
    soa_dir = parse_args().soa_dir
    cluster = load_system_paasta_config().get_cluster()
    victims = latest_oom_events(cluster)
    for s_i in get_services_for_cluster(cluster):
        instance = get_instance_config(
            s_i[0], s_i[1], cluster,
            load_deployments=False, soa_dir=soa_dir,
        )
        send_sensu_event(instance, compose_sensu_status(s_i, victims.get(s_i, 0)))


if __name__ == '__main__':
    main()
