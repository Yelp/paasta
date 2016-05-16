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
"""
Gets the cluster usage of a mesos cluster and alerts based
on percentage thresholds.

Can only be run on the current mesos-master leader- if this host
isn't the leader, the script exits immediately.
"""
import argparse
import logging

import pysensu_yelp

from paasta_tools.mesos_tools import get_mesos_stats
from paasta_tools.utils import load_system_paasta_config


log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='Checks a mesos clusters resource utilization.')
    parser.add_argument('-p', '--precent', default=90,
                        dest='percent',
                        help='The percentage threshold before alerting. Defaults to 90.',)
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest="verbose", default=False)
    args = parser.parse_args()
    return args


def send_event(status, output):
    result_dict = {
        'name': 'cluster_utilization',
        'team': 'paasta',
        'runbook': 'y/rb-mesos',
        'tip': 'See the mesos web interface?',
        'page': True,
        'alert_after': '15m',
        'check_every': '5m',
        'realert_every': -1,
        'status': status,
        'output': output,
        'source': 'paasta-%s' % load_system_paasta_config().get_cluster()
    }
    pysensu_yelp.send_event(**result_dict)


def check_thresholds(percent):
    """Gets the current state of the mesos cluster and compares it
    to a given percentage. If either the ram or CPU utilization is over that
    percentage, the sensu event will be sent with a status code of 2."""
    stats = get_mesos_stats()
    over_threshold = False
    output = ""
    current_mem = stats['master/mem_percent']
    current_cpu = stats['master/cpus_percent']
    current_disk = stats['master/disk_percent']
    percent = int(percent)
    cpu_print_tuple = (percent, current_cpu)
    mem_print_tuple = (percent, current_mem)
    disk_print_tuple = (percent, current_disk)

    if current_mem >= percent:
        output += "CRITICAL: Memory usage is over %d%%! Currently at %f%%!\n" % mem_print_tuple
        over_threshold = True
    else:
        output += "OK: Memory usage is under %d%%. (Currently at %f%%)\n" % mem_print_tuple

    if current_cpu >= percent:
        output += "CRITICAL: CPU usage is over %d%%! Currently at %f%%!\n" % cpu_print_tuple
        over_threshold = True
    else:
        output += "OK: CPU usage is under %d%%. (Currently at %f%%)\n" % cpu_print_tuple

    if current_disk >= percent:
        output += "CRITICAL: Disk usage is over %d%%! Currently at %f%%!\n" % disk_print_tuple
        over_threshold = True
    else:
        output += "OK: Disk usage is under %d%%. (Currently at %f%%)\n" % disk_print_tuple

    if over_threshold is True:
        status = 2
    else:
        status = 0

    send_event(status, output)
    return output


def main():
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)
    print check_thresholds(args.percent)


if __name__ == "__main__":
    main()
