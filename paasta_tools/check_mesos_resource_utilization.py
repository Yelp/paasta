#!/usr/bin/env python
"""
Gets the cluster usage of a mesos cluster and alerts based
on percentage thresholds.

Can only be run on the current mesos-master leader- if this host
isn't the leader, the script exits immediately.
"""

import argparse
import logging
import sys

import pysensu_yelp
import paasta_tools.marathon_tools
from paasta_tools.mesos_tools import fetch_mesos_stats


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
        'team': 'mesos',
        'runbook': 'y/rb-mesos',
        'tip': 'See the mesos web interface?',
        'page': True,
        'alert_after': '15m',
        'check_every': '5m',
        'realert_every': -1,
        'status': status,
        'output': output,
        'source': 'mesos-%s' % paasta_tools.marathon_tools.get_cluster()
    }
    pysensu_yelp.send_event(**result_dict)


def check_thresholds(percent):
    """Gets the current state of the mesos cluster and compares it
    to a given percentage. If either the ram or CPU utilization is over that
    percentage, the sensu event will be sent with a status code of 2."""
    stats = fetch_mesos_stats()
    over_threshold = False
    output = ""
    current_mem = stats['master/mem_percent']
    current_cpu = stats['master/cpus_percent']
    percent = int(percent)
    cpu_print_tuple = (percent, current_cpu)
    mem_print_tuple = (percent, current_mem)
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
    if over_threshold is True:
        status = 2
    else:
        status = 0
    send_event(status, output)
    return output


def main():
    args = parse_args()
    if args.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)
    print check_thresholds(args.percent)


if __name__ == "__main__":
    if paasta_tools.marathon_tools.is_mesos_leader():
        main()
    else:
        print "No the leader. Exiting 0."
        sys.exit(0)
