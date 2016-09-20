#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
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
import logging
import sys
from socket import getfqdn
from socket import gethostname

from paasta_tools import mesos_maintenance


log = logging.getLogger(__name__)


def parse_args():
    """Parses the command line arguments passed to this script"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d', '--duration',
        type=mesos_maintenance.parse_timedelta,
        default='1h',
        help="Duration of the maintenance window. Any pytimeparse unit is supported.",
    )
    parser.add_argument(
        '-s', '--start',
        type=mesos_maintenance.parse_datetime,
        default=str(mesos_maintenance.now()),
        help="Time to start the maintenance window. Defaults to now.",
    )
    parser.add_argument(
        'action',
        choices=[
            'down',
            'drain',
            'is_host_down',
            'is_host_drained',
            'is_host_draining',
            'is_hosts_past_maintenance_end',
            'is_hosts_past_maintenance_start',
            'is_safe_to_drain',
            'is_safe_to_kill',
            'schedule',
            'status',
            'undrain',
            'up',
        ],
        help="Action to perform on the speicifed hosts",
    )
    parser.add_argument(
        'hostname',
        nargs='*',
        default=[getfqdn()],
        help='Hostname(s) of machine(s) to start draining. '
        'You can specify <hostname>|<ip> to avoid querying DNS to determine the corresponding IP.',
    )
    parser.add_argument(
        '-v', '--verbose',
        action='count',
        dest="verbose",
        default=0,
        help="Print out more output."
    )
    return parser.parse_args()


def is_safe_to_kill(hostname):
    """Checks if a host has drained or reached its maintenance window
    :param hostname: hostname to check
    :returns: True or False
    """
    return mesos_maintenance.is_host_drained(hostname) or \
        mesos_maintenance.is_host_past_maintenance_start(hostname)


def is_hostname_local(hostname):
    return hostname == 'localhost' or \
        hostname == getfqdn() or hostname == gethostname()


def is_safe_to_drain(hostname):
    """Checks if a host has healthy tasks running locally that have low
    replication in other places
    :param hostname: hostname to check
    :returns: True or False
    """
    if not is_hostname_local(hostname):
        print (
            "Due to the way is_safe_to_drain is implemented, it can only work "
            "on localhost."
        )
        return False
    return not are_local_tasks_in_danger()


def are_local_tasks_in_danger():
    return False


def paasta_maintenance():
    """Manipulate the maintenance state of a PaaSTA host.
    :returns: None
    """
    args = parse_args()

    if args.verbose >= 2:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose == 1:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    action = args.action
    hostnames = args.hostname

    if action != 'status' and not hostnames:
        print "You must specify one or more hostnames"
        return

    start = args.start
    duration = args.duration

    ret = "Done"
    if action == 'drain':
        mesos_maintenance.drain(hostnames, start, duration)
    elif action == 'undrain':
        mesos_maintenance.undrain(hostnames)
    elif action == 'down':
        mesos_maintenance.down(hostnames)
    elif action == 'up':
        mesos_maintenance.up(hostnames)
    elif action == 'status':
        ret = "%s" % mesos_maintenance.status()
    elif action == 'schedule':
        ret = "%s" % mesos_maintenance.schedule()
    elif action == 'is_safe_to_drain':
        ret = is_safe_to_drain(hostnames[0])
    elif action == 'is_safe_to_kill':
        ret = is_safe_to_kill(hostnames[0])
    elif action == 'is_host_drained':
        ret = mesos_maintenance.is_host_drained(hostnames[0])
    elif action == 'is_host_down':
        ret = mesos_maintenance.is_host_down(hostnames[0])
    elif action == 'is_host_draining':
        ret = mesos_maintenance.is_host_draining(hostnames[0])
    elif action == 'is_host_past_maintenance_start':
        ret = mesos_maintenance.is_host_past_maintenance_start(hostnames[0])
    elif action == 'is_host_past_maintenance_end':
        ret = mesos_maintenance.is_host_past_maintenance_end(hostnames[0])
    else:
        raise NotImplementedError("Action: '%s' is not implemented." % action)
    print ret
    return ret

if __name__ == '__main__':
    if paasta_maintenance():
        sys.exit(0)
    sys.exit(1)
