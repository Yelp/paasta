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
Usage: ./cleanup_maintenance.py

Clean up boxes that should no longer be marked as 'draining' or 'down' for
maintenance. Also cleanup the associated dynamic reservations.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import logging
import sys

from paasta_tools.mesos_maintenance import get_draining_hosts
from paasta_tools.mesos_maintenance import get_hosts_forgotten_down
from paasta_tools.mesos_maintenance import get_hosts_forgotten_draining
from paasta_tools.mesos_maintenance import reserve_all_resources
from paasta_tools.mesos_maintenance import seconds_to_nanoseconds
from paasta_tools.mesos_maintenance import undrain
from paasta_tools.mesos_maintenance import unreserve_all_resources
from paasta_tools.mesos_maintenance import up
from paasta_tools.mesos_tools import get_slaves


log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='Cleans up forgotten maintenance cruft.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest="verbose", default=False)
    args = parser.parse_args()
    return args


def cleanup_forgotten_draining():
    """Clean up hosts forgotten draining"""
    log.debug("Cleaning up hosts forgotten draining")
    hosts_forgotten_draining = get_hosts_forgotten_draining(grace=seconds_to_nanoseconds(10 * 60))
    if hosts_forgotten_draining:
        undrain(hostnames=hosts_forgotten_draining)
    else:
        log.debug("No hosts forgotten draining")


def cleanup_forgotten_down():
    """Clean up hosts forgotten down"""
    log.debug("Cleaning up hosts forgotten down")
    hosts_forgotten_down = get_hosts_forgotten_down(grace=seconds_to_nanoseconds(10 * 60))
    if hosts_forgotten_down:
        up(hostnames=hosts_forgotten_down)
    else:
        log.debug("No hosts forgotten down")


def unreserve_all_resources_on_non_draining_hosts():
    """Unreserve all resources on non-draining hosts"""
    log.debug("Unreserving all resources on non-draining hosts")
    slaves = get_slaves()
    hostnames = [slave['hostname'] for slave in slaves]
    draining_hosts = get_draining_hosts()
    non_draining_hosts = list(set(hostnames) - set(draining_hosts))
    if non_draining_hosts:
        unreserve_all_resources(hostnames=non_draining_hosts)
    else:
        log.debug("No non-draining hosts")


def reserve_all_resources_on_draining_hosts():
    """Reserve all resources on draining hosts"""
    log.debug("Reserving all resources on draining hosts")
    draining_hosts = get_draining_hosts()
    if draining_hosts:
        reserve_all_resources(hostnames=draining_hosts)
    else:
        log.debug("No draining hosts")


def main():
    log.debug("Cleaning up maintenance cruft")
    args = parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    cleanup_forgotten_draining()
    cleanup_forgotten_down()
    unreserve_all_resources_on_non_draining_hosts()
    reserve_all_resources_on_draining_hosts()


if __name__ == "__main__":
    if main():
        sys.exit(0)
    sys.exit(1)
