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
import logging
import time

from paasta_tools.long_running_service_tools import AUTOSCALING_ZK_ROOT
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print
from paasta_tools.utils import ZookeeperPool

log = logging.getLogger(__name__)

DEFAULT_PAUSE_DURATION = 120


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description='',
    )
    parser.add_argument(
        '-t',
        '--timeout',
        type=int,
        default=DEFAULT_PAUSE_DURATION,
        dest="timeout",
        help='amount of time to pause autoscaler for, in minutes',
    )
    parser.add_argument(
        '-r', '--resume',
        help='Resume autoscaling (unpause) in a cluster',
        action='store_true',
        dest='resume',
        default=False,
    )

    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    current_time = time.time()
    expiry_time = 0
    if args.resume:
        expiry_time = current_time - 10
    else:
        expiry_time = current_time + (60 * args.timeout)
    zk_pause_autoscale_path = '{}/paused'.format(AUTOSCALING_ZK_ROOT)
    cluster = load_system_paasta_config().get_cluster()

    with ZookeeperPool() as zk:
        try:
            zk.ensure_path(zk_pause_autoscale_path)
            zk.set(zk_pause_autoscale_path, str(expiry_time).encode('utf-8'))
        except Exception:
            log.error('Could not set pause node in Zookeeper')
            raise

    log.info('Service autoscaler paused in {c}, for {m} minutes'.format(c=cluster, m=str(args.timeout)))
    paasta_print('Service autoscaler paused in {c}, for {m} minutes'.format(c=cluster, m=str(args.timeout)))


if __name__ == '__main__':
    main()
