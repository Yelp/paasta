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
import sys

from a_sync import block

from paasta_tools.mesos.exceptions import MasterNotAvailableException
from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.metrics.metastatus_lib import assert_frameworks_exist
from paasta_tools.utils import paasta_print


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--expected', '-e', dest='expected', type=str, default='',
        help='Comma separated list of frameworks to expect.\n'
        'Will fail if any of these are not found',
    )
    return parser.parse_args()


def check_mesos_active_frameworks() -> None:
    options = parse_args()
    expected = options.expected.split(',')
    master = get_mesos_master()
    try:
        state = block(master.state)
    except MasterNotAvailableException as e:
        paasta_print("CRITICAL: %s" % e.args[0])
        sys.exit(2)

    result = assert_frameworks_exist(state, expected)
    if result.healthy:
        paasta_print("OK: " + result.message)
        sys.exit(0)
    else:
        paasta_print(result.message)
        sys.exit(2)


if __name__ == '__main__':
    check_mesos_active_frameworks()
