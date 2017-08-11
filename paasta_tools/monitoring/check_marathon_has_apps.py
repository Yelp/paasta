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
import sys

from marathon.exceptions import InternalServerError
from marathon.exceptions import MarathonError

from paasta_tools.marathon_tools import load_marathon_config
from paasta_tools.metrics.metastatus_lib import assert_marathon_apps
from paasta_tools.metrics.metastatus_lib import get_marathon_client
from paasta_tools.utils import paasta_print


def check_marathon_apps():
    config = load_marathon_config()
    if not config:
        paasta_print("UNKNOWN: Failed to load marathon config")
        sys.exit(3)
    client = get_marathon_client(config)

    try:
        result = assert_marathon_apps(client)
    except (MarathonError, InternalServerError, ValueError) as e:
        paasta_print("CRITICAL: Unable to connect to Marathon cluster: %s" % e.message)
        sys.exit(2)

    if result.healthy:
        paasta_print("OK: " + result.message)
        sys.exit(0)
    else:
        paasta_print(result.message)
        sys.exit(2)


if __name__ == '__main__':
    check_marathon_apps()
