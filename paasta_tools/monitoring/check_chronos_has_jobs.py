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

from chronos import ChronosAPIError

from paasta_tools.chronos_tools import get_chronos_client
from paasta_tools.chronos_tools import load_chronos_config
from paasta_tools.metrics.metastatus_lib import assert_chronos_scheduled_jobs
from paasta_tools.utils import paasta_print


def check_chronos_jobs():
    config = load_chronos_config()
    if not config:
        paasta_print("UNKNOWN: Failed to load chronos config")
        sys.exit(3)
    client = get_chronos_client(config)

    try:
        result = assert_chronos_scheduled_jobs(client)
    except (ChronosAPIError) as e:
        paasta_print("CRITICAL: Unable to connect to Chronos: %s" % e.message)
        sys.exit(2)

    if result.healthy:
        paasta_print("OK: " + result.message)
        sys.exit(0)
    else:
        paasta_print("CRITICAL: " + result.message)
        sys.exit(2)


if __name__ == '__main__':
    check_chronos_jobs()
