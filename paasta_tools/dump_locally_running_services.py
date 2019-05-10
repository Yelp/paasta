#!/usr/bin/env python
# Copyright 2015-2019 Yelp Inc.
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
Usage: ./paasta_dump_locally_running_services.py

Outputs a JSON-encoded list of services that are running on this host along
with the host port that each service is listening on.
"""
import json
import sys

from paasta_tools.marathon_tools import get_marathon_services_running_here_for_nerve
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import paasta_print


def main():
    local_services = get_marathon_services_running_here_for_nerve(
        cluster=None,
        soa_dir=DEFAULT_SOA_DIR,
    )
    paasta_print(json.dumps(local_services))
    sys.exit(0)


if __name__ == '__main__':
    main()
