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
Usage: ./paasta_dump_locally_running_services.py [options]

Outputs a JSON-encoded list of services that are running on this host along
with the host port that each service is listening on.

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
"""
import argparse
import json
import sys

from paasta_tools.kubernetes_tools import get_kubernetes_services_running_here_for_nerve
from paasta_tools.marathon_tools import get_marathon_services_running_here_for_nerve
from paasta_tools.marathon_tools import get_puppet_services_running_here_for_nerve
from paasta_tools.utils import DEFAULT_SOA_DIR


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Dumps information about locally running services."
    )
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    soa_dir = args.soa_dir

    service_dump = (
        get_marathon_services_running_here_for_nerve(cluster=None, soa_dir=soa_dir)
        + get_puppet_services_running_here_for_nerve(soa_dir=soa_dir)
        + get_kubernetes_services_running_here_for_nerve(cluster=None, soa_dir=soa_dir)
    )

    print(json.dumps(service_dump))
    sys.exit(0)


if __name__ == "__main__":
    main()
