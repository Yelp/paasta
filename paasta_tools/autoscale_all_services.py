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

from paasta_tools.autoscaling_lib import autoscale_services
from paasta_tools.marathon_tools import DEFAULT_SOA_DIR


def parse_args():
    parser = argparse.ArgumentParser(description='Autoscales marathon jobs')
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    soa_dir = args.soa_dir
    autoscale_services(soa_dir)


if __name__ == '__main__':
    main()
