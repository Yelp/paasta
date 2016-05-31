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

from paasta_tools.autoscaling_lib import autoscale_local_cluster


log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='Autoscales the local PaaSTA cluster')
    parser.add_argument('-v', '--verbose', action='count', dest="verbose", default=0,
                        help="Print out more output.")
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    if args.verbose >= 2:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose == 1:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    autoscale_local_cluster()


if __name__ == '__main__':
    main()
