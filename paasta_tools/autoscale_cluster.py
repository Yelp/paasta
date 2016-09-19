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

from paasta_tools.autoscaling.autoscaling_cluster_lib import autoscale_local_cluster


log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='Autoscales the local PaaSTA cluster')
    parser.add_argument('-v', '--verbose', action='count', dest="verbose", default=0,
                        help="Print out more output.")
    parser.add_argument('-d', '--dry-run', action='store_true',
                        help="Perform no actions, only print what to do")
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    log_format = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
    if args.verbose >= 2:
        logging.basicConfig(level=logging.DEBUG, format=log_format)
    elif args.verbose == 1:
        logging.basicConfig(level=logging.INFO, format=log_format)
    else:
        logging.basicConfig(level=logging.WARNING, format=log_format)

    autoscale_local_cluster(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
