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
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import logging

from paasta_tools.autoscaling.autoscaling_cluster_lib import autoscale_local_cluster


log = logging.getLogger(__name__)


def parse_args(argv):
    parser = argparse.ArgumentParser(description='Autoscales the local PaaSTA cluster')
    parser.add_argument('-v', '--verbose', action='count', dest="verbose", default=0,
                        help="Print out more output.")
    parser.add_argument('-d', '--dry-run', action='store_true',
                        help="Perform no actions, only print what to do")
    parser.add_argument('-a', '--autoscaler-configs',
                        help="Path to autoscaler config files",
                        default='/etc/paasta/cluster_autoscaling')
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    log_format = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
    log_level = None
    if args.verbose >= 3:
        logging.basicConfig(level=logging.DEBUG, format=log_format)
    elif args.verbose == 2:
        logging.basicConfig(level=logging.INFO, format=log_format)
        log_level = logging.DEBUG
    elif args.verbose == 1:
        logging.basicConfig(level=logging.INFO, format=log_format)
    else:
        logging.basicConfig(level=logging.WARNING, format=log_format)

    autoscale_local_cluster(
        dry_run=args.dry_run,
        config_folder=args.autoscaler_configs,
        log_level=log_level,
    )


if __name__ == '__main__':
    main()
