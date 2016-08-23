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


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            'paasta_readiness_check is a tool to verfiy if changes are safe to '
            'be made to a host.\n\n'
            'It does this by inspecting the running tasks on localhost, and returns '
            'non-0 if any of thoses tasks are "at-risk"\n\n'
            'A task is at-risk if it is low in replication as determined by the replication '
            'count compared to the desired count.'
        )
    )
    parser.add_argument('-v', '--verbose', action='count', dest="verbose", default=0,
                        help="Print out more output regarding the state of the cluster")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)
    print "Everything is fine (not implemented yet"


if __name__ == '__main__':
    main()
