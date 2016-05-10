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


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'security-check',
        description='Performs a security check (not implemented)',
        help='Performs a security check (not implemented)',
    )
    list_parser.add_argument(
        '-s', '--service',
        help='Name of service for which you wish to check. Leading "services-", as included in a '
             'Jenkins job name, will be stripped.',
        required=True,
    )
    list_parser.add_argument(
        '-c', '--commit',
        help='Git sha of the image to check',
        required=True,
    )
    list_parser.set_defaults(command=perform_security_check)


def perform_security_check(args):
    print 'Not implemented yet'
    return 0
