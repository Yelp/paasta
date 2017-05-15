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

from paasta_tools.utils import _run
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'security-check',
        description='Performs a security check (alpha)',
        help='Performs a security check (alpha)',
    )
    list_parser.add_argument(
        '-s', '--service',
        help='Name of service for which you wish to check. Leading "services-", as included in a '
             'Jenkins job name, will be stripped.',
        required=False,
    )
    list_parser.add_argument(
        '-c', '--commit',
        help='Git sha of the image to check',
        required=False,
    )
    list_parser.set_defaults(command=perform_security_check)


def perform_security_check(args):
    security_check_command = load_system_paasta_config().get_security_check_command()
    if not security_check_command:
        paasta_print("Nothing to be executed during the security-check step")
        return 0

    ret_code, output = _run(security_check_command, timeout=300, stream=True)
    paasta_print("{} exited with code {}".format(security_check_command, ret_code))

    return ret_code
