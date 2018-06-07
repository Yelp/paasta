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
import pysensu_yelp

from paasta_tools.chronos_tools import DEFAULT_SOA_DIR
from paasta_tools.monitoring_tools import send_event
from paasta_tools.utils import _run
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'security-check',
        description='Performs a security check consisting of a few tests.',
        help='Performs a security check',
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
    """It runs a few security tests, checks the return code and prints output that should help in fixing failures.
    If you are at Yelp, please visit https://confluence.yelpcorp.com/display/PAASTA/PaaSTA+security-check+explained
    to learn more.
    :param args: service - the name of the service; commit - upstream git commit.
    :return: 0 if the security-check passed, non-zero if it failed.
    """
    security_check_command = load_system_paasta_config().get_security_check_command()
    if not security_check_command:
        paasta_print("Nothing to be executed during the security-check step")
        return 0

    command = f"{security_check_command} {args.service} {args.commit}"

    ret_code, output = _run(command, timeout=300, stream=True)
    if ret_code != 0:
        paasta_print(
            "The security-check failed. Please visit y/security-check-runbook to learn how to fix it ("
            "including whitelisting safe versions of packages and docker images).",
        )

    sensu_status = pysensu_yelp.Status.CRITICAL if ret_code != 0 else pysensu_yelp.Status.OK
    send_event(
        service=args.service, check_name='%s.security_check' % args.service,
        overrides={'page': False, 'ticket': True}, status=sensu_status, output=output, soa_dir=DEFAULT_SOA_DIR,
    )

    return ret_code
