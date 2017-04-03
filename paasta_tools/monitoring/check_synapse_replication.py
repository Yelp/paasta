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
import sys

from paasta_tools import utils
from paasta_tools.smartstack_tools import get_replication_for_services
from paasta_tools.utils import paasta_print


def check_replication(service, service_replication,
                      warn_range, crit_range):
    """Check for sufficient replication of a service

    :param service: A string representing the name of the service
                         this replication check is relevant to.
    :param service_replication: An int representing the number of available
                                service instances
    :param warn_range: A two tuple of integers representing the minimum and
                       maximum allowed replication before entering the WARNING
                       state.
    :param crit_range: A two tuple of integers representing the minimum and
                       maximum allowed replication before entering the CRITICAL
                       state.

    Note that all ranges are closed interval. If the replication is outside the
    closed interval for the relevant level (e.g. warning, critical), then
    the error code will change appropriately.

    :returns check_result: A tuple of error code and a human readable error
        message. The error codes conform to the nagios plugin api.

        e.g. for an OK service
        (0,
        "OK lucy has 1 instance(s)")

        e.g. for a CRITICAL service
        (2,
        "CRITICAL lucy has 0 instance(s), expected value in [1, 1e18])
    """
    code, status, interval = 0, 'OK', None
    if not (crit_range[0] <= service_replication <= crit_range[1]):
        code, status, interval = 2, 'CRITICAL', crit_range
    elif not (warn_range[0] <= service_replication <= warn_range[1]):
        code, status, interval = 1, 'WARNING', warn_range

    expected_message = ""
    if interval is not None:
        expected_message = ", expected value in {}".format(interval)

    message = "{} {} has {} instance(s){}".format(
        status, service, service_replication, expected_message
    )

    return code, message


def parse_range(str_range):
    int_range = str_range.split(":")
    if len(int_range) != 2:
        fail('Incorrect range, see --help', 2)
    if int_range[0] == '':
        int_range[0] = 0
    if int_range[1] == '':
        int_range[1] = sys.maxsize
    try:
        return tuple(map(int, int_range))
    except Exception:
        fail("Failed to parse range {}".format(str_range))


def parse_synapse_check_options(system_paasta_config):
    epilog = "RANGEs are specified 'min:max' or 'min:' or ':max'"
    parser = argparse.ArgumentParser(epilog=epilog)

    parser.add_argument(dest='services', nargs='+', type=str,
                        help="A series of service names to check.\n"
                        "e.g. lucy_east_0 lucy_east_1 ...")
    parser.add_argument('-H', '--synapse-host',
                        dest='synapse_host', type=str,
                        help='The host to check',
                        default=system_paasta_config.get_default_synapse_host())
    parser.add_argument('-P', '--synapse-port',
                        dest='synapse_port', type=int,
                        help='The synapse port to check',
                        default=system_paasta_config.get_synapse_port())
    parser.add_argument('-F', '--synapse-haproxy-url-format',
                        dest='synapse_haproxy_url_format', type=str,
                        help='The synapse haproxy url format',
                        default=system_paasta_config.get_synapse_haproxy_url_format())
    parser.add_argument('-w', '--warn', dest='warn', type=str,
                        metavar='RANGE',
                        help="Generate warning state if number of "
                        "service instances is outside this range")
    parser.add_argument('-c', '--critcal', dest='crit', type=str,
                        metavar='RANGE',
                        help="Generate critical state if number of "
                        "service instances is outside this range")
    options = parser.parse_args()

    options.crit = parse_range(options.crit)
    options.warn = parse_range(options.warn)

    return options


def fail(message, code):
    paasta_print(message)
    sys.exit(code)


def run_synapse_check():
    system_paasta_config = utils.load_system_paasta_config()
    options = parse_synapse_check_options(system_paasta_config)
    try:
        service_replications = get_replication_for_services(
            options.synapse_host,
            options.synapse_port,
            options.synapse_haproxy_url_format,
            options.services
        )

        all_codes = []
        for name, replication in service_replications.items():
            code, message = check_replication(name, replication,
                                              options.warn, options.crit)
            all_codes.append(code)
            paasta_print(message)
        sys.exit(max(all_codes))
    except Exception as e:
        fail('UNKNOWN: {}'.format(e), 3)


if __name__ == "__main__":
    run_synapse_check()
