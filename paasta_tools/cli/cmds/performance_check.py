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
import requests
from service_configuration_lib import read_extra_service_information

from paasta_tools.cli.utils import validate_service_name
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import timeout


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'performance-check',
        description='Performs a performance check',
        help='Performs a performance check',
    )
    list_parser.add_argument(
        '-s', '--service',
        help='Name of service for which you wish to check. Leading "services-", as included in a '
             'Jenkins job name, will be stripped.',
    )
    list_parser.add_argument(
        '-k', '--commit',
        help='Git commit sha.',
    )
    list_parser.add_argument(
        '-d', '--soa-dir',
        dest='soa_dir',
        metavar='SOA_DIR',
        default=DEFAULT_SOA_DIR,
        help='Define a different soa config directory',
    )
    list_parser.set_defaults(command=perform_performance_check)


def load_performance_check_config(service, soa_dir):
    return read_extra_service_information(
        service_name=service,
        extra_info='performance-check',
        soa_dir=soa_dir,
    )


def submit_performance_check_job(service, soa_dir):
    performance_check_config = load_performance_check_config(service, soa_dir)

    if not performance_check_config:
        print "No performance-check.yaml. Skipping performance-check."
        return

    endpoint = performance_check_config.pop('endpoint')
    r = requests.post(
        url=endpoint,
        params=performance_check_config,
    )
    r.raise_for_status()
    print "Posted a submission to the PaaSTA performance-check service."
    print "Endpoint: {}".format(endpoint)
    print "Parameters: {}".format(performance_check_config)


@timeout()
def perform_performance_check(args):
    service = args.service
    if service.startswith('services-'):
        service = service.split('services-', 1)[1]
    validate_service_name(service, args.soa_dir)

    try:
        submit_performance_check_job(
            service=service,
            soa_dir=args.soa_dir,
        )
    except Exception as e:
        print "Something went wrong with the performance check. Safely bailing. No need to panic."
        print "Here was the error:"
        print str(e)
