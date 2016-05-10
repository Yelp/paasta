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
import json
import os
import sys

import requests

from paasta_tools.utils import get_username
from paasta_tools.utils import PATH_TO_SYSTEM_PAASTA_CONFIG_DIR
from paasta_tools.utils import timeout


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'performance-check',
        description='Performs a performance check (not implemented)',
        help='Performs a performance check (not implemented)',
    )
    list_parser.add_argument(
        '-s', '--service',
        help='Name of service for which you wish to check. Leading "services-", as included in a '
             'Jenkins job name, will be stripped.',
    )
    list_parser.add_argument(
        '-c', '--commit',
        help='Git sha of the image to check',
    )
    list_parser.add_argument(
        '-i', '--image',
        help='Optional docker image to performance check. Must be available on a registry for '
             'use, like http://docker-dev.yelpcorp.com/example_service-kwa-test1',
    )
    list_parser.set_defaults(command=perform_performance_check)


def load_performance_check_config():
    config_file = os.path.join(PATH_TO_SYSTEM_PAASTA_CONFIG_DIR, 'performance-check.json')
    try:
        with open(config_file) as f:
            return json.load(f)
    except IOError as e:
        print "No performance check config to use. Safely bailing."
        print e.strerror
        sys.exit(0)


def submit_performance_check_job(service, commit, image):
    performance_check_config = load_performance_check_config()
    payload = {
        'service': service,
        'commit': commit,
        'submitter': get_username(),
        'image': image,
    }
    r = requests.post(
        url=performance_check_config['endpoint'],
        data=payload,
    )
    print "Posted a submission to the PaaSTA performance-check service:"
    print r.text


@timeout()
def perform_performance_check(args):
    try:
        submit_performance_check_job(service=args.service, commit=args.commit, image=args.image)
    except Exception as e:
        print "Something went wrong with the performance check. Safely bailing. No need to panic."
        print "Here was the error:"
        print str(e)
