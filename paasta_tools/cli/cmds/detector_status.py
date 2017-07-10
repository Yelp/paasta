#!/usr/bin/env python
#
# Copyright 2017 Yelp Inc.
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
import collections
import datetime
import json
import logging
import os
import sys
import urllib

import grequests

from paasta_tools.utils import load_system_paasta_config

MAX_ROWS_IN_SFX_RESULTS = 1000
MAX_CONCURRENT_QUERIES = 10

def sfx_token():
    try:
        return os.environ['SFX_TOKEN']
    except KeyError:
        logging.info(
            'SFX_TOKEN env variable not found:' +
            ' looking in paasta system config')
    system_paasta_config = load_system_paasta_config()
    return system_paasta_config.get_monitoring_config()['signalfx_api_key']

def format_timestamp(ts_millis):
    return datetime.datetime.fromtimestamp(
        ts_millis/1000.
    ).isoformat() + 'Z'

def make_requests(urls):
    for url in urls:
        logging.info('GET %s', url)
    headers = {
        'Content-Type': 'application/json',
        'X-SF-TOKEN': sfx_token(),
        }
    def exception_handler(request, exception):
        logging.warn('request failed: %s -> %s', request.url, exception)
    unsent = (grequests.get(url, headers=headers) for url in urls)
    responses = grequests.imap(
            unsent, exception_handler=exception_handler,
            size=MAX_CONCURRENT_QUERIES,
            )
    return responses

def discover_detector_ids(detector_name):
    url = 'https://api.signalfx.com/v2/detector/?limit={}'.format(
            MAX_ROWS_IN_SFX_RESULTS)
    if detector_name != 'all':
        url += '&{}'.format(urllib.urlencode({'name': detector_name}))
    r = list(make_requests([url]))[0]
    return frozenset((result['id'] for result in r.json()['results']))

def get_detector_incidents(detector_ids):
    urls = ['https://api.signalfx.com/v2/detector/{}/incidents'.format(id)
            for id in detector_ids]
    responses = make_requests(urls)
    incident_data = []
    for response in responses:
        for i in response.json():
            events = i['events']
            detector_name = events[0]['detectorName']
            severity = i['severity']
            since_timestamp = format_timestamp(
                    sorted(e['timestamp'] for e in events)[0])
            incident_data.append({
                'detectorId': i['detectorId'],
                'detector_name': detector_name,
                'detectLabel': i['detectLabel'],
                'since_timestamp': since_timestamp,
                'severity': severity
            })
    return incident_data

def add_subparser(subparsers):
    parser = subparsers.add_parser(
        'detector-status',
        description=( 'Queries signalfx detector status, ' +
                      'returning the number of live incidents.' ),
        help='Queries signalfx detector status.',
    )
    parser.add_argument(
            '--detector_name',
            help='example: "Kafka CPU Idle", or "all"')
    parser.set_defaults(command=detector_status)

def detector_status(args):
    detector_ids = discover_detector_ids(args.detector_name)
    incidents = get_detector_incidents(detector_ids)
    print(json.dumps(incidents, indent=4))
    sys.exit(len(incidents))
