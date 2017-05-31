#!/usr/bin/env python

# TODO read in detector name from cmdline
# TODO return status
# TODO log progress on stderr
# TODO return json for extra context that might be machine consumable


import argparse
import collections
import datetime
import json
import logging
import os
import sys

import requests
from requests_futures.sessions import FuturesSession


class SfxError(Exception):
    pass


def sfx_token():
    return os.environ['SFX_TOKEN']

def format_timestamp(ts_millis):
    return datetime.datetime.fromtimestamp(
        ts_millis/1000.
    ).isoformat() + 'Z'

def make_request(url, session=None):
    logging.info('GET %s', url)
    headers = {
        'Content-Type': 'application/json', 
        'X-SF-TOKEN': sfx_token(),
        }
    if session:
        r = session.get(url, headers=headers)
    else:
        r = requests.get(url, headers=headers)
        if not r.ok:
            logging.warn('SFX ingest response reason: %s', r.reason)
            logging.warn('SFX ingest response text: %s', r.text)
            raise SfxError(
                    'couldn''t find any detectors: maybe you they were not ' +
                    'created using the API? GUI created detectors are not ' +
                    'supported by the API')
    return r


def discover_detector_ids(detector_name):
    url = 'https://api.signalfx.com/v2/detector?limit=1000'
    if detector_name != 'all':
        url += '?name={}'.format(detector_name)
    r = make_request(url)
    return frozenset((result['id'] for result in r.json()['results']))


def get_detector_incidents(detector_ids):
    session = FuturesSession()
    futures = []
    for id in detector_ids:
#        import pdb; pdb.set_trace()
        url = 'https://api.signalfx.com/v2/detector/{}/incidents'.format(id)
        futures.append((url, make_request(url, session=session)))
    incident_data = []
    import pdb; pdb.set_trace()
    for url, future in futures:
        remaining_attempts=3
        while remaining_attempts:
            try:
                result = future.result()
                for i in result.json():
                    events = i['events']
                    detector_name = events[0]['detectorName']
                    severity = i['severity']    
                    since_timestamp = format_timestamp(sorted(e['timestamp'] for e in events)[0])
                    incident_data.append({
                        'detectorId': i['detectorId'],
                        'detector_name': detector_name,
                        'detectLabel': i['detectLabel'],
                        'since_timestamp': since_timestamp,
                        'severity': severity
                    })
                break
            except requests.exceptions.ConnectionError as ce:
                remaining_attempts -= 1
                logging.warn('Got exception while fetching url %s, %s', url, ce)
    return incident_data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--detector_name', help='example: "Kafka CPU Idle"')
    args = parser.parse_args()
    logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)-8s %(message)s')
    detector_ids = discover_detector_ids(args.detector_name)
    incidents = get_detector_incidents(detector_ids)
    print(json.dumps(incidents, indent=4))
    sys.exit(len(incidents))


if __name__ == '__main__':
    main()
