#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
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
import datetime
import json
from socket import getfqdn
from socket import gethostbyname

from dateutil import tz
from pytimeparse import timeparse
from requests import Request
from requests import Session
from requests.exceptions import HTTPError

from paasta_tools.mesos_tools import get_mesos_leader


def parse_args():
    """Parses the command line arguments passed to this script"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d', '--duration',
        type=parse_timedelta,
        default='1h',
        help="Duration of the maintenance window. Any pytimeparse unit is supported.",
    )
    parser.add_argument(
        '-s', '--start',
        default=now(),
        help="Time to start the maintenance window. Defaults to now.",
    )
    parser.add_argument(
        'action',
        choices=['drain', 'undrain', 'down', 'up', 'status'],
        help="Action to perform on the speicifed hosts",
    )
    parser.add_argument(
        'hostname',
        nargs='*',
        default=[getfqdn()],
        help='Hostname(s) of machine(s) to start draining. '
        'You can specify <hostname>|<ip> to avoid querying DNS to determine the corresponding IP.',
    )
    return parser.parse_args()


def base_api():
    """Helper function for making all API requests

    :returns: a function that can be callecd to make a request
    """
    leader = get_mesos_leader()

    def execute_request(method, endpoint, **kwargs):
        url = "%s%s" % (leader, endpoint)
        timeout = 15
        s = Session()
        s.auth = load_credentials()
        req = Request(method, url, **kwargs)
        prepared = s.prepare_request(req)
        try:
            resp = s.send(
                prepared,
                timeout=timeout,
            )
            resp.raise_for_status()
            return resp
        except HTTPError as e:
            e.msg = "Error executing API request calling %s. Got error: %s" % (url, e.msg)
            raise
    return execute_request


def master_api():
    """Helper function for making API requests to the /master API endpoints

    :returns: a function that can be called to make a request to /master
    """
    def execute_master_api_request(method, endpoint, **kwargs):
        base_api_client = base_api()
        return base_api_client(method, "/master%s" % endpoint, **kwargs)
    return execute_master_api_request


def maintenance_api():
    """Helper function for making API requests to the /master/maintenance API endpoints

    :returns: a function that can be called to make a request to /master/maintenance
    """
    def execute_schedule_api_request(method, endpoint, **kwargs):
        master_api_client = master_api()
        return master_api_client(method, "/maintenance%s" % endpoint, **kwargs)
    return execute_schedule_api_request


def get_schedule_client():
    """Helper function for making API requests to the /master/maintenance/schedule API endpoints

    :returns: a function that can be called to make a request to /master/maintenance/schedule
    """
    def execute_schedule_api_request(method, endpoint, **kwargs):
        maintenance_api_client = maintenance_api()
        return maintenance_api_client(method, "/schedule%s" % endpoint, **kwargs)
    return execute_schedule_api_request


def get_maintenance_schedule():
    """Makes a GET request to the /master/maintenance API endpoint

    :returns: a function that can be called to make a request to /master/maintenance
    """
    client_fn = get_schedule_client()
    return client_fn(method="GET", endpoint="")


def get_maintenance_status():
    """Makes a GET request to the /master/maintenance/schedule/status API endpoint

    :returns: a requests.response object representing the current maintenance status
    """
    client_fn = get_schedule_client()
    return client_fn(method="GET", endpoint="/status")


def get_hosts_with_state(state):
    """Helper function to check the maintenance status and return all hosts
    listed as being in a current state

    :param state: State we are interested in ('down_machines' or 'draining_machines')
    :returns: A list of hostnames in the specified state or an empty list if no machines
    """
    status = get_maintenance_status()
    if not status or state not in status:
        return []
    return [machine['id']['hostname'] for machine in status[state]]


def get_draining_hosts():
    """Returns a list of hostnames that are marked as draining

    :returns: a list of strings representing hostnames
    """
    return get_hosts_with_state(state='draining_machines')


def get_down_hosts():
    """Returns a list of hostnames that are marked as down

    :returns: a list of strings representing hostnames
    """
    return get_hosts_with_state(state='down_machines')


def is_host_draining(hostname=getfqdn()):
    """Checks if the specified hostname is marked as draining

    :param hostname: Hostname we want to check if draining (defaults to current host)
    :returns: a boolean representing whether or not the specified hostname is draining
    """
    return hostname in get_draining_hosts()


def is_host_down(hostname=getfqdn()):
    """Checks if the specified hostname is marked as down

    :param hostname: Hostname we want to check if down (defaults to current host)
    :returns: a boolean representing whether or not the specified hostname is down
    """
    return hostname in get_down_hosts()


def parse_timedelta(value):
    """Return the delta in nanoseconds.
    :param value: a string containing a time format supported by :mod:`pytimeparse`
    :returns: an integer (or float) representing the specified delta in nanoseconds
    """
    error_msg = "'%s' is not a valid time expression" % value
    try:
        seconds = timeparse.timeparse(value)
    except TypeError:
        raise argparse.ArgumentTypeError(error_msg)
    if not seconds:
        raise argparse.ArgumentTypeError(error_msg)
    return seconds_to_nanoseconds(seconds)


def datetime_seconds_from_now(seconds):
    """Given a number of seconds, returns a datetime object representing that number of seconds in the future from the
    current time.
    :param seconds: an integer representing a certain number of seconds
    :returns: a datetime.timedelta representing now + the specified number of seconds
    """
    return now() + datetime.timedelta(seconds=seconds)


def now():
    """Returns a datetime object representing the current time in UTC

    :returns: a datetime.datetime object representing the current time
    """
    return datetime.datetime.now(tz.tzutc())


def seconds_to_nanoseconds(seconds):
    """Convert the specified number of seconds to nanoseconds
    :param seconds: an integer representing a certain number of seconds
    :returns: an integer (or float) representation of the specified number of seconds as nanoseconds
    """
    return seconds * 1000000000


def datetime_to_nanoseconds(dt):
    """Convert the provided datetime object into nanoseconds

    :returns: an integer (or float) representation of the specified datetime as nanoseconds
    """
    return seconds_to_nanoseconds(int(dt.strftime("%s")))


def build_start_maintenance_payload(hostnames):
    """Creates the JSON payload necessary to bring the specified hostnames up/down for maintenance.
    :param hostnames: a list of hostnames
    :returns: a dictionary representing the list of machines to bring up/down for maintenance
    """
    return get_machine_ids(hostnames)


def get_machine_ids(hostnames):
    """Helper function to convert a list of hostnames into a JSON list of hostname/ip pairs.
    :param hostnames: a list of hostnames
    :returns: a dictionary representing the list of machines to bring up/down for maintenance
    """
    machine_ids = []
    for hostname in hostnames:
        machine_id = dict()
        # This is to allow specifying a hostname as "hostname|ipaddress"
        # to avoid querying DNS for the IP.
        if '|' in hostname:
            (host, ip) = hostname.split('|')
            machine_id['hostname'] = host
            machine_id['ip'] = ip
        else:
            machine_id['hostname'] = hostname
            machine_id['ip'] = gethostbyname(hostname)
        machine_ids.append(machine_id)
    return machine_ids


def build_maintenance_schedule_payload(hostnames, start, duration, drain=True):
    """Creates the JSON payload needed to (un)schedule maintenance on the specified hostnames.
    :param hostnames: a list of hostnames
    :param start: the time to start the maintenance, represented as number of nanoseconds since the epoch
    :param duration: length of the maintenance window, represented as number of nanoseconds since the epoch
    :param drain: boolean to note whether we are draining (True) the specified hosts or undraining (False) them
    :returns: a dictionary that can be sent to Mesos to (un)schedule maintenance
    """
    schedule = get_maintenance_schedule().json()
    machine_ids = get_machine_ids(hostnames)

    unavailability = dict()
    unavailability['start'] = dict()
    unavailability['start']['nanoseconds'] = int(start)
    unavailability['duration'] = dict()
    unavailability['duration']['nanoseconds'] = int(duration)

    window = dict()
    window['machine_ids'] = machine_ids
    window['unavailability'] = unavailability

    if schedule:
        for existing_window in schedule['windows']:
            for existing_machine_id in existing_window['machine_ids']:
                if existing_machine_id in machine_ids:
                    existing_window['machine_ids'].remove(existing_machine_id)
                    if not existing_window['machine_ids']:
                        schedule['windows'].remove(existing_window)
        if drain:
            windows = schedule['windows'] + [window]
        else:
            windows = schedule['windows']
    elif drain:
        windows = [window]
    else:
        windows = []

    payload = dict()
    payload['windows'] = windows

    return payload


def load_credentials(mesos_secrets='/nail/etc/mesos-slave-secret'):
    """Loads the mesos-slave credentials from the specified file. These credentials will be used for all
    maintenance API requests.
    :param mesos_secrets: optional argument specifying the path to the file containing the mesos-slave credentials
    :returns: a tuple of the form (username, password)
    """
    try:
        with open(mesos_secrets) as data_file:
            data = json.load(data_file)
    except EnvironmentError:
        print "paasta_maintenance must be run on a Mesos slave containing valid credentials (%s)" % mesos_secrets
        raise
    try:
        username = data['principal']
        password = data['secret']
    except KeyError:
        print "%s does not contain Mesos slave credentials in the expected format." % mesos_secrets
        print "See http://mesos.apache.org/documentation/latest/authentication/ for details"
        raise
    return username, password


def drain(hostnames, start, duration):
    """Schedules a maintenance window for the specified hosts and marks them as draining.
    :param hostnames: a list of hostnames
    :param start: the time to start the maintenance, represented as number of nanoseconds since the epoch
    :param duration: length of the maintenance window, represented as number of nanoseconds since the epoch
    :returns: None
    """
    payload = build_maintenance_schedule_payload(hostnames, start, duration, drain=True)
    client_fn = get_schedule_client()
    print client_fn(method="POST", endpoint="", data=json.dumps(payload)).text


def undrain(hostnames, start, duration):
    """Unschedules the maintenance window for the specified hosts and unmarks them as draining. They are ready for
    regular use.
    :param hostnames: a list of hostnames
    :param start: the time to start the maintenance, represented as number of nanoseconds since the epoch
    :param duration: length of the maintenance window, represented as number of nanoseconds since the epoch
    :returns: None
    """
    payload = build_maintenance_schedule_payload(hostnames, start, duration, drain=False)
    client_fn = get_schedule_client()
    print client_fn(method="POST", endpoint="", data=json.dumps(payload)).text


def down(hostnames):
    """Marks the specified hostnames as being down for maintenance, and makes them unavailable for use.
    :param hostnames: a list of hostnames
    :returns: None
    """
    payload = build_start_maintenance_payload(hostnames)
    client_fn = master_api()
    print client_fn(method="POST", endpoint="/machine/down", data=json.dumps(payload)).text


def up(hostnames):
    """Marks the specified hostnames as no longer being down for maintenance, and makes them available for use.
    :param hostnames: a list of hostnames
    :returns: None
    """
    payload = build_start_maintenance_payload(hostnames)
    client_fn = master_api()
    print client_fn(method="POST", endpoint="/machine/up", data=json.dumps(payload)).text


def status():
    """Get the Mesos maintenance status. This contains hostname/ip mappings for hosts that are either marked as being
    down for maintenance or draining.
    :returns: None
    """
    status = get_maintenance_status()
    print "%s:%s" % (status, status.text)


def schedule():
    """Get the Mesos maintenance schedule. This contains hostname/ip mappings and their maintenance window.
    :returns: None
    """
    schedule = get_maintenance_schedule()
    print "%s:%s" % (schedule, schedule.text)


def paasta_maintenance():
    """Manipulate the maintenance state of a PaaSTA host.
    :returns: None
    """
    args = parse_args()

    action = args.action
    hostnames = args.hostname

    if action not in ['drain', 'undrain', 'down', 'up', 'status']:
        print "action must be 'drain', 'undrain', 'down', 'up', or 'status'"
        return

    if action != 'status' and not hostnames:
        print "You must specify one or more hostnames"
        return

    start = args.start.strftime("%s")
    duration = args.duration

    if action == 'drain':
        drain(hostnames, start, duration)
    elif action == 'undrain':
        undrain(hostnames, start, duration)
    elif action == 'down':
        down(hostnames)
    elif action == 'up':
        up(hostnames)
    elif action == 'status':
        status()
        schedule()


if __name__ == '__main__':
    paasta_maintenance()
