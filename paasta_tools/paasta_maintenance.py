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
import logging
import sys
from socket import getfqdn
from socket import gethostbyname

from dateutil import parser
from pytimeparse import timeparse
from requests import Request
from requests import Session
from requests.exceptions import HTTPError

from paasta_tools.mesos_tools import get_mesos_leader
from paasta_tools.mesos_tools import get_mesos_state_from_leader
from paasta_tools.mesos_tools import get_mesos_task_count_by_slave
from paasta_tools.mesos_tools import MESOS_MASTER_PORT

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


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
        type=parse_datetime,
        default=str(now()),
        help="Time to start the maintenance window. Defaults to now.",
    )
    parser.add_argument(
        'action',
        choices=[
            'down',
            'drain',
            'is_host_down',
            'is_host_drained',
            'is_host_draining',
            'is_hosts_past_maintenance_end',
            'is_hosts_past_maintenance_start',
            'is_safe_to_kill',
            'schedule',
            'status',
            'undrain',
            'up',
        ],
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
        url = "http://%s:%d%s" % (leader, MESOS_MASTER_PORT, endpoint)
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
    """Makes a GET request to the /master/maintenance/schedule API endpoint

    :returns: a function that can be called to make a request to /master/maintenance/schedule
    """
    client_fn = get_schedule_client()
    return client_fn(method="GET", endpoint="")


def get_maintenance_status():
    """Makes a GET request to the /master/maintenance/schedule/status API endpoint

    :returns: a requests.response object representing the current maintenance status
    """
    client_fn = maintenance_api()
    return client_fn(method="GET", endpoint="/status")


def schedule():
    """Get the Mesos maintenance schedule. This contains hostname/ip mappings and their maintenance window.
    :returns: None
    """
    try:
        schedule = get_maintenance_schedule()
    except HTTPError as e:
        e.msg = "Error getting maintenance schedule. Got error: %s" % e.msg
        raise
    print "%s" % schedule.text


def get_hosts_with_state(state):
    """Helper function to check the maintenance status and return all hosts
    listed as being in a current state

    :param state: State we are interested in ('down_machines' or 'draining_machines')
    :returns: A list of hostnames in the specified state or an empty list if no machines
    """
    try:
        status = get_maintenance_status().json()
    except HTTPError as e:
        e.msg = "Error getting maintenance status. Got error: %s" % e.msg
        raise
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


def parse_datetime(value):
    """Return the datetime in nanoseconds.
    :param value: a string containing a datetime supported by :mod:`dateutil.parser`
    :returns: an integer (or float) representing the specified datetime in nanoseconds
    """
    error_msg = "'%s' is not a valid datetime expression" % value
    try:
        dt = parser.parse(value)
    except:
        raise argparse.ArgumentTypeError(error_msg)
    if not dt:
        raise argparse.ArgumentTypeError(error_msg)
    return datetime_to_nanoseconds(dt)


def datetime_seconds_from_now(seconds):
    """Given a number of seconds, returns a datetime object representing that number of seconds in the future from the
    current time.
    :param seconds: an integer representing a certain number of seconds
    :returns: a datetime.timedelta representing now + the specified number of seconds
    """
    return now() + datetime.timedelta(seconds=seconds)


def now():
    """Returns a datetime object representing the current time

    :returns: a datetime.datetime object representing the current time
    """
    return datetime.datetime.now()


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


def build_maintenance_schedule_payload(hostnames, start=None, duration=None, drain=True):
    """Creates the JSON payload needed to (un)schedule maintenance on the specified hostnames.
    :param hostnames: a list of hostnames
    :param start: the time to start the maintenance, represented as number of nanoseconds since the epoch
    :param duration: length of the maintenance window, represented as number of nanoseconds since the epoch
    :param drain: boolean to note whether we are draining (True) the specified hosts or undraining (False) them
    :returns: a dictionary that can be sent to Mesos to (un)schedule maintenance
    """
    schedule = get_maintenance_schedule().json()
    machine_ids = get_machine_ids(hostnames)

    if drain:
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
                # If we already have a maintenance window scheduled for one of the hosts,
                # replace it with the new window.
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
    log.info("Draining: %s" % hostnames)
    payload = build_maintenance_schedule_payload(hostnames, start, duration, drain=True)
    client_fn = get_schedule_client()
    try:
        drain_output = client_fn(method="POST", endpoint="", data=json.dumps(payload)).text
    except HTTPError as e:
        e.msg = "Error performing maintenance drain. Got error: %s" % e.msg
        raise
    print drain_output
    return 0


def undrain(hostnames):
    """Unschedules the maintenance window for the specified hosts and unmarks them as draining. They are ready for
    regular use.
    :param hostnames: a list of hostnames
    :returns: None
    """
    log.info("Undraining: %s" % hostnames)
    payload = build_maintenance_schedule_payload(hostnames, drain=False)
    client_fn = get_schedule_client()
    try:
        undrain_output = client_fn(method="POST", endpoint="", data=json.dumps(payload)).text
    except HTTPError as e:
        e.msg = "Error performing maintenance drain. Got error: %s" % e.msg
        raise
    print undrain_output
    return 0


def down(hostnames):
    """Marks the specified hostnames as being down for maintenance, and makes them unavailable for use.
    :param hostnames: a list of hostnames
    :returns: None
    """
    log.info("Bringing down: %s" % hostnames)
    payload = build_start_maintenance_payload(hostnames)
    client_fn = master_api()
    try:
        down_output = client_fn(method="POST", endpoint="/machine/down", data=json.dumps(payload)).text
    except HTTPError as e:
        e.msg = "Error performing maintenance down. Got error: %s" % e.msg
        raise
    print down_output
    return 0


def up(hostnames):
    """Marks the specified hostnames as no longer being down for maintenance, and makes them available for use.
    :param hostnames: a list of hostnames
    :returns: None
    """
    log.info("Bringing up: %s" % hostnames)
    payload = build_start_maintenance_payload(hostnames)
    client_fn = master_api()
    try:
        up_output = client_fn(method="POST", endpoint="/machine/up", data=json.dumps(payload)).text
    except HTTPError as e:
        e.msg = "Error performing maintenance up. Got error: %s" % e.msg
        raise
    print up_output
    return 0


def status():
    """Get the Mesos maintenance status. This contains hostname/ip mappings for hosts that are either marked as being
    down for maintenance or draining.
    :returns: None
    """
    try:
        status = get_maintenance_status()
    except HTTPError as e:
        e.msg = "Error performing maintenance status. Got error: %s" % e.msg
        raise
    print "%s" % status.text
    return 0


def is_safe_to_kill(hostname):
    """Checks if a host has drained or reached its maintenance window
    :param hostname: hostname to check
    :returns: True or False
    """
    return is_host_drained(hostname) or hostname in get_hosts_past_maintenance_start()


def is_host_drained(hostname):
    """Checks if a host has drained successfully by confirming it is
    draining and currently running 0 tasks
    :param hostname: hostname to check
    :returns: True or False
    """
    mesos_state = get_mesos_state_from_leader()
    task_counts = get_mesos_task_count_by_slave(mesos_state)
    if hostname in task_counts:
        slave_task_count = task_counts[hostname].count
    else:
        slave_task_count = 0
    return is_host_draining(hostname=hostname) and slave_task_count == 0


def is_host_past_maintenance_start(hostname):
    """Checks if a host has reached the start of its maintenance window
    :param hostname: hostname to check
    :returns: True or False
    """
    return hostname in get_hosts_past_maintenance_start()


def is_host_past_maintenance_end(hostname):
    """Checks if a host has reached the end of its maintenance window
    :param hostname: hostname to check
    :returns: True or False
    """
    return hostname in get_hosts_past_maintenance_end()


def get_hosts_past_maintenance_start():
    """Get a list of hosts that have reached the start of their maintenance window
    :returns: List of hostnames
    """
    schedules = get_maintenance_schedule().json()
    current_time = datetime_to_nanoseconds(now())
    ret = []
    if 'windows' in schedules:
        for window in schedules['windows']:
            if window['unavailability']['start']['nanoseconds'] < current_time:
                ret += [host['hostname'] for host in window['machine_ids']]
    print ret
    return ret


def get_hosts_past_maintenance_end():
    """Get a list of hosts that have reached the end of their maintenance window
    :returns: List of hostnames
    """
    schedules = get_maintenance_schedule().json()
    current_time = datetime_to_nanoseconds(now())
    ret = []
    if 'windows' in schedules:
        for window in schedules['windows']:
            end = window['unavailability']['start']['nanoseconds'] + window['unavailability']['duration']['nanoseconds']
            if end < current_time:
                ret += [host['hostname'] for host in window['machine_ids']]
    print ret
    return ret


def paasta_maintenance():
    """Manipulate the maintenance state of a PaaSTA host.
    :returns: None
    """
    args = parse_args()

    action = args.action
    hostnames = args.hostname

    if action not in [
            'down',
            'drain',
            'is_host_down',
            'is_host_drained',
            'is_host_draining',
            'is_host_past_maintenance_end',
            'is_host_past_maintenance_start',
            'is_safe_to_kill',
            'schedule',
            'status',
            'undrain',
            'up',
    ]:
        print "action must be 'drain', 'undrain', 'down', 'up', 'status', or 'schedule'"
        return

    if action != 'status' and not hostnames:
        print "You must specify one or more hostnames"
        return

    start = args.start
    duration = args.duration

    if action == 'drain':
        return drain(hostnames, start, duration)
    elif action == 'undrain':
        return undrain(hostnames)
    elif action == 'down':
        return down(hostnames)
    elif action == 'up':
        return up(hostnames)
    elif action == 'status':
        return status()
    elif action == 'schedule':
        return schedule()
    elif action == 'is_safe_to_kill':
        return is_safe_to_kill(hostnames[0])
    elif action == 'is_host_drained':
        return is_host_drained(hostnames[0])
    elif action == 'is_host_down':
        return is_host_down(hostnames[0])
    elif action == 'is_host_draining':
        return is_host_draining(hostnames[0])
    elif action == 'is_host_past_maintenance_start':
        return is_host_past_maintenance_start(hostnames[0])
    elif action == 'is_host_past_maintenance_end':
        return is_host_past_maintenance_end(hostnames[0])


if __name__ == '__main__':
    if paasta_maintenance():
        sys.exit(0)
    sys.exit(1)
