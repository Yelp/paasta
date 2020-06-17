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
import argparse
import datetime
import json
import logging
from socket import gaierror
from socket import getfqdn
from socket import gethostbyname
from typing import List
from typing import NamedTuple
from typing import Optional

import a_sync
from dateutil import parser
from pytimeparse import timeparse
from requests import Request
from requests import Session
from requests.exceptions import HTTPError

from paasta_tools.mesos_tools import get_count_running_tasks_on_slave
from paasta_tools.mesos_tools import get_mesos_config_path
from paasta_tools.mesos_tools import get_mesos_leader
from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.mesos_tools import MESOS_MASTER_PORT
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import time_cache
from paasta_tools.utils import to_bytes


log = logging.getLogger(__name__)


class Hostname(NamedTuple):
    host: str
    ip: str


class Credentials(NamedTuple):
    file: str
    principal: str
    secret: str


class Resource(NamedTuple):
    name: str
    amount: int


MAINTENANCE_ROLE = "maintenance"


def base_api(mesos_config_path: Optional[str] = None):
    """Helper function for making all API requests

    :returns: a function that can be called to make a request
    """
    leader = get_mesos_leader(mesos_config_path)

    def execute_request(method, endpoint, timeout=(3, 2), **kwargs):
        url = "http://%s:%d%s" % (leader, MESOS_MASTER_PORT, endpoint)
        s = Session()
        s.auth = (get_principal(), get_secret())
        req = Request(method, url, **kwargs)
        prepared = s.prepare_request(req)
        try:
            resp = s.send(prepared, timeout=timeout)
            resp.raise_for_status()
            return resp
        except HTTPError:
            raise HTTPError("Error executing API request calling %s." % url)

    return execute_request


def master_api(mesos_config_path: Optional[str] = None):
    """Helper function for making API requests to the /master API endpoints

    :returns: a function that can be called to make a request to /master
    """

    def execute_master_api_request(method, endpoint, **kwargs):
        base_api_client = base_api(mesos_config_path=mesos_config_path)
        return base_api_client(method, "/master%s" % endpoint, **kwargs)

    return execute_master_api_request


def operator_api(mesos_config_path: Optional[str] = None):
    def execute_operator_api_request(**kwargs):
        base_api_client = base_api(mesos_config_path=mesos_config_path)
        if "headers" in kwargs:
            kwargs["headers"]["Content-Type"] = "application/json"
        else:
            kwargs["headers"] = {"Content-Type": "application/json"}
        data = kwargs.pop("data")
        return base_api_client("POST", "/api/v1", data=json.dumps(data), **kwargs)

    return execute_operator_api_request


def reserve_api():
    """Helper function for making API requests to the /reserve API endpoints

    :returns: a function that can be called to make a request to /reserve
    """

    def execute_reserve_api_request(method, endpoint, **kwargs):
        master_api_client = master_api()
        return master_api_client(method, "/reserve%s" % endpoint, **kwargs)

    return execute_reserve_api_request


def unreserve_api():
    """Helper function for making API requests to the /unreserve API endpoints

    :returns: a function that can be called to make a request to /unreserve
    """

    def execute_unreserve_api_request(method, endpoint, **kwargs):
        master_api_client = master_api()
        return master_api_client(method, "/unreserve%s" % endpoint, **kwargs)

    return execute_unreserve_api_request


def maintenance_api():
    """Helper function for making API requests to the /master/maintenance API endpoints

    :returns: a function that can be called to make a request to /master/maintenance
    """

    def execute_schedule_api_request(method, endpoint, **kwargs):
        master_api_client = master_api()
        return master_api_client(
            method, "/maintenance%s" % endpoint, timeout=(3, 10), **kwargs
        )

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
    """Makes a GET_MAINTENANCE_SCHEDULE request to the operator api

    :returns: a GET_MAINTENANCE_SCHEDULE response
    """
    client_fn = operator_api()
    return client_fn(data={"type": "GET_MAINTENANCE_SCHEDULE"})


@time_cache(ttl=10)
def get_maintenance_status(mesos_config_path: Optional[str] = None):
    """Makes a GET_MAINTENANCE_STATUS request to the operator api

    :returns: a GET_MAINTENANCE_STATUS response
    """
    client_fn = operator_api(mesos_config_path=mesos_config_path)
    return client_fn(data={"type": "GET_MAINTENANCE_STATUS"})


def schedule():
    """Get the Mesos maintenance schedule. This contains hostname/ip mappings and their maintenance window.
    :returns: GET_MAINTENANCE_SCHEDULE response text
    """
    try:
        schedule = get_maintenance_schedule()
    except HTTPError:
        raise HTTPError("Error getting maintenance schedule.")
    return schedule.text


def get_hosts_with_state(
    state, system_paasta_config: Optional[SystemPaastaConfig] = None
) -> List[str]:
    """Helper function to check the maintenance status and return all hosts
    listed as being in a current state

    :param state: State we are interested in ('down_machines' or 'draining_machines')
    :returns: A list of hostnames in the specified state or an empty list if no machines
    """

    mesos_config_path = get_mesos_config_path(system_paasta_config)
    try:
        status = get_maintenance_status(mesos_config_path).json()
        status = status["get_maintenance_status"]["status"]
    except HTTPError:
        raise HTTPError("Error getting maintenance status.")
    if not status or state not in status:
        return []
    if "id" in status[state][0]:
        return [machine["id"]["hostname"] for machine in status[state]]
    else:
        return [machine["hostname"] for machine in status[state]]


def get_draining_hosts(system_paasta_config: Optional[SystemPaastaConfig] = None):
    """Returns a list of hostnames that are marked as draining

    :returns: a list of strings representing hostnames
    """
    return get_hosts_with_state(
        state="draining_machines", system_paasta_config=system_paasta_config
    )


def get_down_hosts():
    """Returns a list of hostnames that are marked as down

    :returns: a list of strings representing hostnames
    """
    return get_hosts_with_state(state="down_machines")


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


def get_hosts_forgotten_draining(grace=0):
    """Find hosts that are still marked as draining (rather than down) after the start
    of their maintenance window.
    :param grace: integer number of nanoseconds to allow a host to be left in the draining
    state after the start of its maintenance window before we consider it forgotten.
    :returns: a list of hostnames of hosts forgotten draining
    """
    draining_hosts = get_draining_hosts()
    log.debug("draining_hosts: %s" % draining_hosts)

    hosts_past_maintenance_start = get_hosts_past_maintenance_start(grace=grace)
    log.debug("hosts_past_maintenance_start: %s" % hosts_past_maintenance_start)

    forgotten_draining = list(
        set(draining_hosts).intersection(hosts_past_maintenance_start)
    )
    log.debug("forgotten_draining: %s" % forgotten_draining)

    return forgotten_draining


def are_hosts_forgotten_draining():
    """Quick way to test if there are any forgotten draining hosts.
    :returns: a boolean that is True if there are any forgotten draining
    hosts and False otherwise
    """
    return bool(get_hosts_forgotten_draining())


def get_hosts_forgotten_down(grace=0):
    """Find hosts that are still marked as down (rather than up) after the end
    of their maintenance window.
    :param grace: integer number of nanoseconds to allow a host to be left in the down
    state after the end of its maintenance window before we consider it forgotten.
    :returns: a list of hostnames of hosts forgotten down
    """
    down_hosts = get_down_hosts()
    log.debug("down_hosts: %s" % down_hosts)

    hosts_past_maintenance_end = get_hosts_past_maintenance_end(grace=grace)
    log.debug("hosts_past_maintenance_end: %s" % hosts_past_maintenance_end)

    forgotten_down = list(set(down_hosts).intersection(hosts_past_maintenance_end))
    log.debug("forgotten_down: %s" % forgotten_down)

    return forgotten_down


def are_hosts_forgotten_down():
    """Quick way to test if there are any forgotten down hosts.
    :returns: a boolean that is True if there are any forgotten down
    hosts and False otherwise
    """
    return bool(get_hosts_forgotten_down())


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
    except Exception:
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


def build_maintenance_payload(hostnames, maint_type):
    """Creates the JSON payload necessary to bring the specified hostnames up/down for maintenance.
    :param hostnames: a list of hostnames
    :returns: a dictionary representing the list of machines to bring up/down for maintenance
    """
    return {
        "type": maint_type.upper(),
        maint_type.lower(): {"machines": get_machine_ids(hostnames)},
    }


def hostnames_to_components(hostnames, resolve=False):
    """Converts a list of 'host[|ip]' entries into namedtuples containing 'host' and 'ip' attributes,
    optionally performing a DNS lookup to resolve the hostname into an IP address
    :param hostnames: a list of hostnames where each hostname can be of the form 'host[|ip]'
    :param resolve: boolean representing whether to lookup the IP address corresponding to the hostname via DNS
    :returns: a namedtuple containing the hostname and IP components
    """

    components = []
    for hostname in hostnames:
        # This is to allow specifying a hostname as "hostname|ipaddress"
        # to avoid querying DNS for the IP.
        if "|" in hostname:
            (host, ip) = hostname.split("|")
            components.append(Hostname(host=host, ip=ip))
        else:
            try:
                ip = gethostbyname(hostname) if resolve else None
            except gaierror:
                log.error(f"Failed to resolve IP for {hostname}, continuing regardless")
                continue
            components.append(Hostname(host=hostname, ip=ip))
    return components


def get_machine_ids(hostnames):
    """Helper function to convert a list of hostnames into a JSON list of hostname/ip pairs.
    :param hostnames: a list of hostnames
    :returns: a dictionary representing the list of machines to bring up/down for maintenance
    """
    machine_ids = []
    components = hostnames_to_components(hostnames, resolve=True)
    for component in components:
        machine_id = {"hostname": component.host, "ip": component.ip}
        machine_ids.append(machine_id)
    return machine_ids


def build_reservation_payload(resources):
    """Creates the JSON payload needed to dynamically (un)reserve resources in mesos.
    :param resources: list of Resource named tuples specifying the name and amount of the resource to (un)reserve
    :returns: a dictionary that can be sent to Mesos to (un)reserve resources
    """
    payload = []
    for resource in resources:
        payload.append(
            {
                "name": resource.name,
                "type": "SCALAR",
                "scalar": {"value": resource.amount},
                "role": MAINTENANCE_ROLE,
                "reservation": {"principal": get_principal()},
            }
        )
    return payload


def build_maintenance_schedule_payload(
    hostnames, start=None, duration=None, drain=True
):
    """Creates the JSON payload needed to (un)schedule maintenance on the specified hostnames.
    :param hostnames: a list of hostnames
    :param start: the time to start the maintenance, represented as number of nanoseconds since the epoch
    :param duration: length of the maintenance window, represented as number of nanoseconds since the epoch
    :param drain: boolean to note whether we are draining (True) the specified hosts or undraining (False) them
    :returns: a dictionary that can be sent to Mesos to (un)schedule maintenance
    """
    schedule = get_maintenance_schedule().json()["get_maintenance_schedule"]["schedule"]
    machine_ids = get_machine_ids(hostnames)

    if drain:
        unavailability = dict()
        unavailability["start"] = dict()
        unavailability["start"]["nanoseconds"] = int(start)
        unavailability["duration"] = dict()
        unavailability["duration"]["nanoseconds"] = int(duration)

        window = dict()
        window["machine_ids"] = machine_ids
        window["unavailability"] = unavailability

    if schedule:
        for existing_window in schedule["windows"]:
            for existing_machine_id in existing_window["machine_ids"]:
                # If we already have a maintenance window scheduled for one of the hosts,
                # replace it with the new window.
                if existing_machine_id in machine_ids:
                    existing_window["machine_ids"].remove(existing_machine_id)
                    if not existing_window["machine_ids"]:
                        schedule["windows"].remove(existing_window)
        if drain:
            windows = schedule["windows"] + [window]
        else:
            windows = schedule["windows"]
    elif drain:
        windows = [window]
    else:
        windows = []

    payload = dict()
    payload["windows"] = windows

    return {
        "type": "UPDATE_MAINTENANCE_SCHEDULE",
        "update_maintenance_schedule": {"schedule": payload},
    }


def load_credentials(mesos_secrets="/nail/etc/mesos-slave-secret"):
    """Loads the mesos-slave credentials from the specified file. These credentials will be used for all
    maintenance API requests.
    :param mesos_secrets: optional argument specifying the path to the file containing the mesos-slave credentials
    :returns: a tuple of the form (username, password)
    """
    try:
        with open(mesos_secrets) as data_file:
            data = json.load(data_file)
    except EnvironmentError:
        log.error(
            "maintenance calls must be run on a Mesos slave containing valid credentials (%s)"
            % mesos_secrets
        )
        raise
    try:
        username = data["principal"]
        password = data["secret"]
    except KeyError:
        log.error(
            "%s does not contain Mesos slave credentials in the expected format. "
            "See http://mesos.apache.org/documentation/latest/authentication/ for details"
            % mesos_secrets
        )
        raise
    return Credentials(file=mesos_secrets, principal=username, secret=password)


def get_principal(mesos_secrets="/nail/etc/mesos-slave-secret"):
    """Helper function to get the principal from the mesos-slave credentials
    :param mesos_secrets: optional argument specifying the path to the file containing the mesos-slave credentials
    :returns: a string containing the principal/username
    """
    return load_credentials(mesos_secrets).principal


def get_secret(mesos_secrets="/nail/etc/mesos-slave-secret"):
    """Helper function to get the secret from the mesos-slave credentials
    :param mesos_secrets: optional argument specifying the path to the file containing the mesos-slave credentials
    :returns: a string containing the secret/password
    """
    return load_credentials(mesos_secrets).secret


def _make_request_payload(slave_id, reservation_payload):
    return {
        "slaveId": slave_id.encode("UTF-8"),
        # We used to_bytes here since py2 json doesn't have a well defined
        # return type.  When moving to python 3, replace with .encode()
        "resources": to_bytes(json.dumps(reservation_payload)).replace(b"+", b"%20"),
    }


def _make_operator_reservation_request_payload(slave_id, payload, request_type):
    return {
        "type": request_type.upper(),
        request_type.lower(): {"agent_id": {"value": slave_id}},
        "resources": payload,
    }


def reserve(slave_id, resources):
    """Dynamically reserve resources in marathon to prevent tasks from using them.
    :param slave_id: the id of the mesos slave
    :param resources: list of Resource named tuples specifying the name and amount of the resource to (un)reserve
    :returns: boolean where 0 represents success and 1 is a failure
    """
    log.info(f"Dynamically reserving resources on {slave_id}: {resources}")
    payload = _make_operator_reservation_request_payload(
        slave_id=slave_id,
        payload=build_reservation_payload(resources),
        request_type="reserve_resources",
    )
    client_fn = operator_api()
    try:
        print(payload)
        reserve_output = client_fn(data=payload).text
    except HTTPError:
        raise HTTPError("Error adding dynamic reservation.")
    return reserve_output


def unreserve(slave_id, resources):
    """Dynamically unreserve resources in marathon to allow tasks to using them.
    :param slave_id: the id of the mesos slave
    :param resources: list of Resource named tuples specifying the name and amount of the resource to (un)reserve
    :returns: boolean where 0 represents success and 1 is a failure
    """
    log.info(f"Dynamically unreserving resources on {slave_id}: {resources}")
    payload = _make_operator_reservation_request_payload(
        slave_id=slave_id,
        payload=build_reservation_payload(resources),
        request_type="unreserve_resources",
    )
    client_fn = operator_api()
    try:
        unreserve_output = client_fn(data=payload).text
    except HTTPError:
        raise HTTPError("Error adding dynamic unreservation.")
    return unreserve_output


def components_to_hosts(components):
    """Convert a list of Component namedtuples to a list of their hosts
    :param components: a list of Component namedtuples
    :returns: list of the hosts associated with each Component
    """
    hosts = []
    for component in components:
        hosts.append(component.host)
    return hosts


def reserve_all_resources(hostnames):
    """Dynamically reserve all available resources on the specified hosts
    :param hostnames: list of hostnames to reserve resources on
    """
    mesos_state = a_sync.block(get_mesos_master().state_summary)
    components = hostnames_to_components(hostnames)
    hosts = components_to_hosts(components)
    known_slaves = [
        slave for slave in mesos_state["slaves"] if slave["hostname"] in hosts
    ]
    for slave in known_slaves:
        hostname = slave["hostname"]
        log.info("Reserving all resources on %s" % hostname)
        slave_id = slave["id"]
        resources = []
        for resource in ["disk", "mem", "cpus", "gpus"]:
            free_resource = (
                slave["resources"][resource] - slave["used_resources"][resource]
            )
            for role in slave["reserved_resources"]:
                free_resource -= slave["reserved_resources"][role][resource]
            resources.append(Resource(name=resource, amount=free_resource))
        try:
            reserve(slave_id=slave_id, resources=resources)
        except HTTPError:
            raise HTTPError(
                f"Failed reserving all of the resources on {hostname} ({slave_id}). Aborting."
            )


def unreserve_all_resources(hostnames):
    """Dynamically unreserve all available resources on the specified hosts
    :param hostnames: list of hostnames to unreserve resources on
    """
    mesos_state = a_sync.block(get_mesos_master().state_summary)
    components = hostnames_to_components(hostnames)
    hosts = components_to_hosts(components)
    known_slaves = [
        slave for slave in mesos_state["slaves"] if slave["hostname"] in hosts
    ]
    for slave in known_slaves:
        hostname = slave["hostname"]
        log.info("Unreserving all resources on %s" % hostname)
        slave_id = slave["id"]
        resources = []
        if MAINTENANCE_ROLE in slave["reserved_resources"]:
            for resource in ["disk", "mem", "cpus", "gpus"]:
                reserved_resource = slave["reserved_resources"][MAINTENANCE_ROLE][
                    resource
                ]
                resources.append(Resource(name=resource, amount=reserved_resource))
            try:
                unreserve(slave_id=slave_id, resources=resources)
            except HTTPError:
                raise HTTPError(
                    f"Failed unreserving all of the resources on {hostname} ({slave_id}). Aborting."
                )


def drain(hostnames, start, duration, reserve_resources=True):
    """Schedules a maintenance window for the specified hosts and marks them as draining.
    :param hostnames: a list of hostnames
    :param start: the time to start the maintenance, represented as number of nanoseconds since the epoch
    :param duration: length of the maintenance window, represented as number of nanoseconds since the epoch
    :param reserve_resources: bool setting to also reserve the free resources on the agent before the drain call
    :returns: None
    """
    log.info("Draining: %s" % hostnames)
    if reserve_resources:
        try:
            reserve_all_resources(hostnames)
        except HTTPError as e:
            log.warning("Failed to reserve resources, will continue to drain: %s" % e)
    payload = build_maintenance_schedule_payload(hostnames, start, duration, drain=True)
    client_fn = operator_api()
    try:
        drain_output = client_fn(data=payload).text
    except HTTPError:
        raise HTTPError("Error performing maintenance drain.")
    return drain_output


def undrain(hostnames, unreserve_resources=True):
    """Unschedules the maintenance window for the specified hosts and unmarks them as draining. They are ready for
    regular use.
    :param hostnames: a list of hostnames
    :param unreserve_resources: bool setting to also unreserve resources on the agent before the undrain call
    :returns: None
    """
    log.info("Undraining: %s" % hostnames)
    if unreserve_resources:
        try:
            unreserve_all_resources(hostnames)
        except HTTPError as e:
            log.warning(
                "Failed to unreserve resources, will continue to undrain: %s" % e
            )
    payload = build_maintenance_schedule_payload(hostnames, drain=False)
    client_fn = get_schedule_client()
    client_fn = operator_api()
    try:
        undrain_output = client_fn(data=payload).text
    except HTTPError:
        raise HTTPError("Error performing maintenance undrain.")
    return undrain_output


def down(hostnames):
    """Marks the specified hostnames as being down for maintenance, and makes them unavailable for use.
    :param hostnames: a list of hostnames
    :returns: None
    """
    log.info("Bringing down: %s" % hostnames)
    payload = build_maintenance_payload(hostnames, "start_maintenance")
    client_fn = operator_api()
    try:
        down_output = client_fn(data=payload).text
    except HTTPError:
        raise HTTPError("Error performing maintenance down.")
    return down_output


def up(hostnames):
    """Marks the specified hostnames as no longer being down for maintenance, and makes them available for use.
    :param hostnames: a list of hostnames
    :returns: None
    """
    log.info("Bringing up: %s" % hostnames)
    payload = build_maintenance_payload(hostnames, "stop_maintenance")
    client_fn = operator_api()
    try:
        up_output = client_fn(data=payload).text
    except HTTPError:
        raise HTTPError("Error performing maintenance up.")
    return up_output


def raw_status():
    """Get the Mesos maintenance status. This contains hostname/ip mappings for hosts that are either marked as being
    down for maintenance or draining.
    :returns: Response Object containing status
    """
    try:
        status = get_maintenance_status()
    except HTTPError:
        raise HTTPError("Error performing maintenance status.")
    return status


def status():
    """Get the Mesos maintenance status. This contains hostname/ip mappings for hosts that are either marked as being
    down for maintenance or draining.
    :returns: Text representation of the status
    """
    return raw_status().text


def friendly_status():
    """Display the Mesos maintenance status in a human-friendly way.
    :returns: Text representation of the human-friendly status
    """
    status = raw_status().json()["get_maintenance_status"]["status"]
    ret = ""
    for machine in status.get("draining_machines", []):
        ret += "{} ({}): Draining\n".format(
            machine["id"]["hostname"], machine["id"]["ip"]
        )
    for machine in status.get("down_machines", []):
        ret += "{} ({}): Down\n".format(machine["hostname"], machine["ip"])
    return ret


def is_host_drained(hostname):
    """Checks if a host has drained successfully by confirming it is
    draining and currently running 0 tasks
    :param hostname: hostname to check
    :returns: True or False
    """
    return (
        is_host_draining(hostname=hostname)
        and get_count_running_tasks_on_slave(hostname) == 0
    )


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


def get_hosts_past_maintenance_start(grace=0):
    """Get a list of hosts that have reached the start of their maintenance window
    :param grace: integer number of nanoseconds to allow a host to be left in the draining
    state after the start of its maintenance window before we consider it past its maintenance start
    :returns: List of hostnames
    """
    schedules = get_maintenance_schedule().json()["get_maintenance_schedule"][
        "schedule"
    ]
    current_time = datetime_to_nanoseconds(now()) - grace
    ret = []
    if "windows" in schedules:
        for window in schedules["windows"]:
            if window["unavailability"]["start"]["nanoseconds"] < current_time:
                ret += [host["hostname"] for host in window["machine_ids"]]
    log.debug(f"Hosts past maintenance start: {ret}")
    return ret


def get_hosts_past_maintenance_end(grace=0):
    """Get a list of hosts that have reached the end of their maintenance window
    :param grace: integer number of nanoseconds to allow a host to be left in the down
    state after the end of its maintenance window before we consider it past its maintenance end
    :returns: List of hostnames
    """
    schedules = get_maintenance_schedule().json()["get_maintenance_schedule"][
        "schedule"
    ]
    current_time = datetime_to_nanoseconds(now()) - grace
    ret = []
    if "windows" in schedules:
        for window in schedules["windows"]:
            end = (
                window["unavailability"]["start"]["nanoseconds"]
                + window["unavailability"]["duration"]["nanoseconds"]
            )
            if end < current_time:
                ret += [host["hostname"] for host in window["machine_ids"]]
    log.debug(f"Hosts past maintenance end: {ret}")
    return ret
