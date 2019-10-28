# Copied from https://github.com/Yelp/paasta/blob/master/paasta_tools/mesos_maintenance.py
# stripped to just drain/down/up and necessary sub methods and slightly refactored so
# the cluster name can be passed as an argument
import json
import os
from collections import namedtuple
from socket import gethostbyname

import colorlog
from requests import Request
from requests import Session
from requests.exceptions import HTTPError
from requests.exceptions import Timeout
from retry import retry
Hostname = namedtuple('Hostname', ['host', 'ip'])
MESOS_MASTER_PORT = 5050
Credentials = namedtuple('Credentials', ['file', 'principal', 'secret'])
log = colorlog.getLogger(__name__)


def get_principal(mesos_secret_path):
    """Helper function to get the principal from the mesos-slave credentials
    :param mesos_secret_paths: specifying the path to the file containing the mesos-slave credentials
    :returns: a string containing the principal/username
    """
    return load_credentials(mesos_secret_path).principal


def get_secret(mesos_secret_path):
    """Helper function to get the secret from the mesos-slave credentials
    :param mesos_secret_paths: argument specifying the path to the file containing the mesos-slave credentials
    :returns: a string containing the secret/password
    """
    return load_credentials(mesos_secret_path).secret


def load_credentials(mesos_secret_path):
    """Loads the mesos-slave credentials from the specified file. These credentials will be used for all
    maintenance API requests.
    :param mesos_secret_paths: argument specifying the path to the file containing the mesos-slave credentials
    :returns: a tuple of the form (username, password)
    """
    if not mesos_secret_path:
        data = json.loads(os.environ['PAASTA_SECRET_MESOS_SLAVE'])
    else:
        try:
            with open(mesos_secret_path) as data_file:
                data = json.load(data_file)
        except EnvironmentError:
            log.error(f'maintenance calls must have valid credentials ({mesos_secret_path})')
            raise
    try:
        username = data['principal']
        password = data['secret']
    except KeyError:
        log.error(
            '%s does not contain Mesos slave credentials in the expected format. '
            'See http://mesos.apache.org/documentation/latest/authentication/ for details' % mesos_secret_path,
        )
        raise
    return Credentials(file=mesos_secret_path, principal=username, secret=password)


def base_api(mesos_master_fqdn, mesos_secret_path):
    """Helper function for making all API requests
    :returns: a function that can be called to make a request
    """

    def execute_request(method, endpoint, timeout=(3, 1), **kwargs):
        url = 'http://%s:%d%s' % (mesos_master_fqdn, MESOS_MASTER_PORT, endpoint)
        s = Session()
        s.auth = (get_principal(mesos_secret_path), get_secret(mesos_secret_path))
        req = Request(method, url, **kwargs)
        prepared = s.prepare_request(req)
        try:
            resp = s.send(
                prepared,
                timeout=timeout,
            )
            resp.raise_for_status()
            return resp
        except HTTPError:
            raise HTTPError('Error executing API request calling %s.' % url)
    return execute_request


def operator_api(mesos_master_fqdn, mesos_secret_path):
    def execute_operator_api_request(**kwargs):
        base_api_client = base_api(mesos_master_fqdn, mesos_secret_path)
        if 'headers' in kwargs:
            kwargs['headers']['Content-Type'] = 'application/json'
        else:
            kwargs['headers'] = {'Content-Type': 'application/json'}
        data = kwargs.pop('data')
        return base_api_client('POST', '/api/v1', data=json.dumps(data), **kwargs)
    return execute_operator_api_request


@retry(exceptions=Timeout, tries=5, delay=5)
def down(operator_client, hostnames):
    """Marks the specified hostnames as being down for maintenance, and makes them unavailable for use.
    :param hostnames: a list of hostnames
    :returns: None
    """
    log.info('Bringing down: %s' % hostnames)
    payload = build_maintenance_payload(hostnames, 'start_maintenance')
    try:
        down_output = operator_client(data=payload).text
    except HTTPError:
        raise HTTPError('Error performing maintenance down.')
    return down_output


@retry(exceptions=Timeout, tries=5, delay=5)
def up(operator_client, hostnames):
    """Marks the specified hostnames as no longer being down for maintenance, and makes them available for use.
    :param hostnames: a list of hostnames
    :returns: None
    """
    log.info('Bringing up: %s' % hostnames)
    payload = build_maintenance_payload(hostnames, 'stop_maintenance')
    try:
        up_output = operator_client(data=payload).text
    except HTTPError:
        raise HTTPError('Error performing maintenance up.')
    return up_output


@retry(exceptions=Timeout, tries=5, delay=5)
def drain(operator_client, hostnames, start, duration):
    """Schedules a maintenance window for the specified hosts and marks them as draining.
    :param hostnames: a list of hostnames
    :param start: the time to start the maintenance, represented as number of nanoseconds since the epoch
    :param duration: length of the maintenance window, represented as number of nanoseconds since the epoch
    :returns: None
    """
    log.info('Draining: %s' % hostnames)
    payload = build_maintenance_schedule_payload(operator_client, hostnames, start, duration, drain=True)
    try:
        drain_output = operator_client(data=payload).text
    except HTTPError:
        raise HTTPError('Error performing maintenance drain.')
    return drain_output


def build_maintenance_schedule_payload(operator_client, hostnames, start=None, duration=None, drain=True):
    """Creates the JSON payload needed to (un)schedule maintenance on the specified hostnames.
    :param hostnames: a list of hostnames
    :param start: the time to start the maintenance, represented as number of nanoseconds since the epoch
    :param duration: length of the maintenance window, represented as number of nanoseconds since the epoch
    :param drain: boolean to note whether we are draining (True) the specified hosts or undraining (False) them
    :returns: a dictionary that can be sent to Mesos to (un)schedule maintenance
    """
    schedule = get_maintenance_schedule(operator_client).json()['get_maintenance_schedule']['schedule']
    machine_ids = get_machine_ids(hostnames)

    if drain:
        unavailability = {
            'start': {'nanoseconds': int(start)},
            'duration': {'nanoseconds': int(duration)},
        }
        window = {'machine_ids': machine_ids, 'unavailability': unavailability}

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

    payload = {'windows': windows}

    return {
        'type': 'UPDATE_MAINTENANCE_SCHEDULE',
        'update_maintenance_schedule': {'schedule': payload},
    }


def build_maintenance_payload(hostnames, maint_type):
    """Creates the JSON payload necessary to bring the specified hostnames up/down for maintenance.
    :param hostnames: a list of hostnames
    :returns: a dictionary representing the list of machines to bring up/down for maintenance
    """
    return {
        'type': maint_type.upper(),
        maint_type.lower(): {
            'machines': get_machine_ids(hostnames),
        },
    }


def get_maintenance_schedule(operator_client):
    """Makes a GET_MAINTENANCE_SCHEDULE request to the operator api
    :returns: a GET_MAINTENANCE_SCHEDULE response
    """
    return operator_client(data={'type': 'GET_MAINTENANCE_SCHEDULE'})


def get_machine_ids(hostnames):
    """Helper function to convert a list of hostnames into a JSON list of hostname/ip pairs.
    :param hostnames: a list of hostnames
    :returns: a dictionary representing the list of machines to bring up/down for maintenance
    """
    machine_ids = []
    components = hostnames_to_components(hostnames, resolve=True)
    for component in components:
        machine_id = {
            'hostname': component.host,
            'ip': component.ip,
        }
        machine_ids.append(machine_id)
    return machine_ids


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
        if '|' in hostname:
            (host, ip) = hostname.split('|')
            components.append(Hostname(host=host, ip=ip))
        else:
            ip = gethostbyname(hostname) if resolve else None
            components.append(Hostname(host=hostname, ip=ip))
    return components
