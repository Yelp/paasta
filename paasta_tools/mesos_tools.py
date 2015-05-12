import os
import socket
import requests
import json
import re

from kazoo.client import KazooClient

# mesos.cli.master reads its config file at *import* time, so we must have
# this environment variable set and ready to go at that time so we can
# read in the config for zookeeper, etc
os.environ['MESOS_CLI_CONFIG'] = '/nail/etc/mesos-cli.json'
from mesos.cli import master

MESOS_MASTER_PORT = 5050
MESOS_SLAVE_PORT = '5051'


class MesosSlaveConnectionError(Exception):
    pass


def get_mesos_tasks_from_master(job_id):
    return master.CURRENT.tasks(fltr=job_id)


def get_running_mesos_tasks_for_service(job_id):
    all_tasks = get_mesos_tasks_from_master(job_id)
    return [task for task in all_tasks if task['state'] == 'TASK_RUNNING']


def get_non_running_mesos_tasks_for_service(job_id):
    all_tasks = get_mesos_tasks_from_master(job_id)
    return [task for task in all_tasks if task['state'] != 'TASK_RUNNING']


def fetch_mesos_stats():
    """Queries the mesos stats api and returns a dictionary of the results"""
    response = master.CURRENT.fetch('metrics/snapshot')
    response.raise_for_status()
    return response.json()


def fetch_local_slave_state():
    """Fetches mesos slave state.json and returns it as a dict."""
    hostname = socket.getfqdn()
    stats_uri = 'http://%s:%s/state.json' % (hostname, MESOS_SLAVE_PORT)
    try:
        response = requests.get(stats_uri, timeout=10)
    except requests.ConnectionError as e:
        raise MesosSlaveConnectionError(
            'Could not connect to the mesos slave to see which services are running\n'
            'on %s. Is the mesos-slave running?\n'
            'Error was: %s\n' % (e.request.url, e.message)
        )
    response.raise_for_status()
    return json.loads(response.text)


def fetch_mesos_state_from_leader():
    """Fetches mesos state from the leader."""
    return master.CURRENT.state


def get_mesos_quorum(state):
    """Returns the configured quorum size.
    :param state: mesos state dictionary"""
    return int(state['flags']['quorum'])


def get_zookeeper_config(state):
    """Returns dict, containing the zookeeper hosts and path.
    :param state: mesos state dictionary"""
    re_zk = re.match(r"^zk://([^/]*)/(.*)$", state['flags']['zk'])
    return {'hosts': re_zk.group(1), 'path': re_zk.group(2)}


def get_number_of_mesos_masters(zk_config):
    """Returns an array, containing mesos masters
    :param zk_config: dict containing information about zookeeper config.
    Masters register themself in zookeeper by creating info_ entries.
    We count these entries to get the number of masters.
    """
    zk = KazooClient(hosts=zk_config['hosts'], read_only=True)
    zk.start()
    root_entries = zk.get_children(zk_config['path'])
    result = [info for info in root_entries if info.startswith('info_')]
    zk.stop()
    return len(result)
