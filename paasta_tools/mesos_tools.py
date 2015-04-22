import os
import socket
import requests
import json
import re

from kazoo.client import KazooClient

from paasta_tools.marathon_tools import compose_job_id
# mesos.cli.master reads its config file at *import* time, so we must have
# this environment variable set and ready to go at that time so we can
# read in the config for zookeeper, etc
os.environ['MESOS_CLI_CONFIG'] = '/nail/etc/mesos-cli.json'
from mesos.cli import master


def get_mesos_tasks_from_master(job_id):
    return master.CURRENT.tasks(fltr=job_id)


def get_mesos_tasks_for_service(service, instance):
    job_id = compose_job_id(service, instance)
    return get_mesos_tasks_from_master(job_id)


def get_running_mesos_tasks_for_service(service, instance):
    all_tasks = get_mesos_tasks_for_service(service, instance)
    return [task for task in all_tasks if task['state'] == 'TASK_RUNNING']


def get_non_running_mesos_tasks_for_service(service, instance):
    all_tasks = get_mesos_tasks_for_service(service, instance)
    return [task for task in all_tasks if task['state'] != 'TASK_RUNNING']


def fetch_mesos_stats():
    """Queries the mesos stats api and returns a dictionary of the results"""
    # We make mesos bind on the "primary" of the server
    my_ip = socket.getfqdn()
    stats_uri = 'http://%s:5050/metrics/snapshot' % my_ip
    response = requests.get(stats_uri, timeout=5)
    return json.loads(response.text)


def fetch_mesos_state():
    """Fetches mesos state.json and returns it as a dict."""
    my_ip = socket.getfqdn()
    stats_uri = 'http://%s:5050/state.json' % my_ip
    response = requests.get(stats_uri, timeout=5)
    return json.loads(response.text)


def get_mesos_quorum(state):
    """Returns the configured quorum size.
    :param state: mesos state dictionary"""
    return int(state['flags']['quorum'])


def get_zookeeper_config(state):
    """Returns dict, containing the zookeeper hosts and path.
    :param state: mesos state dictionary"""
    re_zk = re.match('^zk:\/\/([^\/]*)\/(.*)$', state['flags']['zk'])
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
