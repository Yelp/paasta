import os
import socket
import requests
import json
import re


from kazoo.client import KazooClient

# mesos.cli.master reads its config file at *import* time, so we must have
# this environment variable set and ready to go at that time so we can
# read in the config for zookeeper, etc
if 'MESOS_CLI_CONFIG' not in os.environ:
    os.environ['MESOS_CLI_CONFIG'] = '/nail/etc/mesos-cli.json'


class MasterNotAvailableException(Exception):
    pass


class NoSlavesAvailable(Exception):
    pass


def raise_cli_exception(msg):
    if msg.startswith("unable to connect to a master"):
        raise MasterNotAvailableException(msg)
    else:
        raise Exception(msg)

# monkey patch the log.fatal method to raise an exception rather than a sys.exit
import mesos.cli.log
mesos.cli.log.fatal = lambda msg, code = 1: raise_cli_exception(msg)


MY_HOSTNAME = socket.getfqdn()
MESOS_MASTER_PORT = 5050
MESOS_SLAVE_PORT = '5051'
from mesos.cli import master


class MesosMasterConnectionError(Exception):
    pass


class MesosSlaveConnectionError(Exception):
    pass


def get_mesos_leader(hostname=MY_HOSTNAME):
    """Get the current mesos-master leader's hostname. Raise
    MesosMasterConnectionError if we can't connect.

    :param hostname: The hostname to query mesos-master on
    :returns: The current mesos-master hostname"""
    redirect_url = 'http://%s:%s/redirect' % (hostname, MESOS_MASTER_PORT)
    try:
        r = requests.get(redirect_url, timeout=10)
    except requests.exceptions.ConnectionError as e:
        # Repackage the exception so upstream code can handle this case without
        # knowing our implementation details.
        raise MesosMasterConnectionError(repr(e))
    r.raise_for_status()
    return re.search('(?<=http://)[0-9a-zA-Z\.\-]+', r.url).group(0)


def is_mesos_leader(hostname=MY_HOSTNAME):
    """Check if a hostname is the current mesos leader.

    :param hostname: The hostname to query mesos-master on
    :returns: True if hostname is the mesos-master leader, False otherwise"""
    return hostname in get_mesos_leader(hostname)


def get_current_tasks(job_id):
    """ Returns a list of all the tasks with a given job id.
    Note: this will only return tasks from active frameworks.
    :param job_id: the job id of the tasks.
    :return tasks: a list of mesos.cli.Task.
    """
    return master.CURRENT.tasks(fltr=job_id, active_only=True)


def filter_running_tasks(tasks):
    """ Filters those tasks where it's state is TASK_RUNNING.
    :param tasks: a list of mesos.cli.Task
    :return filtered: a list of running tasks
    """
    return [task for task in tasks if task['state'] == 'TASK_RUNNING']


def filter_not_running_tasks(tasks):
    """ Filters those tasks where it's state is *not* TASK_RUNNING.
    :param tasks: a list of mesos.cli.Task
    :return filtered: a list of tasks *not* running
    """
    return [task for task in tasks if task['state'] != 'TASK_RUNNING']


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
    """Fetches mesos state from the leader.
    Raises an exception if the state doesn't look like it came from an
    elected leader, as we never want non-leader state data."""
    state = master.CURRENT.state
    if 'elected_time' not in state:
        raise MasterNotAvailableException("We asked for the current leader state, "
                                          "but it wasn't the elected leader. Please try again.")
    return state


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


def filter_out_slaves_by_constraints(slaves, constraints):
    """Takes in a list of slaves and filters them according to the
    input constraints.

    :param slaves: list of slaves. Each slave object must come from the
    mesos api so it has all the relevant attributes
    :param constraints: A marathon-like expression to filter out slaves that will
    not be used by mesos for deployment anyway.
    :returns: A list of mesos slaves
    """
    filtered_slaves = []
    for slave in slaves:
        if slave_passes_constraints(slave, constraints):
            filtered_slaves.append(slave)
    return filtered_slaves


def slave_passes_constraints(slave, constraints):
    for constraint in constraints:
        if not slave_passes_constraint(slave=slave, constraint=constraint):
            # As soon as we know a single constraint cannot be met, we can
            # exit early.
            return False
    return True


def slave_passes_constraint(slave, constraint):
    """Tests a single constraint, and returns a bool if the input slave
    passes the constraint

    :param slave: A single slave with attributes
    :param constraint: A single mesos constraint
    :returns boolean
    """
    attributes = slave['attributes']
    if constraint == [] or constraint is None:
        return True
    elif constraint[1] == "LIKE":
        test_attribute = constraint[0]
        test_regex = constraint[2]
        if test_attribute not in attributes:
            return False
        else:
            test_attribute_value = attributes[test_attribute]
        return bool(re.search(test_regex, test_attribute_value))
    elif constraint[1] == "UNLIKE":
        test_attribute = constraint[0]
        test_regex = constraint[2]
        if test_attribute not in attributes:
            return True
        else:
            test_attribute_value = attributes[test_attribute]
        return not bool(re.search(test_regex, test_attribute_value))
    else:
        return True


def get_mesos_slaves_grouped_by_attribute(attribute, constraints=[]):
    """Returns a dictionary of unique values and the corresponding hosts for a given Mesos attribute

    :param attribute: an attribute to filter
    :param constraints: A marathon-like expression to filter out slaves that will
    not be used by mesos for deployment anyway.
    :returns: a dictionary of the form {'<attribute_value>': [<list of hosts with attribute=attribute_value>]}
              (response can contain multiple 'attribute_value)
    """
    attr_map = {}
    mesos_state = fetch_mesos_state_from_leader()
    slaves = mesos_state['slaves']
    if slaves == []:
        raise NoSlavesAvailable("No mesos slaves were available to query. Try again later")
    else:
        constrained_slaves = filter_out_slaves_by_constraints(slaves=slaves, constraints=constraints)
        for slave in constrained_slaves:
            if attribute in slave['attributes']:
                attr_val = slave['attributes'][attribute]
                attr_map.setdefault(attr_val, []).append(slave['hostname'])
        return attr_map
