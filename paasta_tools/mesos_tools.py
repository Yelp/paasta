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

import datetime
import json
import os
import re
import requests
import socket

import humanize
from kazoo.client import KazooClient
from mesos.cli.exceptions import SlaveDoesNotExist

from paasta_tools.utils import format_table
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import timeout
from paasta_tools.utils import TimeoutError


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
    return master.CURRENT.tasks(fltr=job_id, active_only=False)


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


def get_running_tasks_from_active_frameworks(job_id):
    active_framework_tasks = get_current_tasks(job_id)
    running_tasks = filter_running_tasks(active_framework_tasks)
    return running_tasks


def get_non_running_tasks_from_active_frameworks(job_id):
    active_framework_tasks = get_current_tasks(job_id)
    not_running_tasks = filter_not_running_tasks(active_framework_tasks)
    return not_running_tasks


def get_short_hostname_from_task(task):
    try:
        slave_hostname = task.slave['hostname']
        return slave_hostname.split(".")[0]
    except (AttributeError, SlaveDoesNotExist):
        return 'Unknown'


def get_first_status_timestamp(task):
    """Gets the first status timestamp from a task id and returns a human
    readable string with the local time and a humanized duration:
    ``2015-01-30T08:45 (an hour ago)``
    """
    try:
        start_time_string = task['statuses'][0]['timestamp']
        start_time = datetime.datetime.fromtimestamp(float(start_time_string))
        return "%s (%s)" % (start_time.strftime("%Y-%m-%dT%H:%M"), humanize.naturaltime(start_time))
    except (IndexError, SlaveDoesNotExist):
        return "Unknown"


@timeout()
def get_mem_usage(task):
    try:
        task_mem_limit = task.mem_limit
        task_rss = task.rss
        if task_mem_limit == 0:
            return "Undef"
        mem_percent = task_rss / task_mem_limit * 100
        mem_string = "%d/%dMB" % ((task_rss / 1024 / 1024), (task_mem_limit / 1024 / 1024))
        if mem_percent > 90:
            return PaastaColors.red(mem_string)
        else:
            return mem_string
    except (AttributeError, SlaveDoesNotExist):
        return "None"
    except TimeoutError:
        return "Timed Out"


@timeout()
def get_cpu_usage(task):
    """Calculates a metric of used_cpu/allocated_cpu
    To do this, we take the total number of cpu-seconds the task has consumed,
    (the sum of system and user time), OVER the total cpu time the task
    has been allocated.

    The total time a task has been allocated is the total time the task has
    been running (https://github.com/mesosphere/mesos/blob/0b092b1b0/src/webui/master/static/js/controllers.js#L140)
    multiplied by the "shares" a task has.
    """
    try:
        start_time = round(task['statuses'][0]['timestamp'])
        current_time = int(datetime.datetime.now().strftime('%s'))
        duration_seconds = current_time - start_time
        # The CPU shares has an additional .1 allocated to it for executor overhead.
        # We subtract this to the true number
        # (https://github.com/apache/mesos/blob/dc7c4b6d0bcf778cc0cad57bb108564be734143a/src/slave/constants.hpp#L100)
        cpu_shares = task.cpu_limit - .1
        allocated_seconds = duration_seconds * cpu_shares
        used_seconds = task.stats.get('cpus_system_time_secs', 0.0) + task.stats.get('cpus_user_time_secs', 0.0)
        if allocated_seconds == 0:
            return "Undef"
        percent = round(100 * (used_seconds / allocated_seconds), 1)
        percent_string = "%s%%" % percent
        if percent > 90:
            return PaastaColors.red(percent_string)
        else:
            return percent_string
    except (AttributeError, SlaveDoesNotExist):
        return "None"
    except TimeoutError:
        return "Timed Out"


def format_running_mesos_task_row(task, get_short_task_id):
    """Returns a pretty formatted string of a running mesos task attributes"""
    return (
        get_short_task_id(task['id']),
        get_short_hostname_from_task(task),
        get_mem_usage(task),
        get_cpu_usage(task),
        get_first_status_timestamp(task),
    )


def format_non_running_mesos_task_row(task, get_short_task_id):
    """Returns a pretty formatted string of a running mesos task attributes"""
    return (
        PaastaColors.grey(get_short_task_id(task['id'])),
        PaastaColors.grey(get_short_hostname_from_task(task)),
        PaastaColors.grey(get_first_status_timestamp(task)),
        PaastaColors.grey(task['state']),
    )


def status_mesos_tasks_verbose(job_id, get_short_task_id):
    """Returns detailed information about the mesos tasks for a service.

    :param job_id: An id used for looking up Mesos tasks
    :param get_short_task_id: A function which given a
                              task_id returns a short task_id suitable for
                              printing.
    """
    output = []
    running_and_active_tasks = get_running_tasks_from_active_frameworks(job_id)
    output.append("  Running Tasks:")
    rows_running = [[
        "Mesos Task ID",
        "Host deployed to",
        "Ram",
        "CPU",
        "Deployed at what localtime"
    ]]
    for task in running_and_active_tasks:
        rows_running.append(format_running_mesos_task_row(task, get_short_task_id))
    output.extend(["    %s" % row for row in format_table(rows_running)])

    non_running_tasks = reversed(get_non_running_tasks_from_active_frameworks(job_id)[-10:])
    output.append(PaastaColors.grey("  Non-Running Tasks"))
    rows_non_running = [[
        PaastaColors.grey("Mesos Task ID"),
        PaastaColors.grey("Host deployed to"),
        PaastaColors.grey("Deployed at what localtime"),
        PaastaColors.grey("Status"),
    ]]
    for task in non_running_tasks:
        rows_non_running.append(format_non_running_mesos_task_row(task, get_short_task_id))
    output.extend(["    %s" % row for row in format_table(rows_non_running)])

    return "\n".join(output)


def get_mesos_stats():
    """Queries the mesos stats api and returns a dictionary of the results"""
    response = master.CURRENT.fetch('metrics/snapshot')
    response.raise_for_status()
    return response.json()


def get_local_slave_state():
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


def get_mesos_state_from_leader():
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
    Masters register themselves in zookeeper by creating ``info_`` entries.
    We count these entries to get the number of masters.
    """
    zk = KazooClient(hosts=zk_config['hosts'], read_only=True)
    zk.start()
    root_entries = zk.get_children(zk_config['path'])
    result = [info for info in root_entries if info.startswith('info_')]
    zk.stop()
    return len(result)


def get_mesos_slaves_grouped_by_attribute(attribute, blacklist=None):
    """Returns a dictionary of unique values and the corresponding hosts for a given Mesos attribute

    :param attribute: an attribute to filter
    :param blacklist: a list of [attribute, value] lists to exclude from the output list
    :returns: a dictionary of the form {'<attribute_value>': [<list of hosts with attribute=attribute_value>]}
              (response can contain multiple 'attribute_value)
    """
    if blacklist is None:
        blacklist = []
    attr_map = {}
    mesos_state = get_mesos_state_from_leader()
    slaves = mesos_state['slaves']
    filtered_slaves = filter_mesos_slaves_by_blacklist(slaves=slaves, blacklist=blacklist)
    if filtered_slaves == []:
        raise NoSlavesAvailable("No mesos slaves were available to query. Try again later")
    else:
        for slave in filtered_slaves:
            if attribute in slave['attributes']:
                attr_val = slave['attributes'][attribute]
                attr_map.setdefault(attr_val, []).append(slave['hostname'])
        return attr_map


def filter_mesos_slaves_by_blacklist(slaves, blacklist):
    """Takes an input list of slaves and filters them based on the given blacklist.
    The blacklist is in the form of:

        [["location_type", "location]]

    Where the list inside is something like ["region", "uswest1-prod"]

    :returns: The list of mesos slaves after the filter
    """
    filtered_slaves = []
    for slave in slaves:
        if slave_passes_blacklist(slave, blacklist):
            filtered_slaves.append(slave)
    return filtered_slaves


def slave_passes_blacklist(slave, blacklist):
    """
    :param slave: A single mesos slave with attributes
    :param blacklist: A list of lists like [["location_type", "location"], ["foo", "bar"]]
    :returns: boolean, True if the slave gets passed the blacklist
    """
    attributes = slave['attributes']
    for location_type, location in blacklist:
        if attributes.get(location_type) == location:
            return False
    return True
