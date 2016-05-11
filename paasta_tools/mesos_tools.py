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
import datetime
import json
import logging
import os
import re
import socket
from urlparse import urlparse

import humanize
import requests
from kazoo.client import KazooClient
from mesos.cli import util
from mesos.cli.exceptions import SlaveDoesNotExist

from paasta_tools.utils import format_table
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import timeout
from paasta_tools.utils import TimeoutError


log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


# mesos.cli.master reads its config file at *import* time, so we must have
# this environment variable set and ready to go at that time so we can
# read in the config for zookeeper, etc
if 'MESOS_CLI_CONFIG' not in os.environ:
    os.environ['MESOS_CLI_CONFIG'] = '/nail/etc/mesos-cli.json'


class MasterNotAvailableException(Exception):
    pass


class SlaveNotAvailableException(Exception):
    pass


class NoSlavesAvailable(Exception):
    pass


class TaskNotFoundException(Exception):
    pass


class FileNotFoundForTaskException(Exception):
    pass


def raise_cli_exception(msg):
    if msg.startswith("unable to connect to a master"):
        raise MasterNotAvailableException(msg)
    if msg.startswith("Slave no longer exists"):
        raise SlaveNotAvailableException(msg)
    if msg.startswith("Cannot find a task by that name"):
        raise TaskNotFoundException(msg)
    if msg.startswith("No such task has the requested file or directory"):
        raise FileNotFoundForTaskException(msg)
    else:
        raise Exception(msg)

# monkey patch the log.fatal method to raise an exception rather than a sys.exit
import mesos.cli.log  # noqa
mesos.cli.log.fatal = lambda msg, code = 1: raise_cli_exception(msg)


MY_HOSTNAME = socket.getfqdn()
MESOS_MASTER_PORT = 5050
MESOS_SLAVE_PORT = '5051'
from mesos.cli import master  # noqa
import mesos.cli.cluster  # noqa


# monkey patch MesosMaster.state to use a larger ttl
@util.CachedProperty(ttl=60)
def fetch_state(self):
    return master.CURRENT.fetch("/master/state.json").json()
master.MesosMaster.state = fetch_state

# Works around a mesos-cli bug ('MesosSlave' object has no attribute 'id' - PAASTA-4119).
# The method below gets a slave by its ID. Original code here:
# https://github.com/mesosphere/mesos-cli/blob/master/mesos/cli/master.py#L176
# uses "in" instead of "==", matching when the wanted ID is a substring of
# the candidate. Because of that, multiple slaves are returned and mesos-cli
# finds itself in a buggy "if" branch (hence the AttributeError).
import itertools  # noqa


def _mesos_cli_master_MesosMaster_slaves(self, fltr=''):
    return list(map(
        lambda x: mesos.cli.slave.MesosSlave(x),
        itertools.ifilter(
            lambda x: fltr == x['id'], self.state['slaves'])))
mesos.cli.master.MesosMaster.slaves = _mesos_cli_master_MesosMaster_slaves


class MesosMasterConnectionError(Exception):
    pass


class MesosSlaveConnectionError(Exception):
    pass


def get_mesos_leader():
    """Get the current mesos-master leader's hostname.
    Attempts to determine this by using mesos.cli to query ZooKeeper.

    :returns: The current mesos-master hostname"""
    try:
        url = master.CURRENT.host
    except MesosMasterConnectionError:
        log.debug('mesos.cli failed to provide the master host')
        raise
    log.debug("mesos.cli thinks the master host is: %s" % url)
    hostname = urlparse(url).hostname
    log.debug("The parsed master hostname is: %s" % hostname)
    # This check is necessary, as if we parse a value such as 'localhost:5050',
    # it won't have a hostname attribute
    if hostname:
        try:
            host = socket.gethostbyaddr(hostname)[0]
            fqdn = socket.getfqdn(host)
        except (socket.error, socket.herror, socket.gaierror, socket.timeout):
            log.debug("Failed to convert mesos leader hostname to fqdn!")
            raise
        log.debug("Mesos Leader: %s" % fqdn)
        return fqdn
    else:
        raise ValueError('Expected to receive a valid URL, got: %s' % url)


def is_mesos_leader(hostname=MY_HOSTNAME):
    """Check if a hostname is the current mesos leader.

    :param hostname: The hostname to query mesos-master on
    :returns: True if hostname is the mesos-master leader, False otherwise"""
    return get_mesos_leader() == hostname


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


def get_running_tasks_from_active_frameworks(job_id=''):
    active_framework_tasks = get_current_tasks(job_id)
    running_tasks = filter_running_tasks(active_framework_tasks)
    return running_tasks


def get_non_running_tasks_from_active_frameworks(job_id=''):
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


@timeout()
def format_stdstreams_tail_for_task(task, get_short_task_id, nlines=10):
    """Returns the formatted "tail" of stdout/stderr, for a given a task.

    :param get_short_task_id: A function which given a
                              task_id returns a short task_id suitable for
                              printing.
    """
    error_message = PaastaColors.red("      couldn't read stdout/stderr for %s (%s)")
    output = []
    try:
        fobjs = list(mesos.cli.cluster.files(lambda x: x, flist=['stdout', 'stderr'], fltr=task['id']))
        fobjs.sort(key=lambda fobj: fobj.path, reverse=True)
        if not fobjs:
            output.append(PaastaColors.blue("      no stdout/stderrr for %s" % get_short_task_id(task['id'])))
            return output
        for fobj in fobjs:
            output.append(PaastaColors.blue("      %s tail for %s" % (fobj.path, get_short_task_id(task['id']))))
            # read nlines, starting from EOF
            # mesos.cli is smart and can efficiently read a file backwards
            reversed_file = reversed(fobj)
            tail = []
            for _ in xrange(nlines):
                line = next(reversed_file, None)
                if line is None:
                    break
                tail.append(line)
            # reverse the tail, so that EOF is at the bottom again
            if tail:
                output.extend(tail[::-1])
            output.append(PaastaColors.blue("      %s EOF" % fobj.path))
    except (MasterNotAvailableException,
            SlaveNotAvailableException,
            TaskNotFoundException,
            FileNotFoundForTaskException) as e:
        output.append(error_message % (get_short_task_id(task['id']), e.message))
    except TimeoutError:
        output.append(error_message % (get_short_task_id(task['id']), 'timeout'))
    return output


def zip_tasks_verbose_output(table, stdstreams):
    """Zip a list of strings (table) with a list of lists (stdstreams)
    :param table: a formatted list of tasks
    :param stdstreams: for each task, a list of lines from stdout/stderr tail
    """
    if len(table) != len(stdstreams):
        raise ValueError('Can only zip same-length lists')
    output = []
    for i in xrange(len(table)):
        output.append(table[i])
        output.extend([line for line in stdstreams[i]])
    return output


def format_task_list(tasks, list_title, table_header, get_short_task_id, format_task_row, grey, tail_stdstreams):
    """Formats a list of tasks, returns a list of output lines
    :param tasks: List of tasks as returned by get_*_tasks_from_active_frameworks.
    :param list_title: 'Running Tasks:' or 'Non-Running Tasks'.
    :param table_header: List of column names used in the tasks table.
    :param get_short_task_id: A function which given a
                              task_id returns a short task_id suitable for
                              printing.
    :param format_task_row: Formatting function, works on a task and a get_short_task_id function.
    :param tail_stdstreams: If True, also display the stdout/stderr tail,
                            as obtained from the Mesos sandbox.
    :param grey: If True, the list will be made less visually prominent.
    :return output: Formatted output (list of output lines).
    """
    if not grey:
        def colorize(x):
            return(x)
    else:
        def colorize(x):
            return(PaastaColors.grey(x))
    output = []
    output.append(colorize("  %s" % list_title))
    table_rows = [
        [colorize(th) for th in table_header]
    ]
    for task in tasks:
        table_rows.append(format_task_row(task, get_short_task_id))
    tasks_table = ["    %s" % row for row in format_table(table_rows)]
    if not tail_stdstreams:
        output.extend(tasks_table)
    else:
        stdstreams = []
        for task in tasks:
            stdstreams.append(format_stdstreams_tail_for_task(task, get_short_task_id))
        output.append(tasks_table[0])  # header
        output.extend(zip_tasks_verbose_output(tasks_table[1:], stdstreams))

    return output


def status_mesos_tasks_verbose(job_id, get_short_task_id, tail_stdstreams=False):
    """Returns detailed information about the mesos tasks for a service.

    :param job_id: An id used for looking up Mesos tasks
    :param get_short_task_id: A function which given a
                              task_id returns a short task_id suitable for
                              printing.
    :param tail_stdstreams: If True, also display the stdout/stderr tail,
                            as obtained from the Mesos sandbox.
    """
    output = []
    running_and_active_tasks = get_running_tasks_from_active_frameworks(job_id)
    list_title = "Running Tasks:"
    table_header = [
        "Mesos Task ID",
        "Host deployed to",
        "Ram",
        "CPU",
        "Deployed at what localtime"
    ]
    output.extend(format_task_list(
        running_and_active_tasks,
        list_title,
        table_header,
        get_short_task_id,
        format_running_mesos_task_row,
        False,
        tail_stdstreams
    ))

    non_running_tasks = get_non_running_tasks_from_active_frameworks(job_id)
    # Order the tasks by timestamp
    non_running_tasks.sort(key=lambda task: get_first_status_timestamp(task))
    non_running_tasks_ordered = list(reversed(non_running_tasks[-10:]))

    list_title = "Non-Running Tasks"
    table_header = [
        "Mesos Task ID",
        "Host deployed to",
        "Deployed at what localtime",
        "Status",
    ]
    output.extend(format_task_list(
        non_running_tasks_ordered,
        list_title,
        table_header,
        get_short_task_id,
        format_non_running_mesos_task_row,
        True,
        tail_stdstreams
    ))

    return "\n".join(output)


def get_mesos_stats():
    """Queries the mesos stats api and returns a dictionary of the results"""
    response = master.CURRENT.fetch('metrics/snapshot')
    response.raise_for_status()
    return response.json()


def get_local_slave_state():
    """Fetches mesos slave state and returns it as a dict."""
    hostname = socket.getfqdn()
    stats_uri = 'http://%s:%s/state' % (hostname, MESOS_SLAVE_PORT)
    try:
        response = requests.get(stats_uri, timeout=10)
        if response.status_code == 404:
            fallback_stats_uri = 'http://%s:%s/state.json' % (hostname, MESOS_SLAVE_PORT)
            response = requests.get(fallback_stats_uri, timeout=10)
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
    result = [info for info in root_entries if info.startswith('json.info_') or info.startswith('info_')]
    zk.stop()
    return len(result)


def get_mesos_slaves_grouped_by_attribute(attribute, blacklist=None, whitelist=None):
    """Returns a dictionary of unique values and the corresponding hosts for a given Mesos attribute

    :param attribute: an attribute to filter
    :param blacklist: a list of [attribute, value] lists to exclude from the output list
    :returns: a dictionary of the form {'<attribute_value>': [<list of hosts with attribute=attribute_value>]}
              (response can contain multiple 'attribute_value)
    """
    if blacklist is None:
        blacklist = []
    if whitelist is None:
        whitelist = []
    attr_map = {}
    mesos_state = get_mesos_state_from_leader()
    slaves = mesos_state['slaves']
    filtered_slaves = filter_mesos_slaves_by_blacklist(slaves=slaves, blacklist=blacklist, whitelist=whitelist)
    if filtered_slaves == []:
        raise NoSlavesAvailable("No mesos slaves were available to query. Try again later")
    else:
        for slave in filtered_slaves:
            if attribute in slave['attributes']:
                attr_val = slave['attributes'][attribute]
                attr_map.setdefault(attr_val, []).append(slave['hostname'])
        return attr_map


def filter_mesos_slaves_by_blacklist(slaves, blacklist, whitelist):
    """Takes an input list of slaves and filters them based on the given blacklist.
    The blacklist is in the form of:

        [["location_type", "location]]

    Where the list inside is something like ["region", "uswest1-prod"]

    :returns: The list of mesos slaves after the filter
    """
    filtered_slaves = []
    for slave in slaves:
        if slave_passes_blacklist(slave, blacklist) and slave_passes_whitelist(slave, whitelist):
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


def slave_passes_whitelist(slave, whitelist):
    """
    :param slave: A single mesos slave with attributes.
    :param whitelist: A list of lists like ["location_type", ["location1", 'location2']]
    :returns: boolean, True if the slave gets past the whitelist
    """
    # No whitelist, so disable whitelisting behaviour.
    if len(whitelist) == 0:
        return True
    attributes = slave["attributes"]
    (location_type, locations) = whitelist
    if attributes.get(location_type) in locations:
        return True
    return False


def get_container_id_for_mesos_id(client, mesos_task_id):
    running_containers = client.containers()

    container_id = None
    for container in running_containers:
        info = client.inspect_container(container)
        if info['Config']['Env']:
            for env_var in info['Config']['Env']:
                if ('MESOS_TASK_ID=%s' % mesos_task_id) in env_var:
                    container_id = info['Id']
                    break

    return container_id


def get_mesos_id_from_container(container, client):
    mesos_id = None
    info = client.inspect_container(container)
    if info['Config']['Env']:
        for env_var in info['Config']['Env']:
            # In marathon it is like this
            if 'MESOS_TASK_ID=' in env_var:
                mesos_id = re.match("MESOS_TASK_ID=(.*)", env_var).group(1)
                break
            # Chronos it is like this?
            if 'mesos_task_id=' in env_var:
                mesos_id = re.match("mesos_task_id=(.*)", env_var).group(1)
                break
    return mesos_id


def get_mesos_network_for_net(net):
    docker_mesos_net_mapping = {
        'none': 'NONE',
        'bridge': 'BRIDGE',
        'host': 'HOST',
    }
    return docker_mesos_net_mapping.get(net, net)
