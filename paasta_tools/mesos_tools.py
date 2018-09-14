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
import asyncio
import datetime
import itertools
import json
import logging
import re
import socket
from collections import namedtuple
from pathlib import Path
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Collection
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union
from urllib.parse import urlparse

import a_sync
import humanize
import requests
from kazoo.client import KazooClient
from mypy_extensions import TypedDict

import paasta_tools.mesos.cluster as cluster
import paasta_tools.mesos.exceptions as mesos_exceptions
from paasta_tools.async_utils import aiter_to_list
from paasta_tools.async_utils import async_timeout
from paasta_tools.async_utils import async_ttl_cache
from paasta_tools.long_running_service_tools import host_passes_blacklist
from paasta_tools.long_running_service_tools import host_passes_whitelist
from paasta_tools.mesos.cfg import load_mesos_config
from paasta_tools.mesos.exceptions import SlaveDoesNotExist
from paasta_tools.mesos.master import MesosMaster
from paasta_tools.mesos.master import MesosState
from paasta_tools.mesos.task import Task
from paasta_tools.utils import DeployBlacklist
from paasta_tools.utils import DeployWhitelist
from paasta_tools.utils import format_table
from paasta_tools.utils import get_user_agent
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import TimeoutError

MARATHON_FRAMEWORK_NAME_PREFIX = 'marathon'

ZookeeperHostPath = namedtuple('ZookeeperHostPath', ['host', 'path'])
SlaveTaskCount = namedtuple('SlaveTaskCount', ['count', 'batch_count', 'slave'])


DEFAULT_MESOS_CLI_CONFIG_LOCATION = "/nail/etc/mesos-cli.json"

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


def get_mesos_config_path():
    """
    Determine where to find the configuration for mesos-cli.
    """
    return load_system_paasta_config(). \
        get_mesos_cli_config(). \
        get("path", DEFAULT_MESOS_CLI_CONFIG_LOCATION)


def get_mesos_config():
    return load_mesos_config(get_mesos_config_path())


def get_mesos_master(**overrides: Any) -> MesosMaster:
    config = get_mesos_config()
    for k, v in overrides.items():
        config[k] = v
    return MesosMaster(config)


MY_HOSTNAME = socket.getfqdn()
MESOS_MASTER_PORT = 5050
MESOS_SLAVE_PORT = '5051'


class MesosSlaveConnectionError(Exception):
    pass


def get_mesos_leader() -> str:
    """Get the current mesos-master leader's hostname.
    Attempts to determine this by using mesos.cli to query ZooKeeper.

    :returns: The current mesos-master hostname"""
    try:
        url = get_mesos_master().host
    except mesos_exceptions.MasterNotAvailableException:
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


def is_mesos_leader(hostname: str=MY_HOSTNAME) -> bool:
    """Check if a hostname is the current mesos leader.

    :param hostname: The hostname to query mesos-master on
    :returns: True if hostname is the mesos-master leader, False otherwise"""
    return get_mesos_leader() == hostname


def find_mesos_leader(master):
    """ Find the leader with redirect given one mesos master.
    """
    if master is None:
        raise ValueError("Mesos master is required to find leader")

    url = f"http://{master}:{MESOS_MASTER_PORT}/redirect"
    response = requests.get(url)
    return urlparse(response.url).hostname


async def get_current_tasks(job_id: str) -> List[Task]:
    """ Returns a list of all the tasks with a given job id.
    :param job_id: the job id of the tasks.
    :return tasks: a list of mesos.cli.Task.
    """
    mesos_master = get_mesos_master()
    framework_tasks = await mesos_master.tasks(fltr=job_id, active_only=False)
    return framework_tasks


def is_task_running(task: Task) -> bool:
    return task['state'] == 'TASK_RUNNING'


def filter_running_tasks(tasks: Collection[Task]) -> List[Task]:
    """ Filters those tasks where it's state is TASK_RUNNING.
    :param tasks: a list of mesos.cli.Task
    :return filtered: a list of running tasks
    """
    return [task for task in tasks if is_task_running(task)]


def filter_not_running_tasks(tasks: Collection[Task]) -> List[Task]:
    """ Filters those tasks where it's state is *not* TASK_RUNNING.
    :param tasks: a list of mesos.cli.Task
    :return filtered: a list of tasks *not* running
    """
    return [task for task in tasks if not is_task_running(task)]


async def get_running_tasks_from_frameworks(job_id=''):
    """ Will include tasks from active and completed frameworks
    but NOT orphaned tasks
    """
    active_framework_tasks = await get_current_tasks(job_id)
    running_tasks = filter_running_tasks(active_framework_tasks)
    return running_tasks


async def get_all_running_tasks() -> Collection[Task]:
    """ Will include all running tasks; for now orphans are not included
    """
    framework_tasks = await get_current_tasks('')
    mesos_master = get_mesos_master()
    framework_tasks += await mesos_master.orphan_tasks()
    running_tasks = filter_running_tasks(framework_tasks)
    return running_tasks


@async_ttl_cache(ttl=600)
async def get_cached_list_of_all_current_tasks():
    """Returns a cached list of all mesos tasks.

    This function is used by 'paasta status' and 'paasta_serviceinit status'
    to avoid re-querying mesos master and re-parsing json to get mesos.Task objects.


    The async_ttl_cache decorator caches the list for 600 seconds.
    ttl doesn't really matter for this function because when we run 'paasta status'
    the corresponding HTTP request to mesos master is cached by requests_cache.

    :return tasks: a list of mesos.Task
    """
    return await get_current_tasks('')


@async_ttl_cache(ttl=600)
async def get_cached_list_of_running_tasks_from_frameworks():
    """Returns a cached list of all running mesos tasks.
    See the docstring for get_cached_list_of_all_current_tasks().

    :return tasks: a list of mesos.Task
    """
    return [task for task in filter_running_tasks(await get_cached_list_of_all_current_tasks())]


@async_ttl_cache(ttl=600)
async def get_cached_list_of_not_running_tasks_from_frameworks():
    """Returns a cached list of mesos tasks that are NOT running.
    See the docstring for get_cached_list_of_all_current_tasks().

    :return tasks: a list of mesos.Task"""
    return [task for task in filter_not_running_tasks(await get_cached_list_of_all_current_tasks())]


def select_tasks_by_id(tasks: Collection[Task], job_id: str='') -> List[Task]:
    """Returns a list of the tasks with a given job_id.

    :param tasks: a list of mesos.Task.
    :param job_id: the job id.
    :return tasks: a list of mesos.Task.
    """
    return [task for task in tasks if job_id in task['id']]


async def get_non_running_tasks_from_frameworks(job_id: str='') -> List[Task]:
    """ Will include tasks from active and completed frameworks
    but NOT orphaned tasks
    """
    active_framework_tasks = await get_current_tasks(job_id)
    not_running_tasks = filter_not_running_tasks(active_framework_tasks)
    return not_running_tasks


async def get_short_hostname_from_task(task: Task) -> str:
    try:
        slave_hostname = (await task.slave())['hostname']
        return slave_hostname.split(".")[0]
    except (AttributeError, SlaveDoesNotExist):
        return 'Unknown'


def get_first_status_timestamp(task: Task) -> str:
    """Gets the first status timestamp from a task id and returns a human
    readable string with the local time and a humanized duration:
    ``2015-01-30T08:45 (an hour ago)``
    """
    try:
        start_time_string = task['statuses'][0]['timestamp']
        start_time = datetime.datetime.fromtimestamp(float(start_time_string))
        return "{} ({})".format(start_time.strftime("%Y-%m-%dT%H:%M"), humanize.naturaltime(start_time))
    except (IndexError, SlaveDoesNotExist):
        return "Unknown"


async def get_mem_usage(task: Task) -> str:
    try:
        task_mem_limit = await task.mem_limit()
        task_rss = await task.rss()
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


async def get_cpu_usage(task: Task) -> str:
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
        cpu_shares = (await task.cpu_limit()) - .1
        allocated_seconds = duration_seconds * cpu_shares
        task_stats = await task.stats()
        used_seconds = task_stats.get('cpus_system_time_secs', 0.0) + task_stats.get('cpus_user_time_secs', 0.0)
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


async def results_or_unknown(future: Awaitable[str]) -> str:
    try:
        return await future
    except Exception:
        return PaastaColors.red("Unknown")


async def format_running_mesos_task_row(task: Task, get_short_task_id: Callable[[str], str]) -> Tuple[str, ...]:
    """Returns a pretty formatted string of a running mesos task attributes"""

    short_task_id = get_short_task_id(task['id'])
    short_hostname_future = asyncio.ensure_future(results_or_unknown(get_short_hostname_from_task(task)))
    mem_usage_future = asyncio.ensure_future(results_or_unknown(get_mem_usage(task)))
    cpu_usage_future = asyncio.ensure_future(results_or_unknown(get_cpu_usage(task)))
    first_status_timestamp = get_first_status_timestamp(task)

    await asyncio.wait([short_hostname_future, mem_usage_future, cpu_usage_future])

    return (
        short_task_id,
        short_hostname_future.result(),
        mem_usage_future.result(),
        cpu_usage_future.result(),
        first_status_timestamp,
    )


async def format_non_running_mesos_task_row(task: Task, get_short_task_id: Callable[[str], str]) -> Tuple[str, ...]:
    """Returns a pretty formatted string of a running mesos task attributes"""
    return (
        PaastaColors.grey(get_short_task_id(task['id'])),
        PaastaColors.grey(await results_or_unknown(get_short_hostname_from_task(task))),
        PaastaColors.grey(get_first_status_timestamp(task)),
        PaastaColors.grey(task['state']),
    )


@async_timeout()
async def format_stdstreams_tail_for_task(task, get_short_task_id, nlines=10):
    """Returns the formatted "tail" of stdout/stderr, for a given a task.

    :param get_short_task_id: A function which given a
                              task_id returns a short task_id suitable for
                              printing.
    """
    error_message = PaastaColors.red("      couldn't read stdout/stderr for %s (%s)")
    output = []
    mesos_cli_config = get_mesos_config()
    try:
        fobjs = await aiter_to_list(cluster.get_files_for_tasks(
            task_list=[task],
            file_list=['stdout', 'stderr'],
            max_workers=mesos_cli_config["max_workers"],
        ))
        fobjs.sort(key=lambda fobj: fobj.path, reverse=True)
        if not fobjs:
            output.append(PaastaColors.blue("      no stdout/stderrr for %s" % get_short_task_id(task['id'])))
            return output
        for fobj in fobjs:
            output.append(PaastaColors.blue("      {} tail for {}".format(fobj.path, get_short_task_id(task['id']))))
            # read nlines, starting from EOF
            tail = []
            lines_seen = 0
            if nlines > 0:
                async for line in fobj._readlines_reverse():
                    tail.append(line)
                    lines_seen += 1
                    if lines_seen >= nlines:
                        break

            # reverse the tail, so that EOF is at the bottom again
            if tail:
                output.extend(tail[::-1])
    except (
        mesos_exceptions.MasterNotAvailableException,
        mesos_exceptions.SlaveDoesNotExist,
        mesos_exceptions.TaskNotFoundException,
        mesos_exceptions.FileNotFoundForTaskException,
    ) as e:
        output.append(error_message % (get_short_task_id(task['id']), str(e)))
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
    for i in range(len(table)):
        output.append(table[i])
        output.extend([line for line in stdstreams[i]])
    return output


async def format_task_list(
    tasks: Sequence[Task],
    list_title: str,
    table_header: Sequence[str],
    get_short_task_id: Callable[[str], str],
    format_task_row: Callable[[Task, Callable[[str], str]], Awaitable[Union[Sequence[str], str]]],
    grey: bool,
    tail_lines: int,
) -> List[str]:
    """Formats a list of tasks, returns a list of output lines
    :param tasks: List of tasks as returned by get_*_tasks_from_all_frameworks.
    :param list_title: 'Running Tasks:' or 'Non-Running Tasks'.
    :param table_header: List of column names used in the tasks table.
    :param get_short_task_id: A function which given a task_id returns a short task_id suitable for printing.
    :param format_task_row: Formatting function, works on a task and a get_short_task_id function.
    :param tail_lines (int): number of lines of stdout/stderr to tail, as obtained from the Mesos sandbox.
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
    table_rows: List[Union[str, Sequence[str]]] = [
        [colorize(th) for th in table_header],
    ]

    if tasks:
        task_row_futures = [asyncio.ensure_future(format_task_row(task, get_short_task_id)) for task in tasks]
        await asyncio.wait(task_row_futures)

        for future in task_row_futures:
            table_rows.append(future.result())

    tasks_table = ["    %s" % row for row in format_table(table_rows)]
    if tail_lines == 0:
        output.extend(tasks_table)
    else:
        stdstreams = []
        for task in tasks:
            stdstreams.append(await format_stdstreams_tail_for_task(task, get_short_task_id, nlines=tail_lines))
        output.append(tasks_table[0])  # header
        output.extend(zip_tasks_verbose_output(tasks_table[1:], stdstreams))

    return output


@a_sync.to_blocking
async def status_mesos_tasks_verbose(
    filter_string: str,
    get_short_task_id: Callable[[str], str],
    tail_lines: int=0,
) -> str:
    """Returns detailed information about the mesos tasks for a service.

    :param filter_string: An id used for looking up Mesos tasks
    :param get_short_task_id: A function which given a
                              task_id returns a short task_id suitable for
                              printing.
    :param tail_lines: int representing the number of lines of stdout/err to
                       report.
    """
    output: List[str] = []
    running_and_active_tasks = select_tasks_by_id(
        await get_cached_list_of_running_tasks_from_frameworks(),
        filter_string,
    )
    list_title = "Running Tasks:"
    table_header = [
        "Mesos Task ID",
        "Host deployed to",
        "Ram",
        "CPU",
        "Deployed at what localtime",
    ]
    output.extend(await format_task_list(
        tasks=running_and_active_tasks,
        list_title=list_title,
        table_header=table_header,
        get_short_task_id=get_short_task_id,
        format_task_row=format_running_mesos_task_row,
        grey=False,
        tail_lines=tail_lines,
    ))

    non_running_tasks = select_tasks_by_id(await get_cached_list_of_not_running_tasks_from_frameworks(), filter_string)
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
    output.extend(await format_task_list(
        tasks=non_running_tasks_ordered,
        list_title=list_title,
        table_header=table_header,
        get_short_task_id=get_short_task_id,
        format_task_row=format_non_running_mesos_task_row,
        grey=True,
        tail_lines=tail_lines,
    ))

    return "\n".join(output)


def get_local_slave_state(hostname=None):
    """Fetches mesos slave state and returns it as a dict.

    :param hostname: The host from which to fetch slave state. If not specified, defaults to the local machine."""
    if hostname is None:
        hostname = socket.getfqdn()
    stats_uri = f'http://{hostname}:{MESOS_SLAVE_PORT}/state'
    try:
        headers = {'User-Agent': get_user_agent()}
        response = requests.get(stats_uri, timeout=10, headers=headers)
        if response.status_code == 404:
            fallback_stats_uri = f'http://{hostname}:{MESOS_SLAVE_PORT}/state.json'
            response = requests.get(fallback_stats_uri, timeout=10, headers=headers)
    except requests.ConnectionError as e:
        raise MesosSlaveConnectionError(
            'Could not connect to the mesos slave to see which services are running\n'
            'on %s. Is the mesos-slave running?\n'
            'Error was: %s\n' % (e.request.url, str(e)),
        )
    response.raise_for_status()
    return json.loads(response.text)


async def get_mesos_quorum():
    """Returns the configured quorum size."""
    return int((await get_master_flags())['flags']['quorum'])


MesosResources = Mapping[str, Any]


class MesosTask(TypedDict):
    resources: MesosResources
    slave_id: str
    id: str
    state: str


def get_all_tasks_from_state(
    mesos_state: MesosState,
    include_orphans: bool = False,
) -> Sequence[MesosTask]:
    """Given a mesos state, find the tasks from all frameworks.
    :param mesos_state: the mesos_state
    :returns: a list of tasks
    """
    tasks = [task for framework in mesos_state.get('frameworks', []) for task in framework.get('tasks', [])]
    if include_orphans:
        tasks += mesos_state.get('orphan_tasks', [])
    return tasks


async def get_master_flags():
    res = await get_mesos_master().fetch("/master/flags")
    return await res.json()


def get_zookeeper_host_path():
    zk_url = 'zk://%s' % load_system_paasta_config().get_zk_hosts()
    parsed = urlparse(zk_url)
    return ZookeeperHostPath(host=parsed.netloc, path=parsed.path)


def get_zookeeper_config(state):
    """Returns dict, containing the zookeeper hosts and path.
    :param state: mesos state dictionary"""
    re_zk = re.match(r"^zk://([^/]*)/(.*)$", state['flags']['zk'])
    return {'hosts': re_zk.group(1), 'path': re_zk.group(2)}


def get_number_of_mesos_masters(host, path):
    """Returns an array, containing mesos masters
    :param zk_config: dict containing information about zookeeper config.
    Masters register themselves in zookeeper by creating ``info_`` entries.
    We count these entries to get the number of masters.
    """
    zk = KazooClient(hosts=host, read_only=True)
    zk.start()
    try:
        root_entries = zk.get_children(path)
        result = [
            info
            for info in root_entries
            if info.startswith('json.info_') or info.startswith('info_')
        ]
        return len(result)
    finally:
        zk.stop()
        zk.close()


def get_all_slaves_for_blacklist_whitelist(blacklist: DeployBlacklist, whitelist: DeployWhitelist):
    """
    A wrapper function to get all slaves and filter according to
    provided blacklist and whitelist.

    :param blacklist: a blacklist, used to filter mesos slaves by attribute
    :param whitelist: a whitelist, used to filter mesos slaves by attribute

    :returns: a list of mesos slave objects, filtered by those which are acceptable
    according to the provided blacklist and whitelists.
    """
    all_slaves = get_slaves()
    return filter_mesos_slaves_by_blacklist(all_slaves, blacklist, whitelist)


def get_mesos_slaves_grouped_by_attribute(slaves, attribute):
    """Returns a dictionary of unique values and the corresponding hosts for a given Mesos attribute

    :param slaves: a list of mesos slaves to group
    :param attribute: an attribute to filter
    :returns: a dictionary of the form {'<attribute_value>': [<list of hosts with attribute=attribute_value>]}
              (response can contain multiple 'attribute_value)
    """
    sorted_slaves = sorted(
        slaves,
        key=lambda slave: slave['attributes'].get(attribute),
    )
    return {
        key: list(group) for key, group in itertools.groupby(
            sorted_slaves,
            key=lambda slave: slave['attributes'].get(attribute),
        ) if key
    }


# TODO: remove to_blocking, convert call sites (smartstack_tools and marathon_serviceinit) to asyncio.
@a_sync.to_blocking
async def get_slaves():
    return (await (await get_mesos_master().fetch("/master/slaves")).json())['slaves']


def filter_mesos_slaves_by_blacklist(slaves, blacklist: DeployBlacklist, whitelist: DeployWhitelist):
    """Takes an input list of slaves and filters them based on the given blacklist.
    The blacklist is in the form of:

        [["location_type", "location]]

    Where the list inside is something like ["region", "uswest1-prod"]

    :returns: The list of mesos slaves after the filter
    """
    filtered_slaves = []
    for slave in slaves:
        if host_passes_blacklist(
            slave['attributes'], blacklist,
        ) and host_passes_whitelist(
            slave['attributes'], whitelist,
        ):
            filtered_slaves.append(slave)
    return filtered_slaves


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


async def get_mesos_task_count_by_slave(
    mesos_state: MesosState,
    slaves_list: Sequence[Dict]=None,
    pool: Optional[str]=None,
) -> List[Dict]:
    """Get counts of running tasks per mesos slave. Also include separate count of chronos tasks

    :param mesos_state: mesos state dict
    :param slaves_list: a list of slave dicts to count running tasks for.
    :param pool: pool of slaves to return (None means all)
    :returns: list of slave dicts {'task_count': SlaveTaskCount}
    """
    all_mesos_tasks = await get_all_running_tasks()  # empty string = all app ids
    slaves = {
        slave['id']: {'count': 0, 'slave': slave, 'batch_count': 0} for slave in mesos_state.get('slaves', [])
    }
    for task in all_mesos_tasks:
        try:
            task_slave = await task.slave()
            if task_slave['id'] not in slaves:
                log.debug("Slave {} not found for task".format(task_slave['id']))
                continue
            else:
                slaves[task_slave['id']]['count'] += 1
                task_framework = await task.framework()
                log.debug(f"Task framework: {task_framework.name}")
                # Marathon is only framework that runs service. Others are batch.
                if not task_framework.name.startswith(MARATHON_FRAMEWORK_NAME_PREFIX):
                    slaves[task_slave['id']]['batch_count'] += 1
        except SlaveDoesNotExist:
            log.debug("Tried to get mesos slaves for task {}, but none existed.".format(task['id']))
            continue
    if slaves_list:
        for slave in slaves_list:
            slave['task_counts'] = SlaveTaskCount(**slaves[slave['task_counts'].slave['id']])
        slaves_with_counts = list(slaves_list)
    elif pool:
        slaves_with_counts = [
            {'task_counts': SlaveTaskCount(**slave_counts)} for slave_counts in slaves.values()
            if slave_counts['slave']['attributes'].get('pool', 'default') == pool
        ]
    else:
        slaves_with_counts = [{'task_counts': SlaveTaskCount(**slave_counts)} for slave_counts in slaves.values()]
    for slave in slaves_with_counts:
        log.debug("Slave: {}, running {} tasks, "
                  "including {} chronos tasks".format(
                      slave['task_counts'].slave['hostname'],
                      slave['task_counts'].count,
                      slave['task_counts'].batch_count,
                  ))
    return slaves_with_counts


def get_count_running_tasks_on_slave(hostname: str) -> int:
    """Return the number of tasks running on a particular slave
    or 0 if the slave is not found.
    :param hostname: hostname of the slave
    :returns: integer count of mesos tasks"""
    mesos_state = a_sync.block(get_mesos_master().state_summary)
    task_counts = a_sync.block(get_mesos_task_count_by_slave, mesos_state)
    counts = [slave['task_counts'].count for slave in task_counts if slave['task_counts'].slave['hostname'] == hostname]
    if counts:
        return counts[0]
    else:
        return 0


def slave_pid_to_ip(slave_pid: str) -> str:
    """Convert slave_pid to IP

    :param: slave pid e.g. slave(1)@10.40.31.172:5051
    :returns: ip address"""
    regex = re.compile(r'.+?@([\d\.]+):\d+')
    return regex.match(slave_pid).group(1)


async def list_framework_ids(active_only=False):
    return [f.id for f in await get_mesos_master().frameworks(active_only=active_only)]


@a_sync.to_blocking
async def get_all_frameworks(active_only=False):
    return await get_mesos_master().frameworks(active_only=active_only)


def terminate_framework(framework_id):
    resp = requests.post(
        'http://%s:%d/master/teardown' % (get_mesos_leader(), MESOS_MASTER_PORT),
        data={'frameworkId': framework_id},
    )
    resp.raise_for_status()


async def get_tasks_from_app_id(app_id, slave_hostname=None):
    tasks = await get_running_tasks_from_frameworks(app_id)
    if slave_hostname:
        tasks = [task for task in tasks if await filter_task_by_hostname(task, slave_hostname)]
    return tasks


async def get_task(
    task_id: str,
    app_id: str = '',
) -> MesosTask:
    tasks = await get_running_tasks_from_frameworks(app_id)
    tasks = [task for task in tasks if filter_task_by_task_id(task, task_id)]
    if len(tasks) < 1:
        raise TaskNotFound(f"Couldn't find task for given id: {task_id}")
    if len(tasks) > 1:
        raise TooManyTasks(f"Found more than one task with id: {task_id}, this should not happen!")
    return tasks[0]


def filter_task_by_task_id(
    task: MesosTask,
    task_id: str,
) -> bool:
    return task['id'] == task_id


async def filter_task_by_hostname(task, hostname):
    return (await task.slave())['hostname'].startswith(hostname)


class TaskNotFound(Exception):
    pass


class TooManyTasks(Exception):
    pass


# TODO: async this
def mesos_services_running_here(framework_filter, parse_service_instance_from_executor_id, hostname=None):
    """See what paasta_native services are being run by a mesos-slave on this host.

    :param framework_filter: a function that returns true if we should consider a given framework.
    :param parse_service_instance_from_executor_id: A function that returns a tuple of (service, instance) from the
                                                    executor ID.
    :param hostname: Hostname to fetch mesos slave state from. See get_local_slave_state.

    :returns: A list of triples of (service, instance, port)"""
    slave_state = get_local_slave_state(hostname=hostname)
    frameworks = [fw for fw in slave_state.get('frameworks', []) if framework_filter(fw)]
    executors = [ex for fw in frameworks for ex in fw.get('executors', [])
                 if 'TASK_RUNNING' in [t['state'] for t in ex.get('tasks', [])]]
    srv_list = []
    for executor in executors:
        srv_name, srv_instance = parse_service_instance_from_executor_id(executor['id'])
        if 'ports' in executor['resources']:
            srv_port = int(re.findall('[0-9]+', executor['resources']['ports'])[0])
        else:
            srv_port = None
        srv_list.append((srv_name, srv_instance, srv_port))
    return srv_list


def is_task_terminal(
    task: MesosTask,
) -> bool:
    """Return whether a given mesos task is terminal.

    Terminal states are documented in
    http://mesos.apache.org/api/latest/java/org/apache/mesos/Protos.TaskState.html

    :param task: the task to be inspected
    :returns: a boolean indicating if the task is considered to be in a terminal state
    """
    return task['state'] in ['TASK_ERROR', 'TASK_KILLED', 'TASK_FAILED', 'TASK_FINISHED']


def is_mesos_available() -> bool:
    return Path(get_mesos_config_path()).exists()
