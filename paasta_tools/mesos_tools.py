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
import logging
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Collection
from typing import Dict
from typing import List
from typing import MutableMapping
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union
from urllib.parse import urlparse

import a_sync
import humanize
import requests

import paasta_tools.mesos.cluster as cluster
import paasta_tools.mesos.exceptions as mesos_exceptions
from paasta_tools.async_utils import aiter_to_list
from paasta_tools.async_utils import async_timeout
from paasta_tools.async_utils import async_ttl_cache
from paasta_tools.mesos.cfg import load_mesos_config
from paasta_tools.mesos.exceptions import SlaveDoesNotExist
from paasta_tools.mesos.master import MesosMaster
from paasta_tools.mesos.task import Task
from paasta_tools.utils import format_table
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import TimeoutError


DEFAULT_MESOS_CLI_CONFIG_LOCATION = "/nail/etc/mesos-cli.json"

TERMINAL_STATES = (
    "TASK_ERROR",
    "TASK_KILLED",
    "TASK_FAILED",
    "TASK_FINISHED",
    "TASK_DROPPED",
    "TASK_GONE",
    "TASK_GONE_BY_OPERATOR",
)

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


def get_mesos_config_path(
    system_paasta_config: Optional[SystemPaastaConfig] = None,
) -> str:
    """
    Determine where to find the configuration for mesos-cli.
    """
    if system_paasta_config is None:
        system_paasta_config = load_system_paasta_config()

    return system_paasta_config.get_mesos_cli_config().get(
        "path", DEFAULT_MESOS_CLI_CONFIG_LOCATION
    )


def get_mesos_config(mesos_config_path: Optional[str] = None) -> Dict:
    if mesos_config_path is None:
        mesos_config_path = get_mesos_config_path()
    return load_mesos_config(mesos_config_path)


def get_mesos_master(
    mesos_config_path: Optional[str] = None, **overrides: Any
) -> MesosMaster:
    config = get_mesos_config(mesos_config_path)
    for k, v in overrides.items():
        config[k] = v
    return MesosMaster(config)


MESOS_MASTER_PORT = 5050


class MesosTailLines(NamedTuple):
    stdout: List[str]
    stderr: List[str]
    error_message: str


class MesosLeaderUnavailable(Exception):
    pass


def find_mesos_leader(cluster):
    """Find the leader with redirect given one mesos master."""
    master = (
        load_system_paasta_config().get_cluster_fqdn_format().format(cluster=cluster)
    )
    if master is None:
        raise ValueError("Mesos master is required to find leader")

    url = f"http://{master}:{MESOS_MASTER_PORT}/redirect"
    try:
        # Timeouts here are for connect, read
        response = requests.get(url, timeout=(5, 30))
    except Exception as e:
        raise MesosLeaderUnavailable(e)
    hostname = urlparse(response.url).hostname
    return f"{hostname}:{MESOS_MASTER_PORT}"


async def get_current_tasks(job_id: str) -> List[Task]:
    """Returns a list of all the tasks with a given job id.
    :param job_id: the job id of the tasks.
    :return tasks: a list of mesos.cli.Task.
    """
    mesos_master = get_mesos_master()
    framework_tasks = await mesos_master.tasks(fltr=job_id, active_only=False)
    return framework_tasks


def is_task_running(task: Task) -> bool:
    return task["state"] == "TASK_RUNNING"


def filter_running_tasks(tasks: Collection[Task]) -> List[Task]:
    """Filters those tasks where it's state is TASK_RUNNING.
    :param tasks: a list of mesos.cli.Task
    :return filtered: a list of running tasks
    """
    return [task for task in tasks if is_task_running(task)]


def filter_not_running_tasks(tasks: Collection[Task]) -> List[Task]:
    """Filters those tasks where it's state is *not* TASK_RUNNING.
    :param tasks: a list of mesos.cli.Task
    :return filtered: a list of tasks *not* running
    """
    return [task for task in tasks if not is_task_running(task)]


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
    return await get_current_tasks("")


@async_ttl_cache(ttl=600)
async def get_cached_list_of_running_tasks_from_frameworks():
    """Returns a cached list of all running mesos tasks.
    See the docstring for get_cached_list_of_all_current_tasks().

    :return tasks: a list of mesos.Task
    """
    return [
        task
        for task in filter_running_tasks(await get_cached_list_of_all_current_tasks())
    ]


@async_ttl_cache(ttl=600)
async def get_cached_list_of_not_running_tasks_from_frameworks():
    """Returns a cached list of mesos tasks that are NOT running.
    See the docstring for get_cached_list_of_all_current_tasks().

    :return tasks: a list of mesos.Task"""
    return [
        task
        for task in filter_not_running_tasks(
            await get_cached_list_of_all_current_tasks()
        )
    ]


def select_tasks_by_id(tasks: Collection[Task], job_id: str = "") -> List[Task]:
    """Returns a list of the tasks with a given job_id.

    :param tasks: a list of mesos.Task.
    :param job_id: the job id.
    :return tasks: a list of mesos.Task.
    """
    return [task for task in tasks if job_id in task["id"]]


async def get_short_hostname_from_task(task: Task) -> str:
    try:
        slave_hostname = (await task.slave())["hostname"]
        return slave_hostname.split(".")[0]
    except (AttributeError, SlaveDoesNotExist):
        return "Unknown"


def get_first_status_timestamp(task: Task) -> Optional[float]:
    try:
        start_time_string = task["statuses"][0]["timestamp"]
        return float(start_time_string)
    except (IndexError, SlaveDoesNotExist):
        return None


def get_first_status_timestamp_string(task: Task) -> str:
    """Gets the first status timestamp from a task id and returns a human
    readable string with the local time and a humanized duration:
    ``2015-01-30T08:45 (an hour ago)``
    """
    first_status_timestamp = get_first_status_timestamp(task)
    if first_status_timestamp is None:
        return "Unknown"
    else:
        first_status_datetime = datetime.datetime.fromtimestamp(first_status_timestamp)
        return "{} ({})".format(
            first_status_datetime.strftime("%Y-%m-%dT%H:%M"),
            humanize.naturaltime(first_status_datetime),
        )


async def get_mem_usage(task: Task) -> str:
    try:
        task_mem_limit = await task.mem_limit()
        task_rss = await task.rss()
        if task_mem_limit == 0:
            return "Undef"
        mem_percent = task_rss / task_mem_limit * 100
        mem_string = "%d/%dMB" % (
            (task_rss / 1024 / 1024),
            (task_mem_limit / 1024 / 1024),
        )
        if mem_percent > 90:
            return PaastaColors.red(mem_string)
        else:
            return mem_string
    except (AttributeError, SlaveDoesNotExist):
        return "None"
    except TimeoutError:
        return "Timed Out"


async def get_cpu_shares(task: Task) -> float:
    # The CPU shares has an additional .1 allocated to it for executor overhead.
    # We subtract this to the true number
    # (https://github.com/apache/mesos/blob/dc7c4b6d0bcf778cc0cad57bb108564be734143a/src/slave/constants.hpp#L100)
    cpu_shares = await task.cpu_limit()
    return cpu_shares - 0.1


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
        start_time = round(task["statuses"][0]["timestamp"])
        current_time = int(datetime.datetime.now().strftime("%s"))
        duration_seconds = current_time - start_time
        cpu_shares = await get_cpu_shares(task)
        allocated_seconds = duration_seconds * cpu_shares
        task_stats = await task.stats()
        used_seconds = task_stats.get("cpus_system_time_secs", 0.0) + task_stats.get(
            "cpus_user_time_secs", 0.0
        )
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


async def format_running_mesos_task_row(
    task: Task, get_short_task_id: Callable[[str], str]
) -> Tuple[str, ...]:
    """Returns a pretty formatted string of a running mesos task attributes"""

    short_task_id = get_short_task_id(task["id"])
    short_hostname_future = asyncio.ensure_future(
        results_or_unknown(get_short_hostname_from_task(task))
    )
    mem_usage_future = asyncio.ensure_future(results_or_unknown(get_mem_usage(task)))
    cpu_usage_future = asyncio.ensure_future(results_or_unknown(get_cpu_usage(task)))
    first_status_timestamp = get_first_status_timestamp_string(task)

    await asyncio.wait([short_hostname_future, mem_usage_future, cpu_usage_future])

    return (
        short_task_id,
        short_hostname_future.result(),
        mem_usage_future.result(),
        cpu_usage_future.result(),
        first_status_timestamp,
    )


async def format_non_running_mesos_task_row(
    task: Task, get_short_task_id: Callable[[str], str]
) -> Tuple[str, ...]:
    """Returns a pretty formatted string of a running mesos task attributes"""
    return (
        PaastaColors.grey(get_short_task_id(task["id"])),
        PaastaColors.grey(await results_or_unknown(get_short_hostname_from_task(task))),
        PaastaColors.grey(get_first_status_timestamp_string(task)),
        PaastaColors.grey(task["state"]),
    )


@async_timeout()
async def get_tail_lines_for_mesos_task(
    task: Task, get_short_task_id: Callable[[str], str], num_tail_lines: int
) -> MutableMapping[str, Sequence[str]]:
    tail_lines_dict: MutableMapping[str, Sequence[str]] = {}
    mesos_cli_config = get_mesos_config()

    try:
        fobjs = await aiter_to_list(
            cluster.get_files_for_tasks(
                task_list=[task],
                file_list=["stdout", "stderr"],
                max_workers=mesos_cli_config["max_workers"],
            )
        )
        if not fobjs:
            return {"stdout": [], "stderr": []}

        fobjs.sort(key=lambda fobj: fobj.path, reverse=True)

        for fobj in fobjs:
            # read nlines, starting from EOF
            tail = []
            lines_seen = 0

            async for line in fobj._readlines_reverse():
                tail.append(line)
                lines_seen += 1
                if lines_seen >= num_tail_lines:
                    break

            # reverse the tail, so that EOF is at the bottom again
            tail_lines_dict[fobj.path] = tail[::-1]
    except (
        mesos_exceptions.MasterNotAvailableException,
        mesos_exceptions.SlaveDoesNotExist,
        mesos_exceptions.TaskNotFoundException,
        mesos_exceptions.FileNotFoundForTaskException,
        TimeoutError,
    ) as e:
        short_task_id = get_short_task_id(task["id"])
        error_name = e.__class__.__name__
        return {
            "error_message": f"couldn't read stdout/stderr for {short_task_id} ({error_name})"
        }

    return tail_lines_dict


def format_tail_lines_for_mesos_task(tail_lines, task_id):
    rows = []
    if (tail_lines.stderr or tail_lines.stdout) is not None:
        if len(tail_lines.stderr) + len(tail_lines.stdout) == 0:
            rows.append(PaastaColors.blue(f"  no stdout/stderrr for {task_id}"))
        else:
            for stdstream in ("stdout", "stderr"):
                rows.append(PaastaColors.blue(f"{stdstream} tail for {task_id}"))
                rows.extend(f"  {line}" for line in getattr(tail_lines, stdstream, []))
    elif tail_lines.error_message is not None:
        rows.append(PaastaColors.red(f"  {tail_lines.error_message}"))

    return rows


@async_timeout()
async def format_stdstreams_tail_for_task(task, get_short_task_id, nlines=10):
    tail_lines_dict = await get_tail_lines_for_mesos_task(
        task, get_short_task_id, nlines
    )
    tail_lines = MesosTailLines(
        stdout=tail_lines_dict.get("stdout"),
        stderr=tail_lines_dict.get("stderr"),
        error_message=tail_lines_dict.get("error_message"),
    )
    return [
        f"    {line}"
        for line in format_tail_lines_for_mesos_task(tail_lines, task["id"])
    ]


def zip_tasks_verbose_output(table, stdstreams):
    """Zip a list of strings (table) with a list of lists (stdstreams)
    :param table: a formatted list of tasks
    :param stdstreams: for each task, a list of lines from stdout/stderr tail
    """
    if len(table) != len(stdstreams):
        raise ValueError("Can only zip same-length lists")
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
    format_task_row: Callable[
        [Task, Callable[[str], str]], Awaitable[Union[Sequence[str], str]]
    ],
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
            return x

    else:

        def colorize(x):
            return PaastaColors.grey(x)

    output = []
    output.append(colorize("  %s" % list_title))
    table_rows: List[Union[str, Sequence[str]]] = [
        [colorize(th) for th in table_header]
    ]

    if tasks:
        task_row_futures = [
            asyncio.ensure_future(format_task_row(task, get_short_task_id))
            for task in tasks
        ]
        await asyncio.wait(task_row_futures)

        for future in task_row_futures:
            table_rows.append(future.result())

    tasks_table = ["    %s" % row for row in format_table(table_rows)]
    if tail_lines == 0:
        output.extend(tasks_table)
    else:
        stdstreams = []
        for task in tasks:
            stdstreams.append(
                await format_stdstreams_tail_for_task(
                    task, get_short_task_id, nlines=tail_lines
                )
            )
        output.append(tasks_table[0])  # header
        output.extend(zip_tasks_verbose_output(tasks_table[1:], stdstreams))

    return output


@a_sync.to_blocking
async def status_mesos_tasks_verbose(
    filter_string: str, get_short_task_id: Callable[[str], str], tail_lines: int = 0
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
        await get_cached_list_of_running_tasks_from_frameworks(), filter_string
    )
    list_title = "Running Tasks:"
    table_header = [
        "Mesos Task ID",
        "Host deployed to",
        "Ram",
        "CPU",
        "Deployed at what localtime",
    ]
    output.extend(
        await format_task_list(
            tasks=running_and_active_tasks,
            list_title=list_title,
            table_header=table_header,
            get_short_task_id=get_short_task_id,
            format_task_row=format_running_mesos_task_row,
            grey=False,
            tail_lines=tail_lines,
        )
    )

    non_running_tasks = select_tasks_by_id(
        await get_cached_list_of_not_running_tasks_from_frameworks(), filter_string
    )
    # Order the tasks by timestamp
    non_running_tasks.sort(key=lambda task: get_first_status_timestamp_string(task))
    non_running_tasks_ordered = list(reversed(non_running_tasks[-10:]))

    list_title = "Non-Running Tasks"
    table_header = [
        "Mesos Task ID",
        "Host deployed to",
        "Deployed at what localtime",
        "Status",
    ]
    output.extend(
        await format_task_list(
            tasks=non_running_tasks_ordered,
            list_title=list_title,
            table_header=table_header,
            get_short_task_id=get_short_task_id,
            format_task_row=format_non_running_mesos_task_row,
            grey=True,
            tail_lines=tail_lines,
        )
    )

    return "\n".join(output)


# TODO: remove to_blocking, convert call sites (smartstack_tools and marathon_serviceinit) to asyncio.
@a_sync.to_blocking
async def get_slaves():
    return (await (await get_mesos_master().fetch("/master/slaves")).json())["slaves"]


@a_sync.to_blocking
async def get_all_frameworks(active_only=False):
    return await get_mesos_master().frameworks(active_only=active_only)
