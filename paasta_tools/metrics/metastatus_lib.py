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
import copy
import itertools
import math
from collections import Counter
from collections import namedtuple
from collections import OrderedDict
from typing import Any
from typing import Callable
from typing import Mapping
from typing import NamedTuple
from typing import Sequence
from typing import Tuple
from typing import TypeVar

import a_sync
from humanize import naturalsize
from kubernetes.client import V1Node
from mypy_extensions import TypedDict
from typing_extensions import Counter as _Counter  # noqa

from paasta_tools import chronos_tools
from paasta_tools.kubernetes_tools import get_all_nodes
from paasta_tools.kubernetes_tools import get_all_pods
from paasta_tools.kubernetes_tools import get_pod_status
from paasta_tools.kubernetes_tools import is_node_ready
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import list_all_deployments
from paasta_tools.kubernetes_tools import maybe_add_yelp_prefix
from paasta_tools.kubernetes_tools import PodStatus
from paasta_tools.marathon_tools import MarathonClient
from paasta_tools.mesos.master import MesosMetrics
from paasta_tools.mesos.master import MesosState
from paasta_tools.mesos_maintenance import MAINTENANCE_ROLE
from paasta_tools.mesos_tools import get_all_tasks_from_state
from paasta_tools.mesos_tools import get_mesos_quorum
from paasta_tools.mesos_tools import get_number_of_mesos_masters
from paasta_tools.mesos_tools import get_zookeeper_host_path
from paasta_tools.mesos_tools import is_task_terminal
from paasta_tools.mesos_tools import MesosResources
from paasta_tools.mesos_tools import MesosTask
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import print_with_indent


class ResourceInfo(namedtuple('ResourceInfo', ['cpus', 'mem', 'disk', 'gpus'])):
    def __new__(cls, cpus, mem, disk, gpus=0):
        return super().__new__(cls, cpus, mem, disk, gpus)


class HealthCheckResult(NamedTuple):
    message: str
    healthy: bool


class ResourceUtilization(NamedTuple):
    metric: str
    total: int
    free: int


HIGH_QUEUE_GAUGE = 'org.apache.mesos.chronos.scheduler.jobs.TaskManager.highQueueSize'
QUEUE_GAUGE = 'org.apache.mesos.chronos.scheduler.jobs.TaskManager.queueSize'


def get_num_masters() -> int:
    """ Gets the number of masters from mesos state """
    zookeeper_host_path = get_zookeeper_host_path()
    return get_number_of_mesos_masters(zookeeper_host_path.host, zookeeper_host_path.path)


def get_mesos_cpu_status(
    metrics: MesosMetrics,
    mesos_state: MesosState,
) -> Tuple[int, int, int]:
    """Takes in the mesos metrics and analyzes them, returning the status.

    :param metrics: mesos metrics dictionary.
    :param mesos_state: mesos state dictionary.
    :returns: Tuple of total, used, and available CPUs.
    """

    total = metrics['master/cpus_total']
    used = metrics['master/cpus_used']

    for slave in mesos_state['slaves']:
        used += reserved_maintenence_resources(slave['reserved_resources'])['cpus']

    available = total - used
    return total, used, available


def get_kube_cpu_status(
    nodes: Sequence[V1Node],
) -> Tuple[int, int, int]:
    """Takes in the list of Kubernetes nodes and analyzes them, returning the status.

    :param nodes: list of Kubernetes nodes.
    :returns: Tuple of total, used, and available CPUs.
    """

    total = 0
    available = 0
    for node in nodes:
        available += suffixed_number_value(node.status.allocatable['cpu'])
        total += suffixed_number_value(node.status.capacity['cpu'])

    used = total - available
    return total, used, available


def get_mesos_memory_status(
    metrics: MesosMetrics,
    mesos_state: MesosState,
) -> Tuple[int, int, int]:
    """Takes in the mesos metrics and analyzes them, returning the status.

    :param metrics: mesos metrics dictionary.
    :param mesos_state: mesos state dictionary.
    :returns: Tuple of total, used, and available memory in Mi.
    """
    total = metrics['master/mem_total']
    used = metrics['master/mem_used']

    for slave in mesos_state['slaves']:
        used += reserved_maintenence_resources(slave['reserved_resources'])['mem']

    available = total - used

    return total, used, available


def get_kube_memory_status(
    nodes: Sequence[V1Node],
) -> Tuple[int, int, int]:
    """Takes in the list of Kubernetes nodes and analyzes them, returning the status.

    :param nodes: list of Kubernetes nodes.
    :returns: Tuple of total, used, and available memory in Mi.
    """
    total = 0
    available = 0
    for node in nodes:
        available += suffixed_number_value(node.status.allocatable['memory'])
        total += suffixed_number_value(node.status.capacity['memory'])

    total //= 1024 * 1024
    available //= 1024 * 1024
    used = total - available
    return total, used, available


def get_mesos_disk_status(
    metrics: MesosMetrics,
    mesos_state: MesosState,
) -> Tuple[int, int, int]:
    """Takes in the mesos metrics and analyzes them, returning the status.

    :param metrics: mesos metrics dictionary.
    :param mesos_state: mesos state dictionary.
    :returns: Tuple of total, used, and available disk space in Mi.
    """

    total = metrics['master/disk_total']
    used = metrics['master/disk_used']

    for slave in mesos_state['slaves']:
        used += reserved_maintenence_resources(slave['reserved_resources'])['disk']

    available = total - used
    return total, used, available


def get_kube_disk_status(
    nodes: Sequence[V1Node],
) -> Tuple[int, int, int]:
    """Takes in the list of Kubernetes nodes and analyzes them, returning the status.

    :param nodes: list of Kubernetes nodes.
    :returns: Tuple of total, used, and available disk space in Mi.
    """

    total = 0
    available = 0
    for node in nodes:
        available += suffixed_number_value(node.status.allocatable['ephemeral-storage'])
        total += suffixed_number_value(node.status.capacity['ephemeral-storage'])

    total //= 1024 * 1024
    available //= 1024 * 1024
    used = total - available
    return total, used, available


def get_mesos_gpu_status(
    metrics: MesosMetrics,
    mesos_state: MesosState,
) -> Tuple[int, int, int]:
    """Takes in the mesos metrics and analyzes them, returning gpus status.

    :param metrics: mesos metrics dictionary.
    :param mesos_state: mesos state dictionary.
    :returns: Tuple of total, used, and available GPUs.
    """
    total = metrics['master/gpus_total']
    used = metrics['master/gpus_used']

    for slave in mesos_state['slaves']:
        used += reserved_maintenence_resources(slave['reserved_resources'])['gpus']

    available = total - used
    return total, used, available


def get_kube_gpu_status(
    nodes: Sequence[V1Node],
) -> Tuple[int, int, int]:
    """Takes in the list of Kubernetes nodes and analyzes them, returning the status.

    :param nodes: list of Kubernetes nodes.
    :returns: Tuple of total, used, and available GPUs.
    """

    total = 0
    available = 0
    for node in nodes:
        available += suffixed_number_value(node.status.allocatable.get('nvidia.com/gpu', '0'))
        total += suffixed_number_value(node.status.capacity.get('nvidia.com/gpu', '0'))

    used = total - available
    return total, used, available


def filter_mesos_state_metrics(dictionary: Mapping[str, Any]) -> Mapping[str, Any]:
    valid_keys = ['cpus', 'mem', 'disk', 'gpus']
    return {key: value for (key, value) in dictionary.items() if key in valid_keys}


def filter_kube_resources(dictionary: Mapping[str, str]) -> Mapping[str, str]:
    valid_keys = ['cpu', 'memory', 'ephemeral-storage', 'nvidia.com/gpu']
    return {key: value for (key, value) in dictionary.items() if key in valid_keys}


def healthcheck_result_for_resource_utilization(
    resource_utilization: ResourceUtilization,
    threshold: int,
) -> HealthCheckResult:
    """ Given a resource data dict, assert that cpu
    data is ok.

    :param resource_utilization: the resource_utilization tuple to check
    :returns: a HealthCheckResult
    """
    try:
        utilization = percent_used(resource_utilization.total, resource_utilization.total - resource_utilization.free)
    except ZeroDivisionError:
        utilization = 0
    message = "{}: {:.2f}/{:.2f}({:.2f}%) used. Threshold ({:.2f}%)".format(
        resource_utilization.metric,
        float(resource_utilization.total - resource_utilization.free),
        resource_utilization.total,
        utilization,
        threshold,
    )
    healthy = utilization <= threshold
    return HealthCheckResult(
        message=message,
        healthy=healthy,
    )


def quorum_ok(masters: int, quorum: int) -> bool:
    return masters >= quorum


def check_threshold(percent_used: float, threshold: int) -> bool:
    return (100 - percent_used) > threshold


def percent_used(total: float, used: float) -> float:
    return round(used / float(total) * 100.0, 2)


def assert_cpu_health(
    cpu_status: Tuple[int, int, int],
    threshold: int = 10,
) -> HealthCheckResult:
    total, used, available = cpu_status
    try:
        perc_used = percent_used(total, used)
    except ZeroDivisionError:
        return HealthCheckResult(
            message="Error reading total available cpu from mesos!",
            healthy=False,
        )

    if check_threshold(perc_used, threshold):
        return HealthCheckResult(
            message="CPUs: %.2f / %d in use (%s)"
            % (used, total, PaastaColors.green("%.2f%%" % perc_used)),
            healthy=True,
        )
    else:
        return HealthCheckResult(
            message="CRITICAL: Less than %d%% CPUs available. (Currently using %.2f%% of %d)"
            % (threshold, perc_used, total),
            healthy=False,
        )


def assert_memory_health(
    memory_status: Tuple[int, int, int],
    threshold: int = 10,
) -> HealthCheckResult:
    total: float
    used: float
    total, used, _ = memory_status

    total /= 1024
    used /= 1024

    try:
        perc_used = percent_used(total, used)
    except ZeroDivisionError:
        return HealthCheckResult(
            message="Error reading total available memory from mesos!",
            healthy=False,
        )

    if check_threshold(perc_used, threshold):
        return HealthCheckResult(
            message="Memory: %0.2f / %0.2fGB in use (%s)"
            % (used, total, PaastaColors.green("%.2f%%" % perc_used)),
            healthy=True,
        )
    else:
        return HealthCheckResult(
            message="CRITICAL: Less than %d%% memory available. (Currently using %.2f%% of %.2fGB)"
                    % (threshold, perc_used, total),
                    healthy=False,
        )


def assert_disk_health(
    disk_status: Tuple[int, int, int],
    threshold: int = 10,
) -> HealthCheckResult:
    total: float
    used: float
    total, used, _ = disk_status

    total /= 1024
    used /= 1024

    try:
        perc_used = percent_used(total, used)
    except ZeroDivisionError:
        return HealthCheckResult(
            message="Error reading total available disk from mesos!",
            healthy=False,
        )

    if check_threshold(perc_used, threshold):
        return HealthCheckResult(
            message="Disk: %0.2f / %0.2fGB in use (%s)"
            % (used, total, PaastaColors.green("%.2f%%" % perc_used)),
            healthy=True,
        )
    else:
        return HealthCheckResult(
            message="CRITICAL: Less than %d%% disk available. (Currently using %.2f%%)" % (threshold, perc_used),
            healthy=False,
        )


def assert_gpu_health(
    gpu_status: Tuple[int, int, int],
    threshold: int = 0,
) -> HealthCheckResult:
    total, used, available = gpu_status

    if math.isclose(total, 0):
        # assume that no gpus is healthy since most machines don't have them
        return HealthCheckResult(
            message="No GPUs found!",
            healthy=True,
        )
    else:
        perc_used = percent_used(total, used)

    if check_threshold(perc_used, threshold):
        # only whole gpus can be used
        return HealthCheckResult(
            message="GPUs: %d / %d in use (%s)"
            % (used, total, PaastaColors.green("%.2f%%" % perc_used)),
            healthy=True,
        )
    else:
        return HealthCheckResult(
            message="CRITICAL: Less than %d%% GPUs available. (Currently using %.2f%% of %d)"
            % (threshold, perc_used, total),
            healthy=False,
        )


def assert_mesos_tasks_running(
    metrics: MesosMetrics,
) -> HealthCheckResult:
    running = metrics['master/tasks_running']
    staging = metrics['master/tasks_staging']
    starting = metrics['master/tasks_starting']
    return HealthCheckResult(
        message="Tasks: running: %d staging: %d starting: %d" % (running, staging, starting),
        healthy=True,
    )


def assert_kube_pods_running(
    kube_client: KubeClient,
) -> HealthCheckResult:
    statuses = [get_pod_status(pod) for pod in get_all_pods(kube_client)]
    running = statuses.count(PodStatus.RUNNING)
    pending = statuses.count(PodStatus.PENDING)
    failed = statuses.count(PodStatus.FAILED)
    healthy = running > 0
    return HealthCheckResult(
        message=f'Pods: running: {running} pending: {pending} failed: {failed}',
        healthy=healthy,
    )


def assert_no_duplicate_frameworks(
    state: MesosState,
    framework_list: Sequence[str]=['marathon', 'chronos'],
) -> HealthCheckResult:
    """A function which asserts that there are no duplicate frameworks running, where
    frameworks are identified by their name.

    Note the extra spaces in the output strings: this is to account for the extra indentation
    we add, so we can have:

        frameworks:
          framework: marathon count: 1

    :param state: the state info from the Mesos master
    :returns: a tuple containing (output, ok): output is a log of the state of frameworks, ok a boolean
        indicating if there are any duplicate frameworks.
    """
    output = ['Frameworks:']
    status = True
    frameworks = state['frameworks']
    for name in framework_list:
        shards = [x['name'] for x in frameworks if x['name'].startswith(name)]
        for framework, count in OrderedDict(sorted(Counter(shards).items())).items():
            if count > 1:
                status = False
                output.append("    CRITICAL: There are %d connected %s frameworks! "
                              "(Expected 1)" % (count, framework))
        output.append("    Framework: %s count: %d" % (name, len(shards)))

    return HealthCheckResult(
        message=("\n").join(output),
        healthy=status,
    )


def assert_frameworks_exist(
    state: MesosState,
    expected: Sequence[str],
) -> HealthCheckResult:
    frameworks = [f['name'] for f in state['frameworks']]
    not_found = []
    ok = True

    for f in expected:
        if f not in frameworks:
            ok = False
            not_found.append(f)

    if ok:
        return HealthCheckResult(
            message="all expected frameworks found",
            healthy=ok,
        )
    else:
        return HealthCheckResult(
            message="CRITICAL: framework(s) %s not found" % ', '.join(not_found),
            healthy=ok,
        )


def get_mesos_slaves_health_status(
    metrics: MesosMetrics,
) -> Tuple[int, int]:
    return metrics['master/slaves_active'], metrics['master/slaves_inactive']


def get_kube_nodes_health_status(
    nodes: Sequence[V1Node],
) -> Tuple[int, int]:
    statuses = [is_node_ready(node) for node in nodes]
    return statuses.count(True), statuses.count(False)


def assert_nodes_health(
    nodes_health_status: Tuple[int, int],
) -> HealthCheckResult:
    active, inactive = nodes_health_status
    healthy = active > 0
    return HealthCheckResult(
        message="Nodes: active: %d inactive: %d" % (active, inactive),
        healthy=healthy,
    )


def assert_quorum_size() -> HealthCheckResult:
    masters, quorum = get_num_masters(), a_sync.block(get_mesos_quorum)
    if quorum_ok(masters, quorum):
        return HealthCheckResult(
            message="Quorum: masters: %d configured quorum: %d " % (masters, quorum),
            healthy=True,
        )
    else:
        return HealthCheckResult(
            message="CRITICAL: Number of masters (%d) less than configured quorum(%d)." % (masters, quorum),
            healthy=False,
        )


_KeyFuncRetT = Sequence[Tuple[str, str]]


class _SlaveT(TypedDict):
    id: str
    resources: MesosResources
    reserved_resources: MesosResources
    attributes: Mapping[str, str]


_GenericNodeT = TypeVar('_GenericNodeT', _SlaveT, V1Node)

_GenericNodeGroupingFunctionT = Callable[
    [_GenericNodeT],
    _KeyFuncRetT,
]

_GenericNodeFilterFunctionT = Callable[
    [_GenericNodeT],
    bool,
]

_GenericNodeSortFunctionT = Callable[
    [Sequence[_GenericNodeT]],
    Sequence[_GenericNodeT],
]


def key_func_for_attribute(
    attribute: str,
) -> Callable[
    [_SlaveT],
    str,
]:
    """ Return a closure that given a slave, will return the value of a specific
    attribute.

    :param attribute: the attribute to inspect in the slave
    :returns: a closure, which takes a slave and returns the value of an attribute
    """
    def key_func(slave):
        return slave['attributes'].get(attribute, 'unknown')
    return key_func


def key_func_for_attribute_multi(
    attributes: Sequence[str],
) -> _GenericNodeGroupingFunctionT:
    """ Return a closure that given a slave, will return the value of a list of
    attributes, compiled into a hashable tuple

    :param attributes: the attributes to inspect in the slave
    :returns: a closure, which takes a slave and returns the value of those attributes
    """
    def get_attribute(slave, attribute):
        if attribute == "hostname":
            return slave["hostname"]
        else:
            return slave["attributes"].get(attribute, 'unknown')

    def key_func(slave):
        return tuple((a, get_attribute(slave, a)) for a in attributes)
    return key_func


def key_func_for_attribute_multi_kube(
    attributes: Sequence[str],
) -> Callable[
    [V1Node],
    _KeyFuncRetT,
]:
    """ Return a closure that given a node, will return the value of a list of
    attributes, compiled into a hashable tuple

    :param attributes: the attributes to inspect in the slave
    :returns: a closure, which takes a node and returns the value of those attributes
    """
    def get_attribute(node, attribute):
        return node.metadata.labels.get(maybe_add_yelp_prefix(attribute), 'unknown')

    def key_func(node):
        return tuple((a, get_attribute(node, a)) for a in attributes)
    return key_func


def sort_func_for_attributes(
    attributes: Sequence[str],
) -> _GenericNodeSortFunctionT:
    def sort(slaves):
        for attribute in attributes:
            slaves = sorted(slaves, key=key_func_for_attribute(attribute))
        return slaves
    return sort


def group_slaves_by_key_func(
    key_func: _GenericNodeGroupingFunctionT,
    slaves: Sequence[_GenericNodeT],
    sort_func: _GenericNodeSortFunctionT = None,
) -> Mapping[_KeyFuncRetT, Sequence[_GenericNodeT]]:
    """ Given a function for grouping slaves, return a
    dict where keys are the unique values returned by
    the key_func and the values are all those slaves which
    have that specific value.

    :param key_func: a function which consumes a slave and returns a value
    :param slaves: a list of slaves
    :returns: a dict of key: [slaves]
    """
    sorted_slaves: Sequence[_GenericNodeT]
    if sort_func is None:
        sorted_slaves = sorted(slaves, key=key_func)
    else:
        sorted_slaves = sort_func(slaves)

    return {k: list(v) for k, v in itertools.groupby(sorted_slaves, key=key_func)}


class ResourceUtilizationDict(TypedDict):
    free: ResourceInfo
    total: ResourceInfo
    slave_count: int


def calculate_resource_utilization_for_slaves(
    slaves: Sequence[_SlaveT],
    tasks: Sequence[MesosTask],
) -> ResourceUtilizationDict:
    """ Given a list of slaves and a list of tasks, calculate the total available
    resource available in that list of slaves, and the resources consumed by tasks
    running on those slaves.

    :param slaves: a list of slaves to calculate resource usage for
    :param tasks: the list of tasks running in the mesos cluster
    :returns: a dict, containing keys for "free" and "total" resources. Each of these keys
    is a ResourceInfo tuple, exposing a number for cpu, disk and mem.
    """
    resource_total_dict: _Counter[str] = Counter()
    for slave in slaves:
        filtered_resources = filter_mesos_state_metrics(slave['resources'])
        resource_total_dict.update(Counter(filtered_resources))
    resource_free_dict = copy.deepcopy(resource_total_dict)
    for task in tasks:
        task_resources = task['resources']
        resource_free_dict.subtract(Counter(filter_mesos_state_metrics(task_resources)))
    for slave in slaves:
        filtered_resources = filter_mesos_state_metrics(
            reserved_maintenence_resources(slave['reserved_resources']),
        )
        resource_free_dict.subtract(Counter(filtered_resources))
    return {
        "free": ResourceInfo(
            cpus=resource_free_dict['cpus'],
            disk=resource_free_dict['disk'],
            mem=resource_free_dict['mem'],
            gpus=resource_free_dict.get('gpus', 0),
        ),
        "total": ResourceInfo(
            cpus=resource_total_dict['cpus'],
            disk=resource_total_dict['disk'],
            mem=resource_total_dict['mem'],
            gpus=resource_total_dict.get('gpus', 0),
        ),
        "slave_count": len(slaves),
    }


_IEC_NUMBER_SUFFIXES = {
    'Ki': 1024,
    'Mi': 1024**2,
    'Gi': 1024**3,
    'Ti': 1024**4,
    'Pi': 1024**5,
}


def suffixed_number_value(s: str) -> int:
    suff = s[-2:]
    if suff in _IEC_NUMBER_SUFFIXES:
        return int(s[:-2]) * _IEC_NUMBER_SUFFIXES[suff]
    else:
        return int(s)


def suffixed_number_dict_values(d: Mapping[Any, str]) -> Mapping[Any, int]:
    return {k: suffixed_number_value(v) for k, v in d.items()}


def calculate_resource_utilization_for_kube_nodes(
    nodes: Sequence[V1Node],
) -> ResourceUtilizationDict:
    """ Given a list of Kubernetes nodes, calculate the total available
    resource available and the resources consumed in that list of nodes.

    :param nodes: a list of Kubernetes nodes to calculate resource usage for
    :returns: a dict, containing keys for "free" and "total" resources. Each of these keys
    is a ResourceInfo tuple, exposing a number for cpu, disk and mem.
    """
    resource_total_dict: _Counter[str] = Counter()
    for node in nodes:
        filtered_resources = filter_kube_resources(node.status.capacity)
        resource_total_dict.update(Counter(suffixed_number_dict_values(filtered_resources)))
    resource_free_dict: _Counter[str] = Counter()
    for node in nodes:
        filtered_resources = filter_kube_resources(node.status.allocatable)
        resource_free_dict.update(Counter(suffixed_number_dict_values(filtered_resources)))
    return {
        "free": ResourceInfo(
            cpus=resource_free_dict['cpu'],
            disk=resource_free_dict['ephemeral-storage'] / (1024 ** 2),
            mem=resource_free_dict['memory'] / (1024 ** 2),
            gpus=resource_free_dict.get('nvidia.com/gpu', 0),
        ),
        "total": ResourceInfo(
            cpus=resource_total_dict['cpu'],
            disk=resource_total_dict['ephemeral-storage'] / (1024 ** 2),
            mem=resource_total_dict['memory'] / (1024 ** 2),
            gpus=resource_total_dict.get('nvidia.com/gpu', 0),
        ),
        "slave_count": len(nodes),
    }


def filter_tasks_for_slaves(
    slaves: Sequence[_SlaveT],
    tasks: Sequence[MesosTask],
) -> Sequence[MesosTask]:
    """ Given a list of slaves and a list of tasks, return a filtered
    list of tasks, where those returned belong to slaves in the list of
    slaves

    :param slaves: the list of slaves which the tasks provided should be
    running on.
    :param tasks: the tasks to filter :returns: a list of tasks,
    identical to that provided by the tasks param, but with only those where
    the task is running on one of the provided slaves included.
    """
    slave_ids = [slave['id'] for slave in slaves]
    return [task for task in tasks if task['slave_id'] in slave_ids]


def make_filter_slave_func(
    attribute: str,
    values: Sequence[str],
) -> _GenericNodeFilterFunctionT:
    def filter_func(slave):
        return slave['attributes'].get(attribute, None) in values
    return filter_func


def filter_slaves(
    slaves: Sequence[_GenericNodeT],
    filters: Sequence[_GenericNodeFilterFunctionT],
) -> Sequence[_GenericNodeT]:
    """ Filter slaves by attributes

    :param slaves: list of slaves to filter
    :param filters: list of functions that take a slave and return whether the
    slave should be included
    :returns: list of slaves that return true for all the filters
    """
    if filters is None:
        return slaves
    return [s for s in slaves if all([f(s) for f in filters])]


def get_resource_utilization_by_grouping(
    grouping_func: _GenericNodeGroupingFunctionT,
    mesos_state: MesosState,
    filters: Sequence[_GenericNodeFilterFunctionT]=[],
    sort_func: _GenericNodeSortFunctionT = None,
) -> Mapping[_KeyFuncRetT, ResourceUtilizationDict]:
    """ Given a function used to group slaves and mesos state, calculate
    resource utilization for each value of a given attribute.

    :grouping_func: a function that given a slave, will return the value of an
    attribute to group by.
    :param mesos_state: the mesos state
    :param filters: filters to apply to the slaves in the calculation, with
    filtering preformed by filter_slaves
    :param sort_func: a function that given a list of slaves, will return the
    sorted list of slaves.
    :returns: a dict of {attribute_value: resource_usage}, where resource usage
    is the dict returned by ``calculate_resource_utilization_for_slaves`` for
    slaves grouped by attribute value.
    """
    slaves: Sequence[_SlaveT] = mesos_state.get('slaves', [])
    slaves = filter_slaves(slaves, filters)
    if not has_registered_slaves(mesos_state):
        raise ValueError("There are no slaves registered in the mesos state.")

    tasks = get_all_tasks_from_state(mesos_state, include_orphans=True)
    non_terminal_tasks = [task for task in tasks if not is_task_terminal(task)]
    slave_groupings = group_slaves_by_key_func(grouping_func, slaves, sort_func)

    return {
        attribute_value: calculate_resource_utilization_for_slaves(
            slaves=slaves,
            tasks=filter_tasks_for_slaves(slaves, non_terminal_tasks),
        )
        for attribute_value, slaves in slave_groupings.items()
    }


def get_resource_utilization_by_grouping_kube(
    grouping_func: _GenericNodeGroupingFunctionT,
    kube_client: KubeClient,
    filters: Sequence[_GenericNodeFilterFunctionT]=[],
    sort_func: _GenericNodeSortFunctionT = None,
) -> Mapping[_KeyFuncRetT, ResourceUtilizationDict]:
    """ Given a function used to group nodes, calculate resource utilization
    for each value of a given attribute.

    :grouping_func: a function that given a node, will return the value of an
    attribute to group by.
    :param kube_client: the Kubernetes client
    :param filters: filters to apply to the nodes in the calculation, with
    filtering preformed by filter_slaves
    :param sort_func: a function that given a list of nodes, will return the
    sorted list of nodes.
    :returns: a dict of {attribute_value: resource_usage}, where resource usage
    is the dict returned by ``calculate_resource_utilization_for_kube_nodes`` for
    nodes grouped by attribute value.
    """
    nodes: Sequence[V1Node] = get_all_nodes(kube_client)
    nodes = filter_slaves(nodes, filters)
    if len(nodes) == 0:
        raise ValueError("There are no nodes registered in the Kubernetes.")

    node_groupings = group_slaves_by_key_func(grouping_func, nodes, sort_func)

    return {
        attribute_value: calculate_resource_utilization_for_kube_nodes(nodes)
        for attribute_value, nodes in node_groupings.items()
    }


def resource_utillizations_from_resource_info(
    total: ResourceInfo,
    free: ResourceInfo,
) -> Sequence[ResourceUtilization]:
    """
    Given two ResourceInfo tuples, one for total and one for free,
    create a ResourceUtilization tuple for each metric in the ResourceInfo.
    :param total:
    :param free:
    :returns: ResourceInfo for a metric
    """
    return [
        ResourceUtilization(metric=field, total=total[index], free=free[index])
        for index, field in enumerate(ResourceInfo._fields)
    ]


def has_registered_slaves(
    mesos_state: MesosState,
) -> bool:
    """ Return a boolean indicating if there are any slaves registered
    to the master according to the mesos state.
    :param mesos_state: the mesos state from the master
    :returns: a boolean, indicating if there are > 0 slaves
    """
    return len(mesos_state.get('slaves', [])) > 0


def get_mesos_resource_utilization_health(
    mesos_metrics: MesosMetrics,
    mesos_state: MesosState,
) -> Sequence[HealthCheckResult]:
    """Perform healthchecks against mesos metrics.
    :param mesos_metrics: a dict exposing the mesos metrics described in
    https://mesos.apache.org/documentation/latest/monitoring/
    :returns: a list of HealthCheckResult tuples
    """
    return [
        assert_cpu_health(get_mesos_cpu_status(mesos_metrics, mesos_state)),
        assert_memory_health(get_mesos_memory_status(mesos_metrics, mesos_state)),
        assert_disk_health(get_mesos_disk_status(mesos_metrics, mesos_state)),
        assert_gpu_health(get_mesos_gpu_status(mesos_metrics, mesos_state)),
        assert_mesos_tasks_running(mesos_metrics),
        assert_nodes_health(get_mesos_slaves_health_status(mesos_metrics)),
    ]


def get_kube_resource_utilization_health(
    kube_client: KubeClient,
) -> Sequence[HealthCheckResult]:
    """Perform healthchecks against Kubernetes.
    :param kube_client: the KUbernetes client
    :returns: a list of HealthCheckResult tuples
    """

    nodes = get_all_nodes(kube_client)

    return [
        assert_cpu_health(get_kube_cpu_status(nodes)),
        assert_memory_health(get_kube_memory_status(nodes)),
        assert_disk_health(get_kube_disk_status(nodes)),
        assert_gpu_health(get_kube_gpu_status(nodes)),
        assert_nodes_health(get_kube_nodes_health_status(nodes)),
    ]


def get_mesos_state_status(
    mesos_state: MesosState,
) -> Sequence[HealthCheckResult]:
    """Perform healthchecks against mesos state.
    :param mesos_state: a dict exposing the mesos state described in
    https://mesos.apache.org/documentation/latest/endpoints/master/state.json/
    :returns: a list of HealthCheckResult tuples
    """
    return [
        assert_quorum_size(),
        assert_no_duplicate_frameworks(state=mesos_state, framework_list=['marathon', 'chronos']),
    ]


def run_healthchecks_with_param(
    param: Any,
    healthcheck_functions: Sequence[Callable[..., HealthCheckResult]],
    format_options: Mapping[str, Any] = {},
) -> Sequence[HealthCheckResult]:
    return [healthcheck(param, **format_options) for healthcheck in healthcheck_functions]


def assert_marathon_apps(
    clients: Sequence[MarathonClient],
) -> HealthCheckResult:
    num_apps = [len(c.list_apps()) for c in clients]
    if sum(num_apps) < 1:
        return HealthCheckResult(
            message="CRITICAL: No marathon apps running",
            healthy=False,
        )
    else:
        return HealthCheckResult(
            message="marathon apps: %10d" % sum(num_apps),
            healthy=True,
        )


def assert_marathon_tasks(
    clients: Sequence[MarathonClient],
) -> HealthCheckResult:
    num_tasks = [len(c.list_tasks()) for c in clients]
    return HealthCheckResult(
        message="marathon tasks: %9d" % sum(num_tasks),
        healthy=True,
    )


def assert_marathon_deployments(
    clients: Sequence[MarathonClient],
) -> HealthCheckResult:
    num_deployments = [len(c.list_deployments()) for c in clients]
    return HealthCheckResult(
        message="marathon deployments: %3d" % sum(num_deployments),
        healthy=True,
    )


def assert_kube_deployments(
    kube_client: KubeClient,
) -> HealthCheckResult:
    num_deployments = len(list_all_deployments(kube_client))
    return HealthCheckResult(
        message=f'Kubernetes deployments: {num_deployments:>3}',
        healthy=True,
    )


def get_marathon_status(
    clients: Sequence[MarathonClient],
) -> Sequence[HealthCheckResult]:
    """ Gathers information about marathon.
    :return: string containing the status.  """
    return run_healthchecks_with_param(
        clients, [
            assert_marathon_apps,
            assert_marathon_tasks,
            assert_marathon_deployments,
        ],
    )


def assert_chronos_scheduled_jobs(client):
    """
    :returns: a tuple of a string and a bool containing representing if it is ok or not
    """
    num_jobs = len(chronos_tools.filter_enabled_jobs(client.list()))
    healthy = num_jobs != 0
    return HealthCheckResult(message="Enabled chronos jobs: %d" % num_jobs, healthy=healthy)


def assert_chronos_queued_jobs(client):
    high_priority_queue_size = client.metrics()['gauges'][HIGH_QUEUE_GAUGE]['value']
    normal_priority_queue_size = client.metrics()['gauges'][QUEUE_GAUGE]['value']
    all_jobs_queued = high_priority_queue_size + normal_priority_queue_size
    num_jobs = len(chronos_tools.filter_enabled_jobs(client.list()))

    try:
        perc_used = percent_used(num_jobs, all_jobs_queued)
    except ZeroDivisionError:
        perc_used = 0
    return HealthCheckResult(
        message=f"Jobs Queued: {all_jobs_queued} ({perc_used}%)",
        healthy=True,
    )


def get_chronos_status(chronos_client):
    """Gather information about chronos.
    :return: string containing the status
    """
    return run_healthchecks_with_param(
        chronos_client, [
            assert_chronos_scheduled_jobs,
            assert_chronos_queued_jobs,
        ],
    )


def get_kube_status(
    kube_client: KubeClient,
) -> Sequence[HealthCheckResult]:
    """Gather information about Kubernetes.
    :param kube_client: the KUbernetes client
    :return: string containing the status
    """
    return run_healthchecks_with_param(
        kube_client, [
            assert_kube_deployments,
            assert_kube_pods_running,
        ],
    )


def critical_events_in_outputs(healthcheck_outputs):
    """Given a list of HealthCheckResults return those which are unhealthy.
    """
    return [healthcheck for healthcheck in healthcheck_outputs if healthcheck.healthy is False]


def generate_summary_for_check(name, ok):
    """Given a check name and a boolean indicating if the service is OK, return
    a formatted message.
    """
    status = PaastaColors.green("OK") if ok is True else PaastaColors.red("CRITICAL")
    summary = f"{name} Status: {status}"
    return summary


def status_for_results(healthcheck_results):
    """Given a list of HealthCheckResult tuples, return the ok status
    for each one.
    :param healthcheck_results: a list of HealthCheckResult tuples
    :returns: a list of booleans.
    """
    return [result.healthy for result in healthcheck_results]


def print_results_for_healthchecks(summary, ok, results, verbose, indent=2):
    paasta_print(summary)
    if verbose >= 1:
        for health_check_result in results:
            if health_check_result.healthy:
                print_with_indent(health_check_result.message, indent)
            else:
                print_with_indent(PaastaColors.red(health_check_result.message), indent)
    elif not ok:
        unhealthy_results = critical_events_in_outputs(results)
        for health_check_result in unhealthy_results:
            print_with_indent(PaastaColors.red(health_check_result.message), indent)


def healthcheck_result_resource_utilization_pair_for_resource_utilization(utilization, threshold):
    """Given a ResourceUtilization, produce a tuple of (HealthCheckResult, ResourceUtilization),
    where that HealthCheckResult describes the 'health' of a given utilization.
    :param utilization: a ResourceUtilization tuple
    :param threshold: a threshold which decides the health of the given ResourceUtilization
    :returns: a tuple of (HealthCheckResult, ResourceUtilization)
    """
    return (healthcheck_result_for_resource_utilization(utilization, threshold), utilization)


def format_table_column_for_healthcheck_resource_utilization_pair(healthcheck_utilization_pair):
    """Given a tuple of (HealthCheckResult, ResourceUtilization), return a
    string representation of the ResourceUtilization such that it is formatted
    according to the value of HealthCheckResult.healthy.

    :param healthcheck_utilization_pair: a tuple of (HealthCheckResult, ResourceUtilization)
    :returns: a string representing the ResourceUtilization.
    """
    color_func = PaastaColors.green if healthcheck_utilization_pair[0].healthy else PaastaColors.red
    utilization = healthcheck_utilization_pair[1].total - healthcheck_utilization_pair[1].free
    if int(healthcheck_utilization_pair[1].total) == 0:
        utilization_perc = 100
    else:
        utilization_perc = utilization / float(healthcheck_utilization_pair[1].total) * 100
    if healthcheck_utilization_pair[1].metric not in ['cpus', 'gpus']:
        return color_func('{}/{} ({:.2f}%)'.format(
            naturalsize(utilization * 1024 * 1024, gnu=True),
            naturalsize(healthcheck_utilization_pair[1].total * 1024 * 1024, gnu=True),
            utilization_perc,
        ))
    else:
        return color_func('{:.2f}/{:.0f} ({:.2f}%)'.format(
            utilization,
            healthcheck_utilization_pair[1].total,
            utilization_perc,
        ))


def format_row_for_resource_utilization_healthchecks(healthcheck_utilization_pairs):
    """Given a list of (HealthCheckResult, ResourceUtilization) tuples, return a list with each of those
    tuples represented by a formatted string.

    :param healthcheck_utilization_pairs: a list of (HealthCheckResult, ResourceUtilization) tuples.
    :returns: a list containing a string representation of each (HealthCheckResult, ResourceUtilization) tuple.
    """
    return [
        format_table_column_for_healthcheck_resource_utilization_pair(pair)
        for pair in healthcheck_utilization_pairs
    ]


def get_table_rows_for_resource_info_dict(attribute_values, healthcheck_utilization_pairs):
    """ A wrapper method to join together

    :param attribute: The attribute value and formatted columns to be shown in
    a single row.  :param attribute_value: The value of the attribute
    associated with the row. This becomes index 0 in the array returned.
    :param healthcheck_utilization_pairs: a list of 2-tuples, where each tuple has the elements
    (HealthCheckResult, ResourceUtilization)
    :returns: a list of strings, representing a row in a table to be formatted.
    """
    return attribute_values + format_row_for_resource_utilization_healthchecks(healthcheck_utilization_pairs)


def reserved_maintenence_resources(
    resources: MesosResources,
):
    return resources.get(
        MAINTENANCE_ROLE, {
            'cpus': 0,
            'mem': 0,
            'disk': 0,
            'gpus': 0,
        },
    )
