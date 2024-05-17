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
import re
from collections import Counter
from collections import namedtuple
from typing import Any
from typing import Callable
from typing import Mapping
from typing import Sequence
from typing import Tuple
from typing import TypeVar

from kubernetes.client import V1Node
from mypy_extensions import TypedDict
from typing_extensions import Counter as _Counter

from paasta_tools.mesos.master import MesosState
from paasta_tools.mesos_tools import get_all_tasks_from_state
from paasta_tools.mesos_tools import is_task_terminal
from paasta_tools.mesos_tools import MesosResources
from paasta_tools.mesos_tools import MesosTask

MAINTENANCE_ROLE = "maintenance"


class ResourceInfo(namedtuple("ResourceInfo", ["cpus", "mem", "disk", "gpus"])):
    def __new__(cls, cpus, mem, disk, gpus=0):
        return super().__new__(cls, cpus, mem, disk, gpus)


def filter_mesos_state_metrics(dictionary: Mapping[str, Any]) -> Mapping[str, Any]:
    valid_keys = ["cpus", "mem", "disk", "gpus"]
    return {key: value for (key, value) in dictionary.items() if key in valid_keys}


_KeyFuncRetT = Sequence[Tuple[str, str]]


class _SlaveT(TypedDict):
    id: str
    resources: MesosResources
    reserved_resources: MesosResources
    attributes: Mapping[str, str]


_GenericNodeT = TypeVar("_GenericNodeT", _SlaveT, V1Node)

_GenericNodeGroupingFunctionT = Callable[[_GenericNodeT], _KeyFuncRetT]

_GenericNodeFilterFunctionT = Callable[[_GenericNodeT], bool]

_GenericNodeSortFunctionT = Callable[[Sequence[_GenericNodeT]], Sequence[_GenericNodeT]]


def key_func_for_attribute(
    attribute: str,
) -> Callable[[_SlaveT], str]:
    """Return a closure that given a slave, will return the value of a specific
    attribute.

    :param attribute: the attribute to inspect in the slave
    :returns: a closure, which takes a slave and returns the value of an attribute
    """

    def key_func(slave):
        return slave["attributes"].get(attribute, "unknown")

    return key_func


def key_func_for_attribute_multi(
    attributes: Sequence[str],
) -> _GenericNodeGroupingFunctionT:
    """Return a closure that given a slave, will return the value of a list of
    attributes, compiled into a hashable tuple

    :param attributes: the attributes to inspect in the slave
    :returns: a closure, which takes a slave and returns the value of those attributes
    """

    def get_attribute(slave, attribute):
        if attribute == "hostname":
            return slave["hostname"]
        else:
            return slave["attributes"].get(attribute, "unknown")

    def key_func(slave):
        return tuple((a, get_attribute(slave, a)) for a in attributes)

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
    """Given a function for grouping slaves, return a
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
    slaves: Sequence[_SlaveT], tasks: Sequence[MesosTask]
) -> ResourceUtilizationDict:
    """Given a list of slaves and a list of tasks, calculate the total available
    resource available in that list of slaves, and the resources consumed by tasks
    running on those slaves.

    :param slaves: a list of slaves to calculate resource usage for
    :param tasks: the list of tasks running in the mesos cluster
    :returns: a dict, containing keys for "free" and "total" resources. Each of these keys
    is a ResourceInfo tuple, exposing a number for cpu, disk and mem.
    """
    resource_total_dict: _Counter[str] = Counter()
    for slave in slaves:
        filtered_resources = filter_mesos_state_metrics(slave["resources"])
        resource_total_dict.update(Counter(filtered_resources))
    resource_free_dict = copy.deepcopy(resource_total_dict)
    for task in tasks:
        task_resources = task["resources"]
        resource_free_dict.subtract(Counter(filter_mesos_state_metrics(task_resources)))
    for slave in slaves:
        filtered_resources = filter_mesos_state_metrics(
            reserved_maintenence_resources(slave["reserved_resources"])
        )
        resource_free_dict.subtract(Counter(filtered_resources))
    return {
        "free": ResourceInfo(
            cpus=resource_free_dict["cpus"],
            disk=resource_free_dict["disk"],
            mem=resource_free_dict["mem"],
            gpus=resource_free_dict.get("gpus", 0),
        ),
        "total": ResourceInfo(
            cpus=resource_total_dict["cpus"],
            disk=resource_total_dict["disk"],
            mem=resource_total_dict["mem"],
            gpus=resource_total_dict.get("gpus", 0),
        ),
        "slave_count": len(slaves),
    }


_IEC_NUMBER_SUFFIXES = {
    "k": 1000,
    "m": 1000**-1,
    "M": 1000**2,
    "G": 1000**3,
    "T": 1000**4,
    "P": 1000**5,
    "Ki": 1024,
    "Mi": 1024**2,
    "Gi": 1024**3,
    "Ti": 1024**4,
    "Pi": 1024**5,
}


def suffixed_number_value(s: str) -> float:
    pattern = r"(?P<number>\d+)(?P<suff>\w*)"
    match = re.match(pattern, s)
    number, suff = match.groups()

    if suff in _IEC_NUMBER_SUFFIXES:
        return float(number) * _IEC_NUMBER_SUFFIXES[suff]
    else:
        return float(number)


def filter_tasks_for_slaves(
    slaves: Sequence[_SlaveT], tasks: Sequence[MesosTask]
) -> Sequence[MesosTask]:
    """Given a list of slaves and a list of tasks, return a filtered
    list of tasks, where those returned belong to slaves in the list of
    slaves

    :param slaves: the list of slaves which the tasks provided should be
    running on.
    :param tasks: the tasks to filter :returns: a list of tasks,
    identical to that provided by the tasks param, but with only those where
    the task is running on one of the provided slaves included.
    """
    slave_ids = [slave["id"] for slave in slaves]
    return [task for task in tasks if task["slave_id"] in slave_ids]


def make_filter_slave_func(
    attribute: str, values: Sequence[str]
) -> _GenericNodeFilterFunctionT:
    def filter_func(slave):
        return slave["attributes"].get(attribute, None) in values

    return filter_func


def filter_slaves(
    slaves: Sequence[_GenericNodeT], filters: Sequence[_GenericNodeFilterFunctionT]
) -> Sequence[_GenericNodeT]:
    """Filter slaves by attributes

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
    filters: Sequence[_GenericNodeFilterFunctionT] = [],
    sort_func: _GenericNodeSortFunctionT = None,
) -> Mapping[_KeyFuncRetT, ResourceUtilizationDict]:
    """Given a function used to group slaves and mesos state, calculate
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
    slaves: Sequence[_SlaveT] = mesos_state.get("slaves", [])
    slaves = filter_slaves(slaves, filters)
    if not has_registered_slaves(mesos_state):
        raise ValueError("There are no slaves registered in the mesos state.")

    tasks = get_all_tasks_from_state(mesos_state, include_orphans=True)
    non_terminal_tasks = [task for task in tasks if not is_task_terminal(task)]
    slave_groupings = group_slaves_by_key_func(grouping_func, slaves, sort_func)

    return {
        attribute_value: calculate_resource_utilization_for_slaves(
            slaves=slaves, tasks=filter_tasks_for_slaves(slaves, non_terminal_tasks)
        )
        for attribute_value, slaves in slave_groupings.items()
    }


def has_registered_slaves(
    mesos_state: MesosState,
) -> bool:
    """Return a boolean indicating if there are any slaves registered
    to the master according to the mesos state.
    :param mesos_state: the mesos state from the master
    :returns: a boolean, indicating if there are > 0 slaves
    """
    return len(mesos_state.get("slaves", [])) > 0


def reserved_maintenence_resources(
    resources: MesosResources,
):
    return resources.get(MAINTENANCE_ROLE, {"cpus": 0, "mem": 0, "disk": 0, "gpus": 0})
