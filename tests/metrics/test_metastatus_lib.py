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
import inspect

import mock
from mock import Mock
from mock import patch

from paasta_tools.metrics import metastatus_lib


def test_filter_mesos_state_metrics():
    test_resource_dictionary = {
        "cpus": 0,
        "mem": 1,
        "MEM": 2,
        "garbage_data": 3,
        "disk": 4,
        "gpus": 5,
    }
    expected = {"cpus": 0, "mem": 1, "disk": 4, "gpus": 5}
    assert (
        metastatus_lib.filter_mesos_state_metrics(test_resource_dictionary) == expected
    )


def test_filter_slaves():
    filters = {"foo": ["one", "two"], "bar": ["three", "four"]}
    fns = [metastatus_lib.make_filter_slave_func(k, v) for k, v in filters.items()]

    data = [
        {"name": "aaa", "attributes": {"foo": "one", "bar": "three"}},
        {"name": "bbb", "attributes": {"foo": "one"}},
        {"name": "ccc", "attributes": {"foo": "wrong", "bar": "four"}},
    ]

    slaves = metastatus_lib.filter_slaves(data, fns)
    names = [s["name"] for s in slaves]
    assert "aaa" in names
    assert "bbb" not in names
    assert "ccc" not in names


def test_group_slaves_by_key_func():
    slaves = [
        {
            "id": "somenametest-slave",
            "hostname": "test.somewhere.www",
            "resources": {"cpus": 75, "disk": 250, "mem": 100},
            "attributes": {"habitat": "somenametest-habitat"},
        },
        {
            "id": "somenametest-slave2",
            "hostname": "test2.somewhere.www",
            "resources": {"cpus": 500, "disk": 200, "mem": 750},
            "attributes": {"habitat": "somenametest-habitat-2"},
        },
    ]
    actual = metastatus_lib.group_slaves_by_key_func(
        lambda x: x["attributes"]["habitat"], slaves
    )
    assert len(actual.items()) == 2
    for k, v in actual.items():
        print(k, v)
        assert len(list(v)) == 1


@patch("paasta_tools.metrics.metastatus_lib.group_slaves_by_key_func", autospec=True)
@patch(
    "paasta_tools.metrics.metastatus_lib.calculate_resource_utilization_for_slaves",
    autospec=True,
)
@patch("paasta_tools.metrics.metastatus_lib.get_all_tasks_from_state", autospec=True)
def test_get_resource_utilization_by_grouping(
    mock_get_all_tasks_from_state,
    mock_calculate_resource_utilization_for_slaves,
    mock_group_slaves_by_key_func,
):
    mock_group_slaves_by_key_func.return_value = {
        "somenametest-habitat": [{"id": "abcd", "hostname": "test.somewhere.www"}],
        "somenametest-habitat-2": [{"id": "abcd", "hostname": "test2.somewhere.www"}],
    }
    mock_calculate_resource_utilization_for_slaves.return_value = {
        "free": metastatus_lib.ResourceInfo(cpus=10, mem=10, disk=10),
        "total": metastatus_lib.ResourceInfo(cpus=20, mem=20, disk=20),
    }
    state = {"frameworks": Mock(), "slaves": [{"id": "abcd"}]}
    actual = metastatus_lib.get_resource_utilization_by_grouping(
        grouping_func=mock.sentinel.grouping_func, mesos_state=state
    )
    mock_get_all_tasks_from_state.assert_called_with(state, include_orphans=True)
    assert sorted(actual.keys()) == sorted(
        ["somenametest-habitat", "somenametest-habitat-2"]
    )
    for k, v in actual.items():
        assert v["total"] == metastatus_lib.ResourceInfo(cpus=20, disk=20, mem=20)
        assert v["free"] == metastatus_lib.ResourceInfo(cpus=10, disk=10, mem=10)


def test_get_resource_utilization_by_grouping_correctly_groups():
    fake_state = {
        "slaves": [
            {
                "id": "foo",
                "resources": {"disk": 100, "cpus": 10, "mem": 50},
                "reserved_resources": {},
            },
            {
                "id": "bar",
                "resources": {"disk": 100, "cpus": 10, "mem": 50},
                "reserved_resources": {},
            },
        ],
        "frameworks": [
            {
                "tasks": [
                    {
                        "state": "TASK_RUNNING",
                        "resources": {"cpus": 1, "mem": 10, "disk": 10},
                        "slave_id": "foo",
                    },
                    {
                        "state": "TASK_RUNNING",
                        "resources": {"cpus": 1, "mem": 10, "disk": 10},
                        "slave_id": "bar",
                    },
                ]
            }
        ],
    }

    def grouping_func(x):
        return x["id"]

    free_cpus = metastatus_lib.get_resource_utilization_by_grouping(
        mesos_state=fake_state, grouping_func=grouping_func
    )["foo"]["free"].cpus
    assert free_cpus == 9


def test_get_resource_utilization_by_grouping_correctly_multi_groups():
    fake_state = {
        "slaves": [
            {
                "id": "foo1",
                "resources": {"disk": 100, "cpus": 10, "mem": 50},
                "attributes": {"one": "yes", "two": "yes"},
                "reserved_resources": {},
            },
            {
                "id": "bar1",
                "resources": {"disk": 100, "cpus": 10, "mem": 50},
                "attributes": {"one": "yes", "two": "no"},
                "reserved_resources": {},
            },
            {
                "id": "foo2",
                "resources": {"disk": 100, "cpus": 10, "mem": 50},
                "attributes": {"one": "no", "two": "yes"},
                "reserved_resources": {},
            },
            {
                "id": "bar2",
                "resources": {"disk": 100, "cpus": 10, "mem": 50},
                "attributes": {"one": "no", "two": "no"},
                "reserved_resources": {},
            },
        ],
        "frameworks": [
            {
                "tasks": [
                    {
                        "state": "TASK_RUNNING",
                        "resources": {"cpus": 1, "mem": 10, "disk": 10},
                        "slave_id": "foo1",
                    },
                    {
                        "state": "TASK_RUNNING",
                        "resources": {"cpus": 1, "mem": 10, "disk": 10},
                        "slave_id": "bar1",
                    },
                ]
            }
        ],
    }

    grouping_func = metastatus_lib.key_func_for_attribute_multi(["one", "two"])
    resp = metastatus_lib.get_resource_utilization_by_grouping(
        mesos_state=fake_state, grouping_func=grouping_func
    )
    # resp should have 4 keys...
    assert len(resp.keys()) == 4
    # Each key should be a set with 2 items...
    assert len(list(resp.keys())[0]) == 2
    # Each item in the set should have 2 values (original key, value)
    assert len(list(list(resp.keys())[0])[0]) == 2


def test_get_resource_utilization_per_slave():
    tasks = [
        {"resources": {"cpus": 10, "mem": 10, "disk": 10}, "state": "TASK_RUNNING"},
        {"resources": {"cpus": 10, "mem": 10, "disk": 10}, "state": "TASK_RUNNING"},
    ]
    slaves = [
        {
            "id": "somenametest-slave",
            "hostname": "test.somewhere.www",
            "resources": {"cpus": 75, "disk": 250, "mem": 100},
            "reserved_resources": {},
            "attributes": {"habitat": "somenametest-habitat"},
        },
        {
            "id": "somenametest-slave2",
            "hostname": "test2.somewhere.www",
            "resources": {"cpus": 500, "disk": 200, "mem": 750},
            "reserved_resources": {"maintenance": {"cpus": 10, "disk": 0, "mem": 150}},
            "attributes": {"habitat": "somenametest-habitat-2"},
        },
    ]
    actual = metastatus_lib.calculate_resource_utilization_for_slaves(
        slaves=slaves, tasks=tasks
    )
    assert sorted(actual.keys()) == sorted(["total", "free", "slave_count"])
    assert actual["total"] == metastatus_lib.ResourceInfo(cpus=575, disk=450, mem=850)
    assert actual["free"] == metastatus_lib.ResourceInfo(cpus=545, disk=430, mem=680)
    assert actual["slave_count"] == 2


def test_calculate_resource_utilization_for_slaves():
    fake_slaves = [
        {
            "id": "somenametest-slave2",
            "hostname": "test2.somewhere.www",
            "resources": {"cpus": 500, "disk": 200, "mem": 750, "gpus": 5},
            "reserved_resources": {},
            "attributes": {"habitat": "somenametest-habitat-2"},
        }
    ]
    tasks = [
        {
            "resources": {"cpus": 10, "mem": 10, "disk": 10, "gpus": 1},
            "state": "TASK_RUNNING",
        },
        {
            "resources": {"cpus": 10, "mem": 10, "disk": 10, "gpus": 2},
            "state": "TASK_RUNNING",
        },
    ]
    free = metastatus_lib.calculate_resource_utilization_for_slaves(
        slaves=fake_slaves, tasks=tasks
    )["free"]

    assert free.cpus == 480
    assert free.mem == 730
    assert free.disk == 180
    assert free.gpus == 2


def test_key_func_for_attribute():
    assert inspect.isfunction(metastatus_lib.key_func_for_attribute("habitat"))


def test_reserved_maintenence_resources_no_maintenenance():
    actual = metastatus_lib.reserved_maintenence_resources({})
    assert all([actual[x] == 0 for x in ["cpus", "mem", "disk"]])


def test_reserved_maintenence_resources():
    actual = metastatus_lib.reserved_maintenence_resources(
        {"maintenance": {"cpus": 5, "mem": 5, "disk": 5}}
    )
    assert all([actual[x] == 5 for x in ["cpus", "mem", "disk"]])


def test_reserved_maintenence_resources_ignores_non_maintenance():
    actual = metastatus_lib.reserved_maintenence_resources(
        {
            "maintenance": {"cpus": 5, "mem": 5, "disk": 5},
            "myotherole": {"cpus": 5, "mem": 5, "disk": 5},
        }
    )
    assert all([actual[x] == 5 for x in ["cpus", "mem", "disk"]])


def test_suffixed_number_value():
    assert metastatus_lib.suffixed_number_value("5k") == 5 * 1000
    assert metastatus_lib.suffixed_number_value("5m") == 5 * 1000**-1
    assert metastatus_lib.suffixed_number_value("5M") == 5 * 1000**2
    assert metastatus_lib.suffixed_number_value("5G") == 5 * 1000**3
    assert metastatus_lib.suffixed_number_value("5T") == 5 * 1000**4
    assert metastatus_lib.suffixed_number_value("5P") == 5 * 1000**5
    assert metastatus_lib.suffixed_number_value("5Ki") == 5 * 1024
    assert metastatus_lib.suffixed_number_value("5Mi") == 5 * 1024**2
    assert metastatus_lib.suffixed_number_value("5Gi") == 5 * 1024**3
    assert metastatus_lib.suffixed_number_value("5Ti") == 5 * 1024**4
    assert metastatus_lib.suffixed_number_value("5Pi") == 5 * 1024**5
