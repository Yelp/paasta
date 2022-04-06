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
import re

import mock
import pytest
from kubernetes.client import V1Container
from kubernetes.client import V1Node
from kubernetes.client import V1NodeStatus
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1Pod
from kubernetes.client import V1PodSpec
from kubernetes.client import V1PodStatus
from kubernetes.client import V1ResourceRequirements
from mock import Mock
from mock import patch

from paasta_tools.metrics import metastatus_lib
from paasta_tools.utils import PaastaColors


def test_ok_check_threshold():
    assert metastatus_lib.check_threshold(10, 30)


def test_fail_check_threshold():
    assert not metastatus_lib.check_threshold(80, 30)


def test_get_mesos_cpu_status():
    fake_metrics = {"master/cpus_total": 3, "master/cpus_used": 1}
    fake_mesos_state = {
        "slaves": [{"reserved_resources": {"maintenance": {"cpus": 1}}}]
    }
    total, used, available = metastatus_lib.get_mesos_cpu_status(
        fake_metrics, fake_mesos_state
    )
    assert total == 3
    assert used == 2
    assert available == 1


def test_get_kube_cpu_status():
    fake_nodes = [
        V1Node(status=V1NodeStatus(allocatable={"cpu": "1"}, capacity={"cpu": "3"}))
    ]
    total, used, available = metastatus_lib.get_kube_cpu_status(fake_nodes)
    assert total == 3
    assert used == 2
    assert available == 1


def test_ok_cpu_health():
    ok_status = (10, 1, 9)
    ok_output, ok_health = metastatus_lib.assert_cpu_health(ok_status)
    assert ok_health
    assert "CPUs: 1.00 / 10 in use (%s)" % PaastaColors.green("10.00%") in ok_output


def test_bad_cpu_health():
    failure_status = (10, 9, 1)
    failure_output, failure_health = metastatus_lib.assert_cpu_health(failure_status)
    assert not failure_health
    assert (
        "CRITICAL: Less than 10% CPUs available. (Currently using 90.00% of 10)"
        in failure_output
    )


def test_assert_memory_health():
    ok_status = (1024, 512, 512)
    ok_output, ok_health = metastatus_lib.assert_memory_health(ok_status)
    assert ok_health
    assert (
        "Memory: 0.50 / 1.00GB in use (%s)" % PaastaColors.green("50.00%") in ok_output
    )


def test_failing_memory_health():
    failure_status = (1024, 1000, 24)
    failure_output, failure_health = metastatus_lib.assert_memory_health(failure_status)
    assert not failure_health
    assert (
        "CRITICAL: Less than 10% memory available. (Currently using 97.66% of 1.00GB)"
        in failure_output
    )


def test_assert_disk_health():
    ok_status = (1024, 512, 512)
    ok_output, ok_health = metastatus_lib.assert_disk_health(ok_status)
    assert ok_health
    assert "Disk: 0.50 / 1.00GB in use (%s)" % PaastaColors.green("50.00%") in ok_output


def test_failing_disk_health():
    failure_status = (1024, 1000, 24)
    failure_output, failure_health = metastatus_lib.assert_disk_health(failure_status)
    assert not failure_health
    assert (
        "CRITICAL: Less than 10% disk available. (Currently using 97.66%)"
        in failure_output
    )


def test_assert_gpu_health():
    ok_status = (3, 1, 2)
    ok_output, ok_health = metastatus_lib.assert_gpu_health(ok_status)
    assert ok_health
    assert "GPUs: 1 / 3 in use (%s)" % PaastaColors.green("33.33%") in ok_output


def test_assert_no_gpu_health():
    zero_status = (0, 0, 0)
    zero_output, zero_health = metastatus_lib.assert_gpu_health(zero_status)
    assert zero_health
    assert "No GPUs found!" in zero_output


def test_assert_bad_gpu_health():
    bad_status = (4, 3, 1)
    bad_output, bad_health = metastatus_lib.assert_gpu_health(bad_status, threshold=50)
    assert not bad_health
    assert (
        "CRITICAL: Less than 50% GPUs available. (Currently using 75.00% of 4)"
        in bad_output
    )


def test_cpu_health_mesos_reports_zero():
    status = (0, 1, 42)
    failure_output, failure_health = metastatus_lib.assert_cpu_health(status)
    assert failure_output == "Error reading total available cpu from mesos!"
    assert failure_health is False


def test_memory_health_mesos_reports_zero():
    status = (0, 1, 42)
    failure_output, failure_health = metastatus_lib.assert_memory_health(status)
    assert failure_output == "Error reading total available memory from mesos!"
    assert failure_health is False


def test_disk_health_mesos_reports_zero():
    status = (0, 1, 42)
    failure_output, failure_health = metastatus_lib.assert_disk_health(status)
    assert failure_output == "Error reading total available disk from mesos!"
    assert failure_health is False


def test_assert_no_duplicate_frameworks():
    state = {
        "frameworks": [
            {"name": "test_framework1"},
            {"name": "test_framework2"},
            {"name": "test_framework3"},
            {"name": "test_framework4"},
        ]
    }
    output, ok = metastatus_lib.assert_no_duplicate_frameworks(
        state,
        ["test_framework1", "test_framework2", "test_framework3", "test_framework4"],
    )

    expected_output = "\n".join(
        ["Frameworks:"]
        + ["    Framework: %s count: 1" % x["name"] for x in state["frameworks"]]
    )
    assert output == expected_output
    assert ok


def test_duplicate_frameworks():
    state = {
        "frameworks": [
            {"name": "test_framework1"},
            {"name": "test_framework1"},
            {"name": "test_framework1"},
            {"name": "test_framework2"},
        ]
    }
    output, ok = metastatus_lib.assert_no_duplicate_frameworks(
        state,
        ["test_framework1", "test_framework2", "test_framework3", "test_framework4"],
    )
    assert (
        "    CRITICAL: There are 3 connected test_framework1 frameworks! (Expected 1)"
        in output
    )
    assert not ok


def test_duplicate_frameworks_not_checked():
    state = {
        "frameworks": [
            {"name": "test_framework1"},
            {"name": "test_framework1"},
            {"name": "test_framework1"},
            {"name": "test_framework2"},
        ]
    }
    output, ok = metastatus_lib.assert_no_duplicate_frameworks(
        state, ["test_framework2", "test_framework3", "test_framework4"]
    )
    assert "test_framework2" in output
    assert ok


@pytest.fixture
def mock_get_all_marathon_apps():
    with patch(
        "paasta_tools.metrics.metastatus_lib.get_all_marathon_apps", autospec=True
    ) as mock_get_all_marathon_apps:
        yield mock_get_all_marathon_apps


def test_ok_marathon_apps(mock_get_all_marathon_apps):
    client = mock.Mock()
    mock_get_all_marathon_apps.return_value = ["MarathonApp::1", "MarathonApp::2"]
    output, ok = metastatus_lib.assert_marathon_apps([client])
    assert re.match("marathon apps: +2", output)
    assert ok


def test_no_marathon_apps(mock_get_all_marathon_apps):
    client = mock.Mock()
    mock_get_all_marathon_apps.return_value = []
    output, ok = metastatus_lib.assert_marathon_apps([client])
    assert "CRITICAL: No marathon apps running" in output
    assert not ok


def test_marathon_tasks():
    client = Mock()
    client.list_tasks.return_value = ["MarathonTask:1"]
    output, ok = metastatus_lib.assert_marathon_tasks([client])
    assert re.match("marathon tasks: +1", output)
    assert ok


def test_assert_marathon_deployments():
    client = Mock()
    client.list_deployments.return_value = ["MarathonDeployment:1"]
    output, ok = metastatus_lib.assert_marathon_deployments([client])
    assert re.match("marathon deployments: +1", output)
    assert ok


def test_assert_kube_deployments():
    with mock.patch(
        "paasta_tools.metrics.metastatus_lib.list_all_deployments", autospec=True
    ) as mock_list_all_deployments:
        client = Mock()
        mock_list_all_deployments.return_value = ["KubeDeployment:1"]
        output, ok = metastatus_lib.assert_kube_deployments(client)
        assert re.match("Kubernetes deployments:   1", output)
        assert ok


def test_assert_kube_pods_running():
    with mock.patch(
        "paasta_tools.metrics.metastatus_lib.get_all_pods_cached", autospec=True
    ) as mock_get_all_pods:
        client = Mock()
        mock_get_all_pods.return_value = [
            V1Pod(status=V1PodStatus(phase="Running")),
            V1Pod(status=V1PodStatus(phase="Pending")),
            V1Pod(status=V1PodStatus(phase="Pending")),
            V1Pod(status=V1PodStatus(phase="Failed")),
            V1Pod(status=V1PodStatus(phase="Failed")),
            V1Pod(status=V1PodStatus(phase="Failed")),
        ]
        output, ok = metastatus_lib.assert_kube_pods_running(client)
        assert re.match("Pods: running: 1 pending: 2 failed: 3", output)
        assert ok


def test_assert_nodes_health():
    nodes_health_status = (10, 10)
    output, ok = metastatus_lib.assert_nodes_health(nodes_health_status)
    assert "Nodes: active: 10 inactive: 10" in output
    assert ok


def test_get_mesos_slaves_health_status():
    fake_slave_info = {"master/slaves_active": 10, "master/slaves_inactive": 7}
    active, inactive = metastatus_lib.get_mesos_slaves_health_status(fake_slave_info)
    assert active == 10
    assert inactive == 7


def test_assert_mesos_tasks_running():
    fake_tasks_info = {
        "master/tasks_running": 20,
        "master/tasks_staging": 10,
        "master/tasks_starting": 10,
    }
    output, ok = metastatus_lib.assert_mesos_tasks_running(fake_tasks_info)
    assert "Tasks: running: 20 staging: 10 starting: 10" in output
    assert ok


@patch("paasta_tools.metrics.metastatus_lib.get_mesos_quorum", autospec=True)
@patch("paasta_tools.metrics.metastatus_lib.get_num_masters", autospec=True)
def test_healthy_asssert_quorum_size(mock_num_masters, mock_quorum_size):
    mock_num_masters.return_value = 5
    mock_quorum_size.return_value = 3
    output, health = metastatus_lib.assert_quorum_size()
    assert health
    assert "Quorum: masters: 5 configured quorum: 3 " in output


@patch("paasta_tools.metrics.metastatus_lib.get_mesos_quorum", autospec=True)
@patch("paasta_tools.metrics.metastatus_lib.get_num_masters", autospec=True)
def test_unhealthy_asssert_quorum_size(mock_num_masters, mock_quorum_size):
    mock_num_masters.return_value = 1
    mock_quorum_size.return_value = 3
    output, health = metastatus_lib.assert_quorum_size()
    assert not health
    assert "CRITICAL: Number of masters (1) less than configured quorum(3)." in output


def test_get_marathon_status(mock_get_all_marathon_apps):
    client = Mock()
    mock_get_all_marathon_apps.return_value = ["MarathonApp::1", "MarathonApp::2"]
    client.list_deployments.return_value = ["MarathonDeployment::1"]
    client.list_tasks.return_value = [
        "MarathonTask::1",
        "MarathonTask::2",
        "MarathonTask::3",
    ]
    expected_apps_output = ("marathon apps:          2", True)
    expected_deployment_output = ("marathon deployments:   1", True)
    expected_tasks_output = ("marathon tasks:         3", True)

    results = metastatus_lib.get_marathon_status([client])

    assert expected_apps_output in results
    assert expected_deployment_output in results
    assert expected_tasks_output in results


def test_status_for_results():
    assert metastatus_lib.status_for_results(
        [
            metastatus_lib.HealthCheckResult(message="message", healthy=True),
            metastatus_lib.HealthCheckResult(message="message", healthy=False),
        ]
    ) == [True, False]


def test_generate_summary_for_results_ok():
    assert metastatus_lib.generate_summary_for_check(
        "Myservice", True
    ) == "Myservice Status: %s" % PaastaColors.green("OK")


def test_generate_summary_for_results_critical():
    assert metastatus_lib.generate_summary_for_check(
        "Myservice", False
    ) == "Myservice Status: %s" % PaastaColors.red("CRITICAL")


def test_critical_events_in_outputs():
    assert metastatus_lib.critical_events_in_outputs(
        [
            metastatus_lib.HealthCheckResult("myservice", True),
            metastatus_lib.HealthCheckResult("myservice_false", False),
        ]
    ) == [("myservice_false", False)]


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


def test_filter_kube_resources():
    test_resource_dictionary = {
        "cpu": 0,
        "memory": 1,
        "MEMORY": 2,
        "garbage_data": 3,
        "ephemeral-storage": 4,
        "nvidia.com/gpu": 5,
    }
    expected = {"cpu": 0, "memory": 1, "ephemeral-storage": 4, "nvidia.com/gpu": 5}
    assert metastatus_lib.filter_kube_resources(test_resource_dictionary) == expected


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


def test_calculate_resource_utilization_for_kube_nodes():
    fake_nodes = [
        V1Node(
            metadata=V1ObjectMeta(name="fake_node1"),
            status=V1NodeStatus(
                allocatable={
                    "cpu": "500",
                    "ephemeral-storage": "200Mi",
                    "memory": "750Mi",
                },
            ),
        )
    ]
    fake_pods_by_node = {
        "fake_node1": [
            V1Pod(
                metadata=V1ObjectMeta(name="pod1"),
                status=V1PodStatus(phase="Running"),
                spec=V1PodSpec(
                    containers=[
                        V1Container(
                            name="container1",
                            resources=V1ResourceRequirements(
                                requests={
                                    "cpu": "20",
                                    "ephemeral-storage": "20Mi",
                                    "memory": "20Mi",
                                }
                            ),
                        )
                    ]
                ),
            )
        ]
    }
    free = metastatus_lib.calculate_resource_utilization_for_kube_nodes(
        nodes=fake_nodes, pods_by_node=fake_pods_by_node
    )["free"]

    assert free.cpus == 480
    assert free.mem == 730
    assert free.disk == 180


def test_healthcheck_result_for_resource_utilization_ok():
    expected_message = "cpus: 5.00/10.00(50.00%) used. Threshold (90.00%)"
    expected = metastatus_lib.HealthCheckResult(message=expected_message, healthy=True)
    resource_utilization = metastatus_lib.ResourceUtilization(
        metric="cpus", total=10, free=5
    )
    assert (
        metastatus_lib.healthcheck_result_for_resource_utilization(
            resource_utilization=resource_utilization, threshold=90
        )
        == expected
    )


def test_healthcheck_result_for_resource_utilization_unhealthy():
    expected_message = "cpus: 5.00/10.00(50.00%) used. Threshold (10.00%)"
    expected = metastatus_lib.HealthCheckResult(message=expected_message, healthy=False)
    resource_utilization = metastatus_lib.ResourceUtilization(
        metric="cpus", total=10, free=5
    )
    assert (
        metastatus_lib.healthcheck_result_for_resource_utilization(
            resource_utilization=resource_utilization, threshold=10
        )
        == expected
    )


def test_healthcheck_result_for_resource_utilization_zero():
    expected_message = "cpus: 0.00/0.00(0.00%) used. Threshold (10.00%)"
    expected = metastatus_lib.HealthCheckResult(message=expected_message, healthy=True)
    resource_utilization = metastatus_lib.ResourceUtilization(
        metric="cpus", total=0, free=0
    )
    assert (
        metastatus_lib.healthcheck_result_for_resource_utilization(
            resource_utilization=resource_utilization, threshold=10
        )
        == expected
    )


def test_format_table_column_for_healthcheck_resource_utilization_pair_healthy_human_non_cpu():
    fake_healthcheckresult = Mock()
    fake_healthcheckresult.healthy = True
    fake_healthcheckresult.metric = "mem"
    fake_resource_utilization = Mock()
    fake_resource_utilization.free = 10
    fake_resource_utilization.total = 20
    fake_resource_utilization.metric = "mem"
    expected = PaastaColors.green("10.0M/20.0M (50.00%)")
    assert (
        metastatus_lib.format_table_column_for_healthcheck_resource_utilization_pair(
            (fake_healthcheckresult, fake_resource_utilization)
        )
        == expected
    )


def test_format_table_column_for_healthcheck_resource_utilization_pair_healthy_human_cpu():
    fake_healthcheckresult = Mock()
    fake_healthcheckresult.healthy = True
    fake_healthcheckresult.metric = "mem"
    fake_resource_utilization = Mock()
    fake_resource_utilization.free = 10.114
    fake_resource_utilization.total = 20
    fake_resource_utilization.metric = "cpus"
    expected = PaastaColors.green("9.89/20 (49.43%)")
    assert (
        metastatus_lib.format_table_column_for_healthcheck_resource_utilization_pair(
            (fake_healthcheckresult, fake_resource_utilization)
        )
        == expected
    )


def test_format_table_column_for_healthcheck_resource_utilization_pair_unhealthy_human():
    fake_healthcheckresult = Mock()
    fake_healthcheckresult.healthy = False
    fake_healthcheckresult.metric = "mem"
    fake_resource_utilization = Mock()
    fake_resource_utilization.free = 10
    fake_resource_utilization.total = 20
    expected = PaastaColors.red("10.0M/20.0M (50.00%)")
    assert (
        metastatus_lib.format_table_column_for_healthcheck_resource_utilization_pair(
            (fake_healthcheckresult, fake_resource_utilization)
        )
        == expected
    )


def test_format_table_column_for_healthcheck_resource_utilization_pair_zero_human():
    fake_healthcheckresult = Mock()
    fake_healthcheckresult.healthy = False
    fake_healthcheckresult.metric = "mem"
    fake_resource_utilization = Mock()
    fake_resource_utilization.free = 0
    fake_resource_utilization.total = 0
    expected = PaastaColors.red("0B/0B (100.00%)")
    assert (
        metastatus_lib.format_table_column_for_healthcheck_resource_utilization_pair(
            (fake_healthcheckresult, fake_resource_utilization)
        )
        == expected
    )


@patch(
    "paasta_tools.metrics.metastatus_lib.format_table_column_for_healthcheck_resource_utilization_pair",
    autospec=True,
)
def test_format_row_for_resource_utilization_checks(mock_format_row):
    fake_pairs = [(Mock(), Mock()), (Mock(), Mock()), (Mock(), Mock())]
    assert metastatus_lib.format_row_for_resource_utilization_healthchecks(fake_pairs)
    assert mock_format_row.call_count == len(fake_pairs)


@patch(
    "paasta_tools.metrics.metastatus_lib.format_row_for_resource_utilization_healthchecks",
    autospec=True,
)
def test_get_table_rows_for_resource_usage_dict(mock_format_row):
    fake_pairs = [(Mock(), Mock()), (Mock(), Mock()), (Mock(), Mock())]
    mock_format_row.return_value = ["10/10", "10/10", "10/10"]
    actual = metastatus_lib.get_table_rows_for_resource_info_dict(
        ["myhabitat"], fake_pairs
    )
    assert actual == ["myhabitat", "10/10", "10/10", "10/10"]


def test_key_func_for_attribute():
    assert inspect.isfunction(metastatus_lib.key_func_for_attribute("habitat"))


def test_get_mesos_memory_status():
    metrics = {"master/mem_total": 100, "master/mem_used": 50}
    fake_mesos_state = {
        "slaves": [{"reserved_resources": {"maintenance": {"mem": 33}}}]
    }
    actual = metastatus_lib.get_mesos_memory_status(metrics, fake_mesos_state)
    assert actual == (100, 83, 17)


def test_get_kube_memory_status():
    fake_nodes = [
        V1Node(
            status=V1NodeStatus(
                allocatable={"memory": "1Gi"}, capacity={"memory": "4Gi"}
            )
        )
    ]
    total, used, available = metastatus_lib.get_kube_memory_status(fake_nodes)
    assert total == 4 * 1024
    assert used == 3 * 1024
    assert available == 1 * 1024


def test_get_mesos_disk_status():
    metrics = {"master/disk_total": 100, "master/disk_used": 50}
    fake_mesos_state = {
        "slaves": [{"reserved_resources": {"maintenance": {"disk": 33}}}]
    }
    actual = metastatus_lib.get_mesos_disk_status(metrics, fake_mesos_state)
    assert actual == (100, 83, 17)


def test_get_kube_disk_status():
    fake_nodes = [
        V1Node(
            status=V1NodeStatus(
                allocatable={"ephemeral-storage": "1Ti"},
                capacity={"ephemeral-storage": "4Ti"},
            )
        )
    ]
    total, used, available = metastatus_lib.get_kube_disk_status(fake_nodes)
    assert total == 4 * 1024**2
    assert used == 3 * 1024**2
    assert available == 1 * 1024**2


def test_get_mesos_gpu_status():
    metrics = {"master/gpus_total": 10, "master/gpus_used": 5}
    fake_mesos_state = {
        "slaves": [{"reserved_resources": {"maintenance": {"gpus": 2}}}]
    }
    actual = metastatus_lib.get_mesos_gpu_status(metrics, fake_mesos_state)
    assert actual == (10, 7, 3)


def test_get_kube_gpu_status():
    fake_nodes = [
        V1Node(
            status=V1NodeStatus(
                allocatable={"nvidia.com/gpu": "1"}, capacity={"nvidia.com/gpu": "4"}
            )
        )
    ]
    total, used, available = metastatus_lib.get_kube_gpu_status(fake_nodes)
    assert total == 4
    assert used == 3
    assert available == 1


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
