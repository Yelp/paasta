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
from mock import Mock
from mock import patch

from paasta_tools.marathon_tools import MarathonConfig
from paasta_tools.metrics import metastatus_lib
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors


def test_ok_check_threshold():
    assert metastatus_lib.check_threshold(10, 30)


def test_fail_check_threshold():
    assert not metastatus_lib.check_threshold(80, 30)


def test_get_mesos_cpu_status():
    fake_metrics = {
        'master/cpus_total': 3,
        'master/cpus_used': 1,
    }
    fake_mesos_state = {
        'slaves': [
            {
                'reserved_resources': {
                    'maintenance': {
                        'cpus': 1,
                    },
                },
            },
        ],
    }
    total, used, available = metastatus_lib.get_mesos_cpu_status(fake_metrics, fake_mesos_state)
    assert total == 3
    assert used == 2
    assert available == 1


def test_ok_cpu_health():
    ok_metrics = {
        'master/cpus_total': 10,
        'master/cpus_used': 0.5,
    }
    fake_mesos_state = {
        'slaves': [
            {
                'reserved_resources': {
                    'maintenance': {
                        'cpus': 0.5,
                    },
                },
            },
        ],
    }
    ok_output, ok_health = metastatus_lib.assert_cpu_health(ok_metrics, fake_mesos_state)
    assert ok_health
    assert "CPUs: 1.00 / 10 in use (%s)" % PaastaColors.green("10.00%") in ok_output


def test_bad_cpu_health():
    failure_metrics = {
        'master/cpus_total': 10,
        'master/cpus_used': 8,
    }
    fake_mesos_state = {
        'slaves': [
            {
                'reserved_resources': {
                    'maintenance': {
                        'cpus': 1,
                    },
                },
            },
        ],
    }
    failure_output, failure_health = metastatus_lib.assert_cpu_health(failure_metrics, fake_mesos_state)
    assert not failure_health
    assert "CRITICAL: Less than 10% CPUs available. (Currently using 90.00% of 10)" in failure_output


def test_assert_memory_health():
    ok_metrics = {
        'master/mem_total': 1024,
        'master/mem_used': 256,
    }
    fake_mesos_state = {
        'slaves': [
            {
                'reserved_resources': {
                    'maintenance': {
                        'mem': 256,
                    },
                },
            },
        ],
    }
    ok_output, ok_health = metastatus_lib.assert_memory_health(ok_metrics, fake_mesos_state)
    assert ok_health
    assert "Memory: 0.50 / 1.00GB in use (%s)" % PaastaColors.green("50.00%") in ok_output


def test_failing_memory_health():
    failure_metrics = {
        'master/mem_total': 1024,
        'master/mem_used': 500,
    }
    fake_mesos_state = {
        'slaves': [
            {
                'reserved_resources': {
                    'maintenance': {
                        'mem': 500,
                    },
                },
            },
        ],
    }
    failure_output, failure_health = metastatus_lib.assert_memory_health(failure_metrics, fake_mesos_state)
    assert not failure_health
    assert "CRITICAL: Less than 10% memory available. (Currently using 97.66% of 1.00GB)" in failure_output


def test_assert_disk_health():
    ok_metrics = {
        'master/disk_total': 1024,
        'master/disk_used': 256,
    }
    fake_mesos_state = {
        'slaves': [
            {
                'reserved_resources': {
                    'maintenance': {
                        'disk': 256,
                    },
                },
            },
        ],
    }
    ok_output, ok_health = metastatus_lib.assert_disk_health(ok_metrics, fake_mesos_state)
    assert ok_health
    assert "Disk: 0.50 / 1.00GB in use (%s)" % PaastaColors.green("50.00%") in ok_output


def test_failing_disk_health():
    failure_metrics = {
        'master/disk_total': 1024,
        'master/disk_used': 500,
    }
    fake_mesos_state = {
        'slaves': [
            {
                'reserved_resources': {
                    'maintenance': {
                        'disk': 500,
                    },
                },
            },
        ],
    }
    failure_output, failure_health = metastatus_lib.assert_disk_health(failure_metrics, fake_mesos_state)
    assert not failure_health
    assert "CRITICAL: Less than 10% disk available. (Currently using 97.66%)" in failure_output


def test_assert_gpu_health():
    ok_metrics = {
        'master/gpus_total': 3,
        'master/gpus_used': 1,
    }
    ok_output, ok_health = metastatus_lib.assert_gpu_health(ok_metrics)
    assert ok_health
    assert "GPUs: 1 / 3 in use (%s)" % PaastaColors.green("33.33%") in ok_output


def test_assert_no_gpu_health():
    zero_metrics = {
        'master/gpus_total': 0,
        'master/gpus_used': 0,
    }
    zero_output, zero_health = metastatus_lib.assert_gpu_health(zero_metrics)
    assert zero_health
    assert "No gpus found from mesos!" in zero_output


def test_assert_bad_gpu_health():
    bad_metrics = {
        'master/gpus_total': 4,
        'master/gpus_used': 3,
    }
    bad_output, bad_health = metastatus_lib.assert_gpu_health(bad_metrics, threshold=50)
    assert not bad_health
    assert "CRITICAL: Less than 50% GPUs available. (Currently using 75.00% of 4)" in bad_output


def test_cpu_health_mesos_reports_zero():
    mesos_metrics = {
        'master/cpus_total': 0,
        'master/cpus_used': 1,
    }
    fake_mesos_state = {'slaves': []}
    failure_output, failure_health = metastatus_lib.assert_cpu_health(mesos_metrics, fake_mesos_state)
    assert failure_output == "Error reading total available cpu from mesos!"
    assert failure_health is False


def test_memory_health_mesos_reports_zero():
    mesos_metrics = {
        'master/mem_total': 0,
        'master/mem_used': 1,
    }
    fake_mesos_state = {'slaves': []}
    failure_output, failure_health = metastatus_lib.assert_memory_health(mesos_metrics, fake_mesos_state)
    assert failure_output == "Error reading total available memory from mesos!"
    assert failure_health is False


def test_disk_health_mesos_reports_zero():
    mesos_metrics = {
        'master/disk_total': 0,
        'master/disk_used': 1,
    }
    fake_mesos_state = {'slaves': []}
    failure_output, failure_health = metastatus_lib.assert_disk_health(mesos_metrics, fake_mesos_state)
    assert failure_output == "Error reading total available disk from mesos!"
    assert failure_health is False


def test_assert_framework_count_not_ok():
    state = {
        'frameworks': [
            {
                'name': 'marathon',
                'id': 'id1',
            },
            {
                'name': 'marathon1',
                'id': 'id2',
            },
            {
                'name': 'marathon2',
                'id': 'id3',
            },
            {
                'name': 'chronos',
                'id': 'id_chronos',
            },
        ],
    }
    output, ok = metastatus_lib.assert_framework_count(
        state,
        marathon_framework_ids=['id1', 'id2'],
    )

    assert "CRITICAL: There are 3 marathon frameworks connected! (Expected 2)" in output
    assert not ok


def test_assert_framework_count_ok():
    state = {
        'frameworks': [
            {
                'name': 'chronos',
                'id': 'id_chronos',
            },
            {
                'name': 'marathon',
                'id': 'id1',
            },
            {
                'name': 'marathon1',
                'id': 'id2',
            },
            {
                'name': 'test_framework',
                'id': 'id_test',
            },
        ],
    }
    output, ok = metastatus_lib.assert_framework_count(
        state,
        marathon_framework_ids=['id1', 'id2'],
    )
    assert "Framework: marathon count: 2" in output
    assert "Framework: chronos count: 1" in output
    assert ok


def test_ok_marathon_apps():
    client = Mock()
    client.list_apps.return_value = [
        "MarathonApp::1",
        "MarathonApp::2",
    ]
    output, ok = metastatus_lib.assert_marathon_apps([client])
    assert re.match("marathon apps: +2", output)
    assert ok


@patch('paasta_tools.marathon_tools.get_marathon_client', autospec=True)
def test_no_marathon_apps(mock_get_marathon_client):
    client = mock_get_marathon_client.return_value
    client.list_apps.return_value = []
    output, ok = metastatus_lib.assert_marathon_apps(client)
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


def test_assert_slave_health():
    fake_slave_info = {
        'master/slaves_active': 10,
        'master/slaves_inactive': 10,
    }
    output, ok = metastatus_lib.assert_slave_health(fake_slave_info)
    assert "Slaves: active: 10 inactive: 10" in output
    assert ok


def test_assert_tasks_running():
    fake_tasks_info = {
        'master/tasks_running': 20,
        'master/tasks_staging': 10,
        'master/tasks_starting': 10,
    }
    output, ok = metastatus_lib.assert_tasks_running(fake_tasks_info)
    assert "Tasks: running: 20 staging: 10 starting: 10" in output
    assert ok


@patch('paasta_tools.metrics.metastatus_lib.get_mesos_quorum', autospec=True)
@patch('paasta_tools.metrics.metastatus_lib.get_num_masters', autospec=True)
def test_healthy_asssert_quorum_size(mock_num_masters, mock_quorum_size):
    mock_num_masters.return_value = 5
    mock_quorum_size.return_value = 3
    output, health = metastatus_lib.assert_quorum_size()
    assert health
    assert 'Quorum: masters: 5 configured quorum: 3 ' in output


@patch('paasta_tools.metrics.metastatus_lib.get_mesos_quorum', autospec=True)
@patch('paasta_tools.metrics.metastatus_lib.get_num_masters', autospec=True)
def test_unhealthy_asssert_quorum_size(mock_num_masters, mock_quorum_size):
    mock_num_masters.return_value = 1
    mock_quorum_size.return_value = 3
    output, health = metastatus_lib.assert_quorum_size()
    assert not health
    assert "CRITICAL: Number of masters (1) less than configured quorum(3)." in output


def test_get_marathon_status():
    client = Mock()
    client.list_apps.return_value = [
        "MarathonApp::1",
        "MarathonApp::2",
    ]
    client.list_deployments.return_value = [
        "MarathonDeployment::1",
    ]
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


def test_get_marathon_client():
    fake_config = MarathonConfig({
        'url': 'fakeurl',
        'user': 'fakeuser',
        'password': 'fakepass',
    })
    client = metastatus_lib.get_marathon_client(fake_config)
    assert client.servers == ['fakeurl']
    assert client.auth == ('fakeuser', 'fakepass')


def test_assert_chronos_scheduled_jobs():
    mock_client = Mock()
    mock_client.list.return_value = [
        {'name': 'myjob', 'disabled': False},
        {'name': 'myjob', 'disabled': True},
    ]
    results = metastatus_lib.assert_chronos_scheduled_jobs(mock_client)
    assert results == ('Enabled chronos jobs: 1', True)


def test_assert_chronos_queued_jobs_no_queued():
    mock_client = Mock()
    mock_client.metrics.return_value = {
        'gauges': {
            metastatus_lib.HIGH_QUEUE_GAUGE: {'value': 0},
            metastatus_lib.QUEUE_GAUGE: {'value': 0},
        },
    }
    mock_client.list.return_value = [
        {'name': 'myjob', 'disabled': False},
        {'name': 'myjob', 'disabled': True},
    ]
    assert metastatus_lib.assert_chronos_queued_jobs(mock_client) == metastatus_lib.HealthCheckResult(
        message="Jobs Queued: 0 (0.0%)",
        healthy=True,
    )


def test_assert_chronos_queued_jobs_queued():
    mock_client = Mock()
    mock_client.metrics.return_value = {
        'gauges': {
            metastatus_lib.HIGH_QUEUE_GAUGE: {'value': 1},
            metastatus_lib.QUEUE_GAUGE: {'value': 0},
        },
    }
    mock_client.list.return_value = [
        {'name': 'myjob', 'disabled': False},
        {'name': 'myjob', 'disabled': False},
    ]
    assert metastatus_lib.assert_chronos_queued_jobs(mock_client) == metastatus_lib.HealthCheckResult(
        message="Jobs Queued: 1 (50.0%)",
        healthy=True,
    )


@patch('paasta_tools.metrics.metastatus_lib.assert_chronos_queued_jobs', autospec=True)
@patch('paasta_tools.metrics.metastatus_lib.assert_chronos_scheduled_jobs', autospec=True)
def test_get_chronos_status(mock_queued_jobs, mock_scheduled_jobs):
    mock_scheduled_jobs_result = metastatus_lib.HealthCheckResult(
        message='Enabled chronos jobs: 1',
        healthy=True,
    )
    mock_queued_jobs_result = metastatus_lib.HealthCheckResult(
        message="Jobs Queued: 0 (0%)",
        healthy=True,
    )
    mock_queued_jobs.return_value = mock_queued_jobs_result
    mock_scheduled_jobs.return_value = mock_scheduled_jobs_result

    expected_results = [mock_queued_jobs_result, mock_scheduled_jobs_result]

    assert metastatus_lib.get_chronos_status(Mock()) == expected_results


def test_status_for_results():
    assert metastatus_lib.status_for_results([
        metastatus_lib.HealthCheckResult(
            message='message',
            healthy=True,
        ),
        metastatus_lib.HealthCheckResult(
            message='message',
            healthy=False,
        ),
    ]) == [True, False]


def test_generate_summary_for_results_ok():
    assert (metastatus_lib.generate_summary_for_check("Myservice", True) ==
            "Myservice Status: %s" % PaastaColors.green("OK"))


def test_generate_summary_for_results_critical():
    assert (metastatus_lib.generate_summary_for_check("Myservice", False) ==
            "Myservice Status: %s" % PaastaColors.red("CRITICAL"))


def test_critical_events_in_outputs():
    assert (metastatus_lib.critical_events_in_outputs([
        metastatus_lib.HealthCheckResult('myservice', True),
        metastatus_lib.HealthCheckResult('myservice_false', False),
    ]) == [('myservice_false', False)])


def test_filter_mesos_state_metrics():
    test_resource_dictionary = {
        'cpus': 0,
        'mem': 1,
        'MEM': 2,
        'garbage_data': 3,
        'disk': 4,
        'gpus': 5,
    }
    expected = {
        'cpus': 0,
        'mem': 1,
        'disk': 4,
        'gpus': 5,
    }
    assert metastatus_lib.filter_mesos_state_metrics(test_resource_dictionary) == expected


def test_filter_slaves():
    filters = {"foo": ['one', 'two'], "bar": ['three', 'four']}
    fns = [
        metastatus_lib.make_filter_slave_func(k, v) for k, v in filters.items()
    ]

    data = [
        {"name": "aaa", "attributes": {"foo": "one", "bar": "three"}},
        {"name": "bbb", "attributes": {"foo": "one"}},
        {"name": "ccc", "attributes": {"foo": "wrong", "bar": "four"}},
    ]

    slaves = metastatus_lib.filter_slaves(data, fns)
    names = [s["name"] for s in slaves]
    assert("aaa" in names)
    assert("bbb" not in names)
    assert("ccc" not in names)


def test_group_slaves_by_key_func():
    slaves = [
        {
            'id': 'somenametest-slave',
            'hostname': 'test.somewhere.www',
            'resources': {
                'cpus': 75,
                'disk': 250,
                'mem': 100,
            },
            'attributes': {
                'habitat': 'somenametest-habitat',
            },
        },
        {
            'id': 'somenametest-slave2',
            'hostname': 'test2.somewhere.www',
            'resources': {
                'cpus': 500,
                'disk': 200,
                'mem': 750,
            },
            'attributes': {
                'habitat': 'somenametest-habitat-2',
            },
        },
    ]
    actual = metastatus_lib.group_slaves_by_key_func(
        lambda x: x['attributes']['habitat'],
        slaves,
    )
    assert len(actual.items()) == 2
    for k, v in actual.items():
        paasta_print(k, v)
        assert len(list(v)) == 1


@patch('paasta_tools.metrics.metastatus_lib.group_slaves_by_key_func', autospec=True)
@patch('paasta_tools.metrics.metastatus_lib.calculate_resource_utilization_for_slaves', autospec=True)
@patch('paasta_tools.metrics.metastatus_lib.get_all_tasks_from_state', autospec=True)
def test_get_resource_utilization_by_grouping(
        mock_get_all_tasks_from_state,
        mock_calculate_resource_utilization_for_slaves,
        mock_group_slaves_by_key_func,
):
    mock_group_slaves_by_key_func.return_value = {
        'somenametest-habitat': [{
            'id': 'abcd',
            'hostname': 'test.somewhere.www',
        }],
        'somenametest-habitat-2': [{
            'id': 'abcd',
            'hostname': 'test2.somewhere.www',
        }],
    }
    mock_calculate_resource_utilization_for_slaves.return_value = {
        'free': metastatus_lib.ResourceInfo(cpus=10, mem=10, disk=10),
        'total': metastatus_lib.ResourceInfo(cpus=20, mem=20, disk=20),
    }
    state = {
        'frameworks': Mock(),
        'slaves': [{'id': 'abcd'}],
    }
    actual = metastatus_lib.get_resource_utilization_by_grouping(
        grouping_func=mock.sentinel.grouping_func,
        mesos_state=state,
    )
    mock_get_all_tasks_from_state.assert_called_with(state, include_orphans=True)
    assert sorted(actual.keys()) == sorted(['somenametest-habitat', 'somenametest-habitat-2'])
    for k, v in actual.items():
        assert v['total'] == metastatus_lib.ResourceInfo(
            cpus=20,
            disk=20,
            mem=20,
        )
        assert v['free'] == metastatus_lib.ResourceInfo(
            cpus=10,
            disk=10,
            mem=10,
        )


def test_get_resource_utilization_by_grouping_correctly_groups():
    fake_state = {
        'slaves': [
            {
                'id': 'foo',
                'resources': {
                    'disk': 100,
                    'cpus': 10,
                    'mem': 50,
                },
                'reserved_resources': {},
            },
            {
                'id': 'bar',
                'resources': {
                    'disk': 100,
                    'cpus': 10,
                    'mem': 50,
                },
                'reserved_resources': {},
            },
        ],
        'frameworks': [
            {'tasks': [
                {
                    'state': 'TASK_RUNNING',
                    'resources': {'cpus': 1, 'mem': 10, 'disk': 10},
                    'slave_id': 'foo',
                },
                {
                    'state': 'TASK_RUNNING',
                    'resources': {'cpus': 1, 'mem': 10, 'disk': 10},
                    'slave_id': 'bar',
                },
            ]},
        ],
    }

    def grouping_func(x): return x['id']
    free_cpus = metastatus_lib.get_resource_utilization_by_grouping(
        mesos_state=fake_state,
        grouping_func=grouping_func,
    )['foo']['free'].cpus
    assert free_cpus == 9


def test_get_resource_utilization_by_grouping_correctly_multi_groups():
    fake_state = {
        'slaves': [
            {
                'id': 'foo1',
                'resources': {
                    'disk': 100,
                    'cpus': 10,
                    'mem': 50,
                },
                'attributes': {'one': 'yes', 'two': 'yes'},
                'reserved_resources': {},
            },
            {
                'id': 'bar1',
                'resources': {
                    'disk': 100,
                    'cpus': 10,
                    'mem': 50,
                },
                'attributes': {'one': 'yes', 'two': 'no'},
                'reserved_resources': {},
            },
            {
                'id': 'foo2',
                'resources': {
                    'disk': 100,
                    'cpus': 10,
                    'mem': 50,
                },
                'attributes': {'one': 'no', 'two': 'yes'},
                'reserved_resources': {},
            },
            {
                'id': 'bar2',
                'resources': {
                    'disk': 100,
                    'cpus': 10,
                    'mem': 50,
                },
                'attributes': {'one': 'no', 'two': 'no'},
                'reserved_resources': {},
            },
        ],
        'frameworks': [
            {'tasks': [
                {
                    'state': 'TASK_RUNNING',
                    'resources': {'cpus': 1, 'mem': 10, 'disk': 10},
                    'slave_id': 'foo1',
                },
                {
                    'state': 'TASK_RUNNING',
                    'resources': {'cpus': 1, 'mem': 10, 'disk': 10},
                    'slave_id': 'bar1',
                },
            ]},
        ],
    }

    grouping_func = metastatus_lib.key_func_for_attribute_multi(['one', 'two'])
    resp = metastatus_lib.get_resource_utilization_by_grouping(
        mesos_state=fake_state,
        grouping_func=grouping_func,
    )
    # resp should have 4 keys...
    assert(len(resp.keys()) == 4)
    # Each key should be a set with 2 items...
    assert(len(list(resp.keys())[0]) == 2)
    # Each item in the set should have 2 values (original key, value)
    assert(len(list(list(resp.keys())[0])[0]) == 2)


def test_get_resource_utilization_per_slave():
    tasks = [
        {
            'resources': {
                'cpus': 10,
                'mem': 10,
                'disk': 10,
            },
            'state': 'TASK_RUNNING',
        },
        {
            'resources': {
                'cpus': 10,
                'mem': 10,
                'disk': 10,
            },
            'state': 'TASK_RUNNING',
        },
    ]
    slaves = [
        {
            'id': 'somenametest-slave',
            'hostname': 'test.somewhere.www',
            'resources': {
                'cpus': 75,
                'disk': 250,
                'mem': 100,
            },
            'reserved_resources': {
            },
            'attributes': {
                'habitat': 'somenametest-habitat',
            },
        },
        {
            'id': 'somenametest-slave2',
            'hostname': 'test2.somewhere.www',
            'resources': {
                'cpus': 500,
                'disk': 200,
                'mem': 750,
            },
            'reserved_resources': {
                'maintenance': {
                    'cpus': 10,
                    'disk': 0,
                    'mem': 150,
                },
            },
            'attributes': {
                'habitat': 'somenametest-habitat-2',
            },
        },
    ]
    actual = metastatus_lib.calculate_resource_utilization_for_slaves(
        slaves=slaves,
        tasks=tasks,
    )
    assert sorted(actual.keys()) == sorted(['total', 'free', 'slave_count'])
    assert actual['total'] == metastatus_lib.ResourceInfo(
        cpus=575,
        disk=450,
        mem=850,
    )
    assert actual['free'] == metastatus_lib.ResourceInfo(
        cpus=545,
        disk=430,
        mem=680,
    )
    assert actual['slave_count'] == 2


def test_calculate_resource_utilization_for_slaves():
    fake_slaves = [
        {
            'id': 'somenametest-slave2',
            'hostname': 'test2.somewhere.www',
            'resources': {
                'cpus': 500,
                'disk': 200,
                'mem': 750,
                'gpus': 5,
            },
            'reserved_resources': {},
            'attributes': {
                'habitat': 'somenametest-habitat-2',
            },
        },
    ]
    tasks = [
        {
            'resources': {
                'cpus': 10,
                'mem': 10,
                'disk': 10,
                'gpus': 1,
            },
            'state': 'TASK_RUNNING',
        },
        {
            'resources': {
                'cpus': 10,
                'mem': 10,
                'disk': 10,
                'gpus': 2,
            },
            'state': 'TASK_RUNNING',
        },
    ]
    free = metastatus_lib.calculate_resource_utilization_for_slaves(
        slaves=fake_slaves,
        tasks=tasks,
    )['free']

    assert free.cpus == 480
    assert free.mem == 730
    assert free.disk == 180
    assert free.gpus == 2


def test_healthcheck_result_for_resource_utilization_ok():
    expected_message = 'cpus: 5.00/10.00(50.00%) used. Threshold (90.00%)'
    expected = metastatus_lib.HealthCheckResult(
        message=expected_message,
        healthy=True,
    )
    resource_utilization = metastatus_lib.ResourceUtilization(
        metric='cpus',
        total=10,
        free=5,
    )
    assert metastatus_lib.healthcheck_result_for_resource_utilization(
        resource_utilization=resource_utilization,
        threshold=90,
    ) == expected


def test_healthcheck_result_for_resource_utilization_unhealthy():
    expected_message = 'cpus: 5.00/10.00(50.00%) used. Threshold (10.00%)'
    expected = metastatus_lib.HealthCheckResult(
        message=expected_message,
        healthy=False,
    )
    resource_utilization = metastatus_lib.ResourceUtilization(
        metric='cpus',
        total=10,
        free=5,
    )
    assert metastatus_lib.healthcheck_result_for_resource_utilization(
        resource_utilization=resource_utilization,
        threshold=10,
    ) == expected


def test_healthcheck_result_for_resource_utilization_zero():
    expected_message = 'cpus: 0.00/0.00(0.00%) used. Threshold (10.00%)'
    expected = metastatus_lib.HealthCheckResult(
        message=expected_message,
        healthy=True,
    )
    resource_utilization = metastatus_lib.ResourceUtilization(
        metric='cpus',
        total=0,
        free=0,
    )
    assert metastatus_lib.healthcheck_result_for_resource_utilization(
        resource_utilization=resource_utilization,
        threshold=10,
    ) == expected


def test_format_table_column_for_healthcheck_resource_utilization_pair_healthy():
    fake_healthcheckresult = Mock()
    fake_healthcheckresult.healthy = True
    fake_resource_utilization = Mock()
    fake_resource_utilization.free = 10
    fake_resource_utilization.total = 20
    expected = PaastaColors.green("10/20 (50.00%)")
    assert metastatus_lib.format_table_column_for_healthcheck_resource_utilization_pair(
        (fake_healthcheckresult, fake_resource_utilization),
        False,
    ) == expected


def test_format_table_column_for_healthcheck_resource_utilization_pair_unhealthy():
    fake_healthcheckresult = Mock()
    fake_healthcheckresult.healthy = False
    fake_healthcheckresult.metric = 'mem'
    fake_resource_utilization = Mock()
    fake_resource_utilization.free = 10
    fake_resource_utilization.total = 20
    expected = PaastaColors.red("10/20 (50.00%)")
    assert metastatus_lib.format_table_column_for_healthcheck_resource_utilization_pair(
        (fake_healthcheckresult, fake_resource_utilization),
        False,
    ) == expected


def test_format_table_column_for_healthcheck_resource_utilization_pair_zero():
    fake_healthcheckresult = Mock()
    fake_healthcheckresult.healthy = False
    fake_healthcheckresult.metric = 'mem'
    fake_resource_utilization = Mock()
    fake_resource_utilization.free = 0
    fake_resource_utilization.total = 0
    expected = PaastaColors.red("0/0 (100.00%)")
    assert metastatus_lib.format_table_column_for_healthcheck_resource_utilization_pair(
        (fake_healthcheckresult, fake_resource_utilization),
        False,
    ) == expected


def test_format_table_column_for_healthcheck_resource_utilization_pair_healthy_human():
    fake_healthcheckresult = Mock()
    fake_healthcheckresult.healthy = True
    fake_healthcheckresult.metric = 'mem'
    fake_resource_utilization = Mock()
    fake_resource_utilization.free = 10
    fake_resource_utilization.total = 20
    expected = PaastaColors.green("10.0M/20.0M (50.00%)")
    assert metastatus_lib.format_table_column_for_healthcheck_resource_utilization_pair(
        (fake_healthcheckresult, fake_resource_utilization),
        True,
    ) == expected


def test_format_table_column_for_healthcheck_resource_utilization_pair_unhealthy_human():
    fake_healthcheckresult = Mock()
    fake_healthcheckresult.healthy = False
    fake_healthcheckresult.metric = 'mem'
    fake_resource_utilization = Mock()
    fake_resource_utilization.free = 10
    fake_resource_utilization.total = 20
    expected = PaastaColors.red("10.0M/20.0M (50.00%)")
    assert metastatus_lib.format_table_column_for_healthcheck_resource_utilization_pair(
        (fake_healthcheckresult, fake_resource_utilization),
        True,
    ) == expected


def test_format_table_column_for_healthcheck_resource_utilization_pair_zero_human():
    fake_healthcheckresult = Mock()
    fake_healthcheckresult.healthy = False
    fake_healthcheckresult.metric = 'mem'
    fake_resource_utilization = Mock()
    fake_resource_utilization.free = 0
    fake_resource_utilization.total = 0
    expected = PaastaColors.red("0B/0B (100.00%)")
    assert metastatus_lib.format_table_column_for_healthcheck_resource_utilization_pair(
        (fake_healthcheckresult, fake_resource_utilization),
        True,
    ) == expected


@patch(
    'paasta_tools.metrics.metastatus_lib.format_table_column_for_healthcheck_resource_utilization_pair',
    autospec=True,
)
def test_format_row_for_resource_utilization_checks(mock_format_row):
    fake_pairs = [
        (Mock(), Mock()),
        (Mock(), Mock()),
        (Mock(), Mock()),
    ]
    assert metastatus_lib.format_row_for_resource_utilization_healthchecks(fake_pairs, False)
    assert mock_format_row.call_count == len(fake_pairs)


@patch('paasta_tools.metrics.metastatus_lib.format_row_for_resource_utilization_healthchecks', autospec=True)
def test_get_table_rows_for_resource_usage_dict(mock_format_row):
    fake_pairs = [
        (Mock(), Mock()),
        (Mock(), Mock()),
        (Mock(), Mock()),
    ]
    mock_format_row.return_value = ['10/10', '10/10', '10/10']
    actual = metastatus_lib.get_table_rows_for_resource_info_dict('myhabitat', fake_pairs, False)
    assert actual == ['myhabitat', '10/10', '10/10', '10/10']


def test_key_func_for_attribute():
    assert inspect.isfunction(metastatus_lib.key_func_for_attribute('habitat'))


def test_get_mesos_disk_status():
    metrics = {
        'master/disk_total': 100,
        'master/disk_used': 50,
    }
    actual = metastatus_lib.get_mesos_disk_status(metrics)
    assert actual == (100, 50, 50)


def test_get_mesos_gpu_status():
    metrics = {
        'master/gpus_total': 3,
        'master/gpus_used': 1,
    }
    actual = metastatus_lib.get_mesos_gpu_status(metrics)
    assert actual == (3, 1, 2)


def test_reserved_maintenence_resources_no_maintenenance():
    actual = metastatus_lib.reserved_maintenence_resources({})
    assert all([actual[x] == 0 for x in ['cpus', 'mem', 'disk']])


def test_reserved_maintenence_resources():
    actual = metastatus_lib.reserved_maintenence_resources({
        'maintenance': {
            'cpus': 5,
            'mem': 5,
            'disk': 5,
        },
    })
    assert all([actual[x] == 5 for x in ['cpus', 'mem', 'disk']])


def test_reserved_maintenence_resources_ignores_non_maintenance():
    actual = metastatus_lib.reserved_maintenence_resources({
        'maintenance': {
            'cpus': 5,
            'mem': 5,
            'disk': 5,
        },
        'myotherole': {
            'cpus': 5,
            'mem': 5,
            'disk': 5,
        },
    })
    assert all([actual[x] == 5 for x in ['cpus', 'mem', 'disk']])
