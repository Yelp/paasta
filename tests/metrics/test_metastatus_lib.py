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
from __future__ import absolute_import
from __future__ import unicode_literals

import inspect

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
                    'some-role': {
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
                    'some-role': {
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
                    'some-role': {
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
                    'some-role': {
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
                    'some-role': {
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
                    'some-role': {
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
                    'some-role': {
                        'disk': 500,
                    },
                },
            },
        ],
    }
    failure_output, failure_health = metastatus_lib.assert_disk_health(failure_metrics, fake_mesos_state)
    assert not failure_health
    assert "CRITICAL: Less than 10% disk available. (Currently using 97.66%)" in failure_output


def assert_cpu_health_mesos_reports_zero():
    mesos_metrics = {
        'master/cpus_total': 0,
        'master/cpus_used': 1,
    }
    failure_output, failure_health = metastatus_lib.assert_cpu_health(mesos_metrics)
    assert failure_output == "Error reading total available cpu from mesos!"
    assert failure_health is False


def assert_memory_health_mesos_reports_zero():
    mesos_metrics = {
        'master/mem_total': 0,
        'master/mem_used': 1,
    }
    failure_output, failure_health = metastatus_lib.assert_memory_health(mesos_metrics)
    assert failure_output == "Error reading total available memory from mesos!"
    assert failure_health is False


def assert_disk_health_mesos_reports_zero():
    mesos_metrics = {
        'master/disk_total': 0,
        'master/disk_used': 1,
    }
    failure_output, failure_health = metastatus_lib.assert_disk_health(mesos_metrics)
    assert failure_output == "Error reading total available disk from mesos!"
    assert failure_health is False


def test_assert_no_duplicate_frameworks():
    state = {
        'frameworks': [
            {
                'name': 'test_framework1',
            },
            {
                'name': 'test_framework2',
            },
            {
                'name': 'test_framework3',
            },
            {
                'name': 'test_framework4',
            },
        ]
    }
    output, ok = metastatus_lib.assert_no_duplicate_frameworks(state)

    expected_output = "\n".join(["Frameworks:"] +
                                map(lambda x: '    Framework: %s count: 1' % x['name'], state['frameworks']))
    assert output == expected_output
    assert ok


def test_duplicate_frameworks():
    state = {
        'frameworks': [
            {
                'name': 'test_framework1',
            },
            {
                'name': 'test_framework1',
            },
            {
                'name': 'test_framework1',
            },
            {
                'name': 'test_framework2',
            },
        ]
    }
    output, ok = metastatus_lib.assert_no_duplicate_frameworks(state)
    assert "    CRITICAL: Framework test_framework1 has 3 instances running--expected no more than 1." in output
    assert not ok


def assert_connected_frameworks():
    metrics = {
        'master/frameworks_connected': 2,
    }
    hcr = metastatus_lib.assert_connected_frameworks(metrics)
    assert hcr.healthy
    assert "Connected Frameworks: expected: 2 actual: 2" in hcr.message


def assert_disconnected_frameworks():
    metrics = {
        'master/frameworks_disconnected': 1
    }
    hcr = metastatus_lib.assert_connected_frameworks(metrics)
    assert not hcr.healthy
    assert "Disconnected Frameworks: expected: 0 actual: 1" in hcr.message


def test_assert_active_frameworks():
    metrics = {
        'master/frameworks_active': 2
    }
    hcr = metastatus_lib.assert_active_frameworks(metrics)
    assert hcr.healthy
    assert "Active Frameworks: expected: 2 actual: 2" in hcr.message


def test_assert_inactive_frameworks():
    metrics = {
        'master/frameworks_inactive': 0,
    }
    hcr = metastatus_lib.assert_inactive_frameworks(metrics)
    assert hcr.healthy
    assert "Inactive Frameworks: expected: 0 actual: 0" in hcr.message


@patch('paasta_tools.marathon_tools.get_marathon_client', autospec=True)
def test_ok_marathon_apps(mock_get_marathon_client):
    client = mock_get_marathon_client.return_value
    client.list_apps.return_value = [
        "MarathonApp::1",
        "MarathonApp::2"
    ]
    output, ok = metastatus_lib.assert_marathon_apps(client)
    assert "marathon apps: 2" in output
    assert ok


@patch('paasta_tools.marathon_tools.get_marathon_client', autospec=True)
def test_no_marathon_apps(mock_get_marathon_client):
    client = mock_get_marathon_client.return_value
    client.list_apps.return_value = []
    output, ok = metastatus_lib.assert_marathon_apps(client)
    assert "CRITICAL: No marathon apps running" in output
    assert not ok


@patch('paasta_tools.marathon_tools.get_marathon_client', autospec=True)
def test_marathon_tasks(mock_get_marathon_client):
    client = mock_get_marathon_client.return_value
    client.list_tasks.return_value = ["MarathonTask:1"]
    output, ok = metastatus_lib.assert_marathon_tasks(client)
    assert "marathon tasks: 1" in output
    assert ok


@patch('paasta_tools.marathon_tools.get_marathon_client', autospec=True)
def test_assert_marathon_deployments(mock_get_marathon_client):
    client = mock_get_marathon_client.return_value
    client.list_deployments.return_value = ["MarathonDeployment:1"]
    output, ok = metastatus_lib.assert_marathon_deployments(client)
    assert "marathon deployments: 1" in output
    assert ok


def test_assert_slave_health():
    fake_slave_info = {
        'master/slaves_active': 10,
        'master/slaves_inactive': 10
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


@patch('paasta_tools.metrics.metastatus_lib.marathon_tools.get_marathon_client', autospec=True)
@patch('paasta_tools.metrics.metastatus_lib.marathon_tools.load_marathon_config', autospec=True)
def test_get_marathon_status(
    mock_load_marathon_config,
    mock_get_marathon_client,
):
    mock_load_marathon_config.return_value = MarathonConfig({
        'url': 'fakeurl',
        'user': 'fakeuser',
        'password': 'fakepass',
    })
    client = mock_get_marathon_client.return_value
    client.list_apps.return_value = [
        "MarathonApp::1",
        "MarathonApp::2"
    ]
    client.list_deployments.return_value = [
        "MarathonDeployment::1",
    ]
    client.list_tasks.return_value = [
        "MarathonTask::1",
        "MarathonTask::2",
        "MarathonTask::3"
    ]
    expected_apps_output = ("marathon apps: 2", True)
    expected_deployment_output = ("marathon deployments: 1", True)
    expected_tasks_output = ("marathon tasks: 3", True)

    results = metastatus_lib.get_marathon_status(client)

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


def test_assert_chronos_queued_jobs():
    mock_client = Mock()
    mock_client.metrics.return_value = {
        'gauges': {
            metastatus_lib.HIGH_QUEUE_GAUGE: {'value': 0},
            metastatus_lib.QUEUE_GAUGE: {'value': 0}
        }
    }
    mock_client.list.return_value = [
        {'name': 'myjob', 'disabled': False},
        {'name': 'myjob', 'disabled': True},
    ]
    assert metastatus_lib.assert_chronos_queued_jobs(mock_client) == metastatus_lib.HealthCheckResult(
        message="Jobs Queued: 0 (0%)",
        healthy=True
    )


@patch('paasta_tools.metrics.metastatus_lib.assert_chronos_queued_jobs', autospec=True)
@patch('paasta_tools.metrics.metastatus_lib.assert_chronos_scheduled_jobs', autospec=True)
def test_get_chronos_status(mock_queued_jobs, mock_scheduled_jobs):
    mock_scheduled_jobs_result = metastatus_lib.HealthCheckResult(
        message='Enabled chronos jobs: 1',
        healthy=True
    )
    mock_queued_jobs_result = metastatus_lib.HealthCheckResult(
        message="Jobs Queued: 0 (0%)",
        healthy=True
    )
    mock_queued_jobs.return_value = mock_queued_jobs_result
    mock_scheduled_jobs.return_value = mock_scheduled_jobs_result

    expected_results = [mock_queued_jobs_result, mock_scheduled_jobs_result]

    assert metastatus_lib.get_chronos_status(Mock()) == expected_results


def test_status_for_results():
    assert metastatus_lib.status_for_results([
        metastatus_lib.HealthCheckResult(
            message='message',
            healthy=True
        ),
        metastatus_lib.HealthCheckResult(
            message='message',
            healthy=False
        )
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
        metastatus_lib.HealthCheckResult('myservice_false', False)
    ]) == [('myservice_false', False)])


def test_filter_mesos_state_metrics():
    test_resource_dictionary = {
        'cpus': 0,
        'mem': 1,
        'MEM': 2,
        'garbage_data': 3,
    }
    expected = {
        'cpus': 0,
        'mem': 1,
    }
    assert metastatus_lib.filter_mesos_state_metrics(test_resource_dictionary) == expected


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
        slaves
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
            'hostname': 'test.somewhere.www'
        }],
        'somenametest-habitat-2': [{
            'id': 'abcd',
            'hostname': 'test2.somewhere.www'
        }]
    }
    mock_calculate_resource_utilization_for_slaves.return_value = {
        'free': metastatus_lib.ResourceInfo(cpus=10, mem=10, disk=10),
        'total': metastatus_lib.ResourceInfo(cpus=20, mem=20, disk=20)
    }
    mock_get_all_tasks_from_state([Mock(), Mock()])
    state = {
        'frameworks': Mock(),
        'slaves': [{}]
    }
    actual = metastatus_lib.get_resource_utilization_by_grouping(
        grouping_func=lambda slave: slave['attributes']['habitat'],
        mesos_state=state,
    )
    assert sorted(actual.keys()) == sorted(['somenametest-habitat', 'somenametest-habitat-2'])
    for k, v in actual.items():
        paasta_print(v)
        assert v['total'] == metastatus_lib.ResourceInfo(
            cpus=20,
            disk=20,
            mem=20
        )
        assert v['free'] == metastatus_lib.ResourceInfo(
            cpus=10,
            disk=10,
            mem=10
        )


def test_get_resource_utilization_per_slave():
    tasks = [
        {
            'resources': {
                'cpus': 10,
                'mem': 10,
                'disk': 10
            }
        },
        {
            'resources': {
                'cpus': 10,
                'mem': 10,
                'disk': 10
            }
        }
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
                'some-role': {
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
        tasks=tasks
    )
    assert sorted(actual.keys()) == sorted(['total', 'free'])
    assert actual['total'] == metastatus_lib.ResourceInfo(
        cpus=575,
        disk=450,
        mem=850
    )
    assert actual['free'] == metastatus_lib.ResourceInfo(
        cpus=545,
        disk=430,
        mem=680
    )


def test_healthcheck_result_for_resource_utilization_ok():
    expected_message = 'cpus: 5.00/10.00(50.00%) used. Threshold (90.00%)'
    expected = metastatus_lib.HealthCheckResult(
        message=expected_message,
        healthy=True
    )
    resource_utilization = metastatus_lib.ResourceUtilization(
        metric='cpus',
        total=10,
        free=5
    )
    assert metastatus_lib.healthcheck_result_for_resource_utilization(
        resource_utilization=resource_utilization,
        threshold=90
    ) == expected


def test_healthcheck_result_for_resource_utilization_unhealthy():
    expected_message = 'cpus: 5.00/10.00(50.00%) used. Threshold (10.00%)'
    expected = metastatus_lib.HealthCheckResult(
        message=expected_message,
        healthy=False
    )
    resource_utilization = metastatus_lib.ResourceUtilization(
        metric='cpus',
        total=10,
        free=5
    )
    assert metastatus_lib.healthcheck_result_for_resource_utilization(
        resource_utilization=resource_utilization,
        threshold=10
    ) == expected


def test_healthcheck_result_for_resource_utilization_zero():
    expected_message = 'cpus: 0.00/0.00(0.00%) used. Threshold (10.00%)'
    expected = metastatus_lib.HealthCheckResult(
        message=expected_message,
        healthy=True
    )
    resource_utilization = metastatus_lib.ResourceUtilization(
        metric='cpus',
        total=0,
        free=0,
    )
    assert metastatus_lib.healthcheck_result_for_resource_utilization(
        resource_utilization=resource_utilization,
        threshold=10
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
        False
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
        False
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
        False
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
        True
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
        True
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
        True
    ) == expected


@patch('paasta_tools.metrics.metastatus_lib.format_table_column_for_healthcheck_resource_utilization_pair',
       autospec=True)
def test_format_row_for_resource_utilization_checks(mock_format_row):
    fake_pairs = [
        (Mock(), Mock()),
        (Mock(), Mock()),
        (Mock(), Mock())
    ]
    assert metastatus_lib.format_row_for_resource_utilization_healthchecks(fake_pairs, False)
    assert mock_format_row.call_count == len(fake_pairs)


@patch('paasta_tools.metrics.metastatus_lib.format_row_for_resource_utilization_healthchecks', autospec=True)
def test_get_table_rows_for_resource_usage_dict(mock_format_row):
    fake_pairs = [
        (Mock(), Mock()),
        (Mock(), Mock()),
        (Mock(), Mock())
    ]
    mock_format_row.return_value = ['10/10', '10/10', '10/10']
    actual = metastatus_lib.get_table_rows_for_resource_info_dict('myhabitat', fake_pairs, False)
    assert actual == ['myhabitat', '10/10', '10/10', '10/10']


def test_key_func_for_attribute():
    assert inspect.isfunction(metastatus_lib.key_func_for_attribute('habitat'))


def test_get_mesos_disk_status():
    metrics = {
        'master/disk_total': 100,
        'master/disk_used': 50
    }
    actual = metastatus_lib.get_mesos_disk_status(metrics)
    assert actual == (100, 50, 50)
