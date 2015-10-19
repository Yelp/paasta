#!/usr/bin/env python
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

import contextlib

from mock import Mock
from mock import patch
from chronos import ChronosClient
from httplib2 import ServerNotFoundError
from pytest import raises

from chronos_tools import ChronosNotConfigured
from paasta_tools import paasta_metastatus
from paasta_tools import mesos_tools
from paasta_tools.utils import PaastaColors
from paasta_tools.marathon_tools import MarathonConfig
from paasta_tools.marathon_tools import MarathonNotConfigured


def test_ok_check_threshold():
    assert paasta_metastatus.check_threshold(10, 30)


def test_fail_check_threshold():
    assert not paasta_metastatus.check_threshold(80, 30)


def test_get_mesos_cpu_status():
    fake_metrics = {
        'master/cpus_total': 3,
        'master/cpus_used': 1,
    }
    total, used, available = paasta_metastatus.get_mesos_cpu_status(fake_metrics)
    assert total == 3
    assert used == 1
    assert available == 2


def test_ok_cpu_health():
    ok_metrics = {
        'master/cpus_total': 10,
        'master/cpus_used': 1,
    }
    ok_output, ok_health = paasta_metastatus.assert_cpu_health(ok_metrics)
    assert ok_health
    assert "CPUs: 1.00 / 10 in use (%s)" % PaastaColors.green("10.00%") in ok_output


def test_bad_cpu_health():
    failure_metrics = {
        'master/cpus_total': 10,
        'master/cpus_used': 9,
    }
    failure_output, failure_health = paasta_metastatus.assert_cpu_health(failure_metrics)
    assert not failure_health
    assert PaastaColors.red("CRITICAL: Less than 10% CPUs available. (Currently using 90.00%)") in failure_output


def test_assert_memory_health():
    ok_metrics = {
        'master/mem_total': 1024,
        'master/mem_used': 512,
    }
    ok_output, ok_health = paasta_metastatus.assert_memory_health(ok_metrics)
    assert ok_health
    assert "Memory: 0.50 / 1.00GB in use (%s)" % PaastaColors.green("50.00%") in ok_output


def test_failing_memory_health():
    failure_metrics = {
        'master/mem_total': 1024,
        'master/mem_used': 1000,
    }
    failure_output, failure_health = paasta_metastatus.assert_memory_health(failure_metrics)
    assert not failure_health
    assert PaastaColors.red("CRITICAL: Less than 10% memory available. (Currently using 97.66%)") in failure_output


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
    output, ok = paasta_metastatus.assert_no_duplicate_frameworks(state)

    expected_output = "\n".join(["frameworks:"] +
                                map(lambda x: '    framework: %s count: 1' % x['name'], state['frameworks']))
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
    output, ok = paasta_metastatus.assert_no_duplicate_frameworks(state)
    assert PaastaColors.red("    CRITICAL: Framework test_framework1 has 3 instances running--expected no more than 1.") \
        in output
    assert not ok


@patch('paasta_tools.paasta_metastatus.get_mesos_state_from_leader')
def test_missing_master_exception(mock_fetch_from_leader):
    mock_fetch_from_leader.side_effect = mesos_tools.MasterNotAvailableException('Missing')
    with raises(mesos_tools.MasterNotAvailableException) as exception_info:
        paasta_metastatus.get_mesos_status()
    assert 'Missing' in str(exception_info.value)


@patch('paasta_tools.marathon_tools.get_marathon_client')
def test_ok_marathon_apps(mock_get_marathon_client):
    client = mock_get_marathon_client.return_value
    client.list_apps.return_value = [
        "MarathonApp::1",
        "MarathonApp::2"
    ]
    output, ok = paasta_metastatus.assert_marathon_apps(client)
    assert "marathon apps: 2" in output
    assert ok


@patch('paasta_tools.marathon_tools.get_marathon_client')
def test_no_marathon_apps(mock_get_marathon_client):
    client = mock_get_marathon_client.return_value
    client.list_apps.return_value = []
    output, ok = paasta_metastatus.assert_marathon_apps(client)
    assert PaastaColors.red("CRITICAL: No marathon apps running") in output
    assert not ok


@patch('paasta_tools.marathon_tools.get_marathon_client')
def test_marathon_tasks(mock_get_marathon_client):
    client = mock_get_marathon_client.return_value
    client.list_tasks.return_value = ["MarathonTask:1"]
    output, ok = paasta_metastatus.assert_marathon_tasks(client)
    assert "marathon tasks: 1" in output
    assert ok


@patch('paasta_tools.marathon_tools.get_marathon_client')
def test_assert_marathon_deployments(mock_get_marathon_client):
    client = mock_get_marathon_client.return_value
    client.list_deployments.return_value = ["MarathonDeployment:1"]
    output, ok = paasta_metastatus.assert_marathon_deployments(client)
    assert "marathon deployments: 1" in output
    assert ok


def test_assert_slave_health():
    fake_slave_info = {
        'master/slaves_active': 10,
        'master/slaves_inactive': 10
    }
    output, ok = paasta_metastatus.assert_slave_health(fake_slave_info)
    assert "slaves: active: 10 inactive: 10" in output
    assert ok


def test_assert_tasks_running():
    fake_tasks_info = {
        'master/tasks_running': 20,
        'master/tasks_staging': 10,
        'master/tasks_starting': 10,
    }
    output, ok = paasta_metastatus.assert_tasks_running(fake_tasks_info)
    assert "tasks: running: 20 staging: 10 starting: 10" in output
    assert ok


@patch('paasta_tools.paasta_metastatus.get_mesos_quorum')
@patch('paasta_tools.paasta_metastatus.get_num_masters')
def test_healthy_asssert_quorum_size(mock_num_masters, mock_quorum_size):
    mock_num_masters.return_value = 5
    mock_quorum_size.return_value = 3
    output, health = paasta_metastatus.assert_quorum_size({})
    assert health
    assert 'quorum: masters: 5 configured quorum: 3 ' in output


@patch('paasta_tools.paasta_metastatus.get_mesos_quorum')
@patch('paasta_tools.paasta_metastatus.get_num_masters')
def test_unhealthy_asssert_quorum_size(mock_num_masters, mock_quorum_size):
    mock_num_masters.return_value = 1
    mock_quorum_size.return_value = 3
    output, health = paasta_metastatus.assert_quorum_size({})
    assert not health
    assert "CRITICAL: Number of masters (1) less than configured quorum(3)." in output


@patch('socket.getfqdn', autospec=True)
@patch('paasta_tools.paasta_metastatus.get_mesos_quorum')
@patch('paasta_tools.paasta_metastatus.get_num_masters')
@patch('paasta_tools.paasta_metastatus.get_mesos_stats')
@patch('paasta_tools.paasta_metastatus.get_mesos_state_from_leader')
def test_get_mesos_status(
    mock_get_mesos_state_from_leader,
    mock_get_mesos_stats,
    mock_get_num_masters,
    mock_get_configured_quorum_size,
    mock_getfqdn,
):
    mock_getfqdn.return_value = 'fakename'
    mock_get_mesos_stats.return_value = {
        'master/cpus_total': 10,
        'master/cpus_used': 8,
        'master/mem_total': 10240,
        'master/mem_used': 2048,
        'master/tasks_running': 3,
        'master/tasks_staging': 4,
        'master/tasks_starting': 0,
        'master/slaves_active': 4,
        'master/slaves_inactive': 0,
    }
    mock_get_mesos_state_from_leader.return_value = {
        'flags': {
            'zk': 'zk://1.1.1.1:2222/fake_cluster',
            'quorum': 2,
        },
        'frameworks': [
            {
                'name': 'test_framework1',
            },
            {
                'name': 'test_framework1',
            },
        ]
    }
    mock_get_num_masters.return_value = 5
    mock_get_configured_quorum_size.return_value = 3
    expected_cpus_output = "CPUs: 8.00 / 10 in use (%s)" % PaastaColors.green("80.00%")
    expected_mem_output = \
        "Memory: 2.00 / 10.00GB in use (%s)" % PaastaColors.green("20.00%")
    expected_tasks_output = \
        "tasks: running: 3 staging: 4 starting: 0"
    expected_duplicate_frameworks_output = \
        "frameworks:\n%s" % \
        PaastaColors.red("    CRITICAL: Framework test_framework1 has 2 instances running--expected no more than 1.")
    expected_slaves_output = \
        "slaves: active: 4 inactive: 0"
    expected_masters_quorum_output = \
        "quorum: masters: 5 configured quorum: 3 "

    outputs, oks = paasta_metastatus.get_mesos_status()

    assert mock_get_mesos_stats.called_once()
    assert mock_get_mesos_state_from_leader.called_once()
    assert expected_masters_quorum_output in outputs
    assert expected_cpus_output in outputs
    assert expected_mem_output in outputs
    assert expected_tasks_output in outputs
    assert expected_duplicate_frameworks_output in outputs
    assert expected_slaves_output in outputs


@patch('paasta_tools.paasta_metastatus.marathon_tools.get_marathon_client', autospec=True)
@patch('paasta_tools.paasta_metastatus.marathon_tools.load_marathon_config', autospec=True)
def test_get_marathon_status(
    mock_load_marathon_config,
    mock_get_marathon_client,
):
    mock_load_marathon_config.return_value = MarathonConfig({
        'url': 'fakeurl',
        'user': 'fakeuser',
        'password': 'fakepass',
    }, '/fake_config/fake_marathon.json')
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
    expected_apps_output = "marathon apps: 2"
    expected_deployment_output = "marathon deployments: 1"
    expected_tasks_output = "marathon tasks: 3"

    output, oks = paasta_metastatus.get_marathon_status(client)

    assert expected_apps_output in output
    assert expected_deployment_output in output
    assert expected_tasks_output in output


def test_get_marathon_client():
    fake_config = MarathonConfig({
        'url': 'fakeurl',
        'user': 'fakeuser',
        'password': 'fakepass',
    }, '/fake_config/fake_marathon.json')
    client = paasta_metastatus.get_marathon_client(fake_config)
    assert client.servers == ['fakeurl']
    assert client.auth == ('fakeuser', 'fakepass')


def test_assert_chronos_scheduled_jobs():
    mock_client = ChronosClient(servers="fake_hostname")
    mock_client.list = lambda: []
    output, ok = paasta_metastatus.assert_chronos_scheduled_jobs(mock_client)
    assert output == 'chronos jobs: 0'
    assert ok


def test_get_chronos_status_no_chronos():
    """ Asserts that chronos checks return ok, even when chronos
        is not available. This needs to be removed and fixed when
        we have chronos available everywhere, but worth verifying
        it works as expected for now """
    mock_client = ChronosClient(servers="fake_hostname")

    # force the raising of the error rather than
    # relying on the hostname of the config being
    # unavailable.
    mock_client.list = Mock(side_effect=ServerNotFoundError)

    outputs, oks = paasta_metastatus.get_chronos_status(mock_client)
    assert outputs == ['chronos jobs: 0']
    assert all(oks)


@patch('paasta_tools.chronos_tools.get_chronos_client', autospec=True)
def test_get_chronos_status(
    mock_get_chronos_client,
):
    client = mock_get_chronos_client.return_value
    client.list.return_value = [
        {'name': 'fake_job1'},
        {'name': 'fake_job1'},
    ]
    expected_jobs_output = "chronos jobs: 2"

    output, oks = paasta_metastatus.get_chronos_status(client)

    assert expected_jobs_output in output


def test_main_no_marathon_config():
    with contextlib.nested(
        patch('paasta_tools.marathon_tools.load_marathon_config', autospec=True),
        patch('paasta_tools.chronos_tools.load_chronos_config', autospec=True),
        patch('paasta_tools.paasta_metastatus.get_mesos_status', autospec=True,
              return_value=(['fake_output'], [True])),
        patch('paasta_tools.paasta_metastatus.get_marathon_status', autospec=True,
              return_value=(['fake_output'], [True])),
    ) as (
        load_marathon_config_patch,
        load_chronos_config_patch,
        load_get_mesos_status_patch,
        load_get_marathon_status_patch,
    ):
        load_marathon_config_patch.side_effect = MarathonNotConfigured
        with raises(SystemExit) as excinfo:
            paasta_metastatus.main()
        assert excinfo.value.code == 0


def test_main_no_chronos_config():
    with contextlib.nested(
        patch('paasta_tools.marathon_tools.load_marathon_config', autospec=True),
        patch('paasta_tools.chronos_tools.load_chronos_config', autospec=True),
        patch('paasta_tools.paasta_metastatus.get_mesos_status', autospec=True,
              return_value=(['fake_output'], [True])),
        patch('paasta_tools.paasta_metastatus.get_marathon_status', autospec=True,
              return_value=(['fake_output'], [True])),
    ) as (
        load_marathon_config_patch,
        load_chronos_config_patch,
        load_get_mesos_status_patch,
        load_get_marathon_status_patch,
    ):
        load_chronos_config_patch.side_effect = ChronosNotConfigured
        with raises(SystemExit) as excinfo:
            paasta_metastatus.main()
        assert excinfo.value.code == 0
