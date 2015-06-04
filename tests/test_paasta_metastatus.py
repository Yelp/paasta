#!/usr/bin/env python

from mock import patch
from paasta_tools import paasta_metastatus
from paasta_tools.utils import PaastaColors


def test_get_mesos_cpu_status_good():
    fake_metrics = {
        'master/cpus_total': 3,
        'master/cpus_used': 1,
    }
    (output, is_ok) = paasta_metastatus.get_mesos_cpu_status(fake_metrics)
    assert is_ok is True
    assert "    cpus: 3 total => 1 used, 2 available" in output


def test_get_mesos_cpu_status_bad():
    fake_metrics = {
        'master/cpus_total': 100,
        'master/cpus_used': 99,
    }
    (output, is_ok) = paasta_metastatus.get_mesos_cpu_status(fake_metrics)
    assert is_ok is False
    assert '    cpus: 100 total => 99 used, 1 available' in output
    assert PaastaColors.red('    CRITICAL: Less than 10% CPUs available. (Currently at 1.00%)') in output

@patch('paasta_tools.paasta_metastatus.get_configured_quorum_size')
def test_has_quorum(mock_get_configured_quorum_size):
    mock_get_configured_quorum_size.return_value = 5
    assert paasta_metastatus.quorum_healthy(4, {}) is False
    assert paasta_metastatus.quorum_healthy(6, {}) is True

@patch('socket.getfqdn', autospec=True)
@patch('paasta_tools.paasta_metastatus.get_configured_quorum_size')
@patch('paasta_tools.paasta_metastatus.get_num_masters')
@patch('paasta_tools.paasta_metastatus.fetch_mesos_stats')
@patch('paasta_tools.paasta_metastatus.fetch_mesos_state_from_leader')
def test_get_mesos_status(
    mock_fetch_mesos_state_from_leader,
    mock_fetch_mesos_stats,
    mock_get_num_masters,
    mock_get_configured_quorum_size,
    mock_getfqdn,
):
    mock_getfqdn.return_value = 'fakename'
    mock_fetch_mesos_stats.return_value = {
        'master/cpus_total': 3,
        'master/cpus_used': 2,
        'master/mem_total': 10240,
        'master/mem_used': 2048,
        'master/tasks_running': 3,
        'master/tasks_staging': 4,
        'master/tasks_starting': 0,
        'master/slaves_active': 4,
        'master/slaves_inactive': 0,
    }
    mock_fetch_mesos_state_from_leader.return_value = {
        'flags': {
            'zk': 'zk://1.1.1.1:2222/fake_cluster',
            'quorum': 2,
        }
    }
    mock_get_num_masters.return_value = 2
    mock_get_configured_quorum_size = 1
    expected_cpus_output = "cpus: 3 total => 2 used, 1 available"
    expected_mem_output = \
        "memory: 10.00 GB total => 2.00 GB used, 8.00 GB available"
    expected_tasks_output = \
        "tasks: 3 running, 4 staging, 0 starting"
    expected_slaves_output = \
        "slaves: 4 active, 0 inactive"
    expected_masters_quorum_output = \
        "masters: 2 masters (1 configured quorum size.)"

    output = paasta_metastatus.get_mesos_status()

    assert mock_fetch_mesos_stats.called_once()
    assert mock_fetch_mesos_state_from_leader.called_once()
    assert expected_masters_quorum_output in output
    assert expected_cpus_output in output
    assert expected_mem_output in output
    assert expected_tasks_output in output
    assert expected_slaves_output in output


@patch('paasta_tools.paasta_metastatus.marathon_tools.get_marathon_client', autospec=True)
@patch('paasta_tools.paasta_metastatus.marathon_tools.load_marathon_config', autospec=True)
def test_get_marathon_status(
    mock_load_marathon_config,
    mock_get_marathon_client,
):
    mock_load_marathon_config.return_value = {
        'url': 'fakeurl',
        'user': 'fakeuser',
        'pass': 'fakepass',
    }
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
    expected_apps_output = "2 apps"
    expected_deployment_output = "1 deployments"
    expected_tasks_output = "3 tasks"

    output = paasta_metastatus.get_marathon_status()

    assert expected_apps_output in output
    assert expected_deployment_output in output
    assert expected_tasks_output in output
