#!/usr/bin/env python

from mock import patch
from paasta_tools import paasta_metastatus
from paasta_tools import mesos_tools
from paasta_tools.utils import PaastaColors


def test_get_mesos_cpu_status():
    fake_metrics = {
        'master/cpus_total': 3,
        'master/cpus_used': 1,
    }
    total, used, available = paasta_metastatus.get_mesos_cpu_status(fake_metrics)
    assert total == 3
    assert used == 1
    assert available == 2

@patch('paasta_tools.paasta_metastatus.fetch_mesos_state_from_leader')
def test_mesos_cli_exception(
        mock_fetch_from_leader
    ):
    mock_fetch_from_leader.side_effect = mesos_tools.MissingMasterException('Missing')
    try:
        paasta_metastatus.get_mesos_status()
    except paasta_metastatus.MesosCliException as e:
        assert 'Missing' in e.message
    else:
        assert False

@patch('paasta_tools.paasta_metastatus.fetch_mesos_stats')
@patch('paasta_tools.paasta_metastatus.get_configured_quorum_size')
@patch('paasta_tools.paasta_metastatus.get_num_masters')
@patch('paasta_tools.paasta_metastatus.fetch_mesos_state_from_leader')
def test_get_mesos_status_raises_quorum_exception(
        mock_fetch_mesos_state_from_leader,
        mock_num_masters,
        mock_quorum_size,
        mock_fetch_mesos_stats,
    ):
    mock_fetch_mesos_stats.return_value = {
        'master/cpus_total': 100,
        'master/cpus_used': 99,
    }
    mock_fetch_mesos_state_from_leader.return_value = {
        'flags': {
            'zk': 'zk://1.1.1.1:2222/fake_cluster',
            'quorum': 2,
        }
    }
    mock_num_masters.return_value = 2
    mock_quorum_size.return_value = 3
    try:
        paasta_metastatus.get_mesos_status()
    except paasta_metastatus.MesosQuorumException as e:
        assert '    Quorum: masters: 2 configured quorum: 3 ' in e.message
        assert PaastaColors.red("    CRITICAL: Number of masters (2) less than configured quorum(3).") in e.message
    else:
        assert False

@patch('paasta_tools.paasta_metastatus.fetch_mesos_stats')
@patch('paasta_tools.paasta_metastatus.get_configured_quorum_size')
@patch('paasta_tools.paasta_metastatus.get_num_masters')
@patch('paasta_tools.paasta_metastatus.fetch_mesos_state_from_leader')
def test_get_mesos_status_raises_cpu_exception(
        mock_fetch_mesos_state_from_leader,
        mock_num_masters,
        mock_quorum_size,
        mock_fetch_mesos_stats,
    ):
    mock_fetch_mesos_stats.return_value = {
        'master/cpus_total': 100,
        'master/cpus_used': 99,
    }
    mock_fetch_mesos_state_from_leader.return_value = {
        'flags': {
            'zk': 'zk://1.1.1.1:2222/fake_cluster',
            'quorum': 2,
        }
    }
    mock_num_masters.return_value = 5
    mock_quorum_size.return_value = 3
    try:
        paasta_metastatus.get_mesos_status()
    except paasta_metastatus.MesosCPUException as e:
        assert 'cpus: total: 100 used: 99 available: 1 percent_available: 1' in e.message
        assert PaastaColors.red('    CRITICAL: Less than 10% CPUs available. (Currently at 1.00%)') in e.message
    else:
        assert False


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
    mock_fetch_mesos_state_from_leader.return_value = {
        'flags': {
            'zk': 'zk://1.1.1.1:2222/fake_cluster',
            'quorum': 2,
        }
    }
    mock_get_num_masters.return_value = 5
    mock_get_configured_quorum_size.return_value = 3
    expected_cpus_output = "cpus: total: 10 used: 8 available: 2 percent_available: 20"
    expected_mem_output = \
        "memory: 10.00 GB total => 2.00 GB used, 8.00 GB available"
    expected_tasks_output = \
        "tasks: 3 running, 4 staging, 0 starting"
    expected_slaves_output = \
        "slaves: 4 active, 0 inactive"
    expected_masters_quorum_output = \
        "Quorum: masters: 5 configured quorum: 3 "

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
