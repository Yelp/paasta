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
import datetime

import mesos
import mock
import requests
from pytest import raises

import paasta_tools.mesos_tools as mesos_tools
from paasta_tools.marathon_tools import format_job_id
from paasta_tools.utils import PaastaColors


def test_filter_running_tasks():
    tasks = [
        {'id': 1, 'state': 'TASK_RUNNING', 'framework': {'active': True}},
        {'id': 2, 'state': 'TASK_FAILED', 'framework': {'active': True}},
    ]
    running = mesos_tools.filter_running_tasks(tasks)
    assert len(running) == 1
    assert running[0]['id'] == 1


def test_filter_not_running_tasks():
    tasks = [
        {'id': 1, 'state': 'TASK_RUNNING'},
        {'id': 2, 'state': 'TASK_FAILED'},
    ]
    not_running = mesos_tools.filter_not_running_tasks(tasks)
    assert len(not_running) == 1
    assert not_running[0]['id'] == 2


def test_status_mesos_tasks_verbose():
    with contextlib.nested(
        mock.patch('paasta_tools.mesos_tools.get_running_tasks_from_active_frameworks', autospec=True,),
        mock.patch('paasta_tools.mesos_tools.get_non_running_tasks_from_active_frameworks', autospec=True,),
        mock.patch('paasta_tools.mesos_tools.format_running_mesos_task_row', autospec=True,),
        mock.patch('paasta_tools.mesos_tools.format_non_running_mesos_task_row', autospec=True,),
    ) as (
        get_running_mesos_tasks_patch,
        get_non_running_mesos_tasks_patch,
        format_running_mesos_task_row_patch,
        format_non_running_mesos_task_row_patch,
    ):
        get_running_mesos_tasks_patch.return_value = ['doing a lap']
        get_non_running_mesos_tasks_patch.return_value = ['eating a burrito']
        format_running_mesos_task_row_patch.return_value = ['id', 'host', 'mem', 'cpu', 'disk', 'time']
        format_non_running_mesos_task_row_patch.return_value = ['id', 'host', 'time', 'state']
        job_id = format_job_id('fake_service', 'fake_instance'),

        def get_short_task_id(_):
            return 'short_task_id'

        actual = mesos_tools.status_mesos_tasks_verbose(job_id, get_short_task_id)
        assert 'Running Tasks' in actual
        assert 'Non-Running Tasks' in actual
        format_running_mesos_task_row_patch.assert_called_once_with('doing a lap', get_short_task_id)
        format_non_running_mesos_task_row_patch.assert_called_once_with('eating a burrito', get_short_task_id)


def test_get_cpu_usage_good():
    fake_task = mock.create_autospec(mesos.cli.task.Task)
    fake_task.cpu_limit = .35
    fake_duration = 100
    fake_task.stats = {
        'cpus_system_time_secs': 2.5,
        'cpus_user_time_secs': 0.0,
    }
    fake_task.__getitem__.return_value = [{
        'state': 'TASK_RUNNING',
        'timestamp': int(datetime.datetime.now().strftime('%s')) - fake_duration,
    }]
    actual = mesos_tools.get_cpu_usage(fake_task)
    assert '10.0%' == actual


def test_get_cpu_usage_bad():
    fake_task = mock.create_autospec(mesos.cli.task.Task)
    fake_task.cpu_limit = 1.1
    fake_duration = 100
    fake_task.stats = {
        'cpus_system_time_secs': 50.0,
        'cpus_user_time_secs': 50.0,
    }
    fake_task.__getitem__.return_value = [{
        'state': 'TASK_RUNNING',
        'timestamp': int(datetime.datetime.now().strftime('%s')) - fake_duration,
    }]
    actual = mesos_tools.get_cpu_usage(fake_task)
    assert PaastaColors.red('100.0%') in actual


def test_get_cpu_usage_handles_missing_stats():
    fake_task = mock.create_autospec(mesos.cli.task.Task)
    fake_task.cpu_limit = 1.1
    fake_duration = 100
    fake_task.stats = {}
    fake_task.__getitem__.return_value = [{
        'state': 'TASK_RUNNING',
        'timestamp': int(datetime.datetime.now().strftime('%s')) - fake_duration,
    }]
    actual = mesos_tools.get_cpu_usage(fake_task)
    assert "0.0%" in actual


def test_get_mem_usage_good():
    fake_task = mock.create_autospec(mesos.cli.task.Task)
    fake_task.rss = 1024 * 1024 * 10
    fake_task.mem_limit = fake_task.rss * 10
    actual = mesos_tools.get_mem_usage(fake_task)
    assert actual == '10/100MB'


def test_get_mem_usage_bad():
    fake_task = mock.create_autospec(mesos.cli.task.Task)
    fake_task.rss = 1024 * 1024 * 100
    fake_task.mem_limit = fake_task.rss
    actual = mesos_tools.get_mem_usage(fake_task)
    assert actual == PaastaColors.red('100/100MB')


def test_get_mem_usage_divide_by_zero():
    fake_task = mock.create_autospec(mesos.cli.task.Task)
    fake_task.rss = 1024 * 1024 * 10
    fake_task.mem_limit = 0
    actual = mesos_tools.get_mem_usage(fake_task)
    assert actual == "Undef"


def test_get_zookeeper_config():
    zk_hosts = '1.1.1.1:1111,2.2.2.2:2222,3.3.3.3:3333'
    zk_path = 'fake_path'
    fake_state = {'flags': {'zk': 'zk://%s/%s' % (zk_hosts, zk_path)}}
    expected = {'hosts': zk_hosts, 'path': zk_path}
    assert mesos_tools.get_zookeeper_config(fake_state) == expected


def test_get_mesos_leader():
    expected = 'mesos.master.yelpcorp.com'
    fake_master = 'false.authority.yelpcorp.com'
    with mock.patch('requests.get', autospec=True) as mock_requests_get:
        mock_requests_get.return_value = mock_response = mock.Mock()
        mock_response.return_code = 307
        mock_response.url = 'http://%s:999' % expected
        assert mesos_tools.get_mesos_leader(fake_master) == expected
        mock_requests_get.assert_called_once_with('http://%s:5050/redirect' % fake_master, timeout=10)


def test_get_mesos_leader_connection_error():
    fake_master = 'false.authority.yelpcorp.com'
    with mock.patch(
        'requests.get',
        autospec=True,
        side_effect=requests.exceptions.ConnectionError,
    ):
        with raises(mesos_tools.MesosMasterConnectionError):
            mesos_tools.get_mesos_leader(fake_master)


@mock.patch('paasta_tools.mesos_tools.get_mesos_leader')
def test_is_mesos_leader(mock_get_mesos_leader):
    fake_host = 'toast.host.roast'
    mock_get_mesos_leader.return_value = fake_host
    assert mesos_tools.is_mesos_leader(fake_host)
    mock_get_mesos_leader.assert_called_once_with(fake_host)


@mock.patch('paasta_tools.mesos_tools.KazooClient')
def test_get_number_of_mesos_masters(
    mock_kazoo,
):
    fake_zk_config = {'hosts': '1.1.1.1', 'path': 'fake_path'}

    zk = mock_kazoo.return_value
    zk.get_children.return_value = ['log_11', 'state', 'info_1', 'info_2']
    assert mesos_tools.get_number_of_mesos_masters(fake_zk_config) == 2


@mock.patch('requests.get')
@mock.patch('socket.getfqdn')
def test_get_local_slave_state_connection_error(
    mock_getfqdn,
    mock_requests_get,
):
    fake_request = requests.Request('GET', url='doesnt_matter')
    mock_getfqdn.return_value = 'fake_hostname'
    mock_requests_get.side_effect = requests.ConnectionError(
        'fake_message',
        request=fake_request,
    )

    with raises(mesos_tools.MesosSlaveConnectionError):
        mesos_tools.get_local_slave_state()


@mock.patch('paasta_tools.mesos_tools.get_mesos_state_from_leader', autospec=True)
def test_get_mesos_slaves_grouped_by_attribute(mock_fetch_state):
    fake_attribute = 'fake_attribute'
    fake_value_1 = 'fake_value_1'
    fake_value_2 = 'fake_value_2'
    mock_fetch_state.return_value = {
        'slaves': [
            {
                'hostname': 'fake_host_1',
                'attributes': {
                    'fake_attribute': fake_value_1,
                }
            },
            {
                'hostname': 'fake_host_2',
                'attributes': {
                    'fake_attribute': fake_value_2,
                }
            },
            {
                'hostname': 'fake_host_3',
                'attributes': {
                    'fake_attribute': fake_value_1,
                }
            },
            {
                'hostname': 'fake_host_4',
                'attributes': {
                    'fake_attribute': 'fake_other_value',
                }
            }
        ]
    }
    expected = {
        'fake_value_1': ['fake_host_1', 'fake_host_3'],
        'fake_value_2': ['fake_host_2'],
        'fake_other_value': ['fake_host_4'],
    }
    actual = mesos_tools.get_mesos_slaves_grouped_by_attribute(fake_attribute)
    assert actual == expected


@mock.patch('paasta_tools.mesos_tools.get_mesos_state_from_leader', autospec=True)
def test_get_mesos_slaves_grouped_by_attribute_bombs_out_with_no_slaves(mock_fetch_state):
    mock_fetch_state.return_value = {
        'slaves': []
    }
    with raises(mesos_tools.NoSlavesAvailable):
        mesos_tools.get_mesos_slaves_grouped_by_attribute('fake_attribute')


@mock.patch('paasta_tools.mesos_tools.get_mesos_state_from_leader', autospec=True)
@mock.patch('paasta_tools.mesos_tools.filter_mesos_slaves_by_blacklist', autospec=True)
def test_get_mesos_slaves_grouped_by_attribute_uses_blacklist(
    mock_filter_mesos_slaves_by_blacklist,
    mock_fetch_state
):
    fake_blacklist = ['fake_blacklist']
    fake_slaves = [
        {
            'hostname': 'fake_host_1',
            'attributes': {
                'fake_attribute': 'fake_value_1',
            }
        },
        {
            'hostname': 'fake_host_2',
            'attributes': {
                'fake_attribute': 'fake_value_1',
            }
        }
    ]
    mock_fetch_state.return_value = {'slaves': fake_slaves}
    mock_filter_mesos_slaves_by_blacklist.return_value = fake_slaves
    mesos_tools.get_mesos_slaves_grouped_by_attribute('fake_attribute', blacklist=fake_blacklist)
    mock_filter_mesos_slaves_by_blacklist.assert_called_once_with(slaves=fake_slaves, blacklist=fake_blacklist)


@mock.patch('paasta_tools.mesos_tools.slave_passes_blacklist', autospec=True)
def test_filter_mesos_slaves_by_blacklist_when_unfiltered(mock_slave_passes_blacklist):
    mock_slave_passes_blacklist.return_value = True
    slaves = [
        {
            'hostname': 'fake_host_1',
            'attributes': {
                'fake_attribute': 'fake_value_1',
            }
        },
        {
            'hostname': 'fake_host_2',
            'attributes': {
                'fake_attribute': 'fake_value_1',
            }
        }
    ]
    blacklist = []
    actual = mesos_tools.filter_mesos_slaves_by_blacklist(slaves=slaves, blacklist=blacklist)
    assert mock_slave_passes_blacklist.call_count == 2
    assert actual == slaves


@mock.patch('paasta_tools.mesos_tools.slave_passes_blacklist', autospec=True)
def test_filter_mesos_slaves_by_blacklist_when_filtered(mock_slave_passes_blacklist):
    mock_slave_passes_blacklist.return_value = False
    slaves = [
        {
            'hostname': 'fake_host_1',
            'attributes': {
                'fake_attribute': 'fake_value_1',
            }
        },
        {
            'hostname': 'fake_host_2',
            'attributes': {
                'fake_attribute': 'fake_value_1',
            }
        }
    ]
    blacklist = []
    actual = mesos_tools.filter_mesos_slaves_by_blacklist(slaves=slaves, blacklist=blacklist)
    assert mock_slave_passes_blacklist.call_count == 2
    assert actual == []


def test_slave_passes_blacklist_passes():
    slave = {
        'hostname': 'fake_host_3',
        'attributes': {
            'fake_attribute': 'fake_value_1',
        }
    }
    blacklist = [["fake_attribute", "No what we have here"], ['foo', 'bar']]
    actual = mesos_tools.slave_passes_blacklist(slave=slave, blacklist=blacklist)
    assert actual is True


def test_slave_passes_blacklist_blocks_blacklisted_locations():
    slave = {
        'hostname': 'fake_host_3',
        'attributes': {
            'fake_attribute': 'fake_value_1',
        }
    }
    blacklist = [["fake_attribute", "fake_value_1"]]
    actual = mesos_tools.slave_passes_blacklist(slave=slave, blacklist=blacklist)
    assert actual is False


def test_get_mesos_state_from_leader_works_on_elected_leader():
    # Elected leaders return 'elected_time' to indicate when
    # they were elected.
    good_fake_state = {
        "activated_slaves": 3,
        "cluster": "test",
        "completed_frameworks": [],
        "deactivated_slaves": 0,
        "elected_time": 1439503288.00787,
        "failed_tasks": 1,
    }
    mesos.cli.master.CURRENT.state = good_fake_state
    assert mesos_tools.get_mesos_state_from_leader() == good_fake_state


def test_get_mesos_state_from_leader_raises_on_non_elected_leader():
    # Non-elected leaders do not return 'elected_time' in their state
    # because they were not elected.
    un_elected_fake_state = {
        "activated_slaves": 3,
        "cluster": "test",
        "completed_frameworks": [],
        "deactivated_slaves": 0,
        "failed_tasks": 1,
    }
    mesos.cli.master.CURRENT.state = un_elected_fake_state
    with raises(mesos_tools.MasterNotAvailableException):
        assert mesos_tools.get_mesos_state_from_leader() == un_elected_fake_state
