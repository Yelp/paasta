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
import contextlib
import datetime
import random
import socket

import docker
import mock
import requests
from pytest import mark
from pytest import raises

import paasta_tools.mesos as mesos
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


@mark.parametrize('test_case', [
    [0, 0],
    [10, 1 + 10]  # 1 running task, 10 non-running taks (truncated)
])
def test_status_mesos_tasks_verbose(test_case):
    tail_lines, expected_format_tail_call_count = test_case
    with contextlib.nested(
        mock.patch('paasta_tools.mesos_tools.get_running_tasks_from_active_frameworks', autospec=True,),
        mock.patch('paasta_tools.mesos_tools.get_non_running_tasks_from_active_frameworks', autospec=True,),
        mock.patch('paasta_tools.mesos_tools.format_running_mesos_task_row', autospec=True,),
        mock.patch('paasta_tools.mesos_tools.format_non_running_mesos_task_row', autospec=True,),
        mock.patch('paasta_tools.mesos_tools.format_stdstreams_tail_for_task', autospec=True,),
    ) as (
        get_running_mesos_tasks_patch,
        get_non_running_mesos_tasks_patch,
        format_running_mesos_task_row_patch,
        format_non_running_mesos_task_row_patch,
        format_stdstreams_tail_for_task_patch,
    ):
        get_running_mesos_tasks_patch.return_value = ['doing a lap']

        template_task_return = {
            'statuses': [{'timestamp': '##########'}],
            'state': 'NOT_RUNNING',
        }
        non_running_mesos_tasks = []
        for _ in xrange(15):  # excercise the code that sorts/truncates the list of non running tasks
            task_return = template_task_return.copy()
            task_return['statuses'][0]['timestamp'] = str(1457109986 + random.randrange(-60 * 60 * 24, 60 * 60 * 24))
            non_running_mesos_tasks.append(task_return)
        get_non_running_mesos_tasks_patch.return_value = non_running_mesos_tasks

        format_running_mesos_task_row_patch.return_value = ['id', 'host', 'mem', 'cpu', 'time']
        format_non_running_mesos_task_row_patch.return_value = ['id', 'host', 'time', 'state']
        format_stdstreams_tail_for_task_patch.return_value = ['tail']
        job_id = format_job_id('fake_service', 'fake_instance'),

        def get_short_task_id(_):
            return 'short_task_id'

        actual = mesos_tools.status_mesos_tasks_verbose(
            job_id=job_id,
            get_short_task_id=get_short_task_id,
            tail_lines=tail_lines,
        )
        assert 'Running Tasks' in actual
        assert 'Non-Running Tasks' in actual
        format_running_mesos_task_row_patch.assert_called_once_with('doing a lap', get_short_task_id)
        assert format_non_running_mesos_task_row_patch.call_count == 10  # maximum n of tasks we display
        assert format_stdstreams_tail_for_task_patch.call_count == expected_format_tail_call_count


def test_get_cpu_usage_good():
    fake_task = mock.create_autospec(mesos.task.Task)
    fake_task.cpu_limit = .35
    fake_duration = 100
    fake_task.stats = {
        'cpus_system_time_secs': 2.5,
        'cpus_user_time_secs': 0.0,
    }
    current_time = datetime.datetime.now()
    fake_task.__getitem__.return_value = [{
        'state': 'TASK_RUNNING',
        'timestamp': int(current_time.strftime('%s')) - fake_duration,
    }]
    with mock.patch('paasta_tools.mesos_tools.datetime.datetime', autospec=True) as mock_datetime:
        mock_datetime.now.return_value = current_time
        actual = mesos_tools.get_cpu_usage(fake_task)
    assert '10.0%' == actual


def test_get_cpu_usage_bad():
    fake_task = mock.create_autospec(mesos.task.Task)
    fake_task.cpu_limit = 1.1
    fake_duration = 100
    fake_task.stats = {
        'cpus_system_time_secs': 50.0,
        'cpus_user_time_secs': 50.0,
    }
    current_time = datetime.datetime.now()
    fake_task.__getitem__.return_value = [{
        'state': 'TASK_RUNNING',
        'timestamp': int(current_time.strftime('%s')) - fake_duration,
    }]
    with mock.patch('paasta_tools.mesos_tools.datetime.datetime', autospec=True) as mock_datetime:
        mock_datetime.now.return_value = current_time
        actual = mesos_tools.get_cpu_usage(fake_task)
    assert PaastaColors.red('100.0%') in actual


def test_get_cpu_usage_handles_missing_stats():
    fake_task = mock.create_autospec(mesos.task.Task)
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
    fake_task = mock.create_autospec(mesos.task.Task)
    fake_task.rss = 1024 * 1024 * 10
    fake_task.mem_limit = fake_task.rss * 10
    actual = mesos_tools.get_mem_usage(fake_task)
    assert actual == '10/100MB'


def test_get_mem_usage_bad():
    fake_task = mock.create_autospec(mesos.task.Task)
    fake_task.rss = 1024 * 1024 * 100
    fake_task.mem_limit = fake_task.rss
    actual = mesos_tools.get_mem_usage(fake_task)
    assert actual == PaastaColors.red('100/100MB')


def test_get_mem_usage_divide_by_zero():
    fake_task = mock.create_autospec(mesos.task.Task)
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
    fake_url = 'http://93.184.216.34:5050'
    with contextlib.nested(
        mock.patch('paasta_tools.mesos_tools.master.CURRENT'),
        mock.patch('paasta_tools.mesos_tools.socket.gethostbyaddr'),
        mock.patch('paasta_tools.mesos_tools.socket.getfqdn'),
    ) as (
        mock_CURRENT,
        mock_gethostbyaddr,
        mock_getfqdn,
    ):
        mock_CURRENT.host = fake_url
        mock_gethostbyaddr.return_value = 'example.org'
        mock_getfqdn.return_value = 'example.org'
        assert mesos_tools.get_mesos_leader() == 'example.org'


def test_get_mesos_leader_socket_eroror():
    fake_url = 'http://93.184.216.34:5050'
    with contextlib.nested(
        mock.patch('paasta_tools.mesos_tools.master.CURRENT'),
        mock.patch('paasta_tools.mesos_tools.socket.gethostbyaddr', side_effect=socket.error),
    ) as (
        mock_CURRENT,
        mock_gethostbyaddr,
    ):
        mock_CURRENT.host = fake_url
        with raises(socket.error):
            mesos_tools.get_mesos_leader()


def test_get_mesos_leader_no_hostname():
    fake_url = 'localhost:5050'
    with contextlib.nested(
        mock.patch('paasta_tools.mesos_tools.master.CURRENT'),
    ) as (
        mock_CURRENT,
    ):
        mock_CURRENT.host = fake_url
        with raises(ValueError):
            mesos_tools.get_mesos_leader()


def test_get_mesos_leader_cli_mesosmasterconnectionerror():
    with contextlib.nested(
        mock.patch('paasta_tools.mesos.master.MesosMaster.resolve', side_effect=mesos_tools.MesosMasterConnectionError),
    ) as (
        mock_resolve,
    ):
        with raises(mesos_tools.MesosMasterConnectionError):
            mesos_tools.get_mesos_leader()


@mock.patch('paasta_tools.mesos_tools.get_mesos_leader')
def test_is_mesos_leader(mock_get_mesos_leader):
    fake_host = 'toast.host.roast'
    mock_get_mesos_leader.return_value = fake_host
    assert mesos_tools.is_mesos_leader(fake_host)
    mock_get_mesos_leader.assert_called_once_with()


@mock.patch('paasta_tools.mesos_tools.get_mesos_leader')
def test_is_mesos_leader_substring(mock_get_mesos_leader):
    fake_host = 'toast.host.roast'
    mock_get_mesos_leader.return_value = "fake_prefix." + fake_host + ".fake_suffix"
    assert not mesos_tools.is_mesos_leader(fake_host)
    mock_get_mesos_leader.assert_called_once_with()


@mock.patch('paasta_tools.mesos_tools.KazooClient')
def test_get_number_of_mesos_masters(
    mock_kazoo,
):
    host = '1.1.1.1'
    path = 'fake_path'

    zk = mock_kazoo.return_value
    zk.get_children.return_value = ['log_11', 'state', 'json.info_1', 'info_2']
    assert mesos_tools.get_number_of_mesos_masters(host, path) == 2


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


def test_get_mesos_slaves_grouped_by_attribute():
    fake_value_1 = 'fake_value_1'
    fake_value_2 = 'fake_value_2'
    fake_slaves = [
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
    expected = {
        'fake_value_1': [
            {
                'hostname': 'fake_host_1',
                'attributes': {
                    'fake_attribute': fake_value_1,
                }
            },
            {
                'hostname': 'fake_host_3',
                'attributes': {
                    'fake_attribute': fake_value_1,
                }
            },
        ],
        'fake_value_2': [
            {
                'hostname': 'fake_host_2',
                'attributes': {
                    'fake_attribute': fake_value_2,
                }
            },

        ],
        'fake_other_value': [
            {
                'hostname': 'fake_host_4',
                'attributes': {
                    'fake_attribute': 'fake_other_value',
                }
            }
        ]
    }
    actual = mesos_tools.get_mesos_slaves_grouped_by_attribute(fake_slaves, 'fake_attribute')
    assert actual == expected


def test_slave_passes_whitelist():
    fake_slave = {
        'attributes': {
            'location_type': 'fake_location',
            'fake_location_type': 'fake_location'
        }
    }
    fake_whitelist_allow = ['fake_location_type', ['fake_location']]
    fake_whitelist_deny = ['anoterfake_location_type', ['anotherfake_location']]

    slave_passes = mesos_tools.slave_passes_whitelist(fake_slave, fake_whitelist_deny)
    assert not slave_passes
    slave_passes = mesos_tools.slave_passes_whitelist(fake_slave, fake_whitelist_allow)
    assert slave_passes
    slave_passes = mesos_tools.slave_passes_whitelist(fake_slave, [])
    assert slave_passes


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
    whitelist = []
    actual = mesos_tools.filter_mesos_slaves_by_blacklist(slaves=slaves, blacklist=blacklist, whitelist=whitelist)
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
    whitelist = []
    actual = mesos_tools.filter_mesos_slaves_by_blacklist(slaves=slaves, blacklist=blacklist, whitelist=whitelist)
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
    mesos.master.CURRENT.state = good_fake_state
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
    mesos.master.CURRENT.state = un_elected_fake_state
    with raises(mesos_tools.MasterNotAvailableException):
        assert mesos_tools.get_mesos_state_from_leader() == un_elected_fake_state


def test_get_paasta_execute_docker_healthcheck():
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_container_id = 'fake_container_id'
    fake_mesos_id = 'fake_mesos_id'
    fake_container_info = [
        {'Config': {'Env': None}},
        {'Config': {'Env': ['fake_key1=fake_value1', 'MESOS_TASK_ID=fake_other_mesos_id']}, 'Id': '11111'},
        {'Config': {'Env': ['fake_key2=fake_value2', 'MESOS_TASK_ID=%s' % fake_mesos_id]}, 'Id': fake_container_id},
    ]
    mock_docker_client.containers = mock.MagicMock(
        spec_set=docker.Client,
        return_value=['fake_container_1', 'fake_container_2', 'fake_container_3'],
    )
    mock_docker_client.inspect_container = mock.MagicMock(
        spec_set=docker.Client,
        side_effect=fake_container_info,
    )
    assert mesos_tools.get_container_id_for_mesos_id(mock_docker_client, fake_mesos_id) == fake_container_id


def test_get_paasta_execute_docker_healthcheck_when_not_found():
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_mesos_id = 'fake_mesos_id'
    fake_container_info = [
        {'Config': {'Env': ['fake_key1=fake_value1', 'MESOS_TASK_ID=fake_other_mesos_id']}, 'Id': '11111'},
        {'Config': {'Env': ['fake_key2=fake_value2', 'MESOS_TASK_ID=fake_other_mesos_id2']}, 'Id': '2222'},
    ]
    mock_docker_client.containers = mock.MagicMock(
        spec_set=docker.Client,
        return_value=['fake_container_1', 'fake_container_2'],
    )
    mock_docker_client.inspect_container = mock.MagicMock(
        spec_set=docker.Client,
        side_effect=fake_container_info,
    )
    assert mesos_tools.get_container_id_for_mesos_id(mock_docker_client, fake_mesos_id) is None


@mark.parametrize('test_case', [
    [['taska', 'taskb'],  # test_case0 - OK
     [['outlna1', 'outlna2', 'errlna1'],
      ['outlnb1', 'errlnb1', 'errlnb2']],
     ['taska', 'outlna1', 'outlna2', 'errlna1', 'taskb', 'outlnb1', 'errlnb1', 'errlnb2'],
     False],
    [['a'],  # test_case1 - can't zip different length lists
     [1, 2],
     None,
     True]])
def test_zip_tasks_verbose_output(test_case):
    table, stdstreams, expected, should_raise = test_case
    result = None
    raised = False
    try:
        result = mesos_tools.zip_tasks_verbose_output(table, stdstreams)
    except ValueError:
        raised = True

    assert raised == should_raise
    assert result == expected


@mark.parametrize('test_case', [
    # task_id, file1, file2, nlines, raise_what
    ['a_task',  # test_case0 - OK
     ['stdout', [str(x) for x in range(20)]],
     ['stderr', [str(x) for x in range(30)]],
     10,
     None],
    ['a_task',  # test_case1 - OK, short stdout, swapped stdout/stderr
     ['stderr', [str(x) for x in range(30)]],
     ['stdout', ['1', '2']],
     10,
     None],
    ['a_task', None, None, 10, 'MasterNotAvailableException'],
    ['a_task', None, None, 10, 'SlaveNotAvailableException'],
    ['a_task', None, None, 10, 'TaskNotFoundException'],
    ['a_task', None, None, 10, 'FileNotFoundForTaskException'],
    ['a_task', None, None, 10, 'TimeoutError']
])
def test_format_stdstreams_tail_for_task(test_case):
    def gen_mesos_cli_fobj(file_path, file_lines):
        """mesos.cli.cluster.files (0.1.5),
        returns a list of mesos.cli.mesos_file.File
        `File` is an iterator-like object.
        """
        fake_iter = mock.MagicMock()
        fake_iter.return_value = reversed(file_lines)
        fobj = mock.create_autospec(mesos.mesos_file.File)
        fobj.path = file_path
        fobj.__reversed__ = fake_iter
        return fobj

    def get_short_task_id(task_id):
        return task_id

    def gen_mock_cluster_files(file1, file2, raise_what):
        def retfunc(*args, **kwargs):
            # If we're asked to raise a particular exception we do so.
            # .message is set to the exception class name.
            if raise_what:
                exception_class = getattr(mesos_tools, raise_what)
                raise exception_class(exception_class.__name__)
            return [
                gen_mesos_cli_fobj(file1[0], file1[1]),
                gen_mesos_cli_fobj(file2[0], file2[1])
            ]
        mock_cluster_files = mock.MagicMock()
        mock_cluster_files.side_effect = retfunc
        return mock_cluster_files

    def gen_output(task_id, file1, file2, nlines, raise_what):
        error_message = PaastaColors.red("      couldn't read stdout/stderr for %s (%s)")
        output = []
        if not raise_what:
            files = [file1, file2]
            # reverse sort because stdout is supposed to always come before stderr in the output
            files.sort(key=lambda f: f[0], reverse=True)
            for f in files:
                output.append(PaastaColors.blue("      %s tail for %s" % (f[0], task_id)))
                output.extend(f[1][-nlines:])
                output.append(PaastaColors.blue("      %s EOF" % f[0]))
        else:
            if raise_what == 'TimeoutError':
                raise_what = 'timeout'
            output.append(error_message % (task_id, raise_what))
        return output

    task_id, file1, file2, nlines, raise_what = test_case

    mock_cluster_files = gen_mock_cluster_files(file1, file2, raise_what)
    fake_task = {'id': task_id}
    expected = gen_output(task_id, file1, file2, nlines, raise_what)
    with mock.patch('paasta_tools.mesos_tools.cluster.files', mock_cluster_files):
        result = mesos_tools.format_stdstreams_tail_for_task(fake_task, get_short_task_id)
        assert result == expected


def test_slave_pid_to_ip():
    ret = mesos_tools.slave_pid_to_ip('slave(1)@10.40.31.172:5051')
    assert ret == '10.40.31.172'


def test_get_mesos_task_count_by_slave():
    with contextlib.nested(
        mock.patch('paasta_tools.mesos_tools.get_running_tasks_from_active_frameworks'),
    ) as (
        mock_get_running_tasks_from_active_frameworks,
    ):
        mock_chronos = mock.Mock()
        mock_chronos.name = 'chronos'
        mock_marathon = mock.Mock()
        mock_marathon.name = 'marathon'
        mock_task1 = mock.Mock()
        mock_task1.slave = {'id': 'slave1'}
        mock_task1.framework = mock_chronos
        mock_task2 = mock.Mock()
        mock_task2.slave = {'id': 'slave1'}
        mock_task2.framework = mock_marathon
        mock_task3 = mock.Mock()
        mock_task3.slave = {'id': 'slave2'}
        mock_task3.framework = mock_marathon
        mock_task4 = mock.Mock()
        mock_task4.slave = {'id': 'slave2'}
        mock_task4.framework = mock_marathon
        mock_tasks = [mock_task1, mock_task2, mock_task3, mock_task4]
        mock_get_running_tasks_from_active_frameworks.return_value = mock_tasks
        mock_slave_1 = {'id': 'slave1', 'attributes': {'pool': 'default'}, 'hostname': 'host1'}
        mock_slave_2 = {'id': 'slave2', 'attributes': {'pool': 'default'}, 'hostname': 'host2'}
        mock_slave_3 = {'id': 'slave3', 'attributes': {'pool': 'another'}, 'hostname': 'host3'}
        mock_mesos_state = {'slaves': [mock_slave_1, mock_slave_2, mock_slave_3]}
        ret = mesos_tools.get_mesos_task_count_by_slave(mock_mesos_state, pool='default')
        mock_get_running_tasks_from_active_frameworks.assert_called_with('')
        expected = [{'task_counts': mesos_tools.SlaveTaskCount(count=2, chronos_count=1, slave=mock_slave_1)},
                    {'task_counts': mesos_tools.SlaveTaskCount(count=2, chronos_count=0, slave=mock_slave_2)}]
        assert len(ret) == len(expected) and sorted(ret) == sorted(expected)
        ret = mesos_tools.get_mesos_task_count_by_slave(mock_mesos_state, pool=None)
        mock_get_running_tasks_from_active_frameworks.assert_called_with('')
        expected = [{'task_counts': mesos_tools.SlaveTaskCount(count=2, chronos_count=1, slave=mock_slave_1)},
                    {'task_counts': mesos_tools.SlaveTaskCount(count=2, chronos_count=0, slave=mock_slave_2)},
                    {'task_counts': mesos_tools.SlaveTaskCount(count=0, chronos_count=0, slave=mock_slave_3)}]
        assert len(ret) == len(expected) and sorted(ret) == sorted(expected)

        # test slaves_list override
        mock_task2 = mock.Mock()
        mock_task2.slave = {'id': 'slave2'}
        mock_task2.framework = mock_marathon
        mock_tasks = [mock_task1, mock_task2, mock_task3, mock_task4]
        mock_get_running_tasks_from_active_frameworks.return_value = mock_tasks
        ret = mesos_tools.get_mesos_task_count_by_slave(mock_mesos_state,
                                                        slaves_list=[mock_slave_1, mock_slave_2, mock_slave_3])
        expected = [{'id': 'slave1', 'attributes': {'pool': 'default'}, 'hostname': 'host1',
                     'task_counts': mesos_tools.SlaveTaskCount(count=1, chronos_count=1, slave=mock_slave_1)},
                    {'id': 'slave2', 'attributes': {'pool': 'default'}, 'hostname': 'host2',
                     'task_counts': mesos_tools.SlaveTaskCount(count=3, chronos_count=0, slave=mock_slave_2)},
                    {'id': 'slave3', 'attributes': {'pool': 'another'}, 'hostname': 'host3',
                     'task_counts': mesos_tools.SlaveTaskCount(count=0, chronos_count=0, slave=mock_slave_3)}]
        assert len(ret) == len(expected) and sorted(ret) == sorted(expected)


def test_get_count_running_tasks_on_slave():
    with contextlib.nested(
        mock.patch('paasta_tools.mesos_tools.get_mesos_state_summary_from_leader'),
        mock.patch('paasta_tools.mesos_tools.get_mesos_task_count_by_slave'),
    ) as (
        mock_get_mesos_state_summary_from_leader,
        mock_get_mesos_task_count_by_slave
    ):
        mock_mesos_state = mock.Mock()
        mock_slave_counts = [{'task_counts': mock.Mock(count=3, slave={'hostname': 'host1'})},
                             {'task_counts': mock.Mock(count=0, slave={'hostname': 'host2'})}]
        mock_get_mesos_state_summary_from_leader.return_value = mock_mesos_state
        mock_get_mesos_task_count_by_slave.return_value = mock_slave_counts

        assert mesos_tools.get_count_running_tasks_on_slave('host1') == 3
        assert mesos_tools.get_count_running_tasks_on_slave('host2') == 0
        assert mesos_tools.get_count_running_tasks_on_slave('host3') == 0
        assert mock_get_mesos_state_summary_from_leader.called
        mock_get_mesos_task_count_by_slave.assert_called_with(mock_mesos_state)
