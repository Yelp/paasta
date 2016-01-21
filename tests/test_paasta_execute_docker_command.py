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

import docker
import mock
import pytest

from paasta_tools.paasta_execute_docker_command import execute_in_container
from paasta_tools.paasta_execute_docker_command import get_container_id_for_mesos_id
from paasta_tools.paasta_execute_docker_command import main
from paasta_tools.paasta_execute_docker_command import TimeoutException


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
    assert get_container_id_for_mesos_id(mock_docker_client, fake_mesos_id) == fake_container_id


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
    assert get_container_id_for_mesos_id(mock_docker_client, fake_mesos_id) is None


def test_execute_in_container():
    fake_container_id = 'fake_container_id'
    fake_return_code = 0
    fake_output = 'fake_output'
    fake_command = 'fake_cmd'
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.exec_start.return_value = fake_output
    mock_docker_client.exec_inspect.return_value = {'ExitCode': fake_return_code}

    assert execute_in_container(mock_docker_client, fake_container_id, fake_command, 1) == (
        fake_output, fake_return_code)
    expected_cmd = ['/bin/sh', '-c', fake_command]
    mock_docker_client.exec_create.assert_called_once_with(fake_container_id, expected_cmd)


def test_execute_in_container_reuses_exec():
    fake_container_id = 'fake_container_id'
    fake_execid = 'fake_execid'
    fake_return_code = 0
    fake_output = 'fake_output'
    fake_command = 'fake_cmd'
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.inspect_container.return_value = {'ExecIDs': [fake_execid]}
    mock_docker_client.exec_start.return_value = fake_output
    mock_docker_client.exec_inspect.return_value = {
        'ExitCode': fake_return_code,
        'ProcessConfig': {
            'entrypoint': '/bin/sh',
            'arguments': ['-c', fake_command],
        }
    }

    assert execute_in_container(mock_docker_client, fake_container_id, fake_command, 1) == (
        fake_output, fake_return_code)
    assert mock_docker_client.exec_create.call_count == 0
    mock_docker_client.exec_start.assert_called_once_with(fake_execid, stream=False)


def test_execute_in_container_reuses_only_valid_exec():
    fake_container_id = 'fake_container_id'
    fake_execid = 'fake_execid'
    fake_return_code = 0
    fake_output = 'fake_output'
    fake_command = 'fake_cmd'
    bad_exec = {
        'ExitCode': fake_return_code,
        'ProcessConfig': {
            'entrypoint': '/bin/sh',
            'arguments': ['-c', 'some_other_command'],
        }
    }
    good_exec = {
        'ExitCode': fake_return_code,
        'ProcessConfig': {
            'entrypoint': '/bin/sh',
            'arguments': ['-c', fake_command],
        }
    }
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.inspect_container.return_value = {'ExecIDs': ['fake_other_exec', fake_execid, 'fake_other_exec']}
    mock_docker_client.exec_start.return_value = fake_output
    # the last side effect is used to check the exit code of the command
    mock_docker_client.exec_inspect.side_effect = [bad_exec, good_exec, bad_exec, good_exec]

    assert execute_in_container(mock_docker_client, fake_container_id, fake_command, 1) == (
        fake_output, fake_return_code)
    assert mock_docker_client.exec_create.call_count == 0
    mock_docker_client.exec_start.assert_called_once_with(fake_execid, stream=False)


def test_main():
    fake_container_id = 'fake_container_id'
    fake_timeout = 3
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_execute_docker_command.get_container_id_for_mesos_id',
                   return_value=fake_container_id),
        mock.patch('paasta_tools.paasta_execute_docker_command.parse_args'),
        mock.patch('paasta_tools.paasta_execute_docker_command.execute_in_container',
                   return_value=('fake_output', 0)),
        mock.patch('paasta_tools.paasta_execute_docker_command.time_limit')
    ) as (
        get_id_patch,
        args_patch,
        exec_patch,
        time_limit_patch,
    ):
        args_patch.return_value.mesos_id = 'fake_task_id'
        args_patch.return_value.timeout = fake_timeout
        with pytest.raises(SystemExit) as excinfo:
            main()
        time_limit_patch.assert_called_once_with(fake_timeout)
        assert excinfo.value.code == 0


def test_main_with_empty_task_id():
    fake_container_id = 'fake_container_id'
    fake_timeout = 3
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_execute_docker_command.get_container_id_for_mesos_id',
                   return_value=fake_container_id),
        mock.patch('paasta_tools.paasta_execute_docker_command.parse_args'),
        mock.patch('paasta_tools.paasta_execute_docker_command.execute_in_container',
                   return_value=('fake_output', 0)),
        mock.patch('paasta_tools.paasta_execute_docker_command.time_limit')
    ) as (
        get_id_patch,
        args_patch,
        exec_patch,
        time_limit_patch,
    ):
        args_patch.return_value.mesos_id = ''
        args_patch.return_value.timeout = fake_timeout
        with pytest.raises(SystemExit) as excinfo:
            main()
        assert excinfo.value.code == 2


def test_main_container_not_found_failure():
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_execute_docker_command.get_container_id_for_mesos_id',
                   return_value=None),
        mock.patch('paasta_tools.paasta_execute_docker_command.execute_in_container',
                   return_value=('fake_output', 2)),
        mock.patch('paasta_tools.paasta_execute_docker_command.parse_args'),
        mock.patch('paasta_tools.paasta_execute_docker_command.time_limit')
    ) as (
        get_id_patch,
        exec_patch,
        args_patch,
        time_limit_patch,
    ):
        args_patch.return_value.mesos_id = 'fake_task_id'
        with pytest.raises(SystemExit) as excinfo:
            main()
        assert excinfo.value.code == 1


def test_main_cmd_unclean_exit_failure():
    fake_container_id = 'fake_container_id'
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_execute_docker_command.get_container_id_for_mesos_id',
                   return_value=fake_container_id),
        mock.patch('paasta_tools.paasta_execute_docker_command.execute_in_container',
                   return_value=('fake_output', 2)),
        mock.patch('paasta_tools.paasta_execute_docker_command.parse_args'),
        mock.patch('paasta_tools.paasta_execute_docker_command.time_limit')
    ) as (
        get_id_patch,
        exec_patch,
        args_patch,
        time_limit_patch,
    ):
        args_patch.return_value.mesos_id = 'fake_task_id'
        with pytest.raises(SystemExit) as excinfo:
            main()
        assert excinfo.value.code == 2


def test_main_timeout_failure():
    fake_container_id = 'fake_container_id'
    fake_timeout = 3
    with contextlib.nested(
        mock.patch('paasta_tools.paasta_execute_docker_command.get_container_id_for_mesos_id',
                   return_value=fake_container_id),
        mock.patch('paasta_tools.paasta_execute_docker_command.parse_args'),
        mock.patch('paasta_tools.paasta_execute_docker_command.execute_in_container',
                   return_value=('fake_output', 0)),
        mock.patch('paasta_tools.paasta_execute_docker_command.time_limit', side_effect=TimeoutException())
    ) as (
        get_id_patch,
        args_patch,
        exec_patch,
        time_limit_patch,
    ):
        args_patch.return_value.mesos_id = 'fake_task_id'
        args_patch.return_value.timeout = fake_timeout
        with pytest.raises(SystemExit) as excinfo:
            main()
        time_limit_patch.assert_called_once_with(fake_timeout)
        assert excinfo.value.code == 1
