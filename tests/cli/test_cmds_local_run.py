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
import pipes
import shlex

import docker
import mock
from pytest import raises

from paasta_tools.cli.cmds.local_run import command_function_for_framework
from paasta_tools.cli.cmds.local_run import configure_and_run_docker_container
from paasta_tools.cli.cmds.local_run import docker_pull_image
from paasta_tools.cli.cmds.local_run import get_container_id
from paasta_tools.cli.cmds.local_run import get_container_name
from paasta_tools.cli.cmds.local_run import get_docker_run_cmd
from paasta_tools.cli.cmds.local_run import LostContainerException
from paasta_tools.cli.cmds.local_run import paasta_local_run
from paasta_tools.cli.cmds.local_run import perform_cmd_healthcheck
from paasta_tools.cli.cmds.local_run import perform_http_healthcheck
from paasta_tools.cli.cmds.local_run import perform_tcp_healthcheck
from paasta_tools.cli.cmds.local_run import run_docker_container
from paasta_tools.cli.cmds.local_run import run_healthcheck_on_container
from paasta_tools.cli.cmds.local_run import simulate_healthcheck_on_service
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import TimeoutError


@mock.patch('paasta_tools.cli.cmds.local_run.execute_in_container')
def test_perform_cmd_healthcheck_success(mock_exec_container):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_container_id = 'fake_container_id'
    fake_cmd = '/bin/false'
    fake_timeout = 10
    mock_exec_container.return_value = ('fake_output', 0)
    assert perform_cmd_healthcheck(mock_docker_client, fake_container_id, fake_cmd, fake_timeout)
    mock_exec_container.assert_called_with(mock_docker_client, fake_container_id, fake_cmd, fake_timeout)


@mock.patch('socket.socket.connect_ex')
def test_perform_tcp_healthcheck_success(mock_socket_connect):
    fake_tcp_url = "tcp://fakehost:1234"
    fake_timeout = 10
    mock_socket_connect.return_value = 0
    assert perform_tcp_healthcheck(fake_tcp_url, fake_timeout)
    mock_socket_connect.assert_called_with(('fakehost', 1234))


@mock.patch('socket.socket.connect_ex')
def test_perform_tcp_healthcheck_failure(mock_socket_connect):
    fake_tcp_url = "tcp://fakehost:1234"
    fake_timeout = 10
    mock_socket_connect.return_value = 1
    actual = perform_tcp_healthcheck(fake_tcp_url, fake_timeout)
    assert actual[0] is False
    assert 'timeout' in actual[1]
    assert '10 seconds' in actual[1]


@mock.patch('requests.head')
def test_perform_http_healthcheck_success(mock_http_conn):
    fake_http_url = "http://fakehost:1234/fake_status_path"
    fake_timeout = 10

    mock_http_conn.return_value = mock.Mock(status_code=200, headers={})
    assert perform_http_healthcheck(fake_http_url, fake_timeout)
    mock_http_conn.assert_called_once_with(fake_http_url)


@mock.patch('requests.head')
def test_perform_http_healthcheck_failure(mock_http_conn):
    fake_http_url = "http://fakehost:1234/fake_status_path"
    fake_timeout = 10

    mock_http_conn.return_value = mock.Mock(status_code=400, headers={})
    result, reason = perform_http_healthcheck(fake_http_url, fake_timeout)
    assert result is False
    mock_http_conn.assert_called_once_with(fake_http_url)


@mock.patch('requests.head', side_effect=TimeoutError)
def test_perform_http_healthcheck_timeout(mock_http_conn):
    fake_http_url = "http://fakehost:1234/fake_status_path"
    fake_timeout = 10

    actual = perform_http_healthcheck(fake_http_url, fake_timeout)
    assert actual[0] is False
    assert "10" in actual[1]
    assert "timed out" in actual[1]
    mock_http_conn.assert_called_once_with(fake_http_url)


@mock.patch('requests.head')
def test_perform_http_healthcheck_failure_with_multiple_content_type(mock_http_conn):
    fake_http_url = "http://fakehost:1234/fake_status_path"
    fake_timeout = 10

    mock_http_conn.return_value = mock.Mock(
        status_code=200, headers={'content-type': 'fake_content_type_1, fake_content_type_2'})
    actual = perform_http_healthcheck(fake_http_url, fake_timeout)
    assert actual[0] is False
    assert "200" in actual[1]
    mock_http_conn.assert_called_once_with(fake_http_url)


@mock.patch('paasta_tools.cli.cmds.local_run.perform_http_healthcheck')
@mock.patch('time.sleep')
def test_run_healthcheck_http_success(mock_sleep, mock_perform_http_healthcheck):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_container_id = 'fake_container_id'
    fake_mode = 'http'
    fake_url = 'http://fakehost:666/fake_status_path'
    fake_timeout = 10

    mock_perform_http_healthcheck.return_value = True
    assert run_healthcheck_on_container(
        mock_docker_client, fake_container_id, fake_mode, fake_url, fake_timeout)
    mock_perform_http_healthcheck.assert_called_once_with(fake_url, fake_timeout)


@mock.patch('paasta_tools.cli.cmds.local_run.perform_http_healthcheck')
@mock.patch('time.sleep')
def test_run_healthcheck_http_fails(mock_sleep, mock_perform_http_healthcheck):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_container_id = 'fake_container_id'
    fake_mode = 'http'
    fake_url = 'http://fakehost:666/fake_status_path'
    fake_timeout = 10

    mock_perform_http_healthcheck.return_value = False
    assert not run_healthcheck_on_container(
        mock_docker_client, fake_container_id, fake_mode, fake_url, fake_timeout)
    mock_perform_http_healthcheck.assert_called_once_with(fake_url, fake_timeout)


@mock.patch('paasta_tools.cli.cmds.local_run.perform_tcp_healthcheck')
@mock.patch('time.sleep')
def test_run_healthcheck_tcp_success(mock_sleep, mock_perform_tcp_healthcheck):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_container_id = 'fake_container_id'
    fake_mode = 'tcp'
    fake_url = 'tcp://fakehost:666'
    fake_timeout = 10

    mock_perform_tcp_healthcheck.return_value = True
    assert run_healthcheck_on_container(
        mock_docker_client, fake_container_id, fake_mode, fake_url, fake_timeout)
    assert mock_perform_tcp_healthcheck.call_count == 1
    mock_perform_tcp_healthcheck.assert_called_once_with(fake_url, fake_timeout)


@mock.patch('paasta_tools.cli.cmds.local_run.perform_tcp_healthcheck')
@mock.patch('time.sleep')
def test_run_healthcheck_tcp_fails(mock_sleep, mock_perform_tcp_healthcheck):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_container_id = 'fake_container_id'
    fake_mode = 'tcp'
    fake_url = 'tcp://fakehost:666'
    fake_timeout = 10

    mock_perform_tcp_healthcheck.return_value = False
    assert not run_healthcheck_on_container(
        mock_docker_client, fake_container_id, fake_mode, fake_url, fake_timeout)
    assert mock_perform_tcp_healthcheck.call_count == 1
    mock_perform_tcp_healthcheck.assert_called_once_with(fake_url, fake_timeout)


@mock.patch('paasta_tools.cli.cmds.local_run.perform_cmd_healthcheck')
@mock.patch('time.sleep')
def test_run_healthcheck_cmd_success(mock_sleep, mock_perform_cmd_healthcheck):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_container_id = 'fake_container_id'
    fake_mode = 'cmd'
    fake_cmd = '/bin/true'
    fake_timeout = 10

    mock_perform_cmd_healthcheck.return_value = True
    assert run_healthcheck_on_container(
        mock_docker_client, fake_container_id, fake_mode, fake_cmd, fake_timeout)
    assert mock_perform_cmd_healthcheck.call_count == 1
    mock_perform_cmd_healthcheck.assert_called_once_with(mock_docker_client, fake_container_id, fake_cmd, fake_timeout)


@mock.patch('paasta_tools.cli.cmds.local_run.perform_cmd_healthcheck')
@mock.patch('time.sleep')
def test_run_healthcheck_cmd_fails(mock_sleep, mock_perform_cmd_healthcheck):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_container_id = 'fake_container_id'
    fake_mode = 'cmd'
    fake_cmd = '/bin/true'
    fake_timeout = 10

    mock_perform_cmd_healthcheck.return_value = False
    assert not run_healthcheck_on_container(
        mock_docker_client, fake_container_id, fake_mode, fake_cmd, fake_timeout)
    assert mock_perform_cmd_healthcheck.call_count == 1
    mock_perform_cmd_healthcheck.assert_called_once_with(mock_docker_client, fake_container_id, fake_cmd, fake_timeout)


@mock.patch('paasta_tools.cli.cmds.local_run.randint',
            autospec=True,
            return_value=543534,
            )
@mock.patch('paasta_tools.cli.cmds.local_run.get_username',
            autospec=True,
            return_value='fsmonste',
            )
def test_get_container_name(mock_get_username, mock_randint):
    expected = 'paasta_local_run_%s_%s' % (
        mock_get_username.return_value, mock_randint.return_value)
    actual = get_container_name()
    assert actual == expected


@mock.patch('paasta_tools.cli.cmds.local_run.socket.getfqdn', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.run_docker_container', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.load_system_paasta_config', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_instance_config', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.validate_service_instance', autospec=True)
def test_configure_and_run_command_uses_cmd_from_config(
    mock_validate_service_instance,
    mock_get_instance_config,
    mock_load_system_paasta_config,
    mock_run_docker_container,
    mock_socket_getfqdn,
):
    mock_validate_service_instance.return_value = 'marathon'
    mock_load_system_paasta_config.return_value = SystemPaastaConfig(
        {'cluster': 'fake_cluster', 'volumes': []}, '/fake_dir/')
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_get_instance_config.return_value.get_cmd.return_value = 'fake_command'
    mock_socket_getfqdn.return_value = 'fake_hostname'

    fake_service = 'fake_service'
    docker_hash = '8' * 40
    args = mock.MagicMock()
    args.cmd = ''
    args.service = fake_service
    args.instance = 'fake_instance'
    args.healthcheck = False
    args.healthcheck_only = False
    args.interactive = False

    assert configure_and_run_docker_container(
        docker_client=mock_docker_client,
        docker_hash=docker_hash,
        service=fake_service,
        instance='fake_instance',
        cluster='fake_cluster',
        args=args,
    ) is None
    mock_run_docker_container.assert_called_once_with(
        docker_client=mock_docker_client,
        service=fake_service,
        instance=args.instance,
        docker_hash=docker_hash,
        volumes=[],
        interactive=args.interactive,
        command=shlex.split(mock_get_instance_config.return_value.get_cmd.return_value),
        hostname=mock_socket_getfqdn.return_value,
        healthcheck=args.healthcheck,
        healthcheck_only=args.healthcheck_only,
        instance_config=mock_get_instance_config.return_value,
        soa_dir=args.yelpsoa_config_root,
    )


@mock.patch('paasta_tools.cli.cmds.local_run.socket.getfqdn', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.run_docker_container', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.load_system_paasta_config', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_instance_config', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.validate_service_instance', autospec=True)
def test_configure_and_run_uses_bash_by_default_when_interactive(
    mock_validate_service_instance,
    mock_get_instance_config,
    mock_load_system_paasta_config,
    mock_run_docker_container,
    mock_socket_getfqdn,
):
    mock_validate_service_instance.return_value = 'marathon'
    mock_load_system_paasta_config.return_value = SystemPaastaConfig(
        {'cluster': 'fake_cluster', 'volumes': []}, '/fake_dir/')
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_socket_getfqdn.return_value = 'fake_hostname'

    fake_service = 'fake_service'
    docker_hash = '8' * 40
    args = mock.MagicMock()
    args.cmd = None
    args.service = fake_service
    args.healthcheck = False
    args.healthcheck_only = False
    args.instance = 'fake_instance'
    args.interactive = True

    assert configure_and_run_docker_container(
        docker_client=mock_docker_client,
        docker_hash=docker_hash,
        service=fake_service,
        instance='fake_instance',
        cluster='fake_cluster',
        args=args,
    ) is None
    mock_run_docker_container.assert_called_once_with(
        docker_client=mock_docker_client,
        service=fake_service,
        instance=args.instance,
        docker_hash=docker_hash,
        volumes=[],
        interactive=args.interactive,
        command=['bash'],
        hostname=mock_socket_getfqdn.return_value,
        healthcheck=args.healthcheck,
        healthcheck_only=args.healthcheck_only,
        instance_config=mock_get_instance_config.return_value,
        soa_dir=args.yelpsoa_config_root,
    )


@mock.patch('paasta_tools.cli.cmds.local_run.socket.getfqdn', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.docker_pull_image', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.run_docker_container', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.load_system_paasta_config', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_instance_config', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.validate_service_instance', autospec=True)
def test_configure_and_run_pulls_image_when_asked(
    mock_validate_service_instance,
    mock_get_instance_config,
    mock_load_system_paasta_config,
    mock_run_docker_container,
    mock_docker_pull_image,
    mock_socket_getfqdn,
):
    mock_validate_service_instance.return_value = 'marathon'
    mock_load_system_paasta_config.return_value = SystemPaastaConfig(
        {'cluster': 'fake_cluster', 'volumes': [], 'docker_registry': 'fake_registry'}, '/fake_dir/')
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_socket_getfqdn.return_value = 'fake_hostname'

    fake_instance_config = mock.MagicMock(InstanceConfig)
    fake_instance_config.get_docker_image.return_value = 'fake_image'
    mock_get_instance_config.return_value = fake_instance_config
    fake_service = 'fake_service'
    args = mock.MagicMock()
    args.cmd = None
    args.service = fake_service
    args.instance = 'fake_instance'
    args.healthcheck = False
    args.healthcheck_only = False
    args.interactive = True

    assert configure_and_run_docker_container(
        docker_client=mock_docker_client,
        docker_hash=None,
        service=fake_service,
        instance='fake_instance',
        cluster='fake_cluster',
        args=args,
        pull_image=True,
    ) is None
    mock_docker_pull_image.assert_called_once_with('fake_registry/fake_image')
    mock_run_docker_container.assert_called_once_with(
        docker_client=mock_docker_client,
        service=fake_service,
        instance=args.instance,
        docker_hash='fake_registry/fake_image',
        volumes=[],
        interactive=args.interactive,
        command=['bash'],
        hostname=mock_socket_getfqdn.return_value,
        healthcheck=args.healthcheck,
        healthcheck_only=args.healthcheck_only,
        instance_config=mock_get_instance_config.return_value,
        soa_dir=args.yelpsoa_config_root,
    )


@mock.patch('paasta_tools.cli.cmds.local_run.figure_out_service_name', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.configure_and_run_docker_container', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_docker_client', spec_set=docker.Client)
@mock.patch('paasta_tools.cli.cmds.cook_image.validate_service_name', autospec=True)
@mock.patch('paasta_tools.cli.cmds.cook_image.makefile_responds_to', autospec=True)
@mock.patch('paasta_tools.cli.cmds.cook_image._run', autospec=True)
def test_run_success(
    mock_run,
    mock_makefile_responds_to,
    mock_validate_service_name,
    mock_Client,
    mock_run_docker_container,
    mock_figure_out_service_name,
):
    mock_run.return_value = (0, 'Output')
    mock_makefile_responds_to.return_value = True
    mock_validate_service_name.return_value = True
    mock_Client.return_value = None
    mock_run_docker_container.return_value = None
    mock_figure_out_service_name.return_value = 'fake_service'

    args = mock.MagicMock()
    args.service = 'fake_service'
    args.healthcheck = False
    args.interactive = False
    assert paasta_local_run(args) is None


@mock.patch('paasta_tools.cli.cmds.local_run.figure_out_service_name', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.configure_and_run_docker_container', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_docker_client', spec_set=docker.Client)
@mock.patch('paasta_tools.cli.cmds.local_run.paasta_cook_image', autospec=True)
def test_run_cook_image_fails(
    mock_paasta_cook_image,
    mock_Client,
    mock_run_docker_container,
    mock_figure_out_service_name,
):
    mock_paasta_cook_image.return_value = 1
    mock_Client.return_value = None
    mock_run_docker_container.return_value = None
    mock_figure_out_service_name.return_value = 'fake_service'

    args = mock.MagicMock()
    args.service = 'fake_service'
    args.healthcheck = False
    args.interactive = False
    args.pull = False
    assert paasta_local_run(args) is 1
    assert not mock_run_docker_container.called


def test_get_docker_run_cmd_without_additional_args():
    memory = 555
    random_port = 666
    container_name = 'Docker' * 6 + 'Doc'
    volumes = ['7_Brides_for_7_Brothers', '7-Up', '7-11']
    env = {}
    interactive = False
    docker_hash = '8' * 40
    command = None
    hostname = 'fake_hostname'
    net = 'bridge'
    actual = get_docker_run_cmd(memory, random_port, container_name, volumes, env,
                                interactive, docker_hash, command, hostname, net)
    # Since we can't assert that the command isn't present in the output, we do
    # the next best thing and check that the docker hash is the last thing in
    # the docker run command (the command would have to be after it if it existed)
    assert actual[-1] == docker_hash


def test_get_docker_run_cmd_with_env_vars():
    memory = 555
    random_port = 666
    container_name = 'Docker' * 6 + 'Doc'
    volumes = ['7_Brides_for_7_Brothers', '7-Up', '7-11']
    env = {'foo': 'bar', 'baz': 'qux', 'x': ' with spaces'}
    interactive = False
    docker_hash = '8' * 40
    command = None
    hostname = 'fake_hostname'
    net = 'bridge'
    actual = get_docker_run_cmd(memory, random_port, container_name, volumes, env,
                                interactive, docker_hash, command, hostname, net)
    assert '--env="foo=bar"' in actual
    assert '--env="baz=qux"' in actual
    assert '--env="x= with spaces"' in actual


def test_get_docker_run_cmd_interactive_false():
    memory = 555
    random_port = 666
    container_name = 'Docker' * 6 + 'Doc'
    volumes = ['7_Brides_for_7_Brothers', '7-Up', '7-11']
    env = {}
    interactive = False
    docker_hash = '8' * 40
    command = ['IE9.exe', '/VERBOSE', '/ON_ERROR_RESUME_NEXT']
    hostname = 'fake_hostname'
    net = 'bridge'
    actual = get_docker_run_cmd(memory, random_port, container_name, volumes, env,
                                interactive, docker_hash, command, hostname, net)

    assert any(['--env=MARATHON_PORT=%s' % random_port in arg for arg in actual])
    assert '--memory=%dm' % memory in actual
    assert any(['--publish=%s' % random_port in arg for arg in actual])
    assert '--name=%s' % container_name in actual
    assert all(['--volume=%s' % volume in actual for volume in volumes])
    assert '--detach=true' in actual
    assert '--interactive=true' not in actual
    assert '--tty=true' not in actual
    assert docker_hash in actual
    assert ' '.join(pipes.quote(part) for part in command) in actual


def test_get_docker_run_cmd_interactive_true():
    memory = 555
    random_port = 666
    container_name = 'Docker' * 6 + 'Doc'
    volumes = ['7_Brides_for_7_Brothers', '7-Up', '7-11']
    env = {}
    interactive = True
    docker_hash = '8' * 40
    command = ['IE9.exe', '/VERBOSE', '/ON_ERROR_RESUME_NEXT']
    hostname = 'fake_hostname'
    net = 'bridge'
    actual = get_docker_run_cmd(memory, random_port, container_name, volumes, env,
                                interactive, docker_hash, command, hostname, net)

    assert '--interactive=true' in actual
    assert '--tty=true' in actual


def test_get_docker_run_cmd_memory_swap():
    memory = 555
    random_port = 666
    container_name = 'Docker' * 6 + 'Doc'
    volumes = ['7_Brides_for_7_Brothers', '7-Up', '7-11']
    env = {}
    interactive = False
    docker_hash = '8' * 40
    command = ['IE9.exe', '/VERBOSE', '/ON_ERROR_RESUME_NEXT']
    hostname = 'fake_hostname'
    net = 'bridge'
    actual = get_docker_run_cmd(memory, random_port, container_name, volumes, env,
                                interactive, docker_hash, command, hostname, net)
    assert '--memory-swap=555m' in actual


def test_get_docker_run_cmd_host_networking():
    memory = 555
    random_port = 666
    container_name = 'Docker' * 6 + 'Doc'
    volumes = ['7_Brides_for_7_Brothers', '7-Up', '7-11']
    env = {}
    interactive = True
    docker_hash = '8' * 40
    command = ['IE9.exe', '/VERBOSE', '/ON_ERROR_RESUME_NEXT']
    hostname = 'fake_hostname'
    net = 'host'
    actual = get_docker_run_cmd(memory, random_port, container_name, volumes, env,
                                interactive, docker_hash, command, hostname, net)

    assert '--net=host' in actual


def test_get_container_id():
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_containers = [
        {'Names': ['/paasta_local_run_1'], 'Id': '11111'},
        {'Names': ['/paasta_local_run_2'], 'Id': '22222'},
    ]
    mock_docker_client.containers = mock.MagicMock(
        spec_set=docker.Client,
        return_value=fake_containers,
    )
    container_name = 'paasta_local_run_2'
    expected = '22222'
    actual = get_container_id(mock_docker_client, container_name)
    assert actual == expected


def test_get_container_id_name_not_found():
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_containers = [
        {'Names': ['/paasta_local_run_1'], 'Id': '11111'},
        {'Names': ['/paasta_local_run_2'], 'Id': '22222'},
    ]
    mock_docker_client.containers = mock.MagicMock(
        spec_set=docker.Client,
        return_value=fake_containers,
    )
    container_name = 'paasta_local_run_DOES_NOT_EXIST'
    with raises(LostContainerException):
        get_container_id(mock_docker_client, container_name)


@mock.patch('paasta_tools.cli.cmds.local_run.pick_random_port', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_docker_run_cmd', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.execlp', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run._run', autospec=True, return_value=(0, 'fake _run output'))
@mock.patch('paasta_tools.cli.cmds.local_run.get_container_id', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_healthcheck_for_instance',
            autospec=True,
            return_value=('fake_healthcheck_mode', 'fake_healthcheck_uri'),
            )
def test_run_docker_container_non_interactive(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_execlp,
    mock_get_docker_run_cmd,
    mock_pick_random_port,
):
    mock_pick_random_port.return_value = 666
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.attach = mock.MagicMock(spec_set=docker.Client.attach)
    mock_docker_client.stop = mock.MagicMock(spec_set=docker.Client.stop)
    mock_docker_client.remove_container = mock.MagicMock(spec_set=docker.Client.remove_container)
    mock_docker_client.inspect_container.return_value = {'State': {'ExitCode': 666, 'Running': True}}
    mock_service_manifest = mock.MagicMock(spec_set=MarathonServiceConfig)
    with raises(SystemExit) as excinfo:
        run_docker_container(
            mock_docker_client,
            'fake_service',
            'fake_instance',
            'fake_hash',
            [],
            False,  # interactive
            'fake_command',
            'fake_hostname',
            False,  # healthcheck
            False,  # terminate after healthcheck
            mock_service_manifest,
        )
        mock_service_manifest.get_mem.assert_called_once_with()
        mock_pick_random_port.assert_called_once_with()
        assert mock_get_docker_run_cmd.call_count == 1
        assert mock_get_healthcheck_for_instance.call_count == 1
        assert mock_execlp.call_count == 0
        assert mock_run.call_count == 1
        assert mock_get_container_id.call_count == 1
        assert mock_docker_client.attach.call_count == 1
        assert mock_docker_client.stop.call_count == 1
        assert mock_docker_client.remove_container.call_count == 1
        assert excinfo.value.code == 666


@mock.patch('paasta_tools.cli.cmds.local_run.pick_random_port', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_docker_run_cmd', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.execlp', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run._run', autospec=True, return_value=(0, 'fake _run output'))
@mock.patch('paasta_tools.cli.cmds.local_run.get_container_id', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_healthcheck_for_instance',
            autospec=True,
            return_value=('fake_healthcheck_mode', 'fake_healthcheck_uri'),
            )
def test_run_docker_container_interactive(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_execlp,
    mock_get_docker_run_cmd,
    mock_pick_random_port,
):
    mock_pick_random_port.return_value = 666
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.attach = mock.MagicMock(spec_set=docker.Client.attach)
    mock_docker_client.stop = mock.MagicMock(spec_set=docker.Client.stop)
    mock_docker_client.remove_container = mock.MagicMock(spec_set=docker.Client.remove_container)
    mock_service_manifest = mock.MagicMock(spec_set=MarathonServiceConfig)
    run_docker_container(
        mock_docker_client,
        'fake_service',
        'fake_instance',
        'fake_hash',
        [],
        True,  # interactive
        'fake_command',
        'fake_hostname',
        False,  # healthcheck
        False,  # terminate after healthcheck
        mock_service_manifest,
    )
    mock_service_manifest.get_mem.assert_called_once_with()
    mock_pick_random_port.assert_called_once_with()
    assert mock_get_docker_run_cmd.call_count == 1
    assert mock_get_healthcheck_for_instance.call_count == 1
    assert mock_execlp.call_count == 1
    assert mock_run.call_count == 0
    assert mock_get_container_id.call_count == 0
    assert mock_docker_client.attach.call_count == 0
    assert mock_docker_client.stop.call_count == 0
    assert mock_docker_client.remove_container.call_count == 0


@mock.patch('paasta_tools.cli.cmds.local_run.pick_random_port', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_docker_run_cmd', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.execlp', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run._run', autospec=True, return_value=(0, 'fake _run output'))
@mock.patch('paasta_tools.cli.cmds.local_run.get_container_id', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_healthcheck_for_instance',
            autospec=True,
            return_value=('fake_healthcheck_mode', 'fake_healthcheck_uri'),
            )
def test_run_docker_container_non_interactive_keyboard_interrupt(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_execlp,
    mock_get_docker_run_cmd,
    mock_pick_random_port,
):
    mock_pick_random_port.return_value = 666
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.attach = mock.MagicMock(
        spec_set=docker.Client.attach,
        side_effect=KeyboardInterrupt,
    )
    mock_docker_client.stop = mock.MagicMock(spec_set=docker.Client.stop)
    mock_docker_client.remove_container = mock.MagicMock(spec_set=docker.Client.remove_container)
    mock_service_manifest = mock.MagicMock(spec_set=MarathonServiceConfig)
    mock_docker_client.inspect_container.return_value = {'State': {'ExitCode': 99, 'Running': True}}
    with raises(SystemExit) as excinfo:
        run_docker_container(
            mock_docker_client,
            'fake_service',
            'fake_instance',
            'fake_hash',
            [],
            False,  # interactive
            'fake_command',
            'fake_hostname',
            False,  # healthcheck
            False,  # terminate after healthcheck
            mock_service_manifest,
        )
        assert mock_docker_client.stop.call_count == 1
        assert mock_docker_client.remove_container.call_count == 1
        assert excinfo.value.code == 99


@mock.patch('paasta_tools.cli.cmds.local_run.pick_random_port', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_docker_run_cmd', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.execlp', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run._run', autospec=True, return_value=(42, 'fake _run output'))
@mock.patch('paasta_tools.cli.cmds.local_run.get_container_id', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_healthcheck_for_instance',
            autospec=True,
            return_value=('fake_healthcheck_mode', 'fake_healthcheck_uri'),
            )
def test_run_docker_container_non_interactive_run_returns_nonzero(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_execlp,
    mock_get_docker_run_cmd,
    mock_pick_random_port,
):
    mock_pick_random_port.return_value = 666
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.attach = mock.MagicMock(
        spec_set=docker.Client.attach,
    )
    mock_docker_client.stop = mock.MagicMock(spec_set=docker.Client.stop)
    mock_docker_client.remove_container = mock.MagicMock(spec_set=docker.Client.remove_container)
    mock_service_manifest = mock.MagicMock(spec_set=MarathonServiceConfig)
    mock_docker_client.inspect_container.return_value = {'State': {'ExitCode': 99, 'Running': True}}
    with raises(SystemExit) as excinfo:
        run_docker_container(
            mock_docker_client,
            'fake_service',
            'fake_instance',
            'fake_hash',
            [],
            False,  # interactive
            'fake_command',
            'fake_hostname',
            False,  # healthcheck
            False,  # terminate after healthcheck
            mock_service_manifest,
        )
        # Cleanup wont' be necessary and the function should bail out early.
        assert mock_docker_client.stop.call_count == 0
        assert mock_docker_client.remove_container.call_count == 0
        assert excinfo.value.code == 99


@mock.patch('paasta_tools.cli.cmds.local_run.simulate_healthcheck_on_service', autospec=True, return_value=(True, ''))
@mock.patch('paasta_tools.cli.cmds.local_run.pick_random_port', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_docker_run_cmd', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.execlp', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run._run', autospec=True, return_value=(0, 'fake _run output'))
@mock.patch('paasta_tools.cli.cmds.local_run.get_container_id', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_healthcheck_for_instance',
            autospec=True,
            return_value=('fake_healthcheck_mode', 'fake_healthcheck_uri'),
            )
def test_run_docker_container_with_custom_soadir_uses_healthcheck(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_execlp,
    mock_get_docker_run_cmd,
    mock_pick_random_port,
    mock_simulate_healthcheck,
):
    mock_pick_random_port.return_value = 666
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.attach = mock.MagicMock(spec_set=docker.Client.attach)
    mock_docker_client.stop = mock.MagicMock(spec_set=docker.Client.stop)
    mock_docker_client.remove_container = mock.MagicMock(spec_set=docker.Client.remove_container)
    mock_service_manifest = mock.MagicMock(spec_set=MarathonServiceConfig)
    with raises(SystemExit) as excinfo:
        run_docker_container(
            mock_docker_client,
            'fake_service',
            'fake_instance',
            'fake_hash',
            [],
            False,  # interactive
            'fake_command',
            'fake_hostname',
            False,  # healthcheck
            True,  # terminate after healthcheck
            mock_service_manifest,
            soa_dir='fake_soa_dir',
        )
    assert mock_docker_client.stop.call_count == 1
    assert mock_docker_client.remove_container.call_count == 1
    assert excinfo.value.code == 0
    mock_get_healthcheck_for_instance.assert_called_with(
        'fake_service',
        'fake_instance',
        mock_service_manifest,
        mock_pick_random_port.return_value,
        soa_dir='fake_soa_dir',
    )


@mock.patch('paasta_tools.cli.cmds.local_run.simulate_healthcheck_on_service', autospec=True, return_value=(True, ''))
@mock.patch('paasta_tools.cli.cmds.local_run.pick_random_port', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_docker_run_cmd', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.execlp', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run._run', autospec=True, return_value=(0, 'fake _run output'))
@mock.patch('paasta_tools.cli.cmds.local_run.get_container_id', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_healthcheck_for_instance',
            autospec=True,
            return_value=('fake_healthcheck_mode', 'fake_healthcheck_uri'),
            )
def test_run_docker_container_terminates_with_healthcheck_only_success(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_execlp,
    mock_get_docker_run_cmd,
    mock_pick_random_port,
    mock_simulate_healthcheck
):
    mock_pick_random_port.return_value = 666
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.attach = mock.MagicMock(spec_set=docker.Client.attach)
    mock_docker_client.stop = mock.MagicMock(spec_set=docker.Client.stop)
    mock_docker_client.remove_container = mock.MagicMock(spec_set=docker.Client.remove_container)
    mock_service_manifest = mock.MagicMock(spec_set=MarathonServiceConfig)
    with raises(SystemExit) as excinfo:
        run_docker_container(
            mock_docker_client,
            'fake_service',
            'fake_instance',
            'fake_hash',
            [],
            False,  # interactive
            'fake_command',
            'fake_hostname',
            False,  # healthcheck
            True,  # terminate after healthcheck
            mock_service_manifest,
        )
    assert mock_docker_client.stop.call_count == 1
    assert mock_docker_client.remove_container.call_count == 1
    assert excinfo.value.code == 0


@mock.patch('paasta_tools.cli.cmds.local_run.simulate_healthcheck_on_service', autospec=True, return_value=(False, ''))
@mock.patch('paasta_tools.cli.cmds.local_run.pick_random_port', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_docker_run_cmd', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.execlp', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run._run', autospec=True, return_value=(0, 'fake _run output'))
@mock.patch('paasta_tools.cli.cmds.local_run.get_container_id', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.get_healthcheck_for_instance',
            autospec=True,
            return_value=('fake_healthcheck_mode', 'fake_healthcheck_uri'),
            )
def test_run_docker_container_terminates_with_healthcheck_only_fail(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_execlp,
    mock_get_docker_run_cmd,
    mock_pick_random_port,
    mock_simulate_healthcheck
):
    mock_pick_random_port.return_value = 666
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.attach = mock.MagicMock(spec_set=docker.Client.attach)
    mock_docker_client.stop = mock.MagicMock(spec_set=docker.Client.stop)
    mock_docker_client.remove_container = mock.MagicMock(spec_set=docker.Client.remove_container)
    mock_service_manifest = mock.MagicMock(spec_set=MarathonServiceConfig)
    with raises(SystemExit) as excinfo:
        run_docker_container(
            mock_docker_client,
            'fake_service',
            'fake_instance',
            'fake_hash',
            [],
            False,  # interactive
            'fake_command',
            'fake_hostname',
            False,  # healthcheck
            True,  # terminate after healthcheck
            mock_service_manifest,
        )
    assert mock_docker_client.stop.call_count == 1
    assert mock_docker_client.remove_container.call_count == 1
    assert excinfo.value.code == 1


@mock.patch('time.sleep', autospec=True)
def test_simulate_healthcheck_on_service_disabled(mock_sleep):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = mock.MagicMock(spec_set=MarathonServiceConfig)
    fake_container_id = 'fake_container_id'
    fake_mode = 'http'
    fake_url = 'http://fake_host/fake_status_path'
    assert simulate_healthcheck_on_service(
        mock_service_manifest, mock_docker_client, fake_container_id, fake_mode, fake_url, False)


@mock.patch('time.sleep', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.run_healthcheck_on_container', autospec=True)
def test_simulate_healthcheck_on_service_enabled_success(mock_run_healthcheck_on_container, mock_sleep):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = MarathonServiceConfig(
        service='fake_name',
        cluster='fake_cluster',
        instance='fake_instance',
        config_dict={
            'healthcheck_grace_period_seconds': 0,
        },
        branch_dict={},
    )
    fake_container_id = 'fake_container_id'
    fake_mode = 'http'
    fake_url = 'http://fake_host/fake_status_path'
    mock_run_healthcheck_on_container.return_value = (True, "it works")
    assert simulate_healthcheck_on_service(
        mock_service_manifest, mock_docker_client, fake_container_id, fake_mode, fake_url, True)


@mock.patch('time.sleep', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.run_healthcheck_on_container', autospec=True)
def test_simulate_healthcheck_on_service_enabled_failure(mock_run_healthcheck_on_container, mock_sleep):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = MarathonServiceConfig(
        service='fake_name',
        cluster='fake_cluster',
        instance='fake_instance',
        config_dict={
            'healthcheck_grace_period_seconds': 0,
        },
        branch_dict={},
    )
    mock_service_manifest

    fake_container_id = 'fake_container_id'
    fake_mode = 'http'
    fake_url = 'http://fake_host/fake_status_path'
    mock_run_healthcheck_on_container.return_value = (False, "it failed")
    actual = simulate_healthcheck_on_service(
        mock_service_manifest, mock_docker_client, fake_container_id, fake_mode, fake_url, True)
    assert actual[0] is False


@mock.patch('time.sleep', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.run_healthcheck_on_container', autospec=True)
def test_simulate_healthcheck_on_service_enabled_partial_failure(mock_run_healthcheck_on_container, mock_sleep):
    mock_run_healthcheck_on_container.side_effect = iter([
        (False, ""), (False, ""), (False, ""), (False, ""), (True, "")])
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = MarathonServiceConfig(
        service='fake_name',
        cluster='fake_cluster',
        instance='fake_instance',
        config_dict={
            'healthcheck_grace_period_seconds': 0,
        },
        branch_dict={},
    )

    fake_container_id = 'fake_container_id'
    fake_mode = 'http'
    fake_url = 'http://fake_host/fake_status_path'
    assert simulate_healthcheck_on_service(
        mock_service_manifest, mock_docker_client, fake_container_id, fake_mode, fake_url, True)
    # First run_healthcheck_on_container call happens silently
    assert mock_run_healthcheck_on_container.call_count == 5
    assert mock_sleep.call_count == 4


@mock.patch('time.sleep', autospec=True)
@mock.patch('time.time', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.run_healthcheck_on_container',
            autospec=True, return_value=(True, "healcheck status"))
def test_simulate_healthcheck_on_service_enabled_during_grace_period(
    mock_run_healthcheck_on_container,
    mock_time,
    mock_sleep
):
    # prevent grace period from ending
    mock_time.side_effect = [0, 0]
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = MarathonServiceConfig(
        service='fake_name',
        cluster='fake_cluster',
        instance='fake_instance',
        config_dict={
            'healthcheck_grace_period_seconds': 1,
        },
        branch_dict={},
    )

    fake_container_id = 'fake_container_id'
    fake_mode = 'http'
    fake_url = 'http://fake_host/fake_status_path'
    assert simulate_healthcheck_on_service(
        mock_service_manifest, mock_docker_client, fake_container_id, fake_mode, fake_url, True)
    assert mock_sleep.call_count == 0
    assert mock_run_healthcheck_on_container.call_count == 1


@mock.patch('time.sleep', autospec=True)
@mock.patch('time.time', autospec=True)
@mock.patch('paasta_tools.cli.cmds.local_run.run_healthcheck_on_container', autospec=True)
def test_simulate_healthcheck_on_service_enabled_honors_grace_period(
    mock_run_healthcheck_on_container,
    mock_time,
    mock_sleep,
    capsys,
):
    # change time to make sure we are sufficiently past grace period
    mock_run_healthcheck_on_container.side_effect = iter([
        (False, "noop"), (False, "noop"), (True, "noop")])

    mock_time.side_effect = [0, 1, 5]
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = MarathonServiceConfig(
        service='fake_name',
        cluster='fake_cluster',
        instance='fake_instance',
        config_dict={
            # only one healthcheck will be performed silently
            'healthcheck_grace_period_seconds': 2,
        },
        branch_dict={},
    )

    fake_container_id = 'fake_container_id'
    fake_mode = 'http'
    fake_url = 'http://fake_host/fake_status_path'
    assert simulate_healthcheck_on_service(
        mock_service_manifest, mock_docker_client, fake_container_id, fake_mode, fake_url, True)
    assert mock_sleep.call_count == 2
    assert mock_run_healthcheck_on_container.call_count == 3
    out, _ = capsys.readouterr()
    assert out.count('Healthcheck failed! (disregarded due to grace period)') == 1
    assert out.count('Healthcheck failed! (Attempt') == 1
    assert out.count('Healthcheck succeeded!') == 1


def test_simulate_healthcheck_on_service_dead_container_exits_immediately(capsys):
    with mock.patch(
            'time.sleep',
            side_effect=AssertionError('sleep should not have been called'),
    ):
        mock_client = mock.MagicMock(spec_set=docker.Client)
        mock_client.inspect_container.return_value = {
            'State': {'Running': False, 'ExitCode': 127},
        }
        fake_service_manifest = MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        ret = simulate_healthcheck_on_service(
            fake_service_manifest, mock_client, mock.sentinel.container_id,
            'http', 'http://fake_host/status', True,
        )
        assert ret == (False, 'Aborted by the user')
        out, _ = capsys.readouterr()
        assert out.count('Container exited with code 127') == 1


@mock.patch('paasta_tools.cli.cmds.local_run._run', autospec=True, return_value=(0, 'fake _run output'))
def test_pull_image_runs_docker_pull(mock_run):
    docker_pull_image('fake_image')
    mock_run.assert_called_once_with('docker pull fake_image', stream=True, stdin=mock.ANY)


@mock.patch('paasta_tools.cli.cmds.local_run._run', autospec=True, return_value=(42, 'fake _run output'))
def test_pull_docker_image_exists_with_failure(mock_run):
    with raises(SystemExit) as excinfo:
        docker_pull_image('fake_image')
    assert excinfo.value.code == 42
    mock_run.assert_called_once_with('docker pull fake_image', stream=True, stdin=mock.ANY)


def test_command_function_for_framework_for_marathon():
    fn = command_function_for_framework('marathon')
    assert fn('foo') == 'foo'


@mock.patch('paasta_tools.cli.cmds.local_run.parse_time_variables')
@mock.patch('paasta_tools.cli.cmds.local_run.datetime')
def test_command_function_for_framework_for_chronos(mock_datetime, mock_parse_time_variables):
    fake_date = mock.Mock()
    mock_datetime.datetime.now.return_value = fake_date
    mock_parse_time_variables.return_value = "foo"
    fn = command_function_for_framework('chronos')
    fn("foo")
    mock_parse_time_variables.assert_called_once_with('foo', fake_date)


def test_command_function_for_framework_throws_error():
    with raises(ValueError):
        assert command_function_for_framework('bogus_string')
