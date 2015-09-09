import shlex

import docker
import mock
from pytest import raises

from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.paasta_cli.cmds.local_run import LostContainerException
from paasta_tools.paasta_cli.cmds.local_run import build_docker_container
from paasta_tools.paasta_cli.cmds.local_run import configure_and_run_docker_container
from paasta_tools.paasta_cli.cmds.local_run import get_cmd
from paasta_tools.paasta_cli.cmds.local_run import get_cmd_string
from paasta_tools.paasta_cli.cmds.local_run import get_container_id
from paasta_tools.paasta_cli.cmds.local_run import get_container_name
from paasta_tools.paasta_cli.cmds.local_run import get_docker_run_cmd
from paasta_tools.paasta_cli.cmds.local_run import paasta_local_run
from paasta_tools.paasta_cli.cmds.local_run import perform_http_healthcheck
from paasta_tools.paasta_cli.cmds.local_run import perform_tcp_healthcheck
from paasta_tools.paasta_cli.cmds.local_run import perform_cmd_healthcheck
from paasta_tools.paasta_cli.cmds.local_run import simulate_healthcheck_on_service
from paasta_tools.paasta_cli.cmds.local_run import run_healthcheck_on_container
from paasta_tools.paasta_cli.cmds.local_run import run_docker_container
from paasta_tools.paasta_cli.cmds.local_run import validate_environment
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import TimeoutError


def test_build_docker_container():
    docker_client = mock.MagicMock()
    args = mock.MagicMock()

    docker_client.build.return_value = [
        '{"stream":null}',
        '{"stream":"foo\\n"}',
        '{"stream":"foo\\n"}',
        '{"stream":"Successfully built 1234\\n"}'
    ]
    assert build_docker_container(docker_client, args) == '1234'


def test_build_docker_container_fails():
    docker_client = mock.MagicMock()
    args = mock.MagicMock()

    docker_client.build.return_value = [
        '{"stream":null}',
        '{"stream":"foo\\n"}',
        '{"stream":"foo\\n"}',
        '{"stream":"failed\\n"}'
    ]
    with raises(SystemExit) as sys_exit:
        build_docker_container(docker_client, args)
    assert sys_exit.value.code == 1


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.execute_in_container')
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
    assert not perform_tcp_healthcheck(fake_tcp_url, fake_timeout)


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
    assert not perform_http_healthcheck(fake_http_url, fake_timeout)
    mock_http_conn.assert_called_once_with(fake_http_url)


@mock.patch('requests.head', side_effect=TimeoutError)
def test_perform_http_healthcheck_timeout(mock_http_conn):
    fake_http_url = "http://fakehost:1234/fake_status_path"
    fake_timeout = 10

    assert not perform_http_healthcheck(fake_http_url, fake_timeout)
    mock_http_conn.assert_called_once_with(fake_http_url)


@mock.patch('requests.head')
def test_perform_http_healthcheck_failure_with_multiple_content_type(mock_http_conn):
    fake_http_url = "http://fakehost:1234/fake_status_path"
    fake_timeout = 10

    mock_http_conn.return_value = mock.Mock(
        status_code=200, headers={'content-type': 'fake_content_type_1, fake_content_type_2'})
    assert not perform_http_healthcheck(fake_http_url, fake_timeout)
    mock_http_conn.assert_called_once_with(fake_http_url)


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.perform_http_healthcheck')
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


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.perform_http_healthcheck')
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


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.perform_tcp_healthcheck')
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


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.perform_tcp_healthcheck')
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


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.perform_cmd_healthcheck')
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


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.perform_cmd_healthcheck')
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


@mock.patch('os.path.expanduser', autospec=True)
@mock.patch('os.getcwd', autospec=True)
def test_validate_environment_fail_in_homedir(
    mock_getcwd,
    mock_expanduser,
):
    fake_home = '/fake_home'
    mock_getcwd.return_value = fake_home
    mock_expanduser.return_value = fake_home

    with raises(SystemExit) as sys_exit:
        validate_environment()

    assert sys_exit.value.code == 1


@mock.patch('os.path.join', autospec=True)
@mock.patch('os.path.isfile', autospec=True)
@mock.patch('os.path.expanduser', autospec=True)
@mock.patch('os.getcwd', autospec=True)
def test_validate_environment_fail_no_dockerfile(
    mock_getcwd,
    mock_expanduser,
    mock_isfile,
    mock_pathjoin,
):
    mock_getcwd.return_value = 'doesntmatter'
    mock_expanduser.return_value = 'nothomedir'
    mock_pathjoin.return_value = 'something'
    mock_isfile.return_value = False

    with raises(SystemExit) as sys_exit:
        validate_environment()

    assert sys_exit.value.code == 1


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.randint',
            autospec=True,
            # http://operations.irclogs.yelpcorp.com/2015-05-12.html#0/h0,1
            return_value=543534,
            )
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_username',
            autospec=True,
            return_value='fsmonste',
            )
def test_get_container_name(mock_get_username, mock_randint):
    expected = 'paasta_local_run_%s_%s' % (
        mock_get_username.return_value, mock_randint.return_value)
    actual = get_container_name()
    assert actual == expected


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.load_system_paasta_config', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.load_marathon_service_config', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.run_docker_container', autospec=True)
def test_configure_and_run_explicit_cluster(
    mock_run_docker_container,
    mock_load_marathon_service_config,
    mock_load_system_paasta_config,
):
    mock_load_system_paasta_config.return_value = SystemPaastaConfig(
        {'cluster': 'fake_cluster_that_will_be_overriden', 'volumes': []}, '/fake_dir/')
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_service = 'fake_service'
    docker_hash = '8' * 40
    args = mock.MagicMock()
    args.cmd = 'fake_command'
    args.service = fake_service
    args.healthcheck = False
    args.instance = 'fake_instance'
    args.interactive = False
    args.cluster = 'fake_cluster'
    args.soaconfig_root = 'fakesoa-configs/'
    assert configure_and_run_docker_container(mock_docker_client, docker_hash, fake_service, args) is None
    mock_load_marathon_service_config.assert_called_once_with(
        fake_service,
        args.instance,
        args.cluster,
        load_deployments=False,
        soa_dir=args.soaconfig_root
    )


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.load_system_paasta_config', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.load_marathon_service_config', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_default_cluster_for_service', autospec=True)
def test_configure_and_run_missing_cluster_exception(
    mock_get_default_cluster_for_service,
    mock_load_marathon_service_config,
    mock_load_system_paasta_config,
):
    mock_get_default_cluster_for_service.side_effect = NoConfigurationForServiceError()
    mock_load_system_paasta_config.return_value = SystemPaastaConfig({'volumes': []}, '/fake_dir/')
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_service = 'fake_service'
    docker_hash = '8' * 40
    args = mock.MagicMock()
    args.cmd = 'fake_command'
    args.service = fake_service
    args.healthcheck = False
    args.instance = 'fake_instance'
    args.interactive = False
    args.cluster = None
    with raises(SystemExit) as excinfo:
        configure_and_run_docker_container(mock_docker_client, docker_hash, fake_service, args)
    assert excinfo.value.code == 2


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.run_docker_container', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.load_system_paasta_config', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.load_marathon_service_config', autospec=True)
def test_configure_and_run_command_uses_cmd_from_config(
    mock_load_marathon_service_config,
    mock_load_system_paasta_config,
    mock_run_docker_container,
):
    mock_load_system_paasta_config.return_value = SystemPaastaConfig(
        {'cluster': 'fake_cluster', 'volumes': []}, '/fake_dir/')
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    fake_service = 'fake_service'
    docker_hash = '8' * 40
    args = mock.MagicMock()
    args.cmd = ''
    args.service = fake_service
    args.healthcheck = False
    args.healthcheck_only = False
    args.instance = 'fake_instance'
    args.interactive = False
    args.cluster = 'fake_cluster'

    mock_load_marathon_service_config.return_value.get_cmd.return_value = 'fake_command'

    configure_and_run_docker_container(mock_docker_client, docker_hash, fake_service, args) is None
    mock_run_docker_container.assert_called_once_with(
        mock_docker_client,
        fake_service,
        args.instance,
        docker_hash,
        [],
        args.interactive,
        shlex.split(mock_load_marathon_service_config.return_value.get_cmd.return_value),
        args.healthcheck,
        args.healthcheck_only,
        mock_load_marathon_service_config.return_value
    )


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.validate_environment', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.figure_out_service_name', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.configure_and_run_docker_container', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.build_docker_container', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.Client', autospec=True)
def test_run_success(
    mock_Client,
    mock_build_docker_container,
    mock_run_docker_container,
    mock_figure_out_service_name,
    mock_validate_environment,
):
    mock_Client.return_value = None
    mock_build_docker_container.return_value = None
    mock_run_docker_container.return_value = None
    mock_figure_out_service_name.return_value = 'fake_service'
    mock_validate_environment.return_value = None

    args = mock.MagicMock()
    args.service = 'fake_service'
    args.healthcheck = False
    args.interactive = False
    assert paasta_local_run(args) is None


def test_get_docker_run_cmd_without_additional_args():
    memory = 555
    random_port = 666
    container_name = 'Docker' * 6 + 'Doc'
    volumes = ['7_Brides_for_7_Brothers', '7-Up', '7-11']
    env = {}
    interactive = False
    docker_hash = '8' * 40
    command = None
    actual = get_docker_run_cmd(memory, random_port, container_name, volumes, env, interactive, docker_hash, command)
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
    actual = get_docker_run_cmd(memory, random_port, container_name, volumes, env, interactive, docker_hash, command)
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
    actual = get_docker_run_cmd(memory, random_port, container_name, volumes, env, interactive, docker_hash, command)

    assert any(['--env=PORT=' in arg for arg in actual])
    assert '--memory=%dm' % memory in actual
    assert any(['--publish=%s' % random_port in arg for arg in actual])
    assert '--name=%s' % container_name in actual
    assert all(['--volume=%s' % volume in actual for volume in volumes])
    assert '--detach=true' in actual
    assert '--interactive=true' not in actual
    assert '--tty=true' not in actual
    assert docker_hash in actual
    assert all([arg in actual for arg in command])


def test_get_docker_run_cmd_interactive_true():
    memory = 555
    random_port = 666
    container_name = 'Docker' * 6 + 'Doc'
    volumes = ['7_Brides_for_7_Brothers', '7-Up', '7-11']
    env = {}
    interactive = True
    docker_hash = '8' * 40
    command = ['IE9.exe', '/VERBOSE', '/ON_ERROR_RESUME_NEXT']
    actual = get_docker_run_cmd(memory, random_port, container_name, volumes, env, interactive, docker_hash, command)

    assert '--interactive=true' in actual
    assert '--tty=true' in actual


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


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.pick_random_port', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_docker_run_cmd', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.execlp', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_cmd_string', autospec=True, return_value='CMD.exe')
@mock.patch('paasta_tools.paasta_cli.cmds.local_run._run', autospec=True, return_value=(0, 'fake _run output'))
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_container_id', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_healthcheck_for_instance',
            autospec=True,
            return_value=('fake_healthcheck_mode', 'fake_healthcheck_uri'),
            )
def test_run_docker_container_non_interactive(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_get_cmd_string,
    mock_execlp,
    mock_get_docker_run_cmd,
    mock_pick_random_port,
):
    mock_pick_random_port.return_value = 666
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_docker_client.attach = mock.MagicMock(spec_set=docker.Client.attach)
    mock_docker_client.stop = mock.MagicMock(spec_set=docker.Client.stop)
    mock_docker_client.remove_container = mock.MagicMock(spec_set=docker.Client.remove_container)
    mock_docker_client.inspect_container.return_value = {"State": {"ExitCode": 666}}
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
            False,  # healthcheck
            False,  # terminate after healthcheck
            mock_service_manifest,
        )
        mock_service_manifest.get_mem.assert_called_once_with()
        mock_pick_random_port.assert_called_once_with()
        assert mock_get_docker_run_cmd.call_count == 1
        assert mock_get_healthcheck_for_instance.call_count == 1
        assert mock_get_cmd_string.call_count == 0
        assert mock_execlp.call_count == 0
        assert mock_run.call_count == 1
        assert mock_get_container_id.call_count == 1
        assert mock_docker_client.attach.call_count == 1
        assert mock_docker_client.stop.call_count == 1
        assert mock_docker_client.remove_container.call_count == 1
        assert excinfo.value.code == 666


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.pick_random_port', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_docker_run_cmd', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.execlp', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_cmd_string', autospec=True, return_value='CMD.exe')
@mock.patch('paasta_tools.paasta_cli.cmds.local_run._run', autospec=True, return_value=(0, 'fake _run output'))
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_container_id', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_healthcheck_for_instance',
            autospec=True,
            return_value=('fake_healthcheck_mode', 'fake_healthcheck_uri'),
            )
def test_run_docker_container_interactive(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_get_cmd_string,
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
        False,  # healthcheck
        False,  # terminate after healthcheck
        mock_service_manifest,
    )
    mock_service_manifest.get_mem.assert_called_once_with()
    mock_pick_random_port.assert_called_once_with()
    assert mock_get_docker_run_cmd.call_count == 1
    assert mock_get_healthcheck_for_instance.call_count == 1
    assert mock_get_cmd_string.call_count == 1
    assert mock_execlp.call_count == 1
    assert mock_run.call_count == 0
    assert mock_get_container_id.call_count == 0
    assert mock_docker_client.attach.call_count == 0
    assert mock_docker_client.stop.call_count == 0
    assert mock_docker_client.remove_container.call_count == 0


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.pick_random_port', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_docker_run_cmd', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.execlp', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_cmd_string', autospec=True, return_value='CMD.exe')
@mock.patch('paasta_tools.paasta_cli.cmds.local_run._run', autospec=True, return_value=(0, 'fake _run output'))
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_container_id', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_healthcheck_for_instance',
            autospec=True,
            return_value=('fake_healthcheck_mode', 'fake_healthcheck_uri'),
            )
def test_run_docker_container_non_interactive_keyboard_interrupt(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_get_cmd_string,
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
    mock_docker_client.inspect_container.return_value = {"State": {"ExitCode": 99}}
    with raises(SystemExit) as excinfo:
        run_docker_container(
            mock_docker_client,
            'fake_service',
            'fake_instance',
            'fake_hash',
            [],
            False,  # interactive
            'fake_command',
            False,  # healthcheck
            False,  # terminate after healthcheck
            mock_service_manifest,
        )
        assert mock_docker_client.stop.call_count == 1
        assert mock_docker_client.remove_container.call_count == 1
        assert excinfo.value.code == 99


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.pick_random_port', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_docker_run_cmd', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.execlp', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_cmd_string', autospec=True, return_value='CMD.exe')
@mock.patch('paasta_tools.paasta_cli.cmds.local_run._run', autospec=True, return_value=(42, 'fake _run output'))
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_container_id', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_healthcheck_for_instance',
            autospec=True,
            return_value=('fake_healthcheck_mode', 'fake_healthcheck_uri'),
            )
def test_run_docker_container_non_interactive_run_returns_nonzero(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_get_cmd_string,
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
    mock_docker_client.inspect_container.return_value = {"State": {"ExitCode": 99}}
    with raises(SystemExit) as excinfo:
        run_docker_container(
            mock_docker_client,
            'fake_service',
            'fake_instance',
            'fake_hash',
            [],
            False,  # interactive
            'fake_command',
            False,  # healthcheck
            False,  # terminate after healthcheck
            mock_service_manifest,
        )
        # Cleanup wont' be necessary and the function should bail out early.
        assert mock_docker_client.stop.call_count == 0
        assert mock_docker_client.remove_container.call_count == 0
        assert excinfo.value.code == 99


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.simulate_healthcheck_on_service', autospec=True, return_value=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.pick_random_port', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_docker_run_cmd', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.execlp', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_cmd_string', autospec=True, return_value='CMD.exe')
@mock.patch('paasta_tools.paasta_cli.cmds.local_run._run', autospec=True, return_value=(0, 'fake _run output'))
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_container_id', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_healthcheck_for_instance',
            autospec=True,
            return_value=('fake_healthcheck_mode', 'fake_healthcheck_uri'),
            )
def test_run_docker_container_terminates_with_healthcheck_only_success(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_get_cmd_string,
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
            False,  # healthcheck
            True,  # terminate after healthcheck
            mock_service_manifest,
        )
    assert mock_docker_client.stop.call_count == 1
    assert mock_docker_client.remove_container.call_count == 1
    assert excinfo.value.code == 0


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.simulate_healthcheck_on_service', autospec=True, return_value=False)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.pick_random_port', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_docker_run_cmd', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.execlp', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_cmd_string', autospec=True, return_value='CMD.exe')
@mock.patch('paasta_tools.paasta_cli.cmds.local_run._run', autospec=True, return_value=(0, 'fake _run output'))
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_container_id', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_healthcheck_for_instance',
            autospec=True,
            return_value=('fake_healthcheck_mode', 'fake_healthcheck_uri'),
            )
def test_run_docker_container_terminates_with_healthcheck_only_fail(
    mock_get_healthcheck_for_instance,
    mock_get_container_id,
    mock_run,
    mock_get_cmd_string,
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
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.run_healthcheck_on_container', autospec=True)
def test_simulate_healthcheck_on_service_enabled_success(mock_run_healthcheck_on_container, mock_sleep):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = MarathonServiceConfig('fake_name', 'fake_instance', {
        'healthcheck_grace_period_seconds': 0,
    }, {})
    fake_container_id = 'fake_container_id'
    fake_mode = 'http'
    fake_url = 'http://fake_host/fake_status_path'
    mock_run_healthcheck_on_container.return_value = True
    assert simulate_healthcheck_on_service(
        mock_service_manifest, mock_docker_client, fake_container_id, fake_mode, fake_url, True)


@mock.patch('time.sleep', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.run_healthcheck_on_container', autospec=True)
def test_simulate_healthcheck_on_service_enabled_failure(mock_run_healthcheck_on_container, mock_sleep):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = MarathonServiceConfig('fake_name', 'fake_instance', {
        'healthcheck_grace_period_seconds': 0,
    }, {})
    mock_service_manifest

    fake_container_id = 'fake_container_id'
    fake_mode = 'http'
    fake_url = 'http://fake_host/fake_status_path'
    mock_run_healthcheck_on_container.return_value = False
    assert not simulate_healthcheck_on_service(
        mock_service_manifest, mock_docker_client, fake_container_id, fake_mode, fake_url, True)


@mock.patch('time.sleep', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.run_healthcheck_on_container',
            autospec=True, side_effect=[False, False, False, False, True])
def test_simulate_healthcheck_on_service_enabled_partial_failure(mock_run_healthcheck_on_container, mock_sleep):
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = MarathonServiceConfig('fake_name', 'fake_instance', {
        'healthcheck_grace_period_seconds': 0,
    }, {})

    fake_container_id = 'fake_container_id'
    fake_mode = 'http'
    fake_url = 'http://fake_host/fake_status_path'
    assert simulate_healthcheck_on_service(
        mock_service_manifest, mock_docker_client, fake_container_id, fake_mode, fake_url, True)
    # First run_healthcheck_on_container call happens silently
    assert mock_run_healthcheck_on_container.call_count == 5
    assert mock_sleep.call_count == 3


@mock.patch('time.sleep', autospec=True)
@mock.patch('time.time', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.run_healthcheck_on_container',
            autospec=True, return_value=True)
def test_simulate_healthcheck_on_service_enabled_during_grace_period(
    mock_run_healthcheck_on_container,
    mock_time,
    mock_sleep
):
    # prevent grace period from ending
    mock_time.side_effect = [0, 0]
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = MarathonServiceConfig('fake_name', 'fake_instance', {
        'healthcheck_grace_period_seconds': 1,
    }, {})

    fake_container_id = 'fake_container_id'
    fake_mode = 'http'
    fake_url = 'http://fake_host/fake_status_path'
    assert simulate_healthcheck_on_service(
        mock_service_manifest, mock_docker_client, fake_container_id, fake_mode, fake_url, True)
    assert mock_sleep.call_count == 0
    # First run_healthcheck_on_container call happens silently
    assert mock_run_healthcheck_on_container.call_count == 2


@mock.patch('time.sleep', autospec=True)
@mock.patch('time.time', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.local_run.run_healthcheck_on_container',
            autospec=True, return_value=[False, True])
def test_simulate_healthcheck_on_service_enabled_honors_grace_period(
    mock_run_healthcheck_on_container,
    mock_time,
    mock_sleep
):
    # change time to make sure we are sufficiently past grace period
    mock_time.side_effect = [0, 2]
    mock_docker_client = mock.MagicMock(spec_set=docker.Client)
    mock_service_manifest = MarathonServiceConfig('fake_name', 'fake_instance', {
        # only one healthcheck will be performed silently
        'healthcheck_grace_period_seconds': 1,
    }, {})

    fake_container_id = 'fake_container_id'
    fake_mode = 'http'
    fake_url = 'http://fake_host/fake_status_path'
    assert simulate_healthcheck_on_service(
        mock_service_manifest, mock_docker_client, fake_container_id, fake_mode, fake_url, True)
    assert mock_sleep.call_count == 0
    assert mock_run_healthcheck_on_container.call_count == 2


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.get_cmd', autospec=True)
def test_get_cmd_string(
    mock_get_cmd,
):
    mock_get_cmd.return_value = 'fake_cmd'
    actual = get_cmd_string()
    assert 'fake_cmd' in actual


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.read_local_dockerfile_lines', autospec=True)
def test_get_cmd_when_working(
    mock_read_local_dockerfile_lines,
):
    mock_read_local_dockerfile_lines.return_value = ['CMD BLA']
    actual = get_cmd()
    assert 'BLA' == actual


@mock.patch('paasta_tools.paasta_cli.cmds.local_run.read_local_dockerfile_lines', autospec=True)
def test_get_cmd_when_unknown(
    mock_read_local_dockerfile_lines,
):
    mock_read_local_dockerfile_lines.return_value = []
    actual = get_cmd()
    assert 'Unknown' in actual
