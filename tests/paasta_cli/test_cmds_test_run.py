import mock
from pytest import raises

from paasta_tools.marathon_tools import CONTAINER_PORT
from paasta_tools.paasta_cli.cmds.test_run import build_docker_container
from paasta_tools.paasta_cli.cmds.test_run import get_cmd
from paasta_tools.paasta_cli.cmds.test_run import get_cmd_string
from paasta_tools.paasta_cli.cmds.test_run import paasta_test_run
from paasta_tools.paasta_cli.cmds.test_run import run_docker_container_non_interactive
from paasta_tools.paasta_cli.cmds.test_run import validate_environment


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


@mock.patch('paasta_tools.paasta_cli.cmds.test_run.validate_environment', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.test_run.figure_out_service_name', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.test_run.validate_service_name', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.test_run.run_docker_container', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.test_run.build_docker_container', autospec=True)
@mock.patch('paasta_tools.paasta_cli.cmds.test_run.Client', autospec=True)
def test_run_success(
    mock_Client,
    mock_build_docker_container,
    mock_run_docker_container,
    mock_validate_service_name,
    mock_figure_out_service_name,
    mock_validate_environment,
):
    mock_Client.return_value = None
    mock_build_docker_container.return_value = None
    mock_run_docker_container.return_value = None
    mock_validate_service_name.return_value = True
    mock_figure_out_service_name.return_value = 'fake_service'
    mock_validate_environment.return_value = None

    args = mock.MagicMock()
    args.service = 'fake_service'

    assert paasta_test_run(args) is None


@mock.patch('paasta_tools.paasta_cli.cmds.test_run.pick_random_port', autospec=True)
def test_run_docker_container_non_interactive(
    mock_pick_random_port,
):
    mock_pick_random_port.return_value = 666
    mock_docker_client = mock.MagicMock(spec='docker.Client')
    mock_docker_client.create_container = mock.MagicMock(spec='docker.Client.create_container')
    mock_docker_client.start = mock.MagicMock(spec='docker.Client.start')
    mock_docker_client.attach = mock.MagicMock(spec='docker.Client.attach')
    mock_docker_client.stop = mock.MagicMock(spec='docker.Client.stop')
    mock_docker_client.remove_container = mock.MagicMock(spec='docker.Client.remove_container')
    run_docker_container_non_interactive(
        mock_docker_client,
        'fake_service',
        'fake_instance',
        'fake_hash',
        [],
        'fake_command',
        mock.MagicMock(),
    )
    mock_pick_random_port.assert_called_once_with()
    mock_docker_client.start.assert_called_once_with(mock.ANY, port_bindings={CONTAINER_PORT: 666})


@mock.patch('paasta_tools.paasta_cli.cmds.test_run.get_cmd', autospec=True)
def test_get_cmd_string(
    mock_get_cmd,
):
    mock_get_cmd.return_value = 'fake_cmd'
    actual = get_cmd_string()
    assert 'fake_cmd' in actual


@mock.patch('paasta_tools.paasta_cli.cmds.test_run.read_local_dockerfile_lines', autospec=True)
def test_get_cmd_when_working(
    mock_read_local_dockerfile_lines,
):
    mock_read_local_dockerfile_lines.return_value = ['CMD BLA']
    actual = get_cmd()
    assert 'BLA' == actual


@mock.patch('paasta_tools.paasta_cli.cmds.test_run.read_local_dockerfile_lines', autospec=True)
def test_get_cmd_when_unknown(
    mock_read_local_dockerfile_lines,
):
    mock_read_local_dockerfile_lines.return_value = []
    actual = get_cmd()
    assert 'Unknown' in actual
