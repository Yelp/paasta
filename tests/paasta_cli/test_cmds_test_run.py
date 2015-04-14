from mock import MagicMock
from mock import patch
from pytest import raises

from paasta_tools.paasta_cli.cmds.test_run import build_docker_container
from paasta_tools.paasta_cli.cmds.test_run import paasta_test_run
from paasta_tools.paasta_cli.cmds.test_run import validate_environment


def test_build_docker_container():
    docker_client = MagicMock()
    args = MagicMock()

    docker_client.build.return_value = [
        '{"stream":"foo\\n"}',
        '{"stream":"foo\\n"}',
        '{"stream":"Successfully built 1234\\n"}'
    ]
    assert build_docker_container(docker_client, args) == '1234'


@patch('os.path.expanduser', autospec=True)
@patch('os.getcwd', autospec=True)
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


@patch('os.path.join', autospec=True)
@patch('os.path.isfile', autospec=True)
@patch('os.path.expanduser', autospec=True)
@patch('os.getcwd', autospec=True)
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


@patch('paasta_tools.paasta_cli.cmds.test_run.validate_environment', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.test_run.figure_out_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.test_run.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.test_run.run_docker_container', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.test_run.build_docker_container', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.test_run.Client', autospec=True)
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

    args = MagicMock()
    args.service = 'fake_service'

    assert paasta_test_run(args) is None
