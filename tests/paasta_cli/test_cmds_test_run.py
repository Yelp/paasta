from mock import MagicMock
from mock import patch

from paasta_tools.paasta_cli.cmds.test_run import build_docker_container
from paasta_tools.paasta_cli.cmds.test_run import paasta_test_run


def test_build_docker_container():
    docker_client = MagicMock()
    args = MagicMock()

    docker_client.build.return_value = [
        '{"stream":"foo\\n"}',
        '{"stream":"foo\\n"}',
        '{"stream":"Successfully built 1234\\n"}'
    ]
    assert build_docker_container(docker_client, args) == '1234'


@patch('paasta_tools.paasta_cli.cmds.test_run.Client', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.test_run.build_docker_container', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.test_run.run_docker_container', autospec=True)
def test_run_success(
    mock_Client,
    mock_build_docker_container,
    mock_run_docker_container,
):
    mock_Client.return_value = None
    mock_build_docker_container.return_value = None
    mock_run_docker_container.return_value = None

    args = MagicMock()

    assert paasta_test_run(args) is None
