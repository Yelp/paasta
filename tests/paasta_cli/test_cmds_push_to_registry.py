from mock import MagicMock
from mock import patch

from paasta_tools.paasta_cli.cmds.push_to_registry import build_command
from paasta_tools.paasta_cli.cmds.push_to_registry import paasta_push_to_registry


def test_build_command():
    upstream_job_name = 'fake_upstream_job_name'
    upstream_git_commit = 'fake_upstream_git_commit'
    expected = 'docker push docker-paasta.yelpcorp.com:443/services-%s:paasta-%s' % (
        upstream_job_name,
        upstream_git_commit,
    )
    actual = build_command(upstream_job_name, upstream_git_commit)
    assert actual == expected


@patch('paasta_tools.paasta_cli.cmds.push_to_registry.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.push_to_registry._run', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.push_to_registry._log', autospec=True)
@patch('sys.exit', autospec=True)
def test_push_to_registry_run_fail(
    mock_exit,
    mock_log,
    mock_run,
    mock_validate_service_name,
):
    mock_run.return_value = (1, 'Bad')
    args = MagicMock()
    paasta_push_to_registry(args)
    mock_exit.assert_called_once_with(1)


@patch('paasta_tools.paasta_cli.cmds.push_to_registry.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.push_to_registry._run', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.push_to_registry._log', autospec=True)
@patch('sys.exit', autospec=True)
def test_push_to_registry_success(
    mock_exit,
    mock_log,
    mock_run,
    mock_validate_service_name,
):
    mock_run.return_value = (0, 'Success')
    args = MagicMock()
    assert paasta_push_to_registry(args) is None


@patch('paasta_tools.paasta_cli.cmds.push_to_registry.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.push_to_registry._run', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.push_to_registry._log', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.push_to_registry.build_command', autospec=True)
@patch('sys.exit', autospec=True)
def test_push_to_registry_works_when_service_name_starts_with_services_dash(
    mock_exit,
    mock_build_command,
    mock_log,
    mock_run,
    mock_validate_service_name,
):
    mock_run.return_value = (0, 'Success')
    args = MagicMock()
    args.service = 'fake_service'
    args.commit = 'unused'
    assert paasta_push_to_registry(args) is None
    mock_build_command.assert_called_once_with('fake_service', 'unused')
