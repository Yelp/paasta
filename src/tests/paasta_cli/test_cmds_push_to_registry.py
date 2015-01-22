import shlex
from subprocess import CalledProcessError

from mock import MagicMock
from mock import patch
from pytest import raises

from paasta_tools.paasta_cli.cmds.push_to_registry import build_command
from paasta_tools.paasta_cli.cmds.push_to_registry import paasta_push_to_registry


def test_build_command():
    upstream_job_name = 'fake_upstream_job_name'
    upstream_git_commit = 'fake_upstream_git_commit'
    expected = 'docker push docker-paasta.yelpcorp.com:443/services-%s:paasta-%s' % (
        upstream_job_name,
        upstream_git_commit,
    )
    expected = shlex.split(expected)
    actual = build_command(upstream_job_name, upstream_git_commit)
    assert actual == expected


@patch('paasta_tools.paasta_cli.cmds.push_to_registry.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.push_to_registry.subprocess', autospec=True)
def test_push_to_registry_subprocess_fail(
    mock_subprocess,
    mock_validate_service_name,
):
    mock_subprocess.check_output.side_effect = [
        CalledProcessError(1, 'fake_cmd'), 0]
    args = MagicMock()
    with raises(CalledProcessError):
        paasta_push_to_registry(args)


@patch('paasta_tools.paasta_cli.cmds.push_to_registry.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.push_to_registry.subprocess', autospec=True)
def test_push_to_registry_success(
    mock_subprocess,
    mock_validate_service_name,
):
    mock_subprocess.check_call.return_value = 0
    args = MagicMock()
    assert paasta_push_to_registry(args) is None


@patch('paasta_tools.paasta_cli.cmds.push_to_registry.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.push_to_registry.subprocess', autospec=True)
def test_push_to_registry_success_with_opts(
    mock_subprocess,
    mock_validate_service_name,
):
    mock_subprocess.check_call.return_value = 0
    args = MagicMock()
    assert paasta_push_to_registry(args) is None


@patch('paasta_tools.paasta_cli.cmds.push_to_registry.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.push_to_registry.subprocess', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.push_to_registry.build_command', autospec=True)
def test_push_to_registry_works_when_service_name_starts_with_services_dash(
    mock_build_command,
    mock_subprocess,
    mock_validate_service_name,
):
    mock_subprocess.check_call.return_value = 0
    args = MagicMock()
    args.service = 'fake_service'
    args.commit = 'unused'
    assert paasta_push_to_registry(args) is None
    mock_build_command.assert_called_once_with('fake_service', 'unused')
