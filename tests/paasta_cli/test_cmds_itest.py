from subprocess import CalledProcessError

from mock import MagicMock
from mock import patch
from pytest import raises

from paasta_tools.paasta_cli.cmds.itest import build_command
from paasta_tools.paasta_cli.cmds.itest import paasta_itest


def test_build_command():
    upstream_job_name = 'fake_upstream_job_name'
    upstream_git_commit = 'fake_upstream_git_commit'
    expected = 'DOCKER_TAG="docker-paasta.yelpcorp.com:443/services-%s:paasta-%s" make itest' % (
        upstream_job_name,
        upstream_git_commit,
    )
    actual = build_command(upstream_job_name, upstream_git_commit)
    assert actual == expected


@patch('paasta_tools.paasta_cli.cmds.itest.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.itest.subprocess', autospec=True)
def test_itest_subprocess_fail(
    mock_subprocess,
    mock_validate_service_name,
):
    mock_subprocess.check_output.side_effect = [
        CalledProcessError(1, 'fake_cmd'), 0]
    args = MagicMock()
    with raises(CalledProcessError):
        paasta_itest(args)


@patch('paasta_tools.paasta_cli.cmds.itest.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.itest.subprocess', autospec=True)
def test_itest_success(
    mock_subprocess,
    mock_validate_service_name,
):
    mock_subprocess.check_call.return_value = 0

    args = MagicMock()
    assert paasta_itest(args) is None


@patch('paasta_tools.paasta_cli.cmds.itest.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.itest.subprocess', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.itest.build_command', autospec=True)
def test_itest_works_when_service_name_starts_with_services_dash(
    mock_build_command,
    mock_subprocess,
    mock_validate_service_name,
):
    args = MagicMock()
    args.service = 'services-fake_service'
    args.commit = 'unused'
    assert paasta_itest(args) is None
    mock_build_command.assert_called_once_with('fake_service', 'unused')
