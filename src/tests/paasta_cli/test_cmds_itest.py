import sys
from subprocess import CalledProcessError

from mock import patch
from pytest import raises

from paasta_tools.paasta_cli.cmds.itest import build_command
from paasta_tools.paasta_cli.cmds.itest import paasta_itest
from paasta_tools.paasta_cli.paasta_cli import parse_args


def test_build_command():
    upstream_job_name = 'fake_upstream_job_name'
    upstream_git_commit = 'fake_upstream_git_commit'
    expected = 'DOCKER_TAG="docker-paasta.yelpcorp.com:443/%s:paasta-%s" make itest' % (
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
    sys.argv = [
        './paasta_cli', 'itest', '--service', 'unused', '--commit', 'unused',
    ]
    parsed_args = parse_args()

    with raises(CalledProcessError):
        paasta_itest(parsed_args)


@patch('paasta_tools.paasta_cli.cmds.itest.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.itest.subprocess', autospec=True)
def test_itest_success(
    mock_subprocess,
    mock_validate_service_name,
):
    mock_subprocess.check_call.return_value = 0

    sys.argv = [
        './paasta_cli', 'itest', '--service', 'unused', '--commit', 'unused',
    ]
    parsed_args = parse_args()
    assert paasta_itest(parsed_args) is None


@patch('paasta_tools.paasta_cli.cmds.itest.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.itest.subprocess', autospec=True)
def test_itest_success_with_opts(
    mock_subprocess,
    mock_validate_service_name,
):
    mock_subprocess.check_call.return_value = 0

    sys.argv = [
        './paasta_cli', 'itest', '--service', 'fake_service', '--commit', 'deadbeef',
    ]
    parsed_args = parse_args()
    assert paasta_itest(parsed_args) is None
