import sys
from subprocess import CalledProcessError

from mock import patch
from pytest import raises

from paasta_tools.paasta_cli.cmds.promote_to_registry import build_promote_command
from paasta_tools.paasta_cli.cmds.promote_to_registry import paasta_promote_to_registry
from paasta_tools.paasta_cli.paasta_cli import parse_args


def test_build_promote_command():
    upstream_job_name = "fake_upstream_job_name"
    upstream_git_commit = "fake_upstream_git_commit"
    expected = "docker push docker-paasta.yelpcorp.com:443/%s:paasta-%s" % (
        upstream_job_name,
        upstream_git_commit,
    )
    actual = build_promote_command(upstream_job_name, upstream_git_commit)
    assert actual == expected


@patch('paasta_tools.paasta_cli.cmds.promote_to_registry.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.promote_to_registry.subprocess', autospec=True)
def test_promote_to_registry_subprocess_fail(
    mock_subprocess,
    mock_validate_service_name,
):
    mock_subprocess.check_call.side_effect = [
        CalledProcessError(1, 'jenkins cmd 1'), 0]
    sys.argv = [
        './paasta_cli', 'promote-to-registry', '--service', 'unused', '--commit', 'unused',
    ]
    parsed_args = parse_args()

    with raises(CalledProcessError):
        paasta_promote_to_registry(parsed_args)


@patch('paasta_tools.paasta_cli.cmds.promote_to_registry.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.promote_to_registry.subprocess', autospec=True)
def test_promote_to_registry_success(
    mock_subprocess,
    mock_validate_service_name,
):
    mock_subprocess.check_call.return_value = 0

    sys.argv = [
        './paasta_cli', 'promote-to-registry', '--service', 'unused', '--commit', 'unused',
    ]
    parsed_args = parse_args()
    assert paasta_promote_to_registry(parsed_args) is None


@patch('paasta_tools.paasta_cli.cmds.promote_to_registry.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.promote_to_registry.subprocess', autospec=True)
def test_promote_to_registry_success_with_opts(
    mock_subprocess,
    mock_validate_service_name,
):
    mock_subprocess.check_call.return_value = 0

    sys.argv = [
        './paasta_cli', 'promote-to-registry', '--service', 'fake_service', '--commit', 'deadbeef',
    ]
    parsed_args = parse_args()
    assert paasta_promote_to_registry(parsed_args) is None
