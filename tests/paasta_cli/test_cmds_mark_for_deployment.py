import shlex
from subprocess import CalledProcessError

from mock import MagicMock
from mock import patch
from pytest import raises

from paasta_tools.paasta_cli.cmds.mark_for_deployment import build_command
from paasta_tools.paasta_cli.cmds.mark_for_deployment import paasta_mark_for_deployment


def test_build_command():
    upstream_git_url = 'fake_upstream_git_url'
    upstream_git_commit = 'fake_upstream_git_commit'
    clusterinstance = 'fake_clusterinstance'
    expected = 'git push -f %s %s:refs/heads/paasta-%s' % (
        upstream_git_url,
        upstream_git_commit,
        clusterinstance,
    )
    expected = shlex.split(expected)
    actual = build_command(upstream_git_url, upstream_git_commit, clusterinstance)
    assert actual == expected


@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment.subprocess', autospec=True)
def test_mark_for_deployment_subprocess_fail(
    mock_subprocess,
):
    mock_subprocess.check_output.side_effect = [
        CalledProcessError(1, 'fake_cmd'), 0]
    args = MagicMock()
    with raises(CalledProcessError):
        paasta_mark_for_deployment(args)


@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment.subprocess', autospec=True)
def test_mark_for_deployment_success(
    mock_subprocess,
):
    mock_subprocess.check_call.return_value = 0
    args = MagicMock()
    assert paasta_mark_for_deployment(args) is None
