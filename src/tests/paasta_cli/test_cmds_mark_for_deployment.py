import shlex
import sys
from subprocess import CalledProcessError

from mock import patch
from pytest import raises

from paasta_tools.paasta_cli.cmds.mark_for_deployment import build_command
from paasta_tools.paasta_cli.cmds.mark_for_deployment import paasta_mark_for_deployment
from paasta_tools.paasta_cli.paasta_cli import parse_args


def test_build_command():
    upstream_git_url = "fake_upstream_git_url"
    upstream_git_commit = "fake_upstream_git_commit"
    clustername = "fake_clustername"
    instancename = "fake_instancename"
    expected = "git push %s %s:refs/heads/paasta-%s.%s" % (
        upstream_git_url,
        upstream_git_commit,
        clustername,
        instancename,
    )
    expected = shlex.split(expected)
    actual = build_command(upstream_git_url, upstream_git_commit, clustername, instancename)
    assert actual == expected


@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment.subprocess', autospec=True)
def test_mark_for_deployment_subprocess_fail(
    mock_subprocess,
):
    mock_subprocess.check_output.side_effect = [
        CalledProcessError(1, 'fake_cmd'), 0]
    sys.argv = [
        './paasta_cli', 'mark-for-deployment',
        '--git-url', 'unused',
        '--commit', 'unused',
        '--clustername', 'unused',
        '--instancename', 'unused',
    ]
    parsed_args = parse_args()

    with raises(CalledProcessError):
        paasta_mark_for_deployment(parsed_args)


@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment.subprocess', autospec=True)
def test_mark_for_deployment_success(
    mock_subprocess,
):
    mock_subprocess.check_call.return_value = 0

    sys.argv = [
        './paasta_cli', 'mark-for-deployment',
        '--git-url', 'unused',
        '--commit', 'unused',
        '--clustername', 'unused',
        '--instancename', 'unused',
    ]
    parsed_args = parse_args()
    assert paasta_mark_for_deployment(parsed_args) is None


@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment.subprocess', autospec=True)
def test_mark_for_deployment_success_with_opts(
    mock_subprocess,
):
    mock_subprocess.check_call.return_value = 0

    sys.argv = [
        './paasta_cli', 'mark-for-deployment',
        '--git-url', 'unused',
        '--commit', 'unused',
        '--clustername', 'unused',
        '--instancename', 'unused',
    ]
    parsed_args = parse_args()
    assert paasta_mark_for_deployment(parsed_args) is None
