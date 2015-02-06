from mock import MagicMock
from mock import patch

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
@patch('paasta_tools.paasta_cli.cmds.itest._run', autospec=True)
@patch('sys.exit')
def test_itest_run_fail(
    mock_exit,
    mock_run,
    mock_validate_service_name,
):
    mock_run.return_value = (1, 'fake_output')
    args = MagicMock()
    paasta_itest(args)
    mock_exit.assert_called_once_with(1)


@patch('paasta_tools.paasta_cli.cmds.itest.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.itest._run', autospec=True)
def test_itest_success(
    mock_run,
    mock_validate_service_name,
):
    mock_run.return_value = (0, 'Yeeehaaa')

    args = MagicMock()
    assert paasta_itest(args) is None


@patch('paasta_tools.paasta_cli.cmds.itest.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.itest._run', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.itest.build_command', autospec=True)
def test_itest_works_when_service_name_starts_with_services_dash(
    mock_build_command,
    mock_run,
    mock_validate_service_name,
):
    mock_run.return_value = (0, 'Yeeehaaa')
    args = MagicMock()
    args.service = 'services-fake_service'
    args.commit = 'unused'
    assert paasta_itest(args) is None
    mock_build_command.assert_called_once_with('fake_service', 'unused')
