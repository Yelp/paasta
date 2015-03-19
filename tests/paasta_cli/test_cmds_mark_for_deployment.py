from mock import patch

from paasta_tools.paasta_cli.cmds.mark_for_deployment import paasta_mark_for_deployment


class fake_args:
    clusterinstance = 'cluster.instance'
    service = 'test_service'
    git_url = 'git://false.repo/services/test_services'
    commit = 'fake-hash'


@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment._run', autospec=True)
@patch('sys.exit', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment._log', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment.validate_service_name', autospec=True)
def test_mark_for_deployment_run_fail(
    mock_validate_service_name,
    mock_log,
    mock_exit,
    mock_run,
):
    mock_run.return_value = (1, 'Exterminate!')
    paasta_mark_for_deployment(fake_args)
    mock_exit.assert_called_once_with(1)


@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment._run', autospec=True)
@patch('sys.exit', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment._log', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment.validate_service_name', autospec=True)
def test_mark_for_deployment_success(
    mock_validate_service_name,
    mock_log,
    mock_exit,
    mock_run,
):
    mock_run.return_value = (0, 'Interminate!')
    assert paasta_mark_for_deployment(fake_args) is None
