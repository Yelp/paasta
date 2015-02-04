from mock import MagicMock
from mock import patch

from paasta_tools.paasta_cli.cmds.mark_for_deployment import paasta_mark_for_deployment


@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment._run', autospec=True)
@patch('sys.exit', autospec=True)
def test_mark_for_deployment_run_fail(
    mock_exit,
    mock_run,
):
    mock_run.return_value = (1, 'Exterminate!')
    args = MagicMock()
    paasta_mark_for_deployment(args)
    mock_exit.assert_called_once_with(1)


@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment._run', autospec=True)
def test_mark_for_deployment_success(
    mock_run,
):
    mock_run.return_value = (0, 'Interminate!')
    args = MagicMock()
    assert paasta_mark_for_deployment(args) is None
