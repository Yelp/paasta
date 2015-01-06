import sys

from mock import patch

from paasta_tools.paasta_cli import paasta_cli


@patch('paasta_tools.paasta_cli.cmds.list.paasta_list')
def test_paasta_list(mock_paasta_list):

    sys.argv = ['./paasta_cli', 'list']
    paasta_cli.main()
    assert mock_paasta_list.called


@patch('paasta_tools.paasta_cli.cmds.check.paasta_check')
def test_paasta_check(mock_paasta_check):

    sys.argv = ['./paasta_cli', 'check']
    paasta_cli.main()
    assert mock_paasta_check.called


@patch('paasta_tools.paasta_cli.cmds.generate_pipeline.'
       'paasta_generate_pipeline')
def test_paasta_generate_pipeline(mock_paasta_generate_pipeline):

    sys.argv = ['./paasta_cli', 'generate-pipeline']
    paasta_cli.main()
    assert mock_paasta_generate_pipeline.called


@patch('paasta_tools.paasta_cli.cmds.status.paasta_status')
def test_paasta_status(mock_paasta_status):

    sys.argv = ['./paasta_cli', 'status']
    paasta_cli.main()
    assert mock_paasta_status.called
