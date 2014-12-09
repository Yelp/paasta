import sys

from mock import patch

from service_deployment_tools.paasta_cli import paasta_cli


@patch('service_deployment_tools.paasta_cli.cmds.list.paasta_list')
def test_paasta_list(mock_paasta_list):
    # 'paasta list' results in check.paasta_list getting executed

    sys.argv = ['./paasta_cli', 'list']
    paasta_cli.main()
    assert mock_paasta_list.called


@patch('service_deployment_tools.paasta_cli.cmds.check.paasta_check')
def test_paasta_check(mock_paasta_check):
    # 'paasta check' results in check.paasta_check getting executed

    sys.argv = ['./paasta_cli', 'check']
    paasta_cli.main()
    assert mock_paasta_check.called


@patch('service_deployment_tools.paasta_cli.cmds.generate_pipeline.'
       'paasta_generate_pipeline')
def test_paasta_generate_pipeline(mock_paasta_generate_pipeline):
    # 'paasta check' results in check.paasta_check getting executed

    sys.argv = ['./paasta_cli', 'generate-pipeline']
    paasta_cli.main()
    assert mock_paasta_generate_pipeline.called
