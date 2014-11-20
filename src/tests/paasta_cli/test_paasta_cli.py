import sys
from StringIO import StringIO

from mock import patch

from service_deployment_tools.paasta_cli import paasta_cli

@patch('sys.stdout', new_callable=StringIO)
@patch('service_deployment_tools.paasta_cli.cmds.list.get_services')
@patch('service_deployment_tools.paasta_cli.paasta_cli.paasta_commands')
def test_paasta_list(mock_paasta_commands, mock_get_services, mock_stdout):
    # 'paasta list' with no args prints list of services in list.get_services

    mock_paasta_commands.return_value = ['list']
    mock_get_services.return_value = ['service_1', 'service_2']
    sys.argv = ['./paasta_cli', 'list']
    paasta_cli.main()
    output = mock_stdout.getvalue()
    assert output == 'service_1\nservice_2\n'
