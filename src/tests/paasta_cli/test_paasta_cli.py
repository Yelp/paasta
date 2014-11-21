import sys
from StringIO import StringIO

from mock import patch, MagicMock

from service_deployment_tools.paasta_cli import paasta_cli


@patch('sys.stdout', new_callable=StringIO)
@patch('service_deployment_tools.paasta_cli.cmds.list.read_services_configuration')
@patch('service_deployment_tools.paasta_cli.paasta_cli.paasta_commands')
def test_paasta_list(mock_paasta_commands, mock_read_services, mock_stdout):
    # 'paasta list' with no args prints list of services in list.get_services

    mock_paasta_commands.return_value = ['list']

    attrs = {'keys.return_value': ['service_1', 'service_2']}
    mock_function = MagicMock()
    mock_function.configure_mock(**attrs)
    mock_read_services.return_value = mock_function

    sys.argv = ['./paasta_cli', 'list']
    paasta_cli.main()
    output = mock_stdout.getvalue()
    assert output == 'service_1\nservice_2\n'
