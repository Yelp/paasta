from mock import patch
from StringIO import StringIO

from service_deployment_tools.paasta_cli.cmds.list import paasta_list

@patch('service_deployment_tools.paasta_cli.cmds.list.get_services')
@patch('sys.stdout', new_callable=StringIO)
def test_list_paasta_list(mock_stdout, mock_get_services):
    # paasta_list print each service returned by get_services

    mock_get_services.return_value = ['service_1, service_2']
    args = ['./paasta_cli', 'list']
    paasta_list(args)
    output = mock_stdout.getvalue()
    assert output == 'service_1, service_2\n'
