import mock
from StringIO import StringIO

from paasta_tools.paasta_cli.cmds.list import paasta_list


@mock.patch('sys.stdout', new_callable=StringIO)
@mock.patch('paasta_tools.paasta_cli.cmds.list.list_services', autospec=True)
def test_list_paasta_list(mock_list_services, mock_stdout):
    """ paasta_list print each service returned by get_services """

    mock_services = ['service_1', 'service_2']

    mock_list_services.return_value = mock_services
    args = mock.MagicMock()
    args.print_instances = False
    paasta_list(args)

    output = mock_stdout.getvalue()
    assert output == 'service_1\nservice_2\n'


@mock.patch('sys.stdout', new_callable=StringIO)
@mock.patch('paasta_tools.paasta_cli.cmds.list.list_service_instances', autospec=True)
def test_list_paasta_list_instances(mock_list_service_instances, mock_stdout):
    """ paasta_list print each service.instance """

    mock_services = ['service_1.main', 'service_2.canary']

    mock_list_service_instances.return_value = mock_services
    args = mock.MagicMock()
    args.print_instances = True
    paasta_list(args)

    output = mock_stdout.getvalue()
    assert output == 'service_1.main\nservice_2.canary\n'
