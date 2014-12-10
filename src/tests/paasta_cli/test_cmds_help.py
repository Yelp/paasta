from mock import patch
from StringIO import StringIO

from service_deployment_tools.paasta_cli.cmds.help import paasta_help


@patch('sys.stdout', new_callable=StringIO)
def test_list_paasta_list(mock_stdout):
    args = ['./paasta_cli', 'help']
    paasta_help(args)
    output = mock_stdout.getvalue()
    assert 'http://y/paasta' in output
