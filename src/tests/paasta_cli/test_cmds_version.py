import subprocess
import sys
from mock import patch
from StringIO import StringIO

from service_deployment_tools.paasta_cli.cmds.version import paasta_version
from service_deployment_tools.paasta_cli.paasta_cli import parse_args


@patch('sys.stdout', new_callable=StringIO)
def test_version(mock_stdout):
    # paasta_status with no args and non-service directory results in error
    expected_output = subprocess.check_output(['git', 'describe', '--tags'])

    sys.argv = [
        './paasta_cli', 'version']
    parsed_args = parse_args()
    paasta_version(parsed_args)

    output = mock_stdout.getvalue()
    assert output == expected_output
