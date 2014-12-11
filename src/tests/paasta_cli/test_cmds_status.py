import sys
from mock import patch
from pytest import raises
from StringIO import StringIO

from service_deployment_tools.paasta_cli.utils import \
    NoSuchService, PaastaColors
from service_deployment_tools.paasta_cli.cmds.status import paasta_status
from service_deployment_tools.paasta_cli.paasta_cli import parse_args


@patch('service_deployment_tools.paasta_cli.cmds.status.guess_service_name')
@patch('sys.stdout', new_callable=StringIO)
def test_status_service_not_found_error(mock_stdout, mock_guess_service_name):
    # paasta_status with no args and non-service directory results in error
    mock_guess_service_name.side_effect = NoSuchService('foo')
    sys.argv = [
        './paasta_cli', 'status']
    parsed_args = parse_args()

    expected_output = '%s\n' % NoSuchService.ERROR_MSG

    # Fail if exit(1) does not get called
    with raises(SystemExit) as sys_exit:
        paasta_status(parsed_args)

    output = mock_stdout.getvalue()
    assert sys_exit.value.code == 1
    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.status._get_deployments_json')
@patch('service_deployment_tools.paasta_cli.cmds.status.guess_service_name')
@patch('sys.stdout', new_callable=StringIO)
def test_status_displays_deployed_service(
        mock_stdout, mock_guess_service_name, mock_get_deployments):
    # paasta_status with no args displays deploy info

    service_name = 'fake_service'
    mock_guess_service_name.return_value = service_name
    deployments_json_dict = {
        'fake_service:cluster.instance': 'this_is_a_sha'
    }
    mock_get_deployments.return_value = deployments_json_dict
    expected_output = "\nRunning instance(s) of %s:\n\n" \
                      "cluster: %s\n" \
                      "\tinstance: instance\n" \
                      "\t\tversion: this_is_a_sha\n\n" \
                      % (PaastaColors.cyan(service_name),
                         PaastaColors.green('cluster'))

    sys.argv = [
        './paasta_cli', 'status', '-s', 'fake_service']
    parsed_args = parse_args()
    paasta_status(parsed_args)

    output = mock_stdout.getvalue()
    assert output == expected_output
