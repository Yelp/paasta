from mock import patch
from StringIO import StringIO

from service_deployment_tools.paasta_cli.cmds.check import paasta_check
from service_deployment_tools.paasta_cli.cmds.check import deploy_check
from service_deployment_tools.paasta_cli.utils import check_mark, x_mark


@patch('service_deployment_tools.paasta_cli.cmds.check.deploy_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.docker_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.marathon_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.sensu_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.smartstack_check')
def test_check_paasta_check(
        mock_smartstart_check, mock_sensu_check, mock_marathon_check,
        mock_docker_check, mock_deploy_check):

    # Ensure each check in 'paasta_check' is called

    args = ['./paasta_cli', 'check']
    paasta_check(args)

    assert mock_deploy_check.called
    assert mock_docker_check.called
    assert mock_marathon_check.called
    assert mock_sensu_check.called
    assert mock_smartstart_check.called


@patch('service_deployment_tools.paasta_cli.cmds.check.guess_service_name')
@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_deploy_check_pass(mock_stdout, mock_is_file_in_dir,
                                 mock_guess_service_name):
    mock_is_file_in_dir.return_value = 'servicedocs'
    mock_is_file_in_dir.return_value = True
    deploy_check()
    expected_output = "%s Found deploy.yaml\n" % check_mark()
    output = mock_stdout.getvalue()
    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.guess_service_name')
@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_deploy_check_fail(mock_stdout, mock_is_file_in_dir,
                                 mock_guess_service_name):
    mock_is_file_in_dir.return_value = 'servicedocs'
    mock_is_file_in_dir.return_value = False
    deploy_check()
    expected_output = "%s Missing deploy.yaml\n" % x_mark()
    output = mock_stdout.getvalue()
    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.guess_service_name')
@patch('sys.stdout', new_callable=StringIO)
def test_check_service_name_not_found(mock_stdout, mock_guess_service_name):
    mock_guess_service_name.return_value = False
    deploy_check()

    expected_output = "%s deploy check failed. " \
                      "Could not decipher service name. " \
                      "Ensure you are in the service repo root.\n" % x_mark()

    output = mock_stdout.getvalue()
    assert output == expected_output
