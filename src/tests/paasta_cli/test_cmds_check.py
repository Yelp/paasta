from mock import patch
from pytest import raises
from StringIO import StringIO

from service_deployment_tools.paasta_cli.cmds.check import paasta_check
from service_deployment_tools.paasta_cli.cmds.check import deploy_check
from service_deployment_tools.paasta_cli.utils import check_mark, x_mark


@patch('service_deployment_tools.paasta_cli.cmds.check.guess_service_name')
@patch('service_deployment_tools.paasta_cli.cmds.check.deploy_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.docker_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.marathon_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.sensu_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.smartstack_check')
def test_check_paasta_check(
        mock_smartstart_check, mock_sensu_check, mock_marathon_check,
        mock_docker_check, mock_deploy_check, mock_guess_service_name):
    # All checks run when service name found

    mock_guess_service_name.return_value = 'servicedocs'

    # Ensure each check in 'paasta_check' is called
    args = ['./paasta_cli', 'check']
    paasta_check(args)

    assert mock_deploy_check.called
    assert mock_docker_check.called
    assert mock_marathon_check.called
    assert mock_sensu_check.called
    assert mock_smartstart_check.called


@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_deploy_check_pass(mock_stdout, mock_is_file_in_dir):
    # Deploy check passes when file found in service path

    mock_is_file_in_dir.return_value = True

    deploy_check('service_path')
    expected_output = "%s Found deploy.yaml\n" % check_mark()
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_deploy_check_fail(mock_stdout, mock_is_file_in_dir):
    # Deploy check fails when file not in service path

    mock_is_file_in_dir.return_value = False

    deploy_check('service_path')
    expected_output = "%s Missing deploy.yaml\n" % x_mark()
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.guess_service_name')
@patch('sys.stdout', new_callable=StringIO)
def test_check_service_name_not_found(mock_stdout, mock_guess_service_name):
    # Paasta checks do not run when service name cannot be guessed, exit(1)

    mock_guess_service_name.return_value = False
    args = ['./paasta_cli', 'check']
    expected_output = 'Could not figure out the service name.\n' \
                      'Please run this from the root of a copy ' \
                      '(git clone) of your service.\n'

    with raises(SystemExit) as sys_exit:
        paasta_check(args)

    output = mock_stdout.getvalue()
    assert sys_exit.value.code == 1
    assert output == expected_output
