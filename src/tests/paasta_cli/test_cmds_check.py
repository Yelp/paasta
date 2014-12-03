from mock import patch
from pytest import raises
from StringIO import StringIO

from service_deployment_tools.paasta_cli.cmds.check import \
    paasta_check, deploy_check, docker_check, marathon_check, \
    sensu_check, smartstack_check
from service_deployment_tools.paasta_cli.utils import PaastaCheckMessages


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


@patch('service_deployment_tools.paasta_cli.cmds.check.guess_service_name')
@patch('sys.stdout', new_callable=StringIO)
def test_check_service_name_not_found(mock_stdout, mock_guess_service_name):
    # Paasta checks do not run when service name cannot be guessed, exit(1)

    mock_guess_service_name.return_value = False
    args = ['./paasta_cli', 'check']
    expected_output = '%s\n' % PaastaCheckMessages.SERVICE_NAME_NOT_FOUND

    with raises(SystemExit) as sys_exit:
        paasta_check(args)

    output = mock_stdout.getvalue()
    assert sys_exit.value == 1
    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_deploy_check_pass(mock_stdout, mock_is_file_in_dir):
    # Deploy check passes when file found in service path

    mock_is_file_in_dir.return_value = True

    deploy_check('service_path')
    expected_output = "%s\n" % PaastaCheckMessages.DEPLOY_YAML_FOUND
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_deploy_check_fail(mock_stdout, mock_is_file_in_dir):
    # Deploy check fails when file not in service path

    mock_is_file_in_dir.return_value = False

    deploy_check('service_path')
    expected_output = "%s\n" % PaastaCheckMessages.DEPLOY_YAML_MISSING
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('service_deployment_tools.paasta_cli.cmds.check.docker_file_valid')
@patch('sys.stdout', new_callable=StringIO)
def test_check_docker_check_pass(mock_stdout, mock_docker_file_valid,
                                 mock_is_file_in_dir):
    # Dockerfile exists and is valid

    mock_is_file_in_dir.return_value = "/fake/path"
    mock_docker_file_valid.return_value = True
    expected_output = "%s\n%s\n" % (PaastaCheckMessages.DOCKERFILE_FOUND,
                                    PaastaCheckMessages.DOCKERFILE_VALID)

    docker_check()
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('service_deployment_tools.paasta_cli.cmds.check.docker_file_valid')
@patch('sys.stdout', new_callable=StringIO)
def test_check_docker_check_invalid(mock_stdout, mock_docker_file_valid,
                                    mock_is_file_in_dir):
    # Dockerfile exists but is not valid

    mock_is_file_in_dir.return_value = "/fake/path"
    mock_docker_file_valid.return_value = False
    expected_output = "%s\n%s\n" % (PaastaCheckMessages.DOCKERFILE_FOUND,
                                    PaastaCheckMessages.DOCKERFILE_INVALID)

    docker_check()
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_docker_check_file_not_found(mock_stdout, mock_is_file_in_dir):
    # Dockerfile doesn't exist

    mock_is_file_in_dir.return_value = False
    expected_output = "%s\n" % PaastaCheckMessages.DOCKERFILE_MISSING

    docker_check()
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_marathon_check_pass(mock_stdout, mock_is_file_in_dir):
    # marathon.yaml exists and is valid

    mock_is_file_in_dir.return_value = "/fake/path"
    expected_output = "%s\n" % PaastaCheckMessages.MARATHON_YAML_FOUND

    marathon_check('path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_marathon_check_fail(mock_stdout, mock_is_file_in_dir):
    # marathon.yaml exists and is valid

    mock_is_file_in_dir.return_value = False
    expected_output = "%s\n" % PaastaCheckMessages.MARATHON_YAML_MISSING

    marathon_check('path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('service_deployment_tools.paasta_cli.cmds.check.get_team')
@patch('sys.stdout', new_callable=StringIO)
def test_check_sensu_check_pass(mock_stdout, mock_get_team,
                                mock_is_file_in_dir):
    # monitoring.yaml exists and team is found

    mock_is_file_in_dir.return_value = "/fake/path"
    team = 'team-service-infra'
    mock_get_team.return_value = team
    expected_output = "%s\n%s\n" % (PaastaCheckMessages.SENSU_MONITORING_FOUND,
                                    PaastaCheckMessages.sensu_team_found(team))

    sensu_check('fake_service', 'path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('service_deployment_tools.paasta_cli.cmds.check.get_team')
@patch('sys.stdout', new_callable=StringIO)
def test_check_sensu_team_missing(mock_stdout, mock_get_team,
                                  mock_is_file_in_dir):
    # monitoring.yaml exists but team is not found

    mock_is_file_in_dir.return_value = "/fake/path"
    mock_get_team.return_value = None
    expected_output = "%s\n%s\n" % (PaastaCheckMessages.SENSU_MONITORING_FOUND,
                                    PaastaCheckMessages.SENSU_TEAM_MISSING)

    sensu_check('fake_service', 'path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_sensu_check_fail(mock_stdout, mock_is_file_in_dir):
    # monitoring.yaml doest exist

    mock_is_file_in_dir.return_value = False
    expected_output = "%s\n" % PaastaCheckMessages.SENSU_MONITORING_MISSING

    sensu_check('fake_service', 'path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('service_deployment_tools.paasta_cli.cmds.check.'
       'get_proxy_port_for_instance')
@patch('sys.stdout', new_callable=StringIO)
def test_check_smartstack_check_pass(mock_stdout, mock_get_port,
                                     mock_is_file_in_dir):
    # smartstack.yaml exists and port is found

    mock_is_file_in_dir.return_value = "/fake/path"
    port = 80
    mock_get_port.return_value = port
    expected_output = "%s\n%s\n" % (PaastaCheckMessages.SMARTSTACK_YAML_FOUND,
                                    PaastaCheckMessages.smartstack_port_found(
                                        port))

    smartstack_check('fake_service', 'path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('service_deployment_tools.paasta_cli.cmds.check.'
       'get_proxy_port_for_instance')
@patch('sys.stdout', new_callable=StringIO)
def test_check_smartstack_no_port(mock_stdout, mock_get_port,
                                  mock_is_file_in_dir):
    # smartstack.yaml exists not port is not found

    mock_is_file_in_dir.return_value = "/fake/path"

    mock_get_port.return_value = None
    expected_output = "%s\n%s\n" % (PaastaCheckMessages.SMARTSTACK_YAML_FOUND,
                                    PaastaCheckMessages.SMARTSTACK_PORT_MISSING)

    smartstack_check('fake_service', 'path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_smartstack_check_fail(mock_stdout, mock_is_file_in_dir):
    # smartstack.yaml doest exist

    mock_is_file_in_dir.return_value = False
    expected_output = "%s\n" % PaastaCheckMessages.SMARTSTACK_YAML_MISSING

    smartstack_check('fake_service', 'path')
    output = mock_stdout.getvalue()

    assert output == expected_output
