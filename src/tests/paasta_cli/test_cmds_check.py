import sys
from mock import patch, MagicMock
from StringIO import StringIO

from service_deployment_tools.paasta_cli.cmds.check import \
    paasta_check, deploy_check, docker_check, marathon_check, \
    sensu_check, smartstack_check, NoSuchService, service_dir_check, \
    pipeline_check, git_repo_check
from service_deployment_tools.paasta_cli.paasta_cli import parse_args
from service_deployment_tools.paasta_cli.utils import PaastaCheckMessages


@patch('service_deployment_tools.paasta_cli.cmds.check.git_repo_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.pipeline_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.service_dir_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.validate_service_name')
@patch('service_deployment_tools.paasta_cli.cmds.check.guess_service_name')
@patch('service_deployment_tools.paasta_cli.cmds.check.deploy_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.docker_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.marathon_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.sensu_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.smartstack_check')
def test_check_paasta_check(
        mock_smartstart_check, mock_sensu_check, mock_marathon_check,
        mock_docker_check, mock_deploy_check,
        mock_guess_service_name, mock_validate_service_name,
        mock_service_dir_check, mock_pipeline_check, mock_git_repo_check):
    # Ensure each check in 'paasta_check' is called

    mock_guess_service_name.return_value = 'servicedocs'
    mock_validate_service_name.return_value = None
    sys.argv = ['./paasta_cli', 'check']
    parsed_args = parse_args()

    paasta_check(parsed_args)

    assert mock_git_repo_check.called
    assert mock_pipeline_check.called
    assert mock_service_dir_check.called
    assert mock_deploy_check.called
    assert mock_docker_check.called
    assert mock_marathon_check.called
    assert mock_sensu_check.called
    assert mock_smartstart_check.called


@patch('service_deployment_tools.paasta_cli.cmds.check.validate_service_name')
@patch('sys.stdout', new_callable=StringIO)
def test_check_service_dir_check_pass(mock_stdout, mock_validate_service_name):
    mock_validate_service_name.return_value = None
    service_name = 'fake_service'
    expected_output = \
        "%s\n" % PaastaCheckMessages.service_dir_found(service_name)
    service_dir_check(service_name)
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.validate_service_name')
@patch('sys.stdout', new_callable=StringIO)
def test_check_service_dir_check_fail(mock_stdout, mock_validate_service_name):
    service_name = 'fake_service'
    mock_validate_service_name.side_effect = NoSuchService(service_name)
    expected_output = "%s\n" \
                      % PaastaCheckMessages.service_dir_missing(service_name)
    service_dir_check(service_name)
    output = mock_stdout.getvalue()

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


@patch('service_deployment_tools.paasta_cli.cmds.check.'
       'expose_8888_in_dockerfile')
@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('service_deployment_tools.paasta_cli.cmds.check.'
       'docker_file_reads_from_yelpcorp')
@patch('sys.stdout', new_callable=StringIO)
def test_check_docker_check_pass(
        mock_stdout, mock_docker_file_reads_from_yelpcorp,
        mock_is_file_in_dir, mock_expose_8888_in_dockerfile):
    # Dockerfile exists and is valid

    mock_is_file_in_dir.return_value = "/fake/path"
    mock_docker_file_reads_from_yelpcorp.return_value = True
    mock_expose_8888_in_dockerfile.return_value = True

    docker_check()
    output = mock_stdout.getvalue()

    assert PaastaCheckMessages.DOCKERFILE_FOUND in output
    assert PaastaCheckMessages.DOCKERFILE_YELPCORP in output
    assert PaastaCheckMessages.DOCKERFILE_EXPOSES_8888 in output


@patch('service_deployment_tools.paasta_cli.cmds.check.'
       'expose_8888_in_dockerfile')
@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('service_deployment_tools.paasta_cli.cmds.check.'
       'docker_file_reads_from_yelpcorp')
@patch('sys.stdout', new_callable=StringIO)
def test_check_docker_check_doesnt_expose_8888(
        mock_stdout, mock_docker_file_reads_from_yelpcorp,
        mock_is_file_in_dir, mock_expose_8888_in_dockerfile):
    # Dockerfile doesn't contain 'EXPOSE 8888'

    mock_is_file_in_dir.return_value = "/fake/path"
    mock_docker_file_reads_from_yelpcorp.return_value = True
    mock_expose_8888_in_dockerfile.return_value = False

    docker_check()
    output = mock_stdout.getvalue()

    assert PaastaCheckMessages.DOCKERFILE_FOUND in output
    assert PaastaCheckMessages.DOCKERFILE_YELPCORP in output
    assert PaastaCheckMessages.DOCKERFILE_DOESNT_EXPOSE_8888 in output


@patch('service_deployment_tools.paasta_cli.cmds.check.'
       'expose_8888_in_dockerfile')
@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('service_deployment_tools.paasta_cli.cmds.check.'
       'docker_file_reads_from_yelpcorp')
@patch('sys.stdout', new_callable=StringIO)
def test_check_docker_check_doesnt_read_yelpcorp(
        mock_stdout, mock_docker_file_reads_from_yelpcorp,
        mock_is_file_in_dir, mock_expose_8888_in_dockerfile):
    # Dockerfile doesn't read from Yelpcorp

    mock_is_file_in_dir.return_value = "/fake/path"
    mock_docker_file_reads_from_yelpcorp.return_value = False
    mock_expose_8888_in_dockerfile.return_value = False

    docker_check()
    output = mock_stdout.getvalue()

    assert PaastaCheckMessages.DOCKERFILE_FOUND in output
    assert PaastaCheckMessages.DOCKERFILE_NOT_YELPCORP in output
    assert PaastaCheckMessages.DOCKERFILE_DOESNT_EXPOSE_8888 in output


@patch('service_deployment_tools.paasta_cli.cmds.check.'
       'expose_8888_in_dockerfile')
@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('service_deployment_tools.paasta_cli.cmds.check.'
       'docker_file_reads_from_yelpcorp')
@patch('sys.stdout', new_callable=StringIO)
def test_check_docker_check_file_not_found(
        mock_stdout, mock_docker_file_reads_from_yelpcorp,
        mock_is_file_in_dir, mock_expose_8888_in_dockerfile):
    # Dockerfile doesn't exist

    mock_is_file_in_dir.return_value = False
    mock_docker_file_reads_from_yelpcorp.return_value = False
    mock_expose_8888_in_dockerfile.return_value = False

    docker_check()
    output = mock_stdout.getvalue()

    assert PaastaCheckMessages.DOCKERFILE_MISSING in output
    assert PaastaCheckMessages.DOCKERFILE_NOT_YELPCORP not in output
    assert PaastaCheckMessages.DOCKERFILE_DOESNT_EXPOSE_8888 not in output


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


@patch('service_deployment_tools.paasta_cli.cmds.check.'
       'read_extra_service_information')
@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_smartstack_check_pass(mock_stdout, mock_is_file_in_dir,
                                     mock_read_service_info):
    # smartstack.yaml exists and port is found

    mock_is_file_in_dir.return_value = True
    port = 80
    instance = 'main'
    smartstack_dict = {
        instance: {
            'proxy_port': port
        }
    }
    mock_read_service_info.return_value = smartstack_dict
    expected_output = "%s\n%s\n" \
                      % (PaastaCheckMessages.SMARTSTACK_YAML_FOUND,
                         PaastaCheckMessages.smartstack_port_found(
                             instance, port))

    smartstack_check('fake_service', 'path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.'
       'read_extra_service_information')
@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_smartstack_check_missing_port(
        mock_stdout, mock_is_file_in_dir, mock_read_service_info):
    # smartstack.yaml, instance exists, but no ports found

    mock_is_file_in_dir.return_value = True
    instance = 'main'
    smartstack_dict = {
        instance: {
            'foo': 0
        }
    }
    mock_read_service_info.return_value = smartstack_dict
    expected_output = "%s\n%s\n" \
                      % (PaastaCheckMessages.SMARTSTACK_YAML_FOUND,
                         PaastaCheckMessages.SMARTSTACK_PORT_MISSING)

    smartstack_check('fake_service', 'path')
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.'
       'read_extra_service_information')
@patch('service_deployment_tools.paasta_cli.cmds.check.is_file_in_dir')
@patch('sys.stdout', new_callable=StringIO)
def test_check_smartstack_check_missing_instance(
        mock_stdout, mock_is_file_in_dir, mock_read_service_info):
    # smartstack.yaml exists, but no instances found

    mock_is_file_in_dir.return_value = True
    smartstack_dict = {}
    mock_read_service_info.return_value = smartstack_dict
    expected_output = "%s\n%s\n" \
                      % (PaastaCheckMessages.SMARTSTACK_YAML_FOUND,
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


@patch('service_deployment_tools.paasta_cli.cmds.check.urllib2.urlopen')
@patch('sys.stdout', new_callable=StringIO)
def test_check_pipeline_check_pass(mock_stdout, mock_urlopen):
    attrs = {'getcode.return_value': 200}
    mock_function = MagicMock()
    mock_function.configure_mock(**attrs)
    mock_urlopen.return_value = mock_function
    expected_output = "%s\n" % PaastaCheckMessages.PIPELINE_FOUND
    pipeline_check("fake_service")
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.urllib2')
@patch('sys.stdout', new_callable=StringIO)
def test_check_pipeline_check_fail_404(mock_stdout, mock_urllib2):
    attrs = {'getcode.return_value': 404}
    mock_function = MagicMock()
    mock_function.configure_mock(**attrs)
    mock_urllib2.urlopen.return_value = mock_function
    expected_output = "%s\n" % PaastaCheckMessages.PIPELINE_MISSING
    pipeline_check("fake_service")
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.urllib2.HTTPERROR')
@patch('service_deployment_tools.paasta_cli.cmds.check.urllib2')
@patch('sys.stdout', new_callable=StringIO)
def test_check_pipeline_check_fail_httperr(mock_stdout, mock_urllib2, mock_error):

    mock_urllib2.urlopen.side_effect = mock_error
    expected_output = "%s\n" % PaastaCheckMessages.PIPELINE_MISSING
    pipeline_check("fake_service")
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.subprocess')
@patch('sys.stdout', new_callable=StringIO)
def test_check_git_repo_check_pass(mock_stdout, mock_subprocess):
    mock_subprocess.call.return_value = 0
    git_repo_check('fake_service')
    expected_output = "%s\n" % PaastaCheckMessages.GIT_REPO_FOUND
    output = mock_stdout.getvalue()

    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.check.subprocess')
@patch('sys.stdout', new_callable=StringIO)
def test_check_git_repo_check_fail(mock_stdout, mock_subprocess):
    mock_subprocess.call.return_value = 2
    service = 'fake_service'
    git_repo_check(service)
    expected_output = "%s\n" % PaastaCheckMessages.git_repo_missing(service)
    output = mock_stdout.getvalue()

    assert output == expected_output
