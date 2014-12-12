import sys
from mock import patch
from pytest import raises
from StringIO import StringIO

from service_deployment_tools.paasta_cli.utils import \
    NoSuchService, PaastaColors, PaastaCheckMessages
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
@patch('sys.stdout', new_callable=StringIO)
def test_status_missing_deployments_err(mock_stdout, mock_get_deployments_json):
    # paasta_status exits on error if deployments.json missing
    mock_get_deployments_json.return_value = {}
    sys.argv = [
        './paasta_cli', 'status', '-s', 'fake_service']
    parsed_args = parse_args()

    expected_output = 'Failed to locate deployments.json ' \
                      'in default SOA directory\n'

    # Fail if exit(1) does not get called
    with raises(SystemExit) as sys_exit:
        paasta_status(parsed_args)

    output = mock_stdout.getvalue()
    assert sys_exit.value.code == 1
    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.status.read_deploy')
@patch('sys.stdout', new_callable=StringIO)
def test_status_missing_deploy_err(mock_stdout, mock_read_deploy):
    # paasta_status exits on error if deploy.yaml missing
    mock_read_deploy.return_value = False
    sys.argv = [
        './paasta_cli', 'status', '-s', 'fake_service']
    parsed_args = parse_args()

    expected_output = '%s\n' % PaastaCheckMessages.DEPLOY_YAML_MISSING

    # Fail if exit(1) does not get called
    with raises(SystemExit) as sys_exit:
        paasta_status(parsed_args)

    output = mock_stdout.getvalue()
    assert sys_exit.value.code == 1
    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.status.get_deploy_yaml')
@patch('service_deployment_tools.paasta_cli.cmds.status._get_deployments_json')
@patch('sys.stdout', new_callable=StringIO)
def test_status_displays_deployed_service(
        mock_stdout, mock_get_deployments, mock_get_deploy_yaml):
    # paasta_status with no args displays deploy info - vanilla case

    pipeline = [{'instance_name': 'cluster.instance'}]
    mock_get_deploy_yaml.return_value = {'pipeline': pipeline}

    deployments_json_dict = {
        'fake_service:paasta-cluster.instance': 'this_is_a_sha'
    }
    mock_get_deployments.return_value = deployments_json_dict
    expected_output = "cluster: cluster\n" \
                      "\tinstance: %s\n" \
                      "\t\tversion: this_is_a_sha\n\n" \
                      % PaastaColors.green('instance')

    sys.argv = [
        './paasta_cli', 'status', '-s', 'fake_service']
    parsed_args = parse_args()
    paasta_status(parsed_args)

    output = mock_stdout.getvalue()
    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.status.get_deploy_yaml')
@patch('service_deployment_tools.paasta_cli.cmds.status._get_deployments_json')
@patch('sys.stdout', new_callable=StringIO)
def test_status_sorts_in_deploy_order(
        mock_stdout, mock_get_deployments, mock_get_deploy_yaml):
    # paasta_status with no args displays deploy info

    pipeline = [{'instance_name': 'a_cluster.a_instance'},
                {'instance_name': 'a_cluster.b_instance'},
                {'instance_name': 'b_cluster.b_instance'}]
    mock_get_deploy_yaml.return_value = {'pipeline': pipeline}

    deployments_json_dict = {
        'fake_service:paasta-b_cluster.b_instance': 'this_is_a_sha',
        'fake_service:paasta-a_cluster.a_instance': 'this_is_a_sha',
        'fake_service:paasta-a_cluster.b_instance': 'this_is_a_sha'
    }
    mock_get_deployments.return_value = deployments_json_dict
    expected_output = "cluster: a_cluster\n" \
                      "\tinstance: %s\n" \
                      "\t\tversion: this_is_a_sha\n\n" \
                      "\tinstance: %s\n" \
                      "\t\tversion: this_is_a_sha\n\n" \
                      "cluster: b_cluster\n" \
                      "\tinstance: %s\n" \
                      "\t\tversion: this_is_a_sha\n\n" \
                      % (PaastaColors.green('a_instance'),
                         PaastaColors.green('b_instance'),
                         PaastaColors.green('b_instance'))

    sys.argv = [
        './paasta_cli', 'status', '-s', 'fake_service']
    parsed_args = parse_args()
    paasta_status(parsed_args)

    output = mock_stdout.getvalue()
    assert output == expected_output


@patch('service_deployment_tools.paasta_cli.cmds.status.get_deploy_yaml')
@patch('service_deployment_tools.paasta_cli.cmds.status._get_deployments_json')
@patch('sys.stdout', new_callable=StringIO)
def test_status_missing_deploys_in_red(
        mock_stdout, mock_get_deployments, mock_get_deploy_yaml):
    # paasta_status displays missing deploys in red

    pipeline = [{'instance_name': 'a_cluster.a_instance'},
                {'instance_name': 'a_cluster.b_instance'},
                {'instance_name': 'b_cluster.b_instance'}]
    mock_get_deploy_yaml.return_value = {'pipeline': pipeline}

    deployments_json_dict = {
        'fake_service:paasta-b_cluster.b_instance': 'this_is_a_sha',
        'fake_service:paasta-a_cluster.a_instance': 'this_is_a_sha'
    }
    mock_get_deployments.return_value = deployments_json_dict
    expected_output = "cluster: a_cluster\n" \
                      "\tinstance: %s\n" \
                      "\t\tversion: this_is_a_sha\n\n" \
                      "\tinstance: %s\n" \
                      "\t\tversion: None\n\n" \
                      "cluster: b_cluster\n" \
                      "\tinstance: %s\n" \
                      "\t\tversion: this_is_a_sha\n\n" \
                      % (PaastaColors.green('a_instance'),
                         PaastaColors.red('b_instance'),
                         PaastaColors.green('b_instance'))

    sys.argv = [
        './paasta_cli', 'status', '-s', 'fake_service']
    parsed_args = parse_args()
    paasta_status(parsed_args)

    output = mock_stdout.getvalue()
    assert output == expected_output
