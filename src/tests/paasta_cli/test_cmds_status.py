import sys
from mock import patch
from mock import Mock
from pytest import raises
from StringIO import StringIO

from paasta_tools.paasta_cli.utils import \
    NoSuchService, PaastaColors, PaastaCheckMessages
from paasta_tools.paasta_cli.cmds.status import paasta_status, \
    missing_deployments_message
from paasta_tools.paasta_cli.cmds import status
#from paasta_tools.paasta_cli.cmds.status import remote_execute
from paasta_tools.paasta_cli.paasta_cli import parse_args


@patch('paasta_tools.paasta_cli.cmds.status.validate_service_name')
@patch('sys.stdout', new_callable=StringIO)
def test_figure_out_service_name_not_found(mock_stdout,
                                           mock_validate_service_name):
    # paasta_status with invalid -s service_name arg results in error
    mock_validate_service_name.side_effect = NoSuchService(None)
    parsed_args = Mock() 
    parsed_args.service = 'fake_service'

    expected_output = '%s\n' % NoSuchService.GUESS_ERROR_MSG

    # Fail if exit(1) does not get called
    with raises(SystemExit) as sys_exit:
        status.figure_out_service_name(parsed_args)

    output = mock_stdout.getvalue()
    assert sys_exit.value.code == 1
    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.status.validate_service_name')
@patch('paasta_tools.paasta_cli.cmds.status.guess_service_name')
@patch('sys.stdout', new_callable=StringIO)
def test_status_arg_service_not_found(mock_stdout, mock_guess_service_name,
                                      mock_validate_service_name):
    # paasta_status with no args and non-service directory results in error
    mock_guess_service_name.return_value = 'not_a_service'
    error = NoSuchService('fake_service')
    mock_validate_service_name.side_effect = error
    sys.argv = [
        './paasta_cli', 'status']
    parsed_args = parse_args()

    expected_output = str(error) + "\n"

    # Fail if exit(1) does not get called
    with raises(SystemExit) as sys_exit:
        paasta_status(parsed_args)

    output = mock_stdout.getvalue()
    assert sys_exit.value.code == 1
    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.status._get_deployments_json')
@patch('sys.stdout', new_callable=StringIO)
def test_status_missing_deployments_err(mock_stdout, mock_get_deployments_json):
    # paasta_status exits on error if deployments.json missing
    mock_get_deployments_json.return_value = {}

    expected_output = 'Failed to locate deployments.json ' \
                      'in default SOA directory\n'

    # Fail if exit(1) does not get called
    with raises(SystemExit) as sys_exit:
        status.get_actual_deployments('fake_service')

    output = mock_stdout.getvalue()
    assert sys_exit.value.code == 1
    assert output == expected_output


@patch('sys.stdout', new_callable=StringIO)
def test_report_status_displays_deployed_service(mock_stdout):
    # paasta_status with no args displays deploy info - vanilla case
    service_name = 'fake_service'
    planned_deployments = ['cluster.instance']
    actual_deployments = {
        'cluster.instance': 'this_is_a_sha'
    }
    expected_output = "cluster: cluster\n" \
                      "\tinstance: %s\n" \
                      "\t\tversion: this_is_a_sha\n\n" \
                      % PaastaColors.green('instance')

    status.report_status(service_name, planned_deployments, actual_deployments)
    output = mock_stdout.getvalue()
    assert expected_output in output


@patch('sys.stdout', new_callable=StringIO)
def test_report_status_sorts_in_deploy_order(mock_stdout):
    # paasta_status with no args displays deploy info
    service_name = 'fake_service'
    planned_deployments = [
       'a_cluster.a_instance',
       'a_cluster.b_instance',
       'b_cluster.b_instance',
    ]
    actual_deployments = {
        'a_cluster.a_instance': 'this_is_a_sha',
        'a_cluster.b_instance': 'this_is_a_sha',
        'b_cluster.b_instance': 'this_is_a_sha',
    }
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

    status.report_status(service_name, planned_deployments, actual_deployments)
    output = mock_stdout.getvalue()
    assert expected_output in output


@patch('sys.stdout', new_callable=StringIO)
def test_report_status_missing_deploys_in_red(mock_stdout):
    # paasta_status displays missing deploys in red
    service_name = 'fake_service'
    planned_deployments = [
        'a_cluster.a_instance',
        'a_cluster.b_instance',
        'b_cluster.b_instance',
    ]
    actual_deployments = {
        'a_cluster.a_instance': 'this_is_a_sha',
        'b_cluster.b_instance': 'this_is_a_sha',
    }
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

    status.report_status(service_name, planned_deployments, actual_deployments)
    output = mock_stdout.getvalue()
    assert expected_output in output


@patch('paasta_tools.paasta_cli.cmds.status.figure_out_service_name')
@patch('paasta_tools.paasta_cli.cmds.status.get_deploy_info')
@patch('paasta_tools.paasta_cli.cmds.status.get_actual_deployments')
@patch('sys.stdout', new_callable=StringIO)
def test_status_pending_pipeline_build_message(
        mock_stdout, mock_get_actual_deployments, mock_get_deploy_info,
        mock_figure_out_service_name):
    # If deployments.json is missing SERVICE, output the appropriate message
    service_name = 'fake_service'
    mock_figure_out_service_name.return_value = service_name
    pipeline = [{'instancename': 'cluster.instance'}]
    mock_get_deploy_info.return_value = {'pipeline': pipeline}

    actual_deployments = {}
    mock_get_actual_deployments.return_value = actual_deployments
    expected_output = missing_deployments_message(service_name)

    sys.argv = [
        './paasta_cli', 'status', '-s', service_name]
    parsed_args = parse_args()
    paasta_status(parsed_args)

    output = mock_stdout.getvalue()
    assert expected_output in output


@patch('paasta_tools.paasta_cli.cmds.status._get_deployments_json')
def test_get_actual_deployments(mock_get_deployments,):
    mock_get_deployments.return_value = {
        'fake_service:paasta-b_cluster.b_instance': 'this_is_a_sha',
        'fake_service:paasta-a_cluster.a_instance': 'this_is_a_sha'
    }
    expected = {
        'a_cluster.a_instance': 'this_is_a_sha',
        'b_cluster.b_instance': 'this_is_a_sha',
    }

    actual = status.get_actual_deployments('fake_service')
    assert expected == actual


@patch('paasta_tools.paasta_cli.cmds.status.join', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.read_deploy', autospec=True)
def test_get_deploy_info_exists(mock_read_deploy, mock_join):
    expected = 'fake deploy yaml'
    mock_read_deploy.return_value = expected
    actual = status.get_deploy_info('fake_service')
    assert expected == actual


@patch('paasta_tools.paasta_cli.cmds.status.join', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.read_deploy', autospec=True)
@patch('sys.stdout', new_callable=StringIO)
def test_get_deploy_info_does_not_exist(mock_stdout, mock_read_deploy, mock_join):
    mock_read_deploy.return_value = False
    expected_output = '%s\n' % PaastaCheckMessages.DEPLOY_YAML_MISSING
    with raises(SystemExit) as sys_exit:
        status.get_deploy_info('fake_service')
    output = mock_stdout.getvalue()
    assert sys_exit.value.code == 1
    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.status.figure_out_service_name')
@patch('paasta_tools.paasta_cli.cmds.status.get_deploy_info')
@patch('paasta_tools.paasta_cli.cmds.status.get_actual_deployments')
@patch('paasta_tools.paasta_cli.cmds.status.get_planned_deployments')
@patch('paasta_tools.paasta_cli.cmds.status.report_status')
@patch('sys.stdout', new_callable=StringIO)
def test_status_calls_sergeants(
        mock_stdout,
        mock_report_status,
        mock_get_planned_deployments,
        mock_get_actual_deployments,
        mock_get_deploy_info,
        mock_figure_out_service_name
    ):
    service_name = 'fake_service'
    mock_figure_out_service_name.return_value = service_name

    pipeline = [{'instancename': 'cluster.instance'}]
    deploy_info = {'pipeline': pipeline}
    planned_deployments = ['cluster1.instance1', 'cluster1.instance2', 'cluster2.instance1']
    mock_get_deploy_info.return_value = deploy_info
    mock_get_planned_deployments.return_value = planned_deployments

    actual_deployments = {
        'fake_service:paasta-cluster.instance': 'this_is_a_sha'
    }
    mock_get_actual_deployments.return_value = actual_deployments

    sys.argv = [
        './paasta_cli', 'status', '-s', service_name]
    parsed_args = parse_args()
    paasta_status(parsed_args)

    mock_figure_out_service_name.assert_called_once_with(parsed_args)
    mock_get_actual_deployments.assert_called_once_with(service_name)
    mock_get_deploy_info.assert_called_once_with(service_name)
    mock_report_status.assert_called_once_with(service_name, planned_deployments, actual_deployments)


#@patch('paasta_tools.paasta_cli.cmds.status.validate_service_name')
#@patch('paasta_tools.paasta_cli.cmds.status.guess_service_name')
#@patch('paasta_tools.paasta_cli.cmds.status.get_deploy_info')
#@patch('paasta_tools.paasta_cli.cmds.status._get_deployments_json')
#@patch('sys.stdout', new_callable=StringIO)
#@patch('paasta_tools.paasta_cli.cmds.status.get_remote_status')
#def test_status_pending_pipeline_build_message(
#        mock_get_remote_status,
#        mock_stdout, mock_get_deployments, mock_get_deploy_info,
#        mock_guess_service_name, mock_validate_service_name):
#    # If deployments.json is missing SERVICE, output the appropriate message
#    service_name = 'fake_service'
#    mock_guess_service_name.return_value = service_name
#    mock_validate_service_name.return_value = None
#    pipeline = [{'instancename': 'cluster.instance'}]
#    mock_get_deploy_info.return_value = {'pipeline': pipeline}
#
#    deployments_json_dict = {
#        'a_different_service:paasta-cluster.instance': 'another_sha'
#    }
#    mock_get_deployments.return_value = deployments_json_dict
#
#    sys.argv = [
#        './paasta_cli', 'status', '-s', service_name]
#    parsed_args = parse_args()
#    paasta_status(parsed_args)
#
#    expected_output = 'mock remote status output'
#    mock_get_remote_status.return_value = expected_output
#    output = mock_stdout.getvalue()
#    assert expected_output in output
#

#def test_status_remote_execute():
#
#    expected_output = 'mock_output from ssh command'
#
#    output = remote_execute()
#    assert expected_output in output
