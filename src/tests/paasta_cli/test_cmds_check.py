from mock import patch

from service_deployment_tools.paasta_cli.cmds.check import paasta_check


@patch('service_deployment_tools.paasta_cli.cmds.check.deploy_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.docker_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.sensu_check')
@patch('service_deployment_tools.paasta_cli.cmds.check.smartstack_check')
def test_list_paasta_check(
        mock_smartstart_check, mock_sensu_check, mock_docker_check,
        mock_deploy_check):

    # Ensure each check in 'paasta_check' is called

    mock_deploy_check.deploy_yaml_exists.return_value = None
    mock_docker_check.dockerfile_exists.return_value = None
    mock_sensu_check.monitoring_yaml_exists.return_value = None
    mock_smartstart_check.smartstack_yaml_exists.return_value = None

    args = ['./paasta_cli', 'check']
    paasta_check(args)

    assert mock_deploy_check.deploy_yaml_exists.called
    assert mock_docker_check.dockerfile_exists.called
    assert mock_sensu_check.monitoring_yaml_exists.called
    assert mock_smartstart_check.smartstack_yaml_exists.called
