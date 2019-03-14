# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os

from mock import call
from mock import MagicMock
from mock import patch

from paasta_tools.cli.cmds.check import deploy_check
from paasta_tools.cli.cmds.check import deploy_has_performance_check
from paasta_tools.cli.cmds.check import deploy_has_security_check
from paasta_tools.cli.cmds.check import deployments_check
from paasta_tools.cli.cmds.check import docker_check
from paasta_tools.cli.cmds.check import get_deploy_groups_used_by_framework
from paasta_tools.cli.cmds.check import makefile_check
from paasta_tools.cli.cmds.check import makefile_has_a_tab
from paasta_tools.cli.cmds.check import makefile_has_docker_tag
from paasta_tools.cli.cmds.check import makefile_responds_to
from paasta_tools.cli.cmds.check import NoSuchService
from paasta_tools.cli.cmds.check import paasta_check
from paasta_tools.cli.cmds.check import sensu_check
from paasta_tools.cli.cmds.check import service_dir_check
from paasta_tools.cli.cmds.check import smartstack_check
from paasta_tools.cli.cmds.check import yaml_check
from paasta_tools.cli.utils import PaastaCheckMessages
from paasta_tools.marathon_tools import MarathonServiceConfig


@patch('paasta_tools.cli.cmds.check.git_repo_check', autospec=True)
@patch('paasta_tools.cli.cmds.check.service_dir_check', autospec=True)
@patch('paasta_tools.cli.cmds.check.validate_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.check.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.check.deploy_has_performance_check', autospec=True)
@patch('paasta_tools.cli.cmds.check.deploy_has_security_check', autospec=True)
@patch('paasta_tools.cli.cmds.check.deploy_check', autospec=True)
@patch('paasta_tools.cli.cmds.check.docker_check', autospec=True)
@patch('paasta_tools.cli.cmds.check.makefile_check', autospec=True)
@patch('paasta_tools.cli.cmds.check.yaml_check', autospec=True)
@patch('paasta_tools.cli.cmds.check.deployments_check', autospec=True)
@patch('paasta_tools.cli.cmds.check.sensu_check', autospec=True)
@patch('paasta_tools.cli.cmds.check.smartstack_check', autospec=True)
@patch('paasta_tools.cli.cmds.check.paasta_validate_soa_configs', autospec=True)
def test_check_paasta_check_calls_everything(
        mock_paasta_validate_soa_configs,
        mock_smartstart_check,
        mock_sensu_check,
        mock_deployments_check,
        mock_yaml_check,
        mock_makefile_check,
        mock_docker_check,
        mock_deploy_check,
        mock_deploy_security_check,
        mock_deploy_performance_check,
        mock_figure_out_service_name,
        mock_validate_service_name,
        mock_service_dir_check,
        mock_git_repo_check,
):
    # Ensure each check in 'paasta_check' is called

    mock_figure_out_service_name.return_value = 'servicedocs'
    mock_validate_service_name.return_value = None
    args = MagicMock()
    args.yelpsoa_config_root = '/fake/path'
    paasta_check(args)

    assert mock_git_repo_check.called
    assert mock_service_dir_check.called
    assert mock_deploy_check.called
    assert mock_deploy_security_check.called
    assert mock_deploy_performance_check.called
    assert mock_docker_check.called
    assert mock_makefile_check.called
    assert mock_yaml_check.called
    assert mock_sensu_check.called
    assert mock_smartstart_check.called
    assert mock_paasta_validate_soa_configs.called

    service_path = os.path.join(
        args.yelpsoa_config_root,
        mock_figure_out_service_name.return_value,
    )
    mock_deploy_check.assert_called_once_with(service_path)


@patch('paasta_tools.cli.cmds.check.validate_service_name', autospec=True)
def test_check_service_dir_check_pass(mock_validate_service_name, capfd):
    mock_validate_service_name.return_value = None
    service = 'fake_service'
    soa_dir = '/fake_yelpsoa_configs'
    expected_output = \
        "%s\n" % PaastaCheckMessages.service_dir_found(service, soa_dir)
    service_dir_check(service, soa_dir)

    output, _ = capfd.readouterr()
    assert output == expected_output


@patch('paasta_tools.cli.cmds.check.validate_service_name', autospec=True)
def test_check_service_dir_check_fail(mock_validate_service_name, capfd):
    service = 'fake_service'
    soa_dir = '/fake_yelpsoa_configs'
    mock_validate_service_name.side_effect = NoSuchService(service)
    expected_output = "%s\n" \
                      % PaastaCheckMessages.service_dir_missing(service, soa_dir)
    service_dir_check(service, soa_dir)

    output, _ = capfd.readouterr()
    assert output == expected_output


@patch('paasta_tools.cli.cmds.check.is_file_in_dir', autospec=True)
def test_check_deploy_check_pass(mock_is_file_in_dir, capfd):
    # Deploy check passes when file found in service path

    mock_is_file_in_dir.return_value = True

    deploy_check('service_path')
    expected_output = "%s\n" % PaastaCheckMessages.DEPLOY_YAML_FOUND

    output, _ = capfd.readouterr()
    assert output == expected_output


@patch('paasta_tools.cli.cmds.check.is_file_in_dir', autospec=True)
def test_check_deploy_check_fail(mock_is_file_in_dir, capfd):
    # Deploy check fails when file not in service path

    mock_is_file_in_dir.return_value = False

    deploy_check('service_path')
    expected_output = "%s\n" % PaastaCheckMessages.DEPLOY_YAML_MISSING

    output, _ = capfd.readouterr()
    assert output == expected_output


@patch('paasta_tools.cli.cmds.check.is_file_in_dir', autospec=True)
def test_check_docker_exists_and_is_valid(mock_is_file_in_dir, capfd):
    mock_is_file_in_dir.return_value = "/fake/path"

    docker_check()

    output, _ = capfd.readouterr()
    assert PaastaCheckMessages.DOCKERFILE_FOUND in output


@patch('paasta_tools.cli.cmds.check.is_file_in_dir', autospec=True)
def test_check_docker_check_file_not_found(mock_is_file_in_dir, capfd):
    mock_is_file_in_dir.return_value = False

    docker_check()

    output, _ = capfd.readouterr()
    assert PaastaCheckMessages.DOCKERFILE_MISSING in output


@patch('paasta_tools.cli.cmds.check.is_file_in_dir', autospec=True)
def test_check_yaml_check_pass(mock_is_file_in_dir, capfd):
    # marathon.yaml exists and is valid

    mock_is_file_in_dir.return_value = "/fake/path"
    expected_output = "{}\n{}\n{}\n".format(
        PaastaCheckMessages.MARATHON_YAML_FOUND,
        PaastaCheckMessages.CHRONOS_YAML_FOUND,
        PaastaCheckMessages.ADHOC_YAML_FOUND,
    )

    yaml_check('path')

    output, _ = capfd.readouterr()
    assert output == expected_output


@patch('paasta_tools.cli.cmds.check.is_file_in_dir', autospec=True)
def test_check_yaml_check_fail(mock_is_file_in_dir, capfd):
    # marathon.yaml exists and is valid

    mock_is_file_in_dir.return_value = False
    expected_output = "%s\n" % PaastaCheckMessages.YAML_MISSING

    yaml_check('path')

    output, _ = capfd.readouterr()
    assert output == expected_output


@patch('paasta_tools.cli.cmds.check.is_file_in_dir', autospec=True)
@patch('paasta_tools.cli.cmds.check.get_team', autospec=True)
def test_check_sensu_check_pass(mock_get_team, mock_is_file_in_dir, capfd):
    # monitoring.yaml exists and team is found

    mock_is_file_in_dir.return_value = "/fake/path"
    team = 'team-service-infra'
    mock_get_team.return_value = team
    expected_output = "{}\n{}\n".format(
        PaastaCheckMessages.SENSU_MONITORING_FOUND,
        PaastaCheckMessages.sensu_team_found(team),
    )

    sensu_check(service='fake_service', service_path='path', soa_dir='path')

    output, _ = capfd.readouterr()
    assert output == expected_output
    mock_get_team.assert_called_once_with(
        service='fake_service', overrides={},
        soa_dir='path',
    )


@patch('paasta_tools.cli.cmds.check.is_file_in_dir', autospec=True)
@patch('paasta_tools.cli.cmds.check.get_team', autospec=True)
def test_check_sensu_team_missing(mock_get_team, mock_is_file_in_dir, capfd):
    # monitoring.yaml exists but team is not found

    mock_is_file_in_dir.return_value = "/fake/path"
    mock_get_team.return_value = None
    expected_output = "{}\n{}\n".format(
        PaastaCheckMessages.SENSU_MONITORING_FOUND,
        PaastaCheckMessages.SENSU_TEAM_MISSING,
    )

    sensu_check(service='fake_service', service_path='path', soa_dir='path')

    output, _ = capfd.readouterr()
    assert output == expected_output


@patch('paasta_tools.cli.cmds.check.is_file_in_dir', autospec=True)
def test_check_sensu_check_fail(mock_is_file_in_dir, capfd):
    # monitoring.yaml doest exist

    mock_is_file_in_dir.return_value = False
    expected_output = "%s\n" % PaastaCheckMessages.SENSU_MONITORING_MISSING

    sensu_check(service='fake_service', service_path='path', soa_dir='path')

    output, _ = capfd.readouterr()
    assert output == expected_output


@patch(
    'service_configuration_lib.'
    'read_service_configuration', autospec=True,
)
@patch('paasta_tools.cli.cmds.check.is_file_in_dir', autospec=True)
def test_check_smartstack_check_pass(
        mock_is_file_in_dir, mock_read_service_info, capfd,
):
    # smartstack.yaml exists and port is found

    mock_is_file_in_dir.return_value = True
    port = 80
    instance = 'main'
    smartstack_dict = {
        'smartstack': {
            instance: {
                'proxy_port': port,
            },
        },
    }
    mock_read_service_info.return_value = smartstack_dict
    expected_output = "%s\n%s\n" \
                      % (
                          PaastaCheckMessages.SMARTSTACK_YAML_FOUND,
                          PaastaCheckMessages.smartstack_port_found(
                              instance, port,
                          ),
                      )

    smartstack_check(service='fake_service', service_path='path', soa_dir='path')

    output, _ = capfd.readouterr()
    assert output == expected_output


@patch(
    'service_configuration_lib.'
    'read_service_configuration', autospec=True,
)
@patch('paasta_tools.cli.cmds.check.is_file_in_dir', autospec=True)
def test_check_smartstack_check_missing_port(
        mock_is_file_in_dir, mock_read_service_info, capfd,
):
    # smartstack.yaml, instance exists, but no ports found

    mock_is_file_in_dir.return_value = True
    instance = 'main'
    smartstack_dict = {
        instance: {
            'foo': 0,
        },
    }
    mock_read_service_info.return_value = smartstack_dict
    expected_output = "%s\n%s\n" \
                      % (
                          PaastaCheckMessages.SMARTSTACK_YAML_FOUND,
                          PaastaCheckMessages.SMARTSTACK_PORT_MISSING,
                      )

    smartstack_check(service='fake_service', service_path='path', soa_dir='path')

    output, _ = capfd.readouterr()
    assert output == expected_output


@patch(
    'paasta_tools.utils.'
    'read_service_configuration', autospec=True,
)
@patch('paasta_tools.cli.cmds.check.is_file_in_dir', autospec=True)
def test_check_smartstack_check_missing_instance(
        mock_is_file_in_dir, mock_read_service_info, capfd,
):
    # smartstack.yaml exists, but no instances found

    mock_is_file_in_dir.return_value = True
    smartstack_dict = {}
    mock_read_service_info.return_value = smartstack_dict
    expected_output = "%s\n%s\n" \
                      % (
                          PaastaCheckMessages.SMARTSTACK_YAML_FOUND,
                          PaastaCheckMessages.SMARTSTACK_PORT_MISSING,
                      )

    smartstack_check(service='fake_service', service_path='path', soa_dir='path')

    output, _ = capfd.readouterr()
    assert output == expected_output


@patch('paasta_tools.cli.cmds.check.is_file_in_dir', autospec=True)
def test_check_smartstack_check_is_ok_when_no_smartstack(mock_is_file_in_dir, capfd):

    mock_is_file_in_dir.return_value = False
    expected_output = ""
    smartstack_check(service='fake_service', service_path='path', soa_dir='path')

    output, _ = capfd.readouterr()
    assert output == expected_output


@patch('paasta_tools.cli.cmds.check._run', autospec=True)
def test_makefile_responds_to_good(mock_run):
    mock_run.return_value = (0, 'Output')
    actual = makefile_responds_to('present-target')
    assert actual is True


@patch('paasta_tools.cli.cmds.check._run', autospec=True)
def test_makefile_responds_to_run(mock_run):
    mock_run.return_value = (2, 'Output')
    actual = makefile_responds_to('non-present-target')
    assert actual is False


def test_makefile_has_a_tab_true():
    fake_makefile_path = 'UNUSED'
    fake_contents = 'target:\n\tcommand'
    with patch(
        'paasta_tools.cli.cmds.check.get_file_contents',
        autospec=True,
        return_value=fake_contents,
    ):
        assert makefile_has_a_tab(fake_makefile_path) is True


def test_makefile_has_a_tab_false():
    fake_makefile_path = 'UNUSED'
    fake_contents = 'target:\n    command'
    with patch(
        'paasta_tools.cli.cmds.check.get_file_contents',
        autospec=True,
        return_value=fake_contents,
    ):
        assert makefile_has_a_tab(fake_makefile_path) is False


def test_makefile_has_docker_tag_true():
    fake_makefile_path = 'UNUSED'
    fake_contents = 'Blah\nDOCKER_TAG ?= something:\ntarget:\n    command'
    with patch(
        'paasta_tools.cli.cmds.check.get_file_contents',
        autospec=True,
        return_value=fake_contents,
    ):
        assert makefile_has_docker_tag(fake_makefile_path) is True


def test_makefile_has_docker_tag_false():
    fake_makefile_path = 'UNUSED'
    fake_contents = 'target:\n    command'
    with patch(
        'paasta_tools.cli.cmds.check.get_file_contents',
        autospec=True,
        return_value=fake_contents,
    ):
        assert makefile_has_docker_tag(fake_makefile_path) is False


@patch('paasta_tools.cli.cmds.check.get_pipeline_config', autospec=True)
def test_deploy_has_security_check_false(mock_pipeline_config, capfd):
    mock_pipeline_config.return_value = [
        {'step': 'itest', },
        {'step': 'push-to-registry', },
        {'step': 'hab.canary', 'trigger_next_step_manually': True, },
        {'step': 'hab.main', },
    ]
    actual = deploy_has_security_check(service='fake_service', soa_dir='/fake/path')
    assert actual is False


@patch('paasta_tools.cli.cmds.check.get_pipeline_config', autospec=True)
def test_deploy_has_security_check_true(mock_pipeline_config, capfd):
    mock_pipeline_config.return_value = [
        {'step': 'itest', },
        {'step': 'security-check', },
        {'step': 'push-to-registry', },
        {'step': 'hab.canary', 'trigger_next_step_manually': True, },
        {'step': 'hab.main', },
    ]
    actual = deploy_has_security_check(service='fake_service', soa_dir='/fake/path')
    assert actual is True


@patch('paasta_tools.cli.cmds.check.get_pipeline_config', autospec=True)
def test_deploy_has_performance_check_false(mock_pipeline_config, capfd):
    mock_pipeline_config.return_value = [
        {'step': 'itest', },
        {'step': 'push-to-registry', },
        {'step': 'hab.canary', 'trigger_next_step_manually': True, },
        {'step': 'hab.main', },
    ]
    actual = deploy_has_performance_check(service='fake_service', soa_dir='/fake/path')
    assert actual is False


@patch('paasta_tools.cli.cmds.check.get_pipeline_config', autospec=True)
def test_deploy_has_performance_check_true(mock_pipeline_config, capfd):
    mock_pipeline_config.return_value = [
        {'step': 'itest', },
        {'step': 'performance-check', },
        {'step': 'push-to-registry', },
        {'step': 'hab.canary', 'trigger_next_step_manually': True, },
        {'step': 'hab.main', },
    ]
    actual = deploy_has_performance_check(service='fake_service', soa_dir='/fake/path')
    assert actual is True


@patch('paasta_tools.cli.cmds.check.get_instance_config', autospec=True)
@patch('paasta_tools.cli.cmds.check.list_clusters', autospec=True)
@patch('paasta_tools.cli.cmds.check.get_service_instance_list', autospec=True)
def test_get_deploy_groups_used_by_framework(
    mock_get_service_instance_list,
    mock_list_clusters,
    mock_get_instance_config,
):
    mock_list_clusters.return_value = ['cluster1']
    mock_get_service_instance_list.return_value = [('unused', 'instance1'), ('unused', 'instance2')]
    mock_get_instance_config.side_effect = lambda service, instance, cluster, \
        soa_dir, load_deployments, instance_type: \
        MarathonServiceConfig(
            service=service,
            instance=instance,
            cluster=cluster,
            config_dict={},
            branch_dict=None,
        )
    expected = {'cluster1.instance1', 'cluster1.instance2'}
    actual = get_deploy_groups_used_by_framework('marathon', service='unused', soa_dir='/fake/path')
    assert actual == expected


@patch('paasta_tools.cli.cmds.check.get_pipeline_config', autospec=True)
@patch('paasta_tools.cli.cmds.check.get_deploy_groups_used_by_framework', autospec=True)
def test_marathon_deployments_check_good(
    mock_get_deploy_groups_used_by_framework,
    mock_get_pipeline_config,
    capfd,
):
    mock_get_pipeline_config.return_value = [
        {'step': 'itest', },
        {'step': 'performance-check', },
        {'step': 'push-to-registry', },
        {'step': 'hab.canary', 'trigger_next_step_manually': True, },
        {'step': 'hab.main', },
    ]
    mock_get_deploy_groups_used_by_framework.return_value = [
        'hab.canary',
        'hab.main',
    ]
    actual = deployments_check(service='fake_service', soa_dir='/fake/path')
    assert actual is True


@patch('paasta_tools.cli.cmds.check.get_pipeline_config', autospec=True)
@patch('paasta_tools.cli.cmds.check.get_deploy_groups_used_by_framework', autospec=True)
def test_marathon_deployments_deploy_but_not_marathon(
    mock_get_deploy_groups_used_by_framework,
    mock_get_pipeline_config,
    capfd,
):
    mock_get_pipeline_config.return_value = [
        {'step': 'itest', },
        {'step': 'performance-check', },
        {'step': 'push-to-registry', },
        {'step': 'hab.canary', 'trigger_next_step_manually': True, },
        {'step': 'hab.main', },
        {'step': 'hab.EXTRA', },
    ]
    mock_get_deploy_groups_used_by_framework.return_value = [
        'hab.canary',
        'hab.main',
    ]
    actual = deployments_check(service='fake_service', soa_dir='/fake/service')
    assert actual is False
    assert 'EXTRA' in capfd.readouterr()[0]


@patch('paasta_tools.cli.cmds.check.get_pipeline_config', autospec=True)
@patch('paasta_tools.cli.cmds.check.get_deploy_groups_used_by_framework', autospec=True)
def test_marathon_deployments_marathon_but_not_deploy(
    mock_get_deploy_groups_used_by_framework,
    mock_get_pipeline_config,
    capfd,
):
    mock_get_pipeline_config.return_value = [
        {'step': 'itest', },
        {'step': 'performance-check', },
        {'step': 'push-to-registry', },
        {'step': 'hab.canary', 'trigger_next_step_manually': True, },
        {'step': 'hab.main', },
    ]
    mock_get_deploy_groups_used_by_framework.return_value = [
        'hab.canary',
        'hab.main',
        'hab.BOGUS',
    ]
    actual = deployments_check(service='fake_service', soa_dir='/fake/path')
    assert actual is False
    assert 'BOGUS' in capfd.readouterr()[0]


def test_makefile_check():
    fake_makefile_path = 'UNUSED'
    fake_contents = "DOCKER_TAG ?= something\ntest:\n\tsomething\nitest:\n\tsomething"
    with patch(
        'paasta_tools.cli.cmds.check.get_file_contents',
        autospec=True,
        return_value=fake_contents,
    ), patch(
        'paasta_tools.cli.cmds.check.makefile_has_a_tab',
        autospec=True,
    ) as mock_makefile_has_a_tab, patch(
        'paasta_tools.cli.cmds.check.makefile_responds_to',
        autospec=True,
    ) as mock_makefile_responds_to, patch(
        'paasta_tools.cli.cmds.check.makefile_has_docker_tag',
        autospec=True,
    ) as mock_makefile_has_docker_tag, patch(
        'paasta_tools.cli.cmds.check.is_file_in_dir',
        autospec=True,
        return_value=fake_makefile_path,
    ):
        makefile_check()
        assert mock_makefile_has_a_tab.call_count == 1
        calls = [call('test'), call('itest'), call('cook-image')]
        mock_makefile_responds_to.assert_has_calls(calls, any_order=True)
        assert mock_makefile_has_docker_tag.call_count == 1
