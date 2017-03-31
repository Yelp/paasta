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
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from mock import MagicMock
from mock import Mock
from mock import patch
from mock import sentinel
from pytest import mark
from pytest import raises

from paasta_tools import utils
from paasta_tools.cli.cmds import status
from paasta_tools.cli.cmds.status import missing_deployments_message
from paasta_tools.cli.cmds.status import paasta_args_mixer
from paasta_tools.cli.cmds.status import paasta_status
from paasta_tools.cli.cmds.status import report_invalid_whitelist_values
from paasta_tools.cli.cmds.status import report_status
from paasta_tools.cli.utils import NoSuchService
from paasta_tools.cli.utils import PaastaCheckMessages
from paasta_tools.cli.utils import PaastaColors


@patch('paasta_tools.cli.utils.validate_service_name', autospec=True)
def test_figure_out_service_name_not_found(
        mock_validate_service_name, capfd,
):
    # paasta_status with invalid -s service_name arg results in error
    mock_validate_service_name.side_effect = NoSuchService(None)
    parsed_args = Mock()
    parsed_args.service = 'fake_service'

    expected_output = '%s\n' % NoSuchService.GUESS_ERROR_MSG

    # Fail if exit(1) does not get called
    with raises(SystemExit) as sys_exit:
        status.figure_out_service_name(parsed_args)

    output, _ = capfd.readouterr()
    assert sys_exit.value.code == 1
    assert output == expected_output


@patch('paasta_tools.cli.utils.validate_service_name', autospec=True)
@patch('paasta_tools.cli.utils.guess_service_name', autospec=True)
def test_status_arg_service_not_found(
    mock_guess_service_name, mock_validate_service_name, capfd,
):
    # paasta_status with no args and non-service directory results in error
    mock_guess_service_name.return_value = 'not_a_service'
    error = NoSuchService('fake_service')
    mock_validate_service_name.side_effect = error
    expected_output = str(error) + "\n"

    args = MagicMock()
    args.service = False

    # Fail if exit(1) does not get called
    with raises(SystemExit) as sys_exit:
        paasta_status(args)

    output, _ = capfd.readouterr()
    assert sys_exit.value.code == 1
    assert output == expected_output


@patch('paasta_tools.cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_report_status_for_cluster_displays_deployed_service(
    mock_report_invalid_whitelist_values,
    mock_execute_paasta_serviceinit_on_remote_master,
    capfd,
):
    # paasta_status with no args displays deploy info - vanilla case
    service = 'fake_service'
    planned_deployments = ['fake_cluster.fake_instance']
    actual_deployments = {
        'fake_cluster.fake_instance': 'sha'
    }
    instance_whitelist = []
    fake_system_paasta_config = utils.SystemPaastaConfig({}, '/fake/config')
    fake_status = 'status: SOMETHING FAKE'
    mock_execute_paasta_serviceinit_on_remote_master.return_value = (
        sentinel.return_value,
        fake_status,
    )
    expected_output = (
        "\n"
        "cluster: fake_cluster\n"
        "    %s\n"
        % (fake_status)
    )

    status.report_status_for_cluster(
        service=service,
        cluster='fake_cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=fake_system_paasta_config,
    )
    output, _ = capfd.readouterr()
    assert expected_output in output
    mock_execute_paasta_serviceinit_on_remote_master.assert_called_once_with(
        'status', 'fake_cluster', 'fake_service', 'fake_instance',
        fake_system_paasta_config, stream=True, verbose=0, ignore_ssh_output=True)


@patch('paasta_tools.cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_report_status_for_cluster_displays_multiple_lines_from_execute_paasta_serviceinit_on_remote_master(
    mock_report_invalid_whitelist_values,
    mock_execute_paasta_serviceinit_on_remote_master,
    capfd,
):
    # paasta_status with no args displays deploy info - vanilla case
    service = 'fake_service'
    planned_deployments = ['cluster.instance']
    actual_deployments = {
        'cluster.instance': 'this_is_a_sha'
    }
    instance_whitelist = []
    fake_system_paasta_config = utils.SystemPaastaConfig({}, '/fake/config')
    fake_status = 'status: SOMETHING FAKE\nand then something fake\non another line!\n\n\n'
    mock_execute_paasta_serviceinit_on_remote_master.return_value = (
        sentinel.return_value,
        fake_status,
    )
    expected_output = (
        "    status: SOMETHING FAKE\n"
        "    and then something fake\n"
        "    on another line!\n"
    )

    status.report_status_for_cluster(
        service=service,
        cluster='cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=fake_system_paasta_config,
    )
    output, _ = capfd.readouterr()
    assert expected_output in output


@patch('paasta_tools.cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_report_status_for_cluster_instance_sorts_in_deploy_order(
    mock_report_invalid_whitelist_values,
    mock_execute_paasta_serviceinit_on_remote_master,
    capfd,
):
    # paasta_status with no args displays deploy info
    service = 'fake_service'
    planned_deployments = [
        'fake_cluster.fake_instance_a',
        'fake_cluster.fake_instance_b',
    ]
    actual_deployments = {
        'fake_cluster.fake_instance_a': '533976a9',
        'fake_cluster.fake_instance_b': '533976a9',
    }
    instance_whitelist = []
    fake_system_paasta_config = utils.SystemPaastaConfig({}, '/fake/config')
    fake_status = 'status: SOMETHING FAKE'
    mock_execute_paasta_serviceinit_on_remote_master.return_value = (
        sentinel.return_value,
        fake_status,
    )
    expected_output = (
        "\n"
        "cluster: fake_cluster\n"
        "    %s\n"
        % (fake_status)
    )

    status.report_status_for_cluster(
        service=service,
        cluster='fake_cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=fake_system_paasta_config,
    )
    output, _ = capfd.readouterr()
    assert expected_output in output
    mock_execute_paasta_serviceinit_on_remote_master.assert_called_once_with(
        'status', 'fake_cluster', 'fake_service', 'fake_instance_a,fake_instance_b',
        fake_system_paasta_config, stream=True, verbose=0, ignore_ssh_output=True)


@patch('paasta_tools.cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_print_cluster_status_missing_deploys_in_red(
    mock_report_invalid_whitelist_values,
    mock_execute_paasta_serviceinit_on_remote_master,
    capfd,
):
    # paasta_status displays missing deploys in red
    service = 'fake_service'
    planned_deployments = [
        'a_cluster.a_instance',
        'a_cluster.b_instance',
    ]
    actual_deployments = {
        'a_cluster.a_instance': '533976a981679d586bed1cfb534fdba4b4e2c815',
    }
    instance_whitelist = []
    fake_status = 'status: SOMETHING FAKE'
    fake_system_paasta_config = utils.SystemPaastaConfig({}, '/fake/config')
    mock_execute_paasta_serviceinit_on_remote_master.return_value = (
        sentinel.return_value,
        fake_status,
    )
    expected_output = (
        "\n"
        "cluster: a_cluster\n"
        "  instance: %s\n"
        "    Git sha:    None (not deployed yet)\n"
        "    %s\n"
        % (
            PaastaColors.red('b_instance'),
            fake_status,
        )
    )

    status.report_status_for_cluster(
        service=service,
        cluster='a_cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=fake_system_paasta_config,
    )
    output, _ = capfd.readouterr()
    assert expected_output in output


@mark.parametrize('verbosity_level', [0, 2])
@patch('paasta_tools.cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_print_cluster_status_calls_execute_paasta_serviceinit_on_remote_master(
    mock_report_invalid_whitelist_values,
    mock_execute_paasta_serviceinit_on_remote_master,
    verbosity_level,
    capfd,
):
    service = 'fake_service'
    planned_deployments = [
        'a_cluster.a_instance',
        'a_cluster.b_instance',
    ]
    actual_deployments = {
        'a_cluster.a_instance': 'this_is_a_sha',
    }
    instance_whitelist = []
    fake_system_paasta_config = utils.SystemPaastaConfig({}, '/fake/config')

    fake_output = "Marathon: 5 instances"
    mock_execute_paasta_serviceinit_on_remote_master.return_value = (
        sentinel.return_value,
        fake_output,
    )
    expected_output = "    %s\n" % fake_output
    status.report_status_for_cluster(
        service=service,
        cluster='a_cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=fake_system_paasta_config,
        verbose=verbosity_level,
    )
    assert mock_execute_paasta_serviceinit_on_remote_master.call_count == 1
    mock_execute_paasta_serviceinit_on_remote_master.assert_any_call(
        'status', 'a_cluster', service, 'a_instance', fake_system_paasta_config,
        stream=True, verbose=verbosity_level, ignore_ssh_output=True
    )

    output, _ = capfd.readouterr()
    assert expected_output in output


@patch('paasta_tools.cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_report_status_for_cluster_obeys_instance_whitelist(
    mock_report_invalid_whitelist_values,
    mock_execute_paasta_serviceinit_on_remote_master,
    capfd,
):
    service = 'fake_service'
    planned_deployments = ['fake_cluster.fake_instance_a', 'fake_cluster.fake_instance_b']
    actual_deployments = {
        'fake_cluster.fake_instance_a': 'sha',
        'fake_cluster.fake_instance_b': 'sha',
    }
    instance_whitelist = ['fake_instance_a']
    fake_system_paasta_config = utils.SystemPaastaConfig({}, '/fake/config')

    mock_execute_paasta_serviceinit_on_remote_master.return_value = (
        sentinel.return_value,
        'fake_output'
    )

    status.report_status_for_cluster(
        service=service,
        cluster='fake_cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=fake_system_paasta_config,
    )
    mock_execute_paasta_serviceinit_on_remote_master.assert_called_once_with(
        'status', 'fake_cluster', 'fake_service', 'fake_instance_a',
        fake_system_paasta_config, stream=True, verbose=0, ignore_ssh_output=True)


@patch('paasta_tools.cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_report_status_calls_report_invalid_whitelist_values(
    mock_report_invalid_whitelist_values,
    mock_execute_paasta_serviceinit_on_remote_master,
    capfd,
):
    service = 'fake_service'
    planned_deployments = ['cluster.instance1', 'cluster.instance2']
    actual_deployments = {}
    instance_whitelist = []
    fake_system_paasta_config = utils.SystemPaastaConfig({}, '/fake/config')

    status.report_status_for_cluster(
        service=service,
        cluster='cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=fake_system_paasta_config,
    )
    mock_report_invalid_whitelist_values.assert_called_once_with(
        instance_whitelist,
        ['instance1', 'instance2'],
        'instance',
    )


@patch('paasta_tools.cli.cmds.status.load_system_paasta_config', autospec=True)
@patch('paasta_tools.cli.cmds.status.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.status.get_deploy_info', autospec=True)
@patch('paasta_tools.cli.cmds.status.get_actual_deployments', autospec=True)
def test_status_pending_pipeline_build_message(
        mock_get_actual_deployments, mock_get_deploy_info,
        mock_figure_out_service_name, mock_load_system_paasta_config, capfd,
):
    # If deployments.json is missing SERVICE, output the appropriate message
    service = 'fake_service'
    mock_figure_out_service_name.return_value = service
    pipeline = [{'instancename': 'cluster.instance'}]
    mock_get_deploy_info.return_value = {'pipeline': pipeline}
    fake_system_paasta_config = utils.SystemPaastaConfig({}, '/fake/config')
    mock_load_system_paasta_config.return_value = fake_system_paasta_config

    actual_deployments = {}
    mock_get_actual_deployments.return_value = actual_deployments
    expected_output = missing_deployments_message(service)

    args = MagicMock()
    args.service = service
    args.deploy_group = None

    paasta_status(args)
    output, _ = capfd.readouterr()
    assert expected_output in output


@patch('paasta_tools.cli.cmds.status.load_deployments_json', autospec=True)
def test_get_actual_deployments(mock_get_deployments,):
    mock_get_deployments.return_value = utils.DeploymentsJson({
        'fake_service:paasta-b_cluster.b_instance': {
            'docker_image': 'this_is_a_sha',
        },
        'fake_service:paasta-a_cluster.a_instance': {
            'docker_image': 'this_is_a_sha',
        }
    })
    expected = {
        'a_cluster.a_instance': 'this_is_a_sha',
        'b_cluster.b_instance': 'this_is_a_sha',
    }

    actual = status.get_actual_deployments('fake_service', '/fake/soa/dir')
    assert expected == actual


@patch('paasta_tools.cli.cmds.status.read_deploy', autospec=True)
def test_get_deploy_info_exists(mock_read_deploy):
    expected = 'fake deploy yaml'
    mock_read_deploy.return_value = expected
    actual = status.get_deploy_info('fake_service')
    assert expected == actual


@patch('paasta_tools.cli.cmds.status.read_deploy', autospec=True)
def test_get_deploy_info_does_not_exist(mock_read_deploy, capfd):
    mock_read_deploy.return_value = False
    expected_output = '%s\n' % PaastaCheckMessages.DEPLOY_YAML_MISSING
    with raises(SystemExit) as sys_exit:
        status.get_deploy_info('fake_service')
    output, _ = capfd.readouterr()
    assert sys_exit.value.code == 1
    assert output == expected_output


@patch('paasta_tools.cli.cmds.status.load_system_paasta_config', autospec=True)
@patch('paasta_tools.cli.cmds.status.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.status.get_actual_deployments', autospec=True)
@patch('paasta_tools.cli.cmds.status.get_planned_deployments', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_status', autospec=True)
def test_status_calls_sergeants(
    mock_report_status,
    mock_get_planned_deployments,
    mock_get_actual_deployments,
    mock_figure_out_service_name,
    mock_load_system_paasta_config,
    capfd,
):
    service = 'fake_service'
    mock_figure_out_service_name.return_value = service

    planned_deployments = [
        'cluster1.instance1', 'cluster1.instance2', 'cluster2.instance1']
    mock_get_planned_deployments.return_value = planned_deployments

    actual_deployments = {
        'fake_service:paasta-cluster.instance': 'this_is_a_sha'
    }
    mock_get_actual_deployments.return_value = actual_deployments
    fake_system_paasta_config = utils.SystemPaastaConfig({}, '/fake/config')
    mock_load_system_paasta_config.return_value = fake_system_paasta_config
    mock_report_status.return_value = sentinel.return_value

    args = MagicMock()
    args.service = service
    args.clusters = None
    args.instances = None
    args.verbose = False
    args.soa_dir = '/fake/soa/dir'
    return_value = paasta_status(args)

    assert return_value == sentinel.return_value

    mock_figure_out_service_name.assert_called_once_with(args, '/fake/soa/dir')
    mock_get_actual_deployments.assert_called_once_with(service, '/fake/soa/dir')
    mock_report_status.assert_called_once_with(
        service=service,
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        cluster_whitelist=[],
        instance_whitelist=[],
        system_paasta_config=fake_system_paasta_config,
        verbose=0,
        use_api_endpoint=False
    )


def test_report_invalid_whitelist_values_no_whitelists():
    whitelist = []
    items = ['cluster1', 'cluster2', 'cluster3']
    item_type = 'thingy'
    actual = report_invalid_whitelist_values(whitelist, items, item_type)
    assert actual == ''


def test_report_invalid_whitelist_values_with_whitelists():
    whitelist = ['bogus1', 'cluster1']
    items = ['cluster1', 'cluster2', 'cluster3']
    item_type = 'thingy'
    actual = report_invalid_whitelist_values(whitelist, items, item_type)
    assert 'Warning' in actual
    assert item_type in actual
    assert 'bogus1' in actual


@patch('paasta_tools.cli.cmds.status.report_status_for_cluster', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_report_status_returns_zero_when_clusters_pass(
    mock_report_invalid_whitelist_values,
    mock_report_status_for_cluster,
    capfd,
):
    service = 'fake_service'
    cluster_whitelist = []
    instance_whitelist = []
    deploy_pipeline = actual_deployments = [
        'cluster1.main', 'cluster2.main', 'cluster3.main']
    fake_system_paasta_config = utils.SystemPaastaConfig({}, '/fake/config')

    mock_report_status_for_cluster.side_effect = [0, 0, 0]

    return_value = report_status(
        service=service,
        deploy_pipeline=deploy_pipeline,
        actual_deployments=actual_deployments,
        cluster_whitelist=cluster_whitelist,
        instance_whitelist=instance_whitelist,
        system_paasta_config=fake_system_paasta_config,
    )

    assert return_value == 0
    assert mock_report_status_for_cluster.call_count == 3


@patch('paasta_tools.cli.cmds.status.report_status_for_cluster', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_report_status_returns_one_when_clusters_pass(
    mock_report_invalid_whitelist_values,
    mock_report_status_for_cluster,
    capfd,
):
    service = 'fake_service'
    cluster_whitelist = []
    instance_whitelist = []
    deploy_pipeline = actual_deployments = [
        'cluster1.main', 'cluster2.main', 'cluster3.main']
    fake_system_paasta_config = utils.SystemPaastaConfig({}, '/fake/config')

    mock_report_status_for_cluster.side_effect = [0, 0, 255]

    return_value = report_status(
        service=service,
        deploy_pipeline=deploy_pipeline,
        actual_deployments=actual_deployments,
        cluster_whitelist=cluster_whitelist,
        instance_whitelist=instance_whitelist,
        system_paasta_config=fake_system_paasta_config,
    )

    assert return_value == 1
    assert mock_report_status_for_cluster.call_count == 3


@patch('paasta_tools.cli.cmds.status.report_status_for_cluster', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_report_status_obeys_cluster_whitelist(
    mock_report_invalid_whitelist_values,
    mock_report_status_for_cluster,
    capfd,
):
    service = 'fake_service'
    cluster_whitelist = ['cluster1']
    instance_whitelist = []
    deploy_pipeline = actual_deployments = [
        'cluster1.main', 'cluster2.main', 'cluster3.main']
    fake_system_paasta_config = utils.SystemPaastaConfig({}, '/fake/config')
    report_status(
        service=service,
        deploy_pipeline=deploy_pipeline,
        actual_deployments=actual_deployments,
        cluster_whitelist=cluster_whitelist,
        instance_whitelist=instance_whitelist,
        system_paasta_config=fake_system_paasta_config,
    )
    mock_report_invalid_whitelist_values.assert_called_once_with(
        cluster_whitelist, ['cluster1', 'cluster2', 'cluster3'], 'cluster')
    mock_report_status_for_cluster.assert_called_once_with(
        service=service,
        cluster='cluster1',
        deploy_pipeline=deploy_pipeline,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=fake_system_paasta_config,
        verbose=0,
        use_api_endpoint=False
    )


@patch('paasta_tools.cli.cmds.status.report_status_for_cluster', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_report_status_handle_none_whitelist(
    mock_report_invalid_whitelist_values,
    mock_report_status_for_cluster,
    capfd,
):
    service = 'fake_service'
    cluster_whitelist = []
    instance_whitelist = []
    deploy_pipeline = actual_deployments = [
        'cluster1.main', 'cluster2.main', 'cluster3.main']
    fake_system_paasta_config = utils.SystemPaastaConfig({}, '/fake/config')
    report_status(
        service=service,
        deploy_pipeline=deploy_pipeline,
        actual_deployments=actual_deployments,
        cluster_whitelist=cluster_whitelist,
        instance_whitelist=instance_whitelist,
        system_paasta_config=fake_system_paasta_config,
    )

    mock_report_status_for_cluster.assert_any_call(
        service=service,
        cluster='cluster1',
        deploy_pipeline=deploy_pipeline,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=fake_system_paasta_config,
        verbose=0,
        use_api_endpoint=False
    )
    mock_report_status_for_cluster.assert_any_call(
        service=service,
        cluster='cluster2',
        deploy_pipeline=deploy_pipeline,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=fake_system_paasta_config,
        verbose=0,
        use_api_endpoint=False
    )
    mock_report_status_for_cluster.assert_any_call(
        service=service,
        cluster='cluster3',
        deploy_pipeline=deploy_pipeline,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=fake_system_paasta_config,
        verbose=0,
        use_api_endpoint=False
    )


@patch('paasta_tools.cli.cmds.status.get_cluster_instance_map_for_service', autospec=True)
def test_paasta_args_mixer_clusters_and_instances_clusters_instances_deploy_group(mock_cluster_instance_map):
    PaastaArgs = namedtuple('PaastaArgs', ['soa_dir', 'clusters', 'instances', 'deploy_group'])
    args = PaastaArgs(soa_dir='/fake/soa/dir',
                      deploy_group='fake_deploy_group',
                      clusters='cluster1',
                      instances='instance1,instance3')
    mock_cluster_instance_map.return_value = {'cluster1': {'instances': ['instance1', 'instance2']},
                                              'cluster2': {'instances': ['instance3']}}

    pargs = paasta_args_mixer(args, 'fake_service')
    assert pargs.instance_whitelist == ['instance1']
    assert pargs.cluster_whitelist == ['cluster1']


@patch('paasta_tools.cli.cmds.status.get_cluster_instance_map_for_service', autospec=True)
def test_paasta_args_mixer_clusters_uses_deploy_group_when_no_clusters_and_instances(mock_cluster_instance_map):
    PaastaArgs = namedtuple('PaastaArgs', ['soa_dir', 'clusters', 'instances', 'deploy_group'])
    args = PaastaArgs(soa_dir='/fake/soa/dir',
                      deploy_group='fake_deploy_group',
                      clusters=None,
                      instances=None)
    mock_cluster_instance_map.return_value = {'cluster1': {'instances': ['instance1', 'instance2']},
                                              'cluster2': {'instances': ['instance3']}}

    pargs = paasta_args_mixer(args, 'fake_service')
    assert sorted(pargs.instance_whitelist) == ['instance1', 'instance2', 'instance3']
    assert sorted(pargs.cluster_whitelist) == ['cluster1', 'cluster2']


@patch('paasta_tools.cli.cmds.status.get_cluster_instance_map_for_service', autospec=True)
def test_paasta_args_mixer_clusters_do_not_whitelist_anything_by_default(mock_cluster_instance_map):
    PaastaArgs = namedtuple('PaastaArgs', ['soa_dir', 'clusters', 'instances', 'deploy_group'])
    args = PaastaArgs(soa_dir='/fake/soa/dir',
                      deploy_group=None,
                      clusters=None,
                      instances=None)
    mock_cluster_instance_map.return_value = {'cluster1': {'instances': ['instance1', 'instance2']},
                                              'cluster2': {'instances': ['instance3']}}

    pargs = paasta_args_mixer(args, 'fake_service')
    assert len(pargs.instance_whitelist) == 0
    assert len(pargs.cluster_whitelist) == 0


@patch('paasta_tools.cli.cmds.status.get_cluster_instance_map_for_service', autospec=True)
def test_paasta_args_mixer_clusters_return_none_when_cluster_not_in_deploy_group(mock_cluster_instance_map):
    PaastaArgs = namedtuple('PaastaArgs', ['soa_dir', 'clusters', 'instances', 'deploy_group'])
    args = PaastaArgs(soa_dir='/fake/soa/dir',
                      deploy_group='fake_deploy_group',
                      clusters='cluster4',
                      instances=None)
    mock_cluster_instance_map.return_value = {'cluster1': {'instances': ['instance1', 'instance2']},
                                              'cluster2': {'instances': ['instance3']}}

    assert paasta_args_mixer(args, 'fake_service') is None


@patch('paasta_tools.cli.cmds.status.get_cluster_instance_map_for_service', autospec=True)
def test_paasta_args_mixer_clusters_return_none_when_instance_not_in_deploy_group(mock_cluster_instance_map):
    PaastaArgs = namedtuple('PaastaArgs', ['soa_dir', 'clusters', 'instances', 'deploy_group'])
    args = PaastaArgs(soa_dir='/fake/soa/dir',
                      deploy_group='fake_deploy_group',
                      clusters=None,
                      instances='instance5')
    mock_cluster_instance_map.return_value = {'cluster1': {'instances': ['instance1', 'instance2']},
                                              'cluster2': {'instances': ['instance3']}}

    assert paasta_args_mixer(args, 'fake_service') is None
