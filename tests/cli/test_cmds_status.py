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
from collections import namedtuple

from mock import call
from mock import MagicMock
from mock import Mock
from mock import patch
from mock import sentinel
from pytest import mark
from pytest import raises

from paasta_tools import utils
from paasta_tools.cli.cmds import status
from paasta_tools.cli.cmds.status import apply_args_filters
from paasta_tools.cli.cmds.status import missing_deployments_message
from paasta_tools.cli.cmds.status import paasta_status
from paasta_tools.cli.cmds.status import report_invalid_whitelist_values
from paasta_tools.cli.cmds.status import verify_instances
from paasta_tools.cli.utils import NoSuchService
from paasta_tools.cli.utils import PaastaCheckMessages
from paasta_tools.cli.utils import PaastaColors


def make_fake_instance_conf(cluster, service, instance, deploy_group=None, team=None):
    conf = MagicMock()
    conf.get_cluster.return_value = cluster
    conf.get_service.return_value = service
    conf.get_instance.return_value = instance
    conf.get_deploy_group.return_value = deploy_group
    conf.get_team.return_value = team
    return conf


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


@patch('paasta_tools.cli.cmds.status.list_clusters', autospec=True)
@patch('paasta_tools.cli.cmds.status.load_system_paasta_config', autospec=True)
@patch('paasta_tools.cli.utils.validate_service_name', autospec=True)
@patch('paasta_tools.cli.utils.guess_service_name', autospec=True)
def test_status_arg_service_not_found(
    mock_guess_service_name, mock_validate_service_name,
    mock_load_system_paasta_config, mock_list_clusters, capfd,
    system_paasta_config,
):
    # paasta_status with no args and non-service directory results in error
    mock_guess_service_name.return_value = 'not_a_service'
    error = NoSuchService('fake_service')
    mock_validate_service_name.side_effect = error
    mock_list_clusters.return_value = ['cluster1']
    mock_load_system_paasta_config.return_value = system_paasta_config
    expected_output = str(error) + "\n"

    args = MagicMock()
    args.service = None
    args.owner = None
    args.clusters = None
    args.instances = None
    args.deploy_group = None

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
    system_paasta_config,
):
    # paasta_status with no args displays deploy info - vanilla case
    service = 'fake_service'
    planned_deployments = ['fake_cluster.fake_instance']
    actual_deployments = {
        'fake_cluster.fake_instance': 'sha',
    }
    instance_whitelist = []
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

    _, output = status.report_status_for_cluster(
        service=service,
        cluster='fake_cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=system_paasta_config,
    )
    output = '\n'.join(str(line) for line in output)
    assert expected_output in output
    mock_execute_paasta_serviceinit_on_remote_master.assert_called_once_with(
        'status', 'fake_cluster', 'fake_service', 'fake_instance',
        system_paasta_config, stream=False, verbose=0, ignore_ssh_output=True,
    )


@patch('paasta_tools.cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_report_status_for_cluster_displays_multiple_lines_from_execute_paasta_serviceinit_on_remote_master(
    mock_report_invalid_whitelist_values,
    mock_execute_paasta_serviceinit_on_remote_master,
    system_paasta_config,
):
    # paasta_status with no args displays deploy info - vanilla case
    service = 'fake_service'
    planned_deployments = ['cluster.instance']
    actual_deployments = {
        'cluster.instance': 'this_is_a_sha',
    }
    instance_whitelist = []
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

    _, output = status.report_status_for_cluster(
        service=service,
        cluster='cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=system_paasta_config,
    )
    output = '\n'.join(str(line) for line in output)
    assert expected_output in output


@patch('paasta_tools.cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_report_status_for_cluster_instance_sorts_in_deploy_order(
    mock_report_invalid_whitelist_values,
    mock_execute_paasta_serviceinit_on_remote_master,
    system_paasta_config,
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

    _, output = status.report_status_for_cluster(
        service=service,
        cluster='fake_cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=system_paasta_config,
    )
    output = '\n'.join(str(line) for line in output)
    assert expected_output in output
    mock_execute_paasta_serviceinit_on_remote_master.assert_called_once_with(
        'status', 'fake_cluster', 'fake_service', 'fake_instance_a,fake_instance_b',
        system_paasta_config, stream=False, verbose=0, ignore_ssh_output=True,
    )


@patch('paasta_tools.cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_print_cluster_status_missing_deploys_in_red(
    mock_report_invalid_whitelist_values,
    mock_execute_paasta_serviceinit_on_remote_master,
    system_paasta_config,
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

    _, output = status.report_status_for_cluster(
        service=service,
        cluster='a_cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=system_paasta_config,
    )

    output = '\n'.join(str(line) for line in output)
    assert expected_output in output


@mark.parametrize('verbosity_level', [0, 2])
@patch('paasta_tools.cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_print_cluster_status_calls_execute_paasta_serviceinit_on_remote_master(
    mock_report_invalid_whitelist_values,
    mock_execute_paasta_serviceinit_on_remote_master,
    verbosity_level,
    system_paasta_config,
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

    fake_output = "Marathon: 5 instances"
    mock_execute_paasta_serviceinit_on_remote_master.return_value = (
        sentinel.return_value,
        fake_output,
    )
    expected_output = "    %s\n" % fake_output
    _, output = status.report_status_for_cluster(
        service=service,
        cluster='a_cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=system_paasta_config,
        verbose=verbosity_level,
    )
    assert mock_execute_paasta_serviceinit_on_remote_master.call_count == 1
    mock_execute_paasta_serviceinit_on_remote_master.assert_any_call(
        'status', 'a_cluster', service, 'a_instance', system_paasta_config,
        stream=False, verbose=verbosity_level, ignore_ssh_output=True,
    )

    output = '\n'.join(str(line) for line in output)
    assert expected_output in output


@patch('paasta_tools.cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_report_status_for_cluster_obeys_instance_whitelist(
    mock_report_invalid_whitelist_values,
    mock_execute_paasta_serviceinit_on_remote_master,
    system_paasta_config,
):
    service = 'fake_service'
    planned_deployments = ['fake_cluster.fake_instance_a', 'fake_cluster.fake_instance_b']
    actual_deployments = {
        'fake_cluster.fake_instance_a': 'sha',
        'fake_cluster.fake_instance_b': 'sha',
    }
    instance_whitelist = ['fake_instance_a']

    mock_execute_paasta_serviceinit_on_remote_master.return_value = (
        sentinel.return_value,
        'fake_output',
    )

    status.report_status_for_cluster(
        service=service,
        cluster='fake_cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=system_paasta_config,
    )
    mock_execute_paasta_serviceinit_on_remote_master.assert_called_once_with(
        'status', 'fake_cluster', 'fake_service', 'fake_instance_a',
        system_paasta_config, stream=False, verbose=0, ignore_ssh_output=True,
    )


@patch('paasta_tools.cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_invalid_whitelist_values', autospec=True)
def test_report_status_calls_report_invalid_whitelist_values(
    mock_report_invalid_whitelist_values,
    mock_execute_paasta_serviceinit_on_remote_master,
    system_paasta_config,
):
    service = 'fake_service'
    planned_deployments = ['cluster.instance1', 'cluster.instance2']
    actual_deployments = {}
    instance_whitelist = []

    status.report_status_for_cluster(
        service=service,
        cluster='cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=system_paasta_config,
    )
    mock_report_invalid_whitelist_values.assert_called_once_with(
        instance_whitelist,
        ['instance1', 'instance2'],
        'instance',
    )


@patch('paasta_tools.cli.cmds.status.get_instance_configs_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.status.list_services', autospec=True)
@patch('paasta_tools.cli.cmds.status.load_system_paasta_config', autospec=True)
@patch('paasta_tools.cli.cmds.status.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.status.get_deploy_info', autospec=True)
@patch('paasta_tools.cli.cmds.status.get_actual_deployments', autospec=True)
def test_status_pending_pipeline_build_message(
        mock_get_actual_deployments, mock_get_deploy_info,
        mock_figure_out_service_name, mock_load_system_paasta_config,
        mock_list_services, mock_get_instance_configs_for_service, capfd,
        system_paasta_config,
):
    # If deployments.json is missing SERVICE, output the appropriate message
    service = 'fake_service'
    mock_figure_out_service_name.return_value = service
    mock_list_services.return_value = [service]
    pipeline = [{'instancename': 'cluster.instance'}]
    mock_get_deploy_info.return_value = {'pipeline': pipeline}
    mock_load_system_paasta_config.return_value = system_paasta_config
    mock_instance_config = make_fake_instance_conf('cluster', service, 'instancename')
    mock_get_instance_configs_for_service.return_value = [mock_instance_config]

    actual_deployments = {}
    mock_get_actual_deployments.return_value = actual_deployments
    expected_output = missing_deployments_message(service)

    args = MagicMock()
    args.service = service
    args.deploy_group = None
    args.clusters = None
    args.instances = None
    args.owner = None
    args.soa_dir = utils.DEFAULT_SOA_DIR

    paasta_status(args)
    output, _ = capfd.readouterr()
    assert expected_output in output


@patch('paasta_tools.cli.cmds.status.load_deployments_json', autospec=True)
def test_get_actual_deployments(mock_get_deployments,):
    mock_get_deployments.return_value = utils.DeploymentsJsonV1({
        'fake_service:paasta-b_cluster.b_instance': {
            'docker_image': 'this_is_a_sha',
        },
        'fake_service:paasta-a_cluster.a_instance': {
            'docker_image': 'this_is_a_sha',
        },
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


@patch('paasta_tools.cli.cmds.status.get_instance_configs_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.status.list_services', autospec=True)
@patch('paasta_tools.cli.cmds.status.load_system_paasta_config', autospec=True)
@patch('paasta_tools.cli.cmds.status.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.status.get_actual_deployments', autospec=True)
@patch('paasta_tools.cli.cmds.status.get_planned_deployments', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_status_for_cluster', autospec=True)
def test_status_calls_sergeants(
    mock_report_status,
    mock_get_planned_deployments,
    mock_get_actual_deployments,
    mock_figure_out_service_name,
    mock_load_system_paasta_config,
    mock_list_services,
    mock_get_instance_configs_for_service,
    system_paasta_config,
):
    service = 'fake_service'
    cluster = 'fake_cluster'
    mock_figure_out_service_name.return_value = service
    mock_list_services.return_value = [service]

    mock_instance_config = make_fake_instance_conf(cluster, service, 'fi')
    mock_instance_config.get_service.return_value = service
    mock_instance_config.get_cluster.return_value = cluster
    mock_get_instance_configs_for_service.return_value = [mock_instance_config]

    planned_deployments = [
        'cluster1.instance1', 'cluster1.instance2', 'cluster2.instance1',
    ]
    mock_get_planned_deployments.return_value = planned_deployments

    actual_deployments = {
        'fake_service:paasta-cluster.instance': 'this_is_a_sha',
    }
    mock_get_actual_deployments.return_value = actual_deployments
    mock_load_system_paasta_config.return_value = system_paasta_config
    mock_report_status.return_value = 1776, ['dummy', 'output']

    args = MagicMock()
    args.service = service
    args.clusters = None
    args.instances = None
    args.verbose = False
    args.owner = None
    args.deploy_group = None
    args.soa_dir = '/fake/soa/dir'
    return_value = paasta_status(args)

    assert return_value == 1776

    mock_get_actual_deployments.assert_called_once_with(service, '/fake/soa/dir')
    mock_report_status.assert_called_once_with(
        service=service,
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        cluster=cluster,
        instance_whitelist={'fi'},
        system_paasta_config=system_paasta_config,
        verbose=False,
        use_api_endpoint=False,
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


@patch('paasta_tools.cli.cmds.status.get_instance_configs_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.status.list_services', autospec=True)
@patch('paasta_tools.cli.cmds.status.figure_out_service_name', autospec=True)
def test_apply_args_filters_clusters_and_instances_clusters_instances_deploy_group(
    mock_figure_out_service_name, mock_list_services, mock_get_instance_configs_for_service,
):
    PaastaArgs = namedtuple('PaastaArgs', ['soa_dir', 'clusters', 'instances', 'deploy_group', 'owner', 'service'])
    args = PaastaArgs(
        service='fake_service',
        soa_dir='/fake/soa/dir',
        deploy_group='fake_deploy_group',
        clusters='cluster1',
        instances='instance1,instance3',
        owner=None,
    )
    mock_figure_out_service_name.return_value = 'fake_service'
    mock_list_services.return_value = ['fake_service']
    mock_get_instance_configs_for_service.return_value = [
        make_fake_instance_conf('cluster1', 'fake_service', 'instance1', 'fake_deploy_group'),
        make_fake_instance_conf('cluster1', 'fake_service', 'instance2', 'fake_deploy_group'),
        make_fake_instance_conf('cluster2', 'fake_service', 'instance3', 'fake_deploy_group'),
    ]

    pargs = apply_args_filters(args)
    assert sorted(pargs.keys()) == ['cluster1']
    assert pargs['cluster1']['fake_service'] == {'instance1'}


@patch('paasta_tools.cli.cmds.status.get_instance_configs_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.status.list_services', autospec=True)
@patch('paasta_tools.cli.cmds.status.figure_out_service_name', autospec=True)
def test_apply_args_filters_clusters_uses_deploy_group_when_no_clusters_and_instances(
    mock_figure_out_service_name, mock_list_services, mock_get_instance_configs_for_service,
):
    PaastaArgs = namedtuple('PaastaArgs', ['service', 'soa_dir', 'clusters', 'instances', 'deploy_group', 'owner'])
    args = PaastaArgs(
        service='fake_service',
        soa_dir='/fake/soa/dir',
        deploy_group='fake_deploy_group',
        clusters=None,
        instances=None,
        owner=None,
    )

    mock_figure_out_service_name.return_value = 'fake_service'
    mock_list_services.return_value = ['fake_service']
    mock_get_instance_configs_for_service.return_value = [
        make_fake_instance_conf('cluster1', 'fake_service', 'instance1', 'fake_deploy_group'),
        make_fake_instance_conf('cluster1', 'fake_service', 'instance2', 'fake_deploy_group'),
        make_fake_instance_conf('cluster2', 'fake_service', 'instance3', 'fake_deploy_group'),
    ]

    pargs = apply_args_filters(args)
    assert sorted(pargs.keys()) == ['cluster1', 'cluster2']
    assert pargs['cluster1']['fake_service'] == {'instance1', 'instance2'}
    assert pargs['cluster2']['fake_service'] == {'instance3'}


@patch('paasta_tools.cli.cmds.status.get_instance_configs_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.status.list_services', autospec=True)
@patch('paasta_tools.cli.cmds.status.figure_out_service_name', autospec=True)
def test_apply_args_filters_clusters_return_none_when_cluster_not_in_deploy_group(
    mock_figure_out_service_name, mock_list_services, mock_get_instance_configs_for_service,
):
    PaastaArgs = namedtuple('PaastaArgs', ['service', 'soa_dir', 'clusters', 'instances', 'deploy_group', 'owner'])
    args = PaastaArgs(
        service='fake_service',
        soa_dir='/fake/soa/dir',
        deploy_group='fake_deploy_group',
        clusters='cluster4',
        instances=None,
        owner=None,
    )
    mock_figure_out_service_name.return_value = 'fake_service'
    mock_list_services.return_value = ['fake_service']
    mock_get_instance_configs_for_service.return_value = [
        make_fake_instance_conf('cluster1', 'fake_service', 'instance1', 'fake_deploy_group'),
        make_fake_instance_conf('cluster1', 'fake_service', 'instance2', 'fake_deploy_group'),
        make_fake_instance_conf('cluster2', 'fake_service', 'instance3', 'fake_deploy_group'),
    ]

    assert len(apply_args_filters(args)) == 0


@patch('paasta_tools.cli.cmds.status.get_instance_configs_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.status.list_services', autospec=True)
@patch('paasta_tools.cli.cmds.status.figure_out_service_name', autospec=True)
def test_apply_args_filters_clusters_return_none_when_instance_not_in_deploy_group(
    mock_figure_out_service_name, mock_list_services, mock_get_instance_configs_for_service,
):
    PaastaArgs = namedtuple('PaastaArgs', ['service', 'soa_dir', 'clusters', 'instances', 'deploy_group', 'owner'])
    args = PaastaArgs(
        service='fake_service',
        soa_dir='/fake/soa/dir',
        deploy_group='fake_deploy_group',
        clusters=None,
        instances='instance5',
        owner=None,
    )
    mock_figure_out_service_name.return_value = 'fake_service'
    mock_list_services.return_value = ['fake_service']
    mock_get_instance_configs_for_service.return_value = [
        make_fake_instance_conf('cluster1', 'fake_service', 'instance1', 'other_fake_deploy_group'),
        make_fake_instance_conf('cluster1', 'fake_service', 'instance2', 'other_fake_deploy_group'),
        make_fake_instance_conf('cluster2', 'fake_service', 'instance3', 'other_fake_deploy_group'),
    ]

    assert len(apply_args_filters(args)) == 0


@patch('paasta_tools.cli.cmds.status.get_instance_configs_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.status.list_services', autospec=True)
@patch('paasta_tools.cli.cmds.status.figure_out_service_name', autospec=True)
def test_apply_args_filters_clusters_and_instances(
    mock_figure_out_service_name, mock_list_services, mock_get_instance_configs_for_service,
):
    PaastaArgs = namedtuple('PaastaArgs', ['service', 'soa_dir', 'clusters', 'instances', 'deploy_group', 'owner'])
    args = PaastaArgs(
        service='fake_service',
        soa_dir='/fake/soa/dir',
        deploy_group=None,
        clusters='cluster1',
        instances='instance1,instance3',
        owner=None,
    )
    mock_figure_out_service_name.return_value = 'fake_service'
    mock_list_services.return_value = ['fake_service']
    mock_get_instance_configs_for_service.return_value = [
        make_fake_instance_conf('cluster1', 'fake_service', 'instance1', 'fake_deploy_group'),
        make_fake_instance_conf('cluster1', 'fake_service', 'instance2', 'fake_deploy_group'),
        make_fake_instance_conf('cluster1', 'fake_service', 'instance3', 'fake_deploy_group'),
    ]

    pargs = apply_args_filters(args)
    assert sorted(pargs.keys()) == ['cluster1']
    assert pargs['cluster1']['fake_service'] == {'instance1', 'instance3'}


@patch('paasta_tools.cli.cmds.status.list_all_instances_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.status.get_instance_configs_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.status.list_services', autospec=True)
@patch('paasta_tools.cli.cmds.status.figure_out_service_name', autospec=True)
def test_apply_args_filters_no_instances_found(
    mock_figure_out_service_name, mock_list_services, mock_get_instance_configs_for_service,
    mock_list_all_instances_for_service, capfd,
):
    PaastaArgs = namedtuple('PaastaArgs', ['service', 'soa_dir', 'clusters', 'instances', 'deploy_group', 'owner'])
    args = PaastaArgs(
        service='fake_service',
        soa_dir='/fake/soa/dir',
        deploy_group=None,
        clusters='cluster1',
        instances='instance4,instance5',
        owner=None,
    )
    mock_figure_out_service_name.return_value = 'fake_service'
    mock_list_services.return_value = ['fake_service']
    mock_get_instance_configs_for_service.return_value = [
        make_fake_instance_conf('cluster1', 'fake_service', 'instance1', 'fake_deploy_group'),
        make_fake_instance_conf('cluster1', 'fake_service', 'instance2', 'fake_deploy_group'),
        make_fake_instance_conf('cluster1', 'fake_service', 'instance3', 'fake_deploy_group'),
    ]
    mock_list_all_instances_for_service.return_value = ['instance1', 'instance2', 'instance3']
    pargs = apply_args_filters(args)
    output, _ = capfd.readouterr()
    assert len(pargs.keys()) == 0
    assert "fake_service doesn't have any instances matching instance4, instance5 on cluster1." in output
    assert "Did you mean any of these?" in output
    for i in ["instance1", "instance2", "instance3"]:
        assert i in output


@patch('paasta_tools.cli.cmds.status.list_all_instances_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.status.paasta_print', autospec=True)
def test_verify_instances(mock_paasta_print, mock_list_all_instances_for_service):
    mock_list_all_instances_for_service.return_value = ['east', 'west', 'north']

    assert verify_instances('west,esst', 'fake_service', []) == ['west', 'esst']
    assert mock_paasta_print.called
    mock_paasta_print.assert_has_calls([
        call("\x1b[31mfake_service doesn't have any instances matching esst.\x1b[0m"),
        call("Did you mean any of these?"),
        call("  east"),
        call("  west"),
    ])


@patch('paasta_tools.cli.cmds.status.list_all_instances_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.status.paasta_print', autospec=True)
def test_verify_instances_with_clusters(mock_paasta_print, mock_list_all_instances_for_service):
    mock_list_all_instances_for_service.return_value = ['east', 'west', 'north']

    assert verify_instances(
        'west,esst,fake', 'fake_service',
        ['fake_cluster1', 'fake_cluster2'],
    ) == ['west', 'esst', 'fake']
    assert mock_paasta_print.called
    mock_paasta_print.assert_has_calls([
        call(
            "\x1b[31mfake_service doesn't have any instances matching esst,"
            " fake on fake_cluster1, fake_cluster2.\x1b[0m",
        ),
        call("Did you mean any of these?"),
        call("  east"),
        call("  west"),
    ])


@patch('paasta_tools.cli.cmds.status.get_instance_configs_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.status.list_services', autospec=True)
@patch('paasta_tools.cli.cmds.status.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.status.get_actual_deployments', autospec=True)
@patch('paasta_tools.cli.cmds.status.load_system_paasta_config', autospec=True)
@patch('paasta_tools.cli.cmds.status.report_status_for_cluster', autospec=True)
def test_status_with_owner(
        mock_report_status, mock_load_system_paasta_config, mock_get_actual_deployments,
        mock_figure_out_service_name, mock_list_services,
        mock_get_instance_configs_for_service,
        system_paasta_config,
):
    mock_load_system_paasta_config.return_value = system_paasta_config
    mock_list_services.return_value = ['fakeservice', 'otherservice']
    cluster = 'fake_cluster'
    mock_inst_1 = make_fake_instance_conf(cluster, 'fakeservice', 'instance1', team='faketeam')
    mock_inst_2 = make_fake_instance_conf(cluster, 'otherservice', 'instance3', team='faketeam')
    mock_get_instance_configs_for_service.return_value = [
        mock_inst_1,
        mock_inst_2,
    ]

    mock_get_actual_deployments.return_value = {
        'fakeservice.instance1': 'sha1',
        'fakeservice.instance2': 'sha2',
        'otherservice.instance3': 'sha3',
        'otherservice.instance1': 'sha4',
    }
    mock_report_status.return_value = 0, ['dummy', 'output']

    args = MagicMock()
    args.service = None
    args.instances = None
    args.clusters = None
    args.deploy_group = None
    args.owner = 'faketeam'
    args.soa_dir = '/fake/soa/dir'
    return_value = paasta_status(args)

    assert return_value == 0
    assert mock_report_status.call_count == 2
