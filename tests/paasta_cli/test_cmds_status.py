# Copyright 2015 Yelp Inc.
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

from mock import patch
from mock import Mock
from mock import MagicMock
from pytest import raises
from StringIO import StringIO

from paasta_tools import utils
from paasta_tools.paasta_cli.utils import \
    NoSuchService, PaastaColors, PaastaCheckMessages
from paasta_tools.paasta_cli.cmds.status import paasta_status, \
    missing_deployments_message
from paasta_tools.paasta_cli.cmds.status import report_bogus_filters
from paasta_tools.paasta_cli.cmds.status import report_status
from paasta_tools.paasta_cli.cmds import status


@patch('paasta_tools.paasta_cli.utils.validate_service_name', autospec=True)
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


@patch('paasta_tools.paasta_cli.utils.validate_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.utils.guess_service_name', autospec=True)
@patch('sys.stdout', new_callable=StringIO)
def test_status_arg_service_not_found(mock_stdout, mock_guess_service_name,
                                      mock_validate_service_name):
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

    output = mock_stdout.getvalue()
    assert sys_exit.value.code == 1
    assert output == expected_output


@patch('paasta_tools.paasta_cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.report_bogus_filters', autospec=True)
@patch('sys.stdout', new_callable=StringIO)
def test_report_status_for_cluster_displays_deployed_service(
    mock_stdout,
    mock_report_bogus_filters,
    mock_execute_paasta_serviceinit_on_remote_master,
):
    # paasta_status with no args displays deploy info - vanilla case
    service = 'fake_service'
    planned_deployments = ['cluster.instance']
    actual_deployments = {
        'cluster.instance': 'sha'
    }
    instance_filter = None
    fake_status = 'status: SOMETHING FAKE'
    mock_execute_paasta_serviceinit_on_remote_master.return_value = fake_status
    expected_output = (
        "\n"
        "cluster: cluster\n"
        "  instance: %s\n"
        "    Git sha:    sha\n"
        "    %s\n"
        % (
            PaastaColors.blue('instance'),
            fake_status,
        )
    )

    status.report_status_for_cluster(
        service=service,
        cluster='cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_filter=instance_filter,
    )
    output = mock_stdout.getvalue()
    assert expected_output in output


@patch('paasta_tools.paasta_cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.report_bogus_filters', autospec=True)
@patch('sys.stdout', new_callable=StringIO)
def test_report_status_for_cluster_displays_multiple_lines_from_execute_paasta_serviceinit_on_remote_master(
    mock_stdout,
    mock_report_bogus_filters,
    mock_execute_paasta_serviceinit_on_remote_master,
):
    # paasta_status with no args displays deploy info - vanilla case
    service = 'fake_service'
    planned_deployments = ['cluster.instance']
    actual_deployments = {
        'cluster.instance': 'this_is_a_sha'
    }
    instance_filter = None
    fake_status = 'status: SOMETHING FAKE\nand then something fake\non another line!\n\n\n'
    mock_execute_paasta_serviceinit_on_remote_master.return_value = fake_status
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
        instance_filter=instance_filter,
    )
    output = mock_stdout.getvalue()
    assert expected_output in output


@patch('paasta_tools.paasta_cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.report_bogus_filters', autospec=True)
@patch('sys.stdout', new_callable=StringIO)
def test_report_status_for_cluster_instance_sorts_in_deploy_order(
    mock_stdout,
    mock_report_bogus_filters,
    mock_execute_paasta_serviceinit_on_remote_master,
):
    # paasta_status with no args displays deploy info
    service = 'fake_service'
    planned_deployments = [
        'a_cluster.a_instance',
        'a_cluster.b_instance',
    ]
    actual_deployments = {
        'a_cluster.a_instance': '533976a9',
        'a_cluster.b_instance': '533976a9',
    }
    instance_filter = None
    fake_status = 'status: SOMETHING FAKE'
    mock_execute_paasta_serviceinit_on_remote_master.return_value = fake_status
    expected_output = (
        "\n"
        "cluster: a_cluster\n"
        "  instance: %s\n"
        "    Git sha:    533976a9\n"
        "    %s\n"
        "  instance: %s\n"
        "    Git sha:    533976a9\n"
        "    %s\n"
        % (
            PaastaColors.blue('a_instance'),
            fake_status,
            PaastaColors.blue('b_instance'),
            fake_status,
        )
    )

    status.report_status_for_cluster(
        service=service,
        cluster='a_cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_filter=instance_filter,
    )
    output = mock_stdout.getvalue()
    assert expected_output in output


@patch('paasta_tools.paasta_cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.report_bogus_filters', autospec=True)
@patch('sys.stdout', new_callable=StringIO)
def test_print_cluster_status_missing_deploys_in_red(
    mock_stdout,
    mock_report_bogus_filters,
    mock_execute_paasta_serviceinit_on_remote_master,
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
    instance_filter = None
    fake_status = 'status: SOMETHING FAKE'
    mock_execute_paasta_serviceinit_on_remote_master.return_value = fake_status
    expected_output = (
        "\n"
        "cluster: a_cluster\n"
        "  instance: %s\n"
        "    Git sha:    533976a9\n"
        "    %s\n"
        "  instance: %s\n"
        "    Git sha:    None\n"
        % (
            PaastaColors.blue('a_instance'),
            fake_status,
            PaastaColors.red('b_instance'),
        )
    )

    status.report_status_for_cluster(
        service=service,
        cluster='a_cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_filter=instance_filter,
    )
    output = mock_stdout.getvalue()
    assert expected_output in output


@patch('paasta_tools.paasta_cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.report_bogus_filters', autospec=True)
@patch('sys.stdout', new_callable=StringIO)
def test_print_cluster_status_calls_execute_paasta_serviceinit_on_remote_master(
    mock_stdout,
    mock_report_bogus_filters,
    mock_execute_paasta_serviceinit_on_remote_master,
):
    service = 'fake_service'
    planned_deployments = [
        'a_cluster.a_instance',
        'a_cluster.b_instance',
    ]
    actual_deployments = {
        'a_cluster.a_instance': 'this_is_a_sha',
    }
    instance_filter = None
    fake_output = "Marathon: 5 instances"
    mock_execute_paasta_serviceinit_on_remote_master.return_value = fake_output
    expected_output = "    %s\n" % fake_output

    status.report_status_for_cluster(
        service=service,
        cluster='a_cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_filter=instance_filter,
    )
    assert mock_execute_paasta_serviceinit_on_remote_master.call_count == 1
    mock_execute_paasta_serviceinit_on_remote_master.assert_any_call(
        'status', 'a_cluster', service, 'a_instance', verbose=False)

    output = mock_stdout.getvalue()
    assert expected_output in output


@patch('paasta_tools.paasta_cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.report_bogus_filters', autospec=True)
@patch('sys.stdout', new_callable=StringIO)
def test_report_status_for_cluster_obeys_instance_filter(
    mock_stdout,
    mock_report_bogus_filters,
    mock_execute_paasta_serviceinit_on_remote_master,
):
    service = 'fake_service'
    planned_deployments = ['cluster.instance1', 'cluster.instance2']
    actual_deployments = {
        'cluster.instance1': 'sha',
        'cluster.instance2': 'sha',
    }
    instance_filter = ['instance1']

    status.report_status_for_cluster(
        service=service,
        cluster='cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_filter=instance_filter,
    )
    output = mock_stdout.getvalue()
    assert 'instance1' in output
    assert 'instance2' not in output


@patch('paasta_tools.paasta_cli.cmds.status.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.report_bogus_filters', autospec=True)
@patch('sys.stdout', new_callable=StringIO)
def test_report_status_calls_report_bogus_filters(
    mock_stdout,
    mock_report_bogus_filters,
    mock_execute_paasta_serviceinit_on_remote_master,
):
    service = 'fake_service'
    planned_deployments = ['cluster.instance1', 'cluster.instance2']
    actual_deployments = {}
    instance_filter = None

    status.report_status_for_cluster(
        service=service,
        cluster='cluster',
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_filter=instance_filter,
    )
    mock_report_bogus_filters.assert_called_once_with(instance_filter, ['instance1', 'instance2'], 'instance')


@patch('paasta_tools.paasta_cli.cmds.status.figure_out_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.get_deploy_info', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.get_actual_deployments', autospec=True)
@patch('sys.stdout', new_callable=StringIO)
def test_status_pending_pipeline_build_message(
        mock_stdout, mock_get_actual_deployments, mock_get_deploy_info,
        mock_figure_out_service_name):
    # If deployments.json is missing SERVICE, output the appropriate message
    service = 'fake_service'
    mock_figure_out_service_name.return_value = service
    pipeline = [{'instancename': 'cluster.instance'}]
    mock_get_deploy_info.return_value = {'pipeline': pipeline}

    actual_deployments = {}
    mock_get_actual_deployments.return_value = actual_deployments
    expected_output = missing_deployments_message(service)

    args = MagicMock()
    args.service = service

    paasta_status(args)
    output = mock_stdout.getvalue()
    assert expected_output in output


@patch('paasta_tools.paasta_cli.cmds.status.load_deployments_json', autospec=True)
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


@patch('paasta_tools.paasta_cli.cmds.status.figure_out_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.get_deploy_info', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.get_actual_deployments', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.get_planned_deployments', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.report_status', autospec=True)
@patch('sys.stdout', new_callable=StringIO)
def test_status_calls_sergeants(
    mock_stdout,
    mock_report_status,
    mock_get_planned_deployments,
    mock_get_actual_deployments,
    mock_get_deploy_info,
    mock_figure_out_service_name,
):
    service = 'fake_service'
    mock_figure_out_service_name.return_value = service

    pipeline = [{'instancename': 'cluster.instance'}]
    deploy_info = {'pipeline': pipeline}
    planned_deployments = [
        'cluster1.instance1', 'cluster1.instance2', 'cluster2.instance1']
    mock_get_deploy_info.return_value = deploy_info
    mock_get_planned_deployments.return_value = planned_deployments

    actual_deployments = {
        'fake_service:paasta-cluster.instance': 'this_is_a_sha'
    }
    mock_get_actual_deployments.return_value = actual_deployments

    args = MagicMock()
    args.service = service
    args.clusters = None
    args.instances = None
    args.verbose = False
    paasta_status(args)

    mock_figure_out_service_name.assert_called_once_with(args)
    mock_get_actual_deployments.assert_called_once_with(service)
    mock_get_deploy_info.assert_called_once_with(service)
    mock_report_status.assert_called_once_with(
        service=service,
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        cluster_filter=[],
        instance_filter=[],
        verbose=False,
    )


def test_report_bogus_filters_no_filters():
    filters = []
    items = ['cluster1', 'cluster2', 'cluster3']
    item_type = 'thingy'
    actual = report_bogus_filters(filters, items, item_type)
    assert actual == ''


def test_report_bogus_filters_with_filters():
    filters = ['bogus1', 'cluster1']
    items = ['cluster1', 'cluster2', 'cluster3']
    item_type = 'thingy'
    actual = report_bogus_filters(filters, items, item_type)
    assert 'Warning' in actual
    assert item_type in actual
    assert 'bogus1' in actual


@patch('paasta_tools.paasta_cli.cmds.status.report_status_for_cluster', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.report_bogus_filters', autospec=True)
@patch('sys.stdout', new_callable=StringIO)
def test_report_status_obeys_cluster_filter(
    mock_stdout,
    mock_report_bogus_filters,
    mock_report_status_for_cluster,
):
    service = 'fake_service'
    cluster_filter = ['cluster1']
    instance_filter = None
    deploy_pipeline = actual_deployments = [
        'cluster1.main', 'cluster2.main', 'cluster3.main']
    report_status(
        service=service,
        deploy_pipeline=deploy_pipeline,
        actual_deployments=actual_deployments,
        cluster_filter=cluster_filter,
        instance_filter=instance_filter,
    )
    mock_report_bogus_filters.assert_called_once_with(
        cluster_filter, ['cluster1', 'cluster2', 'cluster3'], 'cluster')
    mock_report_status_for_cluster.assert_called_once_with(
        service=service,
        cluster='cluster1',
        deploy_pipeline=deploy_pipeline,
        actual_deployments=actual_deployments,
        instance_filter=instance_filter,
        verbose=False
    )


@patch('paasta_tools.paasta_cli.cmds.status.report_status_for_cluster', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.status.report_bogus_filters', autospec=True)
@patch('sys.stdout', new_callable=StringIO)
def test_report_status_handle_none_filter(
    mock_stdout,
    mock_report_bogus_filters,
    mock_report_status_for_cluster,
):
    service = 'fake_service'
    cluster_filter = None
    instance_filter = None
    deploy_pipeline = actual_deployments = [
        'cluster1.main', 'cluster2.main', 'cluster3.main']
    report_status(
        service=service,
        deploy_pipeline=deploy_pipeline,
        actual_deployments=actual_deployments,
        cluster_filter=cluster_filter,
        instance_filter=instance_filter,
    )
    mock_report_status_for_cluster.assert_any_call(
        service=service,
        cluster='cluster1',
        deploy_pipeline=deploy_pipeline,
        actual_deployments=actual_deployments,
        instance_filter=instance_filter,
        verbose=False
    )
    mock_report_status_for_cluster.assert_any_call(
        service=service,
        cluster='cluster2',
        deploy_pipeline=deploy_pipeline,
        actual_deployments=actual_deployments,
        instance_filter=instance_filter,
        verbose=False
    )
    mock_report_status_for_cluster.assert_any_call(
        service=service,
        cluster='cluster3',
        deploy_pipeline=deploy_pipeline,
        actual_deployments=actual_deployments,
        instance_filter=instance_filter,
        verbose=False
    )
