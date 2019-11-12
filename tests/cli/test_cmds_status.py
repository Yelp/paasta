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
from typing import Any
from typing import Dict
from typing import Set

import pytest
from bravado.exception import HTTPError
from bravado.requests_client import RequestsResponseAdapter
from mock import ANY
from mock import call
from mock import MagicMock
from mock import Mock
from mock import patch

from paasta_tools import marathon_tools
from paasta_tools import utils
from paasta_tools.cli.cmds import status
from paasta_tools.cli.cmds.status import apply_args_filters
from paasta_tools.cli.cmds.status import build_smartstack_backends_table
from paasta_tools.cli.cmds.status import create_autoscaling_info_table
from paasta_tools.cli.cmds.status import create_mesos_non_running_tasks_table
from paasta_tools.cli.cmds.status import create_mesos_running_tasks_table
from paasta_tools.cli.cmds.status import desired_state_human
from paasta_tools.cli.cmds.status import format_kubernetes_pod_table
from paasta_tools.cli.cmds.status import format_kubernetes_replicaset_table
from paasta_tools.cli.cmds.status import format_marathon_task_table
from paasta_tools.cli.cmds.status import get_smartstack_status_human
from paasta_tools.cli.cmds.status import haproxy_backend_report
from paasta_tools.cli.cmds.status import marathon_app_status_human
from paasta_tools.cli.cmds.status import marathon_mesos_status_human
from paasta_tools.cli.cmds.status import marathon_mesos_status_summary
from paasta_tools.cli.cmds.status import missing_deployments_message
from paasta_tools.cli.cmds.status import paasta_status
from paasta_tools.cli.cmds.status import paasta_status_on_api_endpoint
from paasta_tools.cli.cmds.status import print_kubernetes_status
from paasta_tools.cli.cmds.status import print_marathon_status
from paasta_tools.cli.cmds.status import report_invalid_whitelist_values
from paasta_tools.cli.cmds.status import verify_instances
from paasta_tools.cli.utils import NoSuchService
from paasta_tools.cli.utils import PaastaColors
from paasta_tools.utils import remove_ansi_escape_sequences


def make_fake_instance_conf(
    cluster, service, instance, deploy_group=None, team=None, registrations=()
):
    conf = MagicMock()
    conf.get_cluster.return_value = cluster
    conf.get_service.return_value = service
    conf.get_instance.return_value = instance
    conf.get_deploy_group.return_value = deploy_group
    conf.get_team.return_value = team
    conf.get_registrations.return_value = registrations if registrations else []
    if registrations is None:
        del (
            conf.get_registrations
        )  # http://www.voidspace.org.uk/python/mock/mock.html#deleting-attributes
    return conf


@patch("paasta_tools.cli.utils.validate_service_name", autospec=True)
def test_figure_out_service_name_not_found(mock_validate_service_name, capfd):
    # paasta_status with invalid -s service_name arg results in error
    mock_validate_service_name.side_effect = NoSuchService(None)
    parsed_args = Mock()
    parsed_args.service = "fake_service"

    expected_output = "%s\n" % NoSuchService.GUESS_ERROR_MSG

    # Fail if exit(1) does not get called
    with pytest.raises(SystemExit) as sys_exit:
        status.figure_out_service_name(parsed_args)

    output, _ = capfd.readouterr()
    assert sys_exit.value.code == 1
    assert output == expected_output


@patch("paasta_tools.cli.cmds.status.list_clusters", autospec=True)
@patch("paasta_tools.cli.cmds.status.load_system_paasta_config", autospec=True)
@patch("paasta_tools.cli.utils.validate_service_name", autospec=True)
@patch("paasta_tools.cli.utils.guess_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_services", autospec=True)
def test_status_arg_service_not_found(
    mock_list_services,
    mock_guess_service_name,
    mock_validate_service_name,
    mock_load_system_paasta_config,
    mock_list_clusters,
    capfd,
    system_paasta_config,
):
    # paasta_status with no args and non-service directory results in error
    mock_list_services.return_value = []
    mock_guess_service_name.return_value = "not_a_service"
    error = NoSuchService("fake_service")
    mock_validate_service_name.side_effect = error
    mock_list_clusters.return_value = ["cluster1"]
    mock_load_system_paasta_config.return_value = system_paasta_config
    expected_output = str(error) + "\n"

    args = MagicMock()
    args.service = None
    args.owner = None
    args.clusters = None
    args.instances = None
    args.deploy_group = None
    args.registration = None

    # Fail if exit(1) does not get called
    with pytest.raises(SystemExit) as sys_exit:
        paasta_status(args)

    output, _ = capfd.readouterr()
    assert sys_exit.value.code == 1
    assert output == expected_output


@patch("paasta_tools.cli.cmds.status.paasta_status_on_api_endpoint", autospec=True)
@patch("paasta_tools.cli.cmds.status.report_invalid_whitelist_values", autospec=True)
def test_report_status_calls_report_invalid_whitelist_values(
    mock_report_invalid_whitelist_values,
    mock_paasta_status_on_api_endpoint,
    system_paasta_config,
):
    service = "fake_service"
    planned_deployments = ["cluster.instance1", "cluster.instance2"]
    actual_deployments: Dict[str, str] = {}
    instance_whitelist: Dict[str, Any] = {}

    status.report_status_for_cluster(
        service=service,
        cluster="cluster",
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        instance_whitelist=instance_whitelist,
        system_paasta_config=system_paasta_config,
    )
    mock_report_invalid_whitelist_values.assert_called_once_with(
        [], ["instance1", "instance2"], "instance"
    )


@patch("paasta_tools.cli.cmds.status.get_instance_configs_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_services", autospec=True)
@patch("paasta_tools.cli.cmds.status.load_system_paasta_config", autospec=True)
@patch("paasta_tools.cli.cmds.status.figure_out_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.status.get_deploy_info", autospec=True)
@patch("paasta_tools.cli.cmds.status.get_actual_deployments", autospec=True)
@patch("paasta_tools.cli.cmds.status.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_clusters", autospec=True)
def test_status_pending_pipeline_build_message(
    mock_list_clusters,
    mock_validate_service_name,
    mock_get_actual_deployments,
    mock_get_deploy_info,
    mock_figure_out_service_name,
    mock_load_system_paasta_config,
    mock_list_services,
    mock_get_instance_configs_for_service,
    capfd,
    system_paasta_config,
):
    # If deployments.json is missing SERVICE, output the appropriate message
    service = "fake_service"
    mock_list_clusters.return_value = ["cluster"]
    mock_validate_service_name.return_value = None
    mock_figure_out_service_name.return_value = service
    mock_list_services.return_value = [service]
    pipeline = [{"instancename": "cluster.instance"}]
    mock_get_deploy_info.return_value = {"pipeline": pipeline}
    mock_load_system_paasta_config.return_value = system_paasta_config
    mock_instance_config = make_fake_instance_conf("cluster", service, "instancename")
    mock_get_instance_configs_for_service.return_value = [mock_instance_config]

    actual_deployments: Dict[str, str] = {}
    mock_get_actual_deployments.return_value = actual_deployments
    expected_output = missing_deployments_message(service)

    args = MagicMock()
    args.service = service
    args.deploy_group = None
    args.clusters = None
    args.instances = None
    args.owner = None
    args.soa_dir = utils.DEFAULT_SOA_DIR
    args.registration = None

    paasta_status(args)
    output, _ = capfd.readouterr()
    assert expected_output in output


@patch("paasta_tools.cli.cmds.status.load_deployments_json", autospec=True)
def test_get_actual_deployments(mock_get_deployments,):
    mock_get_deployments.return_value = utils.DeploymentsJsonV1(
        {
            "fake_service:paasta-b_cluster.b_instance": {
                "docker_image": "this_is_a_sha"
            },
            "fake_service:paasta-a_cluster.a_instance": {
                "docker_image": "this_is_a_sha"
            },
        }
    )
    expected = {
        "a_cluster.a_instance": "this_is_a_sha",
        "b_cluster.b_instance": "this_is_a_sha",
    }

    actual = status.get_actual_deployments("fake_service", "/fake/soa/dir")
    assert expected == actual


@patch("paasta_tools.cli.cmds.status.read_deploy", autospec=True)
def test_get_deploy_info_exists(mock_read_deploy):
    expected = "fake deploy yaml"
    mock_read_deploy.return_value = expected
    actual = status.get_deploy_info("fake_service")
    assert expected == actual


@patch("paasta_tools.cli.cmds.status.read_deploy", autospec=True)
def test_get_deploy_info_does_not_exist(mock_read_deploy, capfd):
    mock_read_deploy.return_value = False
    with pytest.raises(SystemExit) as sys_exit:
        status.get_deploy_info("fake_service")
    output, _ = capfd.readouterr()
    assert sys_exit.value.code == 1
    assert output.startswith("Error encountered with")


@patch("paasta_tools.cli.cmds.status.get_instance_configs_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_services", autospec=True)
@patch("paasta_tools.cli.cmds.status.load_system_paasta_config", autospec=True)
@patch("paasta_tools.cli.cmds.status.figure_out_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.status.get_actual_deployments", autospec=True)
@patch("paasta_tools.cli.cmds.status.get_planned_deployments", autospec=True)
@patch("paasta_tools.cli.cmds.status.report_status_for_cluster", autospec=True)
@patch("paasta_tools.cli.cmds.status.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_clusters", autospec=True)
def test_status_calls_sergeants(
    mock_list_clusters,
    mock_validate_service_name,
    mock_report_status,
    mock_get_planned_deployments,
    mock_get_actual_deployments,
    mock_figure_out_service_name,
    mock_load_system_paasta_config,
    mock_list_services,
    mock_get_instance_configs_for_service,
    system_paasta_config,
):
    service = "fake_service"
    cluster = "fake_cluster"
    mock_list_clusters.return_value = ["cluster1", "cluster2", "fake_cluster"]
    mock_validate_service_name.return_value = None
    mock_figure_out_service_name.return_value = service
    mock_list_services.return_value = [service]

    mock_instance_config = make_fake_instance_conf(cluster, service, "fi")
    mock_instance_config.get_service.return_value = service
    mock_instance_config.get_cluster.return_value = cluster
    mock_get_instance_configs_for_service.return_value = [mock_instance_config]

    planned_deployments = [
        "cluster1.instance1",
        "cluster1.instance2",
        "cluster2.instance1",
    ]
    mock_get_planned_deployments.return_value = planned_deployments

    actual_deployments = {"fake_service:paasta-cluster.instance": "this_is_a_sha"}
    mock_get_actual_deployments.return_value = actual_deployments
    mock_load_system_paasta_config.return_value = system_paasta_config
    mock_report_status.return_value = 1776, ["dummy", "output"]

    args = MagicMock()
    args.service = service
    args.clusters = None
    args.instances = None
    args.verbose = False
    args.owner = None
    args.deploy_group = None
    args.soa_dir = "/fake/soa/dir"
    args.registration = None
    return_value = paasta_status(args)

    assert return_value == 1776

    mock_get_actual_deployments.assert_called_once_with(service, "/fake/soa/dir")
    mock_report_status.assert_called_once_with(
        service=service,
        deploy_pipeline=planned_deployments,
        actual_deployments=actual_deployments,
        cluster=cluster,
        instance_whitelist={"fi": mock_instance_config.__class__},
        system_paasta_config=system_paasta_config,
        verbose=False,
    )


def test_report_invalid_whitelist_values_no_whitelists():
    whitelist: Set[str] = set()
    items = ["cluster1", "cluster2", "cluster3"]
    item_type = "thingy"
    actual = report_invalid_whitelist_values(whitelist, items, item_type)
    assert actual == ""


def test_report_invalid_whitelist_values_with_whitelists():
    whitelist = {"bogus1", "cluster1"}
    items = ["cluster1", "cluster2", "cluster3"]
    item_type = "thingy"
    actual = report_invalid_whitelist_values(whitelist, items, item_type)
    assert "Warning" in actual
    assert item_type in actual
    assert "bogus1" in actual


StatusArgs = namedtuple(
    "StatusArgs",
    [
        "service",
        "soa_dir",
        "clusters",
        "instances",
        "deploy_group",
        "owner",
        "registration",
        "verbose",
    ],
)


@patch("paasta_tools.cli.cmds.status.get_instance_configs_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_services", autospec=True)
@patch("paasta_tools.cli.cmds.status.figure_out_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.status.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_clusters", autospec=True)
def test_apply_args_filters_clusters_and_instances_clusters_instances_deploy_group(
    mock_list_clusters,
    mock_validate_service_name,
    mock_figure_out_service_name,
    mock_list_services,
    mock_get_instance_configs_for_service,
):
    args = StatusArgs(
        service="fake_service",
        soa_dir="/fake/soa/dir",
        deploy_group="fake_deploy_group",
        clusters="cluster1",
        instances="instance1,instance3",
        owner=None,
        registration=None,
        verbose=False,
    )
    mock_list_clusters.return_value = ["cluster1", "cluster2"]
    mock_validate_service_name.return_value = None
    mock_figure_out_service_name.return_value = "fake_service"
    mock_list_services.return_value = ["fake_service"]
    mock_inst1 = make_fake_instance_conf(
        "cluster1", "fake_service", "instance1", "fake_deploy_group"
    )
    mock_inst2 = make_fake_instance_conf(
        "cluster1", "fake_service", "instance2", "fake_deploy_group"
    )
    mock_inst3 = make_fake_instance_conf(
        "cluster2", "fake_service", "instance3", "fake_deploy_group"
    )
    mock_get_instance_configs_for_service.return_value = [
        mock_inst1,
        mock_inst2,
        mock_inst3,
    ]

    pargs = apply_args_filters(args)
    assert sorted(pargs.keys()) == ["cluster1"]
    assert pargs["cluster1"]["fake_service"] == {"instance1": mock_inst1.__class__}


@patch("paasta_tools.cli.cmds.status.get_instance_configs_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_services", autospec=True)
@patch("paasta_tools.cli.cmds.status.figure_out_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.status.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_clusters", autospec=True)
def test_apply_args_filters_clusters_uses_deploy_group_when_no_clusters_and_instances(
    mock_list_clusters,
    mock_validate_service_name,
    mock_figure_out_service_name,
    mock_list_services,
    mock_get_instance_configs_for_service,
):
    args = StatusArgs(
        service="fake_service",
        soa_dir="/fake/soa/dir",
        deploy_group="fake_deploy_group",
        clusters=None,
        instances=None,
        owner=None,
        registration=None,
        verbose=False,
    )
    mock_list_clusters.return_value = ["cluster1", "cluster2"]
    mock_validate_service_name.return_value = None
    mock_figure_out_service_name.return_value = "fake_service"
    mock_list_services.return_value = ["fake_service"]
    mock_inst1 = make_fake_instance_conf(
        "cluster1", "fake_service", "instance1", "fake_deploy_group"
    )
    mock_inst2 = make_fake_instance_conf(
        "cluster1", "fake_service", "instance2", "fake_deploy_group"
    )
    mock_inst3 = make_fake_instance_conf(
        "cluster2", "fake_service", "instance3", "fake_deploy_group"
    )
    mock_get_instance_configs_for_service.return_value = [
        mock_inst1,
        mock_inst2,
        mock_inst3,
    ]

    pargs = apply_args_filters(args)
    assert sorted(pargs.keys()) == ["cluster1", "cluster2"]
    assert pargs["cluster1"]["fake_service"] == {
        "instance1": mock_inst1.__class__,
        "instance2": mock_inst2.__class__,
    }
    assert pargs["cluster2"]["fake_service"] == {"instance3": mock_inst3.__class__}


@patch("paasta_tools.cli.cmds.status.get_instance_configs_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_services", autospec=True)
@patch("paasta_tools.cli.cmds.status.figure_out_service_name", autospec=True)
def test_apply_args_filters_clusters_return_none_when_cluster_not_in_deploy_group(
    mock_figure_out_service_name,
    mock_list_services,
    mock_get_instance_configs_for_service,
):
    args = StatusArgs(
        service="fake_service",
        soa_dir="/fake/soa/dir",
        deploy_group="fake_deploy_group",
        clusters="cluster4",
        instances=None,
        owner=None,
        registration=None,
        verbose=False,
    )
    mock_figure_out_service_name.return_value = "fake_service"
    mock_list_services.return_value = ["fake_service"]
    mock_get_instance_configs_for_service.return_value = [
        make_fake_instance_conf(
            "cluster1", "fake_service", "instance1", "fake_deploy_group"
        ),
        make_fake_instance_conf(
            "cluster1", "fake_service", "instance2", "fake_deploy_group"
        ),
        make_fake_instance_conf(
            "cluster2", "fake_service", "instance3", "fake_deploy_group"
        ),
    ]

    assert len(apply_args_filters(args)) == 0


@patch("paasta_tools.cli.cmds.status.get_instance_configs_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_services", autospec=True)
@patch("paasta_tools.cli.cmds.status.figure_out_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_clusters", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_all_instances_for_service", autospec=True)
def test_apply_args_filters_clusters_return_none_when_instance_not_in_deploy_group(
    mock_list_clusters,
    mock_figure_out_service_name,
    mock_list_services,
    mock_get_instance_configs_for_service,
    mock_list_all_instances_for_service,
):
    args = StatusArgs(
        service="fake_service",
        soa_dir="/fake/soa/dir",
        deploy_group="fake_deploy_group",
        clusters=None,
        instances="instance5",
        owner=None,
        registration=None,
        verbose=False,
    )
    mock_list_clusters.return_value = ["cluster1", "cluster2"]
    mock_figure_out_service_name.return_value = "fake_service"
    mock_list_services.return_value = ["fake_service"]
    mock_list_all_instances_for_service.return_value = []
    mock_get_instance_configs_for_service.return_value = [
        make_fake_instance_conf(
            "cluster1", "fake_service", "instance1", "other_fake_deploy_group"
        ),
        make_fake_instance_conf(
            "cluster1", "fake_service", "instance2", "other_fake_deploy_group"
        ),
        make_fake_instance_conf(
            "cluster2", "fake_service", "instance3", "other_fake_deploy_group"
        ),
    ]

    assert len(apply_args_filters(args)) == 0


@patch("paasta_tools.cli.cmds.status.get_instance_configs_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_services", autospec=True)
@patch("paasta_tools.cli.cmds.status.figure_out_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.status.validate_service_name", autospec=True)
def test_apply_args_filters_clusters_and_instances(
    mock_validate_service_name,
    mock_figure_out_service_name,
    mock_list_services,
    mock_get_instance_configs_for_service,
):
    args = StatusArgs(
        service="fake_service",
        soa_dir="/fake/soa/dir",
        deploy_group=None,
        clusters="cluster1",
        instances="instance1,instance3",
        owner=None,
        registration=None,
        verbose=False,
    )
    mock_validate_service_name.return_value = None
    mock_figure_out_service_name.return_value = "fake_service"
    mock_list_services.return_value = ["fake_service"]
    mock_inst1 = make_fake_instance_conf(
        "cluster1", "fake_service", "instance1", "fake_deploy_group"
    )
    mock_inst2 = make_fake_instance_conf(
        "cluster1", "fake_service", "instance2", "fake_deploy_group"
    )
    mock_inst3 = make_fake_instance_conf(
        "cluster1", "fake_service", "instance3", "fake_deploy_group"
    )
    mock_get_instance_configs_for_service.return_value = [
        mock_inst1,
        mock_inst2,
        mock_inst3,
    ]

    pargs = apply_args_filters(args)
    assert sorted(pargs.keys()) == ["cluster1"]
    assert pargs["cluster1"]["fake_service"] == {
        "instance1": mock_inst1.__class__,
        "instance3": mock_inst3.__class__,
    }


@patch("paasta_tools.cli.cmds.status.list_services", autospec=True)
def test_apply_args_filters_bad_service_name(mock_list_services, capfd):
    args = StatusArgs(
        service="fake-service",
        soa_dir="/fake/soa/dir",
        deploy_group=None,
        clusters="cluster1",
        instances="instance4,instance5",
        owner=None,
        registration=None,
        verbose=False,
    )
    mock_list_services.return_value = ["fake_service"]
    pargs = apply_args_filters(args)
    output, _ = capfd.readouterr()
    assert len(pargs) == 0
    assert 'The service "fake-service" does not exist.' in output
    assert "Did you mean any of these?" in output
    assert "  fake_service" in output


@patch("paasta_tools.cli.cmds.status.list_all_instances_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.status.get_instance_configs_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_services", autospec=True)
@patch("paasta_tools.cli.cmds.status.figure_out_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.status.validate_service_name", autospec=True)
def test_apply_args_filters_no_instances_found(
    mock_validate_service_name,
    mock_figure_out_service_name,
    mock_list_services,
    mock_get_instance_configs_for_service,
    mock_list_all_instances_for_service,
    capfd,
):
    args = StatusArgs(
        service="fake_service",
        soa_dir="/fake/soa/dir",
        deploy_group=None,
        clusters="cluster1",
        instances="instance4,instance5",
        owner=None,
        registration=None,
        verbose=False,
    )
    mock_validate_service_name.return_value = None
    mock_figure_out_service_name.return_value = "fake_service"
    mock_list_services.return_value = ["fake_service"]
    mock_get_instance_configs_for_service.return_value = [
        make_fake_instance_conf(
            "cluster1", "fake_service", "instance1", "fake_deploy_group"
        ),
        make_fake_instance_conf(
            "cluster1", "fake_service", "instance2", "fake_deploy_group"
        ),
        make_fake_instance_conf(
            "cluster1", "fake_service", "instance3", "fake_deploy_group"
        ),
    ]
    mock_list_all_instances_for_service.return_value = [
        "instance1",
        "instance2",
        "instance3",
    ]
    pargs = apply_args_filters(args)
    output, _ = capfd.readouterr()
    assert len(pargs.keys()) == 0
    assert (
        "fake_service doesn't have any instances matching instance4, instance5 on cluster1."
        in output
    )
    assert "Did you mean any of these?" in output
    for i in ["instance1", "instance2", "instance3"]:
        assert i in output


@patch("paasta_tools.cli.cmds.status.list_all_instances_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.status.paasta_print", autospec=True)
def test_verify_instances(mock_paasta_print, mock_list_all_instances_for_service):
    mock_list_all_instances_for_service.return_value = ["east", "west", "north"]

    assert verify_instances("west,esst", "fake_service", []) == ["west", "esst"]
    assert mock_paasta_print.called
    mock_paasta_print.assert_has_calls(
        [
            call(
                "\x1b[31mfake_service doesn't have any instances matching esst.\x1b[0m"
            ),
            call("Did you mean any of these?"),
            call("  east"),
            call("  west"),
        ]
    )


@patch("paasta_tools.cli.cmds.status.list_all_instances_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.status.paasta_print", autospec=True)
def test_verify_instances_with_clusters(
    mock_paasta_print, mock_list_all_instances_for_service
):
    mock_list_all_instances_for_service.return_value = ["east", "west", "north"]

    assert verify_instances(
        "west,esst,fake", "fake_service", ["fake_cluster1", "fake_cluster2"]
    ) == ["west", "esst", "fake"]
    assert mock_paasta_print.called
    mock_paasta_print.assert_has_calls(
        [
            call(
                "\x1b[31mfake_service doesn't have any instances matching esst,"
                " fake on fake_cluster1, fake_cluster2.\x1b[0m"
            ),
            call("Did you mean any of these?"),
            call("  east"),
            call("  west"),
        ]
    )


@patch("paasta_tools.cli.cmds.status.get_instance_configs_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_services", autospec=True)
@patch("paasta_tools.cli.cmds.status.figure_out_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.status.get_actual_deployments", autospec=True)
@patch("paasta_tools.cli.cmds.status.load_system_paasta_config", autospec=True)
@patch("paasta_tools.cli.cmds.status.report_status_for_cluster", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_clusters", autospec=True)
@patch("paasta_tools.cli.cmds.status.get_planned_deployments", autospec=True)
def test_status_with_owner(
    mock_get_planned_deployments,
    mock_list_clusters,
    mock_report_status,
    mock_load_system_paasta_config,
    mock_get_actual_deployments,
    mock_figure_out_service_name,
    mock_list_services,
    mock_get_instance_configs_for_service,
    system_paasta_config,
):

    mock_load_system_paasta_config.return_value = system_paasta_config
    mock_list_services.return_value = ["fakeservice", "otherservice"]
    cluster = "fake_cluster"
    mock_list_clusters.return_value = [cluster]
    mock_inst_1 = make_fake_instance_conf(
        cluster, "fakeservice", "instance1", team="faketeam"
    )
    mock_inst_2 = make_fake_instance_conf(
        cluster, "otherservice", "instance3", team="faketeam"
    )
    mock_get_instance_configs_for_service.return_value = [mock_inst_1, mock_inst_2]
    mock_get_planned_deployments.return_value = [
        "fakeservice.instance1",
        "otherservice.instance3",
    ]

    mock_get_actual_deployments.return_value = {
        "fakeservice.instance1": "sha1",
        "fakeservice.instance2": "sha2",
        "otherservice.instance3": "sha3",
        "otherservice.instance1": "sha4",
    }
    mock_report_status.return_value = 0, ["dummy", "output"]

    args = MagicMock()
    args.service = None
    args.instances = None
    args.clusters = None
    args.deploy_group = None
    args.owner = "faketeam"
    args.soa_dir = "/fake/soa/dir"
    args.registration = None
    return_value = paasta_status(args)

    assert return_value == 0
    assert mock_report_status.call_count == 2


@patch("paasta_tools.cli.cmds.status.list_clusters", autospec=True)
@patch("paasta_tools.cli.cmds.status.get_instance_configs_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.status.list_services", autospec=True)
@patch("paasta_tools.cli.cmds.status.figure_out_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.status.get_actual_deployments", autospec=True)
@patch("paasta_tools.cli.cmds.status.load_system_paasta_config", autospec=True)
@patch("paasta_tools.cli.cmds.status.report_status_for_cluster", autospec=True)
@patch("paasta_tools.cli.cmds.status.get_planned_deployments", autospec=True)
@patch("paasta_tools.cli.cmds.status.validate_service_name", autospec=True)
def test_status_with_registration(
    mock_validate_service_name,
    mock_get_planned_deployments,
    mock_report_status,
    mock_load_system_paasta_config,
    mock_get_actual_deployments,
    mock_figure_out_service_name,
    mock_list_services,
    mock_get_instance_configs_for_service,
    mock_list_clusters,
    system_paasta_config,
):
    mock_validate_service_name.return_value = None
    mock_load_system_paasta_config.return_value = system_paasta_config
    mock_list_services.return_value = ["fakeservice", "otherservice"]
    cluster = "fake_cluster"
    mock_list_clusters.return_value = [cluster]
    mock_get_planned_deployments.return_value = [
        "fakeservice.main",
        "fakeservice.not_main",
    ]
    mock_inst_1 = make_fake_instance_conf(
        cluster, "fakeservice", "instance1", registrations=["fakeservice.main"]
    )
    mock_inst_2 = make_fake_instance_conf(
        cluster, "fakeservice", "instance2", registrations=["fakeservice.not_main"]
    )
    mock_inst_3 = make_fake_instance_conf(
        cluster, "fakeservice", "instance3", registrations=["fakeservice.also_not_main"]
    )
    mock_inst_4 = make_fake_instance_conf(
        cluster, "fakeservice", "instance4", registrations=None
    )
    mock_get_instance_configs_for_service.return_value = [
        mock_inst_1,
        mock_inst_2,
        mock_inst_3,
        mock_inst_4,
    ]

    mock_get_actual_deployments.return_value = {
        "fakeservice.instance1": "sha1",
        "fakeservice.instance2": "sha2",
        "fakeservice.instance3": "sha3",
    }
    mock_report_status.return_value = 0, ["dummy", "output"]

    args = StatusArgs(
        service="fakeservice",
        instances=None,
        clusters=None,
        deploy_group=None,
        owner=None,
        registration="main,not_main",
        soa_dir="/fake/soa/dir",
        verbose=False,
    )
    return_value = paasta_status(args)

    assert return_value == 0
    assert mock_report_status.call_count == 1
    mock_report_status.assert_called_once_with(
        service="fakeservice",
        cluster=cluster,
        deploy_pipeline=ANY,
        actual_deployments=ANY,
        instance_whitelist={
            "instance1": mock_inst_1.__class__,
            "instance2": mock_inst_2.__class__,
        },
        system_paasta_config=system_paasta_config,
        verbose=args.verbose,
    )


class Struct:
    """
    convert a dictionary to an object
    """

    def __init__(self, **entries):
        self.__dict__.update(entries)


@pytest.fixture
def mock_marathon_status():
    return Struct(
        error_message=None,
        desired_state="start",
        desired_app_id="abc.def",
        autoscaling_info=None,
        app_id="fake_app_id",
        app_count=1,
        running_instance_count=2,
        expected_instance_count=2,
        deploy_status="Running",
        bounce_method="crossover",
        app_statuses=[],
        mesos=Struct(
            running_task_count=2,
            error_message=None,
            running_tasks=[],
            non_running_tasks=[],
        ),
        smartstack=Struct(
            registration="fake_service.fake_instance",
            expected_backends_per_location=1,
            locations=[],
        ),
    )


@pytest.fixture
def mock_kubernetes_status():
    return Struct(
        error_message=None,
        desired_state="start",
        desired_app_id="abc.def",
        autoscaling_info=None,
        app_id="fake_app_id",
        app_count=1,
        running_instance_count=2,
        expected_instance_count=2,
        deploy_status="Running",
        bounce_method="crossover",
        create_timestamp=1562963508,
        namespace="paasta",
        pods=[],
        replicasets=[],
        smartstack=Struct(
            registration="fake_service.fake_instance",
            expected_backends_per_location=1,
            locations=[],
        ),
        evicted_count=1,
    )


def test_paasta_status_on_api_endpoint_marathon(
    system_paasta_config, mock_marathon_status
):
    fake_status_obj = Struct(
        git_sha="fake_git_sha",
        instance="fake_instance",
        service="fake_service",
        marathon=mock_marathon_status,
    )

    system_paasta_config = system_paasta_config

    with patch("bravado.http_future.HttpFuture.result", autospec=True) as mock_result:
        mock_result.return_value = fake_status_obj
        output = []
        paasta_status_on_api_endpoint(
            cluster="fake_cluster",
            service="fake_service",
            instance="fake_instance",
            output=output,
            system_paasta_config=system_paasta_config,
            verbose=0,
        )


def test_paasta_status_exception(system_paasta_config):
    system_paasta_config = system_paasta_config

    with patch(
        "paasta_tools.cli.cmds.status.get_paasta_api_client", autospec=True
    ) as mock_get_paasta_api_client:
        requests_response = Mock(status_code=500, text="Internal Server Error")
        incoming_response = RequestsResponseAdapter(requests_response)

        mock_swagger_client = Mock()
        mock_swagger_client.service.status_instance.side_effect = HTTPError(
            incoming_response
        )
        mock_get_paasta_api_client.return_value = mock_swagger_client
        paasta_status_on_api_endpoint(
            cluster="fake_cluster",
            service="fake_service",
            instance="fake_instance",
            output=[],
            system_paasta_config=system_paasta_config,
            verbose=False,
        )


class TestPrintMarathonStatus:
    def test_error(self, mock_marathon_status):
        mock_marathon_status.error_message = "Things went wrong"
        output = []
        return_value = print_marathon_status(
            service="fake_service",
            instance="fake_instance",
            output=output,
            marathon_status=mock_marathon_status,
        )

        assert return_value == 1
        assert output == ["Things went wrong"]

    def test_successful_return_value(self, mock_marathon_status):
        return_value = print_marathon_status(
            service="fake_service",
            instance="fake_instance",
            output=[],
            marathon_status=mock_marathon_status,
        )
        assert return_value == 0

    @pytest.mark.parametrize("include_smartstack", [True, False])
    @pytest.mark.parametrize("include_autoscaling_info", [True, False])
    @patch("paasta_tools.cli.cmds.status.create_autoscaling_info_table", autospec=True)
    @patch("paasta_tools.cli.cmds.status.get_smartstack_status_human", autospec=True)
    @patch("paasta_tools.cli.cmds.status.marathon_mesos_status_human", autospec=True)
    @patch("paasta_tools.cli.cmds.status.marathon_app_status_human", autospec=True)
    @patch("paasta_tools.cli.cmds.status.status_marathon_job_human", autospec=True)
    @patch("paasta_tools.cli.cmds.status.desired_state_human", autospec=True)
    @patch("paasta_tools.cli.cmds.status.bouncing_status_human", autospec=True)
    def test_output(
        self,
        mock_bouncing_status,
        mock_desired_state,
        mock_status_marathon_job_human,
        mock_marathon_app_status_human,
        mock_marathon_mesos_status_human,
        mock_get_smartstack_status_human,
        mock_create_autoscaling_info_table,
        mock_marathon_status,
        include_autoscaling_info,
        include_smartstack,
    ):
        mock_marathon_app_status_human.side_effect = lambda desired_app_id, app_status: [
            f"{app_status.id} status 1",
            f"{app_status.id} status 2",
        ]
        mock_marathon_mesos_status_human.return_value = [
            "mesos status 1",
            "mesos status 2",
        ]
        mock_get_smartstack_status_human.return_value = [
            "smartstack status 1",
            "smartstack status 2",
        ]
        mock_create_autoscaling_info_table.return_value = [
            "autoscaling info 1",
            "autoscaling info 2",
        ]

        mock_marathon_status.app_statuses = [Struct(id="app_1"), Struct(id="app_2")]
        if include_autoscaling_info:
            mock_marathon_status.autoscaling_info = Struct()
        if not include_smartstack:
            mock_marathon_status.smartstack = None

        output = []
        print_marathon_status(
            service="fake_service",
            instance="fake_instance",
            output=output,
            marathon_status=mock_marathon_status,
        )

        expected_output = [
            f"    Desired state:      {mock_bouncing_status.return_value} and {mock_desired_state.return_value}",
            f"    {mock_status_marathon_job_human.return_value}",
        ]
        if include_autoscaling_info:
            expected_output += ["      autoscaling info 1", "      autoscaling info 2"]
        expected_output += [
            f"      app_1 status 1",
            f"      app_1 status 2",
            f"      app_2 status 1",
            f"      app_2 status 2",
            f"    mesos status 1",
            f"    mesos status 2",
        ]
        if include_smartstack:
            expected_output += [f"    smartstack status 1", f"    smartstack status 2"]

        assert expected_output == output


class TestPrintKubernetesStatus:
    def test_error(self, mock_kubernetes_status):
        mock_kubernetes_status.error_message = "Things went wrong"
        output = []
        return_value = print_kubernetes_status(
            service="fake_service",
            instance="fake_instance",
            output=output,
            kubernetes_status=mock_kubernetes_status,
        )

        assert return_value == 1
        assert output == ["Things went wrong"]

    def test_successful_return_value(self, mock_kubernetes_status):
        return_value = print_kubernetes_status(
            service="fake_service",
            instance="fake_instance",
            output=[],
            kubernetes_status=mock_kubernetes_status,
        )
        assert return_value == 0

    @patch(
        "paasta_tools.cli.cmds.status.format_tail_lines_for_mesos_task", autospec=True
    )
    @patch("paasta_tools.cli.cmds.status.get_smartstack_status_human", autospec=True)
    @patch("paasta_tools.cli.cmds.status.humanize.naturaltime", autospec=True)
    @patch(
        "paasta_tools.cli.cmds.status.kubernetes_app_deploy_status_human", autospec=True
    )
    @patch("paasta_tools.cli.cmds.status.desired_state_human", autospec=True)
    @patch("paasta_tools.cli.cmds.status.bouncing_status_human", autospec=True)
    def test_output(
        self,
        mock_bouncing_status,
        mock_desired_state,
        mock_kubernetes_app_deploy_status_human,
        mock_naturaltime,
        mock_get_smartstack_status_human,
        mock_format_tail_lines_for_mesos_task,
        mock_kubernetes_status,
    ):
        mock_bouncing_status.return_value = "Bouncing (crossover)"
        mock_desired_state.return_value = "Started"
        mock_kubernetes_app_deploy_status_human.return_value = "Running"
        mock_naturaltime.return_value = "a month ago"
        mock_kubernetes_status.pods = [
            Struct(
                name="app_1",
                host="fake_host1",
                deployed_timestamp=1562963508,
                phase="Running",
                tail_lines=Struct(),
                message=None,
            ),
            Struct(
                name="app_2",
                host="fake_host2",
                deployed_timestamp=1562963510,
                phase="Running",
                tail_lines=Struct(),
                message=None,
            ),
            Struct(
                name="app_3",
                host="fake_host3",
                deployed_timestamp=1562963511,
                phase="Failed",
                tail_lines=Struct(),
                message="Disk quota exceeded",
                reason="Evicted",
            ),
        ]
        mock_kubernetes_status.replicasets = [
            Struct(
                name="replicaset_1",
                replicas=3,
                ready_replicas=2,
                create_timestamp=1562963508,
            )
        ]

        output = []
        print_kubernetes_status(
            service="fake_service",
            instance="fake_instance",
            output=output,
            kubernetes_status=mock_kubernetes_status,
        )

        expected_output = [
            f"    State:      {mock_bouncing_status.return_value} - Desired state: {mock_desired_state.return_value}",
            f"    Kubernetes:   {PaastaColors.green('Healthy')} - up with {PaastaColors.green('(2/2)')} instances ({PaastaColors.red('1')} evicted). Status: {mock_kubernetes_app_deploy_status_human.return_value}",
        ]
        expected_output += [
            f"      App created: 2019-07-12 20:31:48 ({mock_naturaltime.return_value}). Namespace: paasta",
            f"      Pods:",
            f"        Pod ID  Host deployed to  Deployed at what localtime      Health",
            f"        app_1   fake_host1        2019-07-12T20:31 ({mock_naturaltime.return_value})  {PaastaColors.green('Healthy')}",
            f"        app_2   fake_host2        2019-07-12T20:31 ({mock_naturaltime.return_value})  {PaastaColors.green('Healthy')}",
            f"        app_3   fake_host3        2019-07-12T20:31 ({mock_naturaltime.return_value})  {PaastaColors.red('Evicted')}",
            f"        {PaastaColors.grey('  Disk quota exceeded')}",
            f"      ReplicaSets:",
            f"        ReplicaSet Name  Ready / Desired  Created at what localtime",
            f"        replicaset_1     {PaastaColors.red('2/3')}              2019-07-12T20:31 ({mock_naturaltime.return_value})",
        ]

        assert expected_output == output


def _formatted_table_to_dict(formatted_table):
    """Convert a single-row table with header to a dictionary"""
    headers = [
        header.strip() for header in formatted_table[0].split("  ") if len(header) > 0
    ]
    fields = [
        field.strip() for field in formatted_table[1].split("  ") if len(field) > 0
    ]
    return dict(zip(headers, fields))


def test_create_autoscaling_info_table():
    mock_autoscaling_info = Struct(
        current_instances=2,
        max_instances=5,
        min_instances=1,
        current_utilization=0.6,
        target_instances=3,
    )
    output = create_autoscaling_info_table(mock_autoscaling_info)
    assert output[0] == "Autoscaling Info:"

    table_headings_to_values = _formatted_table_to_dict(output[1:])
    assert table_headings_to_values == {
        "Current instances": "2",
        "Max instances": "5",
        "Min instances": "1",
        "Current utilization": "60.0%",
        "Target instances": "3",
    }


def test_create_autoscaling_info_table_errors():
    mock_autoscaling_info = Struct(
        current_instances=2,
        max_instances=5,
        min_instances=1,
        current_utilization=None,
        target_instances=None,
    )
    output = create_autoscaling_info_table(mock_autoscaling_info)
    table_headings_to_values = _formatted_table_to_dict(output[1:])

    assert table_headings_to_values["Current utilization"] == "Exception"
    assert table_headings_to_values["Target instances"] == "Exception"


@patch("paasta_tools.cli.cmds.status.humanize.naturaltime", autospec=True)
class TestMarathonAppStatusHuman:
    @pytest.fixture
    def mock_app_status(self):
        return Struct(
            tasks_running=5,
            tasks_healthy=4,
            tasks_staged=3,
            tasks_total=12,
            create_timestamp=1565731681,
            deploy_status="Deploying",
            dashboard_url="http://paasta.party",
            backoff_seconds=2,
            unused_offer_reason_counts=None,
            tasks=[],
        )

    def test_marathon_app_status_human(self, mock_naturaltime, mock_app_status):
        output = marathon_app_status_human("app_id", mock_app_status)
        uncolored_output = [remove_ansi_escape_sequences(line) for line in output]

        assert uncolored_output == [
            f"Dashboard: {mock_app_status.dashboard_url}",
            f"  5 running, 4 healthy, 3 staged out of 12",
            f"  App created: 2019-08-13 21:28:01 ({mock_naturaltime.return_value})",
            f"  Status: Deploying",
        ]

    def test_no_dashboard_url(self, mock_naturaltime, mock_app_status):
        mock_app_status.dashboard_url = None
        output = marathon_app_status_human("app_id", mock_app_status)
        assert remove_ansi_escape_sequences(output[0]) == "App ID: app_id"

    @patch("paasta_tools.cli.cmds.status.format_marathon_task_table", autospec=True)
    def test_tasks_list(
        self, mock_format_marathon_task_table, mock_naturaltime, mock_app_status
    ):
        mock_app_status.tasks = [Struct()]
        mock_format_marathon_task_table.return_value = ["task table 1", "task table 2"]
        output = marathon_app_status_human("app_id", mock_app_status)

        expected_task_table_lines = ["  Tasks:", "    task table 1", "    task table 2"]
        assert output[-3:] == expected_task_table_lines

    def test_unused_offers(self, mock_naturaltime, mock_app_status):
        mock_app_status.unused_offer_reason_counts = {"reason1": 5, "reason2": 3}
        output = marathon_app_status_human("app_id", mock_app_status)
        expected_lines = ["  Possibly stalled for:", "    reason1: 5", "    reason2: 3"]
        assert output[-3:] == expected_lines


@patch("paasta_tools.cli.cmds.status.humanize.naturaltime", autospec=True)
class TestFormatMarathonTaskTable:
    @pytest.fixture
    def mock_marathon_task(self):
        return Struct(
            id="abc123",
            host="paasta.cloud",
            port=4321,
            deployed_timestamp=1565648600,
            is_healthy=True,
        )

    def test_format_marathon_task_table(self, mock_naturaltime, mock_marathon_task):
        output = format_marathon_task_table([mock_marathon_task])
        task_table_dict = _formatted_table_to_dict(output)
        assert task_table_dict == {
            "Mesos Task ID": "abc123",
            "Host deployed to": "paasta.cloud:4321",
            "Deployed at what localtime": f"2019-08-12T22:23 ({mock_naturaltime.return_value})",
            "Health": PaastaColors.green("Healthy"),
        }

    def test_no_host(self, mock_naturaltime, mock_marathon_task):
        mock_marathon_task.host = None
        output = format_marathon_task_table([mock_marathon_task])
        task_table_dict = _formatted_table_to_dict(output)
        assert task_table_dict["Host deployed to"] == "Unknown"

    def test_unhealthy(self, mock_naturaltime, mock_marathon_task):
        mock_marathon_task.is_healthy = False
        output = format_marathon_task_table([mock_marathon_task])
        task_table_dict = _formatted_table_to_dict(output)
        assert task_table_dict["Health"] == PaastaColors.red("Unhealthy")

    def test_no_health(self, mock_naturaltime, mock_marathon_task):
        mock_marathon_task.is_healthy = None
        output = format_marathon_task_table([mock_marathon_task])
        task_table_dict = _formatted_table_to_dict(output)
        assert task_table_dict["Health"] == PaastaColors.grey("N/A")


@patch("paasta_tools.cli.cmds.status.format_tail_lines_for_mesos_task", autospec=True)
@patch("paasta_tools.cli.cmds.status.humanize.naturaltime", autospec=True)
class TestFormatKubernetesPodTable:
    @pytest.fixture
    def mock_kubernetes_pod(self):
        return Struct(
            name="abc123",
            host="paasta.cloud",
            deployed_timestamp=1565648600,
            phase="Running",
            tail_lines=Struct(),
            message=None,
            reason=None,
        )

    @pytest.fixture
    def mock_kubernetes_replicaset(self):
        return Struct(
            name="abc123", replicas=3, ready_replicas=3, create_timestamp=1565648600
        )

    def test_format_kubernetes_pod_table(
        self,
        mock_naturaltime,
        mock_format_tail_lines_for_mesos_task,
        mock_kubernetes_pod,
    ):
        output = format_kubernetes_pod_table([mock_kubernetes_pod])
        pod_table_dict = _formatted_table_to_dict(output)
        assert pod_table_dict == {
            "Pod ID": "abc123",
            "Host deployed to": "paasta.cloud",
            "Deployed at what localtime": f"2019-08-12T22:23 ({mock_naturaltime.return_value})",
            "Health": PaastaColors.green("Healthy"),
        }

    def test_format_kubernetes_replicaset_table(
        self,
        mock_naturaltime,
        mock_format_tail_lines_for_mesos_task,
        mock_kubernetes_replicaset,
    ):
        output = format_kubernetes_replicaset_table([mock_kubernetes_replicaset])
        replicaset_table_dict = _formatted_table_to_dict(output)
        assert replicaset_table_dict == {
            "ReplicaSet Name": "abc123",
            "Ready / Desired": PaastaColors.green("3/3"),
            "Created at what localtime": f"2019-08-12T22:23 ({mock_naturaltime.return_value})",
        }

    def test_no_host(
        self,
        mock_naturaltime,
        mock_format_tail_lines_for_mesos_task,
        mock_kubernetes_pod,
    ):
        mock_kubernetes_pod.host = None
        output = format_kubernetes_pod_table([mock_kubernetes_pod])
        pod_table_dict = _formatted_table_to_dict(output)
        assert pod_table_dict["Host deployed to"] == "Unknown"

    def test_unhealthy(
        self,
        mock_naturaltime,
        mock_format_tail_lines_for_mesos_task,
        mock_kubernetes_pod,
    ):
        mock_kubernetes_pod.phase = "Failed"
        output = format_kubernetes_pod_table([mock_kubernetes_pod])
        pod_table_dict = _formatted_table_to_dict(output)
        assert pod_table_dict["Health"] == PaastaColors.red("Unhealthy")

    def test_evicted(
        self,
        mock_naturaltime,
        mock_format_tail_lines_for_mesos_task,
        mock_kubernetes_pod,
    ):
        mock_kubernetes_pod.phase = "Failed"
        mock_kubernetes_pod.reason = "Evicted"
        output = format_kubernetes_pod_table([mock_kubernetes_pod])
        pod_table_dict = _formatted_table_to_dict(output)
        assert pod_table_dict["Health"] == PaastaColors.red("Evicted")

    def test_no_health(
        self,
        mock_naturaltime,
        mock_format_tail_lines_for_mesos_task,
        mock_kubernetes_pod,
    ):
        mock_kubernetes_pod.phase = None
        output = format_kubernetes_pod_table([mock_kubernetes_pod])
        pod_table_dict = _formatted_table_to_dict(output)
        assert pod_table_dict["Health"] == PaastaColors.grey("N/A")


@patch("paasta_tools.cli.cmds.status.create_mesos_running_tasks_table", autospec=True)
@patch(
    "paasta_tools.cli.cmds.status.create_mesos_non_running_tasks_table", autospec=True
)
@patch("paasta_tools.cli.cmds.status.marathon_mesos_status_summary", autospec=True)
def test_marathon_mesos_status_human(
    mock_marathon_mesos_status_summary,
    mock_create_mesos_non_running_tasks_table,
    mock_create_mesos_running_tasks_table,
):
    mock_create_mesos_running_tasks_table.return_value = [
        "running task 1",
        "running task 2",
    ]
    mock_create_mesos_non_running_tasks_table.return_value = ["non-running task 1"]

    running_tasks = [Struct(), Struct()]
    non_running_tasks = [Struct()]
    output = marathon_mesos_status_human(
        error_message=None,
        running_task_count=2,
        expected_instance_count=2,
        running_tasks=running_tasks,
        non_running_tasks=non_running_tasks,
    )

    assert output == [
        mock_marathon_mesos_status_summary.return_value,
        "  Running Tasks:",
        "    running task 1",
        "    running task 2",
        PaastaColors.grey("  Non-running Tasks:"),
        "    non-running task 1",
    ]
    mock_marathon_mesos_status_summary.assert_called_once_with(2, 2)
    mock_create_mesos_running_tasks_table.assert_called_once_with(running_tasks)
    mock_create_mesos_non_running_tasks_table.assert_called_once_with(non_running_tasks)


def test_marathon_mesos_status_summary():
    status_summary = marathon_mesos_status_summary(
        mesos_task_count=3, expected_instance_count=2
    )
    expected_status = PaastaColors.green("Healthy")
    expected_count = PaastaColors.green(f"(3/2)")
    assert f"{expected_status} - {expected_count}" in status_summary


@patch("paasta_tools.cli.cmds.status.format_tail_lines_for_mesos_task", autospec=True)
@patch("paasta_tools.cli.cmds.status.humanize.naturaltime", autospec=True)
class TestCreateMesosRunningTasksTable:
    @pytest.fixture
    def mock_running_task(self):
        return Struct(
            id="task_id",
            hostname="paasta.yelp.com",
            mem_limit=Struct(value=2 * 1024 * 1024),
            rss=Struct(value=1024 * 1024),
            cpu_shares=Struct(value=0.5),
            cpu_used_seconds=Struct(value=1.2),
            duration_seconds=300,
            deployed_timestamp=1565567511,
            tail_lines=Struct(),
        )

    def test_create_mesos_running_tasks_table(
        self, mock_naturaltime, mock_format_tail_lines_for_mesos_task, mock_running_task
    ):
        mock_format_tail_lines_for_mesos_task.return_value = [
            "tail line 1",
            "tail line 2",
        ]
        output = create_mesos_running_tasks_table([mock_running_task])
        running_tasks_dict = _formatted_table_to_dict(output[:2])
        assert running_tasks_dict == {
            "Mesos Task ID": mock_running_task.id,
            "Host deployed to": mock_running_task.hostname,
            "Ram": "1/2MB",
            "CPU": "0.8%",
            "Deployed at what localtime": f"2019-08-11T23:51 ({mock_naturaltime.return_value})",
        }
        assert output[2:] == ["tail line 1", "tail line 2"]
        mock_format_tail_lines_for_mesos_task.assert_called_once_with(
            mock_running_task.tail_lines, mock_running_task.id
        )

    def test_error_messages(
        self, mock_naturaltime, mock_format_tail_lines_for_mesos_task, mock_running_task
    ):
        mock_running_task.mem_limit = Struct(
            value=None, error_message="Couldn't get memory"
        )
        mock_running_task.rss = Struct(value=1, error_message=None)
        mock_running_task.cpu_shares = Struct(
            value=None, error_message="Couldn't get CPU"
        )

        output = create_mesos_running_tasks_table([mock_running_task])
        running_tasks_dict = _formatted_table_to_dict(output)
        assert running_tasks_dict["Ram"] == "Couldn't get memory"
        assert running_tasks_dict["CPU"] == "Couldn't get CPU"

    def test_undefined_cpu(
        self, mock_naturaltime, mock_format_tail_lines_for_mesos_task, mock_running_task
    ):
        mock_running_task.cpu_shares.value = 0
        output = create_mesos_running_tasks_table([mock_running_task])
        running_tasks_dict = _formatted_table_to_dict(output)
        assert running_tasks_dict["CPU"] == "Undef"

    def test_high_cpu(
        self, mock_naturaltime, mock_format_tail_lines_for_mesos_task, mock_running_task
    ):
        mock_running_task.cpu_shares.value = 0.1
        mock_running_task.cpu_used_seconds.value = 28
        output = create_mesos_running_tasks_table([mock_running_task])
        running_tasks_dict = _formatted_table_to_dict(output)
        assert running_tasks_dict["CPU"] == PaastaColors.red("93.3%")

    def test_tasks_are_none(
        self, mock_naturaltime, mock_format_tail_lines_for_mesos_task, mock_running_task
    ):
        assert len(create_mesos_running_tasks_table(None)) == 1  # just the header


@patch("paasta_tools.cli.cmds.status.format_tail_lines_for_mesos_task", autospec=True)
@patch("paasta_tools.cli.cmds.status.humanize.naturaltime", autospec=True)
def test_create_mesos_non_running_tasks_table(
    mock_naturaltime, mock_format_tail_lines_for_mesos_task
):
    mock_format_tail_lines_for_mesos_task.return_value = ["tail line 1", "tail line 2"]
    mock_non_running_task = Struct(
        id="task_id",
        hostname="paasta.restaurant",
        deployed_timestamp=1564642800,
        state="Not running",
        tail_lines=Struct(),
    )
    output = create_mesos_non_running_tasks_table([mock_non_running_task])
    uncolored_output = [remove_ansi_escape_sequences(line) for line in output]
    task_dict = _formatted_table_to_dict(uncolored_output)
    assert task_dict == {
        "Mesos Task ID": mock_non_running_task.id,
        "Host deployed to": mock_non_running_task.hostname,
        "Deployed at what localtime": f"2019-08-01T07:00 ({mock_naturaltime.return_value})",
        "Status": mock_non_running_task.state,
    }
    assert uncolored_output[2:] == ["tail line 1", "tail line 2"]
    mock_format_tail_lines_for_mesos_task.assert_called_once_with(
        mock_non_running_task.tail_lines, mock_non_running_task.id
    )


@patch("paasta_tools.cli.cmds.status.format_tail_lines_for_mesos_task", autospec=True)
def test_create_mesos_non_running_tasks_table_handles_none_deployed_timestamp(
    mock_format_tail_lines_for_mesos_task,
):
    mock_non_running_task = Struct(
        id="task_id",
        hostname="paasta.restaurant",
        deployed_timestamp=None,
        state="Not running",
        tail_lines=Struct(),
    )
    output = create_mesos_non_running_tasks_table([mock_non_running_task])
    uncolored_output = [remove_ansi_escape_sequences(line) for line in output]
    task_dict = _formatted_table_to_dict(uncolored_output)
    assert task_dict["Deployed at what localtime"] == "Unknown"


def test_create_mesos_non_running_tasks_table_handles_nones():
    assert len(create_mesos_non_running_tasks_table(None)) == 1  # just the header


@patch("paasta_tools.cli.cmds.status.haproxy_backend_report", autospec=True)
@patch("paasta_tools.cli.cmds.status.build_smartstack_backends_table", autospec=True)
def test_get_smartstack_status_human(
    mock_build_smartstack_backends_table, mock_haproxy_backend_report
):
    mock_locations = [
        Struct(
            name="location_1",
            running_backends_count=2,
            backends=[Struct(hostname="location_1_host")],
        ),
        Struct(
            name="location_2",
            running_backends_count=5,
            backends=[
                Struct(hostname="location_2_host1"),
                Struct(hostname="location_2_host2"),
            ],
        ),
    ]
    mock_haproxy_backend_report.side_effect = (
        lambda expected, running: f"haproxy report: {running}/{expected}"
    )
    mock_build_smartstack_backends_table.side_effect = lambda backends: [
        f"{backend.hostname}" for backend in backends
    ]

    output = get_smartstack_status_human(
        registration="fake_service.fake_instance",
        expected_backends_per_location=5,
        locations=mock_locations,
    )
    assert output == [
        "Smartstack:",
        "  Haproxy Service Name: fake_service.fake_instance",
        "  Backends:",
        "    location_1 - haproxy report: 2/5",
        "      location_1_host",
        "    location_2 - haproxy report: 5/5",
        "      location_2_host1",
        "      location_2_host2",
    ]


def test_get_smartstack_status_human_no_locations():
    output = get_smartstack_status_human(
        registration="fake_service.fake_instance",
        expected_backends_per_location=1,
        locations=[],
    )
    assert len(output) == 1
    assert "ERROR" in output[0]


class TestBuildSmartstackBackendsTable:
    @pytest.fixture
    def mock_backend(self):
        return Struct(
            hostname="mock_host",
            port=1138,
            status="UP",
            check_status="L7OK",
            check_code="0",
            check_duration=10,
            last_change=300,
            has_associated_task=True,
        )

    def test_build_smartstack_backends_table(self, mock_backend):
        output = build_smartstack_backends_table([mock_backend])
        backend_dict = _formatted_table_to_dict(output)
        assert backend_dict == {
            "Name": "mock_host:1138",
            "LastCheck": "L7OK/0 in 10ms",
            "LastChange": "5 minutes ago",
            "Status": PaastaColors.default("UP"),
        }

    @pytest.mark.parametrize(
        "backend_status,expected_color",
        [
            ("DOWN", PaastaColors.red),
            ("MAINT", PaastaColors.grey),
            ("OTHER", PaastaColors.yellow),
        ],
    )
    def test_backend_status(self, mock_backend, backend_status, expected_color):
        mock_backend.status = backend_status
        output = build_smartstack_backends_table([mock_backend])
        backend_dict = _formatted_table_to_dict(output)
        assert backend_dict["Status"] == expected_color(backend_status)

    def test_no_associated_task(self, mock_backend):
        mock_backend.has_associated_task = False
        output = build_smartstack_backends_table([mock_backend])
        backend_dict = _formatted_table_to_dict(output)
        assert all(
            field == PaastaColors.grey(remove_ansi_escape_sequences(field))
            for field in backend_dict.values()
        )

    def test_multiple_backends(self, mock_backend):
        assert len(build_smartstack_backends_table([mock_backend, mock_backend])) == 3


def test_get_desired_state_human():
    fake_conf = marathon_tools.MarathonServiceConfig(
        service="service",
        cluster="cluster",
        instance="instance",
        config_dict={},
        branch_dict={"desired_state": "stop"},
    )
    assert "Stopped" in desired_state_human(
        fake_conf.get_desired_state(), fake_conf.get_instances()
    )


def test_get_desired_state_human_started_with_instances():
    fake_conf = marathon_tools.MarathonServiceConfig(
        service="service",
        cluster="cluster",
        instance="instance",
        config_dict={"instances": 42},
        branch_dict={"desired_state": "start"},
    )
    assert "Started" in desired_state_human(
        fake_conf.get_desired_state(), fake_conf.get_instances()
    )


def test_get_desired_state_human_with_0_instances():
    fake_conf = marathon_tools.MarathonServiceConfig(
        service="service",
        cluster="cluster",
        instance="instance",
        config_dict={"instances": 0},
        branch_dict={"desired_state": "start"},
    )
    assert "Stopped" in desired_state_human(
        fake_conf.get_desired_state(), fake_conf.get_instances()
    )


def test_haproxy_backend_report_healthy():
    normal_count = 10
    actual_count = 11
    status = haproxy_backend_report(normal_count, actual_count)
    assert "Healthy" in status


def test_haproxy_backend_report_critical():
    normal_count = 10
    actual_count = 1
    status = haproxy_backend_report(normal_count, actual_count)
    assert "Critical" in status
