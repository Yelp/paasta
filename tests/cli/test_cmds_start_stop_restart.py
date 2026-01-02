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
from unittest import mock

from paasta_tools import remote_git
from paasta_tools.cassandracluster_tools import CassandraClusterDeploymentConfig
from paasta_tools.cassandracluster_tools import CassandraClusterDeploymentConfigDict
from paasta_tools.cli.cli import parse_args
from paasta_tools.cli.cmds import start_stop_restart
from paasta_tools.flink_tools import FlinkDeploymentConfig
from paasta_tools.flink_tools import FlinkDeploymentConfigDict
from paasta_tools.flinkeks_tools import FlinkEksDeploymentConfig
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig


def test_format_tag():
    expected = "refs/tags/paasta-BRANCHNAME-TIMESTAMP-stop"
    actual = start_stop_restart.format_tag(
        branch="BRANCHNAME", force_bounce="TIMESTAMP", desired_state="stop"
    )
    assert actual == expected


@mock.patch("paasta_tools.cli.cmds.start_stop_restart.trigger_deploys", autospec=True)
@mock.patch("paasta_tools.utils.get_git_url", autospec=True)
@mock.patch("dulwich.client.get_transport_and_path", autospec=True)
@mock.patch("paasta_tools.cli.cmds.start_stop_restart.log_event", autospec=True)
def test_issue_state_change_for_service(
    mock_log_event,
    get_transport_and_path,
    get_git_url,
    mock_trigger_deploys,
):
    fake_git_url = "BLOORGRGRGRGR.yelpcorp.com"
    fake_path = "somepath"

    get_git_url.return_value = fake_git_url

    mock_git_client = mock.Mock()
    get_transport_and_path.return_value = (mock_git_client, fake_path)

    start_stop_restart.issue_state_change_for_service(
        KubernetesDeploymentConfig(
            cluster="fake_cluster",
            instance="fake_instance",
            service="fake_service",
            config_dict={},
            branch_dict=None,
        ),
        "0",
        "stop",
    )

    get_transport_and_path.assert_called_once_with(fake_git_url)
    mock_git_client.send_pack.assert_called_once_with(fake_path, mock.ANY, mock.ANY)
    assert mock_log_event.call_count == 1
    mock_trigger_deploys.assert_called_once_with("fake_service")


def test_make_mutate_refs_func():
    mutate_refs = start_stop_restart.make_mutate_refs_func(
        service_config=KubernetesDeploymentConfig(
            cluster="fake_cluster",
            instance="fake_instance",
            service="fake_service",
            config_dict={"deploy_group": "a"},
            branch_dict=None,
        ),
        force_bounce="FORCEBOUNCE",
        desired_state="stop",
    )

    old_refs = {
        "refs/tags/paasta-a-20160308T053933-deploy": "hash_for_a",
        "refs/tags/paasta-b-20160308T053933-deploy": "hash_for_b",
        "refs/tags/paasta-c-20160308T053933-deploy": "hash_for_c",
        "refs/tags/paasta-d-20160308T053933-deploy": "hash_for_d",
    }

    expected = dict(old_refs)
    expected.update(
        {"refs/tags/paasta-fake_cluster.fake_instance-FORCEBOUNCE-stop": "hash_for_a"}
    )

    actual = mutate_refs(old_refs)
    assert actual == expected


def test_log_event():
    with mock.patch(
        "paasta_tools.utils.get_username", autospec=True, return_value="fake_user"
    ), mock.patch(
        "paasta_tools.utils.get_hostname", autospec=True, return_value="fake_fqdn"
    ), mock.patch(
        "socket.getfqdn", autospec=True, return_value="fake_fqdn"
    ), mock.patch(
        "paasta_tools.utils._log", autospec=True
    ) as mock_log, mock.patch(
        "paasta_tools.utils._log_audit", autospec=True
    ) as mock_log_audit:
        service_config = KubernetesDeploymentConfig(
            cluster="fake_cluster",
            instance="fake_instance",
            service="fake_service",
            config_dict={"deploy_group": "fake_deploy_group"},
            branch_dict=None,
        )
        start_stop_restart.log_event(service_config, "stop")
        mock_log.assert_called_once_with(
            instance="fake_instance",
            service="fake_service",
            level="event",
            component="deploy",
            cluster="fake_cluster",
            line=(
                "Issued request to change state of fake_instance (an instance of "
                "fake_service) to 'stop' by fake_user@fake_fqdn"
            ),
        )
        mock_log_audit.assert_called_once_with(
            action="stop",
            instance="fake_instance",
            service="fake_service",
            cluster="fake_cluster",
        )


@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.can_user_deploy_service", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.confirm_to_continue", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.apply_args_filters", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.issue_state_change_for_service",
    autospec=True,
)
@mock.patch("paasta_tools.utils.format_timestamp", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.get_latest_deployment_tag", autospec=True
)
@mock.patch("paasta_tools.cli.cmds.start_stop_restart.get_remote_refs", autospec=True)
@mock.patch("paasta_tools.utils.InstanceConfig", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.get_instance_config", autospec=True
)
@mock.patch("paasta_tools.cli.cmds.start_stop_restart.utils.get_git_url", autospec=True)
@mock.patch("paasta_tools.cli.cmds.status.list_clusters", autospec=True)
def test_paasta_start_or_stop(
    mock_list_clusters,
    mock_get_git_url,
    mock_get_instance_config,
    mock_instance_config,
    get_remote_refs,
    mock_get_latest_deployment_tag,
    mock_format_timestamp,
    mock_issue_state_change_for_service,
    mock_apply_args_filters,
    mock_confirm_to_continue,
    mock_can_user_deploy_service,
):
    args, _ = parse_args(
        [
            "start",
            "-s",
            "fake_service",
            "-i",
            "main1,canary",
            "-c",
            "cluster1,cluster2",
            "-d",
            "/soa/dir",
        ]
    )
    mock_list_clusters.return_value = ["cluster1", "cluster2"]
    mock_get_git_url.return_value = "fake_git_url"
    mock_get_instance_config.return_value = mock_instance_config
    mock_instance_config.get_deploy_group.return_value = "some_group"
    get_remote_refs.return_value = ["not_a_real_tag", "fake_tag"]
    mock_get_latest_deployment_tag.return_value = ("not_a_real_tag", None, None)
    mock_format_timestamp.return_value = "not_a_real_timestamp"
    mock_apply_args_filters.return_value = {
        "cluster1": {"fake_service": {"main1": None, "canary": None}},
        "cluster2": {"fake_service": {"main1": None, "canary": None}},
    }
    mock_confirm_to_continue.return_value = True

    ret = args.command(args)
    c1_get_instance_config_call = mock.call(
        service="fake_service",
        cluster="cluster1",
        instance="main1",
        soa_dir="/soa/dir",
        load_deployments=False,
    )
    c2_get_instance_config_call = mock.call(
        service="fake_service",
        cluster="cluster1",
        instance="canary",
        soa_dir="/soa/dir",
        load_deployments=False,
    )
    c3_get_instance_config_call = mock.call(
        service="fake_service",
        cluster="cluster2",
        instance="main1",
        soa_dir="/soa/dir",
        load_deployments=False,
    )
    c4_get_instance_config_call = mock.call(
        service="fake_service",
        cluster="cluster2",
        instance="canary",
        soa_dir="/soa/dir",
        load_deployments=False,
    )
    mock_get_instance_config.assert_has_calls(
        [
            c1_get_instance_config_call,
            c2_get_instance_config_call,
            c3_get_instance_config_call,
            c4_get_instance_config_call,
        ],
        any_order=True,
    )
    mock_get_latest_deployment_tag.assert_called_with(
        ["not_a_real_tag", "fake_tag"], "some_group"
    )
    mock_issue_state_change_for_service.assert_called_with(
        service_config=mock_instance_config,
        force_bounce="not_a_real_timestamp",
        desired_state="start",
    )
    assert mock_issue_state_change_for_service.call_count == 4
    assert ret == 0


@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.can_user_deploy_service", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.confirm_to_continue", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.apply_args_filters", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.issue_state_change_for_service",
    autospec=True,
)
@mock.patch("paasta_tools.utils.format_timestamp", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.get_latest_deployment_tag", autospec=True
)
@mock.patch("paasta_tools.cli.cmds.start_stop_restart.get_remote_refs", autospec=True)
@mock.patch("paasta_tools.utils.InstanceConfig", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.get_instance_config", autospec=True
)
@mock.patch("paasta_tools.cli.cmds.start_stop_restart.utils.get_git_url", autospec=True)
@mock.patch("paasta_tools.cli.cmds.status.list_clusters", autospec=True)
def test_paasta_start_or_stop_with_deploy_group(
    mock_list_clusters,
    mock_get_git_url,
    mock_get_instance_config,
    mock_instance_config,
    mock_get_remote_refs,
    mock_get_latest_deployment_tag,
    mock_format_timestamp,
    mock_issue_state_change_for_service,
    mock_apply_args_filters,
    mock_confirm_to_continue,
    mock_can_user_deploy_service,
):
    args, _ = parse_args(
        [
            "start",
            "-s",
            "fake_service",
            "-c",
            "cluster1",
            "-l",
            "fake_group",
            "-d",
            "/soa/dir",
        ]
    )
    mock_list_clusters.return_value = ["cluster1", "cluster2"]
    mock_get_git_url.return_value = "fake_git_url"
    mock_get_instance_config.return_value = mock_instance_config
    mock_instance_config.get_deploy_group.return_value = args.deploy_group
    mock_get_remote_refs.return_value = ["not_a_real_tag", "fake_tag"]
    mock_get_latest_deployment_tag.return_value = ("not_a_real_tag", None, None)
    mock_format_timestamp.return_value = "not_a_real_timestamp"
    mock_apply_args_filters.return_value = {
        "cluster1": {"fake_service": {"instance1": None}}
    }
    mock_confirm_to_continue.return_value = True

    ret = args.command(args)

    mock_get_instance_config.assert_called_once_with(
        service="fake_service",
        cluster="cluster1",
        instance="instance1",
        soa_dir="/soa/dir",
        load_deployments=False,
    )
    mock_get_latest_deployment_tag.assert_called_with(
        ["not_a_real_tag", "fake_tag"], args.deploy_group
    )
    mock_issue_state_change_for_service.assert_called_once_with(
        service_config=mock_instance_config,
        force_bounce="not_a_real_timestamp",
        desired_state="start",
    )
    assert ret == 0


@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.can_user_deploy_service", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.confirm_to_continue", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.apply_args_filters", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.issue_state_change_for_service",
    autospec=True,
)
@mock.patch("paasta_tools.utils.format_timestamp", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.get_latest_deployment_tag", autospec=True
)
@mock.patch("paasta_tools.cli.cmds.start_stop_restart.get_remote_refs", autospec=True)
@mock.patch("paasta_tools.utils.InstanceConfig", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.get_instance_config", autospec=True
)
@mock.patch("paasta_tools.cli.cmds.start_stop_restart.utils.get_git_url", autospec=True)
@mock.patch("paasta_tools.cli.cmds.status.list_clusters", autospec=True)
def test_stop_or_start_figures_out_correct_instances(
    mock_list_clusters,
    mock_get_git_url,
    mock_get_instance_config,
    mock_instance_config,
    mock_get_remote_refs,
    mock_get_latest_deployment_tag,
    mock_format_timestamp,
    mock_issue_state_change_for_service,
    mock_apply_args_filters,
    mock_confirm_to_continue,
    mock_can_user_deploy_service,
):
    args, _ = parse_args(
        [
            "start",
            "-s",
            "fake_service",
            "-i",
            "main1,canary",
            "-c",
            "cluster1,cluster2",
            "-d",
            "/soa/dir",
        ]
    )
    mock_list_clusters.return_value = ["cluster1", "cluster2"]
    mock_get_git_url.return_value = "fake_git_url"
    mock_get_instance_config.return_value = mock_instance_config
    mock_instance_config.get_deploy_group.return_value = "some_group"
    mock_get_remote_refs.return_value = ["not_a_real_tag", "fake_tag"]
    mock_get_latest_deployment_tag.return_value = ("not_a_real_tag", None, None)
    mock_format_timestamp.return_value = "not_a_real_timestamp"
    mock_apply_args_filters.return_value = {
        "cluster1": {"fake_service": {"main1": None}},
        "cluster2": {"fake_service": {"main1": None, "canary": None}},
    }
    mock_confirm_to_continue.return_value = True

    ret = args.command(args)
    c1_get_instance_config_call = mock.call(
        service="fake_service",
        cluster="cluster1",
        instance="main1",
        soa_dir="/soa/dir",
        load_deployments=False,
    )
    c2_get_instance_config_call = mock.call(
        service="fake_service",
        cluster="cluster2",
        instance="main1",
        soa_dir="/soa/dir",
        load_deployments=False,
    )
    c3_get_instance_config_call = mock.call(
        service="fake_service",
        cluster="cluster2",
        instance="canary",
        soa_dir="/soa/dir",
        load_deployments=False,
    )
    mock_get_instance_config.assert_has_calls(
        [
            c1_get_instance_config_call,
            c2_get_instance_config_call,
            c3_get_instance_config_call,
        ],
        any_order=True,
    )
    mock_get_latest_deployment_tag.assert_called_with(
        ["not_a_real_tag", "fake_tag"], "some_group"
    )
    mock_issue_state_change_for_service.assert_called_with(
        service_config=mock_instance_config,
        force_bounce="not_a_real_timestamp",
        desired_state="start",
    )
    assert mock_issue_state_change_for_service.call_count == 3
    assert ret == 0


@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.can_user_deploy_service", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.confirm_to_continue", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.apply_args_filters", autospec=True
)
@mock.patch("paasta_tools.cli.cmds.start_stop_restart.get_remote_refs", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.get_instance_config", autospec=True
)
@mock.patch("paasta_tools.cli.cmds.start_stop_restart.utils.get_git_url", autospec=True)
@mock.patch("paasta_tools.cli.cmds.status.list_clusters", autospec=True)
def test_stop_or_start_handle_ls_remote_failures(
    mock_list_clusters,
    mock_get_git_url,
    mock_get_instance_config,
    mock_get_remote_refs,
    mock_apply_args_filters,
    mock_confirm_to_continue,
    mock_can_user_deploy_service,
    capfd,
):
    args, _ = parse_args(
        ["restart", "-s", "fake_service", "-c", "cluster1", "-d", "/soa/dir"]
    )

    mock_list_clusters.return_value = ["cluster1"]
    mock_get_git_url.return_value = "fake_git_url"
    mock_get_instance_config.return_value = None
    mock_get_remote_refs.side_effect = remote_git.LSRemoteException
    mock_apply_args_filters.return_value = {
        "cluster1": {"fake_service": {"instance1": mock.Mock()}}
    }
    mock_confirm_to_continue.return_value = True

    assert args.command(args) == 1
    assert "may be down" in capfd.readouterr()[0]


@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.can_user_deploy_service", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.confirm_to_continue", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.apply_args_filters", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.get_instance_config", autospec=True
)
@mock.patch("paasta_tools.cli.cmds.start_stop_restart.get_remote_refs", autospec=True)
@mock.patch("paasta_tools.cli.cmds.status.list_clusters", autospec=True)
def test_start_or_stop_bad_refs(
    mock_list_clusters,
    mock_get_remote_refs,
    mock_get_instance_config,
    mock_apply_args_filters,
    mock_confirm_to_continue,
    mock_can_user_deploy_service,
    capfd,
):
    args, _ = parse_args(
        [
            "restart",
            "-s",
            "fake_service",
            "-i",
            "fake_instance",
            "-c",
            "fake_cluster1,fake_cluster2",
            "-d",
            "/fake/soa/dir",
        ]
    )
    mock_list_clusters.return_value = ["fake_cluster1", "fake_cluster2"]

    mock_get_instance_config.return_value = KubernetesDeploymentConfig(
        cluster="fake_cluster1",
        instance="fake_instance",
        service="fake_service",
        config_dict={},
        branch_dict=None,
    )
    mock_get_remote_refs.return_value = {
        "refs/tags/paasta-deliberatelyinvalidref-20160304T053919-deploy": "70f7245ccf039d778c7e527af04eac00d261d783"
    }
    mock_apply_args_filters.return_value = {
        "fake_cluster1": {"fake_service": {"fake_instance": None}},
        "fake_cluster2": {"fake_service": {"fake_instance": None}},
    }
    mock_confirm_to_continue.return_value = True
    assert args.command(args) == 1
    assert "deployed there yet?" in capfd.readouterr()[0]


@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.apply_args_filters", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.issue_state_change_for_service",
    autospec=True,
)
@mock.patch("paasta_tools.utils.format_timestamp", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.get_latest_deployment_tag", autospec=True
)
@mock.patch("paasta_tools.cli.cmds.start_stop_restart.get_remote_refs", autospec=True)
@mock.patch("paasta_tools.utils.InstanceConfig", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.get_instance_config", autospec=True
)
@mock.patch("paasta_tools.cli.cmds.start_stop_restart.utils.get_git_url", autospec=True)
@mock.patch("paasta_tools.cli.cmds.status.list_clusters", autospec=True)
def test_start_warn_on_multi_instance(
    mock_list_clusters,
    mock_get_git_url,
    mock_get_instance_config,
    mock_instance_config,
    get_remote_refs,
    mock_get_latest_deployment_tag,
    mock_format_timestamp,
    mock_issue_state_change_for_service,
    mock_apply_args_filters,
    capfd,
):
    args, _ = parse_args(
        [
            "start",
            "-s",
            "fake_service,other_service",
            "-c",
            "cluster1,cluster2",
            "-d",
            "/soa/dir",
        ]
    )
    mock_list_clusters.return_value = ["cluster1", "cluster2"]
    mock_get_git_url.return_value = "fake_git_url"
    mock_get_instance_config.return_value = mock_instance_config
    mock_instance_config.get_deploy_group.return_value = "some_group"
    get_remote_refs.return_value = ["not_a_real_tag", "fake_tag"]
    mock_get_latest_deployment_tag.return_value = ("not_a_real_tag", None, None)
    mock_format_timestamp.return_value = "not_a_real_timestamp"
    mock_apply_args_filters.return_value = {
        "cluster1": {"fake_service": {"main1": None}, "other_service": {"main1": None}},
        "cluster2": {"fake_service": {"main1": None, "canary": None}},
    }
    ret = args.command(args)
    out, err = capfd.readouterr()
    assert ret == 1
    assert "Warning: trying to start/stop/restart multiple services" in out


@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.paasta_start_or_stop",
    autospec=True,
)
class TestStopErrorsIfUnderspecified:
    def run_and_assert_with_args(self, args, capfd):
        parsed_args, _ = parse_args(args)
        ret = parsed_args.command(parsed_args)
        out, err = capfd.readouterr()
        assert ret == 1
        assert start_stop_restart.PAASTA_STOP_UNDERSPECIFIED_ARGS_MESSAGE in out

    def test_no_cluster(self, mock_paasta_start_or_stop, capfd):
        self.run_and_assert_with_args(
            ["stop", "-s", "service", "-i", "instance"],
            capfd,
        )

    def test_no_service(self, mock_paasta_start_or_stop, capfd):
        self.run_and_assert_with_args(
            ["stop", "-c", "cluster", "-i", "instance"],
            capfd,
        )

    def test_no_instance(self, mock_paasta_start_or_stop, capfd):
        self.run_and_assert_with_args(
            ["stop", "-c", "cluster", "-s", "service"],
            capfd,
        )


@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.apply_args_filters", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.issue_state_change_for_service",
    autospec=True,
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.can_user_deploy_service", autospec=True
)
def test_error_if_no_deploy_permissions(
    mock_can_user_deploy_service,
    mock_issue_state_change_for_service,
    mock_apply_args_filters,
):
    args, _ = parse_args(
        [
            "start",
            "-s",
            "service",
            "-c",
            "cluster",
            "-i",
            "instance",
            "-d",
            "/soa/dir",
        ]
    )
    mock_apply_args_filters.return_value = {"cluster": {"service": {"instance": None}}}
    mock_can_user_deploy_service.return_value = False
    ret = args.command(args)

    assert ret == 1
    assert not mock_issue_state_change_for_service.called


@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.load_system_paasta_config", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart._wait_for_flink_stopped", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart._set_flink_desired_state", autospec=True
)
@mock.patch("paasta_tools.cli.cmds.start_stop_restart.paasta_start", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.get_instance_config", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.start_stop_restart.apply_args_filters", autospec=True
)
class TestRestartCmd:
    def test_flink_and_non_flink_instances(
        self,
        mock_apply_args_filters,
        mock_get_instance_config,
        mock_paasta_start,
        mock_set_flink_desired_state,
        mock_wait_for_flink_stopped,
        mock_load_system_paasta_config,
        capfd,
    ):
        mock_apply_args_filters.return_value = {
            "cluster": {"service": {"flink_instance": None, "cas_instance": None}}
        }
        mock_get_instance_config.side_effect = [
            FlinkDeploymentConfig(
                "service",
                "cluster",
                "flink_instance",
                FlinkDeploymentConfigDict(),
                None,
            ),
            CassandraClusterDeploymentConfig(
                "service",
                "cluster",
                "cas_instance",
                CassandraClusterDeploymentConfigDict(),
                None,
            ),
        ]
        mock_paasta_start.return_value = 0
        mock_set_flink_desired_state.return_value = 0
        mock_wait_for_flink_stopped.return_value = (
            True,
            "Cluster stopped successfully",
        )

        args, _ = parse_args(
            "restart -s service -c cluster -i flink_instance,cas_instance -d /soa/dir".split(
                " "
            )
        )
        ret = args.command(args)
        out, _ = capfd.readouterr()

        # Both Flink and non-Flink instances are restarted
        assert ret == 0
        assert mock_paasta_start.called
        # Flink API called twice (stop, then start)
        assert mock_set_flink_desired_state.call_count == 2
        # Wait for stopped was called once
        assert mock_wait_for_flink_stopped.call_count == 1
        assert "Restart complete" in out

    def test_only_non_flink_instances(
        self,
        mock_apply_args_filters,
        mock_get_instance_config,
        mock_paasta_start,
        mock_set_flink_desired_state,
        mock_wait_for_flink_stopped,
        mock_load_system_paasta_config,
        capfd,
    ):
        mock_apply_args_filters.return_value = {
            "cluster": {"service": {"cas_instance": None}}
        }
        mock_get_instance_config.side_effect = [
            CassandraClusterDeploymentConfig(
                "service",
                "cluster",
                "cas_instance",
                CassandraClusterDeploymentConfigDict(),
                None,
            ),
        ]
        mock_paasta_start.return_value = 1

        args, _ = parse_args(
            "restart -s service -c cluster -i cas_instance -d /soa/dir".split(" ")
        )
        ret = args.command(args)
        out, _ = capfd.readouterr()

        assert ret == 1
        assert mock_paasta_start.called
        # No Flink API calls
        assert not mock_set_flink_desired_state.called

    def test_only_flink_instances(
        self,
        mock_apply_args_filters,
        mock_get_instance_config,
        mock_paasta_start,
        mock_set_flink_desired_state,
        mock_wait_for_flink_stopped,
        mock_load_system_paasta_config,
        capfd,
    ):
        mock_apply_args_filters.return_value = {
            "cluster": {"service": {"flink_instance": None}}
        }
        mock_get_instance_config.side_effect = [
            FlinkDeploymentConfig(
                "service",
                "cluster",
                "flink_instance",
                FlinkDeploymentConfigDict(),
                None,
            ),
        ]
        mock_set_flink_desired_state.return_value = 0
        mock_wait_for_flink_stopped.return_value = (
            True,
            "Cluster stopped successfully",
        )

        args, _ = parse_args(
            "restart -s service -c cluster -i flink_instance -d /soa/dir".split(" ")
        )
        ret = args.command(args)
        out, _ = capfd.readouterr()

        assert ret == 0
        assert not mock_paasta_start.called
        # API called twice: stop then start
        assert mock_set_flink_desired_state.call_count == 2
        # Wait for stopped was called once
        assert mock_wait_for_flink_stopped.call_count == 1
        assert "Restart complete" in out
        assert "paasta status" in out

    def test_flink_restart_api_error_on_stop(
        self,
        mock_apply_args_filters,
        mock_get_instance_config,
        mock_paasta_start,
        mock_set_flink_desired_state,
        mock_wait_for_flink_stopped,
        mock_load_system_paasta_config,
        capfd,
    ):
        mock_apply_args_filters.return_value = {
            "cluster": {"service": {"flink_instance": None}}
        }
        mock_get_instance_config.side_effect = [
            FlinkDeploymentConfig(
                "service",
                "cluster",
                "flink_instance",
                FlinkDeploymentConfigDict(),
                None,
            ),
        ]
        # Fail on first call (stop)
        mock_set_flink_desired_state.return_value = 500

        args, _ = parse_args(
            "restart -s service -c cluster -i flink_instance -d /soa/dir".split(" ")
        )
        ret = args.command(args)

        assert ret == 500
        assert not mock_paasta_start.called
        # Only one call (stop failed)
        assert mock_set_flink_desired_state.call_count == 1
        # Wait for stopped should not be called since stop failed
        assert not mock_wait_for_flink_stopped.called

    def test_flink_restart_api_error_on_start(
        self,
        mock_apply_args_filters,
        mock_get_instance_config,
        mock_paasta_start,
        mock_set_flink_desired_state,
        mock_wait_for_flink_stopped,
        mock_load_system_paasta_config,
        capfd,
    ):
        mock_apply_args_filters.return_value = {
            "cluster": {"service": {"flink_instance": None}}
        }
        mock_get_instance_config.side_effect = [
            FlinkDeploymentConfig(
                "service",
                "cluster",
                "flink_instance",
                FlinkDeploymentConfigDict(),
                None,
            ),
        ]
        # Succeed on stop (first call), fail on start (second call)
        mock_set_flink_desired_state.side_effect = [0, 500]
        mock_wait_for_flink_stopped.return_value = (
            True,
            "Cluster stopped successfully",
        )

        args, _ = parse_args(
            "restart -s service -c cluster -i flink_instance -d /soa/dir".split(" ")
        )
        ret = args.command(args)

        assert ret == 500
        assert not mock_paasta_start.called
        # Two calls (stop succeeded, start failed)
        assert mock_set_flink_desired_state.call_count == 2
        # Wait for stopped was called once
        assert mock_wait_for_flink_stopped.call_count == 1

    def test_flinkeks_instances_restart(
        self,
        mock_apply_args_filters,
        mock_get_instance_config,
        mock_paasta_start,
        mock_set_flink_desired_state,
        mock_wait_for_flink_stopped,
        mock_load_system_paasta_config,
        capfd,
    ):
        mock_apply_args_filters.return_value = {
            "flinkeks-pnw-devc": {"service": {"flink_instance": None}}
        }
        mock_get_instance_config.side_effect = [
            FlinkEksDeploymentConfig(
                "service",
                "flinkeks-pnw-devc",
                "flink_instance",
                FlinkDeploymentConfigDict(),
                None,
            ),
        ]
        mock_set_flink_desired_state.return_value = 0
        mock_wait_for_flink_stopped.return_value = (
            True,
            "Cluster stopped successfully",
        )

        args, _ = parse_args(
            "restart -s service -c flinkeks-pnw-devc -i flink_instance -d /soa/dir".split(
                " "
            )
        )
        ret = args.command(args)
        out, _ = capfd.readouterr()

        assert ret == 0
        assert not mock_paasta_start.called
        # API called twice: stop then start
        assert mock_set_flink_desired_state.call_count == 2
        # Wait for stopped was called once
        assert mock_wait_for_flink_stopped.call_count == 1
        assert "Restart complete" in out


class TestSetFlinkDesiredState:
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_client", autospec=True
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_api_clustername",
        autospec=True,
    )
    def test_set_flink_desired_state_success(
        self,
        mock_get_clustername,
        mock_get_client,
    ):
        mock_get_clustername.return_value = "pnw-devc"
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        flink_config = FlinkDeploymentConfig(
            "service",
            "pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        ret = start_stop_restart._set_flink_desired_state(
            flink_config, "stop", system_paasta_config
        )

        assert ret == 0
        mock_client.service.instance_set_state.assert_called_once_with(
            service="service",
            instance="instance",
            desired_state="stop",
        )
        mock_get_clustername.assert_called_once_with(cluster="pnw-devc", is_eks=False)

    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_client", autospec=True
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_api_clustername",
        autospec=True,
    )
    def test_set_flink_desired_state_eks_cluster(
        self,
        mock_get_clustername,
        mock_get_client,
    ):
        mock_get_clustername.return_value = "flinkeks-pnw-devc"
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        flink_config = FlinkEksDeploymentConfig(
            "service",
            "flinkeks-pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        ret = start_stop_restart._set_flink_desired_state(
            flink_config, "start", system_paasta_config
        )

        assert ret == 0
        mock_client.service.instance_set_state.assert_called_once_with(
            service="service",
            instance="instance",
            desired_state="start",
        )
        mock_get_clustername.assert_called_once_with(
            cluster="flinkeks-pnw-devc", is_eks=True
        )

    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_client", autospec=True
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_api_clustername",
        autospec=True,
    )
    def test_set_flink_desired_state_no_client(
        self,
        mock_get_clustername,
        mock_get_client,
        capfd,
    ):
        mock_get_clustername.return_value = "pnw-devc"
        mock_get_client.return_value = None

        flink_config = FlinkDeploymentConfig(
            "service",
            "pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        ret = start_stop_restart._set_flink_desired_state(
            flink_config, "stop", system_paasta_config
        )

        assert ret == 1
        out, _ = capfd.readouterr()
        assert "Cannot get a paasta-api client for service.instance on pnw-devc" in out

    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_client", autospec=True
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_api_clustername",
        autospec=True,
    )
    def test_set_flink_desired_state_api_error(
        self,
        mock_get_clustername,
        mock_get_client,
        capfd,
    ):
        mock_get_clustername.return_value = "pnw-devc"
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        # Create a mock API error
        mock_error = mock.Mock()
        mock_error.reason = "Internal Server Error"
        mock_error.status = 500
        mock_client.api_error = type("ApiError", (Exception,), {})
        mock_client.service.instance_set_state.side_effect = mock_client.api_error()
        # Set the attributes on the raised exception
        mock_client.service.instance_set_state.side_effect.reason = (
            "Internal Server Error"
        )
        mock_client.service.instance_set_state.side_effect.status = 500

        flink_config = FlinkDeploymentConfig(
            "service",
            "pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        ret = start_stop_restart._set_flink_desired_state(
            flink_config, "stop", system_paasta_config
        )

        assert ret == 500
        out, _ = capfd.readouterr()
        assert "Failed to set service.instance to 'stop': Internal Server Error" in out

    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_client", autospec=True
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_api_clustername",
        autospec=True,
    )
    def test_set_flink_desired_state_connection_error(
        self,
        mock_get_clustername,
        mock_get_client,
        capfd,
    ):
        """Test handling of connection/timeout errors."""
        mock_get_clustername.return_value = "pnw-devc"
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        # Create mock error types
        mock_client.api_error = type("ApiError", (Exception,), {})
        mock_client.connection_error = type("ConnectionError", (Exception,), {})
        mock_client.timeout_error = type("TimeoutError", (Exception,), {})
        mock_client.service.instance_set_state.side_effect = (
            mock_client.connection_error("Connection refused")
        )

        flink_config = FlinkDeploymentConfig(
            "service",
            "pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        ret = start_stop_restart._set_flink_desired_state(
            flink_config, "stop", system_paasta_config
        )

        assert ret == 1
        out, _ = capfd.readouterr()
        assert "Connection error setting service.instance to 'stop'" in out


class TestGetFlinkState:
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_client", autospec=True
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_api_clustername",
        autospec=True,
    )
    def test_get_flink_state_success(
        self,
        mock_get_clustername,
        mock_get_client,
    ):
        mock_get_clustername.return_value = "pnw-devc"
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        # Mock the status response with the correct structure
        mock_flink_status = mock.Mock()
        mock_flink_status.status = {"state": "running", "pod_status": []}
        mock_status = mock.Mock()
        mock_status.flink = mock_flink_status
        mock_client.service.status_instance.return_value = mock_status

        flink_config = FlinkDeploymentConfig(
            "service",
            "pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        state, error = start_stop_restart._get_flink_state(
            flink_config, system_paasta_config
        )

        assert state == "running"
        assert error is None
        mock_client.service.status_instance.assert_called_once_with(
            service="service",
            instance="instance",
        )

    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_client", autospec=True
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_api_clustername",
        autospec=True,
    )
    def test_get_flink_state_stopped(
        self,
        mock_get_clustername,
        mock_get_client,
    ):
        mock_get_clustername.return_value = "pnw-devc"
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        # Mock the status response for stopped state
        mock_flink_status = mock.Mock()
        mock_flink_status.status = {"state": "stopped", "pod_status": []}
        mock_status = mock.Mock()
        mock_status.flink = mock_flink_status
        mock_client.service.status_instance.return_value = mock_status

        flink_config = FlinkDeploymentConfig(
            "service",
            "pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        state, error = start_stop_restart._get_flink_state(
            flink_config, system_paasta_config
        )

        assert state == "stopped"
        assert error is None

    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_client", autospec=True
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_api_clustername",
        autospec=True,
    )
    def test_get_flink_state_flinkeks(
        self,
        mock_get_clustername,
        mock_get_client,
    ):
        mock_get_clustername.return_value = "flinkeks-pnw-devc"
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        # Mock the status response for flinkeks
        mock_flink_status = mock.Mock()
        mock_flink_status.status = {"state": "running", "pod_status": []}
        mock_status = mock.Mock()
        mock_status.flink = None
        mock_status.flinkeks = mock_flink_status
        mock_client.service.status_instance.return_value = mock_status

        flink_config = FlinkEksDeploymentConfig(
            "service",
            "flinkeks-pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        state, error = start_stop_restart._get_flink_state(
            flink_config, system_paasta_config
        )

        assert state == "running"
        assert error is None

    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_client", autospec=True
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_api_clustername",
        autospec=True,
    )
    def test_get_flink_state_no_client(
        self,
        mock_get_clustername,
        mock_get_client,
    ):
        mock_get_clustername.return_value = "pnw-devc"
        mock_get_client.return_value = None

        flink_config = FlinkDeploymentConfig(
            "service",
            "pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        state, error = start_stop_restart._get_flink_state(
            flink_config, system_paasta_config
        )

        assert state is None
        assert (
            error == "Cannot get a paasta-api client for service.instance on pnw-devc"
        )

    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_client", autospec=True
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_api_clustername",
        autospec=True,
    )
    def test_get_flink_state_no_status_attribute(
        self,
        mock_get_clustername,
        mock_get_client,
    ):
        mock_get_clustername.return_value = "pnw-devc"
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        # Mock the status response without status attribute
        mock_flink_status = mock.Mock(spec=[])  # Empty spec means no attributes
        mock_status = mock.Mock()
        mock_status.flink = mock_flink_status
        mock_client.service.status_instance.return_value = mock_status

        flink_config = FlinkDeploymentConfig(
            "service",
            "pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        state, error = start_stop_restart._get_flink_state(
            flink_config, system_paasta_config
        )

        assert state is None
        assert error == "Could not get Flink state for service.instance on pnw-devc"

    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_client", autospec=True
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_api_clustername",
        autospec=True,
    )
    def test_get_flink_state_api_error(
        self,
        mock_get_clustername,
        mock_get_client,
    ):
        mock_get_clustername.return_value = "pnw-devc"
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        # Create a mock API error
        mock_client.api_error = type("ApiError", (Exception,), {})
        mock_client.service.status_instance.side_effect = mock_client.api_error()
        mock_client.service.status_instance.side_effect.reason = "Not Found"

        flink_config = FlinkDeploymentConfig(
            "service",
            "pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        state, error = start_stop_restart._get_flink_state(
            flink_config, system_paasta_config
        )

        assert state is None
        assert error == "API error for service.instance on pnw-devc: Not Found"

    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_client", autospec=True
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_api_clustername",
        autospec=True,
    )
    def test_get_flink_state_connection_error(
        self,
        mock_get_clustername,
        mock_get_client,
    ):
        """Test handling of connection/timeout errors."""
        mock_get_clustername.return_value = "pnw-devc"
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        # Create mock connection and timeout error types
        mock_client.api_error = type("ApiError", (Exception,), {})
        mock_client.connection_error = type("ConnectionError", (Exception,), {})
        mock_client.timeout_error = type("TimeoutError", (Exception,), {})
        mock_client.service.status_instance.side_effect = mock_client.connection_error(
            "Connection refused"
        )

        flink_config = FlinkDeploymentConfig(
            "service",
            "pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        state, error = start_stop_restart._get_flink_state(
            flink_config, system_paasta_config
        )

        assert state is None
        assert "Connection error for service.instance on pnw-devc" in error

    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_client", autospec=True
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_api_clustername",
        autospec=True,
    )
    def test_get_flink_state_malformed_response_key_error(
        self,
        mock_get_clustername,
        mock_get_client,
    ):
        """Test handling of malformed API responses that raise KeyError."""
        mock_get_clustername.return_value = "pnw-devc"
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        # Mock a response where accessing status raises KeyError
        mock_flink_status = mock.Mock()
        # Use a PropertyMock to raise KeyError when status is accessed
        type(mock_flink_status).status = mock.PropertyMock(
            side_effect=KeyError("status not found")
        )
        mock_status = mock.Mock()
        mock_status.flink = mock_flink_status
        mock_client.service.status_instance.return_value = mock_status
        # Set up api_error and network error types as exception classes
        mock_client.api_error = type("ApiError", (Exception,), {})
        mock_client.connection_error = type("ConnectionError", (Exception,), {})
        mock_client.timeout_error = type("TimeoutError", (Exception,), {})

        flink_config = FlinkDeploymentConfig(
            "service",
            "pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        state, error = start_stop_restart._get_flink_state(
            flink_config, system_paasta_config
        )

        assert state is None
        assert "Unexpected response format for service.instance on pnw-devc" in error

    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_client", autospec=True
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_paasta_oapi_api_clustername",
        autospec=True,
    )
    def test_get_flink_state_malformed_response_type_error(
        self,
        mock_get_clustername,
        mock_get_client,
    ):
        """Test handling of malformed API responses that raise TypeError."""
        mock_get_clustername.return_value = "pnw-devc"
        mock_client = mock.Mock()
        mock_get_client.return_value = mock_client

        # Set up api_error and network error types as proper exception classes
        mock_client.api_error = type("ApiError", (Exception,), {})
        mock_client.connection_error = type("ConnectionError", (Exception,), {})
        mock_client.timeout_error = type("TimeoutError", (Exception,), {})

        # Mock a response where .get() raises TypeError (e.g., status is not a dict)
        mock_flink_status = mock.Mock()
        mock_flink_status.status = mock.Mock()
        mock_flink_status.status.get = mock.Mock(side_effect=TypeError("not a dict"))
        mock_status = mock.Mock()
        mock_status.flink = mock_flink_status
        mock_client.service.status_instance.return_value = mock_status

        flink_config = FlinkDeploymentConfig(
            "service",
            "pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        state, error = start_stop_restart._get_flink_state(
            flink_config, system_paasta_config
        )

        assert state is None
        assert "Unexpected response format for service.instance on pnw-devc" in error


class TestWaitForFlinkStopped:
    @mock.patch("paasta_tools.cli.cmds.start_stop_restart.time.sleep", autospec=True)
    @mock.patch("paasta_tools.cli.cmds.start_stop_restart.time.time", autospec=True)
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart._get_flink_state", autospec=True
    )
    def test_wait_for_flink_stopped_success(
        self,
        mock_get_flink_state,
        mock_time,
        mock_sleep,
        capfd,
    ):
        """Test successful wait when cluster transitions to stopped."""
        # Simulate time progression: 0, 10, 20 seconds
        mock_time.side_effect = [0, 0, 10, 10, 20]
        # First call returns "running", second call returns "stopped"
        mock_get_flink_state.side_effect = [
            ("running", None),
            ("stopped", None),
        ]

        flink_config = FlinkDeploymentConfig(
            "service",
            "pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        success, message = start_stop_restart._wait_for_flink_stopped(
            flink_config,
            system_paasta_config,
            timeout_seconds=60,
            poll_interval_seconds=10,
        )

        assert success is True
        assert message == "Cluster stopped successfully"
        # Should have slept once between the two state checks
        assert mock_sleep.call_count == 1

    @mock.patch("paasta_tools.cli.cmds.start_stop_restart.time.sleep", autospec=True)
    @mock.patch("paasta_tools.cli.cmds.start_stop_restart.time.time", autospec=True)
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart._get_flink_state", autospec=True
    )
    def test_wait_for_flink_stopped_case_insensitive(
        self,
        mock_get_flink_state,
        mock_time,
        mock_sleep,
        capfd,
    ):
        """Test that state comparison is case-insensitive (STOPPED vs stopped)."""
        # Need enough time values for: start_time, while check, elapsed, state change print
        mock_time.side_effect = [0, 0, 0, 0]
        # Return uppercase "STOPPED"
        mock_get_flink_state.return_value = ("STOPPED", None)

        flink_config = FlinkDeploymentConfig(
            "service",
            "pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        success, message = start_stop_restart._wait_for_flink_stopped(
            flink_config,
            system_paasta_config,
            timeout_seconds=60,
            poll_interval_seconds=10,
        )

        assert success is True
        assert message == "Cluster stopped successfully"

    @mock.patch("paasta_tools.cli.cmds.start_stop_restart.time.sleep", autospec=True)
    @mock.patch("paasta_tools.cli.cmds.start_stop_restart.time.time", autospec=True)
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart._get_flink_state", autospec=True
    )
    def test_wait_for_flink_stopped_timeout(
        self,
        mock_get_flink_state,
        mock_time,
        mock_sleep,
        capfd,
    ):
        """Test timeout when cluster doesn't stop in time."""
        # Simulate time progression past timeout
        mock_time.side_effect = [0, 0, 10, 10, 100, 100, 700]
        # Always return "running", never reaches "stopped"
        mock_get_flink_state.return_value = ("running", None)

        flink_config = FlinkDeploymentConfig(
            "service",
            "pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        success, message = start_stop_restart._wait_for_flink_stopped(
            flink_config,
            system_paasta_config,
            timeout_seconds=600,
            poll_interval_seconds=10,
        )

        assert success is False
        assert "Timeout waiting for service.instance on pnw-devc to stop" in message

    @mock.patch("paasta_tools.cli.cmds.start_stop_restart.time.sleep", autospec=True)
    @mock.patch("paasta_tools.cli.cmds.start_stop_restart.time.time", autospec=True)
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart._get_flink_state", autospec=True
    )
    def test_wait_for_flink_stopped_error_propagation(
        self,
        mock_get_flink_state,
        mock_time,
        mock_sleep,
    ):
        """Test that errors from _get_flink_state are propagated."""
        mock_time.side_effect = [0, 0]
        mock_get_flink_state.return_value = (None, "API error occurred")

        flink_config = FlinkDeploymentConfig(
            "service",
            "pnw-devc",
            "instance",
            FlinkDeploymentConfigDict(),
            None,
        )
        system_paasta_config = mock.Mock()

        success, message = start_stop_restart._wait_for_flink_stopped(
            flink_config,
            system_paasta_config,
            timeout_seconds=60,
            poll_interval_seconds=10,
        )

        assert success is False
        assert "Error getting state: API error occurred" in message


class TestRestartCmdAdditional:
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.apply_args_filters", autospec=True
    )
    def test_paasta_restart_empty_pargs(
        self,
        mock_apply_args_filters,
        capfd,
    ):
        """Test that paasta_restart returns 1 when no instances match filters."""
        mock_apply_args_filters.return_value = {}

        args, _ = parse_args(
            "restart -s service -c cluster -i instance -d /soa/dir".split(" ")
        )
        ret = args.command(args)
        out, _ = capfd.readouterr()

        assert ret == 1
        assert "No instances matched the specified filters" in out

    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.load_system_paasta_config",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart._wait_for_flink_stopped",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart._set_flink_desired_state",
        autospec=True,
    )
    @mock.patch("paasta_tools.cli.cmds.start_stop_restart.paasta_start", autospec=True)
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.get_instance_config", autospec=True
    )
    @mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.apply_args_filters", autospec=True
    )
    def test_flink_restart_wait_timeout(
        self,
        mock_apply_args_filters,
        mock_get_instance_config,
        mock_paasta_start,
        mock_set_flink_desired_state,
        mock_wait_for_flink_stopped,
        mock_load_system_paasta_config,
        capfd,
    ):
        """Test that restart fails when _wait_for_flink_stopped times out."""
        mock_apply_args_filters.return_value = {
            "cluster": {"service": {"flink_instance": None}}
        }
        mock_get_instance_config.side_effect = [
            FlinkDeploymentConfig(
                "service",
                "cluster",
                "flink_instance",
                FlinkDeploymentConfigDict(),
                None,
            ),
        ]
        # Stop succeeds
        mock_set_flink_desired_state.return_value = 0
        # But wait times out
        mock_wait_for_flink_stopped.return_value = (
            False,
            "Timeout waiting for service.flink_instance on cluster to stop",
        )

        args, _ = parse_args(
            "restart -s service -c cluster -i flink_instance -d /soa/dir".split(" ")
        )
        ret = args.command(args)
        out, _ = capfd.readouterr()

        assert ret == 1
        assert "Timeout waiting for" in out
        # Should have called stop but not start
        assert mock_set_flink_desired_state.call_count == 1
        mock_set_flink_desired_state.assert_called_once_with(mock.ANY, "stop", mock.ANY)
