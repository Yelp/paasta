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
import mock

from paasta_tools import remote_git
from paasta_tools.cassandracluster_tools import CassandraClusterDeploymentConfig
from paasta_tools.cassandracluster_tools import CassandraClusterDeploymentConfigDict
from paasta_tools.cli.cli import parse_args
from paasta_tools.cli.cmds import start_stop_restart
from paasta_tools.flink_tools import FlinkDeploymentConfig
from paasta_tools.flink_tools import FlinkDeploymentConfigDict
from paasta_tools.marathon_tools import MarathonServiceConfig


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
        MarathonServiceConfig(
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
        service_config=MarathonServiceConfig(
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
        service_config = MarathonServiceConfig(
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
    mock_get_latest_deployment_tag.return_value = ("not_a_real_tag", None)
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
    mock_get_latest_deployment_tag.return_value = ("not_a_real_tag", None)
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
    mock_get_latest_deployment_tag.return_value = ("not_a_real_tag", None)
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

    mock_get_instance_config.return_value = MarathonServiceConfig(
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


def test_cluster_list_defaults_to_all():
    return True


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
    mock_get_latest_deployment_tag.return_value = ("not_a_real_tag", None)
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


@mock.patch("choice.basicterm.BasicTermBinaryChoice", autospec=True)
@mock.patch("choice.Binary", autospec=True)
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
        mock_binary,
        mock_binary_choice,
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
        mock_paasta_start.return_value = 1
        mock_binary_choice.ask.return_value = True
        mock_binary.return_value = mock_binary_choice

        args, _ = parse_args(
            "restart -s service -c cluster -i flink_instance,cas_instance -d /soa/dir".split(
                " "
            )
        )
        ret = args.command(args)
        out, _ = capfd.readouterr()

        assert ret == 1
        assert mock_paasta_start.called
        assert "paasta restart is currently unsupported for Flink instances" in out

    def test_only_non_flink_instances(
        self,
        mock_apply_args_filters,
        mock_get_instance_config,
        mock_paasta_start,
        mock_binary,
        mock_binary_choice,
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
        assert "paasta restart is currently unsupported for Flink instances" not in out

    def test_only_flink_instances(
        self,
        mock_apply_args_filters,
        mock_get_instance_config,
        mock_paasta_start,
        mock_binary,
        mock_binary_choice,
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

        args, _ = parse_args(
            "restart -s service -c cluster -i flink_instance -d /soa/dir".split(" ")
        )
        ret = args.command(args)
        out, _ = capfd.readouterr()

        assert ret == 1
        assert not mock_paasta_start.called
        assert "paasta restart is currently unsupported for Flink instances" in out
