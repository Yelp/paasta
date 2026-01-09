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
import asyncio
from unittest import mock
from unittest.mock import ANY
from unittest.mock import AsyncMock
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import patch

from pytest import fixture
from pytest import raises
from slackclient import SlackClient

from paasta_tools.cli.cmds import mark_for_deployment
from paasta_tools.utils import DeploymentVersion
from paasta_tools.utils import TimeoutError


class FakeArgs:
    deploy_group = "test_deploy_group"
    service = "test_service"
    git_url = "git://false.repo/services/test_services"
    commit = "d670460b4b4aece5915caf5c68d12f560a9fe3e4"
    image_version = "extrastuff"
    soa_dir = "fake_soa_dir"
    block = False
    verbose = False
    auto_rollback = False
    verify_image = False
    timeout = 10.0
    auto_certify_delay = 1.0
    auto_abandon_delay = 1.0
    auto_rollback_delay = 1.0
    authors = None
    warn = 17
    polling_interval = None
    diagnosis_interval = None
    time_before_first_diagnosis = None


@fixture
def mock_periodically_update_slack():
    with patch(
        "paasta_tools.cli.cmds.mark_for_deployment.MarkForDeploymentProcess.periodically_update_slack",
        new_callable=AsyncMock,
        autospec=None,
    ) as periodically_update_slack:
        yield periodically_update_slack


def test_mark_for_deployment_happy():
    with patch(
        "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config",
        autospec=True,
    ) as mock_load_system_paasta_config, patch(
        "paasta_tools.remote_git.create_remote_refs", autospec=True
    ) as mock_create_remote_refs, patch(
        "paasta_tools.cli.cmds.mark_for_deployment._log_audit", autospec=True
    ) as mock__log_audit, patch(
        "paasta_tools.cli.cmds.mark_for_deployment._log", autospec=True
    ):
        config_mock = mock.Mock()
        config_mock.get_default_push_groups.return_value = None
        mock_load_system_paasta_config.return_value = config_mock
        actual = mark_for_deployment.mark_for_deployment(
            git_url="fake_git_url",
            deploy_group="fake_deploy_group",
            service="fake_service",
            commit="fake_commit",
            image_version="extrastuff",
        )
        assert actual == 0
        mock_create_remote_refs.assert_called_once_with(
            git_url="fake_git_url", ref_mutator=ANY, force=True
        )
        mock__log_audit.assert_called_once_with(
            action="mark-for-deployment",
            action_details={
                "deploy_group": "fake_deploy_group",
                "commit": "fake_commit",
                "image_version": "extrastuff",
            },
            service="fake_service",
        )


def test_mark_for_deployment_sad():
    with patch(
        "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config",
        autospec=True,
    ) as mock_load_system_paasta_config, patch(
        "paasta_tools.remote_git.create_remote_refs", autospec=True
    ) as mock_create_remote_refs, patch(
        "paasta_tools.cli.cmds.mark_for_deployment._log_audit", autospec=True
    ) as mock__log_audit, patch(
        "paasta_tools.cli.cmds.mark_for_deployment._log", autospec=True
    ):
        config_mock = mock.Mock()
        config_mock.get_default_push_groups.return_value = None
        mock_load_system_paasta_config.return_value = config_mock
        mock_create_remote_refs.side_effect = Exception("something bad")
        with patch("time.sleep", autospec=True):
            actual = mark_for_deployment.mark_for_deployment(
                git_url="fake_git_url",
                deploy_group="fake_deploy_group",
                service="fake_service",
                commit="fake_commit",
            )
        assert actual == 1
        assert mock_create_remote_refs.call_count == 3
        assert not mock__log_audit.called


def test_paasta_mark_for_deployment_when_verify_image_fails():
    with patch(
        "paasta_tools.cli.cmds.mark_for_deployment.list_deploy_groups", autospec=True
    ) as mock_list_deploy_groups, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_currently_deployed_sha",
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.mark_for_deployment.is_docker_image_already_in_registry",
        autospec=True,
    ) as mock_is_docker_image_already_in_registry, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.validate_service_name", autospec=True
    ):

        class FakeArgsRollback(FakeArgs):
            verify_image = True

        mock_list_deploy_groups.return_value = ["test_deploy_groups"]
        mock_is_docker_image_already_in_registry.return_value = False
        with raises(ValueError):
            mark_for_deployment.paasta_mark_for_deployment(FakeArgsRollback)
        mock_is_docker_image_already_in_registry.assert_called_with(
            "test_service",
            "fake_soa_dir",
            "d670460b4b4aece5915caf5c68d12f560a9fe3e4",
            "extrastuff",
        )


def test_paasta_mark_for_deployment_with_good_rollback():
    with patch(
        "paasta_tools.metrics.metrics_lib.get_metrics_interface", autospec=True
    ) as mock_get_metrics, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config",
        autospec=True,
    ) as mock_load_system_paasta_config, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.list_deploy_groups", autospec=True
    ) as mock_list_deploy_groups, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_currently_deployed_version",
        autospec=True,
    ) as mock_get_currently_deployed_version, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.MarkForDeploymentProcess.do_wait_for_deployment",
        autospec=True,
    ) as mock_do_wait_for_deployment, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment", autospec=True
    ) as mock_mark_for_deployment, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.validate_service_name", autospec=True
    ), patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_slack_client", autospec=True
    ), patch(
        "paasta_tools.cli.cmds.mark_for_deployment._log_audit", autospec=True
    ) as mock__log_audit, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_instance_configs_for_service_in_deploy_group_all_clusters",
        autospec=True,
    ) as mock_get_instance_configs, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.MarkForDeploymentProcess.run_timeout",
        new=1.0,
        autospec=False,
    ):

        class FakeArgsRollback(FakeArgs):
            auto_rollback = True
            block = True
            timeout = 600
            warn = 80  # % of timeout to warn at
            polling_interval = 15
            diagnosis_interval = 15
            time_before_first_diagnosis = 15

        mock_list_deploy_groups.return_value = ["test_deploy_groups"]
        config_mock = mock.Mock()
        config_mock.get_default_push_groups.return_value = None
        mock_load_system_paasta_config.return_value = config_mock
        mock_get_instance_configs.return_value = {
            "fake_cluster": [],
            "fake_cluster2": [],
        }
        mock_mark_for_deployment.return_value = 0

        def do_wait_for_deployment_side_effect(
            self, target_commit, target_image_version
        ):
            if (
                target_commit == FakeArgs.commit
                and target_image_version == FakeArgs.image_version
            ):
                self.trigger("rollback_button_clicked")
            else:
                self.trigger("deploy_finished")

        mock_do_wait_for_deployment.side_effect = do_wait_for_deployment_side_effect

        def on_enter_rolled_back_side_effect(self):
            self.trigger("abandon_button_clicked")

        mock_get_currently_deployed_version.return_value = DeploymentVersion(
            "old-sha", None
        )
        with patch(
            "paasta_tools.cli.cmds.mark_for_deployment.MarkForDeploymentProcess.on_enter_rolled_back",
            autospec=True,
            wraps=mark_for_deployment.MarkForDeploymentProcess.on_enter_rolled_back,
            side_effect=on_enter_rolled_back_side_effect,
        ):
            assert mark_for_deployment.paasta_mark_for_deployment(FakeArgsRollback) == 1

        mock_mark_for_deployment.assert_any_call(
            service="test_service",
            deploy_group="test_deploy_group",
            commit="d670460b4b4aece5915caf5c68d12f560a9fe3e4",
            git_url="git://false.repo/services/test_services",
            image_version="extrastuff",
        )
        mock_mark_for_deployment.assert_any_call(
            service="test_service",
            deploy_group="test_deploy_group",
            commit="old-sha",
            git_url="git://false.repo/services/test_services",
            image_version=None,
        )
        assert mock_mark_for_deployment.call_count == 2

        mock_do_wait_for_deployment.assert_any_call(
            mock.ANY, "d670460b4b4aece5915caf5c68d12f560a9fe3e4", "extrastuff"
        )
        mock_do_wait_for_deployment.assert_any_call(mock.ANY, "old-sha", None)
        assert mock_do_wait_for_deployment.call_count == 2
        # in normal usage, this would also be called once per m-f-d, but we mock that out above
        # so _log_audit is only called as part of handling the rollback
        assert mock__log_audit.call_count == len(mock_list_deploy_groups.return_value)
        mock__log_audit.assert_called_once_with(
            action="rollback",
            action_details={
                "deploy_group": "test_deploy_group",
                "rolled_back_from": "DeploymentVersion(sha=d670460b4b4aece5915caf5c68d12f560a9fe3e4, image_version=extrastuff)",
                "rolled_back_to": "old-sha",
                "rollback_type": "user_initiated_rollback",
            },
            service="test_service",
        )

        mock_get_metrics.assert_called_once_with("paasta.mark_for_deployment")
        mock_get_metrics.return_value.create_timer.assert_called_once_with(
            name="deploy_duration",
            default_dimensions=dict(
                paasta_service="test_service",
                deploy_group="test_deploy_group",
                old_version="old-sha",
                new_version="DeploymentVersion(sha=d670460b4b4aece5915caf5c68d12f560a9fe3e4, image_version=extrastuff)",
                deploy_timeout=600,
            ),
        )
        mock_timer = mock_get_metrics.return_value.create_timer.return_value
        mock_timer.start.assert_called_once_with()
        mock_timer.stop.assert_called_once_with(tmp_dimensions=dict(exit_status=1))
        mock_emit_event = mock_get_metrics.return_value.emit_event
        event_dimensions = dict(
            paasta_service="test_service",
            deploy_group="test_deploy_group",
            rolled_back_from="DeploymentVersion(sha=d670460b4b4aece5915caf5c68d12f560a9fe3e4, image_version=extrastuff)",
            rolled_back_to="old-sha",
            rollback_type="user_initiated_rollback",
        )
        expected_calls = []
        for cluster in mock_get_instance_configs.return_value.keys():
            dims = dict(event_dimensions)
            dims["paasta_cluster"] = cluster
            exp_call = call(name="rollback", dimensions=dims)
            expected_calls.append(exp_call)
        mock_emit_event.assert_has_calls(expected_calls, any_order=True)


def test_mark_for_deployment_yelpy_repo():
    with patch(
        "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config",
        autospec=True,
    ) as mock_load_system_paasta_config, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.trigger_deploys", autospec=True
    ) as mock_trigger_deploys, patch(
        "paasta_tools.remote_git.create_remote_refs", autospec=True
    ), patch(
        "paasta_tools.cli.cmds.mark_for_deployment._log", autospec=True
    ), patch(
        "paasta_tools.cli.cmds.mark_for_deployment._log_audit", autospec=True
    ):
        config_mock = mock.Mock()
        config_mock.get_default_push_groups.return_value = None
        mock_load_system_paasta_config.return_value = config_mock
        mark_for_deployment.mark_for_deployment(
            git_url="git://false.repo.yelpcorp.com/services/test_services",
            deploy_group="fake_deploy_group",
            service="fake_service",
            commit="fake_commit",
        )
        mock_trigger_deploys.assert_called_once_with(service="fake_service")


def test_mark_for_deployment_nonyelpy_repo():
    with patch(
        "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config",
        autospec=True,
    ) as mock_load_system_paasta_config, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.trigger_deploys", autospec=True
    ) as mock_trigger_deploys, patch(
        "paasta_tools.remote_git.create_remote_refs", autospec=True
    ), patch(
        "paasta_tools.cli.cmds.mark_for_deployment._log", autospec=True
    ), patch(
        "paasta_tools.cli.cmds.mark_for_deployment._log_audit", autospec=True
    ):
        config_mock = mock.Mock()
        config_mock.get_default_push_groups.return_value = None
        mock_load_system_paasta_config.return_value = config_mock

        mark_for_deployment.mark_for_deployment(
            git_url="git://false.repo/services/test_services",
            deploy_group="fake_deploy_group",
            service="fake_service",
            commit="fake_commit",
        )
        assert not mock_trigger_deploys.called


def test_MarkForDeployProcess_handles_wait_for_deployment_failure():
    with patch("sticht.rollbacks.slo.get_slos_for_service", autospec=True), patch(
        "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config",
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment", autospec=True
    ) as mock_wait_for_deployment, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment", autospec=True
    ) as mock_mark_for_deployment, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_slack_client", autospec=True
    ), patch(
        "paasta_tools.remote_git.get_authors", autospec=True
    ) as mock_get_authors, patch(
        "paasta_tools.cli.cmds.mark_for_deployment._log_audit", autospec=True
    ) as mock__log_audit, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_instance_configs_for_service_in_deploy_group_all_clusters",
        autospec=True,
    ):
        mock_get_authors.return_value = 0, "fakeuser1 fakeuser2"
        mfdp = mark_for_deployment.MarkForDeploymentProcess(
            service="service",
            block=True,
            auto_rollback=True,
            deploy_info={
                "pipeline": [{"step": "test_deploy_group", "slack_notify": False}],
                "slack_channels": ["#test"],
            },
            deploy_group="test_deploy_group",
            commit="abc123432u49",
            old_git_sha="abc123455",
            git_url="git@git.yelpcorp.com:services/repo",
            soa_dir=None,
            timeout=None,
            warn_pct=None,
            auto_certify_delay=1,
            auto_abandon_delay=1,
            auto_rollback_delay=1,
        )

        mock_mark_for_deployment.return_value = 0
        mock_wait_for_deployment.side_effect = Exception()

        retval = mfdp.run()

        assert mock_mark_for_deployment.call_count == 1
        assert mock_wait_for_deployment.call_count == 1
        assert mfdp.state == "deploy_errored"
        assert retval == 2
        assert not mock__log_audit.called


def test_MarkForDeployProcess_handles_first_time_deploys(
    mock_periodically_update_slack,
):
    with patch("sticht.rollbacks.slo.get_slos_for_service", autospec=True), patch(
        "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config",
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment", autospec=True
    ) as mock_wait_for_deployment, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment", autospec=True
    ) as mock_mark_for_deployment, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_slack_client", autospec=True
    ), patch(
        "paasta_tools.remote_git.get_authors", autospec=True
    ) as mock_get_authors, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_instance_configs_for_service_in_deploy_group_all_clusters",
        autospec=True,
    ):
        mock_get_authors.return_value = 0, "fakeuser1 fakeuser2"
        mfdp = mark_for_deployment.MarkForDeploymentProcess(
            service="service",
            block=True,
            auto_rollback=True,
            deploy_info=MagicMock(),
            deploy_group=None,
            commit="abc123432u49",
            old_git_sha=None,
            git_url="git@git.yelpcorp.com:services/repo",
            soa_dir=None,
            timeout=None,
            warn_pct=None,
            auto_certify_delay=1,
            auto_abandon_delay=1,
            auto_rollback_delay=1,
        )

        mock_mark_for_deployment.return_value = 0
        mock_wait_for_deployment.side_effect = Exception()

        retval = mfdp.run()

        assert mock_mark_for_deployment.call_count == 1
        assert mock_wait_for_deployment.call_count == 1
        assert mfdp.state == "deploy_errored"
        assert retval == 2


def test_MarkForDeployProcess_get_authors_diffs_against_prod_deploy_group():
    with patch(
        "sticht.rollbacks.slo.get_slos_for_service", autospec=True
    ), patch.object(
        mark_for_deployment, "load_system_paasta_config", autospec=True
    ), patch.object(
        mark_for_deployment, "get_slack_client", autospec=True
    ), patch.object(
        mark_for_deployment, "get_currently_deployed_sha", autospec=True
    ) as mock_get_currently_deployed_sha, patch.object(
        mark_for_deployment, "get_authors_to_be_notified", autospec=True
    ) as mock_get_authors_to_be_notified, patch.object(
        mark_for_deployment,
        "get_instance_configs_for_service_in_deploy_group_all_clusters",
        autospec=True,
    ):
        # get_authors should calculate authors since the production_deploy_group's
        # current SHA, when available.
        mock_get_currently_deployed_sha.return_value = "aaaaaaaa"
        mfdp = mark_for_deployment.MarkForDeploymentProcess(
            service="service",
            block=True,
            auto_rollback=False,
            deploy_info={"production_deploy_group": "prod"},
            deploy_group=None,
            commit="abc123512",
            old_git_sha="asgdser23",
            git_url="git@git.yelpcorp.com:services/repo",
            soa_dir=None,
            timeout=None,
            warn_pct=None,
            auto_certify_delay=1,
            auto_abandon_delay=1,
            auto_rollback_delay=1,
            authors=["fakeuser1"],
        )
        mfdp.get_authors()
        mock_get_authors_to_be_notified.assert_called_once_with(
            git_url="git@git.yelpcorp.com:services/repo",
            from_sha="aaaaaaaa",
            to_sha="abc123512",
            authors=["fakeuser1"],
        )


def test_MarkForDeployProcess_get_authors_falls_back_to_current_deploy_group():
    with patch(
        "sticht.rollbacks.slo.get_slos_for_service", autospec=True
    ), patch.object(
        mark_for_deployment, "load_system_paasta_config", autospec=True
    ), patch.object(
        mark_for_deployment, "get_slack_client", autospec=True
    ), patch.object(
        mark_for_deployment, "get_currently_deployed_sha", autospec=True
    ), patch.object(
        mark_for_deployment, "get_authors_to_be_notified", autospec=True
    ) as mock_get_authors_to_be_notified, patch.object(
        mark_for_deployment,
        "get_instance_configs_for_service_in_deploy_group_all_clusters",
        autospec=True,
    ):
        # When there's no production_deploy_group configured, get_authors should
        # fall back to calculating authors using the previous SHA for this deploy
        # group.
        mfdp = mark_for_deployment.MarkForDeploymentProcess(
            service="service",
            block=True,
            auto_rollback=False,
            # No production_deploy_group!
            deploy_info={},
            deploy_group=None,
            commit="abc123512",
            old_git_sha="asgdser23",
            git_url="git@git.yelpcorp.com:services/repo1",
            soa_dir=None,
            timeout=None,
            warn_pct=None,
            auto_certify_delay=1,
            auto_abandon_delay=1,
            auto_rollback_delay=1,
            authors="fakeuser1",
        )
        mfdp.get_authors()
        mock_get_authors_to_be_notified.assert_called_once_with(
            git_url="git@git.yelpcorp.com:services/repo1",
            from_sha="asgdser23",
            to_sha="abc123512",
            authors="fakeuser1",
        )


def test_MarkForDeployProcess_handles_wait_for_deployment_cancelled(
    mock_periodically_update_slack,
):
    with patch("sticht.rollbacks.slo.get_slos_for_service", autospec=True), patch(
        "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config",
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment",
        new_callable=AsyncMock,
    ) as mock_wait_for_deployment, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment", autospec=True
    ) as mock_mark_for_deployment, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_slack_client", autospec=True
    ), patch(
        "paasta_tools.remote_git.get_authors", autospec=True
    ) as mock_get_authors, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_instance_configs_for_service_in_deploy_group_all_clusters",
        autospec=True,
    ):
        mock_get_authors.return_value = 0, "fakeuser1 fakeuser2"
        mfdp = mark_for_deployment.MarkForDeploymentProcess(
            service="service",
            block=True,
            # For this test, auto_rollback must be True so that the deploy_cancelled trigger takes us to start_rollback
            # instead of deploy_errored.
            auto_rollback=True,
            deploy_info=MagicMock(),
            deploy_group=None,
            commit="abc123512",
            old_git_sha="asgdser23",
            git_url="git@git.yelpcorp.com:services/repo1",
            soa_dir=None,
            timeout=None,
            warn_pct=None,
            auto_certify_delay=1,
            auto_abandon_delay=1,
            auto_rollback_delay=1,
        )

        mock_mark_for_deployment.return_value = 0
        # This should really be a KeyboardInterrupt - but something about the mock -> unittest.mock
        # migration has this test hang if we use KeyboardInterrupt here.
        # XXX: why?!?
        mock_wait_for_deployment.side_effect = TimeoutError()

        retval = mfdp.run()

        assert mock_mark_for_deployment.call_count == 1
        assert retval == 1
        assert mfdp.state == "deploy_cancelled"


def test_MarkForDeployProcess_skips_wait_for_deployment_when_block_is_False():
    with patch(
        "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config",
        autospec=True,
    ), patch("sticht.rollbacks.slo.get_slos_for_service", autospec=True), patch(
        "sticht.slack.get_slack_events", autospec=True
    ), patch(
        "paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment", autospec=True
    ) as mock_wait_for_deployment, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment", autospec=True
    ) as mock_mark_for_deployment, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_slack_client", autospec=True
    ), patch(
        "paasta_tools.cli.cmds.mark_for_deployment.Thread", autospec=True
    ), patch(
        "paasta_tools.remote_git.get_authors", autospec=True
    ) as mock_get_authors, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_instance_configs_for_service_in_deploy_group_all_clusters",
        autospec=True,
    ):
        mock_get_authors.return_value = 0, "fakeuser1 fakeuser2"
        mfdp = mark_for_deployment.MarkForDeploymentProcess(
            service="service",
            block=False,
            auto_rollback=False,
            deploy_info=MagicMock(),
            deploy_group=None,
            commit="abc123456789",
            old_git_sha="oldsha1234",
            git_url="git@git.yelpcorp.com:services/repo1",
            soa_dir=None,
            timeout=None,
            warn_pct=None,
            auto_certify_delay=1,
            auto_abandon_delay=1,
            auto_rollback_delay=1,
        )

        mock_mark_for_deployment.return_value = 0
        mock_wait_for_deployment.side_effect = Exception()

        retval = mfdp.run()

        assert mock_mark_for_deployment.call_count == 1
        assert mock_wait_for_deployment.call_count == 0
        assert retval == 0
        assert mfdp.state == "deploying"


def test_MarkForDeployProcess_goes_to_mfd_failed_when_mark_for_deployment_fails(
    mock_periodically_update_slack,
):
    with patch("sticht.rollbacks.slo.get_slos_for_service", autospec=True), patch(
        "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config",
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment", autospec=True
    ) as mock_wait_for_deployment, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment", autospec=True
    ) as mock_mark_for_deployment, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_slack_client", autospec=True
    ), patch(
        "paasta_tools.remote_git.get_authors", autospec=True
    ) as mock_get_authors, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_instance_configs_for_service_in_deploy_group_all_clusters",
        autospec=True,
    ):
        mock_get_authors.return_value = 0, "fakeuser1 fakeuser2"
        mfdp = mark_for_deployment.MarkForDeploymentProcess(
            service="service",
            block=False,  # shouldn't matter for this test
            auto_rollback=False,  # shouldn't matter for this test
            deploy_info=MagicMock(),
            deploy_group=None,
            commit="asbjkslerj",
            old_git_sha="abscerwerr",
            git_url="git@git.yelpcorp.com:services/repo1",
            soa_dir=None,
            timeout=None,
            warn_pct=None,
            auto_certify_delay=1,
            auto_abandon_delay=1,
            auto_rollback_delay=1,
        )

        mock_mark_for_deployment.return_value = 1
        mock_wait_for_deployment.side_effect = Exception()

        retval = mfdp.run()

        assert mock_mark_for_deployment.call_count == 1
        assert mock_wait_for_deployment.call_count == 0
        assert retval == 1
        assert mfdp.state == "mfd_failed"


class WrappedMarkForDeploymentProcess(mark_for_deployment.MarkForDeploymentProcess):
    def __init__(self, *args, **kwargs):
        self.trigger_history = []
        self.state_history = []
        super().__init__(*args, **kwargs)
        self.orig_trigger = self.trigger
        self.trigger = self._trigger

    def _trigger(self, trigger_name):
        self.trigger_history.append(trigger_name)
        self.orig_trigger(trigger_name)

    def get_slack_client(self):
        fake_slack_client = mock.MagicMock(spec=SlackClient)
        fake_slack_client.api_call.return_value = {
            "ok": True,
            "message": {"ts": 1234531337},
            "channel": "FAKE CHANNEL ID",
        }
        return fake_slack_client

    def start_timer(self, timeout, trigger, message_verb):
        return super().start_timer(0, trigger, message_verb)

    def after_state_change(self, *args, **kwargs):
        self.state_history.append(self.state)
        super().after_state_change(*args, **kwargs)

    def start_slo_watcher_threads(self, service, soa_dir):
        pass


def test_MarkForDeployProcess_happy_path(
    mock_periodically_update_slack,
):
    with patch(
        "paasta_tools.cli.cmds.mark_for_deployment._log", autospec=True
    ) as mock_log, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment", autospec=True
    ) as mock_wait_for_deployment, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment",
        return_value=0,
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_instance_configs_for_service_in_deploy_group_all_clusters",
        autospec=True,
    ):
        mock_wait_for_deployment.return_value = asyncio.sleep(
            0
        )  # make mock wait_for_deployment awaitable.
        mock_log.return_value = None
        mfdp = WrappedMarkForDeploymentProcess(
            service="service",
            deploy_info=MagicMock(),
            deploy_group="deploy_group",
            commit="commit",
            old_git_sha="old_git_sha",
            git_url="git_url",
            auto_rollback=True,
            block=True,
            soa_dir="soa_dir",
            timeout=3600,
            warn_pct=50,
            auto_certify_delay=None,
            auto_abandon_delay=600,
            auto_rollback_delay=30,
            authors=None,
        )

        mfdp.run_timeout = 1
        assert mfdp.run() == 0
        assert mfdp.trigger_history == [
            "start_deploy",
            "mfd_succeeded",
            "deploy_finished",
            "auto_certify",
        ]
        assert mfdp.state_history == [
            "start_deploy",
            "deploying",
            "deployed",
            "complete",
        ]


def test_MarkForDeployProcess_happy_path_skips_complete_if_no_auto_rollback(
    mock_periodically_update_slack,
):
    with patch(
        "paasta_tools.cli.cmds.wait_for_deployment._log", autospec=True
    ) as mock__log1, patch(
        "paasta_tools.cli.cmds.mark_for_deployment._log", autospec=True
    ) as mock__log2, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment", autospec=True
    ) as mock_wait_for_deployment, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment",
        return_value=0,
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_instance_configs_for_service_in_deploy_group_all_clusters",
        autospec=True,
    ):
        mock_wait_for_deployment.return_value = asyncio.sleep(
            0
        )  # make mock wait_for_deployment awaitable.
        mock__log1.return_value = None
        mock__log2.return_value = None
        mfdp = WrappedMarkForDeploymentProcess(
            service="service",
            deploy_info=MagicMock(),
            deploy_group="deploy_group",
            commit="commit",
            old_git_sha="old_git_sha",
            git_url="git_url",
            auto_rollback=False,
            block=True,
            soa_dir="soa_dir",
            timeout=3600,
            warn_pct=50,
            auto_certify_delay=None,
            auto_abandon_delay=600,
            auto_rollback_delay=30,
            authors=None,
        )

        mfdp.run_timeout = 1
        assert mfdp.run() == 0
        assert mfdp.trigger_history == [
            "start_deploy",
            "mfd_succeeded",
            "deploy_finished",
        ]
        assert mfdp.state_history == ["start_deploy", "deploying", "deployed"]


def test_MarkForDeployProcess_get_available_buttons_failing_slos_show_disable_rollback():
    with patch(
        "paasta_tools.cli.cmds.mark_for_deployment.MarkForDeploymentProcess.any_slo_failing",
        autospec=True,
    ) as mock_any_slo_failing, patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_instance_configs_for_service_in_deploy_group_all_clusters",
        autospec=True,
    ):
        mock_any_slo_failing.return_value = True
        mfdp = WrappedMarkForDeploymentProcess(
            service="service",
            deploy_info=MagicMock(),
            deploy_group="deploy_group",
            commit="commit",
            old_git_sha="old_git_sha",
            git_url="git_url",
            auto_rollback=True,
            block=True,
            soa_dir="soa_dir",
            timeout=3600,
            warn_pct=50,
            auto_certify_delay=None,
            auto_abandon_delay=600,
            auto_rollback_delay=30,
            authors=None,
        )

        # Test only get_available_buttons
        mfdp.run_timeout = 1
        mfdp.state = "deploying"
        assert "disable_auto_rollbacks" in mfdp.get_available_buttons()
        assert "enable_auto_rollbacks" not in mfdp.get_available_buttons()

        mock_any_slo_failing.return_value = True
        mfdp = WrappedMarkForDeploymentProcess(
            service="service",
            deploy_info=MagicMock(),
            deploy_group="deploy_group",
            commit="commit",
            old_git_sha="old_git_sha",
            git_url="git_url",
            auto_rollback=False,
            block=True,
            soa_dir="soa_dir",
            timeout=3600,
            warn_pct=50,
            auto_certify_delay=None,
            auto_abandon_delay=600,
            auto_rollback_delay=30,
            authors=None,
        )

        mfdp.run_timeout = 1
        mfdp.state = "deploying"
        assert "disable_auto_rollbacks" not in mfdp.get_available_buttons()
        assert "enable_auto_rollbacks" in mfdp.get_available_buttons()


def test_MarkForDeployProcess_send_manual_rollback_instructions_with_no_old_git_sha():
    """Test that rollback instructions are not sent when old_git_sha is None (new deploy group)."""
    with mock.patch(
        "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config",
        autospec=True,
    ), mock.patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_instance_configs_for_service_in_deploy_group_all_clusters",
        autospec=True,
    ):
        mfdp = WrappedMarkForDeploymentProcess(
            service="service",
            deploy_info=MagicMock(),
            deploy_group="new_deploy_group",
            commit="new_commit_sha",
            old_git_sha=None,
            git_url="git_url",
            auto_rollback=False,
            block=False,
            soa_dir="soa_dir",
            timeout=3600,
            warn_pct=50,
            auto_certify_delay=None,
            auto_abandon_delay=600,
            auto_rollback_delay=30,
            authors=None,
        )

        mfdp.update_slack_thread = mock.Mock(autospec=True)

        with mock.patch("builtins.print", autospec=True) as mock_print:
            mfdp.send_manual_rollback_instructions()

        mfdp.update_slack_thread.assert_not_called()
        mock_print.assert_not_called()


def test_MarkForDeployProcess_send_manual_rollback_instructions_with_old_git_sha():
    """Test that rollback instructions are sent when old_git_sha is set."""
    with mock.patch(
        "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config",
        autospec=True,
    ), mock.patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_instance_configs_for_service_in_deploy_group_all_clusters",
        autospec=True,
    ):
        mfdp = WrappedMarkForDeploymentProcess(
            service="service",
            deploy_info=MagicMock(),
            deploy_group="deploy_group",
            commit="new_commit_sha",
            old_git_sha="old_commit_sha",
            git_url="git_url",
            auto_rollback=False,
            block=False,
            soa_dir="soa_dir",
            timeout=3600,
            warn_pct=50,
            auto_certify_delay=None,
            auto_abandon_delay=600,
            auto_rollback_delay=30,
            authors=None,
        )

        mfdp.update_slack_thread = mock.Mock(autospec=True)

        with mock.patch("builtins.print", autospec=True) as mock_print:
            mfdp.send_manual_rollback_instructions()

        mock_print.assert_called_once()
        assert "--commit old_commit_sha" in mock_print.call_args[0][0]


def test_MarkForDeployProcess_send_manual_rollback_instructions_same_version():
    """Test that rollback instructions are not sent when deployment versions are the same."""
    same_sha = "same_commit_sha"
    with mock.patch(
        "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config",
        autospec=True,
    ), mock.patch(
        "paasta_tools.cli.cmds.mark_for_deployment.get_instance_configs_for_service_in_deploy_group_all_clusters",
        autospec=True,
    ):
        mfdp = WrappedMarkForDeploymentProcess(
            service="service",
            deploy_info=MagicMock(),
            deploy_group="deploy_group",
            commit=same_sha,
            old_git_sha=same_sha,
            git_url="git_url",
            auto_rollback=False,
            block=False,
            soa_dir="soa_dir",
            timeout=3600,
            warn_pct=50,
            auto_certify_delay=None,
            auto_abandon_delay=600,
            auto_rollback_delay=30,
            authors=None,
        )

        mfdp.update_slack_thread = mock.Mock(autospec=True)

        with mock.patch("builtins.print", autospec=True) as mock_print:
            mfdp.send_manual_rollback_instructions()

        mfdp.update_slack_thread.assert_not_called()
        mock_print.assert_not_called()
