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

import asynctest
import mock
from mock import ANY
from mock import MagicMock
from mock import patch
from pytest import fixture
from pytest import raises
from slackclient import SlackClient

from paasta_tools.cli.cmds import mark_for_deployment


class FakeArgs:
    deploy_group = "test_deploy_group"
    service = "test_service"
    git_url = "git://false.repo/services/test_services"
    commit = "d670460b4b4aece5915caf5c68d12f560a9fe3e4"
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


@fixture
def mock_periodically_update_slack():
    # for some reason asynctest.patch doesn't work as a decorator, so I've defined this fixture.
    with asynctest.patch(
        "paasta_tools.cli.cmds.mark_for_deployment.MarkForDeploymentProcess.periodically_update_slack",
        autospec=True,
    ) as periodically_update_slack:
        yield periodically_update_slack


@patch("paasta_tools.cli.cmds.mark_for_deployment._log", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment._log_audit", autospec=True)
@patch("paasta_tools.remote_git.create_remote_refs", autospec=True)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config", autospec=True
)
def test_mark_for_deployment_happy(
    mock_load_system_paasta_config, mock_create_remote_refs, mock__log_audit, mock__log
):
    config_mock = mock.Mock()
    config_mock.get_default_push_groups.return_value = None
    mock_load_system_paasta_config.return_value = config_mock
    actual = mark_for_deployment.mark_for_deployment(
        git_url="fake_git_url",
        deploy_group="fake_deploy_group",
        service="fake_service",
        commit="fake_commit",
    )
    assert actual == 0
    mock_create_remote_refs.assert_called_once_with(
        git_url="fake_git_url", ref_mutator=ANY, force=True
    )
    mock__log_audit.assert_called_once_with(
        action="mark-for-deployment",
        action_details={"deploy_group": "fake_deploy_group", "commit": "fake_commit"},
        service="fake_service",
    )


@patch("paasta_tools.cli.cmds.mark_for_deployment._log", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment._log_audit", autospec=True)
@patch("paasta_tools.remote_git.create_remote_refs", autospec=True)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config", autospec=True
)
def test_mark_for_deployment_sad(
    mock_load_system_paasta_config, mock_create_remote_refs, mock__log_audit, mock__log
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


@patch("paasta_tools.cli.cmds.mark_for_deployment.validate_service_name", autospec=True)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.is_docker_image_already_in_registry",
    autospec=True,
)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.get_currently_deployed_sha",
    autospec=True,
)
@patch("paasta_tools.cli.cmds.mark_for_deployment.list_deploy_groups", autospec=True)
def test_paasta_mark_for_deployment_when_verify_image_fails(
    mock_list_deploy_groups,
    mock_get_currently_deployed_sha,
    mock_is_docker_image_already_in_registry,
    mock_validate_service_name,
):
    class FakeArgsRollback(FakeArgs):
        verify_image = True

    mock_list_deploy_groups.return_value = ["test_deploy_groups"]
    mock_is_docker_image_already_in_registry.return_value = False
    with raises(ValueError):
        mark_for_deployment.paasta_mark_for_deployment(FakeArgsRollback)


@patch("paasta_tools.cli.cmds.mark_for_deployment.validate_service_name", autospec=True)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.is_docker_image_already_in_registry",
    autospec=True,
)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.get_currently_deployed_sha",
    autospec=True,
)
@patch("paasta_tools.cli.cmds.mark_for_deployment.list_deploy_groups", autospec=True)
def test_paasta_mark_for_deployment_when_verify_image_succeeds(
    mock_list_deploy_groups,
    mock_get_currently_deployed_sha,
    mock_is_docker_image_already_in_registry,
    mock_validate_service_name,
):
    class FakeArgsRollback(FakeArgs):
        verify_image = True

    mock_list_deploy_groups.return_value = ["test_deploy_groups"]
    mock_is_docker_image_already_in_registry.return_value = False
    with raises(ValueError):
        mark_for_deployment.paasta_mark_for_deployment(FakeArgsRollback)
    mock_is_docker_image_already_in_registry.assert_called_with(
        "test_service", "fake_soa_dir", "d670460b4b4aece5915caf5c68d12f560a9fe3e4"
    )


@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.MarkForDeploymentProcess.run_timeout",
    new=1.0,
    autospec=False,
)
@patch("paasta_tools.cli.cmds.mark_for_deployment._log_audit", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.get_slack_client", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment", autospec=True)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.MarkForDeploymentProcess.do_wait_for_deployment",
    autospec=True,
)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.get_currently_deployed_sha",
    autospec=True,
)
@patch("paasta_tools.cli.cmds.mark_for_deployment.list_deploy_groups", autospec=True)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config", autospec=True
)
@patch("paasta_tools.metrics.metrics_lib.get_metrics_interface", autospec=True)
def test_paasta_mark_for_deployment_with_good_rollback(
    mock_get_metrics,
    mock_load_system_paasta_config,
    mock_list_deploy_groups,
    mock_get_currently_deployed_sha,
    mock_do_wait_for_deployment,
    mock_mark_for_deployment,
    mock_validate_service_name,
    mock_get_slack_client,
    mock__log_audit,
    mock_periodically_update_slack,
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
    mock_mark_for_deployment.return_value = 0

    def do_wait_for_deployment_side_effect(self, target_commit):
        if target_commit == FakeArgs.commit:
            self.trigger("rollback_button_clicked")
        else:
            self.trigger("deploy_finished")

    mock_do_wait_for_deployment.side_effect = do_wait_for_deployment_side_effect

    def on_enter_rolled_back_side_effect(self):
        self.trigger("abandon_button_clicked")

    mock_get_currently_deployed_sha.return_value = "old-sha"
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
    )
    mock_mark_for_deployment.assert_any_call(
        service="test_service",
        deploy_group="test_deploy_group",
        commit="old-sha",
        git_url="git://false.repo/services/test_services",
    )
    assert mock_mark_for_deployment.call_count == 2

    mock_do_wait_for_deployment.assert_any_call(
        mock.ANY, "d670460b4b4aece5915caf5c68d12f560a9fe3e4"
    )
    mock_do_wait_for_deployment.assert_any_call(mock.ANY, "old-sha")
    assert mock_do_wait_for_deployment.call_count == 2
    # in normal usage, this would also be called once per m-f-d, but we mock that out above
    # so _log_audit is only called as part of handling the rollback
    assert mock__log_audit.call_count == len(mock_list_deploy_groups.return_value)

    mock_get_metrics.assert_called_once_with("paasta.mark_for_deployment")
    mock_get_metrics.return_value.create_timer.assert_called_once_with(
        name="deploy_duration",
        default_dimensions=dict(
            paasta_service="test_service",
            deploy_group="test_deploy_group",
            old_version="old-sha",
            new_version="d670460b4b4aece5915caf5c68d12f560a9fe3e4",
            deploy_timeout=600,
        ),
    )
    mock_timer = mock_get_metrics.return_value.create_timer.return_value
    mock_timer.start.assert_called_once_with()
    mock_timer.stop.assert_called_once_with(tmp_dimensions=dict(exit_status=1))


@patch("paasta_tools.cli.cmds.mark_for_deployment._log_audit", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment._log", autospec=True)
@patch("paasta_tools.remote_git.create_remote_refs", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.trigger_deploys", autospec=True)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config", autospec=True
)
def test_mark_for_deployment_yelpy_repo(
    mock_load_system_paasta_config,
    mock_trigger_deploys,
    mock_create_remote_refs,
    mock__log,
    mock__log_audit,
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


@patch("paasta_tools.cli.cmds.mark_for_deployment._log_audit", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment._log", autospec=True)
@patch("paasta_tools.remote_git.create_remote_refs", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.trigger_deploys", autospec=True)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config", autospec=True
)
def test_mark_for_deployment_nonyelpy_repo(
    mock_load_system_paasta_config,
    mock_trigger_deploys,
    mock_create_remote_refs,
    mock__log,
    mock__log_audit,
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


@patch("paasta_tools.cli.cmds.mark_for_deployment._log_audit", autospec=True)
@patch("paasta_tools.remote_git.get_authors", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.get_slack_client", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment", autospec=True)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config", autospec=True
)
@patch("sticht.slo.get_slos_for_service", autospec=True)
def test_MarkForDeployProcess_handles_wait_for_deployment_failure(
    mock_get_slos_for_service,
    mock_load_system_paasta_config,
    mock_wait_for_deployment,
    mock_mark_for_deployment,
    mock_get_slack_client,
    mock_get_authors,
    mock__log_audit,
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


@patch("paasta_tools.remote_git.get_authors", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.get_slack_client", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment", autospec=True)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config", autospec=True
)
@patch("sticht.slo.get_slos_for_service", autospec=True)
def test_MarkForDeployProcess_handles_first_time_deploys(
    mock_get_slos_for_service,
    mock_load_system_paasta_config,
    mock_wait_for_deployment,
    mock_mark_for_deployment,
    mock_get_slack_client,
    mock_get_authors,
    mock_periodically_update_slack,
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


@patch.object(mark_for_deployment, "get_authors_to_be_notified", autospec=True)
@patch.object(mark_for_deployment, "get_currently_deployed_sha", autospec=True)
@patch.object(mark_for_deployment, "get_slack_client", autospec=True)
@patch.object(mark_for_deployment, "load_system_paasta_config", autospec=True)
@patch("sticht.slo.get_slos_for_service", autospec=True)
def test_MarkForDeployProcess_get_authors_diffs_against_prod_deploy_group(
    mock_get_slos_for_service,
    mock_load_system_paasta_config,
    mock_get_slack_client,
    mock_get_currently_deployed_sha,
    mock_get_authors_to_be_notified,
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


@patch.object(mark_for_deployment, "get_authors_to_be_notified", autospec=True)
@patch.object(mark_for_deployment, "get_currently_deployed_sha", autospec=True)
@patch.object(mark_for_deployment, "get_slack_client", autospec=True)
@patch.object(mark_for_deployment, "load_system_paasta_config", autospec=True)
@patch("sticht.slo.get_slos_for_service", autospec=True)
def test_MarkForDeployProcess_get_authors_falls_back_to_current_deploy_group(
    mock_get_slos_for_service,
    mock_load_system_paasta_config,
    mock_get_slack_client,
    mock_get_currently_deployed_sha,
    mock_get_authors_to_be_notified,
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


@patch("paasta_tools.remote_git.get_authors", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.get_slack_client", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment", autospec=True)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config", autospec=True
)
@patch("sticht.slo.get_slos_for_service", autospec=True)
def test_MarkForDeployProcess_handles_wait_for_deployment_cancelled(
    mock_get_slos_for_service,
    mock_load_system_paasta_config,
    mock_wait_for_deployment,
    mock_mark_for_deployment,
    mock_get_slack_client,
    mock_get_authors,
    mock_periodically_update_slack,
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
    mock_wait_for_deployment.side_effect = KeyboardInterrupt()

    retval = mfdp.run()

    assert mock_mark_for_deployment.call_count == 1
    assert retval == 1
    assert mfdp.state == "deploy_cancelled"


@patch("paasta_tools.remote_git.get_authors", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.Thread", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.get_slack_client", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment", autospec=True)
@patch("sticht.slack.get_slack_events", autospec=True)
@patch("sticht.slo.get_slos_for_service", autospec=True)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config", autospec=True
)
def test_MarkForDeployProcess_skips_wait_for_deployment_when_block_is_False(
    mock_load_system_paasta_config,
    mock_get_slos_for_service,
    mock_get_slack_events,
    mock_wait_for_deployment,
    mock_mark_for_deployment,
    mock_get_slack_client,
    mock_Thread,
    mock_get_authors,
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


@patch("paasta_tools.remote_git.get_authors", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.get_slack_client", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment", autospec=True)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config", autospec=True
)
@patch("sticht.slo.get_slos_for_service", autospec=True)
def test_MarkForDeployProcess_goes_to_mfd_failed_when_mark_for_deployment_fails(
    mock_get_slos_for_service,
    mock_load_system_paasta_config,
    mock_wait_for_deployment,
    mock_mark_for_deployment,
    mock_get_slack_client,
    mock_get_authors,
    mock_periodically_update_slack,
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


@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment",
    return_value=0,
    autospec=True,
)
@patch("paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment._log", autospec=True)
def test_MarkForDeployProcess_happy_path(
    mock_log,
    mock_wait_for_deployment,
    mock_mark_for_deployment,
    mock_periodically_update_slack,
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
    assert mfdp.state_history == ["start_deploy", "deploying", "deployed", "complete"]


@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment",
    return_value=0,
    autospec=True,
)
@patch("paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment._log", autospec=True)
@patch("paasta_tools.cli.cmds.wait_for_deployment._log", autospec=True)
def test_MarkForDeployProcess_happy_path_skips_complete_if_no_auto_rollback(
    mock__log1,
    mock__log2,
    mock_wait_for_deployment,
    mock_mark_for_deployment,
    mock_periodically_update_slack,
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
    assert mfdp.trigger_history == ["start_deploy", "mfd_succeeded", "deploy_finished"]
    assert mfdp.state_history == ["start_deploy", "deploying", "deployed"]
