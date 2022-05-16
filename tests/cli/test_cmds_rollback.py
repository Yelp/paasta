#!/usr/bin/env python
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
from mock import call
from mock import Mock
from mock import patch

from paasta_tools.cli.cli import parse_args
from paasta_tools.cli.cmds.rollback import get_versions_for_service
from paasta_tools.cli.cmds.rollback import list_previously_deployed_image_versions
from paasta_tools.cli.cmds.rollback import list_previously_deployed_shas
from paasta_tools.cli.cmds.rollback import paasta_rollback
from paasta_tools.cli.cmds.rollback import validate_given_deploy_groups
from paasta_tools.utils import DeploymentVersion
from paasta_tools.utils import RollbackTypes


@patch("paasta_tools.cli.cmds.rollback.get_currently_deployed_version", autospec=True)
@patch("paasta_tools.cli.cmds.rollback._log_audit", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.list_deploy_groups", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.figure_out_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.get_git_url", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.mark_for_deployment", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.get_versions_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.can_user_deploy_service", autospec=True)
def test_paasta_rollback_mark_for_deployment_simple_invocation(
    mock_can_user_deploy_service,
    mock_get_versions_for_service,
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_list_deploy_groups,
    mock_log_audit,
    mock_get_currently_deployed_version,
):
    fake_args, _ = parse_args(
        ["rollback", "-s", "fakeservice", "-k", "abcd" * 10, "-l", "fake_deploy_group1"]
    )

    mock_get_versions_for_service.return_value = {
        DeploymentVersion(sha=fake_args.commit, image_version=None): (
            "20170403T025512",
            fake_args.deploy_groups,
        ),
        DeploymentVersion(sha="dcba" * 10, image_version=None): (
            "20161006T025416",
            "fake_deploy_group2",
        ),
    }

    mock_get_git_url.return_value = "git://git.repo"
    mock_figure_out_service_name.return_value = fake_args.service
    mock_list_deploy_groups.return_value = [fake_args.deploy_groups]
    mock_mark_for_deployment.return_value = 0
    mock_get_currently_deployed_version.return_value = DeploymentVersion(
        sha="1234" * 10, image_version=None
    )

    assert paasta_rollback(fake_args) == 0

    mock_mark_for_deployment.assert_called_once_with(
        git_url=mock_get_git_url.return_value,
        deploy_group=fake_args.deploy_groups,
        service=mock_figure_out_service_name.return_value,
        commit=fake_args.commit,
        image_version=None,
    )

    # ensure that we logged each deploy group that was rolled back AND that we logged things correctly
    mock_log_audit.call_count == len(fake_args.deploy_groups)
    for call_args in mock_log_audit.call_args_list:
        _, call_kwargs = call_args
        assert call_kwargs["action"] == "rollback"
        assert call_kwargs["action_details"]["rolled_back_from"] == str(
            mock_get_currently_deployed_version.return_value
        )
        assert call_kwargs["action_details"]["rolled_back_to"] == fake_args.commit
        assert (
            call_kwargs["action_details"]["rollback_type"]
            == RollbackTypes.USER_INITIATED_ROLLBACK.value
        )
        assert call_kwargs["action_details"]["deploy_group"] in fake_args.deploy_groups
        assert call_kwargs["service"] == fake_args.service


@patch("paasta_tools.cli.cmds.rollback.get_currently_deployed_version", autospec=True)
@patch("paasta_tools.cli.cmds.rollback._log_audit", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.list_deploy_groups", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.figure_out_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.get_git_url", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.mark_for_deployment", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.get_versions_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.can_user_deploy_service", autospec=True)
def test_paasta_rollback_mark_for_deployment_with_image(
    mock_can_user_deploy_service,
    mock_get_versions_for_service,
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_list_deploy_groups,
    mock_log_audit,
    mock_get_currently_deployed_version,
):
    fake_args, _ = parse_args(
        [
            "rollback",
            "-s",
            "fakeservice",
            "-k",
            "abcd" * 10,
            "-l",
            "fake_deploy_group1",
            "-i",
            "extra_image_info",
        ]
    )

    mock_get_versions_for_service.return_value = {
        DeploymentVersion(
            sha=fake_args.commit, image_version=fake_args.image_version
        ): (
            "20170403T025512",
            fake_args.deploy_groups,
        ),
        DeploymentVersion(sha="dcba" * 10, image_version=None): (
            "20161006T025416",
            "fake_deploy_group2",
        ),
    }

    mock_get_git_url.return_value = "git://git.repo"
    mock_figure_out_service_name.return_value = fake_args.service
    mock_list_deploy_groups.return_value = [fake_args.deploy_groups]
    mock_mark_for_deployment.return_value = 0
    mock_get_currently_deployed_version.return_value = DeploymentVersion(
        sha="1234" * 10, image_version=None
    )

    rollback_version = DeploymentVersion(
        sha=fake_args.commit, image_version=fake_args.image_version
    )

    assert paasta_rollback(fake_args) == 0

    mock_mark_for_deployment.assert_called_once_with(
        git_url=mock_get_git_url.return_value,
        deploy_group=fake_args.deploy_groups,
        service=mock_figure_out_service_name.return_value,
        commit=fake_args.commit,
        image_version=fake_args.image_version,
    )

    # ensure that we logged each deploy group that was rolled back AND that we logged things correctly
    mock_log_audit.call_count == len(fake_args.deploy_groups)
    for call_args in mock_log_audit.call_args_list:
        _, call_kwargs = call_args
        assert call_kwargs["action"] == "rollback"
        assert call_kwargs["action_details"]["rolled_back_from"] == str(
            mock_get_currently_deployed_version.return_value
        )
        assert call_kwargs["action_details"]["rolled_back_to"] == str(rollback_version)
        assert (
            call_kwargs["action_details"]["rollback_type"]
            == RollbackTypes.USER_INITIATED_ROLLBACK.value
        )
        assert call_kwargs["action_details"]["deploy_group"] in fake_args.deploy_groups
        assert call_kwargs["service"] == fake_args.service


@patch("paasta_tools.cli.cmds.rollback.get_currently_deployed_version", autospec=True)
@patch("paasta_tools.cli.cmds.rollback._log_audit", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.list_deploy_groups", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.figure_out_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.get_git_url", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.mark_for_deployment", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.get_versions_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.can_user_deploy_service", autospec=True)
def test_paasta_rollback_with_force(
    mock_can_user_deploy_service,
    mock_get_versions_for_service,
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_list_deploy_groups,
    mock_log_audit,
    mock_get_currently_deployed_version,
):
    fake_args, _ = parse_args(
        [
            "rollback",
            "-s",
            "fakeservice",
            "-k",
            "abcd" * 10,
            "-l",
            "fake_deploy_group1",
            "-f",
        ]
    )

    mock_get_versions_for_service.return_value = {
        DeploymentVersion(sha="fake_sha1", image_version=None): (
            "20170403T025512",
            "fake_deploy_group1",
        ),
        DeploymentVersion(sha="fake_sha2", image_version=None): (
            "20161006T025416",
            "fake_deploy_group2",
        ),
    }

    mock_get_git_url.return_value = "git://git.repo"
    mock_figure_out_service_name.return_value = fake_args.service
    mock_list_deploy_groups.return_value = [fake_args.deploy_groups]
    mock_mark_for_deployment.return_value = 0
    mock_get_currently_deployed_version.return_value = DeploymentVersion(
        sha="1234" * 10, image_version=None
    )

    assert paasta_rollback(fake_args) == 0

    mock_mark_for_deployment.assert_called_once_with(
        git_url=mock_get_git_url.return_value,
        deploy_group=fake_args.deploy_groups,
        service=mock_figure_out_service_name.return_value,
        commit=fake_args.commit,
        image_version=None,
    )
    # ensure that we logged each deploy group that was rolled back AND that we logged things correctly
    mock_log_audit.call_count == len(fake_args.deploy_groups)
    for call_args in mock_log_audit.call_args_list:
        _, call_kwargs = call_args
        assert call_kwargs["action"] == "rollback"
        assert call_kwargs["action_details"]["rolled_back_from"] == str(
            mock_get_currently_deployed_version.return_value
        )
        assert call_kwargs["action_details"]["rolled_back_to"] == fake_args.commit
        assert (
            call_kwargs["action_details"]["rollback_type"]
            == RollbackTypes.USER_INITIATED_ROLLBACK.value
        )
        assert call_kwargs["action_details"]["deploy_group"] in fake_args.deploy_groups
        assert call_kwargs["service"] == fake_args.service


@patch("paasta_tools.cli.cmds.rollback.get_currently_deployed_version", autospec=True)
@patch("paasta_tools.cli.cmds.rollback._log_audit", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.list_deploy_groups", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.figure_out_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.get_git_url", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.mark_for_deployment", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.get_versions_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.can_user_deploy_service", autospec=True)
def test_paasta_rollback_mark_for_deployment_no_deploy_group_arg(
    mock_can_user_deploy_service,
    mock_get_versions_for_service,
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_list_deploy_groups,
    mock_log_audit,
    mock_get_currently_deployed_version,
):
    fake_args, _ = parse_args(["rollback", "-s", "fakeservice", "-k", "abcd" * 10])

    mock_get_versions_for_service.return_value = {
        DeploymentVersion(sha="fake_sha1", image_version=None): (
            "20170403T025512",
            "fake_deploy_group1",
        ),
        DeploymentVersion(sha=fake_args.commit, image_version=None): (
            "20161006T025416",
            "fake_deploy_group2",
        ),
    }

    mock_get_git_url.return_value = "git://git.repo"
    mock_figure_out_service_name.return_value = fake_args.service
    mock_list_deploy_groups.return_value = [
        "fake_deploy_group",
        "fake_cluster.fake_instance",
    ]
    mock_mark_for_deployment.return_value = 0
    mock_get_currently_deployed_version.return_value = DeploymentVersion(
        sha="1234" * 10, image_version=None
    )

    assert paasta_rollback(fake_args) == 0

    expected = [
        call(
            git_url=mock_get_git_url.return_value,
            service=mock_figure_out_service_name.return_value,
            commit=fake_args.commit,
            deploy_group="fake_cluster.fake_instance",
            image_version=None,
        ),
        call(
            git_url=mock_get_git_url.return_value,
            service=mock_figure_out_service_name.return_value,
            commit=fake_args.commit,
            deploy_group="fake_deploy_group",
            image_version=None,
        ),
    ]

    assert all([x in expected for x in mock_mark_for_deployment.mock_calls])
    assert mock_mark_for_deployment.call_count == len(expected)

    mock_log_audit.call_count == len(fake_args.deploy_groups)
    for call_args in mock_log_audit.call_args_list:
        _, call_kwargs = call_args
        assert call_kwargs["action"] == "rollback"
        assert call_kwargs["action_details"]["rolled_back_from"] == str(
            mock_get_currently_deployed_version.return_value
        )
        assert call_kwargs["action_details"]["rolled_back_to"] == fake_args.commit
        assert (
            call_kwargs["action_details"]["rollback_type"]
            == RollbackTypes.USER_INITIATED_ROLLBACK.value
        )
        assert (
            call_kwargs["action_details"]["deploy_group"]
            in mock_list_deploy_groups.return_value
        )
        assert call_kwargs["service"] == fake_args.service


@patch("paasta_tools.cli.cmds.rollback._log_audit", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.list_deploy_groups", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.figure_out_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.get_git_url", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.mark_for_deployment", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.can_user_deploy_service", autospec=True)
def test_paasta_rollback_mark_for_deployment_wrong_deploy_group_args(
    mock_can_user_deploy_service,
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_list_deploy_groups,
    mock_log_audit,
):
    fake_args, _ = parse_args(
        ["rollback", "-s", "fakeservice", "-k", "abcd" * 10, "-l", "wrong_deploy_group"]
    )

    mock_get_git_url.return_value = "git://git.repo"
    mock_figure_out_service_name.return_value = fake_args.service
    mock_list_deploy_groups.return_value = ["some_other_instance.some_other_cluster"]

    assert paasta_rollback(fake_args) == 1
    assert not mock_mark_for_deployment.called
    assert not mock_log_audit.called


@patch("paasta_tools.cli.cmds.rollback._log_audit", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.list_deploy_groups", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.figure_out_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.get_git_url", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.mark_for_deployment", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.get_versions_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.can_user_deploy_service", autospec=True)
def test_paasta_rollback_git_sha_was_not_marked_before(
    mock_can_user_deploy_service,
    mock_get_versions_for_service,
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_list_deploy_groups,
    mock_log_audit,
):
    fake_args, _ = parse_args(
        ["rollback", "-s", "fakeservice", "-k", "abcd" * 10, "-l", "fake_deploy_group1"]
    )

    mock_get_versions_for_service.return_value = {
        DeploymentVersion(sha="fake_sha1", image_version="fake_image"): (
            "20170403T025512",
            "fake_deploy_group1",
        ),
        DeploymentVersion(sha="fake_sha2", image_version="fake_image"): (
            "20161006T025416",
            "fake_deploy_group2",
        ),
        DeploymentVersion(sha=fake_args.commit, image_version="fake_image"): (
            "20161006T025416",
            "fake_deploy_group2",
        ),
    }

    mock_get_git_url.return_value = "git://git.repo"
    mock_figure_out_service_name.return_value = fake_args.service
    mock_list_deploy_groups.return_value = [fake_args.deploy_groups]
    mock_mark_for_deployment.return_value = 0

    assert paasta_rollback(fake_args) == 1
    assert not mock_mark_for_deployment.called
    assert not mock_log_audit.called


@patch("paasta_tools.cli.cmds.rollback.get_currently_deployed_version", autospec=True)
@patch("paasta_tools.cli.cmds.rollback._log_audit", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.list_deploy_groups", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.figure_out_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.get_git_url", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.mark_for_deployment", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.get_versions_for_service", autospec=True)
@patch("paasta_tools.cli.cmds.rollback.can_user_deploy_service", autospec=True)
def test_paasta_rollback_mark_for_deployment_multiple_deploy_group_args(
    mock_can_user_deploy_service,
    mock_get_versions_for_service,
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_list_deploy_groups,
    mock_log_audit,
    mock_get_currently_deployed_version,
):
    fake_args, _ = parse_args(
        [
            "rollback",
            "-s",
            "fakeservice",
            "-k",
            "abcd" * 10,
            "-l",
            "cluster.instance1,cluster.instance2",
        ]
    )

    fake_deploy_groups = fake_args.deploy_groups.split(",")

    mock_get_versions_for_service.return_value = {
        DeploymentVersion(sha="fake_sha1", image_version=None): (
            "20170403T025512",
            "fake_deploy_group1",
        ),
        DeploymentVersion(sha=fake_args.commit, image_version=None): (
            "20161006T025416",
            "fake_deploy_group2",
        ),
    }

    mock_get_git_url.return_value = "git://git.repo"
    mock_figure_out_service_name.return_value = fake_args.service
    mock_list_deploy_groups.return_value = fake_deploy_groups
    mock_mark_for_deployment.return_value = 0
    mock_get_currently_deployed_version.return_value = DeploymentVersion(
        sha="1234" * 10, image_version=None
    )

    assert paasta_rollback(fake_args) == 0

    expected = [
        call(
            git_url=mock_get_git_url.return_value,
            service=mock_figure_out_service_name.return_value,
            commit=fake_args.commit,
            deploy_group=deploy_group,
            image_version=None,
        )
        for deploy_group in fake_deploy_groups
    ]

    mock_mark_for_deployment.assert_has_calls(expected, any_order=True)
    assert mock_mark_for_deployment.call_count == len(fake_deploy_groups)

    mock_log_audit.call_count == len(fake_args.deploy_groups)
    for call_args in mock_log_audit.call_args_list:
        _, call_kwargs = call_args
        assert call_kwargs["action"] == "rollback"
        assert call_kwargs["action_details"]["rolled_back_from"] == str(
            mock_get_currently_deployed_version.return_value
        )
        assert call_kwargs["action_details"]["rolled_back_to"] == fake_args.commit
        assert (
            call_kwargs["action_details"]["rollback_type"]
            == RollbackTypes.USER_INITIATED_ROLLBACK.value
        )
        assert (
            call_kwargs["action_details"]["deploy_group"]
            in mock_list_deploy_groups.return_value
        )
        assert call_kwargs["service"] == fake_args.service


def test_validate_given_deploy_groups_no_arg():
    service_deploy_groups = ["deploy_group1", "deploy_group2"]
    given_deploy_groups = []

    expected_valid = {"deploy_group1", "deploy_group2"}
    expected_invalid = set()

    actual_valid, actual_invalid = validate_given_deploy_groups(
        service_deploy_groups, given_deploy_groups
    )

    assert actual_valid == expected_valid
    assert actual_invalid == expected_invalid


def test_validate_given_deploy_groups_wrong_arg():
    service_deploy_groups = ["deploy_group1", "deploy_group2"]
    given_deploy_groups = ["deploy_group0", "not_an_deploy_group"]

    expected_valid = set()
    expected_invalid = {"deploy_group0", "not_an_deploy_group"}

    actual_valid, actual_invalid = validate_given_deploy_groups(
        service_deploy_groups, given_deploy_groups
    )

    assert actual_valid == expected_valid
    assert actual_invalid == expected_invalid


def test_validate_given_deploy_groups_single_arg():
    service_deploy_groups = ["deploy_group1", "deploy_group2"]
    given_deploy_groups = ["deploy_group1"]

    expected_valid = {"deploy_group1"}
    expected_invalid = set()

    actual_valid, actual_invalid = validate_given_deploy_groups(
        service_deploy_groups, given_deploy_groups
    )

    assert actual_valid == expected_valid
    assert actual_invalid == expected_invalid


def test_validate_given_deploy_groups_multiple_args():
    service_deploy_groups = ["deploy_group1", "deploy_group2", "deploy_group3"]
    given_deploy_groups = ["deploy_group1", "deploy_group2"]

    expected_valid = {"deploy_group1", "deploy_group2"}
    expected_invalid = set()

    actual_valid, actual_invalid = validate_given_deploy_groups(
        service_deploy_groups, given_deploy_groups
    )

    assert actual_valid == expected_valid
    assert actual_invalid == expected_invalid


def test_validate_given_deploy_groups_duplicate_args():
    service_deploy_groups = ["deploy_group1", "deploy_group2", "deploy_group3"]
    given_deploy_groups = ["deploy_group1", "deploy_group1"]

    expected_valid = {"deploy_group1"}
    expected_invalid = set()

    actual_valid, actual_invalid = validate_given_deploy_groups(
        service_deploy_groups, given_deploy_groups
    )

    assert actual_valid == expected_valid
    assert actual_invalid == expected_invalid


def test_list_previously_deployed_shas():
    fake_refs = {
        "refs/tags/paasta-test.deploy.group-00000000T000000-deploy": "SHA_IN_OUTPUT",
        "refs/tags/paasta-other.deploy.group-00000000T000000-deploy": "NOT_IN_OUTPUT",
        "refs/tags/paasta-other.deploy.group+extra_image_info-00000000T000000-deploy": "NOT_IN_OUTPUT",
    }
    fake_deploy_groups = ["test.deploy.group"]

    with patch(
        "paasta_tools.cli.cmds.rollback.list_remote_refs",
        autospec=True,
        return_value=fake_refs,
    ), patch(
        "paasta_tools.cli.cmds.rollback.list_deploy_groups",
        autospec=True,
        return_value=fake_deploy_groups,
    ):
        fake_args = Mock(
            service="fake_service",
            deploy_groups="test.deploy.group,nonexistant.deploy.group",
            soa_dir="/fake/soa/dir",
            force=None,
        )
        assert set(list_previously_deployed_shas(fake_args)) == {"SHA_IN_OUTPUT"}


def test_list_previously_deployed_image_versions():
    fake_refs = {
        "refs/tags/paasta-test.deploy.group+extra_image_info-00000000T000000-deploy": "SHA_IN_OUTPUT",
        "refs/tags/paasta-other.deploy.group-00000000T000000-deploy": "NOT_IN_OUTPUT",
        "refs/tags/paasta-other.deploy.group+extra_image_info-00000000T000000-deploy": "NOT_IN_OUTPUT",
    }
    fake_deploy_groups = ["test.deploy.group"]

    with patch(
        "paasta_tools.cli.cmds.rollback.list_remote_refs",
        autospec=True,
        return_value=fake_refs,
    ), patch(
        "paasta_tools.cli.cmds.rollback.list_deploy_groups",
        autospec=True,
        return_value=fake_deploy_groups,
    ):
        fake_args = Mock(
            service="fake_service",
            deploy_groups="test.deploy.group,nonexistant.deploy.group",
            soa_dir="/fake/soa/dir",
            force=None,
        )
        assert set(list_previously_deployed_image_versions(fake_args)) == {
            "extra_image_info"
        }


def test_list_previously_deployed_shas_no_deploy_groups():
    fake_refs = {
        "refs/tags/paasta-test.deploy.group-00000000T000000-deploy": "SHA_IN_OUTPUT",
        "refs/tags/paasta-other.deploy.group-00000000T000000-deploy": "SHA_IN_OUTPUT_2",
        "refs/tags/paasta-nonexistant.deploy.group-00000000T000000-deploy": "SHA_NOT_IN_OUTPUT",
    }
    fake_deploy_groups = ["test.deploy.group", "other.deploy.group"]

    with patch(
        "paasta_tools.cli.cmds.rollback.list_remote_refs",
        autospec=True,
        return_value=fake_refs,
    ), patch(
        "paasta_tools.cli.cmds.rollback.list_deploy_groups",
        autospec=True,
        return_value=fake_deploy_groups,
    ):
        fake_args = Mock(
            service="fake_service",
            deploy_groups="",
            soa_dir="/fake/soa/dir",
            force=None,
        )
        assert set(list_previously_deployed_shas(fake_args)) == {
            "SHA_IN_OUTPUT",
            "SHA_IN_OUTPUT_2",
        }


def test_get_versions_for_service_no_service_name():
    assert get_versions_for_service(None, None, "/fake/soa/dir") == {}
