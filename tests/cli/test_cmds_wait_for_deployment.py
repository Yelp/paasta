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

import pytest
from mock import Mock
from mock import patch
from pytest import raises

from paasta_tools.cli.cmds import mark_for_deployment
from paasta_tools.cli.cmds.mark_for_deployment import NoSuchCluster
from paasta_tools.cli.cmds.wait_for_deployment import get_latest_marked_sha
from paasta_tools.cli.cmds.wait_for_deployment import paasta_wait_for_deployment
from paasta_tools.cli.cmds.wait_for_deployment import validate_git_sha_is_latest
from paasta_tools.cli.utils import NoSuchService
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.paastaapi import ApiException
from paasta_tools.remote_git import LSRemoteException
from paasta_tools.utils import TimeoutError


class fake_args:
    deploy_group = "test_deploy_group"
    service = "test_service"
    git_url = ""
    commit = "d670460b4b4aece5915caf5c68d12f560a9fe3e4"
    soa_dir = "fake_soa_dir"
    timeout = 0
    verbose = False
    polling_interval = 5
    diagnosis_interval = 15
    time_before_first_diagnosis = 15


def fake_bounce_status_resp(**kwargs):
    response = Mock(  # default is a good response
        expected_instance_count=1,
        running_instance_count=1,
        desired_state="start",
        app_count=1,
        active_versions=[["abc123", None, "cfg"]],
        deploy_status="Running",
    )
    for k, v in kwargs.items():
        setattr(response, k, v)
    return response


@pytest.mark.parametrize(
    "side_effect,expected",
    [
        (ApiException(status=500, reason=""), False),  # api bad
        (ApiException(status=599, reason=""), False),  # temporary api issue
        (ApiException(status=404, reason=""), False),  # instance dne
        ([""], True),  # status=204 produces empty response
        (  # instance stopped
            [fake_bounce_status_resp(expected_instance_count=0)],
            True,
        ),
        ([fake_bounce_status_resp(desired_state="stop")], True),  # instance stopped
        (  # bounce in-progress
            [
                fake_bounce_status_resp(
                    active_versions=[["wrong1", None, "cfg"], ["abc123", None, "cfg"]]
                )
            ],
            False,
        ),
        (  # previous bounces not yet finished
            [
                fake_bounce_status_resp(
                    active_versions=[
                        ["wrong1", None, "cfg"],
                        ["wrong2", None, "cfg"],
                        ["abc123", None, "cfg"],
                    ]
                )
            ],
            False,
        ),
        (  # bounce not started
            [fake_bounce_status_resp(active_versions=[["wrong1", None, "cfg"]])],
            False,
        ),
        (  # instance not running
            [fake_bounce_status_resp(deploy_status="NotRunning")],
            False,
        ),
        (  # not enough instances up
            [fake_bounce_status_resp(expected_instance_count=10)],
            False,
        ),
        ([fake_bounce_status_resp()], True),  # completed
    ],
)
@patch("paasta_tools.cli.cmds.mark_for_deployment._log", autospec=True)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.client.get_paasta_oapi_client",
    autospec=True,
)
def test_check_if_instance_is_done(
    mock_get_paasta_oapi_client, mock__log, side_effect, expected
):
    mock_paasta_api_client = Mock()
    mock_paasta_api_client.api_error = ApiException
    mock_paasta_api_client.service.bounce_status_instance.side_effect = side_effect
    mock_get_paasta_oapi_client.return_value = mock_paasta_api_client

    assert expected == mark_for_deployment.check_if_instance_is_done(
        service="fake_service",
        instance="fake_instance",
        cluster="fake_cluster",
        git_sha="abc123",
        instance_config=mock_marathon_instance_config("fake_instance"),
    )


@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config", autospec=True
)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.get_instance_configs_for_service_in_deploy_group_all_clusters",
    autospec=True,
)
@patch("paasta_tools.cli.cmds.mark_for_deployment._log", autospec=True)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.check_if_instance_is_done", autospec=True
)
def test_wait_for_deployment(
    mock_check_if_instance_is_done,
    mock__log,
    mock_get_instance_configs_for_service_in_deploy_group_all_clusters,
    mock_load_system_paasta_config,
):
    mock_get_instance_configs_for_service_in_deploy_group_all_clusters.return_value = {
        "cluster1": [
            mock_marathon_instance_config("instance1"),
            mock_marathon_instance_config("instance2"),
            mock_marathon_instance_config("instance3"),
        ],
    }

    def check_if_instance_is_done_side_effect(
        service, instance, cluster, git_sha, instance_config, api=None
    ):
        return instance in ["instance1", "instance2"]

    mock_check_if_instance_is_done.side_effect = check_if_instance_is_done_side_effect

    mock_load_system_paasta_config.return_value.get_api_endpoints.return_value = {
        "cluster1": "some_url_1",
        "cluster2": "some_url_2",
    }

    mock_load_system_paasta_config.return_value.get_mark_for_deployment_max_polling_threads.return_value = (
        4
    )

    with raises(TimeoutError):
        with patch(
            "asyncio.as_completed", side_effect=[asyncio.TimeoutError], autospec=True
        ):
            asyncio.run(
                mark_for_deployment.wait_for_deployment(
                    "service", "fake_deploy_group", "somesha", "/nail/soa", 1
                )
            )

    mock_get_instance_configs_for_service_in_deploy_group_all_clusters.return_value = {
        "cluster1": [
            mock_marathon_instance_config("instance1"),
            mock_marathon_instance_config("instance2"),
        ],
        "cluster2": [
            mock_marathon_instance_config("instance1"),
            mock_marathon_instance_config("instance2"),
        ],
    }
    with patch("sys.stdout", autospec=True, flush=Mock()):
        assert (
            asyncio.run(
                mark_for_deployment.wait_for_deployment(
                    "service", "fake_deploy_group", "somesha", "/nail/soa", 5
                )
            )
            == 0
        )

    mock_get_instance_configs_for_service_in_deploy_group_all_clusters.return_value = {
        "cluster1": [
            mock_marathon_instance_config("instance1"),
            mock_marathon_instance_config("instance2"),
        ],
        "cluster2": [
            mock_marathon_instance_config("instance1"),
            mock_marathon_instance_config("instance3"),
        ],
    }
    with raises(TimeoutError):
        asyncio.run(
            mark_for_deployment.wait_for_deployment(
                "service", "fake_deploy_group", "somesha", "/nail/soa", 0
            )
        )


@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config", autospec=True
)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.PaastaServiceConfigLoader", autospec=True
)
@patch("paasta_tools.cli.cmds.mark_for_deployment._log", autospec=True)
def test_wait_for_deployment_raise_no_such_cluster(
    mock__log,
    mock_paasta_service_config_loader,
    mock_load_system_paasta_config,
):
    mock_load_system_paasta_config.return_value.get_api_endpoints.return_value = {
        "cluster1": "some_url_1",
        "cluster2": "some_url_2",
    }

    mock_paasta_service_config_loader.return_value.clusters = ["cluster3"]
    with raises(NoSuchCluster):
        asyncio.run(
            mark_for_deployment.wait_for_deployment(
                "service", "deploy_group_3", "somesha", "/nail/soa", 0
            )
        )


@patch("paasta_tools.cli.cmds.wait_for_deployment.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment", autospec=True)
def test_paasta_wait_for_deployment_return_1_when_no_such_service(
    mock_wait_for_deployment, mock_validate_service_name
):
    mock_validate_service_name.side_effect = NoSuchService("Some text")
    assert paasta_wait_for_deployment(fake_args) == 1
    assert mock_wait_for_deployment.call_args_list == []
    assert mock_validate_service_name.called


@patch("paasta_tools.cli.cmds.wait_for_deployment.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.wait_for_deployment.list_deploy_groups", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment", autospec=True)
def test_paasta_wait_for_deployment_return_1_when_deploy_group_not_found(
    mock_wait_for_deployment, mock_list_deploy_groups, mock_validate_service_name
):
    mock_list_deploy_groups.return_value = {"another_test_deploy_group"}
    assert paasta_wait_for_deployment(fake_args) == 1
    assert mock_wait_for_deployment.call_args_list == []
    assert mock_validate_service_name.called


@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config", autospec=True
)
@patch(
    "paasta_tools.cli.cmds.mark_for_deployment.PaastaServiceConfigLoader", autospec=True
)
@patch("paasta_tools.cli.cmds.wait_for_deployment.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.wait_for_deployment.validate_git_sha", autospec=True)
@patch(
    "paasta_tools.cli.cmds.wait_for_deployment.validate_git_sha_is_latest",
    autospec=True,
)
@patch("paasta_tools.cli.cmds.wait_for_deployment.list_deploy_groups", autospec=True)
@patch("paasta_tools.cli.cmds.mark_for_deployment._log", autospec=True)
@patch("paasta_tools.cli.cmds.wait_for_deployment._log", autospec=True)
def test_paasta_wait_for_deployment_return_0_when_no_instances_in_deploy_group(
    mock__log1,
    mock__log2,
    mock_list_deploy_groups,
    mock_validate_git_sha_is_latest,
    mock_validate_git_sha,
    mock_validate_service_name,
    mock_paasta_service_config_loader,
    mock_load_system_paasta_config,
    system_paasta_config,
):
    mock__log1.return_value = None
    mock__log2.return_value = None
    mock_load_system_paasta_config.return_value = system_paasta_config
    mock_paasta_service_config_loader.return_value.instance_configs.return_value = [
        mock_marathon_instance_config("some_instance")
    ]
    mock_list_deploy_groups.return_value = {"test_deploy_group"}
    assert paasta_wait_for_deployment(fake_args) == 0
    assert mock_validate_service_name.called


@patch("paasta_tools.cli.cmds.wait_for_deployment.list_remote_refs", autospec=True)
def test_get_latest_marked_sha_good(mock_list_remote_refs):
    mock_list_remote_refs.return_value = {
        "refs/tags/paasta-fake_group1-20161129T203750-deploy": "968b948b3fca457326718dc7b2e278f89ccc5c87",
        "refs/tags/paasta-fake_group1-20161117T122449-deploy": "eac9a6d7909d09ffec00538bbc43b64502aa2dc0",
        "refs/tags/paasta-fake_group2-20161125T095651-deploy": "a4911648beb2e53886658ba7ea7eb93d582d754c",
        "refs/tags/paasta-fake_group1.everywhere-20161109T223959-deploy": "71e97ec397a3f0e7c4ee46e8ea1e2982cbcb0b79",
    }
    assert (
        get_latest_marked_sha("", "fake_group1")
        == "968b948b3fca457326718dc7b2e278f89ccc5c87"
    )


@patch("paasta_tools.cli.cmds.wait_for_deployment.list_remote_refs", autospec=True)
def test_get_latest_marked_sha_bad(mock_list_remote_refs):
    mock_list_remote_refs.return_value = {
        "refs/tags/paasta-fake_group2-20161129T203750-deploy": "968b948b3fca457326718dc7b2e278f89ccc5c87"
    }
    assert get_latest_marked_sha("", "fake_group1") == ""


@patch("paasta_tools.cli.cmds.wait_for_deployment.list_remote_refs", autospec=True)
def test_validate_deploy_group_when_is_git_not_available(mock_list_remote_refs, capsys):
    test_error_message = "Git error"
    mock_list_remote_refs.side_effect = LSRemoteException(test_error_message)
    assert (
        validate_git_sha_is_latest(
            "fake sha", "fake_git_url", "fake_group", "fake_service"
        )
        is None
    )


def mock_marathon_instance_config(fake_name) -> "MarathonServiceConfig":
    return MarathonServiceConfig(
        service="fake_service",
        cluster="fake_cluster",
        instance=fake_name,
        config_dict={"deploy_group": "fake_deploy_group"},
        branch_dict=None,
        soa_dir="fake_soa_dir",
    )


def test_compose_timeout_message():
    remaining_instances = {
        "cluster1": ["instance1", "instance2"],
        "cluster2": ["instance3"],
        "cluster3": [],
    }

    message = mark_for_deployment.compose_timeout_message(
        remaining_instances, 1, "fake_group", "someservice", "some_git_sha"
    )
    assert (
        "  paasta status -c cluster1 -s someservice -i instance1,instance2" in message
    )
    assert "  paasta status -c cluster2 -s someservice -i instance3" in message
    assert (
        "  paasta logs -c cluster1 -s someservice -i instance1,instance2 -C deploy -l 1000"
        in message
    )
    assert (
        "  paasta logs -c cluster2 -s someservice -i instance3 -C deploy -l 1000"
        in message
    )
