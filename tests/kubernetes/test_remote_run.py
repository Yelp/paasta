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
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from kubernetes.client import AuthenticationV1TokenRequest
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1PolicyRule
from kubernetes.client import V1Role
from kubernetes.client import V1RoleBinding
from kubernetes.client import V1RoleRef
from kubernetes.client import V1ServiceAccount
from kubernetes.client import V1Subject
from kubernetes.client import V1TokenRequestSpec
from kubernetes.client.exceptions import ApiException

from paasta_tools.kubernetes.remote_run import bind_role_to_service_account
from paasta_tools.kubernetes.remote_run import create_pod_scoped_role
from paasta_tools.kubernetes.remote_run import create_remote_run_service_account
from paasta_tools.kubernetes.remote_run import create_temp_exec_token
from paasta_tools.kubernetes.remote_run import find_job_pod
from paasta_tools.kubernetes.remote_run import generate_toolbox_deployment
from paasta_tools.kubernetes.remote_run import get_remote_run_jobs
from paasta_tools.kubernetes.remote_run import get_remote_run_role_bindings
from paasta_tools.kubernetes.remote_run import get_remote_run_roles
from paasta_tools.kubernetes.remote_run import remote_run_ready
from paasta_tools.kubernetes.remote_run import remote_run_start
from paasta_tools.kubernetes.remote_run import remote_run_stop
from paasta_tools.kubernetes.remote_run import remote_run_token
from paasta_tools.kubernetes.remote_run import RemoteRunError


@patch("paasta_tools.kubernetes.remote_run.get_application_wrapper", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.load_eks_service_config", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.KubeClient", autospec=True)
def test_remote_run_start(mock_client, mock_load_config, mock_wrapper_getter):
    mock_client = mock_client.return_value
    mock_load_config.return_value.get_namespace.return_value = "namespace"
    mock_load_config.return_value.config_dict = {}
    mock_job = mock_load_config.return_value.format_kubernetes_job.return_value
    mock_job.metadata.name = "somejob"
    assert remote_run_start(
        "foo",
        "bar",
        "dev",
        "someuser",
        interactive=True,
        recreate=False,
        max_duration=1000,
        is_toolbox=False,
    ) == {
        "status": 200,
        "message": "Remote run sandbox started",
        "job_name": "remote-run-someuser-somejob",
    }
    assert mock_load_config.return_value.config_dict == {"cmd": "sleep 1000"}
    mock_load_config.return_value.format_kubernetes_job.assert_called_once_with(
        job_label="remote-run",
        deadline_seconds=1000,
        keep_routable_ip=False,
    )
    mock_wrapper_getter.assert_called_once_with(mock_job)
    mock_wrapper = mock_wrapper_getter.return_value
    mock_wrapper.ensure_service_account.assert_called_once_with(mock_client)
    mock_wrapper.create.assert_called_once_with(mock_client)


@patch("paasta_tools.kubernetes.remote_run.get_application_wrapper", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.load_eks_service_config", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.KubeClient", autospec=True)
def test_remote_run_start_command(mock_client, mock_load_config, mock_wrapper_getter):
    mock_client = mock_client.return_value
    mock_load_config.return_value.get_namespace.return_value = "namespace"
    mock_load_config.return_value.config_dict = {}
    mock_job = mock_load_config.return_value.format_kubernetes_job.return_value
    mock_job.metadata.name = "somejobwithcommand"
    assert remote_run_start(
        "foo",
        "bar",
        "dev",
        "someuser",
        interactive=False,
        recreate=False,
        max_duration=1000,
        is_toolbox=False,
        command="python -m this --and that",
    ) == {
        "status": 200,
        "message": "Remote run sandbox started",
        "job_name": "remote-run-someuser-somejobwithcommand",
    }
    assert mock_load_config.return_value.config_dict == {
        "cmd": "python -m this --and that"
    }
    mock_load_config.return_value.format_kubernetes_job.assert_called_once_with(
        job_label="remote-run",
        deadline_seconds=1000,
        keep_routable_ip=False,
    )
    mock_wrapper_getter.assert_called_once_with(mock_job)
    mock_wrapper = mock_wrapper_getter.return_value
    mock_wrapper.create.assert_called_once_with(mock_client)


@patch("paasta_tools.kubernetes.remote_run.remote_run_stop", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.get_application_wrapper", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.load_eks_service_config", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.KubeClient", autospec=True)
def test_remote_run_start_recreate(
    mock_client, mock_load_config, mock_wrapper_getter, mock_stop
):
    def _create_mock_job(name: str):
        mock_job = MagicMock()
        mock_job.metadata.name = "somejob"
        return mock_job

    mock_client = mock_client.return_value
    mock_load_config.return_value.get_namespace.return_value = "namespace"
    mock_load_config.return_value.config_dict = {}
    mock_load_config.return_value.format_kubernetes_job.side_effect = [
        # we need this to avoid grabbing the same mock over the recursive call
        _create_mock_job("somejob"),
        _create_mock_job("somejob"),
    ]
    mock_wrapper_getter.return_value.create.side_effect = [
        ApiException(status=409),
        None,
    ]
    assert remote_run_start(
        "foo",
        "bar",
        "dev",
        "someuser",
        interactive=False,
        recreate=True,
        max_duration=1000,
        is_toolbox=False,
    ) == {
        "status": 200,
        "message": "Remote run sandbox started",
        "job_name": "remote-run-someuser-somejob",
    }
    mock_stop.assert_called_once_with(
        service="foo", instance="bar", cluster="dev", user="someuser", is_toolbox=False
    )


@patch("paasta_tools.kubernetes.remote_run.get_application_wrapper", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.generate_toolbox_deployment", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.KubeClient", autospec=True)
def test_remote_run_start_toolbox(mock_client, mock_gen_config, mock_wrapper_getter):
    mock_client = mock_client.return_value
    mock_gen_config.return_value.get_namespace.return_value = "remote-run-toolbox"
    mock_gen_config.return_value.config_dict = {}
    mock_job = mock_gen_config.return_value.format_kubernetes_job.return_value
    mock_job.metadata.name = "somejob"
    assert remote_run_start(
        "toolbox-foo",
        "bar",
        "dev",
        "someuser",
        interactive=True,
        recreate=False,
        max_duration=1000,
        is_toolbox=True,
    ) == {
        "status": 200,
        "message": "Remote run sandbox started",
        "job_name": "remote-run-someuser-somejob",
    }
    assert mock_gen_config.return_value.config_dict == {}  # not changing cmd
    mock_gen_config.return_value.format_kubernetes_job.assert_called_once_with(
        job_label="remote-run",
        deadline_seconds=1000,
        keep_routable_ip=True,
    )
    mock_wrapper_getter.assert_called_once_with(mock_job)
    mock_wrapper = mock_wrapper_getter.return_value
    mock_wrapper.create.assert_called_once_with(mock_client)


@patch("paasta_tools.kubernetes.remote_run.find_job_pod", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.generate_toolbox_deployment", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.load_eks_service_config", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.KubeClient", autospec=True)
def test_remote_run_ready(
    mock_client, mock_load_config, mock_gen_config, mock_find_job_pod
):
    mock_client = mock_client.return_value
    mock_load_config.return_value.get_namespace.return_value = "namespace"
    mock_gen_config.return_value.get_namespace.return_value = "remote-run-toolbox"
    mock_find_job_pod.return_value.metadata.name = "somepod"
    mock_find_job_pod.return_value.metadata.deletion_timestamp = None
    mock_find_job_pod.return_value.status.pod_ip = "127.1.33.7"
    # job found and ready
    mock_find_job_pod.return_value.status.phase = "Running"
    assert remote_run_ready(
        "foo", "bar", "dev", "somejob", "someuser", is_toolbox=False
    ) == {
        "status": 200,
        "message": "Pod ready",
        "pod_name": "somepod",
        "namespace": "namespace",
    }
    # toolbox job
    assert remote_run_ready(
        "foo", "bar", "dev", "somejob", "someuser", is_toolbox=True
    ) == {
        "status": 200,
        "message": "Pod ready",
        "pod_address": "127.1.33.7",
        "pod_name": "somepod",
        "namespace": "remote-run-toolbox",
    }
    # job not ready
    mock_find_job_pod.return_value.status.phase = "Pending"
    assert remote_run_ready(
        "foo", "bar", "dev", "somejob", "someuser", is_toolbox=False
    ) == {
        "status": 204,
        "message": "Pod not ready",
    }
    # job not found
    mock_find_job_pod.return_value = None
    assert remote_run_ready(
        "foo", "bar", "dev", "somejob", "someuser", is_toolbox=False
    ) == {
        "status": 404,
        "message": "No pod found",
    }


@patch("paasta_tools.kubernetes.remote_run.get_application_wrapper", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.load_eks_service_config", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.KubeClient", autospec=True)
def test_remote_run_stop(mock_client, mock_load_config, mock_wrapper_getter):
    mock_client = mock_client.return_value
    mock_load_config.return_value.get_namespace.return_value = "namespace"
    mock_job = mock_load_config.return_value.format_kubernetes_job.return_value
    mock_job.metadata.name = "somejob"
    assert remote_run_stop("foo", "bar", "dev", "someuser", is_toolbox=False) == {
        "status": 200,
        "message": "Job successfully removed",
    }
    assert mock_job.metadata.name == "remote-run-someuser-somejob"
    mock_wrapper_getter.assert_called_once_with(mock_job)
    mock_wrapper = mock_wrapper_getter.return_value
    mock_wrapper.deep_delete.assert_called_once_with(mock_client)


@patch("paasta_tools.kubernetes.remote_run.create_temp_exec_token", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.bind_role_to_service_account", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.create_pod_scoped_role", autospec=True)
@patch(
    "paasta_tools.kubernetes.remote_run.create_remote_run_service_account",
    autospec=True,
)
@patch("paasta_tools.kubernetes.remote_run.find_job_pod", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.load_eks_service_config", autospec=True)
@patch("paasta_tools.kubernetes.remote_run.KubeClient", autospec=True)
def test_remote_run_token(
    mock_client,
    mock_load_config,
    mock_find_job_pod,
    mock_create_sa,
    mock_create_role,
    mock_bind_role,
    mock_create_token,
):
    mock_client = mock_client.return_value
    mock_load_config.return_value.get_namespace.return_value = "namespace"
    mock_job = mock_load_config.return_value.format_kubernetes_job.return_value
    mock_job.metadata.name = "somejob"
    mock_find_job_pod.return_value.metadata.name = "remote-run-someuser-somejob-112233"
    mock_create_sa.return_value = "somesa"
    mock_create_role.return_value = "somerole"
    assert (
        remote_run_token("foo", "bar", "dev", "someuser")
        == mock_create_token.return_value
    )
    mock_load_config.assert_called_once_with("foo", "bar", "dev")
    mock_find_job_pod.assert_called_once_with(
        mock_client, "namespace", "remote-run-someuser-somejob"
    )
    mock_create_sa.assert_called_once_with(
        mock_client, "namespace", "remote-run-someuser-somejob-112233", "someuser"
    )
    mock_create_role.assert_called_once_with(
        mock_client, "namespace", "remote-run-someuser-somejob-112233", "someuser"
    )
    mock_bind_role.assert_called_once_with(
        mock_client,
        "namespace",
        "somesa",
        "somerole",
        "someuser",
    )
    mock_create_token.assert_called_once_with(mock_client, "namespace", "somesa")
    # job not found
    mock_find_job_pod.return_value = None
    with pytest.raises(RemoteRunError):
        remote_run_token("foo", "bar", "dev", "someuser")


@patch("paasta_tools.kubernetes.remote_run.sleep", autospec=True)
def test_find_job_pod(mock_sleep):
    def _create_mock_pod_item(name: str):
        mock_pod = MagicMock()
        mock_pod.metadata.name = name
        return mock_pod

    mock_client = MagicMock()
    mock_client.core.list_namespaced_pod.side_effect = [
        MagicMock(items=[]),
        MagicMock(items=[_create_mock_pod_item("somejob-aaabbbccc")]),
    ]
    assert (
        find_job_pod(mock_client, "namespace", "somejob").metadata.name
        == "somejob-aaabbbccc"
    )
    assert mock_sleep.call_count == 1
    mock_client.core.list_namespaced_pod.assert_has_calls(
        [
            call(
                "namespace",
                label_selector="paasta.yelp.com/job_type=remote-run,job-name=somejob",
            )
        ]
        * 2
    )


def test_create_temp_exec_token():
    mock_client = MagicMock()
    mock_client.core.create_namespaced_service_account_token.return_value.status.token = (
        "datoken"
    )
    assert create_temp_exec_token(mock_client, "namespace", "somesa") == "datoken"
    mock_client.core.create_namespaced_service_account_token.assert_called_once_with(
        "somesa",
        "namespace",
        AuthenticationV1TokenRequest(
            spec=V1TokenRequestSpec(
                expiration_seconds=600,
                audiences=[],
            ),
        ),
    )


@patch("paasta_tools.kubernetes.remote_run.get_all_service_accounts", autospec=True)
def test_create_remote_run_service_account(mock_get_all_sa):
    def _create_mock_sa_item(name: str):
        mock_sa_item = MagicMock()
        mock_sa_item.metadata.name = name
        return mock_sa_item

    mock_client = MagicMock()
    # create new
    mock_get_all_sa.return_value = map(_create_mock_sa_item, ["foo", "bar"])
    assert (
        create_remote_run_service_account(
            mock_client, "namespace", "somepod", "someuser"
        )
        == "remote-run-someuser-b5189b42def8"
    )
    mock_client.core.create_namespaced_service_account.assert_called_once_with(
        namespace="namespace",
        body=V1ServiceAccount(
            metadata=V1ObjectMeta(
                name="remote-run-someuser-b5189b42def8",
                namespace="namespace",
                labels={"paasta.yelp.com/pod_owner": "someuser"},
            ),
        ),
    )
    mock_get_all_sa.assert_called_once_with(
        mock_client,
        namespace="namespace",
        label_selector="paasta.yelp.com/pod_owner=someuser",
    )
    # pick existing
    mock_client.reset_mock()
    mock_get_all_sa.return_value = map(
        _create_mock_sa_item, ["foo", "remote-run-someuser-b5189b42def8", "bar"]
    )
    assert (
        create_remote_run_service_account(
            mock_client, "namespace", "somepod", "someuser"
        )
        == "remote-run-someuser-b5189b42def8"
    )
    mock_client.core.create_namespaced_service_account.assert_not_called()


def test_create_pod_scoped_role():
    mock_client = MagicMock()
    assert (
        create_pod_scoped_role(mock_client, "namespace", "somepod", "someuser")
        == "remote-run-role-b5189b42def8"
    )
    mock_client.rbac.create_namespaced_role.assert_called_once_with(
        namespace="namespace",
        body=V1Role(
            rules=[
                V1PolicyRule(
                    verbs=["create", "get"],
                    resources=["pods", "pods/exec", "pods/log"],
                    resource_names=["somepod"],
                    api_groups=[""],
                )
            ],
            metadata=V1ObjectMeta(
                name="remote-run-role-b5189b42def8",
                labels={"paasta.yelp.com/pod_owner": "someuser"},
            ),
        ),
    )


def test_bind_role_to_service_account():
    mock_client = MagicMock()
    bind_role_to_service_account(
        mock_client, "namespace", "somesa", "somerole", "someuser"
    )
    mock_client.rbac.create_namespaced_role_binding.assert_called_once_with(
        namespace="namespace",
        body=V1RoleBinding(
            metadata=V1ObjectMeta(
                name="remote-run-binding-somerole",
                namespace="namespace",
                labels={"paasta.yelp.com/pod_owner": "someuser"},
            ),
            role_ref=V1RoleRef(
                api_group="rbac.authorization.k8s.io",
                kind="Role",
                name="somerole",
            ),
            subjects=[
                V1Subject(
                    kind="ServiceAccount",
                    name="somesa",
                ),
            ],
        ),
    )


def test_get_remote_run_roles():
    mock_client = MagicMock()
    get_remote_run_roles(mock_client, "namespace")
    mock_client.rbac.list_namespaced_role.assert_called_once_with(
        "namespace", label_selector="paasta.yelp.com/pod_owner"
    )


def test_get_remote_run_role_bindings():
    mock_client = MagicMock()
    get_remote_run_role_bindings(mock_client, "namespace")
    mock_client.rbac.list_namespaced_role_binding.assert_called_once_with(
        "namespace", label_selector="paasta.yelp.com/pod_owner"
    )


def test_get_remote_run_jobs():
    mock_client = MagicMock()
    get_remote_run_jobs(mock_client, "namespace")
    mock_client.batches.list_namespaced_job.assert_called_once_with(
        "namespace",
        label_selector="paasta.yelp.com/job_type=remote-run",
    )


@patch("paasta_tools.kubernetes.remote_run.load_adhoc_job_config", autospec=True)
def test_generate_toolbox_deployment(mock_load_config):
    mock_load_config.return_value.config_dict = {}
    result = generate_toolbox_deployment("prod-toolbox-something", "devc", "someone")
    assert result.service == "prod-toolbox-something"
    assert result.cluster == "devc"
    assert result.config_dict == {
        "env": {"SANDBOX_USER": "someone"},
        "extra_volumes": [
            {
                "containerPath": "/etc/authorized_keys.d/someone.pub",
                "hostPath": "/etc/authorized_keys.d/someone.pub",
                "mode": "RO",
            },
        ],
        "routable_ip": True,
    }
    mock_load_config.assert_called_once_with(
        "prod-toolbox", "something", "devc", load_deployments=False
    )
