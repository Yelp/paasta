# Copyright 2015-2019 Yelp Inc.
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
import datetime
from unittest import mock
from unittest.mock import AsyncMock

import a_sync
import pytest
import requests.exceptions

import paasta_tools.instance.kubernetes as pik
from paasta_tools import utils
from paasta_tools.utils import DeploymentVersion
from tests.conftest import Struct
from tests.conftest import wrap_value_in_task


@pytest.fixture
def mock_pod():
    return Struct(
        metadata=Struct(
            owner_references=[Struct(kind="ReplicaSet", name="replicaset_1")],
            name="pod_1",
            namespace="paasta",
            creation_timestamp=datetime.datetime(2021, 3, 6),
            deletion_timestamp=None,
            labels={
                "paasta.yelp.com/git_sha": "aaa000",
                "paasta.yelp.com/config_sha": "config000",
                "paasta.yelp.com/service": "service",
                "paasta.yelp.com/instance": "instance",
            },
        ),
        status=Struct(
            pod_ip="1.2.3.4",
            host_ip="4.3.2.1",
            phase="Running",
            reason=None,
            message=None,
            conditions=[
                Struct(
                    type="Ready",
                    status="True",
                ),
                Struct(
                    type="PodScheduled",
                    status="True",
                ),
            ],
            container_statuses=[
                Struct(
                    name="main_container",
                    restart_count=0,
                    state=Struct(
                        running=dict(
                            reason="a_state_reason",
                            message="a_state_message",
                            started_at=datetime.datetime(2021, 3, 6),
                        ),
                        waiting=None,
                        terminated=None,
                    ),
                    last_state=Struct(
                        running=None,
                        waiting=None,
                        terminated=dict(
                            reason="a_last_state_reason",
                            message="a_last_state_message",
                            started_at=datetime.datetime(2021, 3, 4),
                            finished_at=datetime.datetime(2021, 3, 5),
                        ),
                    ),
                ),
            ],
        ),
        spec=Struct(
            containers=[
                Struct(
                    name="main_container",
                    liveness_probe=Struct(
                        initial_delay_seconds=1,
                        failure_threshold=2,
                        period_seconds=3,
                        timeout_seconds=4,
                        http_get=Struct(
                            port=8080,
                            path="/healthcheck",
                        ),
                    ),
                )
            ]
        ),
    )


def test_instance_types_integrity():
    for it in pik.INSTANCE_TYPES:
        assert it in utils.INSTANCE_TYPES
    for it in pik.INSTANCE_TYPES_WITH_SET_STATE:
        assert it in utils.INSTANCE_TYPES


def instance_status_kwargs():
    return dict(
        service="",
        instance="",
        instance_type="",
        verbose=0,
        include_envoy=False,
        settings=mock.Mock(),
        use_new=False,
        all_namespaces=False,
    )


@mock.patch("paasta_tools.instance.kubernetes.cr_status", autospec=True)
@mock.patch("paasta_tools.instance.kubernetes.kubernetes_status", autospec=True)
def test_instance_status_invalid_instance_type(mock_kubernetes_status, mock_cr_status):
    kwargs = instance_status_kwargs()
    with pytest.raises(RuntimeError) as excinfo:
        pik.instance_status(**kwargs)
    assert "Unknown instance type" in str(excinfo.value)
    assert len(mock_cr_status.mock_calls) == 0
    assert len(mock_kubernetes_status.mock_calls) == 0


@mock.patch("paasta_tools.instance.kubernetes.cr_status", autospec=True)
@mock.patch("paasta_tools.instance.kubernetes.kubernetes_status", autospec=True)
def test_instance_status_kubernetes_only(mock_kubernetes_status, mock_cr_status):
    kwargs = instance_status_kwargs()
    kwargs.update(instance_type="kubernetes")
    pik.instance_status(**kwargs)
    assert len(mock_cr_status.mock_calls) == 0
    assert len(mock_kubernetes_status.mock_calls) == 1


@mock.patch("paasta_tools.instance.kubernetes.cr_status", autospec=True)
@mock.patch("paasta_tools.instance.kubernetes.kubernetes_status", autospec=True)
def test_instance_status_cr_only(mock_kubernetes_status, mock_cr_status):
    kwargs = instance_status_kwargs()
    kwargs.update(instance_type="flink")
    pik.instance_status(**kwargs)
    assert len(mock_cr_status.mock_calls) == 1
    assert len(mock_kubernetes_status.mock_calls) == 0


@mock.patch("paasta_tools.instance.kubernetes.cr_status", autospec=True)
@mock.patch("paasta_tools.instance.kubernetes.kubernetes_status", autospec=True)
def test_instance_status_cr_and_kubernetes(mock_kubernetes_status, mock_cr_status):
    kwargs = instance_status_kwargs()
    kwargs.update(instance_type="cassandracluster")
    pik.instance_status(**kwargs)
    assert len(mock_cr_status.mock_calls) == 1
    assert len(mock_kubernetes_status.mock_calls) == 1


def test_kubernetes_status():
    with mock.patch(
        "paasta_tools.instance.kubernetes.job_status",
        new_callable=AsyncMock,
        autospec=None,
    ), mock.patch(
        "paasta_tools.kubernetes_tools.replicasets_for_service_instance",
        new_callable=AsyncMock,
        autospec=None,
    ) as mock_replicasets_for_service_instance, mock.patch(
        "paasta_tools.kubernetes_tools.pods_for_service_instance",
        new_callable=AsyncMock,
        autospec=None,
    ) as mock_pods_for_service_instance, mock.patch(
        "paasta_tools.kubernetes_tools.get_kubernetes_app_by_name",
        autospec=True,
    ), mock.patch(
        "paasta_tools.instance.kubernetes.LONG_RUNNING_INSTANCE_TYPE_HANDLERS",
        autospec=True,
    ) as mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS:
        mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS["flink"] = mock.Mock()
        mock_pods_for_service_instance.return_value = []
        mock_replicasets_for_service_instance.return_value = []
        status = pik.kubernetes_status(
            service="",
            instance="",
            verbose=0,
            include_envoy=False,
            instance_type="flink",
            settings=mock.Mock(),
        )
        assert "app_count" in status
        assert "evicted_count" in status
        assert "bounce_method" in status
        assert "desired_state" in status


class TestKubernetesStatusV2:
    @pytest.fixture
    def mock_pods_for_service_instance(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.pods_for_service_instance",
            new_callable=AsyncMock,
            autospec=None,
        ) as mock_pods_for_service_instance:
            yield mock_pods_for_service_instance

    @pytest.fixture
    def mock_replicasets_for_service_instance(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.replicasets_for_service_instance",
            new_callable=AsyncMock,
            autospec=None,
        ) as mock_replicasets_for_service_instance:
            yield mock_replicasets_for_service_instance

    @pytest.fixture
    def mock_mesh_status(self):
        with mock.patch(
            "paasta_tools.instance.kubernetes.mesh_status",
            new_callable=AsyncMock,
            autospec=None,
        ) as mock_mesh_status:
            yield mock_mesh_status

    @pytest.fixture
    def mock_load_service_namespace_config(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.load_service_namespace_config",
            autospec=True,
        ) as mock_load_service_namespace_config:
            yield mock_load_service_namespace_config

    @pytest.fixture
    def mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS(self):
        with mock.patch(
            "paasta_tools.instance.kubernetes.LONG_RUNNING_INSTANCE_TYPE_HANDLERS",
            autospec=True,
        ) as mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS:
            yield mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS

    @pytest.fixture
    def mock_controller_revisions_for_service_instance(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.controller_revisions_for_service_instance",
            new_callable=AsyncMock,
            autospec=None,
        ) as mock_controller_revisions_for_service_instance:
            yield mock_controller_revisions_for_service_instance

    @pytest.fixture
    def mock_get_pod_event_messages(self):
        with mock.patch(
            "paasta_tools.instance.kubernetes.get_pod_event_messages",
            new_callable=AsyncMock,
            autospec=None,
        ) as mock_get_pod_event_messages:
            yield mock_get_pod_event_messages

    @pytest.fixture
    def mock_find_all_relevant_namespaces(self):
        with mock.patch(
            "paasta_tools.instance.kubernetes.find_all_relevant_namespaces",
            autospec=True,
        ) as mock_find_all_relevant_namespaces:
            yield mock_find_all_relevant_namespaces

    def test_replicaset(
        self,
        mock_replicasets_for_service_instance,
        mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS,
        mock_load_service_namespace_config,
        mock_pods_for_service_instance,
        mock_mesh_status,
        mock_get_pod_event_messages,
        mock_pod,
        mock_find_all_relevant_namespaces,
    ):
        mock_find_all_relevant_namespaces.return_value = ["paasta"]
        mock_job_config = mock.Mock(
            get_persistent_volumes=mock.Mock(return_value=[]),
        )
        mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS[
            "kubernetes"
        ].loader.return_value = mock_job_config
        mock_replicasets_for_service_instance.return_value = [
            Struct(
                spec=Struct(replicas=1),
                metadata=Struct(
                    name="replicaset_1",
                    namespace="paasta",
                    creation_timestamp=datetime.datetime(2021, 3, 5),
                    deletion_timestamp=None,
                    labels={
                        "paasta.yelp.com/git_sha": "aaa000",
                        "paasta.yelp.com/config_sha": "config000",
                    },
                ),
            ),
        ]
        mock_pods_for_service_instance.return_value = [mock_pod]

        mock_load_service_namespace_config.return_value = {}
        mock_job_config.get_registrations.return_value = ["service.instance"]
        mock_job_config.get_container_port.return_value = 8080
        mock_get_pod_event_messages.return_value = []
        status = pik.kubernetes_status_v2(
            service="service",
            instance="instance",
            verbose=0,
            include_envoy=False,
            instance_type="kubernetes",
            settings=mock.Mock(),
        )

        assert status == {
            "app_name": mock_job_config.get_sanitised_deployment_name.return_value,
            "desired_state": mock_job_config.get_desired_state.return_value,
            "desired_instances": mock_job_config.get_instances.return_value,
            "bounce_method": mock_job_config.get_bounce_method.return_value,
            "versions": [
                {
                    "container_port": 8080,
                    "type": "ReplicaSet",
                    "name": "replicaset_1",
                    "replicas": 1,
                    "ready_replicas": 0,
                    "create_timestamp": datetime.datetime(2021, 3, 5).timestamp(),
                    "git_sha": "aaa000",
                    "image_version": None,
                    "config_sha": "config000",
                    "namespace": "paasta",
                    "pods": [
                        {
                            "name": "pod_1",
                            "ip": "1.2.3.4",
                            "create_timestamp": datetime.datetime(
                                2021, 3, 6
                            ).timestamp(),
                            "delete_timestamp": None,
                            "host": "4.3.2.1",
                            "phase": "Running",
                            "reason": None,
                            "message": None,
                            "scheduled": True,
                            "ready": True,
                            "mesh_ready": None,
                            "events": [],
                            "containers": [
                                {
                                    "healthcheck_grace_period": 1,
                                    "healthcheck_cmd": {
                                        "http_url": "http://1.2.3.4:8080/healthcheck"
                                    },
                                    "name": "main_container",
                                    "restart_count": 0,
                                    "state": "running",
                                    "reason": "a_state_reason",
                                    "message": "a_state_message",
                                    "last_state": "terminated",
                                    "last_reason": "a_last_state_reason",
                                    "last_message": "a_last_state_message",
                                    "last_duration": 86400.0,
                                    "last_timestamp": datetime.datetime(
                                        2021, 3, 4
                                    ).timestamp(),
                                    "previous_tail_lines": None,
                                    "timestamp": datetime.datetime(
                                        2021, 3, 6
                                    ).timestamp(),
                                    "tail_lines": {
                                        "error_message": "",
                                        "stderr": [],
                                        "stdout": [],
                                    },
                                },
                            ],
                        },
                    ],
                }
            ],
        }

    def test_statefulset(
        self,
        mock_controller_revisions_for_service_instance,
        mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS,
        mock_load_service_namespace_config,
        mock_pods_for_service_instance,
        mock_mesh_status,
        mock_pod,
        mock_find_all_relevant_namespaces,
    ):
        mock_find_all_relevant_namespaces.return_value = ["paasta"]
        mock_job_config = mock.Mock(
            get_persistent_volumes=mock.Mock(return_value=[mock.Mock]),
        )
        mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS[
            "kubernetes"
        ].loader.return_value = mock_job_config
        mock_job_config.get_container_port.return_value = 8080
        mock_controller_revisions_for_service_instance.return_value = [
            Struct(
                metadata=Struct(
                    name="controller_revision_1",
                    namespace="paasta",
                    creation_timestamp=datetime.datetime(2021, 4, 1),
                    labels={
                        "paasta.yelp.com/git_sha": "aaa000",
                        "paasta.yelp.com/config_sha": "config000",
                    },
                ),
            ),
        ]

        mock_pod.metadata.owner_references = []
        mock_pods_for_service_instance.return_value = [mock_pod]

        with mock.patch(
            "paasta_tools.instance.kubernetes.get_pod_status",
            new_callable=AsyncMock,
            autospec=None,
        ) as mock_get_pod_status:
            mock_get_pod_status.return_value = {}
            status = pik.kubernetes_status_v2(
                service="service",
                instance="instance",
                verbose=0,
                include_envoy=False,
                instance_type="kubernetes",
                settings=mock.Mock(),
            )

        assert len(status["versions"]) == 1
        assert status["versions"][0] == {
            "container_port": 8080,
            "name": "controller_revision_1",
            "type": "ControllerRevision",
            "replicas": 1,
            "ready_replicas": 1,
            "create_timestamp": datetime.datetime(2021, 4, 1).timestamp(),
            "git_sha": "aaa000",
            "image_version": None,
            "config_sha": "config000",
            "pods": [mock.ANY],
            "namespace": "paasta",
        }

    def test_statefulset_with_image_version(
        self,
        mock_controller_revisions_for_service_instance,
        mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS,
        mock_load_service_namespace_config,
        mock_pods_for_service_instance,
        mock_mesh_status,
        mock_pod,
        mock_find_all_relevant_namespaces,
    ):
        mock_find_all_relevant_namespaces.return_value = ["paasta"]
        mock_job_config = mock.Mock(
            get_persistent_volumes=mock.Mock(return_value=[mock.Mock]),
        )
        mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS[
            "kubernetes"
        ].loader.return_value = mock_job_config
        mock_job_config.get_container_port.return_value = 8080
        mock_controller_revisions_for_service_instance.return_value = [
            Struct(
                metadata=Struct(
                    name="controller_revision_1",
                    namespace="paasta",
                    creation_timestamp=datetime.datetime(2021, 4, 1),
                    labels={
                        "paasta.yelp.com/git_sha": "aaa000",
                        "paasta.yelp.com/config_sha": "config000",
                        "paasta.yelp.com/image_version": "extrastuff",
                    },
                ),
            ),
        ]

        mock_pod.metadata.owner_references = []
        mock_pods_for_service_instance.return_value = [mock_pod]

        with mock.patch(
            "paasta_tools.instance.kubernetes.get_pod_status",
            new_callable=AsyncMock,
            autospec=None,
        ) as mock_get_pod_status:
            mock_get_pod_status.return_value = {}
            status = pik.kubernetes_status_v2(
                service="service",
                instance="instance",
                verbose=0,
                include_envoy=False,
                instance_type="kubernetes",
                settings=mock.Mock(),
            )

        assert len(status["versions"]) == 1
        assert status["versions"][0] == {
            "container_port": 8080,
            "name": "controller_revision_1",
            "type": "ControllerRevision",
            "replicas": 1,
            "ready_replicas": 1,
            "create_timestamp": datetime.datetime(2021, 4, 1).timestamp(),
            "git_sha": "aaa000",
            "image_version": "extrastuff",
            "config_sha": "config000",
            "pods": [mock.ANY],
            "namespace": "paasta",
        }

    def test_event_timeout(
        self,
        mock_replicasets_for_service_instance,
        mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS,
        mock_load_service_namespace_config,
        mock_pods_for_service_instance,
        mock_mesh_status,
        mock_get_pod_event_messages,
        mock_pod,
        mock_find_all_relevant_namespaces,
    ):
        mock_find_all_relevant_namespaces.return_value = ["paasta"]
        mock_job_config = mock.Mock(
            get_persistent_volumes=mock.Mock(return_value=[]),
        )
        mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS[
            "kubernetes"
        ].loader.return_value = mock_job_config
        mock_replicasets_for_service_instance.return_value = [
            Struct(
                spec=Struct(replicas=1),
                metadata=Struct(
                    name="replicaset_1",
                    namespace="paasta",
                    creation_timestamp=datetime.datetime(2021, 3, 5),
                    deletion_timestamp=None,
                    labels={
                        "paasta.yelp.com/git_sha": "aaa000",
                        "paasta.yelp.com/config_sha": "config000",
                    },
                ),
            ),
        ]
        mock_pods_for_service_instance.return_value = [mock_pod]
        mock_load_service_namespace_config.return_value = {}
        mock_job_config.get_registrations.return_value = ["service.instance"]
        mock_get_pod_event_messages.side_effect = asyncio.TimeoutError

        status = pik.kubernetes_status_v2(
            service="service",
            instance="instance",
            verbose=0,
            include_envoy=False,
            instance_type="kubernetes",
            settings=mock.Mock(),
        )

        # Verify  we did not throw an exception
        assert status
        assert all(
            p["events"] == [{"error": "Could not retrieve events. Please try again."}]
            for p in status["versions"][0]["pods"]
        )

    def test_pod_timeout(
        self,
        mock_replicasets_for_service_instance,
        mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS,
        mock_load_service_namespace_config,
        mock_pods_for_service_instance,
        mock_mesh_status,
        mock_get_pod_event_messages,
        mock_pod,
        mock_find_all_relevant_namespaces,
    ):
        mock_find_all_relevant_namespaces.return_value = ["paasta"]
        mock_job_config = mock.Mock(
            get_persistent_volumes=mock.Mock(return_value=[]),
        )
        mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS[
            "kubernetes"
        ].loader.return_value = mock_job_config
        mock_replicasets_for_service_instance.return_value = [
            Struct(
                spec=Struct(replicas=1),
                metadata=Struct(
                    name="replicaset_1",
                    creation_timestamp=datetime.datetime(2021, 3, 5),
                    deletion_timestamp=None,
                    labels={
                        "paasta.yelp.com/git_sha": "aaa000",
                        "paasta.yelp.com/config_sha": "config000",
                    },
                ),
            ),
        ]
        mock_load_service_namespace_config.return_value = {}
        mock_job_config.get_registrations.return_value = ["service.instance"]
        mock_get_pod_event_messages.return_value = []
        mock_pods_for_service_instance.side_effect = asyncio.TimeoutError

        status = pik.kubernetes_status_v2(
            service="service",
            instance="instance",
            verbose=0,
            include_envoy=False,
            instance_type="kubernetes",
            settings=mock.Mock(),
        )

        # Verify  we did not throw an exception
        assert status
        assert "Could not fetch instance data" in status["error_message"]

    def test_all_namespaces(
        self,
        mock_replicasets_for_service_instance,
        mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS,
        mock_load_service_namespace_config,
        mock_pods_for_service_instance,
        mock_mesh_status,
        mock_get_pod_event_messages,
        mock_pod,
        mock_find_all_relevant_namespaces,
    ):
        mock_find_all_relevant_namespaces.return_value = ["paasta"]
        mock_job_config = mock.Mock(
            get_persistent_volumes=mock.Mock(return_value=[]),
            get_kubernetes_namespace=mock.Mock(return_value="paastasvc-service"),
        )
        mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS[
            "kubernetes"
        ].loader.return_value = mock_job_config
        mock_replicasets_for_service_instance.return_value = [
            Struct(
                spec=Struct(replicas=1),
                metadata=Struct(
                    name="replicaset_1",
                    creation_timestamp=datetime.datetime(2021, 3, 5),
                    deletion_timestamp=None,
                    labels={
                        "paasta.yelp.com/git_sha": "aaa000",
                        "paasta.yelp.com/config_sha": "config000",
                    },
                ),
            ),
        ]
        mock_load_service_namespace_config.return_value = {}
        mock_job_config.get_registrations.return_value = ["service.instance"]
        mock_get_pod_event_messages.return_value = []

        with mock.patch(
            "paasta_tools.instance.kubernetes.get_versions_for_replicasets",
            new_callable=AsyncMock,
            autospec=None,
        ) as mock_get_versions_for_replicasets:
            pik.kubernetes_status_v2(
                service="service",
                instance="instance",
                verbose=0,
                include_envoy=False,
                instance_type="kubernetes",
                settings=mock.Mock(),
            )

        # We are only testing that we
        assert not mock_find_all_relevant_namespaces.called
        _, _, get_rs_kwargs = mock_get_versions_for_replicasets.mock_calls[0]
        assert get_rs_kwargs["namespaces"] == {"paastasvc-service"}

        with mock.patch(
            "paasta_tools.instance.kubernetes.get_versions_for_replicasets",
            new_callable=AsyncMock,
            autospec=None,
        ) as mock_get_versions_for_replicasets:
            pik.kubernetes_status_v2(
                service="service",
                instance="instance",
                verbose=0,
                include_envoy=False,
                instance_type="kubernetes",
                settings=mock.Mock(),
                all_namespaces=True,
            )
        assert mock_find_all_relevant_namespaces.called
        _, _, get_rs_kwargs = mock_get_versions_for_replicasets.mock_calls[0]
        assert get_rs_kwargs["namespaces"] == ["paasta"]


@mock.patch("paasta_tools.kubernetes_tools.get_kubernetes_app_by_name", autospec=True)
def test_job_status_include_replicaset_non_verbose(mock_get_kubernetes_app_by_name):
    kstatus = {}
    a_sync.block(
        pik.job_status,
        kstatus=kstatus,
        client=mock.Mock(),
        job_config=mock.Mock(),
        pod_list=[],
        replicaset_list=[mock.Mock(), mock.Mock(), mock.Mock()],
        verbose=0,
        namespace=mock.Mock(),
    )

    assert len(kstatus["replicasets"]) == 3


def test_cr_status_bad_instance_type():
    with pytest.raises(RuntimeError) as excinfo:
        pik.cr_status(
            service="",
            instance="",
            verbose=0,
            instance_type="marathon",
            kube_client=mock.Mock(),
        )
    assert "Unknown instance type" in str(excinfo.value)


@mock.patch("paasta_tools.kubernetes_tools.get_cr", autospec=True)
def test_cr_status_happy_path(mock_get_cr):
    mock_status = mock.Mock()
    mock_metadata = mock.Mock()
    mock_return = dict(status=mock_status, metadata=mock_metadata)
    mock_get_cr.return_value = mock_return
    status = pik.cr_status(
        service="",
        instance="",
        verbose=0,
        instance_type="flink",
        kube_client=mock.Mock(),
    )
    assert status == mock_return


def test_set_cr_desired_state_invalid_instance_type():
    with pytest.raises(RuntimeError) as excinfo:
        pik.set_cr_desired_state(
            kube_client=mock.Mock(),
            service=mock.Mock(),
            instance=mock.Mock(),
            instance_type="marathon",
            desired_state=mock.Mock(),
        )
    assert "Unknown instance type" in str(excinfo.value)


@mock.patch("paasta_tools.kubernetes_tools.set_cr_desired_state", autospec=True)
def test_set_cr_desired_state_calls_k8s_tools(mock_set_cr_desired_state):
    pik.set_cr_desired_state(
        kube_client=mock.Mock(),
        service=mock.Mock(),
        instance=mock.Mock(),
        instance_type="flink",
        desired_state=mock.Mock(),
    )
    assert len(mock_set_cr_desired_state.mock_calls) == 1


def test_can_set_state():
    for it in pik.INSTANCE_TYPES_WITH_SET_STATE:
        assert pik.can_set_state(it)

    assert not pik.can_set_state("marathon")


def test_can_handle():
    for it in pik.INSTANCE_TYPES:
        assert pik.can_handle(it)

    assert not pik.can_handle("marathon")


def test_filter_actually_running_replicasets():
    replicaset_list = [
        mock.Mock(),
        mock.Mock(),
        mock.Mock(),
        mock.Mock(),
    ]
    # the `spec` kwarg is special to Mock so we have to set it this way.
    replicaset_list[0].configure_mock(
        **{"spec.replicas": 5, "status.ready_replicas": 5}
    )
    replicaset_list[1].configure_mock(
        **{"spec.replicas": 5, "status.ready_replicas": 0}
    )
    replicaset_list[2].configure_mock(
        **{"spec.replicas": 0, "status.ready_replicas": 0}
    )
    replicaset_list[3].configure_mock(
        **{"spec.replicas": 0, "status.ready_replicas": 5}
    )

    expected = [
        replicaset_list[0],
        replicaset_list[1],
        replicaset_list[3],
    ]
    assert pik.filter_actually_running_replicasets(replicaset_list) == expected


@pytest.mark.asyncio
async def test_get_pod_status_mesh_ready(event_loop):
    with mock.patch(
        "paasta_tools.instance.kubernetes.get_pod_containers",
        new_callable=AsyncMock,
        autospec=None,
    ) as mock_get_pod_containers, mock.patch(
        "paasta_tools.kubernetes_tools.is_pod_scheduled", autospec=True
    ) as mock_is_pod_scheduled, mock.patch(
        "paasta_tools.kubernetes_tools.get_pod_event_messages",
        new_callable=AsyncMock,
        autospec=None,
    ) as mock_get_pod_event_messages:
        mock_get_pod_containers.return_value = []
        mock_get_pod_event_messages.return_value = []
        mock_is_pod_scheduled.return_value = True
        mock_pod = mock.MagicMock()
        mock_pod.status.pod_ip = "1.2.3.4"
        mock_ready_condition = mock.MagicMock()
        mock_ready_condition.type = "Ready"
        mock_ready_condition.status = "True"
        mock_kube_client = mock.MagicMock()
        mock_pod.status.conditions = [mock_ready_condition]
        backends_task = wrap_value_in_task([{"address": "0.0.0.0"}])
        status = await pik.get_pod_status(mock_pod, backends_task, mock_kube_client, 10)
    assert status["ready"]
    assert not status["mesh_ready"]


def test_kubernetes_mesh_status_include_envoy():
    with mock.patch(
        "paasta_tools.kubernetes_tools.load_service_namespace_config", autospec=True
    ) as mock_load_service_namespace_config, mock.patch(
        "paasta_tools.instance.kubernetes.mesh_status",
        new_callable=AsyncMock,
        autospec=None,
    ) as mock_mesh_status, mock.patch(
        "paasta_tools.kubernetes_tools.pods_for_service_instance",
        new_callable=AsyncMock,
        autospec=None,
    ) as mock_pods_for_service_instance, mock.patch(
        "paasta_tools.instance.kubernetes.LONG_RUNNING_INSTANCE_TYPE_HANDLERS",
        {"flink": mock.Mock()},
        autospec=False,
    ):
        mock_load_service_namespace_config.return_value = {"proxy_port": 1234}
        mock_pods_for_service_instance.return_value = ["pod_1"]
        mock_job_config = pik.LONG_RUNNING_INSTANCE_TYPE_HANDLERS[
            "flink"
        ].loader.return_value
        mock_settings = mock.Mock()

        kmesh = pik.kubernetes_mesh_status(
            service="fake_service",
            instance="fake_instance",
            instance_type="flink",
            settings=mock_settings,
            include_envoy=True,
        )

        assert len(kmesh) == 1
        assert kmesh.get("envoy") == mock_mesh_status.return_value
        assert mock_mesh_status.call_args_list[0] == mock.call(
            service="fake_service",
            instance=mock_job_config.get_nerve_namespace.return_value,
            job_config=mock_job_config,
            service_namespace_config={"proxy_port": 1234},
            pods_task=mock.ANY,
            should_return_individual_backends=True,
            settings=mock_settings,
            service_mesh=getattr(pik.ServiceMesh, "ENVOY"),
        )
        _, kwargs = mock_mesh_status.call_args_list[0]
        assert kwargs["pods_task"].result() == ["pod_1"]

        # include_envoy = False should error
        with pytest.raises(RuntimeError) as excinfo:
            kmesh = pik.kubernetes_mesh_status(
                service="fake_service",
                instance="fake_instance",
                instance_type="flink",
                settings=mock_settings,
                include_envoy=False,
            )
        assert "No mesh types specified" in str(excinfo.value)


@mock.patch(
    "paasta_tools.kubernetes_tools.load_service_namespace_config", autospec=True
)
@mock.patch(
    "paasta_tools.instance.kubernetes.mesh_status",
    new_callable=AsyncMock,
    autospec=None,
)
@mock.patch(
    "paasta_tools.kubernetes_tools.pods_for_service_instance",
    mock.Mock(return_value=("pod_1")),
    autospec=False,
)
@mock.patch(
    "paasta_tools.instance.kubernetes.LONG_RUNNING_INSTANCE_TYPE_HANDLERS",
    {"flink": mock.Mock()},
    autospec=False,
)
@pytest.mark.parametrize(
    "include_mesh,inst_type,service_ns_conf,expected_msg",
    [
        (False, "flink", {"proxy_port": 1234}, "No mesh types"),
        (True, "tron", {"proxy_port": 1234}, "not supported"),
        (True, "flink", {}, "not configured"),
    ],
)
def test_kubernetes_mesh_status_error(
    mock_mesh_status,
    mock_load_service_namespace_config,
    include_mesh,
    inst_type,
    service_ns_conf,
    expected_msg,
):
    mock_load_service_namespace_config.return_value = service_ns_conf
    mock_settings = mock.Mock()

    with pytest.raises(RuntimeError) as excinfo:
        pik.kubernetes_mesh_status(
            service="fake_service",
            instance="fake_instance",
            instance_type=inst_type,
            settings=mock_settings,
            include_envoy=include_mesh,
        )

    assert expected_msg in excinfo.value.args[0]
    assert mock_mesh_status.call_args_list == []


@pytest.mark.asyncio
async def test_backends_from_mesh_status():
    mock_mesh_status = {
        "locations": [
            {"backends": [{"address": "1.1.1.1"}, {"address": "1.2.2.2"}]},
            {"backends": [{"address": "1.1.1.1"}, {"address": "2.2.2.2"}]},
        ]
    }

    mesh_status_task = wrap_value_in_task(mock_mesh_status)

    backends = await pik.get_backends_from_mesh_status(mesh_status_task)
    assert backends == {"1.1.1.1", "1.2.2.2", "2.2.2.2"}


def test_bounce_status():
    with mock.patch(
        "paasta_tools.instance.kubernetes.kubernetes_tools", autospec=True
    ) as mock_kubernetes_tools:
        mock_config = mock_kubernetes_tools.load_kubernetes_service_config.return_value
        mock_kubernetes_tools.get_kubernetes_app_deploy_status.return_value = (
            "deploy_status",
            "message",
        )
        mock_kubernetes_tools.get_active_versions_for_service.return_value = [
            (DeploymentVersion("aaa", None), "config_aaa"),
            (DeploymentVersion("bbb", None), "config_bbb"),
            (DeploymentVersion("ccc", "extrastuff"), "config_ccc"),
        ]
        mock_kubernetes_tools.replicasets_for_service_instance = AsyncMock(
            return_value=[]
        )
        mock_kubernetes_tools.controller_revisions_for_service_instance = AsyncMock(
            return_value=[]
        )

        mock_settings = mock.Mock()
        status = pik.bounce_status("fake_service", "fake_instance", mock_settings)
        assert status == {
            "expected_instance_count": mock_config.get_instances.return_value,
            "desired_state": mock_config.get_desired_state.return_value,
            "running_instance_count": mock_kubernetes_tools.get_kubernetes_app_by_name.return_value.status.ready_replicas,
            "deploy_status": mock_kubernetes_tools.KubernetesDeployStatus.tostring.return_value,
            "active_shas": [
                ("aaa", "config_aaa"),
                ("bbb", "config_bbb"),
                ("ccc", "config_ccc"),
            ],
            "active_versions": [
                ("aaa", None, "config_aaa"),
                ("bbb", None, "config_bbb"),
                ("ccc", "extrastuff", "config_ccc"),
            ],
            "app_count": 3,
        }


@pytest.mark.asyncio
async def test_get_pod_containers(mock_pod):
    mock_client = mock.Mock()

    with mock.patch(
        "paasta_tools.instance.kubernetes.get_tail_lines_for_kubernetes_container",
        new_callable=AsyncMock,
        autospec=None,
        side_effect=[["current"], ["previous"], ["current"], ["previous"]],
    ), mock.patch(
        "paasta_tools.kubernetes_tools.recent_container_restart",
        return_value=True,
        autospec=None,
    ):
        containers = await pik.get_pod_containers(mock_pod, mock_client, 10)
        mock_pod.status.container_statuses[0].state.running["started_at"] = None
        no_start_containers = await pik.get_pod_containers(mock_pod, mock_client, 10)

    assert containers == [
        dict(
            name="main_container",
            restart_count=0,
            state="running",
            reason="a_state_reason",
            message="a_state_message",
            last_state="terminated",
            last_reason="a_last_state_reason",
            last_message="a_last_state_message",
            last_duration=86400.0,
            last_timestamp=datetime.datetime(2021, 3, 4).timestamp(),
            previous_tail_lines=["previous"],
            timestamp=datetime.datetime(2021, 3, 6).timestamp(),
            healthcheck_grace_period=1,
            healthcheck_cmd={"http_url": "http://1.2.3.4:8080/healthcheck"},
            tail_lines=["current"],
        ),
    ]

    assert no_start_containers[0]["timestamp"] is None


@pytest.mark.asyncio
async def test_mesh_status_retry_on_timeout_then_success():
    """Test that mesh_status retries on ConnectTimeout and succeeds on second attempt."""
    with mock.patch(
        "paasta_tools.instance.kubernetes.kubernetes_tools.get_all_nodes",
        return_value=[],
        autospec=True,
    ), mock.patch(
        "paasta_tools.instance.kubernetes.KubeSmartstackEnvoyReplicationChecker",
        autospec=True,
    ) as mock_checker_class, mock.patch(
        "paasta_tools.instance.kubernetes._build_smartstack_location_dict",
        autospec=True,
    ) as mock_build_dict, mock.patch(
        "paasta_tools.instance.kubernetes.get_expected_instance_count_for_namespace",
        return_value=6,
        autospec=True,
    ):
        mock_checker = mock_checker_class.return_value
        mock_checker.get_allowed_locations_and_hosts.return_value = {
            "location1": ["host1", "host2", "host3"]
        }
        mock_checker.get_hostname_in_pool.side_effect = ["host1", "host2"]

        # First call raises ConnectTimeout, second succeeds
        mock_build_dict.side_effect = [
            requests.exceptions.ConnectTimeout(),
            {"location": "location1", "backends": []},
        ]

        mock_job_config = mock.Mock()
        mock_job_config.get_registrations.return_value = ["service.main"]
        mock_job_config.get_pool.return_value = "default"
        mock_job_config.get_nerve_namespace.return_value = "service.main"

        mock_settings = mock.Mock()
        mock_settings.system_paasta_config.get_synapse_port.return_value = 3212
        mock_settings.system_paasta_config.get_synapse_haproxy_url_format.return_value = (
            "http://{host}:{port}/status"
        )

        pods_task = wrap_value_in_task([])

        result = await pik.mesh_status(
            service="test_service",
            service_mesh=pik.ServiceMesh.SMARTSTACK,
            instance="test_instance",
            job_config=mock_job_config,
            service_namespace_config={},
            pods_task=pods_task,
            settings=mock_settings,
        )

        assert result["registration"] == "service.main"
        assert len(result["locations"]) == 1
        assert mock_checker.get_hostname_in_pool.call_count == 2


@pytest.mark.asyncio
async def test_mesh_status_all_retries_exhausted():
    """Test that mesh_status raises ConnectTimeout when all retry attempts fail."""
    with mock.patch(
        "paasta_tools.instance.kubernetes.kubernetes_tools.get_all_nodes",
        return_value=[],
        autospec=True,
    ), mock.patch(
        "paasta_tools.instance.kubernetes.KubeSmartstackEnvoyReplicationChecker",
        autospec=True,
    ) as mock_checker_class, mock.patch(
        "paasta_tools.instance.kubernetes._build_smartstack_location_dict",
        autospec=True,
    ) as mock_build_dict, mock.patch(
        "paasta_tools.instance.kubernetes.get_expected_instance_count_for_namespace",
        return_value=6,
        autospec=True,
    ):
        mock_checker = mock_checker_class.return_value
        mock_checker.get_allowed_locations_and_hosts.return_value = {
            "location1": ["host1", "host2", "host3"]
        }
        mock_checker.get_hostname_in_pool.side_effect = ["host1", "host2", "host3"]

        # All attempts raise ConnectTimeout
        import requests.exceptions

        mock_build_dict.side_effect = requests.exceptions.ConnectTimeout()

        mock_job_config = mock.Mock()
        mock_job_config.get_registrations.return_value = ["service.main"]
        mock_job_config.get_pool.return_value = "default"
        mock_job_config.get_nerve_namespace.return_value = "service.main"

        mock_settings = mock.Mock()
        mock_settings.system_paasta_config.get_synapse_port.return_value = 3212
        mock_settings.system_paasta_config.get_synapse_haproxy_url_format.return_value = (
            "http://{host}:{port}/status"
        )

        pods_task = wrap_value_in_task([])

        with pytest.raises(requests.exceptions.ConnectTimeout):
            await pik.mesh_status(
                service="test_service",
                service_mesh=pik.ServiceMesh.SMARTSTACK,
                instance="test_instance",
                job_config=mock_job_config,
                service_namespace_config={},
                pods_task=pods_task,
                settings=mock_settings,
            )

        # Verify all 3 attempts were made
        assert mock_checker.get_hostname_in_pool.call_count == 3


@pytest.mark.asyncio
async def test_mesh_status_retry_switches_hosts():
    """Test that mesh_status calls get_hostname_in_pool on each retry to switch hosts."""
    with mock.patch(
        "paasta_tools.instance.kubernetes.kubernetes_tools.get_all_nodes",
        return_value=[],
        autospec=True,
    ), mock.patch(
        "paasta_tools.instance.kubernetes.KubeSmartstackEnvoyReplicationChecker",
        autospec=True,
    ) as mock_checker_class, mock.patch(
        "paasta_tools.instance.kubernetes._build_smartstack_location_dict",
        autospec=True,
    ) as mock_build_dict, mock.patch(
        "paasta_tools.instance.kubernetes.get_expected_instance_count_for_namespace",
        return_value=6,
        autospec=True,
    ):
        mock_checker = mock_checker_class.return_value
        mock_checker.get_allowed_locations_and_hosts.return_value = {
            "location1": ["host1", "host2", "host3"]
        }

        # First two calls timeout, third succeeds
        mock_build_dict.side_effect = [
            requests.exceptions.ConnectTimeout(),
            requests.exceptions.ConnectTimeout(),
            {"location": "location1", "backends": []},
        ]

        mock_job_config = mock.Mock()
        mock_job_config.get_registrations.return_value = ["service.main"]
        mock_job_config.get_pool.return_value = "default"
        mock_job_config.get_nerve_namespace.return_value = "service.main"

        mock_settings = mock.Mock()
        mock_settings.system_paasta_config.get_synapse_port.return_value = 3212
        mock_settings.system_paasta_config.get_synapse_haproxy_url_format.return_value = (
            "http://{host}:{port}/status"
        )

        pods_task = wrap_value_in_task([])

        await pik.mesh_status(
            service="test_service",
            service_mesh=pik.ServiceMesh.SMARTSTACK,
            instance="test_instance",
            job_config=mock_job_config,
            service_namespace_config={},
            pods_task=pods_task,
            settings=mock_settings,
        )

        # Verify get_hostname_in_pool was called 3 times (once per attempt)
        assert mock_checker.get_hostname_in_pool.call_count == 3


@pytest.mark.asyncio
async def test_mesh_status_no_timeout_success():
    """Test that mesh_status succeeds on first attempt without retries when no timeout occurs."""
    with mock.patch(
        "paasta_tools.instance.kubernetes.kubernetes_tools.get_all_nodes",
        return_value=[],
        autospec=True,
    ), mock.patch(
        "paasta_tools.instance.kubernetes.KubeSmartstackEnvoyReplicationChecker",
        autospec=True,
    ) as mock_checker_class, mock.patch(
        "paasta_tools.instance.kubernetes._build_smartstack_location_dict",
        autospec=True,
    ) as mock_build_dict, mock.patch(
        "paasta_tools.instance.kubernetes.get_expected_instance_count_for_namespace",
        return_value=6,
        autospec=True,
    ):
        mock_checker = mock_checker_class.return_value
        mock_checker.get_allowed_locations_and_hosts.return_value = {
            "location1": ["host1", "host2", "host3"]
        }
        mock_checker.get_hostname_in_pool.return_value = "host1"

        # First call succeeds immediately
        mock_build_dict.return_value = {"location": "location1", "backends": []}

        mock_job_config = mock.Mock()
        mock_job_config.get_registrations.return_value = ["service.main"]
        mock_job_config.get_pool.return_value = "default"
        mock_job_config.get_nerve_namespace.return_value = "service.main"

        mock_settings = mock.Mock()
        mock_settings.system_paasta_config.get_synapse_port.return_value = 3212
        mock_settings.system_paasta_config.get_synapse_haproxy_url_format.return_value = (
            "http://{host}:{port}/status"
        )

        pods_task = wrap_value_in_task([])

        result = await pik.mesh_status(
            service="test_service",
            service_mesh=pik.ServiceMesh.SMARTSTACK,
            instance="test_instance",
            job_config=mock_job_config,
            service_namespace_config={},
            pods_task=pods_task,
            settings=mock_settings,
        )

        assert result["registration"] == "service.main"
        assert len(result["locations"]) == 1
        # Verify only one attempt was made (no retries)
        assert mock_checker.get_hostname_in_pool.call_count == 1
        assert mock_build_dict.call_count == 1


@pytest.mark.asyncio
async def test_mesh_status_envoy_timeout_retry():
    """Test that mesh_status retries on ConnectTimeout for Envoy mesh."""
    with mock.patch(
        "paasta_tools.instance.kubernetes.kubernetes_tools.get_all_nodes",
        return_value=[],
        autospec=True,
    ), mock.patch(
        "paasta_tools.instance.kubernetes.KubeSmartstackEnvoyReplicationChecker",
        autospec=True,
    ) as mock_checker_class, mock.patch(
        "paasta_tools.instance.kubernetes._build_envoy_location_dict",
        autospec=True,
    ) as mock_build_dict, mock.patch(
        "paasta_tools.instance.kubernetes.get_expected_instance_count_for_namespace",
        return_value=6,
        autospec=True,
    ):
        mock_checker = mock_checker_class.return_value
        mock_checker.get_allowed_locations_and_hosts.return_value = {
            "location1": ["host1", "host2", "host3"]
        }
        mock_checker.get_hostname_in_pool.side_effect = ["host1", "host2"]

        # First call raises ConnectTimeout, second succeeds
        import requests.exceptions

        mock_build_dict.side_effect = [
            requests.exceptions.ConnectTimeout(),
            {"location": "location1", "backends": []},
        ]

        mock_job_config = mock.Mock()
        mock_job_config.get_registrations.return_value = ["service.main"]
        mock_job_config.get_pool.return_value = "default"
        mock_job_config.get_nerve_namespace.return_value = "service.main"

        mock_settings = mock.Mock()
        mock_settings.system_paasta_config.get_envoy_admin_port.return_value = 9901
        mock_settings.system_paasta_config.get_envoy_admin_endpoint_format.return_value = (
            "http://{host}:{port}/clusters"
        )

        pods_task = wrap_value_in_task([])

        result = await pik.mesh_status(
            service="test_service",
            service_mesh=pik.ServiceMesh.ENVOY,
            instance="test_instance",
            job_config=mock_job_config,
            service_namespace_config={},
            pods_task=pods_task,
            settings=mock_settings,
        )

        assert result["registration"] == "service.main"
        assert len(result["locations"]) == 1
        assert mock_checker.get_hostname_in_pool.call_count == 2


@pytest.mark.parametrize(
    "test_type,expected",
    [
        ("kubernetes", True),
        ("eks", True),
        ("tron", False),
        ("adhoc", False),
        ("flink", False),
    ],
)
def test_can_restart_replica(test_type, expected):
    """Test that only standard k8s instance types support replica restart."""
    assert pik.can_restart_replica(test_type) is expected


@mock.patch("paasta_tools.instance.kubernetes.ServiceNamespaceConfig", autospec=True)
@mock.patch(
    "paasta_tools.instance.kubernetes.kubernetes_tools.delete_pod_by_name",
    autospec=True,
)
@mock.patch(
    "paasta_tools.instance.kubernetes.LONG_RUNNING_INSTANCE_TYPE_HANDLERS",
    autospec=True,
)
def test_restart_replica_by_name_success(
    mock_handlers, mock_delete_pod, mock_service_namespace_config
):
    """Test successful replica restart by name."""

    mock_settings = mock.Mock()
    mock_settings.cluster = "test-cluster"
    mock_settings.soa_dir = "/test/soa/dir"
    mock_settings.kubernetes_client = mock.Mock()

    mock_loader = mock.Mock()
    mock_job_config = mock.Mock()
    mock_job_config.get_kubernetes_namespace.return_value = "test-namespace"
    mock_job_config.get_termination_grace_period.return_value = None
    mock_loader.return_value = mock_job_config

    mock_handlers.__getitem__.return_value = mock.Mock(loader=mock_loader)
    mock_delete_pod.return_value = True

    # Mock ServiceNamespaceConfig
    mock_service_namespace_config_instance = mock.Mock()
    mock_service_namespace_config.return_value = mock_service_namespace_config_instance

    result = pik.restart_replica_by_name(
        service="test-service",
        instance="main",
        instance_type="kubernetes",
        replica_name="test-pod-12345",
        settings=mock_settings,
    )

    assert result is True

    mock_loader.assert_called_once_with(
        service="test-service",
        instance="main",
        cluster="test-cluster",
        soa_dir="/test/soa/dir",
        load_deployments=False,
    )

    mock_delete_pod.assert_called_once_with(
        pod_name="test-pod-12345",
        service="test-service",
        instance="main",
        namespace="test-namespace",
        kube_client=mock_settings.kubernetes_client,
        grace_period_seconds=None,
    )


@mock.patch(
    "paasta_tools.instance.kubernetes.kubernetes_tools.delete_pod_by_name",
    autospec=True,
)
@mock.patch(
    "paasta_tools.instance.kubernetes.LONG_RUNNING_INSTANCE_TYPE_HANDLERS",
    autospec=True,
)
def test_restart_replica_by_name_pod_not_found(mock_handlers, mock_delete_pod):
    """Test replica restart when pod is not found."""
    mock_settings = mock.Mock()
    mock_settings.cluster = "test-cluster"
    mock_settings.soa_dir = "/test/soa/dir"
    mock_settings.kubernetes_client = mock.Mock()

    mock_loader = mock.Mock()
    mock_job_config = mock.Mock()
    mock_job_config.get_kubernetes_namespace.return_value = "test-namespace"
    mock_loader.return_value = mock_job_config

    mock_handlers.__getitem__.return_value = mock.Mock(loader=mock_loader)

    # delete_pod's False return value implies pod not found
    mock_delete_pod.return_value = False

    result = pik.restart_replica_by_name(
        service="test-service",
        instance="main",
        instance_type="kubernetes",
        replica_name="nonexistent-pod-99999",
        settings=mock_settings,
    )

    assert result is False


def test_restart_replica_by_name_unsupported_instance_type():
    """Test replica restart with unsupported instance type."""
    mock_settings = mock.Mock()

    with pytest.raises(
        RuntimeError, match="Replica restart not supported for instance type tron"
    ):
        pik.restart_replica_by_name(
            service="test-service",
            instance="main",
            instance_type="tron",
            replica_name="test-pod-12345",
            settings=mock_settings,
        )


@mock.patch(
    "paasta_tools.instance.kubernetes.LONG_RUNNING_INSTANCE_TYPE_HANDLERS",
    autospec=True,
)
def test_restart_replica_by_name_no_kubernetes_client(mock_handlers):
    """Test replica restart when Kubernetes client is not available."""
    # Mock the settings without kubernetes client
    mock_settings = mock.Mock()
    mock_settings.cluster = "test-cluster"
    mock_settings.soa_dir = "/test/soa/dir"
    mock_settings.kubernetes_client = None

    # Mock the job config loader and job config
    mock_loader = mock.Mock()
    mock_job_config = mock.Mock()
    mock_job_config.get_kubernetes_namespace.return_value = "test-namespace"
    mock_loader.return_value = mock_job_config

    mock_handlers.__getitem__.return_value = mock.Mock(loader=mock_loader)

    with pytest.raises(RuntimeError, match="Kubernetes client not available"):
        pik.restart_replica_by_name(
            service="test-service",
            instance="main",
            instance_type="kubernetes",
            replica_name="test-pod-12345",
            settings=mock_settings,
        )


@pytest.mark.parametrize(
    "force,expected_grace_period",
    [
        (True, 0),
        (False, None),
    ],
)
@mock.patch(
    "paasta_tools.instance.kubernetes.kubernetes_tools.delete_pod_by_name",
    autospec=True,
)
@mock.patch(
    "paasta_tools.instance.kubernetes.LONG_RUNNING_INSTANCE_TYPE_HANDLERS",
    autospec=True,
)
def test_restart_replica_by_name_force_parameter(
    mock_handlers,
    mock_delete_pod,
    force,
    expected_grace_period,
):
    """Test restart_replica_by_name with different force parameter values."""
    mock_settings = mock.Mock()
    mock_settings.cluster = "test-cluster"
    mock_settings.soa_dir = "/test/soa/dir"
    mock_settings.kubernetes_client = mock.Mock()

    mock_loader = mock.Mock()
    mock_job_config = mock.Mock()
    mock_job_config.get_kubernetes_namespace.return_value = "test-namespace"
    mock_loader.return_value = mock_job_config

    mock_handlers.__getitem__.return_value = mock.Mock(loader=mock_loader)
    mock_delete_pod.return_value = True

    result = pik.restart_replica_by_name(
        service="test-service",
        instance="main",
        instance_type="kubernetes",
        replica_name="test-pod-12345",
        settings=mock_settings,
        force=force,
    )

    assert result is True

    # Should call delete_pod_by_name with expected grace period
    mock_delete_pod.assert_called_once_with(
        pod_name="test-pod-12345",
        service="test-service",
        instance="main",
        namespace="test-namespace",
        kube_client=mock_settings.kubernetes_client,
        grace_period_seconds=expected_grace_period,
    )
