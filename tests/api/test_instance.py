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
import asynctest
import mock
import pytest
from kubernetes.client import V1Pod
from pyramid import testing

from paasta_tools import kubernetes_tools
from paasta_tools.api import settings
from paasta_tools.api.views import instance
from paasta_tools.api.views.exception import ApiFailure
from paasta_tools.instance.kubernetes import ServiceMesh
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
from paasta_tools.smartstack_tools import DiscoveredHost
from paasta_tools.smartstack_tools import HaproxyBackend
from paasta_tools.utils import NoConfigurationForServiceError


@mock.patch(
    "paasta_tools.api.views.instance.pik.match_backends_and_pods", autospec=True
)
@mock.patch(
    "paasta_tools.api.views.instance.pik.smartstack_tools.get_backends", autospec=True
)
@mock.patch(
    "paasta_tools.api.views.instance.pik.KubeSmartstackEnvoyReplicationChecker",
    autospec=True,
)
@mock.patch(
    "paasta_tools.api.views.instance.pik.kubernetes_tools.get_all_nodes", autospec=True
)
@mock.patch(
    "paasta_tools.marathon_tools.get_expected_instance_count_for_namespace",
    autospec=True,
)
def test_kubernetes_smartstack_status(
    mock_get_expected_instance_count_for_namespace,
    mock_get_all_nodes,
    mock_kube_smartstack_replication_checker,
    mock_get_backends,
    mock_match_backends_and_pods,
):
    mock_get_all_nodes.return_value = [
        {"hostname": "host1.paasta.party", "attributes": {"region": "us-north-3"}}
    ]

    mock_kube_smartstack_replication_checker.return_value.get_allowed_locations_and_hosts.return_value = {
        "us-north-3": [DiscoveredHost(hostname="host1.paasta.party", pool="default")]
    }

    mock_get_expected_instance_count_for_namespace.return_value = 2
    mock_backend = HaproxyBackend(
        status="UP",
        svname="host1_1.2.3.4:123",
        check_status="L7OK",
        check_code="0",
        check_duration="1",
        lastchg="9876",
    )
    mock_pod = mock.create_autospec(V1Pod)
    mock_match_backends_and_pods.return_value = [(mock_backend, mock_pod)]

    mock_job_config = kubernetes_tools.KubernetesDeploymentConfig(
        service="fake_service",
        cluster="fake_cluster",
        instance="fake_instance",
        config_dict={"bounce_method": "fake_bounce"},
        branch_dict=None,
    )
    mock_service_namespace_config = ServiceNamespaceConfig()
    mock_settings = mock.Mock()

    smartstack_status = instance.pik.mesh_status(
        service="fake_service",
        service_mesh=ServiceMesh.SMARTSTACK,
        instance="fake_instance",
        job_config=mock_job_config,
        service_namespace_config=mock_service_namespace_config,
        pods=[mock_pod],
        should_return_individual_backends=True,
        settings=mock_settings,
    )
    assert smartstack_status == {
        "registration": "fake_service.fake_instance",
        "expected_backends_per_location": 2,
        "locations": [
            {
                "name": "us-north-3",
                "running_backends_count": 1,
                "backends": [
                    {
                        "hostname": "host1:1.2.3.4",
                        "port": 123,
                        "status": "UP",
                        "check_status": "L7OK",
                        "check_code": "0",
                        "last_change": 9876,
                        "has_associated_task": True,
                        "check_duration": 1,
                    }
                ],
            }
        ],
    }


@mock.patch("paasta_tools.api.views.instance.adhoc_instance_status", autospec=True)
@mock.patch("paasta_tools.api.views.instance.validate_service_instance", autospec=True)
@mock.patch("paasta_tools.api.views.instance.get_actual_deployments", autospec=True)
def test_instances_status_adhoc(
    mock_get_actual_deployments,
    mock_validate_service_instance,
    mock_adhoc_instance_status,
):
    settings.cluster = "fake_cluster"
    mock_get_actual_deployments.return_value = {
        "fake_cluster.fake_instance": "GIT_SHA",
        "fake_cluster.fake_instance2": "GIT_SHA",
        "fake_cluster2.fake_instance": "GIT_SHA",
        "fake_cluster2.fake_instance2": "GIT_SHA",
    }
    mock_validate_service_instance.return_value = "adhoc"
    mock_adhoc_instance_status.return_value = {}

    request = testing.DummyRequest()
    request.swagger_data = {"service": "fake_service", "instance": "fake_instance"}

    response = instance.instance_status(request)
    assert mock_adhoc_instance_status.called
    assert response == {
        "service": "fake_service",
        "instance": "fake_instance",
        "git_sha": "GIT_SHA",
        "adhoc": {},
    }


def test_add_executor_info():
    mock_mesos_task = mock.Mock()
    mock_executor = {
        "tasks": [mock_mesos_task],
        "some": "thing",
        "completed_tasks": [mock_mesos_task],
        "queued_tasks": [mock_mesos_task],
    }
    mock_task = mock.Mock(
        _Task__items={"a": "thing"},
        executor=asynctest.CoroutineMock(
            return_value=mock_executor,
            func=asynctest.CoroutineMock(),  # https://github.com/notion/a_sync/pull/40
        ),
    )
    ret = instance.add_executor_info(mock_task)
    expected = {"a": "thing", "executor": {"some": "thing"}}
    assert ret._Task__items == expected
    with pytest.raises(KeyError):
        ret._Task__items["executor"]["completed_tasks"]
    with pytest.raises(KeyError):
        ret._Task__items["executor"]["tasks"]
    with pytest.raises(KeyError):
        ret._Task__items["executor"]["queued_tasks"]


def test_add_slave_info():
    mock_slave = asynctest.CoroutineMock(
        return_value=mock.Mock(_MesosSlave__items={"some": "thing"}),
        func=asynctest.CoroutineMock(),  # https://github.com/notion/a_sync/pull/40
    )
    mock_task = mock.Mock(_Task__items={"a": "thing"}, slave=mock_slave)
    expected = {"a": "thing", "slave": {"some": "thing"}}
    assert instance.add_slave_info(mock_task)._Task__items == expected


@mock.patch(
    "paasta_tools.api.views.instance.tron_tools.get_tron_dashboard_for_cluster",
    autospec=True,
)
@mock.patch("paasta_tools.api.views.instance.tron_tools.TronClient", autospec=True)
@mock.patch("paasta_tools.api.views.instance.tron_tools.get_tron_client", autospec=True)
@mock.patch("paasta_tools.api.views.instance.validate_service_instance", autospec=True)
def test_tron_instance_status(
    mock_validate_service_instance,
    mock_get_tron_client,
    mock_tron_client,
    mock_get_tron_dashboard_for_cluster,
):
    settings.cluster = "fake_cluster"
    mock_validate_service_instance.return_value = "tron"
    mock_client = mock_tron_client("fake_url")
    mock_get_tron_client.return_value = mock_client
    mock_client.get_job_content.return_value = {
        "status": "fake_status",
        "scheduler": {"type": "daily", "value": "1 2 3"},
    }
    mock_client.get_action_run.return_value = {
        "state": "fake_state",
        "start_time": "fake_start_time",
        "raw_command": "fake_raw_command",
        "command": "fake_command",
        "stdout": ["fake_stdout"],
        "stderr": ["fake_stderr"],
    }
    mock_get_tron_dashboard_for_cluster.return_value = "http://fake_url/"

    request = testing.DummyRequest()
    request.swagger_data = {
        "service": "fake_service",
        "instance": "fake_job.fake_action",
    }
    response = instance.instance_status(request)
    assert response["tron"]["job_name"] == "fake_job"
    assert response["tron"]["job_status"] == "fake_status"
    assert response["tron"]["job_schedule"] == "daily 1 2 3"
    assert response["tron"]["job_url"] == "http://fake_url/#job/fake_service.fake_job"
    assert response["tron"]["action_name"] == "fake_action"
    assert response["tron"]["action_state"] == "fake_state"
    assert response["tron"]["action_raw_command"] == "fake_raw_command"
    assert response["tron"]["action_command"] == "fake_command"
    assert response["tron"]["action_start_time"] == "fake_start_time"
    assert response["tron"]["action_stdout"] == "fake_stdout"
    assert response["tron"]["action_stderr"] == "fake_stderr"


@mock.patch("paasta_tools.kubernetes_tools.get_kubernetes_app_by_name", autospec=True)
@mock.patch("paasta_tools.instance.kubernetes.job_status", autospec=True)
@mock.patch("paasta_tools.kubernetes_tools.get_active_shas_for_service", autospec=True)
@mock.patch(
    "paasta_tools.kubernetes_tools.replicasets_for_service_instance", autospec=True
)
@mock.patch("paasta_tools.kubernetes_tools.pods_for_service_instance", autospec=True)
@mock.patch(
    "paasta_tools.instance.kubernetes.LONG_RUNNING_INSTANCE_TYPE_HANDLERS",
    autospec=True,
)
def test_kubernetes_instance_status_bounce_method(
    mock_long_running_instance_type_handlers,
    mock_pods_for_service_instance,
    mock_replicasets_for_service_instance,
    mock_get_active_shas_for_service,
    mock_kubernetes_job_status,
    mock_get_kubernetes_app_by_name,
):
    settings.kubernetes_client = True
    svc = "fake-svc"
    inst = "fake-inst"

    mock_job_config = mock.Mock()
    mock_long_running_instance_type_handlers.__getitem__ = mock.Mock(
        return_value=mock.Mock(loader=mock.Mock(return_value=mock_job_config))
    )
    mock_get_kubernetes_app_by_name.return_value = mock.Mock()

    actual = instance.pik.kubernetes_status(
        service=svc,
        instance=inst,
        instance_type="kubernetes",
        verbose=0,
        include_smartstack=False,
        include_envoy=False,
        settings=settings,
    )
    assert actual["bounce_method"] == mock_job_config.get_bounce_method()


@mock.patch("paasta_tools.instance.kubernetes.job_status", autospec=True)
@mock.patch("paasta_tools.kubernetes_tools.get_active_shas_for_service", autospec=True)
@mock.patch(
    "paasta_tools.kubernetes_tools.replicasets_for_service_instance", autospec=True
)
@mock.patch("paasta_tools.kubernetes_tools.pods_for_service_instance", autospec=True)
@mock.patch(
    "paasta_tools.instance.kubernetes.LONG_RUNNING_INSTANCE_TYPE_HANDLERS",
    autospec=True,
)
def test_kubernetes_instance_status_evicted_nodes(
    mock_long_running_instance_type_handlers,
    mock_pods_for_service_instance,
    mock_replicasets_for_service_instance,
    mock_get_active_shas_for_service,
    mock_kubernetes_job_status,
):
    mock_pod_1 = mock.Mock(status=mock.Mock(reason="Evicted"))
    mock_pod_2 = mock.Mock()
    mock_pods_for_service_instance.return_value = [mock_pod_1, mock_pod_2]

    mock_settings = mock.Mock(cluster="kubernetes")

    instance_status = instance.pik.kubernetes_status(
        service="fake-svc",
        instance="fake-inst",
        instance_type="kubernetes",
        verbose=0,
        include_smartstack=False,
        include_envoy=False,
        settings=mock_settings,
    )
    assert instance_status["evicted_count"] == 1


@mock.patch("paasta_tools.instance.kubernetes.kubernetes_mesh_status", autospec=True)
@mock.patch("paasta_tools.api.views.instance.validate_service_instance", autospec=True)
def test_instance_mesh_status(
    mock_validate_service_instance, mock_kubernetes_mesh_status,
):
    mock_validate_service_instance.return_value = "flink"
    mock_kubernetes_mesh_status.return_value = {
        "smartstack": "smtstk status",
        "envoy": "envoy status",
    }

    request = testing.DummyRequest()
    request.swagger_data = {
        "service": "fake_service",
        "instance": "fake_inst",
        "include_smartstack": False,
    }
    instance_mesh = instance.instance_mesh_status(request)

    assert instance_mesh == {
        "service": "fake_service",
        "instance": "fake_inst",
        "smartstack": "smtstk status",
        "envoy": "envoy status",
    }
    assert mock_kubernetes_mesh_status.call_args_list == [
        mock.call(
            service="fake_service",
            instance="fake_inst",
            instance_type="flink",
            settings=settings,
            include_smartstack=False,
            include_envoy=None,  # default of true in api specs
        ),
    ]


@mock.patch("paasta_tools.instance.kubernetes.kubernetes_mesh_status", autospec=True)
@mock.patch("paasta_tools.api.views.instance.validate_service_instance", autospec=True)
@pytest.mark.parametrize(
    "validate_side_eft,mesh_status_side_eft,expected_msg,expected_code",
    [
        (
            NoConfigurationForServiceError(),
            {"envoy": None},
            "No instance named 'fake_service.fake_inst' has been configured",
            404,
        ),
        (Exception(), {"envoy": None}, "Traceback", 500),
        ("flink", RuntimeError("runtimeerror"), "runtimeerror", 405),
        ("flink", Exception(), "Traceback", 500),
    ],
)
def test_instance_mesh_status_error(
    mock_validate_service_instance,
    mock_kubernetes_mesh_status,
    validate_side_eft,
    mesh_status_side_eft,
    expected_msg,
    expected_code,
):
    mock_validate_service_instance.side_effect = [validate_side_eft]
    mock_kubernetes_mesh_status.side_effect = [mesh_status_side_eft]

    request = testing.DummyRequest()
    request.swagger_data = {
        "service": "fake_service",
        "instance": "fake_inst",
        "include_smartstack": False,
    }

    with pytest.raises(ApiFailure) as excinfo:
        instance.instance_mesh_status(request)

    assert expected_msg in excinfo.value.msg
    assert expected_code == excinfo.value.err


@mock.patch("paasta_tools.api.views.instance.validate_service_instance", autospec=True)
@mock.patch("paasta_tools.api.views.instance.pik.bounce_status", autospec=True)
class TestBounceStatus:
    @pytest.fixture(autouse=True)
    def mock_settings(self):
        with mock.patch(
            "paasta_tools.api.views.instance.settings", autospec=True
        ) as _mock_settings:
            _mock_settings.cluster = "test_cluster"
            yield

    @pytest.fixture
    def mock_request(self):
        request = testing.DummyRequest()
        request.swagger_data = {
            "service": "test_service",
            "instance": "test_instance",
        }
        return request

    def test_success(
        self, mock_pik_bounce_status, mock_validate_service_instance, mock_request,
    ):
        mock_validate_service_instance.return_value = "kubernetes"
        response = instance.bounce_status(mock_request)
        assert response == mock_pik_bounce_status.return_value

    def test_not_found(
        self, mock_pik_bounce_status, mock_validate_service_instance, mock_request,
    ):
        mock_validate_service_instance.side_effect = NoConfigurationForServiceError
        with pytest.raises(ApiFailure) as excinfo:
            instance.bounce_status(mock_request)
        assert excinfo.value.err == 404

    def test_not_kubernetes(
        self, mock_pik_bounce_status, mock_validate_service_instance, mock_request,
    ):
        mock_validate_service_instance.return_value = "not_kubernetes"
        response = instance.bounce_status(mock_request)
        assert response.status_code == 204
