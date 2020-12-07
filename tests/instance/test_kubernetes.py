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
import mock
import pytest

import paasta_tools.instance.kubernetes as pik
from paasta_tools.util.const import INSTANCE_TYPES


def test_instance_types_integrity():
    for it in pik.INSTANCE_TYPES:
        assert it in INSTANCE_TYPES
    for it in pik.INSTANCE_TYPES_WITH_SET_STATE:
        assert it in INSTANCE_TYPES


def instance_status_kwargs():
    return dict(
        service="",
        instance="",
        instance_type="",
        verbose=0,
        include_smartstack=False,
        include_envoy=False,
        settings=mock.Mock(),
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


@mock.patch("paasta_tools.instance.kubernetes.job_status", autospec=True)
@mock.patch(
    "paasta_tools.kubernetes_tools.replicasets_for_service_instance", autospec=True
)
@mock.patch("paasta_tools.kubernetes_tools.pods_for_service_instance", autospec=True)
@mock.patch("paasta_tools.kubernetes_tools.get_kubernetes_app_by_name", autospec=True)
@mock.patch(
    "paasta_tools.instance.kubernetes.LONG_RUNNING_INSTANCE_TYPE_HANDLERS",
    autospec=True,
)
def test_kubernetes_status(
    mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS,
    mock_get_kubernetes_app_by_name,
    mock_pods_for_service_instance,
    mock_replicasets_for_service_instance,
    mock_job_status,
):
    mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS["flink"] = mock.Mock()
    mock_pods_for_service_instance.return_value = []
    mock_replicasets_for_service_instance.return_value = []
    status = pik.kubernetes_status(
        service="",
        instance="",
        verbose=0,
        include_smartstack=False,
        include_envoy=False,
        instance_type="flink",
        settings=mock.Mock(),
    )
    assert "app_count" in status
    assert "evicted_count" in status
    assert "bounce_method" in status
    assert "desired_state" in status


@mock.patch("paasta_tools.instance.kubernetes.job_status", autospec=True)
@mock.patch(
    "paasta_tools.kubernetes_tools.load_service_namespace_config", autospec=True
)
@mock.patch("paasta_tools.instance.kubernetes.mesh_status", autospec=True)
@mock.patch(
    "paasta_tools.kubernetes_tools.replicasets_for_service_instance", autospec=True
)
@mock.patch("paasta_tools.kubernetes_tools.pods_for_service_instance", autospec=True)
@mock.patch("paasta_tools.kubernetes_tools.get_kubernetes_app_by_name", autospec=True)
@mock.patch(
    "paasta_tools.instance.kubernetes.LONG_RUNNING_INSTANCE_TYPE_HANDLERS",
    autospec=True,
)
def test_kubernetes_status_include_smartstack(
    mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS,
    mock_get_kubernetes_app_by_name,
    mock_pods_for_service_instance,
    mock_replicasets_for_service_instance,
    mock_mesh_status,
    mock_load_service_namespace_config,
    mock_job_status,
):
    mock_load_service_namespace_config.return_value = {"proxy_port": 1234}
    mock_LONG_RUNNING_INSTANCE_TYPE_HANDLERS["flink"] = mock.Mock()
    mock_pods_for_service_instance.return_value = []
    mock_replicasets_for_service_instance.return_value = []
    mock_service = mock.Mock()
    status = pik.kubernetes_status(
        service=mock_service,
        instance="",
        verbose=0,
        include_smartstack=True,
        include_envoy=False,
        instance_type="flink",
        settings=mock.Mock(),
    )
    assert (
        mock_load_service_namespace_config.mock_calls[0][2]["service"] is mock_service
    )
    assert mock_mesh_status.mock_calls[0][2]["service"] is mock_service
    assert "app_count" in status
    assert "evicted_count" in status
    assert "bounce_method" in status
    assert "desired_state" in status


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
