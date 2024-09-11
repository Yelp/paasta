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
import pytest
from pyramid import testing

from paasta_tools.api.views import autoscaler
from paasta_tools.eks_tools import EksDeploymentConfig
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig


@mock.patch("paasta_tools.api.views.autoscaler.get_instance_config", autospec=True)
@pytest.mark.parametrize(
    "instance_type_class",
    (
        KubernetesDeploymentConfig,
        EksDeploymentConfig,
    ),
)
def test_get_autoscaler_count(mock_get_instance_config, instance_type_class):
    request = testing.DummyRequest()
    request.swagger_data = {"service": "fake_service", "instance": "fake_instance"}

    mock_get_instance_config.return_value = mock.MagicMock(
        get_instances=mock.MagicMock(return_value=123),
        spec=instance_type_class,
    )
    response = autoscaler.get_autoscaler_count(request)
    assert response.json_body["desired_instances"] == 123
    assert response.json_body["calculated_instances"] == 123


@mock.patch("paasta_tools.api.views.autoscaler.get_instance_config", autospec=True)
@pytest.mark.parametrize(
    "instance_type_class",
    (
        KubernetesDeploymentConfig,
        EksDeploymentConfig,
    ),
)
def test_update_autoscaler_count_kubernetes(
    mock_get_instance_config, instance_type_class
):
    request = testing.DummyRequest()
    request.swagger_data = {
        "service": "fake_kubernetes_service",
        "instance": "fake__kubernetes_instance",
        "json_body": {"desired_instances": 155},
    }

    mock_get_instance_config.return_value = mock.MagicMock(
        get_min_instances=mock.MagicMock(return_value=100),
        get_max_instances=mock.MagicMock(return_value=200),
        spec=instance_type_class,
    )

    response = autoscaler.update_autoscaler_count(request)
    assert response.json_body["desired_instances"] == 155
    assert response.status_code == 202


@mock.patch("paasta_tools.api.views.autoscaler.get_instance_config", autospec=True)
@pytest.mark.parametrize(
    "instance_type_class",
    (
        KubernetesDeploymentConfig,
        EksDeploymentConfig,
    ),
)
def test_update_autoscaler_count_warning(mock_get_instance_config, instance_type_class):
    request = testing.DummyRequest()
    request.swagger_data = {
        "service": "fake_service",
        "instance": "fake_instance",
        "json_body": {"desired_instances": 123},
    }

    mock_get_instance_config.return_value = mock.MagicMock(
        get_min_instances=mock.MagicMock(return_value=10),
        get_max_instances=mock.MagicMock(return_value=100),
        spec=instance_type_class,
    )

    response = autoscaler.update_autoscaler_count(request)
    assert response.json_body["desired_instances"] == 100
    assert "WARNING" in response.json_body["status"]
