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
from unittest import mock

import pytest
from pyramid import testing

from paasta_tools.api.views.exception import ApiFailure
from paasta_tools.api.views.service import get_deployment_info
from paasta_tools.api.views.service import list_instances
from paasta_tools.api.views.service import list_services_for_cluster
from paasta_tools.utils import DeploymentsJsonV2
from paasta_tools.utils import NoDeploymentsAvailable


@mock.patch(
    "paasta_tools.api.views.service.list_all_instances_for_service", autospec=True
)
def test_list_instances(
    mock_list_all_instances_for_service,
):
    fake_instances = ["fake_instance_a", "fake_instance_b", "fake_instance_c"]
    mock_list_all_instances_for_service.return_value = fake_instances

    request = testing.DummyRequest()
    request.swagger_data = {"service": "fake_service"}

    response = list_instances(request)
    assert response["instances"] == fake_instances


@mock.patch("paasta_tools.api.views.service.get_services_for_cluster", autospec=True)
def test_list_services_for_cluster(
    mock_get_services_for_cluster,
):
    fake_services_and_instances = [
        ("fake_service", "fake_instance_a"),
        ("fake_service", "fake_instance_b"),
        ("fake_service", "fake_instance_c"),
    ]
    mock_get_services_for_cluster.return_value = fake_services_and_instances

    request = testing.DummyRequest()

    response = list_services_for_cluster(request)
    assert response["services"] == [
        ("fake_service", "fake_instance_a"),
        ("fake_service", "fake_instance_b"),
        ("fake_service", "fake_instance_c"),
    ]


def test_get_deployment_info_success_with_image_version():
    """Test successful deployment info retrieval with image_version."""
    with (
        mock.patch(
            "paasta_tools.api.views.service.load_v2_deployments_json", autospec=True
        ) as mock_load_v2_deployments_json,
        mock.patch(
            "paasta_tools.api.views.service.get_service_docker_registry", autospec=True
        ) as mock_get_service_docker_registry,
    ):
        mock_deployments = mock.Mock(spec=DeploymentsJsonV2)
        mock_deployments.config_dict = {
            "deployments": {
                "prod.main": {
                    "docker_image": "services-fake_service:paasta-abc123",
                    "git_sha": "abc123def456",
                    "image_version": "extrastuff",
                }
            }
        }
        mock_load_v2_deployments_json.return_value = mock_deployments
        mock_get_service_docker_registry.return_value = "docker-registry.example.com"

        request = testing.DummyRequest()
        request.swagger_data = {"service": "fake_service", "deploy_group": "prod.main"}

        response = get_deployment_info(request)
        assert response.status_code == 200
        assert response.json_body == {
            "docker_image": "services-fake_service:paasta-abc123",
            "git_sha": "abc123def456",
            "image_version": "extrastuff",
            "image_url": "docker-registry.example.com/services-fake_service:paasta-abc123",
        }


def test_get_deployment_info_success_without_image_version():
    """Test successful deployment info retrieval when image_version is None."""
    with (
        mock.patch(
            "paasta_tools.api.views.service.load_v2_deployments_json", autospec=True
        ) as mock_load_v2_deployments_json,
        mock.patch(
            "paasta_tools.api.views.service.get_service_docker_registry", autospec=True
        ) as mock_get_service_docker_registry,
    ):
        mock_deployments = mock.Mock(spec=DeploymentsJsonV2)
        mock_deployments.config_dict = {
            "deployments": {
                "prod.main": {
                    "docker_image": "services-fake_service:paasta-abc123",
                    "git_sha": "abc123def456",
                    # No image_version field
                }
            }
        }
        mock_load_v2_deployments_json.return_value = mock_deployments
        mock_get_service_docker_registry.return_value = "docker-registry.example.com"

        request = testing.DummyRequest()
        request.swagger_data = {"service": "fake_service", "deploy_group": "prod.main"}

        response = get_deployment_info(request)
        assert response.status_code == 200
        # image_version should be None when not present
        assert response.json_body == {
            "docker_image": "services-fake_service:paasta-abc123",
            "git_sha": "abc123def456",
            "image_version": None,
            "image_url": "docker-registry.example.com/services-fake_service:paasta-abc123",
        }


def test_get_deployment_info_no_deployments_json():
    """Test 404 when deployments.json doesn't exist."""
    with mock.patch(
        "paasta_tools.api.views.service.load_v2_deployments_json", autospec=True
    ) as mock_load_v2_deployments_json:
        mock_load_v2_deployments_json.side_effect = NoDeploymentsAvailable(
            "fake_service has no deployments"
        )

        request = testing.DummyRequest()
        request.swagger_data = {"service": "fake_service", "deploy_group": "prod.main"}

        with pytest.raises(ApiFailure) as exc_info:
            get_deployment_info(request)
        assert exc_info.value.msg == "fake_service has no deployments"
        assert exc_info.value.err == 404


def test_get_deployment_info_not_deployed_to_deploy_group():
    """Test 404 when service is not deployed to the specified deploy_group."""
    with mock.patch(
        "paasta_tools.api.views.service.load_v2_deployments_json", autospec=True
    ) as mock_load_v2_deployments_json:
        mock_deployments = mock.Mock(spec=DeploymentsJsonV2)
        mock_deployments.config_dict = {
            "deployments": {
                "prod.other": {
                    "docker_image": "services-fake_service:paasta-abc123",
                    "git_sha": "abc123def456",
                }
            }
        }
        mock_load_v2_deployments_json.return_value = mock_deployments

        request = testing.DummyRequest()
        request.swagger_data = {"service": "fake_service", "deploy_group": "prod.main"}

        with pytest.raises(ApiFailure) as exc_info:
            get_deployment_info(request)
        assert exc_info.value.msg == "fake_service not deployed to prod.main"
        assert exc_info.value.err == 404


def test_get_deployment_info_missing_docker_image_field():
    """Test 500 when docker_image field is missing from config."""
    with (
        mock.patch(
            "paasta_tools.api.views.service.load_v2_deployments_json", autospec=True
        ) as mock_load_v2_deployments_json,
        mock.patch(
            "paasta_tools.api.views.service.get_service_docker_registry", autospec=True
        ) as mock_get_service_docker_registry,
    ):
        mock_deployments = mock.Mock(spec=DeploymentsJsonV2)
        mock_deployments.config_dict = {
            "deployments": {
                "prod.main": {
                    "git_sha": "abc123def456",
                    # Missing docker_image field
                }
            }
        }
        mock_load_v2_deployments_json.return_value = mock_deployments
        mock_get_service_docker_registry.return_value = "docker-registry.example.com"

        request = testing.DummyRequest()
        request.swagger_data = {"service": "fake_service", "deploy_group": "prod.main"}

        with pytest.raises(ApiFailure) as exc_info:
            get_deployment_info(request)
        assert exc_info.value.msg == "'docker_image'"
        assert exc_info.value.err == 500
