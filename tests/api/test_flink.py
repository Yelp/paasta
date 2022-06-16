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

from paasta_tools.api.views import flink


@mock.patch("paasta_tools.flink_tools.curl_flink_endpoint", autospec=True)
@mock.patch("paasta_tools.api.views.flink.get_cluster_overview", autospec=True)
class TestGetClusterOverview:
    @pytest.fixture(autouse=True)
    def mock_settings(self):
        with mock.patch(
            "paasta_tools.api.views.flink.settings", autospec=True
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
        self, mock_get_cluster_overview, mock_curl_flink_endpoint, mock_request,
    ):
        mock_curl_flink_endpoint.return_value = {
            "taskmanagers": 5,
            "slots-total": 25,
            "slots-available": 3,
            "jobs-running": 1,
            "jobs-finished": 0,
            "jobs-cancelled": 0,
            "jobs-failed": 0,
            "flink-version": "1.13.5",
            "flink-commit": "0ff28a7",
        }
        response = flink.get_cluster_overview(mock_request)
        assert response == mock_get_cluster_overview.return_value

    # def test_not_found(
    #     self, mock_pik_bounce_status, mock_validate_service_instance, mock_request,
    # ):
    #     mock_validate_service_instance.side_effect = NoConfigurationForServiceError
    #     with pytest.raises(ApiFailure) as excinfo:
    #         instance.bounce_status(mock_request)
    #     assert excinfo.value.err == 404
