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
from paasta_tools.api.views.exception import ApiFailure


@mock.patch("paasta_tools.api.views.flink.curl_flink_endpoint", autospec=True)
class TestGetFlinkClusterOverview:
    @pytest.fixture
    def mock_request(self):
        request = testing.DummyRequest()
        request.swagger_data = {
            "service": "test_service",
            "instance": "test_instance",
        }
        return request

    def test_success(
        self,
        mock_curl_flink_endpoint,
        mock_request,
    ):
        jm_response = {
            "taskmanagers": 5,
            "slots-total": 25,
            "slots-available": 3,
            "jobs-running": 1,
            "jobs-finished": 0,
            "jobs-cancelled": 0,
            "jobs-failed": 0,
        }
        mock_curl_flink_endpoint.return_value = jm_response
        response = flink.get_flink_cluster_overview(mock_request)
        assert response == jm_response

    def test_failure(
        self,
        mock_curl_flink_endpoint,
        mock_request,
    ):
        mock_curl_flink_endpoint.side_effect = ValueError("BOOM")
        with pytest.raises(ApiFailure):
            _ = flink.get_flink_cluster_overview(mock_request)


@mock.patch("paasta_tools.api.views.flink.curl_flink_endpoint", autospec=True)
class TestGetFlinkClusterConfig:
    @pytest.fixture
    def mock_request(self):
        request = testing.DummyRequest()
        request.swagger_data = {
            "service": "test_service",
            "instance": "test_instance",
        }
        return request

    def test_success(
        self,
        mock_curl_flink_endpoint,
        mock_request,
    ):
        jm_response = {
            "flink-version": "1.13.5",
            "flink-revision": "0ff28a7 @ 2021-12-14T23:26:04+01:00",
        }
        mock_curl_flink_endpoint.return_value = jm_response
        response = flink.get_flink_cluster_config(mock_request)
        assert response == jm_response

    def test_failure(
        self,
        mock_curl_flink_endpoint,
        mock_request,
    ):
        mock_curl_flink_endpoint.side_effect = ValueError("BOOM")
        with pytest.raises(ApiFailure):
            _ = flink.get_flink_cluster_config(mock_request)


@mock.patch("paasta_tools.api.views.flink.curl_flink_endpoint", autospec=True)
class TestListFlinkClusterJobs:
    @pytest.fixture
    def mock_request(self):
        request = testing.DummyRequest()
        request.swagger_data = {
            "service": "test_service",
            "instance": "test_instance",
        }
        return request

    def test_success(
        self,
        mock_curl_flink_endpoint,
        mock_request,
    ):
        jm_response = {
            "jobs": [{"id": "4210f0646f5c9ce1db0b3e5ae4372b82", "status": "RUNNING"}]
        }
        mock_curl_flink_endpoint.return_value = jm_response
        response = flink.list_flink_cluster_jobs(mock_request)
        assert response == jm_response

    def test_failure(
        self,
        mock_curl_flink_endpoint,
        mock_request,
    ):
        mock_curl_flink_endpoint.side_effect = ValueError("BOOM")
        with pytest.raises(ApiFailure):
            _ = flink.list_flink_cluster_jobs(mock_request)


@mock.patch("paasta_tools.api.views.flink.curl_flink_endpoint", autospec=True)
class TestGetFlinkJobDetails:
    @pytest.fixture
    def mock_request(self):
        request = testing.DummyRequest()
        request.swagger_data = {
            "service": "test_service",
            "instance": "test_instance",
            "job_id": "4210f0646f5c9ce1db0b3e5ae4372b82",
        }
        return request

    def test_success(
        self,
        mock_curl_flink_endpoint,
        mock_request,
    ):
        jm_response = {
            "jid": "4210f0646f5c9ce1db0b3e5ae4372b82",
            "name": "beam_happyhour.main.test_job",
            "start-time": 1655053223341,
        }
        mock_curl_flink_endpoint.return_value = jm_response
        api_response = flink.get_flink_cluster_job_details(mock_request)
        assert jm_response == api_response

    def test_failure(
        self,
        mock_curl_flink_endpoint,
        mock_request,
    ):
        mock_curl_flink_endpoint.side_effect = ValueError("BOOM")
        with pytest.raises(ApiFailure):
            _ = flink.get_flink_cluster_job_details(mock_request)
