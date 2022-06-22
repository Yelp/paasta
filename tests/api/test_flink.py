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
@mock.patch("paasta_tools.api.views.flink.get_flink_cluster_overview", autospec=True)
class TestGetFlinkClusterOverview:
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
        self,
        mock_get_flink_cluster_overview,
        mock_curl_flink_endpoint,
        mock_request,
    ):
        mock_curl_flink_endpoint.return_value = {
            "taskmanagers": 5,
            "slots-total": 25,
            "slots-available": 3,
            "jobs-running": 1,
            "jobs-finished": 0,
            "jobs-cancelled": 0,
            "jobs-failed": 0,
        }

        response = flink.get_flink_cluster_overview(mock_request)
        assert response == mock_get_flink_cluster_overview.return_value


@mock.patch("paasta_tools.flink_tools.curl_flink_endpoint", autospec=True)
@mock.patch("paasta_tools.api.views.flink.get_flink_cluster_config", autospec=True)
class TestGetFlinkClusterConfig:
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
        self,
        mock_get_flink_cluster_config,
        mock_curl_flink_endpoint,
        mock_request,
    ):
        mock_curl_flink_endpoint.return_value = {
            "flink-version": "1.13.5",
            "flink-revision": "0ff28a7 @ 2021-12-14T23:26:04+01:00",
        }

        response = flink.get_flink_cluster_config(mock_request)
        assert response == mock_get_flink_cluster_config.return_value


@mock.patch("paasta_tools.flink_tools.curl_flink_endpoint", autospec=True)
@mock.patch("paasta_tools.api.views.flink.list_flink_cluster_jobs", autospec=True)
class TestListFlinkClusterJobs:
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
        self,
        mock_list_cluster_jobs,
        mock_curl_flink_endpoint,
        mock_request,
    ):
        mock_curl_flink_endpoint.return_value = {
            "jobs": [{"id": "4210f0646f5c9ce1db0b3e5ae4372b82", "status": "RUNNING"}]
        }
        response = flink.list_flink_cluster_jobs(mock_request)
        assert response == mock_list_cluster_jobs.return_value


@mock.patch("paasta_tools.flink_tools.curl_flink_endpoint", autospec=True)
@mock.patch("paasta_tools.api.views.flink.get_flink_cluster_job_details", autospec=True)
class TestGetFlinkJobDetails:
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
            "job_id": "4210f0646f5c9ce1db0b3e5ae4372b82",
        }
        return request

    def test_success(
        self,
        mock_get_flink_cluster_job_details,
        mock_curl_flink_endpoint,
        mock_request,
    ):
        mock_curl_flink_endpoint.return_value = {
            "jid": "4210f0646f5c9ce1db0b3e5ae4372b82",
            "name": "beam_happyhour.main.test_job",
            "start-time": 1655053223341,
        }

        response = flink.get_flink_cluster_job_details(mock_request)
        assert response == mock_get_flink_cluster_job_details.return_value
