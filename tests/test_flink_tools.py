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

import paasta_tools.flink_tools as flink_tools


def test_get_flink_ingress_url_root():
    assert (
        flink_tools.get_flink_ingress_url_root("mycluster", False)
        == "http://flink.k8s.mycluster.paasta:31080/"
    )


@mock.patch("requests.Response", autospec=True)
@mock.patch("paasta_tools.flink_tools.requests.get", autospec=True)
@mock.patch("paasta_tools.flink_tools.get_cr", autospec=True)
def test_curl_flink_endpoint_error(
    mock_get_cr,
    mock_requests_get,
    mock_response,
):
    mock_get_cr.return_value = {
        "metadata": {
            "labels": {"paasta.yelp.com/cluster": "mocked"},
            "annotations": {
                "flink.yelp.com/dashboard_url": "http://flink.k8s.test_cluster.paasta:31080/kurupt-7f5cfd8ffc"
            },
        }
    }
    mock_requests_get.return_value = mock_response
    mock_response.ok = False
    mock_response.status_code = 401
    mock_response.reason = "Unauthorized"
    mock_response.text = "401 Authorization Required"

    service = "kurupt"
    instance = "main"
    response = flink_tools.curl_flink_endpoint(
        flink_tools.cr_id(service, instance), "overview"
    )

    assert response == {
        "status": 401,
        "error": "Unauthorized",
        "text": "401 Authorization Required",
    }


@mock.patch("requests.Response", autospec=True)
@mock.patch("paasta_tools.flink_tools.requests.get", autospec=True)
@mock.patch("paasta_tools.flink_tools.get_cr", autospec=True)
def test_curl_flink_endpoint_overview(
    mock_get_cr,
    mock_requests_get,
    mock_response,
):
    mock_get_cr.return_value = {
        "metadata": {
            "labels": {"paasta.yelp.com/cluster": "mocked"},
            "annotations": {
                "flink.yelp.com/dashboard_url": "http://flink.k8s.test_cluster.paasta:31080/kurupt-7f5cfd8ffc"
            },
        }
    }
    mock_requests_get.return_value = mock_response
    mock_response.json.return_value = {
        "taskmanagers": 5,
        "slots-total": 25,
        "slots-available": 3,
        "jobs-running": 1,
        "jobs-finished": 0,
        "jobs-cancelled": 0,
        "jobs-failed": 0,
    }

    service = "kurupt"
    instance = "main"
    overview = flink_tools.curl_flink_endpoint(
        flink_tools.cr_id(service, instance), "overview"
    )

    assert overview == {
        "taskmanagers": 5,
        "slots-total": 25,
        "slots-available": 3,
        "jobs-running": 1,
        "jobs-finished": 0,
        "jobs-cancelled": 0,
        "jobs-failed": 0,
    }


@mock.patch("requests.Response", autospec=True)
@mock.patch("paasta_tools.flink_tools.requests.get", autospec=True)
@mock.patch("paasta_tools.flink_tools.get_cr", autospec=True)
def test_curl_flink_endpoint_config(
    mock_get_cr,
    mock_requests_get,
    mock_response,
):
    mock_get_cr.return_value = {
        "metadata": {
            "labels": {"paasta.yelp.com/cluster": "mocked"},
            "annotations": {
                "flink.yelp.com/dashboard_url": "http://flink.k8s.test_cluster.paasta:31080/kurupt-7f5cfd8ffc"
            },
        }
    }
    mock_requests_get.return_value = mock_response
    mock_response.json.return_value = {
        "refresh-interval": 3000,
        "timezone-name": "Coordinated Universal Time",
        "timezone-offset": 0,
        "flink-version": "1.13.5",
        "flink-revision": "0ff28a7 @ 2021-12-14T23:26:04+01:00",
        "features": {"web-submit": True},
    }

    service = "kurupt"
    instance = "main"
    config = flink_tools.curl_flink_endpoint(
        flink_tools.cr_id(service, instance), "config"
    )

    assert config == {
        "flink-version": "1.13.5",
        "flink-revision": "0ff28a7 @ 2021-12-14T23:26:04+01:00",
    }


@mock.patch("requests.Response", autospec=True)
@mock.patch("paasta_tools.flink_tools.requests.get", autospec=True)
@mock.patch("paasta_tools.flink_tools.get_cr", autospec=True)
def test_curl_flink_endpoint_list_jobs(
    mock_get_cr,
    mock_requests_get,
    mock_response,
):
    mock_get_cr.return_value = {
        "metadata": {
            "labels": {"paasta.yelp.com/cluster": "mocked"},
            "annotations": {
                "flink.yelp.com/dashboard_url": "http://flink.k8s.test_cluster.paasta:31080/kurupt-7f5cfd8ffc"
            },
        }
    }
    mock_requests_get.return_value = mock_response
    mock_response.json.return_value = {
        "jobs": [{"id": "4210f0646f5c9ce1db0b3e5ae4372b82", "status": "RUNNING"}]
    }

    service = "kurupt"
    instance = "main"
    jobs = flink_tools.curl_flink_endpoint(flink_tools.cr_id(service, instance), "jobs")

    assert jobs == {
        "jobs": [{"id": "4210f0646f5c9ce1db0b3e5ae4372b82", "status": "RUNNING"}]
    }


@mock.patch("requests.Response", autospec=True)
@mock.patch("paasta_tools.flink_tools.requests.get", autospec=True)
@mock.patch("paasta_tools.flink_tools.get_cr", autospec=True)
def test_curl_flink_endpoint_get_job_details(
    mock_get_cr,
    mock_requests_get,
    mock_response,
):
    mock_get_cr.return_value = {
        "metadata": {
            "labels": {"paasta.yelp.com/cluster": "mocked"},
            "annotations": {
                "flink.yelp.com/dashboard_url": "http://flink.k8s.test_cluster.paasta:31080/kurupt-7f5cfd8ffc"
            },
        }
    }
    mock_requests_get.return_value = mock_response
    mock_response.json.return_value = {
        "jid": "4210f0646f5c9ce1db0b3e5ae4372b82",
        "name": "beam_happyhour.main.test_job",
        "isStoppable": False,
        "state": "RUNNING",
        "start-time": 1655053223341,
        "end-time": -1,
        "duration": 855872054,
        "maxParallelism": -1,
        "now": 1655909095395,
        "timestamps": {
            "CREATED": 1655053223735,
            "SUSPENDED": 0,
            "FAILING": 0,
            "FINISHED": 0,
            "FAILED": 0,
            "RUNNING": 1655842396454,
            "CANCELLING": 0,
            "RESTARTING": 1655842393301,
            "RECONCILING": 0,
            "CANCELED": 0,
            "INITIALIZING": 1655053223341,
        },
    }

    service = "kurupt"
    instance = "main"
    job = flink_tools.curl_flink_endpoint(
        flink_tools.cr_id(service, instance), "jobs/4210f0646f5c9ce1db0b3e5ae4372b82"
    )

    assert job == {
        "jid": "4210f0646f5c9ce1db0b3e5ae4372b82",
        "state": "RUNNING",
        "name": "beam_happyhour.main.test_job",
        "start-time": 1655053223341,
    }


def test_get_flink_jobmanager_overview():
    with mock.patch(
        "paasta_tools.flink_tools._dashboard_get",
        autospec=True,
        return_value='{"taskmanagers":10,"slots-total":10,"flink-version":"1.6.4","flink-commit":"6241481"}',
    ) as mock_dashboard_get:
        cluster = "mycluster"
        cr_name = "kurupt--fm-7c7b459d59"
        overview = flink_tools.get_flink_jobmanager_overview(cr_name, cluster, False)
        mock_dashboard_get.assert_called_once_with(
            cr_name=cr_name, cluster=cluster, path="overview", is_eks=False
        )
        assert overview == {
            "taskmanagers": 10,
            "slots-total": 10,
            "flink-version": "1.6.4",
            "flink-commit": "6241481",
        }
