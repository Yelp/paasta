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
        flink_tools.get_flink_ingress_url_root("mycluster")
        == "http://flink.k8s.mycluster.paasta:31080/"
    )


def test_curl_flink_endpoint_overview():
    with mock.patch(
        "paasta_tools.flink_tools._dashboard_get",
        autospec=True,
        return_value="""
        {
            "taskmanagers": 5,
            "slots-total": 25,
            "slots-available": 3,
            "jobs-running": 1,
            "jobs-finished": 0,
            "jobs-cancelled": 0,
            "jobs-failed": 0,
            "flink-version": "1.13.5",
            "flink-commit": "0ff28a7"
        }
        """,
    ) as mock_dashboard_get:
        cluster = "test_cluster"
        cr_name = "kurupt--fm-7c7b459d59"
        overview = flink_tools.curl_flink_endpoint(cr_name, cluster, "overview")
        mock_dashboard_get.assert_called_once_with(
            cr_name=cr_name, cluster=cluster, path="overview"
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


def test_curl_flink_endpoint_config():
    with mock.patch(
        "paasta_tools.flink_tools._dashboard_get",
        autospec=True,
        return_value="""
            {
                "refresh-interval":3000,
                "timezone-name":"Coordinated Universal Time",
                "timezone-offset":0,
                "flink-version":"1.13.5",
                "flink-revision":"0ff28a7 @ 2021-12-14T23:26:04+01:00",
                "features": { "web-submit":true }
            }
        """,
    ) as mock_dashboard_get:
        cluster = "test_cluster"
        cr_name = "kurupt--fm-7c7b459d59"
        config = flink_tools.curl_flink_endpoint(cr_name, cluster, "config")
        mock_dashboard_get.assert_called_once_with(
            cr_name=cr_name, cluster=cluster, path="config"
        )

        assert config == {
            "flink-version": "1.13.5",
            "flink-revision": "0ff28a7 @ 2021-12-14T23:26:04+01:00",
        }


def test_curl_flink_endpoint_list_jobs():
    with mock.patch(
        "paasta_tools.flink_tools._dashboard_get",
        autospec=True,
        return_value="""
            {
                "jobs":
                    [
                        {
                            "id": "4210f0646f5c9ce1db0b3e5ae4372b82",
                            "status":"RUNNING"
                        }
                    ]
            }
        """,
    ) as mock_dashboard_get:
        cluster = "test_cluster"
        cr_name = "kurupt--fm-7c7b459d59"
        jobs = flink_tools.curl_flink_endpoint(cr_name, cluster, "jobs")
        mock_dashboard_get.assert_called_once_with(
            cr_name=cr_name, cluster=cluster, path="jobs"
        )

        assert jobs == {
            "jobs": [{"id": "4210f0646f5c9ce1db0b3e5ae4372b82", "status": "RUNNING"}]
        }


def test_curl_flink_endpoint_get_job_details():
    with mock.patch(
        "paasta_tools.flink_tools._dashboard_get",
        autospec=True,
        return_value="""
                        {
                            "jid": "4210f0646f5c9ce1db0b3e5ae4372b82",
                            "name": "beam_happyhour.main.test_job",
                            "isStoppable": false,
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
                                "INITIALIZING": 1655053223341
                            }
                        }
        """,
    ) as mock_dashboard_get:
        cluster = "test_cluster"
        cr_name = "kurupt--fm-7c7b459d59"
        job = flink_tools.curl_flink_endpoint(
            cr_name, cluster, "jobs/4210f0646f5c9ce1db0b3e5ae4372b82"
        )
        mock_dashboard_get.assert_called_once_with(
            cr_name=cr_name,
            cluster=cluster,
            path="jobs/4210f0646f5c9ce1db0b3e5ae4372b82",
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
        overview = flink_tools.get_flink_jobmanager_overview(cr_name, cluster)
        mock_dashboard_get.assert_called_once_with(
            cr_name=cr_name, cluster=cluster, path="overview"
        )
        assert overview == {
            "taskmanagers": 10,
            "slots-total": 10,
            "flink-version": "1.6.4",
            "flink-commit": "6241481",
        }
