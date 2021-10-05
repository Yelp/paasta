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
import json

import mock

import paasta_tools.flink_tools as flink_tools


def test_get_flink_ingress_url_root():
    assert (
        flink_tools.get_flink_ingress_url_root("mycluster")
        == "http://flink.k8s.mycluster.paasta:31080/"
    )


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


def test_get_flink_job_exceptions():
    json_response = """
    {"root_exception": "java.util.concurrent.TimeoutException",
     "timestamp": 1629900637343,
      "all-exceptions": [
          {"exception": "java.util.concurrent.TimeoutException", "task": "Source", "location": "10.93.111.113:20841", "timestamp": 1629900637342}
          ],
       "truncated": false}"""
    with mock.patch(
        "paasta_tools.flink_tools._dashboard_get",
        autospec=True,
        return_value=json_response,
    ) as mock_dashboard_get:
        cluster = "mycluster"
        cr_name = "kurupt--fm-7c7b459d59"
        job_id = "b011e2db52306cbf93279c9e4067cd2f"
        exceptions = flink_tools.get_flink_job_exceptions(cr_name, cluster, job_id)
        path = f"/jobs/{job_id}/exceptions"
        mock_dashboard_get.assert_called_once_with(
            cr_name=cr_name, cluster=cluster, path=path
        )
        assert exceptions == json.loads(json_response)
