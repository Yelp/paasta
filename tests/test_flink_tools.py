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
from unittest import mock

import pytest

import paasta_tools.flink_tools as flink_tools
from paasta_tools.flink_tools import FlinkDeploymentConfig
from paasta_tools.flink_tools import FlinkDeploymentConfigDict


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


@mock.patch("requests.Response", autospec=True)
@mock.patch("paasta_tools.flink_tools.requests.get", autospec=True)
@mock.patch("paasta_tools.flink_tools.get_cr", autospec=True)
def test_curl_flink_endpoint_get_job_checkpoints(
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
        "counts": {
            "completed": 100,
            "failed": 2,
            "in_progress": 1,
            "restored": 0,
            "total": 103,
        },
        "summary": {"checkpointed_size": 12345},
        "latest": {"completed": {"id": 100}},
    }

    service = "kurupt"
    instance = "main"
    result = flink_tools.curl_flink_endpoint(
        flink_tools.cr_id(service, instance),
        "jobs/4210f0646f5c9ce1db0b3e5ae4372b82/checkpoints",
    )

    assert result == {
        "counts": {
            "completed": 100,
            "failed": 2,
            "in_progress": 1,
            "restored": 0,
            "total": 103,
        },
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


class TestGetFlinkPoolFromFlinkDeploymentConfig:
    def test_explicit_spot_false(self):
        # When spot is explicitly set to False, should return "flink"
        config_dict = FlinkDeploymentConfigDict({"spot": False})
        flink_deployment_config = FlinkDeploymentConfig(
            service="test_service",
            cluster="test_cluster",
            instance="test_instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        assert flink_deployment_config.get_pool() == "flink"

    def test_explicit_spot_true(self):
        # When spot is explicitly set to True, should return "flink-spot"
        config_dict = FlinkDeploymentConfigDict({"spot": True})
        flink_deployment_config = FlinkDeploymentConfig(
            service="test_service",
            cluster="test_cluster",
            instance="test_instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        assert flink_deployment_config.get_pool() == "flink-spot"

    def test_spot_not_set(self):
        # When spot is not set, should default to "flink-spot"
        config_dict = FlinkDeploymentConfigDict({"some_other_key": "value"})
        flink_deployment_config = FlinkDeploymentConfig(
            service="test_service",
            cluster="test_cluster",
            instance="test_instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        assert flink_deployment_config.get_pool() == "flink-spot"

    def test_empty_config_dict(self):
        # When config_dict is empty (but not None), should default to "flink-spot"
        config_dict = FlinkDeploymentConfigDict({})
        flink_deployment_config = FlinkDeploymentConfig(
            service="test_service",
            cluster="test_cluster",
            instance="test_instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        assert flink_deployment_config.get_pool() == "flink-spot"

    def test_non_bool_spot_value(self):
        # When spot has a non-boolean value like a string, it should treat it as non-False
        config_dict = FlinkDeploymentConfigDict({"spot": "some_string"})
        flink_deployment_config = FlinkDeploymentConfig(
            service="test_service",
            cluster="test_cluster",
            instance="test_instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        assert flink_deployment_config.get_pool() == "flink-spot"

        # Test with a numeric value
        config_dict = FlinkDeploymentConfigDict({"spot": 0})
        flink_deployment_config = FlinkDeploymentConfig(
            service="test_service",
            cluster="test_cluster",
            instance="test_instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        assert flink_deployment_config.get_pool() == "flink-spot"


@pytest.fixture
def flink_instance_config():
    cfg = mock.Mock(spec=FlinkDeploymentConfig)
    cfg.get_pool.return_value = "flink"
    cfg.get_team.return_value = "fake_owner"
    cfg.get_runbook.return_value = "fake_runbook_url"
    return cfg


class TestGetFlinkInstanceDetails:
    def test_running_cluster(self, flink_instance_config):
        metadata = {
            "labels": {"paasta.yelp.com/config_sha": "config00000"},
            "annotations": {
                "flink.yelp.com/dashboard_url": "http://flink.k8s.test.paasta:31080/app"
            },
        }
        flink_config = mock.Mock(
            flink_version="1.13.5",
            flink_revision="0ff28a7",
        )
        result = flink_tools.get_flink_instance_details(
            metadata, flink_config, flink_instance_config, "test_service"
        )
        assert result["config_sha"] == "00000"
        assert result["version"] == "1.13.5"
        assert result["version_revision"] == "0ff28a7"
        assert result["dashboard_url"] == "http://flink.k8s.test.paasta:31080/app"
        assert result["pool"] == "flink"
        assert result["team"] == "fake_owner"
        assert result["runbook"] == "fake_runbook_url"

    def test_stopped_cluster(self, flink_instance_config):
        metadata = {
            "labels": {"paasta.yelp.com/config_sha": "config00000"},
            "annotations": {},
        }
        result = flink_tools.get_flink_instance_details(
            metadata, None, flink_instance_config, "test_service"
        )
        assert result["version"] is None
        assert result["version_revision"] is None
        assert result["dashboard_url"] is None

    def test_missing_config_sha(self, flink_instance_config):
        metadata = {"labels": {}, "annotations": {}}
        with pytest.raises(ValueError, match="expected config sha"):
            flink_tools.get_flink_instance_details(
                metadata, None, flink_instance_config, "test_service"
            )

    def test_config_sha_without_prefix(self, flink_instance_config):
        # Labels without the "config" prefix should be used as-is
        metadata = {
            "labels": {"paasta.yelp.com/config_sha": "abcdef12"},
            "annotations": {},
        }
        result = flink_tools.get_flink_instance_details(
            metadata, None, flink_instance_config, "test_service"
        )
        assert result["config_sha"] == "abcdef12"


class TestFormatFlinkInstanceHeader:
    def test_non_verbose(self):
        details = {
            "config_sha": "00000",
            "version": "1.13.5",
            "version_revision": "0ff28a7",
            "dashboard_url": "http://flink.k8s.test.paasta:31080/app",
            "pool": "flink",
            "team": "test_team",
            "runbook": "test_runbook",
        }
        result = flink_tools.format_flink_instance_header(details, verbose=False)
        assert "    Config SHA: 00000" in result
        assert "    Flink version: 1.13.5" in result
        assert "0ff28a7" not in "\n".join(result)

    def test_verbose(self):
        details = {
            "config_sha": "00000",
            "version": "1.13.5",
            "version_revision": "0ff28a7",
            "dashboard_url": "http://flink.k8s.test.paasta:31080/app",
            "pool": "flink",
            "team": "test_team",
            "runbook": "test_runbook",
        }
        result = flink_tools.format_flink_instance_header(details, verbose=True)
        assert "    Flink version: 1.13.5 0ff28a7" in result
        assert "    URL: http://flink.k8s.test.paasta:31080/app/" in result

    def test_no_version_hides_url(self):
        # Dashboard URL should not be shown when cluster is not running (no version)
        details = {
            "config_sha": "00000",
            "version": None,
            "version_revision": None,
            "dashboard_url": "http://flink.k8s.test.paasta:31080/app",
            "pool": "flink",
            "team": "test_team",
            "runbook": "test_runbook",
        }
        result = flink_tools.format_flink_instance_header(details, verbose=True)
        assert len(result) == 1
        assert "Config SHA" in result[0]
        assert "URL" not in "\n".join(result)


class TestFormatFlinkInstanceMetadata:
    def test_output(self):
        details = {
            "config_sha": "00000",
            "version": "1.13.5",
            "version_revision": "0ff28a7",
            "dashboard_url": "http://test",
            "pool": "flink",
            "team": "test_team",
            "runbook": "test_runbook",
        }
        result = flink_tools.format_flink_instance_metadata(details, "test_service")
        assert (
            "    Repo(git): https://github.yelpcorp.com/services/test_service" in result
        )
        assert (
            "    Repo(sourcegraph): https://sourcegraph.yelpcorp.com/services/test_service"
            in result
        )
        assert "    Flink Pool: flink" in result
        assert "    Owner: test_team" in result
        assert "    Flink Runbook: test_runbook" in result


class TestFormatFlinkConfigLinks:
    def test_output(self):
        result = flink_tools.format_flink_config_links("my_service", "devc")
        assert (
            "    Yelpsoa configs: https://github.yelpcorp.com/sysgit/yelpsoa-configs/tree/master/my_service"
            in result
        )
        assert (
            "    Srv configs: https://github.yelpcorp.com/sysgit/srv-configs/tree/master/ecosystem/devc/my_service"
            in result
        )


class TestFormatFlinkLogCommands:
    def test_output(self):
        result = flink_tools.format_flink_log_commands("my_service", "main", "pnw-devc")
        assert result[0] == "    Flink Log Commands:"
        assert "paasta logs -a 1h -c pnw-devc -s my_service -i main" in result[1]
        assert ".TASKMANAGER" in result[2]
        assert ".JOBMANAGER" in result[3]
        assert ".SUPERVISOR" in result[4]


class TestFormatFlinkMonitoringLinks:
    def test_output(self):
        result = flink_tools.format_flink_monitoring_links(
            "my_service", "main", "devc", "pnw-devc"
        )
        assert result[0] == "    Flink Monitoring:"
        assert "var-service=my_service" in result[1]
        assert "var-instance=main" in result[1]
        assert "uswest2-devc" in result[1]
        assert "pnw-devc" in result[4]  # Flink Cost link uses cluster name
