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
from kubernetes.client.rest import ApiException as KubeApiException

import paasta_tools.flink_tools as flink_tools
from paasta_tools.flink_tools import FlinkDeploymentConfig
from paasta_tools.flink_tools import FlinkDeploymentConfigDict
from paasta_tools.utils import PaastaColors


def test_get_flink_ingress_url_root():
    assert (
        flink_tools.get_flink_ingress_url_root("mycluster")
        == "http://flink.eks.mycluster.paasta:31080/"
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


@mock.patch("paasta_tools.flink_tools.get_cr", autospec=True)
def test_curl_flink_endpoint_kube_api_exception(mock_get_cr):
    mock_get_cr.side_effect = KubeApiException(status=503, reason="Service Unavailable")

    with pytest.raises(ValueError, match="failed HTTP request to flink API"):
        flink_tools.curl_flink_endpoint(flink_tools.cr_id("kurupt", "main"), "config")


@mock.patch("paasta_tools.flink_tools.get_cr", autospec=True)
def test_curl_flink_endpoint_missing_annotation(mock_get_cr):
    mock_get_cr.return_value = {
        "metadata": {
            "labels": {"paasta.yelp.com/cluster": "mocked"},
            "annotations": {},
        }
    }

    with pytest.raises(ValueError, match="missing expected field on Flink CR"):
        flink_tools.curl_flink_endpoint(flink_tools.cr_id("kurupt", "main"), "config")


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

    @pytest.mark.parametrize(
        "config_dict,expected_pool",
        [
            ({"pool": "flink-spot-ebs", "spot": True}, "flink-spot-ebs"),
            ({"pool": "flink-spot-ebs", "spot": False}, "flink-spot-ebs"),
            ({"pool": "flink-spot-ebs"}, "flink-spot-ebs"),
        ],
    )
    def test_explicit_pool_takes_precedence(self, config_dict, expected_pool):
        flink_deployment_config = FlinkDeploymentConfig(
            service="test_service",
            cluster="test_cluster",
            instance="test_instance",
            config_dict=FlinkDeploymentConfigDict(config_dict),
            branch_dict=None,
        )
        assert flink_deployment_config.get_pool() == expected_pool


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

    def test_missing_config_sha(self, flink_instance_config):
        flink_config = mock.Mock(flink_version="1.13.5", flink_revision="0ff28a7")
        metadata = {"labels": {}, "annotations": {}}
        with pytest.raises(ValueError, match="expected config sha"):
            flink_tools.get_flink_instance_details(
                metadata, flink_config, flink_instance_config, "test_service"
            )

    def test_config_sha_without_prefix(self, flink_instance_config):
        # Labels without the "config" prefix should be used as-is
        flink_config = mock.Mock(flink_version="1.13.5", flink_revision="0ff28a7")
        metadata = {
            "labels": {"paasta.yelp.com/config_sha": "abcdef12"},
            "annotations": {},
        }
        result = flink_tools.get_flink_instance_details(
            metadata, flink_config, flink_instance_config, "test_service"
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
        assert "    Flink:      1.13.5" in result
        assert "0ff28a7" not in "\n".join(result)
        assert "Config SHA" not in "\n".join(result)

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
        assert "    Flink:      1.13.5 0ff28a7" in result
        assert "Dashboard" in "\n".join(result)
        assert "http://flink.k8s.test.paasta:31080/app/" in "\n".join(result)

    def test_no_dashboard_url(self):
        details = {
            "config_sha": "00000",
            "version": "1.13.5",
            "version_revision": "0ff28a7",
            "dashboard_url": None,
            "pool": "flink",
            "team": "test_team",
            "runbook": "test_runbook",
        }
        result = flink_tools.format_flink_instance_header(details, verbose=True)
        assert "Dashboard" not in "\n".join(result)


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
        result = flink_tools.format_flink_instance_metadata(
            details, "test_service", "devc"
        )
        joined = "\n".join(result)
        assert "    Links:" in result
        assert "y/service-sg/test_service" in joined
        assert "flink" in joined
        assert "test_team" in joined
        assert "test_runbook" in joined
        assert "y/service-yelpsoa/test_service" in joined
        assert "y/service-srv/devc/test_service" in joined


class TestFormatFlinkLogCommands:
    def test_output(self):
        result = flink_tools.format_flink_log_commands("my_service", "main", "pnw-devc")
        assert result[0] == "    Logs:"
        assert "paasta logs -a 1h -c pnw-devc -s my_service -i main" in result[1]
        assert ".TASKMANAGER" in result[2]
        assert ".JOBMANAGER" in result[3]
        assert ".SUPERVISOR" in result[4]


class TestFormatFlinkMonitoringLinks:
    def test_output(self):
        result = flink_tools.format_flink_monitoring_links(
            "my_service", "main", "devc", "pnw-devc"
        )
        assert result[0] == "    Monitoring:"
        joined = "\n".join(result)
        assert "var-service=my_service" in joined
        assert "var-instance=main" in joined
        assert "uswest2-devc" in joined
        assert "pnw-devc" in joined
        assert "y/flink-job-metrics" in joined
        assert "y/flink-cost-dashboard" in joined


class TestCollectFlinkJobDetails:
    def test_running_with_overview(self):
        status = {
            "state": "running",
            "pod_status": [
                {"phase": "Running"},
                {"phase": "Running"},
                {"phase": "Failed", "reason": "Evicted"},
            ],
        }
        overview = mock.Mock()
        overview.jobs_running = 1
        overview.jobs_finished = 0
        overview.jobs_failed = 0
        overview.jobs_cancelled = 0
        overview.taskmanagers = 2
        overview.slots_available = 3
        overview.slots_total = 8

        result = flink_tools.collect_flink_job_details(status, overview)

        assert result["state"] == "running"
        assert result["pod_counts"]["running"] == 2
        assert result["pod_counts"]["evicted"] == 1
        assert result["pod_counts"]["other"] == 0
        assert result["pod_counts"]["total"] == 3
        assert result["job_counts"] is not None
        assert result["job_counts"]["running"] == 1
        assert result["job_counts"]["total"] == 1
        assert result["taskmanagers"] == 2
        assert result["slots_available"] == 3
        assert result["slots_total"] == 8
        assert result["overview_available"] is True

    def test_running_crashlooping_jobmanager(self):
        # overview object returned but jobs_running is None (jobmanager not responding)
        status = {"state": "running", "pod_status": [{"phase": "Running"}]}
        overview = mock.Mock()
        overview.jobs_running = None

        result = flink_tools.collect_flink_job_details(status, overview)

        assert result["job_counts"] is None
        assert result["overview_available"] is True

    def test_not_running_no_overview(self):
        status = {"state": "stopped", "pod_status": [{"phase": "Running"}]}

        result = flink_tools.collect_flink_job_details(status, None)

        assert result["job_counts"] is None
        assert result["overview_available"] is False

    def test_defensive_pod_access(self):
        # Pods without phase/reason should not crash
        status = {
            "state": "running",
            "pod_status": [
                {},  # no phase
                {"phase": "Failed"},  # no reason
                {"phase": "Failed", "reason": "OOMKilled"},
            ],
        }
        result = flink_tools.collect_flink_job_details(status, None)

        assert result["pod_counts"]["other"] == 3
        assert result["pod_counts"]["evicted"] == 0


class TestFormatFlinkStateAndPods:
    def _make_details(self, **kwargs):
        defaults = {
            "state": "running",
            "pod_counts": {"running": 2, "evicted": 0, "other": 0, "total": 2},
            "job_counts": {
                "running": 1,
                "finished": 0,
                "failed": 0,
                "cancelled": 0,
                "total": 1,
            },
            "taskmanagers": 1,
            "slots_available": 2,
            "slots_total": 8,
            "jobs": [],
            "overview_available": True,
        }
        defaults.update(kwargs)
        return defaults

    def test_running_state(self):

        result = flink_tools.format_flink_state_and_pods(self._make_details())
        assert f"    State: {PaastaColors.green('Running')}" in result

    def test_stopped_state_is_yellow(self):

        result = flink_tools.format_flink_state_and_pods(
            self._make_details(state="stopped")
        )
        assert f"    State: {PaastaColors.yellow('Stopped')}" in result

    def test_pods_line(self):
        result = flink_tools.format_flink_state_and_pods(self._make_details())
        pods_line = next(line for line in result if "Pods:" in line)
        assert "2 running" in pods_line
        assert "0 evicted" in pods_line
        assert "2 total" in pods_line

    def test_evicted_pods_red(self):

        details = self._make_details(
            pod_counts={"running": 1, "evicted": 2, "other": 0, "total": 3}
        )
        result = flink_tools.format_flink_state_and_pods(details)
        pods_line = next(line for line in result if "Pods:" in line)
        assert PaastaColors.red("2") in pods_line

    def test_jobs_summary_line(self):
        result = flink_tools.format_flink_state_and_pods(self._make_details())
        jobs_line = next(line for line in result if "Jobs:" in line)
        assert "1 running" in jobs_line
        assert "1 total" in jobs_line

    def test_jobmanager_not_responding(self):
        details = self._make_details(job_counts=None, overview_available=True)
        result = flink_tools.format_flink_state_and_pods(details)
        assert any("jobmanager is not responding" in line for line in result)

    def test_no_jobs_line_when_not_running(self):
        details = self._make_details(
            state="stopped", job_counts=None, overview_available=False
        )
        result = flink_tools.format_flink_state_and_pods(details)
        assert not any("Jobs:" in line for line in result)

    def test_taskmanagers_slots_line(self):
        result = flink_tools.format_flink_state_and_pods(self._make_details())
        slots_line = next(line for line in result if "taskmanagers" in line)
        assert "1 taskmanagers" in slots_line
        assert "2/8 slots available" in slots_line

    def test_restart_desired_state_shown(self):

        result = flink_tools.format_flink_state_and_pods(
            self._make_details(desired_state="restart")
        )
        state_line = next(line for line in result if "State:" in line)
        assert "desired: restart" in state_line
        assert PaastaColors.yellow("desired: restart") in state_line

    def test_start_desired_state_not_shown(self):
        result = flink_tools.format_flink_state_and_pods(
            self._make_details(desired_state="start")
        )
        state_line = next(line for line in result if "State:" in line)
        assert "desired" not in state_line

    def test_stop_desired_state_shown(self):
        result = flink_tools.format_flink_state_and_pods(
            self._make_details(desired_state="stop")
        )
        state_line = next(line for line in result if "State:" in line)
        assert "desired: stop" in state_line


@mock.patch("paasta_tools.flink_tools.shutil.get_terminal_size")
class TestFormatFlinkJobsTable:
    def _make_job(self, fields):
        defaults = {
            "jid": "abc123",
            "name": "beam.main.test_job",
            "state": "RUNNING",
            "start_time": 1655053223341.0,
            "timestamps": {},
        }
        defaults.update(fields)
        return defaults

    def test_verbose_shows_job_id(self, mock_terminal_size):
        mock_terminal_size.return_value = mock.Mock(columns=120)
        job = self._make_job({"jid": "abc123def456"})
        result = flink_tools.format_flink_jobs_table([job], "http://dashboard", 2)
        output_text = "\n".join(result)
        assert "Job ID" in output_text
        assert "abc123def456" in output_text

    def test_non_verbose_limits_to_3_jobs(self, mock_terminal_size):
        mock_terminal_size.return_value = mock.Mock(columns=120)
        jobs = [
            self._make_job(
                {
                    "jid": f"id_job{i}",
                    "name": f"beam.main.job{i}",
                    "start_time": 1000000000000.0 + i * 1000000,
                }
            )
            for i in range(4)
        ]
        result = flink_tools.format_flink_jobs_table(jobs, "http://dashboard", 0)
        output_text = "\n".join(result)
        assert "Only showing 3" in output_text

    def test_verbose_shows_all_jobs(self, mock_terminal_size):
        mock_terminal_size.return_value = mock.Mock(columns=120)
        jobs = [
            self._make_job(
                {
                    "jid": f"id_job{i}",
                    "name": f"beam.main.job{i}",
                    "start_time": 1000000000000.0 + i * 1000000,
                }
            )
            for i in range(4)
        ]
        result = flink_tools.format_flink_jobs_table(jobs, "http://dashboard", 1)
        output_text = "\n".join(result)
        assert "Only showing" not in output_text
        assert "job3" in output_text

    def test_failed_job_state(self, mock_terminal_size):

        mock_terminal_size.return_value = mock.Mock(columns=120)
        job = self._make_job({"state": "FAILED"})
        result = flink_tools.format_flink_jobs_table([job], "http://dashboard", 0)
        output_text = "\n".join(result)
        assert PaastaColors.red("Failed") in output_text

    def test_checkpoint_and_restart_display(self, mock_terminal_size):
        mock_terminal_size.return_value = mock.Mock(columns=120)
        job = self._make_job({"timestamps": {"RESTARTING": 1655842393301.0}})
        ckpt = mock.Mock()
        ckpt.counts = {"completed": 100, "failed": 2, "in_progress": 1, "restored": 0}
        result = flink_tools.format_flink_jobs_table(
            [job], "http://dashboard", 2, checkpoint_data={"abc123": ckpt}
        )
        output_text = "\n".join(result)
        assert (
            "Checkpoints: 100 completed, 2 failed, 1 in progress, 0 restored"
            in output_text
        )
        assert "Last restart:" in output_text

    def test_no_dashboard_url(self, mock_terminal_size):
        mock_terminal_size.return_value = mock.Mock(columns=120)
        job = self._make_job({})
        result = flink_tools.format_flink_jobs_table([job], None, 2)
        output_text = "\n".join(result)
        assert "None" not in output_text

    def test_skips_jobs_missing_required_display_fields(self, mock_terminal_size):
        mock_terminal_size.return_value = mock.Mock(columns=120)
        empty_job = {}
        malformed_job = {"jid": "bad123", "name": "beam.main.bad_job"}
        valid_job = self._make_job({"jid": "good123", "name": "beam.main.good_job"})
        result = flink_tools.format_flink_jobs_table(
            [empty_job, malformed_job, valid_job], "http://dashboard", 0
        )
        output_text = "\n".join(result)
        rendered_job_lines = [line for line in result if "job" in line]
        assert len(rendered_job_lines) == 1
        assert "good_job" in output_text
        assert "bad_job" not in output_text


class TestFormatFlinkUdfInfo:
    def _make_config(self, extra_config: dict) -> FlinkDeploymentConfig:
        config_dict = FlinkDeploymentConfigDict(**extra_config)
        return FlinkDeploymentConfig(
            service="test_service",
            cluster="test_cluster",
            instance="test_instance",
            config_dict=config_dict,
            branch_dict=None,
            soa_dir="/dev/null",
        )

    def test_no_udf(self):
        config = self._make_config({})
        assert flink_tools.format_flink_udf_info(config) == []

    def test_with_udf(self):
        config = self._make_config(
            {"udf_plugin_name": "canary_noop", "udf_plugin_version": "1.0.1"}
        )
        result = flink_tools.format_flink_udf_info(config)
        assert result == ["    UDF plugin: canary_noop (version 1.0.1)"]

    def test_udf_name_only(self):
        config = self._make_config({"udf_plugin_name": "my_udf"})
        result = flink_tools.format_flink_udf_info(config)
        assert result == ["    UDF plugin: my_udf (version unknown)"]
