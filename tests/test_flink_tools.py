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
from paasta_tools.paastaapi.model.flink_cluster_overview import FlinkClusterOverview


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


class TestCollectFlinkJobDetails:
    """Tests for collect_flink_job_details function."""

    def test_collect_with_no_overview(self):
        """Test collecting job details when cluster is not running."""
        status = {
            "state": "stopped",
            "pod_status": [
                {"phase": "Running"},
                {"phase": "Running"},
            ],
        }
        overview = None
        jobs = []

        result = flink_tools.collect_flink_job_details(status, overview, jobs)

        expected = {
            "state": "stopped",
            "pod_counts": {"running": 2, "evicted": 0, "other": 0, "total": 2},
            "job_counts": None,
            "taskmanagers": None,
            "slots_available": None,
            "slots_total": None,
            "jobs": [],
        }
        assert result == expected

    def test_collect_with_evicted_pods(self):
        """Test collecting pod details with evicted pods."""
        status = {
            "state": "running",
            "pod_status": [
                {"phase": "Running"},
                {"phase": "Failed", "reason": "Evicted"},
                {"phase": "Failed", "reason": "Evicted"},
                {"phase": "Pending"},
            ],
        }
        overview = mock.Mock(spec=FlinkClusterOverview)
        overview.jobs_running = 1
        overview.jobs_finished = 2
        overview.jobs_failed = 0
        overview.jobs_cancelled = 1
        overview.taskmanagers = 3
        overview.slots_available = 5
        overview.slots_total = 15

        result = flink_tools.collect_flink_job_details(status, overview, [])

        expected = {
            "state": "running",
            "pod_counts": {"running": 1, "evicted": 2, "other": 1, "total": 4},
            "job_counts": {
                "running": 1,
                "finished": 2,
                "failed": 0,
                "cancelled": 1,
                "total": 4,
            },
            "taskmanagers": 3,
            "slots_available": 5,
            "slots_total": 15,
            "jobs": [],
        }
        assert result == expected

    def test_collect_with_failed_pod_no_reason(self):
        """Test collecting pod details with Failed pod without reason field."""
        status = {
            "state": "running",
            "pod_status": [
                {"phase": "Running"},
                {"phase": "Failed"},  # No "reason" field
                {"phase": "Failed", "reason": "Evicted"},
            ],
        }
        overview = None

        result = flink_tools.collect_flink_job_details(status, overview, [])

        expected = {
            "state": "running",
            "pod_counts": {"running": 1, "evicted": 1, "other": 1, "total": 3},
            "job_counts": None,
            "taskmanagers": None,
            "slots_available": None,
            "slots_total": None,
            "jobs": [],
        }
        assert result == expected

    def test_collect_with_overview_and_jobs(self):
        """Test collecting complete job details with overview."""
        status = {
            "state": "running",
            "pod_status": [{"phase": "Running"}],
        }
        overview = mock.Mock(spec=FlinkClusterOverview)
        overview.jobs_running = 2
        overview.jobs_finished = 3
        overview.jobs_failed = 1
        overview.jobs_cancelled = 0
        overview.taskmanagers = 5
        overview.slots_available = 10
        overview.slots_total = 25

        mock_jobs = [{}, {}]  # Jobs are passed through without inspection

        result = flink_tools.collect_flink_job_details(status, overview, mock_jobs)

        expected = {
            "state": "running",
            "pod_counts": {"running": 1, "evicted": 0, "other": 0, "total": 1},
            "job_counts": {
                "running": 2,
                "finished": 3,
                "failed": 1,
                "cancelled": 0,
                "total": 6,
            },
            "taskmanagers": 5,
            "slots_available": 10,
            "slots_total": 25,
            "jobs": mock_jobs,
        }
        assert result == expected

    def test_collect_with_missing_pod_status_key(self):
        """Test collecting when pod_status key is entirely absent."""
        status = {"state": "starting"}  # No pod_status key

        result = flink_tools.collect_flink_job_details(status, None, [])

        expected = {
            "state": "starting",
            "pod_counts": {"running": 0, "evicted": 0, "other": 0, "total": 0},
            "job_counts": None,
            "taskmanagers": None,
            "slots_available": None,
            "slots_total": None,
            "jobs": [],
        }
        assert result == expected


class TestFormatFlinkStateAndPods:
    """Tests for format_flink_state_and_pods function."""

    def test_format_running_state_no_evictions(self):
        """Test formatting running state with no evicted pods."""
        job_details = {
            "state": "running",
            "pod_counts": {"running": 3, "evicted": 0, "other": 0, "total": 3},
            "job_counts": {
                "running": 1,
                "finished": 0,
                "failed": 0,
                "cancelled": 0,
                "total": 1,
            },
            "taskmanagers": 2,
            "slots_available": 5,
            "slots_total": 10,
            "jobs": [],
        }

        output = flink_tools.format_flink_state_and_pods(job_details)

        output_text = "\n".join(output)
        assert "State:" in output_text
        assert "Running" in output_text
        assert "3 running" in output_text
        assert "0 evicted" in output_text
        assert "3 total" in output_text
        assert "1 running" in output_text  # jobs
        assert "2 taskmanagers" in output_text
        assert "5/10 slots available" in output_text

    def test_format_stopped_state_with_evictions(self):
        """Test formatting stopped state with evicted pods."""
        job_details = {
            "state": "stopped",
            "pod_counts": {"running": 1, "evicted": 2, "other": 1, "total": 4},
            "job_counts": None,
            "taskmanagers": None,
            "slots_available": None,
            "slots_total": None,
            "jobs": [],
        }

        output = flink_tools.format_flink_state_and_pods(job_details)

        output_text = "\n".join(output)
        assert "State:" in output_text
        assert "Stopped" in output_text
        assert "1 running" in output_text
        assert "evicted" in output_text  # Will be colored red when > 0
        assert "1 other" in output_text
        assert "4 total" in output_text
        # Should not have job counts or taskmanager info
        assert "Jobs:" not in output_text
        assert "taskmanagers" not in output_text

    def test_format_with_job_counts_only(self):
        """Test formatting with job counts but no taskmanager info."""
        job_details = {
            "state": "running",
            "pod_counts": {"running": 2, "evicted": 0, "other": 0, "total": 2},
            "job_counts": {
                "running": 3,
                "finished": 5,
                "failed": 1,
                "cancelled": 2,
                "total": 11,
            },
            "taskmanagers": None,
            "slots_available": None,
            "slots_total": None,
            "jobs": [],
        }

        output = flink_tools.format_flink_state_and_pods(job_details)

        output_text = "\n".join(output)
        assert "3 running" in output_text
        assert "5 finished" in output_text
        assert "1 failed" in output_text
        assert "2 cancelled" in output_text
        assert "11 total" in output_text
        assert "taskmanagers" not in output_text


class TestGetFlinkJobName:
    """Tests for get_flink_job_name helper function."""

    def test_extract_simple_job_name(self):
        """Test extracting job name from simple format."""
        job = {"name": "service.instance.jobname"}
        assert flink_tools.get_flink_job_name(job) == "jobname"

    def test_extract_job_name_with_dots(self):
        """Test extracting job name that contains dots."""
        job = {"name": "service.instance.my.complex.job.name"}
        assert flink_tools.get_flink_job_name(job) == "my.complex.job.name"

    def test_extract_job_name_simple(self):
        """Test extracting job name when there's only one part after splitting."""
        job = {"name": "simplejob"}
        assert flink_tools.get_flink_job_name(job) == "simplejob"


class TestFormatFlinkJobsTable:
    """Tests for format_flink_jobs_table function."""

    @mock.patch("paasta_tools.flink_tools.shutil.get_terminal_size", autospec=True)
    def test_format_empty_jobs_list(self, mock_terminal_size):
        """Test formatting with no jobs."""
        mock_terminal_size.return_value = mock.Mock(columns=120)
        jobs = []
        dashboard_url = "http://dashboard.example.com"

        output = flink_tools.format_flink_jobs_table(jobs, dashboard_url, verbose=0)

        assert output[0] == "    Jobs:"
        assert len(output) == 2  # Header + column header

    @mock.patch("paasta_tools.flink_tools.shutil.get_terminal_size", autospec=True)
    def test_format_single_running_job(self, mock_terminal_size):
        """Test formatting a single running job."""
        mock_terminal_size.return_value = mock.Mock(columns=120)
        jobs = [
            {
                "jid": "abc123",
                "name": "service.instance.myjob",
                "state": "RUNNING",
                "start_time": 1700000000000,  # milliseconds
            }
        ]
        dashboard_url = "http://dashboard.example.com"

        output = flink_tools.format_flink_jobs_table(jobs, dashboard_url, verbose=0)

        output_text = "\n".join(output)
        assert "Jobs:" in output_text
        assert "myjob" in output_text
        assert "Running" in output_text

    @mock.patch("paasta_tools.flink_tools.shutil.get_terminal_size", autospec=True)
    def test_format_verbose_mode_shows_job_id(self, mock_terminal_size):
        """Test that verbose mode shows job ID and dashboard URL."""
        mock_terminal_size.return_value = mock.Mock(columns=150)
        jobs = [
            {
                "jid": "abc123def456",
                "name": "service.instance.testjob",
                "state": "RUNNING",
                "start_time": 1700000000000,
            }
        ]
        dashboard_url = "http://dashboard.example.com"

        output = flink_tools.format_flink_jobs_table(jobs, dashboard_url, verbose=2)

        output_text = "\n".join(output)
        assert "Job ID" in output_text
        assert "abc123def456" in output_text
        assert "dashboard.example.com" in output_text

    @mock.patch("paasta_tools.flink_tools.shutil.get_terminal_size", autospec=True)
    def test_format_limits_jobs_without_verbose(self, mock_terminal_size):
        """Test that non-verbose mode limits to 3 jobs."""
        mock_terminal_size.return_value = mock.Mock(columns=120)
        jobs = [
            {
                "jid": f"job{i}",
                "name": f"service.instance.job{i}",
                "state": "RUNNING",
                "start_time": 1700000000000 + i * 1000,
            }
            for i in range(5)
        ]
        dashboard_url = "http://dashboard.example.com"

        output = flink_tools.format_flink_jobs_table(jobs, dashboard_url, verbose=0)

        output_text = "\n".join(output)
        assert "Only showing 3 Flink jobs" in output_text

    @mock.patch("paasta_tools.flink_tools.shutil.get_terminal_size", autospec=True)
    def test_format_shows_all_jobs_in_verbose(self, mock_terminal_size):
        """Test that verbose mode shows all jobs."""
        mock_terminal_size.return_value = mock.Mock(columns=150)
        jobs = [
            {
                "jid": f"job{i}",
                "name": f"service.instance.job{i}",
                "state": "RUNNING",
                "start_time": 1700000000000 + i * 1000,
            }
            for i in range(5)
        ]
        dashboard_url = "http://dashboard.example.com"

        output = flink_tools.format_flink_jobs_table(jobs, dashboard_url, verbose=1)

        output_text = "\n".join(output)
        assert "job0" in output_text
        assert "job1" in output_text
        assert "job2" in output_text
        assert "job3" in output_text
        assert "job4" in output_text
        assert "Only showing" not in output_text

    @mock.patch("paasta_tools.flink_tools.shutil.get_terminal_size", autospec=True)
    def test_format_failed_job_state(self, mock_terminal_size):
        """Test formatting a job with FAILED state."""
        mock_terminal_size.return_value = mock.Mock(columns=120)
        jobs = [
            {
                "jid": "failed123",
                "name": "service.instance.failedjob",
                "state": "FAILED",
                "start_time": 1700000000000,
            }
        ]
        dashboard_url = "http://dashboard.example.com"

        output = flink_tools.format_flink_jobs_table(jobs, dashboard_url, verbose=0)

        output_text = "\n".join(output)
        assert "failedjob" in output_text
        assert "Failed" in output_text

    @mock.patch("paasta_tools.flink_tools.shutil.get_terminal_size", autospec=True)
    def test_format_failing_job_state(self, mock_terminal_size):
        """Test formatting a job with FAILING state."""
        mock_terminal_size.return_value = mock.Mock(columns=120)
        jobs = [
            {
                "jid": "failing456",
                "name": "service.instance.failingjob",
                "state": "FAILING",
                "start_time": 1700000000000,
            }
        ]
        dashboard_url = "http://dashboard.example.com"

        output = flink_tools.format_flink_jobs_table(jobs, dashboard_url, verbose=0)

        output_text = "\n".join(output)
        assert "failingjob" in output_text
        assert "Failing" in output_text


class TestGetFlinkInstanceDetails:
    """Tests for get_flink_instance_details function."""

    @mock.patch("paasta_tools.flink_tools.get_runbook", autospec=True)
    @mock.patch("paasta_tools.flink_tools.get_team", autospec=True)
    def test_get_instance_details_with_running_cluster(
        self, mock_get_team, mock_get_runbook
    ):
        """Test getting instance details for a running cluster."""
        mock_get_team.return_value = "test-team"
        mock_get_runbook.return_value = "y/rb-test"

        metadata = {
            "labels": {"paasta.yelp.com/config_sha": "config123456"},
            "annotations": {
                "flink.yelp.com/dashboard_url": "http://dashboard.example.com"
            },
        }
        flink_config = mock.Mock(autospec=True)
        flink_config.flink_version = "1.17.2"
        flink_config.flink_revision = "abc123def"

        instance_config = mock.Mock(autospec=True)
        instance_config.get_pool.return_value = "flink-spot"
        instance_config.get_team.return_value = None  # Falls back to get_team
        instance_config.get_runbook.return_value = None  # Falls back to get_runbook

        result = flink_tools.get_flink_instance_details(
            metadata, flink_config, instance_config, "test-service"
        )

        assert result["config_sha"] == "config123456"
        assert result["version"] == "1.17.2"
        assert result["version_revision"] == "abc123def"
        assert result["dashboard_url"] == "http://dashboard.example.com"
        assert result["pool"] == "flink-spot"
        assert result["team"] == "test-team"
        assert result["runbook"] == "y/rb-test"

    @mock.patch("paasta_tools.flink_tools.get_runbook", autospec=True)
    @mock.patch("paasta_tools.flink_tools.get_team", autospec=True)
    def test_get_instance_details_with_stopped_cluster(
        self, mock_get_team, mock_get_runbook
    ):
        """Test getting instance details for a stopped cluster (no flink_config)."""
        mock_get_team.return_value = "test-team"
        mock_get_runbook.return_value = "y/rb-test"

        metadata = {
            "labels": {"paasta.yelp.com/config_sha": "configabc123"},
            "annotations": {},
        }
        flink_config = None  # Cluster not running

        instance_config = mock.Mock(autospec=True)
        instance_config.get_pool.return_value = "flink-reserved"
        instance_config.get_team.return_value = "override-team"
        instance_config.get_runbook.return_value = "y/rb-override"

        result = flink_tools.get_flink_instance_details(
            metadata, flink_config, instance_config, "test-service"
        )

        assert result["config_sha"] == "configabc123"
        assert result["version"] is None
        assert result["version_revision"] is None
        assert result["dashboard_url"] is None
        assert result["pool"] == "flink-reserved"
        assert result["team"] == "override-team"
        assert result["runbook"] == "y/rb-override"

    def test_get_instance_details_raises_when_config_sha_missing(self):
        """Test that ValueError is raised when config_sha label is missing."""
        metadata = {
            "labels": {},  # No config_sha
            "annotations": {},
        }
        instance_config = mock.Mock(autospec=True)

        with pytest.raises(ValueError, match="expected config sha"):
            flink_tools.get_flink_instance_details(
                metadata, None, instance_config, "test-service"
            )


class TestFormatFlinkInstanceHeader:
    """Tests for format_flink_instance_header function."""

    def test_format_header_non_verbose(self):
        """Test formatting header in non-verbose mode."""
        details = {
            "config_sha": "abc123",
            "version": "1.17.2",
            "version_revision": "xyz789",
            "dashboard_url": "http://dashboard.example.com",
        }

        output = flink_tools.format_flink_instance_header(details, verbose=False)

        output_text = "\n".join(output)
        assert "Config SHA: abc123" in output_text
        assert "Flink version: 1.17.2" in output_text
        assert "xyz789" not in output_text  # Revision not shown in non-verbose
        assert "URL: http://dashboard.example.com/" in output_text

    def test_format_header_verbose(self):
        """Test formatting header in verbose mode."""
        details = {
            "config_sha": "def456",
            "version": "1.18.0",
            "version_revision": "rev123",
            "dashboard_url": "http://flink.example.com",
        }

        output = flink_tools.format_flink_instance_header(details, verbose=True)

        output_text = "\n".join(output)
        assert "Config SHA: def456" in output_text
        assert "Flink version: 1.18.0 rev123" in output_text
        assert "URL: http://flink.example.com/" in output_text

    def test_format_header_without_version(self):
        """Test formatting header when cluster is not running (no version)."""
        details = {
            "config_sha": "abc123",
            "version": None,
            "version_revision": None,
            "dashboard_url": None,
        }

        output = flink_tools.format_flink_instance_header(details, verbose=False)

        output_text = "\n".join(output)
        assert "Config SHA: abc123" in output_text
        assert "Flink version" not in output_text
        assert "URL:" not in output_text


class TestFormatFlinkInstanceMetadata:
    """Tests for format_flink_instance_metadata function."""

    def test_format_metadata(self):
        """Test formatting instance metadata."""
        details = {
            "pool": "flink-spot",
            "team": "data-team",
            "runbook": "y/rb-flink-guide",
        }

        output = flink_tools.format_flink_instance_metadata(details, "my-service")

        output_text = "\n".join(output)
        assert (
            "Repo(git): https://github.yelpcorp.com/services/my-service" in output_text
        )
        assert (
            "Repo(sourcegraph): https://sourcegraph.yelpcorp.com/services/my-service"
            in output_text
        )
        assert "Flink Pool: flink-spot" in output_text
        assert "Owner: data-team" in output_text
        assert "Flink Runbook: y/rb-flink-guide" in output_text


class TestFormatFlinkConfigLinks:
    """Tests for format_flink_config_links function."""

    def test_format_config_links(self):
        """Test formatting configuration repository links."""
        output = flink_tools.format_flink_config_links(
            "my-service", "main-instance", "prod"
        )

        output_text = "\n".join(output)
        assert (
            "Yelpsoa configs: https://github.yelpcorp.com/sysgit/yelpsoa-configs/tree/master/my-service"
            in output_text
        )
        assert (
            "Srv configs: https://github.yelpcorp.com/sysgit/srv-configs/tree/master/ecosystem/prod/my-service"
            in output_text
        )


class TestFormatFlinkLogCommands:
    """Tests for format_flink_log_commands function."""

    def test_format_log_commands(self):
        """Test formatting paasta logs commands."""
        output = flink_tools.format_flink_log_commands(
            "my-service", "my-instance", "pnw-prod"
        )

        output_text = "\n".join(output)
        assert "Flink Log Commands:" in output_text
        assert (
            "paasta logs -a 1h -c pnw-prod -s my-service -i my-instance" in output_text
        )
        assert (
            "paasta logs -a 1h -c pnw-prod -s my-service -i my-instance.TASKMANAGER"
            in output_text
        )
        assert (
            "paasta logs -a 1h -c pnw-prod -s my-service -i my-instance.JOBMANAGER"
            in output_text
        )
        assert (
            "paasta logs -a 1h -c pnw-prod -s my-service -i my-instance.SUPERVISOR"
            in output_text
        )


class TestFormatFlinkMonitoringLinks:
    """Tests for format_flink_monitoring_links function."""

    def test_format_monitoring_links(self):
        """Test formatting Grafana and cost monitoring links."""
        output = flink_tools.format_flink_monitoring_links(
            "test-service", "test-instance", "prod", "uswest2-prod"
        )

        output_text = "\n".join(output)
        assert "Flink Monitoring:" in output_text
        assert "Job Metrics:" in output_text
        assert "grafana.yelpcorp.com" in output_text
        assert "var-service=test-service" in output_text
        assert "var-instance=test-instance" in output_text
        assert "uswest2-prod" in output_text
        assert "Container Metrics:" in output_text
        assert "JVM Metrics:" in output_text
        assert "Flink Cost:" in output_text
        assert "app.cloudzero.com" in output_text
