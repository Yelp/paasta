#!/usr/bin/env python
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
import datetime

import humanize
import mock
import pytest
import pytz

from paasta_tools import chronos_serviceinit
from paasta_tools import chronos_tools
from paasta_tools.utils import datetime_from_utc_to_local
from paasta_tools.utils import PaastaColors


def test_start_chronos_job():
    service = "my_service"
    instance = "my_instance"
    job_id = "my_job_id"
    cluster = "my_cluster"
    old_schedule = "R/2015-03-25T19:36:35Z/PT5M"
    job_soa_config = mock.Mock(
        get_disabled=mock.Mock(return_value=False),
        get_desired_state=mock.Mock(return_value="start"),
    )
    job_config = {"beep": "boop", "disabled": False, "schedule": old_schedule}
    with mock.patch(
        "paasta_tools.chronos_serviceinit.chronos_tools.chronos.ChronosClient",
        autospec=True,
    ) as mock_client, mock.patch(
        "paasta_tools.chronos_serviceinit._log", autospec=True
    ):
        chronos_serviceinit.start_chronos_job(
            service,
            instance=instance,
            job_id=job_id,
            client=mock_client,
            cluster=cluster,
            job_config=job_soa_config,
            complete_job_config=job_config,
            emergency=True,
        )
        assert job_config["schedule"] == old_schedule
        mock_client.update.assert_called_once_with(job_config)
        mock_client.run.assert_called_once_with(job_id)


def test_start_chronos_job_does_not_run_disabled_or_stopped_job():
    service = "my_service"
    instance = "my_instance"
    job_id = "my_job_id"
    cluster = "my_cluster"
    old_schedule = "R/2015-03-25T19:36:35Z/PT5M"
    job_soa_config = mock.Mock()
    job_config = {"beep": "boop", "disabled": True, "schedule": old_schedule}
    with mock.patch(
        "paasta_tools.chronos_serviceinit.chronos_tools.chronos.ChronosClient",
        autospec=True,
    ) as mock_client, mock.patch(
        "paasta_tools.chronos_serviceinit._log", autospec=True
    ):
        chronos_serviceinit.start_chronos_job(
            service,
            instance=instance,
            job_id=job_id,
            client=mock_client,
            cluster=cluster,
            job_config=job_soa_config,
            complete_job_config=job_config,
            emergency=True,
        )
        assert job_config["schedule"] == old_schedule
        assert mock_client.update.call_count == 0
        assert mock_client.run.call_count == 0


def test_stop_chronos_job():
    service = "my_service"
    instance = "my_instance"
    cluster = "my_cluster"
    existing_jobs = [
        {"name": "job_v1", "disabled": False},
        {"name": "job_v2", "disabled": False},
        {"name": "job_v3", "disabled": True},
    ]
    with mock.patch(
        "paasta_tools.chronos_serviceinit.chronos_tools.chronos.ChronosClient",
        autospec=True,
    ) as mock_client, mock.patch(
        "paasta_tools.chronos_serviceinit._log", autospec=True
    ):
        chronos_serviceinit.stop_chronos_job(
            service, instance, mock_client, cluster, existing_jobs
        )
        assert mock_client.update.call_count == 3
        assert mock_client.delete_tasks.call_count == 3
        for job in existing_jobs:
            assert job["disabled"] is True
            mock_client.update.assert_any_call(job)
            mock_client.delete_tasks.assert_any_call(job["name"])


def test_get_short_task_id():
    task_id = "ct:1111111111111:0:my_service my_instance gityourmom configyourdad:"
    assert chronos_serviceinit.get_short_task_id(task_id) == "1111111111111"


@mock.patch("paasta_tools.chronos_tools.get_chronos_status_for_job", autospec=True)
def test_format_chronos_job_name_exists(mock_status):
    example_job = {"name": "my_service my_instance", "schedule": "foo"}
    running_tasks = []
    verbose = False
    mock_client = mock.Mock()
    actual = chronos_serviceinit.format_chronos_job_status(
        mock_client, example_job, running_tasks, verbose
    )
    assert example_job["name"] in actual


@mock.patch("paasta_tools.chronos_tools.get_chronos_status_for_job", autospec=True)
def test_format_chronos_job_temp_job(mock_status):
    example_job = {
        "name": "%s my_service my_instance" % chronos_tools.TMP_JOB_IDENTIFIER,
        "schedule": "foo",
    }
    running_tasks = []
    verbose = False
    mock_client = mock.Mock()
    actual = chronos_serviceinit.format_chronos_job_status(
        mock_client, example_job, running_tasks, verbose
    )
    assert "Job:     tmp my_service my_instance (Launched by paasta rerun)" in actual


@mock.patch("paasta_tools.chronos_tools.get_chronos_status_for_job", autospec=True)
def test_format_chronos_job_name_does_not_exist(mock_status):
    example_job = {"name": "my_service my_instance", "schedule": "foo"}
    running_tasks = []
    verbose = False
    mock_client = mock.Mock()
    actual = chronos_serviceinit.format_chronos_job_status(
        mock_client, example_job, running_tasks, verbose
    )
    assert PaastaColors.red("UNKNOWN") in actual


@mock.patch("paasta_tools.chronos_tools.get_chronos_status_for_job", autospec=True)
def test_format_chronos_job_status_disabled(mock_status):
    example_job = {
        "name": "my_service my_instance",
        "disabled": True,
        "schedule": "foo",
    }
    running_tasks = []
    verbose = False
    mock_client = mock.Mock()
    actual = chronos_serviceinit.format_chronos_job_status(
        mock_client, example_job, running_tasks, verbose
    )
    assert PaastaColors.grey("Not scheduled") in actual


@mock.patch("paasta_tools.chronos_tools.get_chronos_status_for_job", autospec=True)
def test_format_chronos_job_status_enabled(mock_status):
    example_job = {
        "name": "my_service my_instance",
        "disabled": False,
        "schedule": "foo",
    }
    running_tasks = []
    verbose = False
    mock_client = mock.Mock()
    actual = chronos_serviceinit.format_chronos_job_status(
        mock_client, example_job, running_tasks, verbose
    )
    assert PaastaColors.green("Scheduled") in actual


@mock.patch(
    "paasta_tools.chronos_tools.get_chronos_status_for_job",
    return_value="fake_chronos_status",
    autospec=True,
)
def test_format_chronos_job_chronos_status(mock_status):
    example_job = {
        "name": "my_service my_instance",
        "disabled": True,
        "schedule": "foo",
    }
    running_tasks = []
    verbose = False
    mock_client = mock.Mock()
    actual = chronos_serviceinit.format_chronos_job_status(
        mock_client, example_job, running_tasks, verbose
    )
    assert PaastaColors.grey("fake_chronos_status") in actual


@mock.patch("paasta_tools.chronos_tools.get_chronos_status_for_job", autospec=True)
def test_format_chronos_job_status_no_last_run(mock_status):
    example_job = {
        "name": "my_service my_instance",
        "lastError": "",
        "lastSuccess": "",
        "schedule": "foo",
    }
    running_tasks = []
    verbose = False
    mock_client = mock.Mock()
    actual = chronos_serviceinit.format_chronos_job_status(
        mock_client, example_job, running_tasks, verbose
    )
    assert PaastaColors.yellow("New") in actual
    assert "(never)" in actual


@mock.patch("paasta_tools.chronos_tools.get_chronos_status_for_job", autospec=True)
def test_format_chronos_job_status_failure_no_success(mock_status):
    example_job = {
        "name": "my_service my_instance",
        "lastError": "2015-04-20T23:20:00.420Z",
        "lastSuccess": "",
        "schedule": "foo",
    }
    running_tasks = []
    verbose = False
    mock_client = mock.Mock()
    actual = chronos_serviceinit.format_chronos_job_status(
        mock_client, example_job, running_tasks, verbose
    )
    assert PaastaColors.red("Failed") in actual
    assert "(2015-04-20T23:20" in actual
    assert "ago)" in actual


@mock.patch("paasta_tools.chronos_tools.get_chronos_status_for_job", autospec=True)
def test_format_chronos_job_status_success_no_failure(mock_status):
    example_job = {
        "name": "my_service my_instance",
        "lastError": "",
        "lastSuccess": "2015-04-20T23:20:00.420Z",
        "schedule": "foo",
    }
    running_tasks = []
    verbose = False
    mock_client = mock.Mock()
    actual = chronos_serviceinit.format_chronos_job_status(
        mock_client, example_job, running_tasks, verbose
    )
    assert PaastaColors.green("OK") in actual
    assert "(2015-04-20" in actual
    assert "ago)" in actual


@mock.patch("paasta_tools.chronos_tools.get_chronos_status_for_job", autospec=True)
def test_format_chronos_job_status_failure_and_then_success(mock_status):
    example_job = {
        "name": "my_service my_instance",
        "lastError": "2015-04-20T23:20:00.420Z",
        "lastSuccess": "2015-04-21T23:20:00.420Z",
        "schedule": "foo",
    }
    running_tasks = []
    verbose = False
    mock_client = mock.Mock()
    actual = chronos_serviceinit.format_chronos_job_status(
        mock_client, example_job, running_tasks, verbose
    )
    assert PaastaColors.green("OK") in actual
    assert "(2015-04-21" in actual
    assert "ago)" in actual


@mock.patch("paasta_tools.chronos_tools.get_chronos_status_for_job", autospec=True)
def test_format_chronos_job_status_success_and_then_failure(mock_status):
    example_job = {
        "name": "my_service my_instance",
        "lastError": "2015-04-21T23:20:00.420Z",
        "lastSuccess": "2015-04-20T23:20:00.420Z",
        "schedule": "foo",
    }
    running_tasks = []
    verbose = False
    mock_client = mock.Mock()
    actual = chronos_serviceinit.format_chronos_job_status(
        mock_client, example_job, running_tasks, verbose
    )
    assert PaastaColors.red("Failed") in actual
    assert "(2015-04-21" in actual
    assert "ago)" in actual


@mock.patch("paasta_tools.chronos_tools.get_chronos_status_for_job", autospec=True)
def test_format_chronos_job_schedule(mock_status):
    example_job = {
        "name": "my_service my_instance",
        "schedule": "R/2015-04-20T23:20:00+00:00/PT60M",
        "epsilon": "PT42S",
    }
    running_tasks = []
    verbose = False
    mock_client = mock.Mock()
    actual = chronos_serviceinit.format_chronos_job_status(
        mock_client, example_job, running_tasks, verbose
    )
    assert example_job["schedule"] in actual
    assert example_job["epsilon"] in actual


@mock.patch("paasta_tools.chronos_tools.get_chronos_status_for_job", autospec=True)
def test_format_chronos_job_command(mock_status):
    example_job = {
        "name": "my_service my_instance",
        "command": "do the hokey pokey",
        "schedule": "foo",
    }
    running_tasks = []
    verbose = False
    mock_client = mock.Mock()
    actual = chronos_serviceinit.format_chronos_job_status(
        mock_client, example_job, running_tasks, verbose
    )
    assert example_job["command"] in actual


@mock.patch("paasta_tools.chronos_tools.get_chronos_status_for_job", autospec=True)
def test_format_chronos_job_zero_mesos_tasks(mock_status):
    example_job = {"name": "my_service my_instance", "schedule": "foo"}
    running_tasks = []
    verbose = False
    mock_client = mock.Mock()
    actual = chronos_serviceinit.format_chronos_job_status(
        mock_client, example_job, running_tasks, verbose
    )
    assert PaastaColors.grey("Not running") in actual


@mock.patch("paasta_tools.chronos_tools.get_chronos_status_for_job", autospec=True)
def test_format_chronos_job_one_mesos_task(mock_status):
    example_job = {"name": "my_service my_instance", "schedule": "foo"}
    running_tasks = ["slay the nemean lion"]
    verbose = False
    mock_client = mock.Mock()
    actual = chronos_serviceinit.format_chronos_job_status(
        mock_client, example_job, running_tasks, verbose
    )
    assert PaastaColors.yellow("Running") in actual


def test_format_parents_summary():
    parents = ["service instance", "service otherinstance"]
    assert (
        chronos_serviceinit._format_parents_summary(parents)
        == " service instance,service otherinstance"
    )


def test_format_parents_verbose():
    example_job = {"name": "myexamplejob", "parents": ["testservice testinstance"]}
    fake_last_datetime = "2007-04-01T17:52:58.908Z"
    fake_last_datetime_dt = datetime.datetime(2007, 4, 1, 17, 52, 58, 908000, pytz.utc)
    example_status = (fake_last_datetime, chronos_tools.LastRunState.Success)
    with mock.patch(
        "paasta_tools.chronos_tools.get_jobs_for_service_instance",
        autospec=True,
        return_value=[{"name": "testservice testinstance"}],
    ), mock.patch(
        "paasta_tools.chronos_tools.get_status_last_run",
        autospec=True,
        return_value=example_status,
    ):
        expected_time = humanize.naturaltime(
            datetime_from_utc_to_local(fake_last_datetime_dt)
        )
        actual = chronos_serviceinit._format_parents_verbose(example_job)
        assert "testservice testinstance" in actual
        assert (
            "  Last Run: {} (2007-04-01T17:52, {})".format(
                PaastaColors.green("OK"), expected_time
            )
            in actual
        )


def test_format_schedule_dependent_job():
    example_job = {"epsilon": "myepsilon", "parents": ["testservice testinstance"]}
    actual = chronos_serviceinit._format_schedule(example_job)
    assert "None (Dependent Job)." in actual
    assert "Epsilon: myepsilon" in actual


def test_format_schedule_null_scheduletimezone():
    example_job = {"scheduleTimeZone": "null"}  # This is what Chronos returns.
    actual = chronos_serviceinit._format_schedule(example_job)
    assert (
        "(UTC) Epsilon" in actual
    )  # In the output, we default to UTC (Chronos should do the same)


def test_format_schedule_scheduletimezone():
    example_job = {"scheduleTimeZone": "Zulu"}
    actual = chronos_serviceinit._format_schedule(example_job)
    assert "(Zulu) Epsilon" in actual


@pytest.mark.parametrize("verbosity_level", [1, 2, 3])
def test_format_chronos_job_mesos_verbose(verbosity_level):
    example_job = {"name": "my_service my_instance", "schedule": "foo"}
    running_tasks = ["slay the nemean lion"]
    expected_tail_lines = {1: 0, 2: 10, 3: 100}[verbosity_level]
    mock_client = mock.Mock()
    with mock.patch(
        "paasta_tools.chronos_serviceinit.status_mesos_tasks_verbose",
        autospec=True,
        return_value="status_mesos_tasks_verbose output",
    ) as mock_status_mesos_tasks_verbose, mock.patch(
        "paasta_tools.chronos_tools.get_chronos_status_for_job", autospec=True
    ):
        actual = chronos_serviceinit.format_chronos_job_status(
            mock_client, example_job, running_tasks, verbosity_level
        )
    mock_status_mesos_tasks_verbose.assert_called_once_with(
        filter_string=example_job["name"],
        get_short_task_id=chronos_serviceinit.get_short_task_id,
        tail_lines=expected_tail_lines,
    )
    assert "status_mesos_tasks_verbose output" in actual


@mock.patch("paasta_tools.chronos_serviceinit.format_chronos_job_status", autospec=True)
@mock.patch(
    "paasta_tools.chronos_serviceinit.get_cached_list_of_running_tasks_from_frameworks",
    autospec=True,
)
def test_status_chronos_jobs_is_deployed(
    mock_get_cached_list_of_running_tasks_from_frameworks,
    mock_format_chronos_job_status,
):
    jobs = [{"name": "my_service my_instance"}]
    complete_job_config = mock.Mock()
    complete_job_config.get_desired_state_human = mock.Mock()
    verbose = False

    mock_format_chronos_job_status.return_value = "job_status_output"
    mock_get_cached_list_of_running_tasks_from_frameworks.return_value = [
        {"id": "ct:1492206300000:0:my_service my_instance:"},
        {"id": "ct:1492206300000:0:not_my_service not_my_instance:"},
    ]
    fake_client = mock.Mock()

    actual = chronos_serviceinit.status_chronos_jobs(
        fake_client, jobs, complete_job_config, verbose
    )
    assert "\njob_status_output" in actual
    assert mock_get_cached_list_of_running_tasks_from_frameworks.called
    mock_format_chronos_job_status.assert_called_once_with(
        fake_client, jobs[0], 1, verbose
    )


def test_status_chronos_jobs_is_not_deployed():
    jobs = []
    complete_job_config = mock.Mock()
    complete_job_config.get_desired_state_human = mock.Mock()
    verbose = False
    with mock.patch(
        "paasta_tools.chronos_serviceinit.format_chronos_job_status",
        autospec=True,
        return_value="job_status_output",
    ), mock.patch(
        "paasta_tools.chronos_serviceinit.get_cached_list_of_running_tasks_from_frameworks",
        autospec=True,
        return_value=[],
    ):
        actual = chronos_serviceinit.status_chronos_jobs(
            mock.Mock(), jobs, complete_job_config, verbose  # Chronos client
        )
        assert "not set up" in actual


def test_status_chronos_jobs_get_desired_state_human():
    jobs = [{"name": "my_service my_instance gityourmom configyourdad"}]
    complete_job_config = mock.Mock()
    complete_job_config.get_desired_state_human = mock.Mock()
    verbose = False
    with mock.patch(
        "paasta_tools.chronos_serviceinit.format_chronos_job_status",
        autospec=True,
        return_value="job_status_output",
    ), mock.patch(
        "paasta_tools.chronos_serviceinit.get_cached_list_of_running_tasks_from_frameworks",
        autospec=True,
        return_value=[],
    ):
        actual = chronos_serviceinit.status_chronos_jobs(
            mock.Mock(), jobs, complete_job_config, verbose  # Chronos client
        )
        assert "Desired:" in actual
        assert repr(complete_job_config.get_desired_state_human.return_value) in actual


def test_status_chronos_jobs_multiple_jobs():
    jobs = [
        {"name": "my_service my_instance gityourmom configyourdad"},
        {"name": "my_service my_instance gityourmom configyourbro"},
    ]
    complete_job_config = mock.Mock()
    complete_job_config.get_desired_state_human = mock.Mock()
    verbose = False
    with mock.patch(
        "paasta_tools.chronos_serviceinit.format_chronos_job_status",
        autospec=True,
        return_value="job_status_output",
    ), mock.patch(
        "paasta_tools.chronos_serviceinit.get_cached_list_of_running_tasks_from_frameworks",
        autospec=True,
        return_value=[],
    ):
        actual = chronos_serviceinit.status_chronos_jobs(
            mock.Mock(), jobs, complete_job_config, verbose  # Chronos client
        )
        assert "\njob_status_output\njob_status_output" in actual


def test_status_chronos_jobs_get_running_tasks():
    jobs = [{"name": "my_service my_instance gityourmom configyourdad"}]
    complete_job_config = mock.Mock()
    complete_job_config.get_desired_state_human = mock.Mock()
    verbose = False
    with mock.patch(
        "paasta_tools.chronos_serviceinit.format_chronos_job_status",
        autospec=True,
        return_value="job_status_output",
    ), mock.patch(
        "paasta_tools.chronos_serviceinit.get_cached_list_of_running_tasks_from_frameworks",
        autospec=True,
        return_value=[],
    ) as mock_get_running_tasks:
        chronos_serviceinit.status_chronos_jobs(
            mock.Mock(), jobs, complete_job_config, verbose  # Chronos client
        )
        assert mock_get_running_tasks.call_count == 1


def test_get_schedule_for_job_type_scheduled():
    assert (
        chronos_serviceinit._get_schedule_field_for_job_type(
            chronos_tools.JobType.Scheduled
        )
        == "Schedule"
    )


def test_get_schedule_for_job_type_dependent():
    assert (
        chronos_serviceinit._get_schedule_field_for_job_type(
            chronos_tools.JobType.Dependent
        )
        == "Parents"
    )


def test_get_schedule_for_job_type_invalid():
    with pytest.raises(ValueError):
        assert chronos_serviceinit._get_schedule_field_for_job_type(3)
