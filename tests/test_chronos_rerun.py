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
import re
import sys

import mock
import pytest

from paasta_tools import chronos_rerun
from paasta_tools import chronos_tools
from paasta_tools.chronos_tools import SPACER


@mock.patch(
    "paasta_tools.chronos_rerun.chronos_tools.parse_time_variables", autospec=True
)
def test_modify_command_for_date(mock_parse_time_variables):
    mock_parse_time_variables.return_value = "2016-03-17"
    fake_chronos_job_config = {"command": "foo"}
    actual = chronos_rerun.modify_command_for_date(
        chronos_job=fake_chronos_job_config, date=datetime.datetime.now(), verbose=False
    )

    assert actual == {"command": "2016-03-17"}


def test_modify_command_for_date_no_command():
    fake_chronos_job_config = {"command": None, "name": "service instance"}
    actual = chronos_rerun.modify_command_for_date(
        chronos_job=fake_chronos_job_config, date=datetime.datetime.now(), verbose=True
    )
    assert actual == fake_chronos_job_config


def test_remove_parents():
    fake_chronos_job_config = {"parents": ["foo", "bar", "baz"]}
    assert chronos_rerun.remove_parents(fake_chronos_job_config) == {}


def test_set_default_schedule():
    fake_chronos_job_config = {"schedule": "foo"}
    assert chronos_rerun.set_default_schedule(fake_chronos_job_config) == {
        "schedule": "R1//PT1M"
    }


def test_set_tmp_naming_scheme():
    fake_chronos_job_config = {"name": "foo bar"}
    name_pattern = re.compile(r"%s-.* foo bar" % chronos_tools.TMP_JOB_IDENTIFIER)
    assert (
        name_pattern.match(
            chronos_rerun.set_tmp_naming_scheme(
                chronos_job=fake_chronos_job_config, timestamp=mock.Mock()
            )["name"]
        )
        is not None
    )


@mock.patch("paasta_tools.chronos_rerun.set_tmp_naming_scheme", autospec=True)
@mock.patch("paasta_tools.chronos_rerun.chronos_tools.get_job_type", autospec=True)
def test_clone_job(mock_get_job_type, mock_set_tmp_naming_scheme):
    fake_chronos_job_config = {"parents": ["foo", "bar"]}
    mock_get_job_type.return_value = chronos_tools.JobType.Dependent
    timestamp = datetime.datetime.utcnow().isoformat()
    chronos_rerun.clone_job(fake_chronos_job_config, timestamp=timestamp)
    assert mock_get_job_type.call_count == 1
    assert mock_set_tmp_naming_scheme.call_count == 1


@mock.patch("paasta_tools.chronos_rerun.modify_command_for_date", autospec=True)
@mock.patch("paasta_tools.chronos_rerun.chronos_tools.get_job_type", autospec=True)
def test_clone_job_dependent_jobs(mock_get_job_type, mock_modify_command_for_date):
    fake_chronos_job_config = {"name": "child", "parents": ["foo", "bar"]}
    timestamp = "2017-06-12T11:59:45.583867"
    timestamp_chronos_name = "2017-06-12T115945583867"

    mock_modify_command_for_date.side_effect = lambda job, date, verbose: job
    mock_get_job_type.return_value = chronos_tools.JobType.Dependent

    cloned_job = chronos_rerun.clone_job(fake_chronos_job_config, timestamp=timestamp)

    expected_job_config = {
        "name": "tmp-{} {}".format(
            timestamp_chronos_name, fake_chronos_job_config["name"]
        ),
        "parents": [
            f"tmp-{timestamp_chronos_name}{SPACER}{parent}"
            for parent in fake_chronos_job_config["parents"]
        ],
    }

    assert cloned_job == expected_job_config


@pytest.mark.parametrize(
    "cluster, service, instance, run_all_related_jobs, is_dependent_job",
    (
        ("testcluster", "testservice", "test_independent_instance_1", False, False),
        ("testcluster", "testservice", "test_dependent_instance_2", False, True),
        ("testcluster", "testservice", "test_dependent_instance_2", True, True),
    ),
)
@mock.patch("paasta_tools.chronos_rerun.clone_job", autospec=True)
@mock.patch("paasta_tools.chronos_rerun.modify_command_for_date", autospec=True)
@mock.patch("paasta_tools.chronos_rerun.chronos_tools.get_job_type", autospec=True)
@mock.patch("paasta_tools.chronos_rerun.remove_parents", autospec=True)
@mock.patch("paasta_tools.chronos_tools.create_complete_config", autospec=True)
@mock.patch("paasta_tools.chronos_tools.load_v2_deployments_json", autospec=True)
@mock.patch("service_configuration_lib.read_services_configuration", autospec=True)
@mock.patch("paasta_tools.chronos_tools.read_chronos_jobs_for_service", autospec=True)
@mock.patch("paasta_tools.chronos_tools.get_chronos_client", autospec=True)
@mock.patch("paasta_tools.chronos_tools.load_chronos_config", autospec=True)
@mock.patch("paasta_tools.chronos_rerun.load_system_paasta_config", autospec=True)
def test_chronos_rerun_main_with_independent_job(
    mock_load_system_paasta_config,
    mock_load_chronos_config,
    mock_get_chronos_client,
    mock_read_chronos_jobs_for_service,
    mock_read_services_configuration,
    mock_load_v2_deployments_json,
    mock_create_complete_config,
    mock_remove_parents,
    mock_get_job_type,
    mock_modify_command_for_date,
    mock_clone_job,
    cluster,
    service,
    instance,
    run_all_related_jobs,
    is_dependent_job,
):
    mock_load_system_paasta_config.return_value.get_cluster.return_value = cluster

    generic_config_dict = {
        "bounce_method": "graceful",
        "cmd": "/bin/sleep 40",
        "epsilon": "PT30M",
        "retries": 5,
        "cpus": 5.5,
        "mem": 1024.4,
        "disk": 1234.5,
        "disabled": False,
        "schedule_time_zone": "Zulu",
        "monitoring": {"fake_monitoring_info": "fake_monitoring_value"},
    }

    def gen_scheduled_job():
        return dict(schedule="R/2015-03-25T19:36:35Z/PT5M", **generic_config_dict)

    def gen_dependent_job(service, instance):
        return dict(parents=f"{service}.{instance}", **generic_config_dict)

    mock_read_services_configuration.return_value = [service]
    mock_read_chronos_jobs_for_service.return_value = {
        "test_independent_instance_1": gen_scheduled_job(),
        "test_dependent_instance_1": gen_scheduled_job(),
        "test_dependent_instance_2": gen_dependent_job(
            service, "test_dependent_instance_1"
        ),
    }

    mock_load_v2_deployments_json.return_value.get_branch_dict.side_effect = lambda service, *args, **kwargs: {
        "desired_state": "start",
        "docker_image": f"paasta-{service}-{cluster}",
    }

    if is_dependent_job:
        mock_get_job_type.return_value = chronos_tools.JobType.Dependent
    else:
        mock_get_job_type.return_value = chronos_tools.JobType.Scheduled

    execution_date = datetime.datetime.now().replace(microsecond=0)

    testargs = ["chronos_rerun"]
    if run_all_related_jobs:
        testargs.append("--run-all-related-jobs")
    testargs.extend([f"{service} {instance}", execution_date.isoformat()])

    with mock.patch.object(sys, "argv", testargs):
        chronos_rerun.main()

    if not run_all_related_jobs:
        # remove_parents should not be called if the job is not a dependent job
        assert mock_remove_parents.call_count == (1 if is_dependent_job else 0)
        assert mock_get_chronos_client.return_value.add.call_count == 1
    else:
        assert mock_remove_parents.call_count == 0
        assert mock_get_chronos_client.return_value.add.call_count == 2
