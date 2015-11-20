#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
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


import contextlib
import mock

import chronos_serviceinit
from paasta_tools.utils import PaastaColors


def test_start_chronos_job():
    service = 'my_service'
    instance = 'my_instance'
    job_id = 'my_job_id'
    cluster = 'my_cluster'
    old_schedule = 'R/2015-03-25T19:36:35Z/PT5M'
    job_config = {'beep': 'boop', 'disabled': False, 'schedule': old_schedule}
    with contextlib.nested(
        mock.patch('chronos_serviceinit.chronos_tools.chronos.ChronosClient', autospec=True),
    ) as (
        mock_client,
    ):
        chronos_serviceinit.start_chronos_job(service,
                                              instance,
                                              job_id,
                                              mock_client,
                                              cluster,
                                              job_config)
        assert job_config['schedule'] == old_schedule
        mock_client.update.assert_called_once_with(job_config)
        mock_client.run.assert_called_once_with(job_id)


def test_start_chronos_job_does_not_run_disabled_job():
    service = 'my_service'
    instance = 'my_instance'
    job_id = 'my_job_id'
    cluster = 'my_cluster'
    old_schedule = 'R/2015-03-25T19:36:35Z/PT5M'
    job_config = {'beep': 'boop', 'disabled': True, 'schedule': old_schedule}
    with contextlib.nested(
        mock.patch('chronos_serviceinit.chronos_tools.chronos.ChronosClient', autospec=True),
    ) as (
        mock_client,
    ):
        chronos_serviceinit.start_chronos_job(service,
                                              instance,
                                              job_id,
                                              mock_client,
                                              cluster,
                                              job_config)
        assert job_config['schedule'] == old_schedule
        mock_client.update.assert_called_once_with(job_config)
        assert mock_client.run.call_count == 0


def test_stop_chronos_job():
    service = 'my_service'
    instance = 'my_instance'
    cluster = 'my_cluster'
    existing_jobs = [{'name': 'job_v1', 'disabled': False},
                     {'name': 'job_v2', 'disabled': False},
                     {'name': 'job_v3', 'disabled': True}]
    with contextlib.nested(
        mock.patch('chronos_serviceinit.chronos_tools.chronos.ChronosClient', autospec=True),
    ) as (
        mock_client,
    ):
        chronos_serviceinit.stop_chronos_job(service, instance, mock_client, cluster, existing_jobs)
        assert mock_client.update.call_count == 3
        assert mock_client.delete_tasks.call_count == 3
        for job in existing_jobs:
            assert job['disabled'] is True
            mock_client.update.assert_any_call(job)
            mock_client.delete_tasks.assert_any_call(job['name'])


def test_get_short_task_id():
    task_id = 'ct:1111111111111:0:my_service my_instance gityourmom configyourdad:'
    assert chronos_serviceinit.get_short_task_id(task_id) == '1111111111111'


def test_format_chronos_job_name_exists():
    example_job = {
        'name': 'my_service my_instance gityourmom configyourdad',
    }
    desired_state = ''
    running_tasks = []
    verbose = False
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state, running_tasks, verbose)
    assert example_job['name'] in actual


def test_format_chronos_job_name_does_not_exist():
    example_job = {}
    desired_state = ''
    running_tasks = []
    verbose = False
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state, running_tasks, verbose)
    assert PaastaColors.red('UNKNOWN') in actual


def test_format_chronos_job_status_disabled():
    example_job = {
        'disabled': True,
    }
    desired_state = ''
    running_tasks = []
    verbose = False
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state, running_tasks, verbose)
    assert PaastaColors.red('Disabled') in actual


def test_format_chronos_job_status_enabled():
    example_job = {
        'disabled': False,
    }
    desired_state = ''
    running_tasks = []
    verbose = False
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state, running_tasks, verbose)
    assert PaastaColors.green('Enabled') in actual


def test_format_chronos_job_status_desired_state_passed_through():
    example_job = {}
    desired_state = 'stopped (or started)'
    running_tasks = []
    verbose = False
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state, running_tasks, verbose)
    assert desired_state in actual


def test_format_chronos_job_status_no_last_run():
    example_job = {
        'lastError': '',
        'lastSuccess': '',
    }
    desired_state = ''
    running_tasks = []
    verbose = False
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state, running_tasks, verbose)
    assert PaastaColors.yellow('New') in actual
    assert '(never)' in actual


def test_format_chronos_job_status_failure_no_success():
    example_job = {
        'lastError': '2015-04-20T23:20:00.420Z',
        'lastSuccess': '',
    }
    desired_state = ''
    running_tasks = []
    verbose = False
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state, running_tasks, verbose)
    assert PaastaColors.red('Failed') in actual
    assert '(2015-04-20T23:20' in actual
    assert 'ago)' in actual


def test_format_chronos_job_status_success_no_failure():
    example_job = {
        'lastError': '',
        'lastSuccess': '2015-04-20T23:20:00.420Z',
    }
    desired_state = ''
    running_tasks = []
    verbose = False
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state, running_tasks, verbose)
    assert PaastaColors.green('OK') in actual
    assert '(2015-04-20' in actual
    assert 'ago)' in actual


def test_format_chronos_job_status_failure_and_then_success():
    example_job = {
        'lastError': '2015-04-20T23:20:00.420Z',
        'lastSuccess': '2015-04-21T23:20:00.420Z',
    }
    desired_state = ''
    running_tasks = []
    verbose = False
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state, running_tasks, verbose)
    assert PaastaColors.green('OK') in actual
    assert '(2015-04-21' in actual
    assert 'ago)' in actual


def test_format_chronos_job_status_success_and_then_failure():
    example_job = {
        'lastError': '2015-04-21T23:20:00.420Z',
        'lastSuccess': '2015-04-20T23:20:00.420Z',
    }
    desired_state = ''
    running_tasks = []
    verbose = False
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state, running_tasks, verbose)
    assert PaastaColors.red('Failed') in actual
    assert '(2015-04-21' in actual
    assert 'ago)' in actual


def test_format_chronos_job_schedule():
    example_job = {
        'schedule': 'R/2015-04-20T23:20:00+00:00/PT60M',
        'epsilon': 'PT42S',
    }
    desired_state = ''
    running_tasks = []
    verbose = False
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state, running_tasks, verbose)
    assert example_job['schedule'] in actual
    assert example_job['epsilon'] in actual


def test_format_chronos_job_command():
    example_job = {
        'command': 'do the hokey pokey',
    }
    desired_state = ''
    running_tasks = []
    verbose = False
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state, running_tasks, verbose)
    assert example_job['command'] in actual


def test_format_chronos_job_zero_mesos_tasks():
    example_job = {}
    desired_state = ''
    running_tasks = []
    verbose = False
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state, running_tasks, verbose)
    assert PaastaColors.grey('Not running') in actual


def test_format_chronos_job_one_mesos_task():
    example_job = {}
    desired_state = ''
    running_tasks = ['slay the nemean lion']
    verbose = False
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state, running_tasks, verbose)
    assert PaastaColors.yellow('Running') in actual


def test_format_chronos_job_two_mesos_tasks():
    example_job = {}
    desired_state = ''
    running_tasks = ['slay the nemean lion', 'slay the lernaean hydra']
    verbose = False
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state, running_tasks, verbose)
    assert 'Critical' in actual


def test_format_chronos_job_mesos_verbose():
    example_job = {
        'name': 'my_service my_instance gityourmom configyourdad',
    }
    desired_state = ''
    running_tasks = ['slay the nemean lion']
    verbose = True
    with mock.patch(
        'chronos_serviceinit.status_mesos_tasks_verbose',
        autospec=True,
        return_value='status_mesos_tasks_verbose output',
    ) as mock_status_mesos_tasks_verbose:
        actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state, running_tasks, verbose)
    mock_status_mesos_tasks_verbose.assert_called_once_with(example_job['name'], chronos_serviceinit.get_short_task_id)
    assert 'status_mesos_tasks_verbose output' in actual


def test_status_chronos_jobs_is_deployed():
    jobs = [{'name': 'my_service my_instance gityourmom configyourdad'}]
    complete_job_config = mock.Mock()
    complete_job_config.get_desired_state_human = mock.Mock()
    verbose = False
    with contextlib.nested(
        mock.patch(
            'chronos_serviceinit.format_chronos_job_status',
            autospec=True,
            return_value='job_status_output',
        ),
        mock.patch(
            'chronos_serviceinit.get_running_tasks_from_active_frameworks',
            autospec=True,
            return_value=[],
        ),
    ):
        actual = chronos_serviceinit.status_chronos_jobs(
            jobs,
            complete_job_config,
            verbose,
        )
        assert actual == 'job_status_output'


def test_status_chronos_jobs_is_not_deployed():
    jobs = []
    complete_job_config = mock.Mock()
    complete_job_config.get_desired_state_human = mock.Mock()
    verbose = False
    with contextlib.nested(
        mock.patch(
            'chronos_serviceinit.format_chronos_job_status',
            autospec=True,
            return_value='job_status_output',
        ),
        mock.patch(
            'chronos_serviceinit.get_running_tasks_from_active_frameworks',
            autospec=True,
            return_value=[],
        ),
    ):
        actual = chronos_serviceinit.status_chronos_jobs(
            jobs,
            complete_job_config,
            verbose,
        )
        assert 'not set up' in actual


def test_status_chronos_jobs_get_desired_state_human():
    jobs = [{'name': 'my_service my_instance gityourmom configyourdad'}]
    complete_job_config = mock.Mock()
    complete_job_config.get_desired_state_human = mock.Mock()
    verbose = False
    with contextlib.nested(
        mock.patch(
            'chronos_serviceinit.format_chronos_job_status',
            autospec=True,
            return_value='job_status_output',
        ),
        mock.patch(
            'chronos_serviceinit.get_running_tasks_from_active_frameworks',
            autospec=True,
            return_value=[],
        ),
    ):
        chronos_serviceinit.status_chronos_jobs(
            jobs,
            complete_job_config,
            verbose,
        )
        assert complete_job_config.get_desired_state_human.call_count == 1


def test_status_chronos_jobs_multiple_jobs():
    jobs = [
        {'name': 'my_service my_instance gityourmom configyourdad'},
        {'name': 'my_service my_instance gityourmom configyourbro'},
    ]
    complete_job_config = mock.Mock()
    complete_job_config.get_desired_state_human = mock.Mock()
    verbose = False
    with contextlib.nested(
        mock.patch(
            'chronos_serviceinit.format_chronos_job_status',
            autospec=True,
            return_value='job_status_output',
        ),
        mock.patch(
            'chronos_serviceinit.get_running_tasks_from_active_frameworks',
            autospec=True,
            return_value=[],
        ),
    ):
        actual = chronos_serviceinit.status_chronos_jobs(
            jobs,
            complete_job_config,
            verbose,
        )
        assert actual == 'job_status_output\njob_status_output'


def test_status_chronos_jobs_get_running_tasks():
    jobs = [{'name': 'my_service my_instance gityourmom configyourdad'}]
    complete_job_config = mock.Mock()
    complete_job_config.get_desired_state_human = mock.Mock()
    verbose = False
    with contextlib.nested(
        mock.patch(
            'chronos_serviceinit.format_chronos_job_status',
            autospec=True,
            return_value='job_status_output',
        ),
        mock.patch(
            'chronos_serviceinit.get_running_tasks_from_active_frameworks',
            autospec=True,
            return_value=[],
        ),
    ) as (_, mock_get_running_tasks):
        chronos_serviceinit.status_chronos_jobs(
            jobs,
            complete_job_config,
            verbose,
        )
        assert mock_get_running_tasks.call_count == 1
