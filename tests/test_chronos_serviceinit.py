#!/usr/bin/env python

import contextlib

import mock

from paasta_tools.utils import PaastaColors
import chronos_serviceinit


def test_format_chronos_job_status_disabled():
    example_job = {
        'disabled': True,
    }
    desired_state = ''
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state)
    assert PaastaColors.red("Disabled") in actual


def test_format_chronos_job_status_enabled():
    example_job = {
        'disabled': False,
    }
    desired_state = ''
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state)
    assert PaastaColors.green("Enabled") in actual


def test_format_chronos_job_status_desired_state():
    example_job = {}
    desired_state = 'stopped (or started)'
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state)
    assert desired_state in actual


def test_format_chronos_job_status_no_last_run():
    example_job = {
        'lastError': '',
        'lastSuccess': '',
    }
    desired_state = ''
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state)
    assert PaastaColors.yellow("New") in actual


def test_format_chronos_job_status_failure_no_success():
    example_job = {
        'lastError': '2015-04-20T23:20:00.420Z',
        'lastSuccess': '',
    }
    desired_state = ''
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state)
    assert PaastaColors.red("Fail") in actual


def test_format_chronos_job_status_success_no_failure():
    example_job = {
        'lastError': '',
        'lastSuccess': '2015-04-20T23:20:00.420Z',
    }
    desired_state = ''
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state)
    assert PaastaColors.green("OK") in actual


def test_format_chronos_job_status_failure_and_then_success():
    example_job = {
        'lastError': '2015-04-20T23:20:00.420Z',
        'lastSuccess': '2015-04-21T23:20:00.420Z',
    }
    desired_state = ''
    actual = chronos_serviceinit.format_chronos_job_status(example_job, desired_state)
    assert PaastaColors.green("OK") in actual


def test_status_chronos_job_is_deployed():
    jobs = [{'name': 'my_service my_instance gityourmom configyourdad'}]
    complete_job_config = ''
    with contextlib.nested(
        mock.patch(
            'chronos_serviceinit.format_chronos_job_status',
            autospec=True,
            return_value='job_status_output',
        ),
        mock.patch(
            'chronos_serviceinit.get_desired_state_human',
            autospec=True,
        ),
    ):
        actual = chronos_serviceinit.status_chronos_job(
            jobs,
            complete_job_config,
        )
        assert actual == 'job_status_output'


def test_status_chronos_job_get_desired_state_human():
    jobs = [{'name': 'my_service my_instance gityourmom configyourdad'}]
    complete_job_config = ''
    with contextlib.nested(
        mock.patch(
            'chronos_serviceinit.format_chronos_job_status',
            autospec=True,
            return_value='job_status_output',
        ),
        mock.patch(
            'chronos_serviceinit.get_desired_state_human',
            autospec=True,
        ),
    ) as (_, mock_get_desired_state_human):
        chronos_serviceinit.status_chronos_job(
            jobs,
            complete_job_config,
        )
        assert mock_get_desired_state_human.call_count == 1


def test_status_chronos_job_is_not_deployed():
    jobs = []
    complete_job_config = ''
    with contextlib.nested(
        mock.patch(
            'chronos_serviceinit.format_chronos_job_status',
            autospec=True,
            return_value='job_status_output',
        ),
        mock.patch(
            'chronos_serviceinit.get_desired_state_human',
            autospec=True,
        ),
    ):
        actual = chronos_serviceinit.status_chronos_job(
            jobs,
            complete_job_config,
        )
        assert 'not setup' in actual


def test_status_chronos_job_multiple_jobs():
    jobs = [
        {'name': 'my_service my_instance gityourmom configyourdad'},
        {'name': 'my_service my_instance gityourmom configyourbro'},
    ]
    complete_job_config = ''
    with contextlib.nested(
        mock.patch(
            'chronos_serviceinit.format_chronos_job_status',
            autospec=True,
            return_value='job_status_output',
        ),
        mock.patch(
            'chronos_serviceinit.get_desired_state_human',
            autospec=True,
        ),
    ):
        actual = chronos_serviceinit.status_chronos_job(
            jobs,
            complete_job_config,
        )
        assert actual == 'job_status_output\njob_status_output'
