#!/usr/bin/env python

import mock

from paasta_tools.utils import PaastaColors
import paasta_chronos_serviceinit


def test_format_chronos_job_status_disabled():
    example_disabled_job = {
        'disabled': True,
    }
    actual = paasta_chronos_serviceinit.format_chronos_job_status(example_disabled_job)
    assert PaastaColors.red("Disabled") in actual


def test_format_chronos_job_status_enabled():
    example_enabled_job = {
        'disabled': False,
    }
    actual = paasta_chronos_serviceinit.format_chronos_job_status(example_enabled_job)
    assert PaastaColors.green("Enabled") in actual


def test_format_chronos_job_no_last_run():
    example_job = {
        'lastError': '',
        'lastSuccess': '',
    }
    actual = paasta_chronos_serviceinit.format_chronos_job_status(example_job)
    assert PaastaColors.yellow("New") in actual


def test_format_chronos_job_failure_no_success():
    example_job = {
        'lastError': '2015-04-20T23:20:00.420Z',
        'lastSuccess': '',
    }
    actual = paasta_chronos_serviceinit.format_chronos_job_status(example_job)
    assert PaastaColors.red("Fail") in actual


def test_format_chronos_job_success_no_failure():
    example_job = {
        'lastError': '',
        'lastSuccess': '2015-04-20T23:20:00.420Z',
    }
    actual = paasta_chronos_serviceinit.format_chronos_job_status(example_job)
    assert PaastaColors.green("OK") in actual


def test_format_chronos_job_failure_and_success():
    example_job = {
        'lastError': '2015-04-20T23:20:00.420Z',
        'lastSuccess': '2015-04-21T23:20:00.420Z',
    }
    actual = paasta_chronos_serviceinit.format_chronos_job_status(example_job)
    assert PaastaColors.green("OK") in actual


def test_status_chronos_job_is_deployed():
    all_jobs = [{'name': 'my_service my_instance gityourmom configyourdad'}]
    with mock.patch('paasta_chronos_serviceinit.format_chronos_job_status',
                    autospec=True, return_value='job_status_output'):
        actual = paasta_chronos_serviceinit.status_chronos_job(
            'my_service my_instance',
            all_jobs,
        )
        assert actual == 'job_status_output'


def test_status_chronos_job_is_not_deployed():
    all_jobs = []
    with mock.patch('paasta_chronos_serviceinit.format_chronos_job_status',
                    autospec=True, return_value='job_status_output'):
        actual = paasta_chronos_serviceinit.status_chronos_job(
            'my_service my_instance',
            all_jobs,
        )
        assert 'not setup' in actual


def test_status_chronos_job_multiple_jobs():
    all_jobs = [
        {'name': 'my_service my_instance gityourmom configyourdad'},
        {'name': 'my_service my_instance gityourmom configyourbro'},
    ]
    with mock.patch('paasta_chronos_serviceinit.format_chronos_job_status',
                    autospec=True, return_value='job_status_output'):
        actual = paasta_chronos_serviceinit.status_chronos_job(
            'my_service my_instance',
            all_jobs,
        )
        assert actual == 'job_status_output\njob_status_output'
