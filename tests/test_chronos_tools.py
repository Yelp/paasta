import contextlib

import isodate
import mock
from pytest import raises

import chronos_tools


def test_get_name():
    job_config = {'name': 'foo'}
    actual = chronos_tools.get_name(job_config)
    expected = 'foo'
    assert actual == expected


def test_get_command():
    job_config = {'command': '/bin/true'}
    actual = chronos_tools.get_command(job_config)
    expected = '/bin/true'
    assert actual == expected


def test_get_executor():
    job_config = {'executor': 'test-executor'}
    expected = 'test-executor'
    actual = chronos_tools.get_executor(job_config)
    assert actual == expected


def test_get_executor_default():
    job_config = {}
    expected = chronos_tools.DEFAULT_EXECUTOR
    actual = chronos_tools.get_executor(job_config)
    assert actual == expected


# def mock_get_docker_url_for_image(docker_image):
#     return 'test-repository/%s' % docker_image


# @patch('paasta_tools.chronos_tools.get_docker_url_for_image', mock_get_docker_url_for_image)
# def test_get_executor_flags():
#     job_config = {
#         'docker_image': 'test_docker_image',
#         'docker_volumes': ['option_1', 'option_2'],
#     }
#     expected = json.dumps({'container': {
#         'image': 'test-repository/test_docker_image',
#         'options': ['option_1', 'option_2'],
#     }})
#     actual = chronos_tools.get_executor_flags(job_config)
#     assert actual == expected


def test_get_epsilon_default():
    job_config = {}
    actual = chronos_tools.get_epsilon(job_config)
    expected = chronos_tools.DEFAULT_EPSILON
    assert actual == expected


def test_get_epsilon_specified():
    job_config = {'epsilon': 'P1Y'}
    actual = chronos_tools.get_epsilon(job_config)
    expected = 'P1Y'
    assert actual == expected


def test_get_epsilon_rejects_invalid():
    job_config = {'epsilon': 'some random string'}

    with raises(isodate.ISO8601Error):
        chronos_tools.get_epsilon(job_config)


def test_get_retries_default():
    job_config = {}
    actual = chronos_tools.get_retries(job_config)
    expected = chronos_tools.DEFAULT_RETRIES
    assert actual == expected


def test_get_retries_specified():
    job_config = {'retries': '5'}
    actual = chronos_tools.get_retries(job_config)
    expected = 5
    assert actual == expected


def test_get_retries_rejects_invalid():
    job_config = {'retries': 'this is no integer'}
    with raises(ValueError):
        chronos_tools.get_retries(job_config)


def test_get_owner():
    fake_owner = 'developer@example.com'
    job_config = {'failure_contact_email': fake_owner}
    actual = chronos_tools.get_owner(job_config)
    expected = fake_owner
    assert actual == expected


def test_get_async():
    job_config = {}
    actual = chronos_tools.get_async(job_config)
    expected = chronos_tools.DEFAULT_ASYNC
    assert actual == expected


def test_get_cpus_default():
    job_config = {}
    actual = chronos_tools.get_cpus(job_config)
    expected = chronos_tools.DEFAULT_CPUS
    assert actual == expected


def test_get_cpus_specified():
    job_config = {'cpus': 5.5}
    actual = chronos_tools.get_cpus(job_config)
    expected = 5.5
    assert actual == expected


def test_get_cpus_rejects_invalid():
    job_config = {'cpus': 'this is not valid'}
    with raises(ValueError):
        chronos_tools.get_cpus(job_config)


def test_get_mem_default():
    job_config = {}
    actual = chronos_tools.get_mem(job_config)
    expected = chronos_tools.DEFAULT_MEM
    assert actual == expected


def test_get_mem_specified():
    job_config = {'mem': 3}
    actual = chronos_tools.get_mem(job_config)
    expected = 3
    assert actual == expected


def test_get_mem_rejects_invalid():
    job_config = {'mem': 'this is not valid'}
    with raises(ValueError):
        chronos_tools.get_mem(job_config)


def test_get_disk_default():
    job_config = {}
    actual = chronos_tools.get_disk(job_config)
    expected = chronos_tools.DEFAULT_DISK
    assert actual == expected


def test_get_disk_specified():
    job_config = {'disk': 14}
    actual = chronos_tools.get_disk(job_config)
    expected = 14
    assert actual == expected


def test_get_disk_rejects_invalid():
    job_config = {'disk': 'this is not valid'}
    with raises(ValueError):
        chronos_tools.get_disk(job_config)


def test_get_disabled_default():
    job_config = {}
    actual = chronos_tools.get_disabled(job_config)
    expected = chronos_tools.DEFAULT_DISABLED
    assert actual == expected


# @patch('paasta_tools.chronos_tools.get_docker_url_for_image', mock_get_docker_url_for_image)
# def test_uris():
#     job_config = {
#         'docker_image': 'test_docker_image',
#     }
#     expected = ['test-repository/test_docker_image']
#     actual = chronos_tools.get_uris(job_config)
#     assert actual == expected


def test_get_schedule():
    fake_schedule = 'R10/2012-10-01T05:52:00Z/PT2S'
    job_config = {'schedule': fake_schedule}
    actual = chronos_tools.get_schedule(job_config)
    expected = fake_schedule
    assert actual == expected


# @patch('paasta_tools.chronos_tools.get_docker_url_for_image', mock_get_docker_url_for_image)
# def test_parse_job_config():
#     job_config = {
#         'name': 'my_test_job',
#         'command': '/bin/true',
#         'failure_contact_email': 'developer@example.com',
#         'schedule': 'R1Y',
#         'docker_image': 'test_image',
#     }
#
#     actual = chronos_tools.parse_job_config(job_config)
#     expected = {
#         'async': False,
#         'command': '/bin/true',
#         'cpus': 0.1,
#         'disabled': False,
#         'disk': 100,
#         'epsilon': 'PT60S',
#         'executor': '',
#         'executorFlags': '{"container": {"image": "test-repository/test_image", "options": []}}',
#         'mem': 100,
#         'name': 'my_test_job',
#         'owner': 'developer@example.com',
#         'retries': 2,
#         'schedule': 'R1Y',
#         'uris': ['test-repository/test_image'],
#     }
#     assert sorted(actual) == sorted(expected)


def test_create_chronos_config():
    fake_service_name = 'test_service'
    fake_command = 'echo foo >> /tmp/test_service_log'
    fake_schedule = 'R10/2012-10-01T05:52:00Z/PT1M'
    fake_owner = 'bob@example.com'
    fake_config = {
        'name': fake_service_name,
        'command': fake_command,
        'schedule': fake_schedule,
        'failure_contact_email': fake_owner,
    }
    expected = {
        'name': fake_service_name,
        'command': fake_command,
        'schedule': fake_schedule,
        'epsilon': chronos_tools.DEFAULT_EPSILON,
        'owner': fake_owner,
        'async': chronos_tools.DEFAULT_ASYNC,
        'cpus': chronos_tools.DEFAULT_CPUS,
        'mem': chronos_tools.DEFAULT_MEM,
        'disk': chronos_tools.DEFAULT_DISK,
        'retries': chronos_tools.DEFAULT_RETRIES,
        'disabled': chronos_tools.DEFAULT_DISABLED,
    }
    with contextlib.nested(
        mock.patch('chronos_tools.load_chronos_job_config', autospec=True, return_value=fake_config),
    ) as (
        mock_load_chronos_job_config,
    ):
        actual = chronos_tools.create_chronos_config('test_service', 'mesosstage', '/tmp/fake/soa/dir')
        assert mock_load_chronos_job_config.call_count == 1
        assert sorted(actual) == sorted(expected)
