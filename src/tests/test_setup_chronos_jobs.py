#!/usr/bin/env python

from service_deployment_tools import setup_chronos_jobs
from mock import patch
import json


def test_extract_chronos_jobs_trivial():
    services = {}
    ecosystem = 'testecosystem'
    soa_dir = '/dne'
    actual = setup_chronos_jobs.extract_chronos_jobs(services, ecosystem, soa_dir)
    expected = []
    assert expected == actual


def mock_read_chronos_soa_configs(service_name, ecosystem, soa_dir):
    if service_name == 'test_service_1':
        return [{
            'name': 'test_job_1',
            'command': '/bin/test_script1',
            'failure_contact_email': 'test_service_1@example.com',
            'schedule': 'R/2014-07-11T18:10:00Z/P1D',
        }]
    elif service_name == 'test_service_2':
        return [
            {
                'name': 'test_job_2',
                'command': '/bin/test_script2',
                'failure_contact_email': 'test_service_2@example.com',
                'schedule': 'R/2014-07-11T18:10:00Z/P1D',
            },
            {
                'name': 'test_job_3',
                'command': '/bin/test_script3',
                'failure_contact_email': 'test_service_3@example.com',
                'schedule': 'R/2014-07-11T18:10:00Z/P1D',
            },
        ]
    else:
        return []


@patch('service_deployment_tools.setup_chronos_jobs.read_chronos_soa_configs', mock_read_chronos_soa_configs)
def test_extract_chronos_jobs():
    services = {'test_service_1': None, 'test_service_2': None}
    ecosystem = 'testecosystem'
    soa_dir = '/dne'

    actual = setup_chronos_jobs.extract_chronos_jobs(services, ecosystem, soa_dir)
    expected = [
        {
            'name': 'test_job_2',
            'command': '/bin/test_script2',
            'failure_contact_email': 'test_service_2@example.com',
            'schedule': 'R/2014-07-11T18:10:00Z/P1D',
        },
        {
            'name': 'test_job_3',
            'command': '/bin/test_script3',
            'failure_contact_email': 'test_service_3@example.com',
            'schedule': 'R/2014-07-11T18:10:00Z/P1D',
        },
        {
            'name': 'test_job_1',
            'command': '/bin/test_script1',
            'failure_contact_email': 'test_service_1@example.com',
            'schedule': 'R/2014-07-11T18:10:00Z/P1D',
        },
    ]
    assert expected == actual


def test_get_name():
    job_config = {'name': 'foo'}
    actual = setup_chronos_jobs.get_name(job_config)
    expected = 'foo'
    assert expected == actual


def test_get_command():
    job_config = {'command': '/bin/true'}
    actual = setup_chronos_jobs.get_command(job_config)
    expected = '/bin/true'
    assert expected == actual


def test_get_executor():
    expected = setup_chronos_jobs.DEFAULT_EXECUTOR
    actual = setup_chronos_jobs.get_executor()
    assert expected == actual


def mock_get_docker_url_for_image(docker_image):
    return 'docker:///test-repository/%s' % docker_image


@patch('service_deployment_tools.setup_chronos_jobs.get_docker_url_for_image', mock_get_docker_url_for_image)
def test_get_executor_flags():
    job_config = {
        'docker_image': 'test_docker_image',
        'docker_options': ['option_1', 'option_2'],
    }
    expected = json.dumps({'container': {
        'image': 'docker:///test-repository/test_docker_image',
        'options': ['option_1', 'option_2'],
    }})
    actual = setup_chronos_jobs.get_executor_flags(job_config)
    assert expected == actual


def test_epsilon_default():
    job_config = dict()
    actual = setup_chronos_jobs.get_epsilon(job_config)
    expected = setup_chronos_jobs.DEFAULT_EPSILON
    assert expected == actual


def test_epsilon_can_be_specified():
    job_config = {'epsilon': 'P1Y'}
    actual = setup_chronos_jobs.get_epsilon(job_config)
    expected = 'P1Y'
    assert expected == actual


def test_epsilon_rejects_invalid():
    job_config = {'epsilon': 'some random string'}

    try:
        setup_chronos_jobs.get_epsilon(job_config)
        assert False
    except ValueError:
        assert True


def test_retries_default():
    job_config = dict()
    actual = setup_chronos_jobs.get_retries(job_config)
    expected = setup_chronos_jobs.DEFAULT_RETRIES
    assert expected == actual


def test_retries_can_be_specified():
    job_config = {'retries': '5'}
    actual = setup_chronos_jobs.get_retries(job_config)
    expected = 5
    assert expected == actual


def test_retries_rejects_invalid():
    job_config = {'retries': 'this is no integer'}
    try:
        setup_chronos_jobs.get_retries(job_config)
        assert False
    except ValueError:
        assert True


def test_get_owner():
    job_config = {'failure_contact_email': 'developer@example.com'}
    actual = setup_chronos_jobs.get_owner(job_config)
    expected = 'developer@example.com'
    assert expected == actual


def test_get_async():
    actual = setup_chronos_jobs.get_async()
    expected = False
    assert expected == actual


def test_get_cpus_default():
    job_config = {}
    actual = setup_chronos_jobs.get_cpus(job_config)
    expected = setup_chronos_jobs.DEFAULT_CPUS
    assert expected == actual


def test_get_cpus_can_be_specified():
    job_config = {'cpus': 5.5}
    actual = setup_chronos_jobs.get_cpus(job_config)
    expected = 5.5
    assert expected == actual


def test_get_cpus_rejects_invalid():
    job_config = {'cpus': 'this is not valid'}
    try:
        setup_chronos_jobs.get_cpus(job_config)
        assert False
    except ValueError:
        assert True


def test_get_mem_default():
    job_config = {}
    actual = setup_chronos_jobs.get_mem(job_config)
    expected = setup_chronos_jobs.DEFAULT_MEM
    assert expected == actual


def test_get_mem_can_be_specified():
    job_config = {'mem': 3}
    actual = setup_chronos_jobs.get_mem(job_config)
    expected = 3
    assert expected == actual


def test_get_mem_rejects_invalid():
    job_config = {'mem': 'this is not valid'}
    try:
        setup_chronos_jobs.get_mem(job_config)
        assert False
    except ValueError:
        assert True


def test_get_disk_default():
    job_config = {}
    actual = setup_chronos_jobs.get_disk(job_config)
    expected = setup_chronos_jobs.DEFAULT_DISK
    assert expected == actual


def test_get_disk_can_be_specified():
    job_config = {'disk': 14}
    actual = setup_chronos_jobs.get_disk(job_config)
    expected = 14
    assert expected == actual


def test_get_disk_rejects_invalid():
    job_config = {'disk': 'this is not valid'}
    try:
        setup_chronos_jobs.get_disk(job_config)
        assert False
    except ValueError:
        assert True


def test_not_disabled():
    actual = setup_chronos_jobs.get_disabled()
    expected = False
    assert actual == expected


def test_uris():
    actual = setup_chronos_jobs.get_uris()
    expected = setup_chronos_jobs.DEFAULT_URIS
    assert actual == expected


def test_schedule():
    job_config = {'schedule': 'P1Y'}
    actual = setup_chronos_jobs.get_schedule(job_config)
    expected = 'P1Y'
    assert expected == actual


@patch('service_deployment_tools.setup_chronos_jobs.get_docker_url_for_image', mock_get_docker_url_for_image)
def test_parse_job_config():
    job_config = {
        'name': 'my_test_job',
        'command': '/bin/true',
        'failure_contact_email': 'developer@example.com',
        'schedule': 'R1Y',
        'docker_image': 'test_image',
    }

    actual = setup_chronos_jobs.parse_job_config(job_config)
    expected = {
        'async': False,
        'command': '/bin/true',
        'cpus': 0.1,
        'disabled': False,
        'disk': 100,
        'epsilon': 'PT60S',
        'executor': '',
        'executorFlags': '{"container": {"image": "docker:///test-repository/test_image", "options": []}}',
        'mem': 100,
        'name': 'my_test_job',
        'owner': 'developer@example.com',
        'retries': 2,
        'schedule': 'R1Y',
        'uris': [],
    }
    assert expected == actual
