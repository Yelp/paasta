#!/usr/bin/env python

from service_deployment_tools import setup_chronos_job


def test_get_name():
    service_name = 'foo'
    actual = setup_chronos_job.get_name(service_name)
    expected = 'foo'
    assert expected == actual


def test_get_command():
    job_config = {'command': '/bin/true'}
    actual = setup_chronos_job.get_command(job_config)
    expected = '/bin/true'
    assert expected == actual


def test_get_executor():
    expected = setup_chronos_job.DEFAULT_EXECUTOR
    actual = setup_chronos_job.get_executor()
    assert expected == actual


def test_get_executor_flags():
    expected = setup_chronos_job.DEFAULT_EXECUTOR_FLAGS
    actual = setup_chronos_job.get_executor_flags()
    assert expected == actual


def test_epsilon_default():
    job_config = dict()
    actual = setup_chronos_job.get_epsilon(job_config)
    expected = setup_chronos_job.DEFAULT_EPSILON
    assert expected == actual


def test_epsilon_can_be_specified():
    job_config = {'epsilon': 'P1Y'}
    actual = setup_chronos_job.get_epsilon(job_config)
    expected = 'P1Y'
    assert expected == actual


def test_epsilon_rejects_invalid():
    job_config = {'epsilon': 'some random string'}

    try:
        setup_chronos_job.get_epsilon(job_config)
        assert False
    except ValueError:
        assert True


def test_retries_default():
    job_config = dict()
    actual = setup_chronos_job.get_retries(job_config)
    expected = setup_chronos_job.DEFAULT_RETRIES
    assert expected == actual


def test_retries_can_be_specified():
    job_config = {'retries': '5'}
    actual = setup_chronos_job.get_retries(job_config)
    expected = 5
    assert expected == actual


def test_retries_rejects_invalid():
    job_config = {'retries': 'this is no integer'}
    try:
        setup_chronos_job.get_retries(job_config)
        assert False
    except ValueError:
        assert True


def test_get_owner():
    job_config = {'failure_contact_email': 'developer@example.com'}
    actual = setup_chronos_job.get_owner(job_config)
    expected = 'developer@example.com'
    assert expected == actual


def test_get_async():
    actual = setup_chronos_job.get_async()
    expected = False
    assert expected == actual


def test_get_cpus_default():
    job_config = {}
    actual = setup_chronos_job.get_cpus(job_config)
    expected = setup_chronos_job.DEFAULT_CPUS
    assert expected == actual


def test_get_cpus_can_be_specified():
    job_config = {'mesos_cpus': 5.5}
    actual = setup_chronos_job.get_cpus(job_config)
    expected = 5.5
    assert expected == actual


def test_get_cpus_rejects_invalid():
    job_config = {'mesos_cpus': 'this is not valid'}
    try:
        setup_chronos_job.get_cpus(job_config)
        assert False
    except ValueError:
        assert True


def test_get_memory_default():
    job_config = {}
    actual = setup_chronos_job.get_memory(job_config)
    expected = setup_chronos_job.DEFAULT_MEMORY
    assert expected == actual


def test_get_memory_can_be_specified():
    job_config = {'mesos_memory': 3}
    actual = setup_chronos_job.get_memory(job_config)
    expected = 3
    assert expected == actual


def test_get_memory_rejects_invalid():
    job_config = {'mesos_memory': 'this is not valid'}
    try:
        setup_chronos_job.get_memory(job_config)
        assert False
    except ValueError:
        assert True


def test_get_disk_default():
    job_config = {}
    actual = setup_chronos_job.get_disk(job_config)
    expected = setup_chronos_job.DEFAULT_DISK
    assert expected == actual


def test_get_disk_can_be_specified():
    job_config = {'mesos_disk': 14}
    actual = setup_chronos_job.get_disk(job_config)
    expected = 14
    assert expected == actual


def test_get_disk_rejects_invalid():
    job_config = {'mesos_disk': 'this is not valid'}
    try:
        setup_chronos_job.get_disk(job_config)
        assert False
    except ValueError:
        assert True


def test_not_disabled():
    actual = setup_chronos_job.get_disabled()
    expected = False
    assert actual == expected


def test_uris():
    actual = setup_chronos_job.get_uris()
    expected = setup_chronos_job.DEFAULT_URIS
    assert actual == expected


def test_schedule():
    job_config = {'schedule': 'P1Y'}
    actual = setup_chronos_job.get_schedule(job_config)
    expected = 'P1Y'
    assert expected == actual


def test_parse_job_config():
    service_name = "my_test_service"
    job_config = {
        'command': '/bin/true',
        'failure_contact_email': 'developer@example.com',
        'schedule': 'R1Y',
    }

    actual = setup_chronos_job.parse_job_config(service_name, job_config)
    expected = {
        'async': False,
        'command': '/bin/true',
        'cpus': 0.10000000000000001,
        'disabled': False,
        'disk': 100,
        'epsilon': 'PT60S',
        'executor': 'some sort of default - look in to this',
        'executorFlags': 'flags that will be determined',
        'memory': 100,
        'name': 'my_test_service',
        'owner': 'developer@example.com',
        'retries': 2,
        'schedule': 'R1Y',
        'uris': [],
    }
    assert expected == actual
