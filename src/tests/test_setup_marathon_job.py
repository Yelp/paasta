#!/usr/bin/env python

from service_deployment_tools import setup_marathon_job

fake_service_config = {
    'port': 42
}
fake_docker_image = 'test_docker:1.0'
fake_marathon_job_config = {
    'instances': 3,
    'cpus': 1,
    'mem': 100,
    'docker_image': fake_docker_image
}
fake_docker_registry = 'remote_registry.com'
fake_marathon_configuration = {
    'cluster': 'test_cluster',
    'url': 'http://test_url',
    'user': 'admin',
    'pass': 'admin_pass',
    'docker_registry': fake_docker_registry,
}


def test_get_marathon_config():
    setup_marathon_job.get_marathon_config()


def test_main():
    pass


def test_docker_url():
    actual = setup_marathon_job.get_docker_url("fake_registry", "fake_image:fake_version")
    expected = "fake_registry/fake_image:fake_version"
    assert actual == expected


def test_get_docker_registry():
    actual = setup_marathon_job.get_docker_registry(fake_marathon_configuration)
    expected = fake_docker_registry
    assert actual == expected


def test_get_docker_image():
    actual = setup_marathon_job.get_docker_image(fake_marathon_job_config)
    expected = fake_docker_image
    assert actual == expected
