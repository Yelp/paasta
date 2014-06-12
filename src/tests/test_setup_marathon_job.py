#!/usr/bin/env python

from service_deployment_tools import setup_marathon_job
import testify as T


class ServiceDeploymentToolsTestCase(T.TestCase):
    fake_service_config = {
        'port': 42,
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

    def test_get_marathon_config(self):
        setup_marathon_job.get_marathon_config()

    def test_main(self):
        pass

    def test_docker_url(self):
        actual = setup_marathon_job.get_docker_url("fake_registry", "fake_image:fake_version")
        expected = "fake_registry/fake_image:fake_version"
        T.assert_equal(actual, expected)

    def test_get_docker_registry(self):
        actual = setup_marathon_job.get_docker_registry(fake_marathon_configuration)
        expected = fake_docker_registry
        T.assert_equal(actual, expected)

    def test_get_docker_image(self):
        actual = setup_marathon_job.get_docker_image(fake_marathon_job_config)
        expected = fake_docker_docker_image
        T.assert_equal(actual, expected)
