import contextlib

import isodate
import mock
from pytest import raises

import chronos_tools


class TestChronosTools:

    fake_chronos_job_config = chronos_tools.ChronosJobConfig({
        'name': 'test',
        'command': '/bin/sleep 40',
        'description': 'This is a test Chronos job.',
        'shell': 'false',
        'epsilon': 'PT30M',
        'executor': 'test-executor',
        'executor_flags': '',
        'retries': 5,
        'owner': 'test@test.com',
        'disabled': 'true',
        'async': 'true',
        'cpus': 5.5,
        'disk': 2048.5,
        'mem': 1024.4,
        'uris': [],
        'environment_variables': [],
        'arguments': [],
        'run_as_user': 'root',
        'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
        'schedule_time_zone': '',
    })

    def test_load_chronos_job_config(self):
        fake_service_name = 'test_service'
        fake_cluster = 'penguin'
        fake_soa_dir = '/tmp/'
        expected_chronos_conf_file = 'chronos-penguin'
        with contextlib.nested(
            mock.patch('service_configuration_lib.read_extra_service_information', autospec=True),
        ) as (
            mock_read_extra_service_information,
        ):
            chronos_tools.load_chronos_job_config(fake_service_name, fake_cluster, fake_soa_dir)
            mock_read_extra_service_information.assert_called_once_with(fake_service_name,
                                                                        expected_chronos_conf_file,
                                                                        soa_dir=fake_soa_dir)

    def test_get_name_default(self):
        job_config = chronos_tools.ChronosJobConfig({})
        actual = job_config.get_name()
        assert actual is None

    def test_get_name_specified(self):
        actual = self.fake_chronos_job_config.get_name()
        expected = 'test'
        assert actual == expected

    def test_get_description_default(self):
        job_config = chronos_tools.ChronosJobConfig({})
        actual = job_config.get_description()
        assert actual is None

    def test_get_description_specified(self):
        actual = self.fake_chronos_job_config.get_description()
        expected = 'This is a test Chronos job.'
        assert actual == expected

    def test_get_command_default(self):
        job_config = chronos_tools.ChronosJobConfig({})
        actual = job_config.get_command()
        assert actual is None

    def test_get_command_specified(self):
        actual = self.fake_chronos_job_config.get_command()
        expected = '/bin/sleep 40'
        assert actual == expected

    def test_get_epsilon_default(self):
        job_config = chronos_tools.ChronosJobConfig({})
        actual = job_config.get_epsilon()
        expected = chronos_tools.DEFAULT_EPSILON
        assert actual == expected

    def test_get_epsilon_specified(self):
        actual = self.fake_chronos_job_config.get_epsilon()
        expected = 'PT30M'
        assert actual == expected

    def test_get_epsilon_invalid(self):
        job_config = chronos_tools.ChronosJobConfig({'epsilon': 'this is not valid'})
        with raises(isodate.ISO8601Error):
            job_config.get_epsilon()

    def test_get_retries_default(self):
        job_config = chronos_tools.ChronosJobConfig({})
        actual = job_config.get_retries()
        expected = chronos_tools.DEFAULT_RETRIES
        assert actual == expected

    def test_get_retries_specified(self):
        actual = self.fake_chronos_job_config.get_retries()
        expected = 5
        assert actual == expected

    def test_get_retries_invalid(self):
        job_config = chronos_tools.ChronosJobConfig({'retries': 'this is not valid'})
        with raises(ValueError):
            job_config.get_retries()

    def test_get_owner_default(self):
        job_config = chronos_tools.ChronosJobConfig({})
        actual = job_config.get_owner()
        assert actual is None

    def test_get_owner_specified(self):
        actual = self.fake_chronos_job_config.get_owner()
        expected = 'test@test.com'
        assert actual == expected

    def test_get_async_default(self):
        job_config = chronos_tools.ChronosJobConfig({})
        actual = job_config.get_async()
        expected = chronos_tools.DEFAULT_ASYNC
        assert actual == expected

    def test_get_cpus_default(self):
        job_config = chronos_tools.ChronosJobConfig({})
        actual = job_config.get_cpus()
        expected = chronos_tools.DEFAULT_CPUS
        assert actual == expected

    def test_get_cpus_specified(self):
        actual = self.fake_chronos_job_config.get_cpus()
        expected = 5.5
        assert actual == expected

    def test_get_cpus_invalid(self):
        job_config = chronos_tools.ChronosJobConfig({'cpus': 'this is not valid'})
        with raises(ValueError):
            job_config.get_cpus()

    def test_get_mem_default(self):
        job_config = chronos_tools.ChronosJobConfig({})
        actual = job_config.get_mem()
        expected = chronos_tools.DEFAULT_MEM
        assert actual == expected

    def test_get_mem_specified(self):
        actual = self.fake_chronos_job_config.get_mem()
        expected = 1024.4
        assert actual == expected

    def test_get_mem_invalid(self):
        job_config = chronos_tools.ChronosJobConfig({'mem': 'this is not valid'})
        with raises(ValueError):
            job_config.get_mem()

    def test_get_disk_default(self):
        job_config = chronos_tools.ChronosJobConfig({})
        actual = job_config.get_disk()
        expected = chronos_tools.DEFAULT_DISK
        assert actual == expected

    def test_get_disk_specified(self):
        actual = self.fake_chronos_job_config.get_disk()
        expected = 2048.5
        assert actual == expected

    def test_get_disk_invalid(self):
        job_config = chronos_tools.ChronosJobConfig({'disk': 'this is not valid'})
        with raises(ValueError):
            job_config.get_disk()

    def test_get_disabled_default(self):
        job_config = chronos_tools.ChronosJobConfig({})
        actual = job_config.get_disabled()
        expected = chronos_tools.DEFAULT_DISABLED
        assert actual == expected

    def test_get_disabled_specified(self):
        actual = self.fake_chronos_job_config.get_disabled()
        expected = 'true'
        assert actual == expected

    def test_get_schedule_default(self):
        job_config = chronos_tools.ChronosJobConfig({})
        actual = job_config.get_schedule()
        assert actual is None

    def test_get_schedule_specified(self):
        actual = self.fake_chronos_job_config.get_schedule()
        expected = 'R/2015-03-25T19:36:35Z/PT5M'
        assert actual == expected

    def test_check_scheduled_job_reqs_complete(self):
        with contextlib.nested(
            mock.patch('chronos_tools.InvalidChronosConfig', autospec=True),
        ) as (
            fake_InvalidChronosConfig,
        ):
            self.fake_chronos_job_config.check_scheduled_job_reqs()
            assert fake_InvalidChronosConfig.call_count == 0

    def test_check_scheduled_job_reqs_incomplete(self):
        job_config = chronos_tools.ChronosJobConfig({'name': None})
        with raises(chronos_tools.InvalidChronosConfig):
            job_config.check_scheduled_job_reqs()

    def test_format_chronos_job_dict(self):
        fake_service_name = 'test_service'
        fake_description = 'this service is just a test'
        fake_command = 'echo foo >> /tmp/test_service_log'
        fake_schedule = 'R10/2012-10-01T05:52:00Z/PT1M'
        fake_owner = 'bob@example.com'
        actual = chronos_tools.ChronosJobConfig({
            'name': fake_service_name,
            'description': fake_description,
            'command': fake_command,
            'schedule': fake_schedule,
            'owner': fake_owner,
        })
        expected = chronos_tools.ChronosJobConfig({
            'name': fake_service_name,
            'description': fake_description,
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
        })
        actual.format_chronos_job_dict()
        assert sorted(actual) == sorted(expected)
