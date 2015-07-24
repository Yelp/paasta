import contextlib

import mock
from pytest import raises

import chronos_tools


class TestChronosTools:

    fake_chronos_job_config = chronos_tools.ChronosJobConfig(
        'test_service',
        {
            'name': 'test',
            'description': 'This is a test Chronos job.',
            'command': '/bin/sleep 40',
            'epsilon': 'PT30M',
            'shell': 'false',
            'retries': 5,
            'owner': 'test@test.com',
            'async': 'true',
            'cpus': 5.5,
            'mem': 1024.4,
            'disk': 2048.5,
            'disabled': 'true',
            'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
            'schedule_time_zone': '',
        }
    )

    def test_load_chronos_job_config(self):
        fake_config = {
            'name': 'test',
            'description': 'This is a test Chronos job.',
            'command': '/bin/sleep 40',
            'epsilon': 'PT30M',
            'shell': 'false',
            'retries': 5,
            'owner': 'test@test.com',
            'async': 'true',
            'cpus': 5.5,
            'mem': 1024.4,
            'disk': 2048.5,
            'disabled': 'true',
            'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
            'schedule_time_zone': '',
        }
        fake_service_name = 'test_service'
        fake_cluster = 'penguin'
        fake_soa_dir = '/tmp/'
        expected_chronos_conf_file = 'chronos-penguin'
        fake_chronos_job_config = ChronosJobConfig(fake_service_name, fake_config)
        with contextlib.nested(
            mock.patch('service_configuration_lib.read_extra_service_information', autospec=True),
        ) as (
            mock_read_extra_service_information,
        ):
            mock_read_extra_service_information.return_value = fake_config
            actual = chronos_tools.load_chronos_job_config(fake_service_name, fake_cluster, fake_soa_dir)
            mock_read_extra_service_information.assert_called_once_with(fake_service_name,
                                                                        expected_chronos_conf_file,
                                                                        soa_dir=fake_soa_dir)
            assert actual == self.fake_chronos_job_config

    def test_get_name_default(self):
        job_config = chronos_tools.ChronosJobConfig('', {})
        actual = job_config.get('name')
        assert actual is None

    def test_get_name_specified(self):
        actual = self.fake_chronos_job_config.get('name')
        expected = 'test'
        assert actual == expected

    def test_get_description_default(self):
        job_config = chronos_tools.ChronosJobConfig('', {})
        actual = job_config.get('description')
        assert actual is None

    def test_get_description_specified(self):
        actual = self.fake_chronos_job_config.get('description')
        expected = 'This is a test Chronos job.'
        assert actual == expected

    def test_get_command_default(self):
        job_config = chronos_tools.ChronosJobConfig('', {})
        actual = job_config.get('command')
        assert actual is None

    def test_get_command_specified(self):
        actual = self.fake_chronos_job_config.get('command')
        expected = '/bin/sleep 40'
        assert actual == expected

    def test_get_epsilon_default(self):
        job_config = chronos_tools.ChronosJobConfig('', {})
        actual = job_config.get('epsilon')
        expected = 'PT60S'
        assert actual == expected

    def test_get_epsilon_specified(self):
        actual = self.fake_chronos_job_config.get('epsilon')
        expected = 'PT30M'
        assert actual == expected

    def test_get_epsilon_invalid(self):
        job_config = chronos_tools.ChronosJobConfig({'epsilon': 'this is not valid'})
        with raises(chronos_tools.InvalidChronosConfigError) as excinfo:
            job_config.get('epsilon')
        assert str(excinfo.value) == ('The specified epsilon value \'this is not valid\' '
                                      'does not conform to the ISO8601 format')

    def test_get_retries_default(self):
        job_config = chronos_tools.ChronosJobConfig('', {})
        actual = job_config.get('retries')
        expected = 2
        assert actual == expected

    def test_get_retries_specified(self):
        actual = self.fake_chronos_job_config.get('retries')
        expected = 5
        assert actual == expected

    def test_get_retries_invalid(self):
        job_config = chronos_tools.ChronosJobConfig({'retries': 'this is not valid'})
        with raises(ValueError):
            job_config.get('retries')

    def test_get_owner_default(self):
        job_config = chronos_tools.ChronosJobConfig('', {})
        actual = job_config.get('owner')
        assert actual is None

    def test_get_owner_specified(self):
        actual = self.fake_chronos_job_config.get('owner')
        expected = 'test@test.com'
        assert actual == expected

    def test_get_async_default(self):
        job_config = chronos_tools.ChronosJobConfig('', {})
        actual = job_config.get('async')
        expected = 'false'
        assert actual == expected

    def test_get_cpus_default(self):
        job_config = chronos_tools.ChronosJobConfig('', {})
        actual = job_config.get('cpus')
        expected = 0.1
        assert actual == expected

    def test_get_cpus_specified(self):
        actual = self.fake_chronos_job_config.get('cpus')
        expected = 5.5
        assert actual == expected

    def test_get_cpus_invalid(self):
        job_config = chronos_tools.ChronosJobConfig({'cpus': 'this is not valid'})
        with raises(ValueError):
            job_config.get('cpus')

    def test_get_mem_default(self):
        job_config = chronos_tools.ChronosJobConfig('', {})
        actual = job_config.get('mem')
        expected = 128
        assert actual == expected

    def test_get_mem_specified(self):
        actual = self.fake_chronos_job_config.get('mem')
        expected = 1024.4
        assert actual == expected

    def test_get_mem_invalid(self):
        job_config = chronos_tools.ChronosJobConfig({'mem': 'this is not valid'})
        with raises(ValueError):
            job_config.get('mem')

    def test_get_disk_default(self):
        job_config = chronos_tools.ChronosJobConfig('', {})
        actual = job_config.get('disk')
        expected = 256
        assert actual == expected

    def test_get_disk_specified(self):
        actual = self.fake_chronos_job_config.get('disk')
        expected = 2048.5
        assert actual == expected

    def test_get_disk_invalid(self):
        job_config = chronos_tools.ChronosJobConfig({'disk': 'this is not valid'})
        with raises(ValueError):
            job_config.get('disk')

    def test_get_disabled_default(self):
        job_config = chronos_tools.ChronosJobConfig('', {})
        actual = job_config.get('disabled')
        expected = 'false'
        assert actual == expected

    def test_get_disabled_specified(self):
        actual = self.fake_chronos_job_config.get('disabled')
        expected = 'true'
        assert actual == expected

    def test_get_schedule_default(self):
        job_config = chronos_tools.ChronosJobConfig('', {})
        actual = job_config.get('schedule')
        assert actual is None

    def test_get_schedule_specified(self):
        actual = self.fake_chronos_job_config.get('schedule')
        expected = 'R/2015-03-25T19:36:35Z/PT5M'
        assert actual == expected

    def test_get_schedule_specified_with_start_time_now(self):
        job_config = chronos_tools.ChronosJobConfig({'schedule': 'R//PT5M'})
        actual = job_config.get('schedule')
        expected = 'R//PT5M'
        assert actual == expected

    def test_get_schedule_invalid_start_time(self):
        job_config = chronos_tools.ChronosJobConfig({'schedule': 'R/12345/PT5M'})
        with raises(chronos_tools.InvalidChronosConfigError) as excinfo:
            job_config.get('schedule')
        assert str(excinfo.value) == ('The specified start time \'12345\' in schedule \'R/12345/PT5M\' '
                                      'does not conform to the ISO 8601 format')

    def test_get_schedule_invalid_interval(self):
        job_config = chronos_tools.ChronosJobConfig({'schedule': 'R//12345'})
        with raises(chronos_tools.InvalidChronosConfigError) as excinfo:
            job_config.get('schedule')
        assert str(excinfo.value) == ('The specified interval \'12345\' in schedule \'R//12345\' '
                                      'does not conform to the ISO 8601 format')

    def test_check_scheduled_job_reqs_complete(self):
        self.fake_chronos_job_config.check_scheduled_job_reqs()

    def test_check_scheduled_job_reqs_incomplete(self):
        job_config = chronos_tools.ChronosJobConfig({'name': None})
        with raises(chronos_tools.InvalidChronosConfigError) as excinfo:
            job_config.check_scheduled_job_reqs()
        assert str(excinfo.value) == ('Your Chronos config is missing \'name\', '
                                      'a required parameter for a scheduled job.')

    def test_format_chronos_job_dict(self):
        fake_service_name = 'test_service'
        fake_description = 'this service is just a test'
        fake_command = 'echo foo >> /tmp/test_service_log'
        fake_schedule = 'R10/2012-10-01T05:52:00Z/PT1M'
        fake_owner = 'bob@example.com'
        incomplete_config = chronos_tools.ChronosJobConfig(
            fake_service_name,
            {
                'name': fake_service_name,
                'description': fake_description,
                'command': fake_command,
                'schedule': fake_schedule,
                'owner': fake_owner,
            }
        )
        expected = chronos_tools.ChronosJobConfig({
            'name': fake_service_name,
            'description': fake_description,
            'command': fake_command,
            'schedule': fake_schedule,
            'epsilon': 'PT60S',
            'owner': fake_owner,
            'async': 'false',
            'cpus': 0.1,
            'mem': 128,
            'disk': 256,
            'retries': 2,
            'disabled': 'false',
        })
        actual = format_chronos_job_dict(incomplete_config, 'scheduled')
        assert sorted(actual) == sorted(expected)
