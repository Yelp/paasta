import contextlib

import mock
from pytest import raises

import chronos_tools


class TestChronosTools:

    fake_service_name = 'test_service'
    fake_cluster = 'penguin'
    fake_config_dict = {
        'name': 'test',
        'description': 'This is a test Chronos job.',
        'command': '/bin/sleep 40',
        'epsilon': 'PT30M',
        'retries': 5,
        'owner': 'test@test.com',
        'async': False,
        'cpus': 5.5,
        'mem': 1024.4,
        'disk': 2048.5,
        'disabled': 'true',
        'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
        'schedule_time_zone': 'Zulu',
    }
    fake_branch_dict = {
        'full_branch': 'paasta-%s-%s' % (fake_service_name, fake_cluster),
    }
    fake_chronos_job_config = chronos_tools.ChronosJobConfig(fake_service_name, fake_config_dict, fake_branch_dict)

    fake_invalid_config_dict = {
        'epsilon': 'nolispe',
        'retries': 5.7,
        'async': True,
        'cpus': 'intel',
        'mem': 'lots',
        'disk': 'all of it',
        'schedule': 'forever/now/5 min',
        'schedule_time_zone': '+0200',
    }
    fake_invalid_chronos_job_config = chronos_tools.ChronosJobConfig(fake_service_name,
                                                                     fake_invalid_config_dict,
                                                                     fake_branch_dict)

    def test_load_chronos_job_config(self):
        fake_soa_dir = '/tmp/'
        expected_chronos_conf_file = 'chronos-penguin'
        with contextlib.nested(
            mock.patch('service_configuration_lib.read_extra_service_information', autospec=True),
        ) as (
            mock_read_extra_service_information,
        ):
            mock_read_extra_service_information.return_value = self.fake_config_dict
            actual = chronos_tools.load_chronos_job_config(self.fake_service_name, self.fake_cluster, fake_soa_dir)
            mock_read_extra_service_information.assert_called_once_with(self.fake_service_name,
                                                                        expected_chronos_conf_file,
                                                                        soa_dir=fake_soa_dir)
            assert actual == self.fake_chronos_job_config

    def test_get_config_dict_param(self):
        param = 'epsilon'
        expected = 'PT30M'
        actual = self.fake_chronos_job_config.get(param)
        assert actual == expected

    def test_get_branch_dict_param(self):
        param = 'full_branch'
        expected = 'paasta-test_service-penguin'
        actual = self.fake_chronos_job_config.get(param)
        assert actual == expected

    def test_get_service_name(self):
        param = 'service_name'
        expected = 'test_service'
        actual = self.fake_chronos_job_config.get(param)
        assert actual == expected

    def test_get_unknown_param(self):
        param = 'mainframe'
        actual = self.fake_chronos_job_config.get(param)
        assert actual is None

    def test_check_epsilon_valid(self):
        status, msgs = chronos_tools.check_epsilon(self.fake_chronos_job_config)
        assert status is True
        assert len(msgs) == 0

    def test_check_epsilon_invalid(self):
        status, msgs = chronos_tools.check_epsilon(self.fake_invalid_chronos_job_config)
        assert status is False
        assert 'The specified epsilon value \'nolispe\' does not conform to the ISO8601 format.' in msgs

    def test_check_retries_valid(self):
        status, msgs = chronos_tools.check_retries(self.fake_chronos_job_config)
        assert status is True
        assert len(msgs) == 0

    def test_check_retries_invalid(self):
        status, msgs = chronos_tools.check_retries(self.fake_invalid_chronos_job_config)
        assert status is False
        assert 'The specified retries value \'5.7\' is not a valid int.' in msgs

    def test_check_async_valid(self):
        status, msgs = chronos_tools.check_async(self.fake_chronos_job_config)
        assert status is True
        assert len(msgs) == 0

    def test_check_async_invalid(self):
        status, msgs = chronos_tools.check_async(self.fake_invalid_chronos_job_config)
        assert status is False
        assert 'The config specifies that the job is async, which we don\'t support.' in msgs

    def test_check_cpus_valid(self):
        status, msgs = chronos_tools.check_cpus(self.fake_chronos_job_config)
        assert status is True
        assert len(msgs) == 0

    def test_check_cpus_invalid(self):
        status, msgs = chronos_tools.check_cpus(self.fake_invalid_chronos_job_config)
        assert status is False
        assert 'The specified cpus value \'intel\' is not a valid float.' in msgs

    def test_check_mem_valid(self):
        status, msgs = chronos_tools.check_mem(self.fake_chronos_job_config)
        assert status is True
        assert len(msgs) == 0

    def test_check_mem_invalid(self):
        status, msgs = chronos_tools.check_mem(self.fake_invalid_chronos_job_config)
        assert status is False
        assert 'The specified mem value \'lots\' is not a valid float.' in msgs

    def test_check_disk_valid(self):
        status, msgs = chronos_tools.check_disk(self.fake_chronos_job_config)
        assert status is True
        assert len(msgs) == 0

    def test_check_disk_invalid(self):
        status, msgs = chronos_tools.check_disk(self.fake_invalid_chronos_job_config)
        assert status is False
        assert 'The specified disk value \'all of it\' is not a valid float.' in msgs

    def test_validate_repeat_valid(self):
        assert True  # TODO test this once regex matching implemented

    def test_validate_repeat_invalid(self):
        assert True  # TODO test this once regex matching implemented

    def test_check_schedule_valid_complete(self):
        status, msgs = chronos_tools.check_schedule(self.fake_chronos_job_config)
        assert status is True
        assert len(msgs) == 0

    def test_check_schedule_valid_empty_start_time(self):
        fake_schedule = 'R10//PT2S'
        chronos_config = chronos_tools.ChronosJobConfig('', {'schedule': fake_schedule}, {})
        status, msgs = chronos_tools.check_schedule(chronos_config)
        assert status is True
        assert len(msgs) == 0

    def test_check_schedule_invalid_start_time(self):
        fake_schedule = 'R10/now/PT2S'
        chronos_config = chronos_tools.ChronosJobConfig('', {'schedule': fake_schedule}, {})
        status, msgs = chronos_tools.check_schedule(chronos_config)
        assert status is False
        assert ('The specified start time \'now\' in schedule \'%s\' does not conform to the ISO 8601 format.'
                % fake_schedule) in msgs

    def test_check_schedule_invalid_empty_interval(self):
        fake_schedule = 'R10//'
        chronos_config = chronos_tools.ChronosJobConfig('', {'schedule': fake_schedule}, {})
        status, msgs = chronos_tools.check_schedule(chronos_config)
        assert status is False
        assert ('The specified interval \'\' in schedule \'%s\' does not conform to the ISO 8601 format.'
                % fake_schedule) in msgs

    def test_check_schedule_invalid_interval(self):
        fake_schedule = 'R10//Mondays'
        chronos_config = chronos_tools.ChronosJobConfig('', {'schedule': fake_schedule}, {})
        status, msgs = chronos_tools.check_schedule(chronos_config)
        assert status is False
        assert ('The specified interval \'Mondays\' in schedule \'%s\' does not conform to the ISO 8601 format.'
                % fake_schedule) in msgs

    def test_check_schedule_invalid_empty_repeat(self):
        fake_schedule = '//PT2S'
        chronos_config = chronos_tools.ChronosJobConfig('', {'schedule': fake_schedule}, {})
        status, msgs = chronos_tools.check_schedule(chronos_config)
        assert status is False
        assert ('The specified repeat \'\' in schedule \'%s\' does not conform to the ISO 8601 format.'
                % fake_schedule) in msgs

    def test_check_schedule_invalid_repeat(self):
        fake_schedule = 'forever//PT2S'
        chronos_config = chronos_tools.ChronosJobConfig('', {'schedule': fake_schedule}, {})
        status, msgs = chronos_tools.check_schedule(chronos_config)
        assert status is True  # FIXME implement the validator
        assert len(msgs) == 0  # FIXME implement the validator
        # assert status is False
        # assert ('The specified repeat \'forever\' in schedule \'%s\' does not conform to the ISO 8601 format.'
        #         % fake_schedule) in msgs

    def test_check_schedule_time_zone_valid(self):
        status, msgs = chronos_tools.check_schedule_time_zone(self.fake_chronos_job_config)
        assert status is True
        assert len(msgs) == 0

    def test_check_schedule_time_zone_valid_empty(self):
        chronos_config = chronos_tools.ChronosJobConfig('', {'schedule_time_zone': ''}, {})
        status, msgs = chronos_tools.check_schedule_time_zone(chronos_config)
        assert status is True
        assert len(msgs) == 0

    def test_check_schedule_time_zone_invalid(self):
        status, msgs = chronos_tools.check_schedule_time_zone(self.fake_invalid_chronos_job_config)
        assert status is True  # FIXME implement the validator
        assert len(msgs) == 0  # FIXME implement the validator
        # assert status is False
        # assert 'The specified time zone \'+0200\' does not conform to the tz database format.' in msgs

    def test_check_param_with_check(self):
        with contextlib.nested(
            mock.patch('chronos_tools.check_cpus', autospec=True),
        ) as (
            mock_check_cpus,
        ):
            mock_check_cpus.return_value = True, ''
            param = 'cpus'
            status, msgs = chronos_tools.check(self.fake_chronos_job_config, param)
            assert mock_check_cpus.call_count == 1
            assert status is True
            assert len(msgs) == 0

    def test_check_param_without_check(self):
        param = 'name'
        status, msgs = chronos_tools.check(self.fake_chronos_job_config, param)
        assert status is True
        assert len(msgs) == 0

    def test_check_unknown_param(self):
        param = 'boat'
        status, msgs = chronos_tools.check(self.fake_chronos_job_config, param)
        assert status is False
        assert 'Your Chronos config specifies \'boat\', an unsupported parameter.' in msgs

    def test_set_missing_params_to_defaults(self):
        chronos_config_defaults = {
            # 'shell': 'true',  # we don't support this param, but it does have a default specified by the Chronos docs
            'epsilon': 'PT60S',
            'retries': 2,
            # 'async': False,  # we don't support this param, but it does have a default specified by the Chronos docs
            'cpus': 0.1,
            'mem': 128,
            'disk': 256,
            'disabled': False,
            # 'data_job': False, # we don't support this param, but it does have a default specified by the Chronos docs
        }
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', {}, {})
        completed_chronos_job_config = chronos_tools.set_missing_params_to_defaults(fake_chronos_job_config)
        for param in chronos_config_defaults:
            assert completed_chronos_job_config.get(param) == chronos_config_defaults[param]

    def test_set_missing_params_to_defaults_no_missing_params(self):
        chronos_config_dict = {
            'epsilon': 'PT5M',
            'retries': 5,
            'cpus': 7.2,
            'mem': 9001,
            'disk': 8,
            'disabled': True,
        }
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', chronos_config_dict, {})

        completed_chronos_job_config = chronos_tools.set_missing_params_to_defaults(fake_chronos_job_config)
        for param in chronos_config_dict:
            assert completed_chronos_job_config.get(param) == chronos_config_dict[param]

    def test_check_job_reqs(self):
        with contextlib.nested(
            mock.patch('chronos_tools.check_scheduled_job_reqs', autospec=True),
        ) as (
            mock_check_scheduled_job_reqs,
        ):
            mock_check_scheduled_job_reqs.return_value = True, ''
            job_type = 'scheduled'
            status, msgs = chronos_tools.check_job_reqs(self.fake_chronos_job_config, job_type)
            assert mock_check_scheduled_job_reqs.call_count == 1
            assert status is True
            assert len(msgs) == 0

    def test_check_scheduled_job_reqs_complete(self):
        status, msgs = chronos_tools.check_scheduled_job_reqs(self.fake_chronos_job_config)
        assert status is True
        assert len(msgs) == 0

    def test_check_scheduled_job_reqs_incomplete(self):
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', {}, {})
        status, msgs = chronos_tools.check_scheduled_job_reqs(fake_chronos_job_config)
        assert status is False
        assert 'Your Chronos config is missing \'name\', a required parameter for a \'scheduled job\'.' in msgs

    def test_check_dependent_job_reqs_complete(self):
        fake_config_dict = {
            'name': 'test',
            'description': 'This is a test Chronos job.',
            'command': '/bin/sleep 40',
            'epsilon': 'PT30M',
            'retries': 5,
            'owner': 'test@test.com',
            'async': False,
            'cpus': 5.5,
            'mem': 1024.4,
            'disk': 2048.5,
            'disabled': 'true',
            'parents': ['jack', 'jill'],
        }
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', fake_config_dict, {})
        status, msgs = chronos_tools.check_dependent_job_reqs(fake_chronos_job_config)
        assert status is True
        assert len(msgs) == 0

    def test_check_dependent_job_reqs_incomplete(self):
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', {}, {})
        status, msgs = chronos_tools.check_dependent_job_reqs(fake_chronos_job_config)
        assert status is False
        assert 'Your Chronos config is missing \'name\', a required parameter for a \'dependent job\'.' in msgs

    def test_check_docker_job_reqs_complete(self):
        fake_config_dict = {
            'name': 'test',
            'description': 'This is a test Chronos job.',
            'command': '/bin/sleep 40',
            'epsilon': 'PT30M',
            'retries': 5,
            'owner': 'test@test.com',
            'async': False,
            'cpus': 5.5,
            'mem': 1024.4,
            'disk': 2048.5,
            'disabled': 'true',
            'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
            'schedule_time_zone': '',
            'container': {
                'type': 'DOCKER',
                'image': 'libmesos/ubuntu',
                'network': 'BRIDGE',
                'volumes': [{'containerPath': '/var/log/', 'hostPath': '/logs/', 'mode': 'RW'}]
            },
        }
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', fake_config_dict, {})
        status, msgs = chronos_tools.check_docker_job_reqs(fake_chronos_job_config)
        assert status is True
        assert len(msgs) == 0

    def test_check_docker_job_reqs_incomplete(self):
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', {}, {})
        status, msgs = chronos_tools.check_docker_job_reqs(fake_chronos_job_config)
        assert status is False
        assert 'Your Chronos config is missing \'name\', a required parameter for a \'Docker job\'.' in msgs

    def test_check_docker_job_reqs_invalid_neither_schedule_nor_parents(self):
        fake_config_dict = {
            'name': 'test',
            'description': 'This is a test Chronos job.',
            'command': '/bin/sleep 40',
            'epsilon': 'PT30M',
            'retries': 5,
            'owner': 'test@test.com',
            'async': False,
            'cpus': 5.5,
            'mem': 1024.4,
            'disk': 2048.5,
            'disabled': 'true',
            'container': {
                'type': 'DOCKER',
                'image': 'libmesos/ubuntu',
                'network': 'BRIDGE',
                'volumes': [{'containerPath': '/var/log/', 'hostPath': '/logs/', 'mode': 'RW'}]
            },
        }
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', fake_config_dict, {})
        status, msgs = chronos_tools.check_docker_job_reqs(fake_chronos_job_config)
        assert status is False
        assert 'Your Chronos config contains neither a schedule nor parents. One is required.' in msgs

    def test_check_docker_job_reqs_invalid_both_schedule_and_parents(self):
        fake_config_dict = {
            'name': 'test',
            'description': 'This is a test Chronos job.',
            'command': '/bin/sleep 40',
            'epsilon': 'PT30M',
            'retries': 5,
            'owner': 'test@test.com',
            'async': False,
            'cpus': 5.5,
            'mem': 1024.4,
            'disk': 2048.5,
            'disabled': 'true',
            'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
            'schedule_time_zone': '',
            'parents': ['jack', 'jill'],
            'container': {
                'type': 'DOCKER',
                'image': 'libmesos/ubuntu',
                'network': 'BRIDGE',
                'volumes': [{'containerPath': '/var/log/', 'hostPath': '/logs/', 'mode': 'RW'}]
            },
        }
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', fake_config_dict, {})
        status, msgs = chronos_tools.check_docker_job_reqs(fake_chronos_job_config)
        assert status is False
        assert 'Your Chronos config contains both schedule and parents. Only one is allowed.' in msgs

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
            },
            {}
        )
        expected = {
            'name': fake_service_name,
            'description': fake_description,
            'command': fake_command,
            'schedule': fake_schedule,
            'epsilon': 'PT60S',
            'owner': fake_owner,
            'async': False,
            'cpus': 0.1,
            'mem': 128,
            'disk': 256,
            'retries': 2,
            'disabled': False,
        }
        actual = chronos_tools.format_chronos_job_dict(incomplete_config, 'scheduled')
        assert actual == expected

    def test_format_chronos_job_dict_invalid_param(self):
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
                'ship': 'Titanic',
            },
            {}
        )
        with raises(chronos_tools.InvalidChronosConfigError) as exc:
            chronos_tools.format_chronos_job_dict(incomplete_config, 'scheduled')
            assert exc.value == 'Your Chronos config specifies \'ship\', an unsupported parameter.'

    def test_format_chronos_job_dict_incomplete(self):
        fake_service_name = 'test_service'
        fake_description = 'this service is just a test'
        fake_command = 'echo foo >> /tmp/test_service_log'
        fake_schedule = 'R10/2012-10-01T05:52:00Z/PT1M'
        fake_owner = 'bob@example.com'
        incomplete_config = chronos_tools.ChronosJobConfig(
            fake_service_name,
            {
                'description': fake_description,
                'command': fake_command,
                'schedule': fake_schedule,
                'owner': fake_owner,
            },
            {}
        )
        with raises(chronos_tools.InvalidChronosConfigError) as exc:
            chronos_tools.format_chronos_job_dict(incomplete_config, 'scheduled')
            assert exc.value == 'Your Chronos config is missing \'name\', a required parameter for a \'scheduled job\'.'
