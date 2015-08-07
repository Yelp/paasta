import mock
from pytest import raises

import contextlib

import chronos_tools


class TestChronosTools:

    fake_service_name = 'test_service'
    fake_job_name = 'test'
    fake_cluster = 'penguin'
    fake_config_dict = {
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
        'docker_image': 'paasta-%s-%s' % (fake_service_name, fake_cluster),
    }
    fake_chronos_job_config = chronos_tools.ChronosJobConfig(fake_service_name,
                                                             fake_job_name,
                                                             fake_config_dict,
                                                             fake_branch_dict)

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
                                                                     fake_job_name,
                                                                     fake_invalid_config_dict,
                                                                     fake_branch_dict)
    fake_config_file = {
        fake_job_name: fake_config_dict,
        'bad_job': fake_invalid_config_dict,
    }

    def test_chronos_config_object_normal(self):
        fake_json_contents = {
            'user': 'fake_user',
            'password': 'fake_password',
            'url': 'fake_host'
        }
        fake_config = chronos_tools.ChronosConfig(fake_json_contents, 'fake_path')
        assert fake_config.get_username() == 'fake_user'
        assert fake_config.get_password() == 'fake_password'
        assert fake_config.get_url() == 'fake_host'

    def test_chronos_config_object_no_user(self):
        fake_json_contents = {
            'password': 'fake_password',
        }
        fake_config = chronos_tools.ChronosConfig(fake_json_contents, 'fake_path')
        with raises(chronos_tools.ChronosNotConfigured):
            fake_config.get_username()

    def test_chronos_config_object_no_password(self):
        fake_json_contents = {
            'user': 'fake_user',
        }
        fake_config = chronos_tools.ChronosConfig(fake_json_contents, 'fake_path')
        with raises(chronos_tools.ChronosNotConfigured):
            fake_config.get_password()

    def test_chronos_config_object_no_url(self):
        fake_json_contents = {
            'user': 'fake_user',
        }
        fake_config = chronos_tools.ChronosConfig(fake_json_contents, 'fake_path')
        with raises(chronos_tools.ChronosNotConfigured):
            fake_config.get_url()

    def test_load_chronos_config_good(self):
        expected = {'foo': 'bar'}
        file_mock = mock.MagicMock(spec=file)
        with contextlib.nested(
            mock.patch('chronos_tools.open', create=True, return_value=file_mock),
            mock.patch('json.load', autospec=True, return_value=expected)
        ) as (
            open_file_patch,
            json_patch
        ):
            assert chronos_tools.load_chronos_config() == expected
            open_file_patch.assert_called_once_with('/etc/paasta/chronos.json')
            json_patch.assert_called_once_with(file_mock.__enter__())

    def test_load_chronos_config_bad(self):
        fake_path = '/dne'
        with contextlib.nested(
            mock.patch('chronos_tools.open', create=True, side_effect=IOError(2, 'a', 'b')),
        ) as (
            open_patch,
        ):
            with raises(chronos_tools.ChronosNotConfigured) as excinfo:
                chronos_tools.load_chronos_config(fake_path)
            assert str(excinfo.value) == "Could not load chronos config file b: a"

    def test_get_chronos_client(self):
        with contextlib.nested(
            mock.patch('chronos.connect', autospec=True),
        ) as (
            mock_connect,
        ):
            fake_config = chronos_tools.ChronosConfig(
                {'user': 'test', 'password': 'pass', 'url': ['some_fake_host']}, '/fake/path')
            chronos_tools.get_chronos_client(fake_config)
            assert mock_connect.call_count == 1

    def test_get_job_id(self):
        actual = chronos_tools.get_job_id('service', 'instance')
        assert actual == "service instance"

    def test_load_chronos_job_config(self):
        fake_soa_dir = '/tmp/'
        expected_chronos_conf_file = 'chronos-penguin'
        with contextlib.nested(
            mock.patch('chronos_tools.load_deployments_json', autospec=True,),
            mock.patch('service_configuration_lib.read_extra_service_information', autospec=True),
        ) as (
            mock_load_deployments_json,
            mock_read_extra_service_information,
        ):
            mock_load_deployments_json.return_value.get_branch_dict.return_value = self.fake_branch_dict
            mock_read_extra_service_information.return_value = self.fake_config_file
            actual = chronos_tools.load_chronos_job_config(self.fake_service_name,
                                                           self.fake_job_name,
                                                           self.fake_cluster,
                                                           fake_soa_dir)
            mock_load_deployments_json.assert_called_once_with(self.fake_service_name, soa_dir=fake_soa_dir)
            mock_read_extra_service_information.assert_called_once_with(self.fake_service_name,
                                                                        expected_chronos_conf_file,
                                                                        soa_dir=fake_soa_dir)
            assert actual == self.fake_chronos_job_config

    def test_load_chronos_job_config_unknown_job(self):
        fake_soa_dir = '/tmp/'
        expected_chronos_conf_file = 'chronos-penguin'
        fake_job_name = 'polar bear'
        with contextlib.nested(
            mock.patch('service_configuration_lib.read_extra_service_information', autospec=True),
        ) as (
            mock_read_extra_service_information,
        ):
            mock_read_extra_service_information.return_value = self.fake_config_file
            with raises(chronos_tools.InvalidChronosConfigError) as exc:
                chronos_tools.load_chronos_job_config(self.fake_service_name,
                                                      fake_job_name,
                                                      self.fake_cluster,
                                                      fake_soa_dir)
                mock_read_extra_service_information.assert_called_once_with(self.fake_service_name,
                                                                            expected_chronos_conf_file,
                                                                            soa_dir=fake_soa_dir)
                assert exc.value == 'No job named "polar bear" in config file chronos-penguin.yaml'

    def test_get_config_dict_param(self):
        param = 'epsilon'
        expected = 'PT30M'
        actual = self.fake_chronos_job_config.get(param)
        assert actual == expected

    def test_get_branch_dict_param(self):
        param = 'docker_image'
        expected = self.fake_branch_dict['docker_image']
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
        okay, msg = self.fake_chronos_job_config.check_epsilon()
        assert okay is True
        assert msg == ''

    def test_check_epsilon_invalid(self):
        okay, msg = self.fake_invalid_chronos_job_config.check_epsilon()
        assert okay is False
        assert msg == 'The specified epsilon value "nolispe" does not conform to the ISO8601 format.'

    def test_check_retries_valid(self):
        okay, msg = self.fake_chronos_job_config.check_retries()
        assert okay is True
        assert msg == ''

    def test_check_retries_invalid(self):
        okay, msg = self.fake_invalid_chronos_job_config.check_retries()
        assert okay is False
        assert msg == 'The specified retries value "5.7" is not a valid int.'

    def test_check_async_valid(self):
        okay, msg = self.fake_chronos_job_config.check_async()
        assert okay is True
        assert msg == ''

    def test_check_async_invalid(self):
        okay, msg = self.fake_invalid_chronos_job_config.check_async()
        assert okay is False
        assert msg == 'The config specifies that the job is async, which we don\'t support.'

    def test_check_cpus_valid(self):
        okay, msg = self.fake_chronos_job_config.check_cpus()
        assert okay is True
        assert msg == ''

    def test_check_cpus_invalid(self):
        okay, msg = self.fake_invalid_chronos_job_config.check_cpus()
        assert okay is False
        assert msg == 'The specified cpus value "intel" is not a valid float.'

    def test_check_mem_valid(self):
        okay, msg = self.fake_chronos_job_config.check_mem()
        assert okay is True
        assert msg == ''

    def test_check_mem_invalid(self):
        okay, msg = self.fake_invalid_chronos_job_config.check_mem()
        assert okay is False
        assert msg == 'The specified mem value "lots" is not a valid float.'

    def test_check_disk_valid(self):
        okay, msg = self.fake_chronos_job_config.check_disk()
        assert okay is True
        assert msg == ''

    def test_check_disk_invalid(self):
        okay, msg = self.fake_invalid_chronos_job_config.check_disk()
        assert okay is False
        assert msg == 'The specified disk value "all of it" is not a valid float.'

    def test_check_schedule_repeat_helper_valid(self):
        assert self.fake_invalid_chronos_job_config._check_schedule_repeat_helper('R32') is True

    def test_check_schedule_repeat_helper_valid_infinite(self):
        assert self.fake_invalid_chronos_job_config._check_schedule_repeat_helper('R') is True

    def test_check_schedule_repeat_helper_invalid_empty(self):
        assert self.fake_invalid_chronos_job_config._check_schedule_repeat_helper('') is False

    def test_check_schedule_repeat_helper_invalid_no_r(self):
        assert self.fake_invalid_chronos_job_config._check_schedule_repeat_helper('32') is False

    def test_check_schedule_repeat_helper_invalid_float(self):
        assert self.fake_invalid_chronos_job_config._check_schedule_repeat_helper('R6.9') is False

    def test_check_schedule_repeat_helper_invalid_negative(self):
        assert self.fake_invalid_chronos_job_config._check_schedule_repeat_helper('R-8') is False

    def test_check_schedule_repeat_helper_invalid_other_text(self):
        assert self.fake_invalid_chronos_job_config._check_schedule_repeat_helper('BR72') is False

    def test_check_schedule_valid_complete(self):
        okay, msg = self.fake_chronos_job_config.check_schedule()
        assert okay is True
        assert msg == ''

    def test_check_schedule_valid_empty_start_time(self):
        fake_schedule = 'R10//PT2S'
        chronos_config = chronos_tools.ChronosJobConfig('', '', {'schedule': fake_schedule}, {})
        okay, msg = chronos_config.check_schedule()
        assert okay is True
        assert msg == ''

    def test_check_schedule_invalid_start_time_no_t_designator(self):
        fake_start_time = 'now'
        fake_schedule = 'R10/%s/PT2S' % fake_start_time
        chronos_config = chronos_tools.ChronosJobConfig('', '', {'schedule': fake_schedule}, {})
        fake_isodate_exception = 'ISO 8601 time designator \'T\' missing. Unable to parse datetime string \'now\''
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified start time "%s" in schedule "%s" does not conform to the ISO 8601 format:\n%s'
                       % (fake_start_time, fake_schedule, fake_isodate_exception))

    def test_check_schedule_invalid_start_time_bad_date(self):
        fake_start_time = 'todayT19:20:30Z'
        fake_schedule = 'R10/%s/PT2S' % fake_start_time
        chronos_config = chronos_tools.ChronosJobConfig('', '', {'schedule': fake_schedule}, {})
        fake_isodate_exception = 'Unrecognised ISO 8601 date format: \'today\''
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified start time "%s" in schedule "%s" does not conform to the ISO 8601 format:\n%s'
                       % (fake_start_time, fake_schedule, fake_isodate_exception))

    def test_check_schedule_invalid_start_time_bad_time(self):
        fake_start_time = '1994-02-18Tmorning'
        fake_schedule = 'R10/%s/PT2S' % fake_start_time
        chronos_config = chronos_tools.ChronosJobConfig('', '', {'schedule': fake_schedule}, {})
        fake_isodate_exception = 'Unrecognised ISO 8601 time format: \'morning\''
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified start time "%s" in schedule "%s" does not conform to the ISO 8601 format:\n%s'
                       % (fake_start_time, fake_schedule, fake_isodate_exception))

    def test_check_schedule_invalid_empty_interval(self):
        fake_schedule = 'R10//'
        chronos_config = chronos_tools.ChronosJobConfig('', '', {'schedule': fake_schedule}, {})
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified interval "" in schedule "%s" does not conform to the ISO 8601 format.'
                       % fake_schedule)

    def test_check_schedule_invalid_interval(self):
        fake_schedule = 'R10//Mondays'
        chronos_config = chronos_tools.ChronosJobConfig('', '', {'schedule': fake_schedule}, {})
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified interval "Mondays" in schedule "%s" does not conform to the ISO 8601 format.'
                       % fake_schedule)

    def test_check_schedule_invalid_empty_repeat(self):
        fake_schedule = '//PT2S'
        chronos_config = chronos_tools.ChronosJobConfig('', '', {'schedule': fake_schedule}, {})
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified repeat "" in schedule "%s" does not conform to the ISO 8601 format.'
                       % fake_schedule)

    def test_check_schedule_invalid_repeat(self):
        fake_schedule = 'forever//PT2S'
        chronos_config = chronos_tools.ChronosJobConfig('', '', {'schedule': fake_schedule}, {})
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified repeat "forever" in schedule "%s" does not conform to the ISO 8601 format.'
                       % fake_schedule)

    def test_check_schedule_time_zone_valid(self):
        okay, msg = self.fake_chronos_job_config.check_schedule_time_zone()
        assert okay is True
        assert msg == ''

    def test_check_schedule_time_zone_valid_empty(self):
        chronos_config = chronos_tools.ChronosJobConfig('', '', {'schedule_time_zone': ''}, {})
        okay, msg = chronos_config.check_schedule_time_zone()
        assert okay is True
        assert msg == ''

    def test_check_schedule_time_zone_invalid(self):
        okay, msg = self.fake_invalid_chronos_job_config.check_schedule_time_zone()
        assert okay is True  # FIXME implement the validator
        assert msg == ''  # FIXME implement the validator
        # assert okay is False
        # assert msg == 'The specified time zone "+0200" does not conform to the tz database format.'

    def test_check_param_with_check(self):
        with contextlib.nested(
            mock.patch('chronos_tools.ChronosJobConfig.check_cpus', autospec=True),
        ) as (
            mock_check_cpus,
        ):
            mock_check_cpus.return_value = True, ''
            param = 'cpus'
            okay, msg = self.fake_chronos_job_config.check(param)
            assert mock_check_cpus.call_count == 1
            assert okay is True
            assert msg == ''

    def test_check_param_without_check(self):
        param = 'owner'
        okay, msg = self.fake_chronos_job_config.check(param)
        assert okay is True
        assert msg == ''

    def test_check_unknown_param(self):
        param = 'boat'
        okay, msg = self.fake_chronos_job_config.check(param)
        assert okay is False
        assert msg == 'Your Chronos config specifies "boat", an unsupported parameter.'

    def test_set_missing_params_to_defaults(self):
        chronos_config_defaults = {
            # 'shell': 'true',  # we don't support this param, but it does have a default specified by the Chronos docs
            'epsilon': 'PT60S',
            'retries': 2,
            'async': False,  # we don't support this param, but it does have a default specified by the Chronos docs
            'cpus': 0.1,
            'mem': 128,
            'disk': 256,
            'disabled': False,
            # 'data_job': False, # we don't support this param, but it does have a default specified by the Chronos docs
        }
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', '', {}, {})
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
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', '', chronos_config_dict, {})

        completed_chronos_job_config = chronos_tools.set_missing_params_to_defaults(fake_chronos_job_config)
        for param in chronos_config_dict:
            assert completed_chronos_job_config.get(param) == chronos_config_dict[param]

    def test_check_job_reqs_scheduled_complete(self):
        okay, msgs = chronos_tools.check_job_reqs(self.fake_chronos_job_config)
        assert okay is True
        assert len(msgs) == 0

    def test_check_job_reqs_scheduled_incomplete(self):
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', '', {}, {})
        okay, msgs = chronos_tools.check_job_reqs(fake_chronos_job_config)
        assert okay is False
        assert 'Your Chronos config is missing "owner", which is a required parameter.' in msgs
        assert 'Your Chronos config contains neither "schedule" nor "parents".' in msgs

    def test_check_job_reqs_dependent_complete(self):
        fake_config_dict = {
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
        fake_chronos_job_config = chronos_tools.ChronosJobConfig(self.fake_service_name,
                                                                 self.fake_job_name,
                                                                 fake_config_dict,
                                                                 {})
        okay, msgs = chronos_tools.check_job_reqs(fake_chronos_job_config)
        assert okay is True
        assert len(msgs) == 0

    def test_check_job_reqs_dependent_incomplete(self):
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', '', {}, {})
        okay, msgs = chronos_tools.check_job_reqs(fake_chronos_job_config)
        assert okay is False
        assert 'Your Chronos config is missing "owner", which is a required parameter.' in msgs
        assert 'Your Chronos config contains neither "schedule" nor "parents".' in msgs

    def test_check_job_reqs_docker_complete(self):
        fake_config_dict = {
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
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', self.fake_job_name, fake_config_dict, {})
        okay, msgs = chronos_tools.check_job_reqs(fake_chronos_job_config)
        assert okay is True
        assert len(msgs) == 0

    def test_check_job_reqs_docker_incomplete(self):
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', '', {}, {})
        okay, msgs = chronos_tools.check_job_reqs(fake_chronos_job_config)
        assert okay is False
        assert 'Your Chronos config is missing "owner", which is a required parameter.' in msgs

    def test_check_job_reqs_docker_invalid_neither_schedule_nor_parents(self):
        fake_config_dict = {
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
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', self.fake_job_name, fake_config_dict, {})
        okay, msgs = chronos_tools.check_job_reqs(fake_chronos_job_config)
        assert okay is False
        assert ('Your Chronos config contains neither "schedule" nor "parents".') in msgs

    def test_check_job_reqs_docker_invalid_both_schedule_and_parents(self):
        fake_config_dict = {
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
        fake_chronos_job_config = chronos_tools.ChronosJobConfig('', self.fake_job_name, fake_config_dict, {})
        okay, msgs = chronos_tools.check_job_reqs(fake_chronos_job_config)
        assert okay is False
        assert ('Your Chronos config contains both "schedule" and "parents".') in msgs

    def test_format_chronos_job_dict(self):
        fake_service_name = 'test_service'
        fake_job_name = 'test_job'
        fake_description = 'this service is just a test'
        fake_command = 'echo foo >> /tmp/test_service_log'
        fake_schedule = 'R10/2012-10-01T05:52:00Z/PT1M'
        fake_owner = 'bob@example.com'
        fake_docker_url = 'fake_docker_image_url'
        fake_docker_volumes = ['fake_docker_volume']

        incomplete_config = chronos_tools.ChronosJobConfig(
            fake_service_name,
            fake_job_name,
            {
                'description': fake_description,
                'command': fake_command,
                'schedule': fake_schedule,
                'owner': fake_owner,
            },
            {}
        )
        expected = {
            'name': fake_job_name,
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
            'container': {
                'docker': {'image': 'fake_docker_image_url'},
                'type': 'DOCKER',
                'volumes': ['fake_docker_volume']
            }
        }
        actual = chronos_tools.format_chronos_job_dict(incomplete_config, fake_docker_url, fake_docker_volumes)
        assert sorted(actual) == sorted(expected)

    def test_format_chronos_job_dict_invalid_param(self):
        fake_service_name = 'test_service'
        fake_job_name = 'test_job'
        fake_description = 'this service is just a test'
        fake_command = 'echo foo >> /tmp/test_service_log'
        fake_schedule = 'R10/2012-10-01T05:52:00Z/PT1M'
        fake_owner = 'bob@example.com'
        incomplete_config = chronos_tools.ChronosJobConfig(
            fake_service_name,
            fake_job_name,
            {
                'description': fake_description,
                'command': fake_command,
                'schedule': fake_schedule,
                'owner': fake_owner,
                'ship': 'Titanic',
            },
            {}
        )
        with raises(chronos_tools.InvalidChronosConfigError) as exc:
            chronos_tools.format_chronos_job_dict(incomplete_config, '', [])
            assert exc.value == 'Your Chronos config specifies "ship", an unsupported parameter.'

    def test_format_chronos_job_dict_incomplete(self):
        fake_service_name = 'test_service'
        fake_job_name = 'test_job'
        fake_description = 'this service is just a test'
        fake_schedule = 'R10/2012-10-01T05:52:00Z/PT1M'
        fake_owner = 'bob@example.com'
        incomplete_config = chronos_tools.ChronosJobConfig(
            fake_service_name,
            fake_job_name,
            {
                'description': fake_description,
                'schedule': fake_schedule,
                'owner': fake_owner,
                'container': {},
            },
            {}
        )
        with raises(chronos_tools.InvalidChronosConfigError) as exc:
            chronos_tools.format_chronos_job_dict(incomplete_config, '', [])
            assert exc.value == 'Your Chronos config is missing "command", a required parameter.'

    def test_get_service_job_list(self):
        fake_name = 'vegetables'
        fake_job_1 = 'carrot'
        fake_job_2 = 'celery'
        fake_cluster = 'broccoli'
        fake_dir = '/nail/home/veggies'
        fake_job_config = {fake_job_1: self.fake_config_dict,
                           fake_job_2: self.fake_config_dict}
        expected = [(fake_name, fake_job_1), (fake_name, fake_job_2)]
        with mock.patch('service_configuration_lib.read_extra_service_information', autospec=True,
                        return_value=fake_job_config) as read_extra_info_patch:
            actual = chronos_tools.get_service_job_list(fake_name, fake_cluster, fake_dir)
            read_extra_info_patch.assert_called_once_with(fake_name, "chronos-broccoli", soa_dir=fake_dir)
            assert sorted(expected) == sorted(actual)

    def test_get_chronos_jobs_for_cluster(self):
        cluster = 'mainframe_42'
        soa_dir = 'my_computer'
        jobs = [['boop', 'beep'], ['bop']]
        expected = ['beep', 'bop', 'boop']
        with contextlib.nested(
            mock.patch('os.path.abspath', autospec=True, return_value='windows_explorer'),
            mock.patch('os.listdir', autospec=True, return_value=['dir1', 'dir2']),
            mock.patch('chronos_tools.get_service_job_list',
                       side_effect=lambda a, b, c: jobs.pop())
        ) as (
            abspath_patch,
            listdir_patch,
            get_jobs_patch,
        ):
            actual = chronos_tools.get_chronos_jobs_for_cluster(cluster, soa_dir)
            assert sorted(expected) == sorted(actual)
            abspath_patch.assert_called_once_with(soa_dir)
            listdir_patch.assert_called_once_with('windows_explorer')
            get_jobs_patch.assert_any_call('dir1', cluster, soa_dir)
            get_jobs_patch.assert_any_call('dir2', cluster, soa_dir)
            assert get_jobs_patch.call_count == 2
