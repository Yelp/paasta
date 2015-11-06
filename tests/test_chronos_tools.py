# Copyright 2015 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import contextlib
import copy
import datetime

import mock
from pytest import raises

import chronos_tools
from mock import Mock


class TestChronosTools:

    fake_service = 'test-service'
    fake_instance = 'fake-instance'
    fake_job_name = 'test'
    fake_cluster = 'penguin'
    fake_monitoring_info = {'fake_monitoring_info': 'fake_monitoring_value'}
    fake_config_dict = {
        'bounce_method': 'graceful',
        'cmd': '/bin/sleep 40',
        'epsilon': 'PT30M',
        'retries': 5,
        'cpus': 5.5,
        'mem': 1024.4,
        'disabled': True,
        'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
        'schedule_time_zone': 'Zulu',
        'monitoring': fake_monitoring_info,
    }
    fake_branch_dict = {
        'desired_state': 'start',
        'docker_image': 'paasta-%s-%s' % (fake_service, fake_cluster),
    }
    fake_chronos_job_config = chronos_tools.ChronosJobConfig(fake_service,
                                                             fake_job_name,
                                                             fake_config_dict,
                                                             fake_branch_dict)

    fake_invalid_config_dict = {
        'bounce_method': 'crossover',
        'epsilon': 'nolispe',
        'retries': 5.7,
        'async': True,
        'cpus': 'intel',
        'mem': 'lots',
        'disk': 'all of it',
        'schedule': 'forever/now/5 min',
        'schedule_time_zone': '+0200',
    }
    fake_invalid_chronos_job_config = chronos_tools.ChronosJobConfig(fake_service,
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

    def test_compose_job_id_without_hashes(self):
        actual = chronos_tools.compose_job_id('service', 'instance')
        assert actual == 'service instance'

    def test_compose_job_id_with_hashes(self):
        actual = chronos_tools.compose_job_id(
            'service',
            'instance',
            'gityourmom',
            'configyourdad',
        )
        assert actual == 'service instance gityourmom configyourdad'

    def test_decompose_job_id_without_hashes(self):
        actual = chronos_tools.decompose_job_id('service instance')
        assert actual == ('service', 'instance', None, None)

    def test_decompose_job_id_with_hashes(self):
        actual = chronos_tools.decompose_job_id('service instance gityourmom configyourdad')
        assert actual == ('service', 'instance', 'gityourmom', 'configyourdad')

    def test_read_chronos_jobs_for_service(self):
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
            actual = chronos_tools.read_chronos_jobs_for_service(self.fake_service,
                                                                 self.fake_cluster,
                                                                 fake_soa_dir)
            mock_read_extra_service_information.assert_called_once_with(self.fake_service,
                                                                        expected_chronos_conf_file,
                                                                        soa_dir=fake_soa_dir)
            assert actual == self.fake_config_file

    def test_load_chronos_job_config(self):
        fake_soa_dir = '/tmp/'
        with contextlib.nested(
            mock.patch('chronos_tools.load_deployments_json', autospec=True,),
            mock.patch('chronos_tools.read_chronos_jobs_for_service', autospec=True),
        ) as (
            mock_load_deployments_json,
            mock_read_chronos_jobs_for_service,
        ):
            mock_load_deployments_json.return_value.get_branch_dict.return_value = self.fake_branch_dict
            mock_read_chronos_jobs_for_service.return_value = self.fake_config_file
            actual = chronos_tools.load_chronos_job_config(service=self.fake_service,
                                                           instance=self.fake_job_name,
                                                           cluster=self.fake_cluster,
                                                           soa_dir=fake_soa_dir)
            mock_load_deployments_json.assert_called_once_with(self.fake_service, soa_dir=fake_soa_dir)
            mock_read_chronos_jobs_for_service.assert_called_once_with(self.fake_service,
                                                                       self.fake_cluster,
                                                                       soa_dir=fake_soa_dir)
            assert actual == self.fake_chronos_job_config

    def test_load_chronos_job_config_can_ignore_deployments(self):
        fake_soa_dir = '/tmp/'
        with contextlib.nested(
            mock.patch('chronos_tools.load_deployments_json', autospec=True,),
            mock.patch('chronos_tools.read_chronos_jobs_for_service', autospec=True),
        ) as (
            mock_load_deployments_json,
            mock_read_chronos_jobs_for_service,
        ):
            mock_read_chronos_jobs_for_service.return_value = self.fake_config_file
            actual = chronos_tools.load_chronos_job_config(service=self.fake_service,
                                                           instance=self.fake_job_name,
                                                           cluster=self.fake_cluster,
                                                           load_deployments=False,
                                                           soa_dir=fake_soa_dir)
            mock_read_chronos_jobs_for_service.assert_called_once_with(self.fake_service,
                                                                       self.fake_cluster,
                                                                       soa_dir=fake_soa_dir)
            assert not mock_load_deployments_json.called
            assert dict(actual) == dict(self.fake_chronos_job_config)

    def test_load_chronos_job_config_unknown_job(self):
        fake_soa_dir = '/tmp/'
        fake_job_name = 'polar bear'
        with contextlib.nested(
            mock.patch('chronos_tools.load_deployments_json', autospec=True,),
            mock.patch('chronos_tools.read_chronos_jobs_for_service', autospec=True),
        ) as (
            mock_load_deployments_json,
            mock_read_chronos_jobs_for_service,
        ):
            mock_load_deployments_json.return_value.get_branch_dict.return_value = self.fake_branch_dict
            mock_read_chronos_jobs_for_service.return_value = self.fake_config_file
            with raises(chronos_tools.InvalidChronosConfigError) as exc:
                chronos_tools.load_chronos_job_config(service=self.fake_service,
                                                      instance=fake_job_name,
                                                      cluster=self.fake_cluster,
                                                      soa_dir=fake_soa_dir)
            mock_read_chronos_jobs_for_service.assert_called_once_with(self.fake_service,
                                                                       self.fake_cluster,
                                                                       soa_dir=fake_soa_dir)
            assert str(exc.value) == 'No job named "polar bear" in config file chronos-penguin.yaml'

    def test_get_bounce_method_in_config(self):
        expected = self.fake_config_dict['bounce_method']
        actual = self.fake_chronos_job_config.get_bounce_method()
        assert actual == expected

    def test_get_bounce_method_default(self):
        fake_conf = chronos_tools.ChronosJobConfig('fake_name', 'fake_instance', {}, {})
        actual = fake_conf.get_bounce_method()
        assert actual == 'graceful'

    def test_get_cpus_in_config(self):
        expected = self.fake_monitoring_info
        actual = self.fake_chronos_job_config.get_monitoring()
        assert actual == expected

    def test_get_epsilon_default(self):
        fake_conf = chronos_tools.ChronosJobConfig('fake_name', 'fake_instance', {}, {})
        actual = fake_conf.get_epsilon()
        assert actual == 'PT60S'

    def test_get_epsilon(self):
        fake_epsilon = 'fake_epsilon'
        fake_conf = chronos_tools.ChronosJobConfig('fake_name', 'fake_instance', {'epsilon': fake_epsilon}, {})
        actual = fake_conf.get_epsilon()
        assert actual == fake_epsilon

    def test_get_docker_image(self):
        expected = self.fake_branch_dict['docker_image']
        actual = self.fake_chronos_job_config.get_docker_image()
        assert actual == expected

    def test_get_service(self):
        expected = 'test-service'
        actual = self.fake_chronos_job_config.get_service()
        assert actual == expected

    def test_get_cmd_uses_time_parser(self):
        fake_cmd = 'foo bar baz'
        fake_config_dict = {
            'bounce_method': 'graceful',
            'cmd': fake_cmd,
            'epsilon': 'PT30M',
            'retries': 5,
            'cpus': 5.5,
            'mem': 1024.4,
            'disabled': True,
            'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
            'schedule_time_zone': 'Zulu',
            'monitoring': {},
        }
        expected = 'parsed_time'
        with mock.patch(
            'chronos_tools.parse_time_variables', autospec=True, return_value=expected
                ) as mock_parse_time_variables:
            fake_chronos_job_config = chronos_tools.ChronosJobConfig(
                'fake_service', 'fake_job', fake_config_dict, {})
            actual = fake_chronos_job_config.get_cmd()
            mock_parse_time_variables.assert_called_once_with(fake_cmd)
        assert actual == expected

    def test_get_owner(self):
        fake_owner = 'fake_team'
        with mock.patch('monitoring_tools.get_team', autospec=True) as mock_get_team:
            mock_get_team.return_value = fake_owner
            actual = self.fake_chronos_job_config.get_owner()
            assert actual == fake_owner

    def test_get_shell_without_args_specified(self):
        fake_conf = chronos_tools.ChronosJobConfig('fake_name', 'fake_instance', {'args': ['a', 'b']}, {})
        actual = fake_conf.get_shell()
        assert actual is False

    def test_get_shell_when_args_present(self):
        fake_conf = chronos_tools.ChronosJobConfig('fake_name', 'fake_instance', {}, {})
        assert fake_conf.get_shell() is True

    def test_get_env(self):
        input_env = {'foo': 'bar', 'biz': 'baz'}
        expected_env = [
            {"name": "foo", "value": "bar"},
            {"name": "biz", "value": "baz"},
        ]
        fake_conf = chronos_tools.ChronosJobConfig('fake_name', 'fake_instance', {'env': input_env}, {})
        assert sorted(fake_conf.get_env()) == sorted(expected_env)

    def test_get_constraints(self):
        fake_constraints = 'fake_constraints'
        fake_conf = chronos_tools.ChronosJobConfig('fake_name', 'fake_instance', {'constraints': fake_constraints}, {})
        actual = fake_conf.get_constraints()
        assert actual == fake_constraints

    def test_get_retries_default(self):
        fake_conf = chronos_tools.ChronosJobConfig('fake_name', 'fake_instance', {}, {})
        actual = fake_conf.get_retries()
        assert actual == 2

    def test_get_retries(self):
        fake_retries = 'fake_retries'
        fake_conf = chronos_tools.ChronosJobConfig('fake_name', 'fake_instance', {'retries': fake_retries}, {})
        actual = fake_conf.get_retries()
        assert actual == fake_retries

    def test_get_disabled_default(self):
        fake_conf = chronos_tools.ChronosJobConfig('fake_name', 'fake_instance', {}, {})
        actual = fake_conf.get_disabled()
        assert not actual

    def test_get_disabled(self):
        fake_conf = chronos_tools.ChronosJobConfig('fake_name', 'fake_instance', {'disabled': True}, {})
        actual = fake_conf.get_disabled()
        assert actual

    def test_get_schedule(self):
        fake_schedule = 'fake_schedule'
        fake_conf = chronos_tools.ChronosJobConfig('fake_name', 'fake_instance', {'schedule': fake_schedule}, {})
        actual = fake_conf.get_schedule()
        assert actual == fake_schedule

    def test_get_schedule_time_zone(self):
        fake_schedule_time_zone = 'fake_schedule_time_zone'
        fake_conf = chronos_tools.ChronosJobConfig(
            'fake_name', 'fake_instance', {'schedule_time_zone': fake_schedule_time_zone}, {})
        actual = fake_conf.get_schedule_time_zone()
        assert actual == fake_schedule_time_zone

    def test_check_bounce_method_valid(self):
        okay, msg = self.fake_chronos_job_config.check_bounce_method()
        assert okay is True
        assert msg == ''

    def test_check_bounce_method_invalid(self):
        okay, msg = self.fake_invalid_chronos_job_config.check_bounce_method()
        assert okay is False
        assert msg.startswith('The specified bounce method "crossover" is invalid. It must be one of (')
        for bounce_method in chronos_tools.VALID_BOUNCE_METHODS:
            assert bounce_method in msg

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

    def test_check_schedule_invalid_empty_start_time(self):
        fake_schedule = 'R10//PT2S'
        chronos_config = chronos_tools.ChronosJobConfig('', '', {'schedule': fake_schedule}, {})
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == 'The specified schedule "%s" does not contain a start time' % fake_schedule

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
        fake_schedule = 'R10/2015-03-25T19:36:35Z/'
        chronos_config = chronos_tools.ChronosJobConfig('', '', {'schedule': fake_schedule}, {})
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified interval "" in schedule "%s" does not conform to the ISO 8601 format.'
                       % fake_schedule)

    def test_check_schedule_invalid_interval(self):
        fake_schedule = 'R10/2015-03-25T19:36:35Z/Mondays'
        chronos_config = chronos_tools.ChronosJobConfig('', '', {'schedule': fake_schedule}, {})
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified interval "Mondays" in schedule "%s" does not conform to the ISO 8601 format.'
                       % fake_schedule)

    def test_check_schedule_invalid_empty_repeat(self):
        fake_schedule = '/2015-03-25T19:36:35Z/PT2S'
        chronos_config = chronos_tools.ChronosJobConfig('', '', {'schedule': fake_schedule}, {})
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified repeat "" in schedule "%s" does not conform to the ISO 8601 format.'
                       % fake_schedule)

    def test_check_schedule_invalid_repeat(self):
        fake_schedule = 'forever/2015-03-25T19:36:35Z/PT2S'
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

    def test_format_chronos_job_dict(self):
        fake_service = 'test_service'
        fake_job_name = 'test_job'
        fake_owner = 'test_team'
        fake_command = 'echo foo >> /tmp/test_service_log'
        fake_schedule = 'R10/2012-10-01T05:52:00Z/PT1M'
        fake_epsilon = 'PT60S'
        fake_docker_url = 'fake_docker_image_url'
        fake_docker_volumes = ['fake_docker_volume']

        chronos_job_config = chronos_tools.ChronosJobConfig(
            fake_service,
            fake_job_name,
            {
                'cmd': fake_command,
                'schedule': fake_schedule,
                'epsilon': 'PT60S',
            },
            {}
        )
        expected = {
            'name': fake_job_name,
            'command': fake_command,
            'schedule': fake_schedule,
            'scheduleTimeZone': None,
            'environmentVariables': [],
            'arguments': None,
            'constraints': None,
            'retries': 2,
            'epsilon': fake_epsilon,
            'name': 'test_job',
            'cpus': 0.25,
            'async': False,
            'owner': fake_owner,
            'disabled': False,
            'mem': 1024,
            'container': {
                'network': 'BRIDGE',
                'volumes': fake_docker_volumes,
                'image': fake_docker_url,
                'type': 'DOCKER',
            },
            'uris': ['file:///root/.dockercfg', ],
            'shell': True,
        }
        with mock.patch('monitoring_tools.get_team', return_value=fake_owner):
            actual = chronos_job_config.format_chronos_job_dict(fake_docker_url, fake_docker_volumes)
            assert actual == expected

    def test_format_chronos_job_dict_invalid_param(self):
        fake_service = 'test_service'
        fake_job_name = 'test_job'
        fake_command = 'echo foo >> /tmp/test_service_log'
        fake_schedule = 'fake_bad_schedule'
        invalid_config = chronos_tools.ChronosJobConfig(
            fake_service,
            fake_job_name,
            {
                'cmd': fake_command,
                'schedule': fake_schedule,
                'epsilon': 'PT60S',
            },
            {}
        )
        with raises(chronos_tools.InvalidChronosConfigError) as exc:
            invalid_config.format_chronos_job_dict('', [])
        assert 'The specified schedule "%s" is invalid' % fake_schedule in exc.value

    def test_format_chronos_job_dict_incomplete(self):
        fake_service = 'test_service'
        fake_job_name = 'test_job'
        incomplete_config = chronos_tools.ChronosJobConfig(
            fake_service,
            fake_job_name,
            {},
            {}
        )
        with raises(chronos_tools.InvalidChronosConfigError) as exc:
            incomplete_config.format_chronos_job_dict('', [])
        assert 'You must specify a "schedule" in your configuration' in exc.value

    def test_validate(self):
        fake_service = 'test_service'
        fake_job_name = 'test_job'
        incomplete_config = chronos_tools.ChronosJobConfig(
            fake_service,
            fake_job_name,
            {},
            {}
        )
        valid, error_msgs = incomplete_config.validate()
        assert 'You must specify a "schedule" in your configuration' in error_msgs
        assert not valid

    def test_list_job_names(self):
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
            actual = chronos_tools.list_job_names(fake_name, fake_cluster, fake_dir)
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
            mock.patch('chronos_tools.list_job_names',
                       side_effect=lambda a, b, c: jobs.pop())
        ) as (
            abspath_patch,
            listdir_patch,
            list_jobs_patch,
        ):
            actual = chronos_tools.get_chronos_jobs_for_cluster(cluster, soa_dir)
            assert sorted(expected) == sorted(actual)
            abspath_patch.assert_called_once_with(soa_dir)
            listdir_patch.assert_called_once_with('windows_explorer')
            list_jobs_patch.assert_any_call('dir1', cluster, soa_dir)
            list_jobs_patch.assert_any_call('dir2', cluster, soa_dir)
            assert list_jobs_patch.call_count == 2

    def test_lookup_chronos_jobs_with_service_and_instance(self):
        fake_service = 'fake_service'
        fake_instance = 'fake_instance'
        fake_jobs = [
            {
                'name': chronos_tools.compose_job_id(
                    fake_service,
                    fake_instance,
                    'git1111',
                    'config1111',
                ),
                'disabled': False,
            },
            {
                'name': chronos_tools.compose_job_id(
                    fake_service,
                    fake_instance,
                    'git2222',
                    'config2222',
                ),
                'disabled': False,
            },
            {
                'name': chronos_tools.compose_job_id(
                    fake_service,
                    fake_instance,
                    'gitdisabled',
                    'configdisabled',
                ),
                'disabled': True,
            },
            {
                'name': chronos_tools.compose_job_id(
                    'some_other_service',
                    'some_other_instance',
                    'git3333',
                    'config3333',
                ),
                'disabled': False,
            },
        ]
        fake_client = mock.Mock(list=mock.Mock(return_value=fake_jobs))
        expected = [fake_jobs[0], fake_jobs[1]]
        actual = chronos_tools.lookup_chronos_jobs(
            service=fake_service,
            instance=fake_instance,
            client=fake_client,
        )
        assert sorted(actual) == sorted(expected)

    def test_lookup_chronos_jobs_include_disabled(self):
        fake_service = 'fake_service'
        fake_instance = 'fake_instance'
        fake_jobs = [
            {
                'name': chronos_tools.compose_job_id(
                    fake_service,
                    fake_instance,
                    'git1111',
                    'config1111',
                ),
                'disabled': False,
            },
            {
                'name': chronos_tools.compose_job_id(
                    fake_service,
                    fake_instance,
                    'git2222',
                    'config2222',
                ),
                'disabled': False,
            },
            {
                'name': chronos_tools.compose_job_id(
                    fake_service,
                    fake_instance,
                    'gitdisabled',
                    'configdisabled',
                ),
                'disabled': True,
            },
            {
                'name': chronos_tools.compose_job_id(
                    'some_other_service',
                    'some_other_instance',
                    'git3333',
                    'config3333',
                ),
                'disabled': False,
            },
        ]
        fake_client = mock.Mock(list=mock.Mock(return_value=fake_jobs))
        expected = [fake_jobs[0], fake_jobs[1], fake_jobs[2]]
        actual = chronos_tools.lookup_chronos_jobs(
            service=fake_service,
            instance=fake_instance,
            client=fake_client,
            include_disabled=True,
        )
        assert sorted(actual) == sorted(expected)

    def test_lookup_chronos_jobs_with_everything_specified(self):
        fake_service = 'fake_service'
        fake_instance = 'fake_instance'
        fake_git_hash = 'fake_git_hash'
        fake_config_hash = 'fake_config_hash'
        fake_jobs = [
            {
                'name': chronos_tools.compose_job_id(
                    fake_service,
                    fake_instance,
                    fake_git_hash,
                    fake_config_hash,
                ),
                'disabled': False,
            },
            {
                'name': chronos_tools.compose_job_id(
                    fake_service,
                    fake_instance,
                    'git2222',
                    'config2222',
                ),
                'disabled': False,
            },
            {
                'name': chronos_tools.compose_job_id(
                    fake_service,
                    fake_instance,
                    'gitdisabled',
                    'configdisabled',
                ),
                'disabled': True,
            },
            {
                'name': chronos_tools.compose_job_id(
                    'some_other_service',
                    'some_other_instance',
                    'git3333',
                    'config3333',
                ),
                'disabled': False,
            },
        ]
        fake_client = mock.Mock(list=mock.Mock(return_value=fake_jobs))
        expected = [fake_jobs[0]]
        actual = chronos_tools.lookup_chronos_jobs(
            service=fake_service,
            instance=fake_instance,
            git_hash=fake_git_hash,
            config_hash=fake_config_hash,
            client=fake_client,
        )
        assert sorted(actual) == sorted(expected)

    def test_lookup_chronos_jobs_skips_non_paasta_job_id(self):
        fake_jobs = [
            {
                'name': 'some non-paasta job',
                'disabled': False,
            },
        ]
        fake_client = mock.Mock(list=mock.Mock(return_value=fake_jobs))
        actual = chronos_tools.lookup_chronos_jobs('whatever', 'whatever', fake_client)
        # The main thing here is that InvalidJobNameError is not raised.
        assert actual == []

    def test_create_complete_config(self):
        fake_owner = 'test_team'
        with contextlib.nested(
            mock.patch('chronos_tools.load_system_paasta_config', autospec=True),
            mock.patch('chronos_tools.load_chronos_job_config',
                       autospec=True, return_value=self.fake_chronos_job_config),
            mock.patch('chronos_tools.get_code_sha_from_dockerurl', autospec=True, return_value="sha"),
            mock.patch('chronos_tools.get_config_hash', autospec=True, return_value="hash"),
            mock.patch('monitoring_tools.get_team', return_value=fake_owner)
        ) as (
            load_system_paasta_config_patch,
            load_chronos_job_config_patch,
            code_sha_patch,
            config_hash_patch,
            mock_get_team,
        ):
            load_system_paasta_config_patch.return_value.get_volumes = mock.Mock(return_value=[])
            load_system_paasta_config_patch.return_value.get_docker_registry = mock.Mock(return_value='fake_registry')
            actual = chronos_tools.create_complete_config('fake-service', 'fake-job')
            expected = {
                'arguments': None,
                'constraints': None,
                'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
                'async': False,
                'cpus': 5.5,
                'scheduleTimeZone': 'Zulu',
                'environmentVariables': [],
                'retries': 5,
                'disabled': False,
                'name': 'fake-service fake-job sha hash',
                'command': '/bin/sleep 40',
                'epsilon': 'PT30M',
                'container': {
                    'network': 'BRIDGE',
                    'volumes': [],
                    'image': "fake_registry/paasta-test-service-penguin",
                    'type': 'DOCKER'
                },
                'uris': ['file:///root/.dockercfg', ],
                'mem': 1024.4,
                'owner': fake_owner,
                'shell': True,
            }
            assert actual == expected

    def test_create_complete_config_desired_state_start(self):
        fake_owner = 'test_team'
        fake_chronos_job_config = chronos_tools.ChronosJobConfig(
            self.fake_service,
            self.fake_job_name,
            self.fake_config_dict,
            {
                'desired_state': 'start',
                'docker_image': 'fake_image'
            }
        )
        with contextlib.nested(
            mock.patch('chronos_tools.load_system_paasta_config', autospec=True),
            mock.patch('chronos_tools.load_chronos_job_config',
                       autospec=True, return_value=fake_chronos_job_config),
            mock.patch('chronos_tools.get_code_sha_from_dockerurl', autospec=True, return_value="sha"),
            mock.patch('chronos_tools.get_config_hash', autospec=True, return_value="hash"),
            mock.patch('monitoring_tools.get_team', return_value=fake_owner)
        ) as (
            load_system_paasta_config_patch,
            load_chronos_job_config_patch,
            code_sha_patch,
            config_hash_patch,
            mock_get_team,
        ):
            load_system_paasta_config_patch.return_value.get_volumes = mock.Mock(return_value=[])
            load_system_paasta_config_patch.return_value.get_docker_registry = mock.Mock(return_value='fake_registry')
            actual = chronos_tools.create_complete_config('fake_service', 'fake_job')
            expected = {
                'arguments': None,
                'constraints': None,
                'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
                'async': False,
                'cpus': 5.5,
                'scheduleTimeZone': 'Zulu',
                'environmentVariables': [],
                'retries': 5,
                'disabled': False,
                'name': 'fake_service fake_job sha hash',
                'command': '/bin/sleep 40',
                'epsilon': 'PT30M',
                'container': {
                    'network': 'BRIDGE',
                    'volumes': [],
                    'image': "fake_registry/fake_image",
                    'type': 'DOCKER'
                },
                'uris': ['file:///root/.dockercfg', ],
                'mem': 1024.4,
                'owner': fake_owner,
                'shell': True,
            }
            assert actual == expected

    def test_create_complete_config_desired_state_stop(self):
        fake_owner = 'test@test.com'
        fake_chronos_job_config = chronos_tools.ChronosJobConfig(
            self.fake_service,
            self.fake_job_name,
            self.fake_config_dict,
            {
                'desired_state': 'stop',
                'docker_image': 'fake_image'
            }
        )
        with contextlib.nested(
            mock.patch('chronos_tools.load_system_paasta_config', autospec=True),
            mock.patch('chronos_tools.load_chronos_job_config',
                       autospec=True, return_value=fake_chronos_job_config),
            mock.patch('chronos_tools.get_code_sha_from_dockerurl', autospec=True, return_value="sha"),
            mock.patch('chronos_tools.get_config_hash', autospec=True, return_value="hash"),
            mock.patch('monitoring_tools.get_team', return_value=fake_owner)
        ) as (
            load_system_paasta_config_patch,
            load_chronos_job_config_patch,
            code_sha_patch,
            config_hash_patch,
            mock_get_team,
        ):
            load_system_paasta_config_patch.return_value.get_volumes = mock.Mock(return_value=[])
            load_system_paasta_config_patch.return_value.get_docker_registry = mock.Mock(return_value='fake_registry')
            actual = chronos_tools.create_complete_config('fake_service', 'fake_job')
            expected = {
                'arguments': None,
                'constraints': None,
                'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
                'async': False,
                'cpus': 5.5,
                'scheduleTimeZone': 'Zulu',
                'environmentVariables': [],
                'retries': 5,
                'disabled': True,
                'name': 'fake_service fake_job sha hash',
                'command': '/bin/sleep 40',
                'epsilon': 'PT30M',
                'container': {
                    'network': 'BRIDGE',
                    'volumes': [],
                    'image': "fake_registry/fake_image",
                    'type': 'DOCKER'
                },
                'uris': ['file:///root/.dockercfg', ],
                'mem': 1024.4,
                'owner': fake_owner,
                'shell': True,
            }
            assert actual == expected

    def test_create_complete_config_respects_extra_volumes(self):
        fake_owner = 'test@test.com'
        fake_extra_volumes = [
            {
                "containerPath": "/extra",
                "hostPath": "/extra",
                "mode": "RO"
            }
        ]
        fake_system_volumes = [
            {
                "containerPath": "/system",
                "hostPath": "/system",
                "mode": "RO"
            }
        ]
        fake_instance_config = self.fake_config_dict
        fake_instance_config['extra_volumes'] = fake_extra_volumes
        fake_chronos_job_config = chronos_tools.ChronosJobConfig(
            self.fake_service,
            self.fake_job_name,
            fake_instance_config,
            {
                'desired_state': 'stop',
                'docker_image': 'fake_image'
            }
        )
        with contextlib.nested(
            mock.patch('chronos_tools.load_system_paasta_config', autospec=True),
            mock.patch('chronos_tools.load_chronos_job_config',
                       autospec=True, return_value=fake_chronos_job_config),
            mock.patch('chronos_tools.get_code_sha_from_dockerurl', autospec=True, return_value="sha"),
            mock.patch('chronos_tools.get_config_hash', autospec=True, return_value="hash"),
            mock.patch('monitoring_tools.get_team', return_value=fake_owner)
        ) as (
            load_system_paasta_config_patch,
            load_chronos_job_config_patch,
            code_sha_patch,
            config_hash_patch,
            mock_get_team,
        ):
            load_system_paasta_config_patch.return_value.get_volumes = mock.Mock(return_value=fake_system_volumes)
            load_system_paasta_config_patch.return_value.get_docker_registry = mock.Mock(return_value='fake_registry')
            actual = chronos_tools.create_complete_config('fake_service', 'fake_job')
            expected = {
                'arguments': None,
                'constraints': None,
                'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
                'async': False,
                'cpus': 5.5,
                'scheduleTimeZone': 'Zulu',
                'environmentVariables': [],
                'retries': 5,
                'disabled': True,
                'name': 'fake_service fake_job sha hash',
                'command': '/bin/sleep 40',
                'epsilon': 'PT30M',
                'container': {
                    'network': 'BRIDGE',
                    'volumes': fake_system_volumes + fake_extra_volumes,
                    'image': "fake_registry/fake_image",
                    'type': 'DOCKER'
                },
                'uris': ['file:///root/.dockercfg', ],
                'mem': 1024.4,
                'owner': fake_owner,
                'shell': True,
            }
            assert actual == expected

    def test_wait_for_job(self):
        fake_config = chronos_tools.ChronosConfig(
            {'user': 'test', 'password': 'pass', 'url': ['some_fake_host']}, '/fake/path')
        client = chronos_tools.get_chronos_client(fake_config)

        # only provide the right response on the third attempt
        client.list = Mock(side_effect=[[], [], [{'name': 'foo'}]])

        assert chronos_tools.wait_for_job(client, 'foo')

    def test_parse_time_variables_parses_shortdate(self):
        input_time = datetime.datetime(2012, 3, 14)
        test_input = 'ls %(shortdate-1)s foo'
        expected = 'ls 2012-03-13 foo'
        actual = chronos_tools.parse_time_variables(input_string=test_input, parse_time=input_time)
        assert actual == expected

    def test_cmp_datetimes(self):
        before = '2015-09-22T16:46:25.111Z'
        after = '2015-09-24T16:54:38.917Z'
        assert chronos_tools.cmp_datetimes(before, after) == 1

    def test_cmp_datetimes_with_empty_value(self):
        before = ''
        after = '2015-09-24T16:54:38.917Z'
        assert chronos_tools.cmp_datetimes(before, after) == 1

    def test_last_success_for_job(self):
        fake_job = {
            'foo': 'bar',
            'lastSuccess': 'fakeTimeStamp',
            'baz': 'qux',
        }
        assert chronos_tools.last_success_for_job(fake_job) == 'fakeTimeStamp'

    def test_last_failure_for_job(self):
        fake_job = {
            'foo': 'bar',
            'lastError': 'fakeTimeStamp',
            'baz': 'qux',
        }
        assert chronos_tools.last_failure_for_job(fake_job) == 'fakeTimeStamp'

    def test_status_last_run_no_runs(self):
        fake_job = {
            'name': 'myjob',
            'lastError': '',
            'lastSuccess': '',
        }
        assert chronos_tools.get_status_last_run(fake_job) == (None, chronos_tools.LastRunState.NotRun)

    def test_stats_last_run_no_failure(self):
        fake_job = {
            'name': 'myjob',
            'lastError': '',
            'lastSuccess': '2015-09-24T16:54:38.917Z',
        }
        assert (chronos_tools.get_status_last_run(fake_job) ==
                ('2015-09-24T16:54:38.917Z', chronos_tools.LastRunState.Success))

    def test_stats_last_run_no_success(self):
        fake_job = {
            'name': 'myjob',
            'lastError': '2015-09-24T16:54:38.917Z',
            'lastSuccess': '',
        }
        assert (chronos_tools.get_status_last_run(fake_job) ==
                ('2015-09-24T16:54:38.917Z', chronos_tools.LastRunState.Fail))

    def test_stats_last_run_failure(self):
        fake_job = {
            'name': 'myjob',
            'lastError': '2015-09-24T16:54:38.917Z',
            'lastSuccess': '2015-09-23T16:54:38.917Z',
        }
        assert (chronos_tools.get_status_last_run(fake_job) ==
                ('2015-09-24T16:54:38.917Z', chronos_tools.LastRunState.Fail))

    def test_stats_last_run_success(self):
        fake_job = {
            'name': 'myjob',
            'lastError': '2015-09-23T16:54:38.917Z',
            'lastSuccess': '2015-09-24T16:54:38.917Z',
        }
        assert (chronos_tools.get_status_last_run(fake_job) ==
                ('2015-09-24T16:54:38.917Z', chronos_tools.LastRunState.Success))

    def test_filter_enabled_jobs(self):
        fake_jobs = [{'name': 'foo', 'disabled': False}, {'name': 'bar', 'disabled': True}]
        assert chronos_tools.filter_enabled_jobs(fake_jobs) == [{'name': 'foo', 'disabled': False}]

    def test_sort_jobs(self):
        early_job = {
            # name isn't strictly necessary but since we're just comparing
            # dicts later this keeps things unambiguous.
            'name': 'early_job',
            'disabled': False,  # Not used but keeping things honest by having a mix of enabled and disabled
            'lastError': '2015-04-20T16:20:00.000Z',
            'lastSuccess': '2015-04-20T16:30:00.000Z',
        }
        late_job = {
            'name': 'late_job',
            'disabled': True,
            # Only last time counts, so even though this job's error comes
            # before either early_job result, that result is superceded by this
            # job's later success.
            'lastError': '2015-04-20T16:10:00.000Z',
            'lastSuccess': '2015-04-20T16:40:00.000Z',
        }
        unrun_job = {
            'name': 'unrun_job',
            'disabled': False,
            'lastError': '',
            'lastSuccess': '',
        }
        jobs = [early_job, late_job, unrun_job]
        assert chronos_tools.sort_jobs(jobs) == [late_job, early_job, unrun_job]

    def test_match_job_names_to_service_handles_mutiple_jobs(self):
        mock_jobs = [{'name': 'fake-service fake-instance git1 config1'},
                     {'name': 'fake-service fake-instance git2 config2'},
                     {'name': 'other-service other-instance git config'}]
        expected = mock_jobs[:2]
        actual = chronos_tools.match_job_names_to_service_instance(
            service='fake-service', instance='fake-instance', jobs=mock_jobs)
        assert sorted(actual) == sorted(expected)

    def test_disable_job(self):
        fake_client_class = mock.Mock(spec='chronos.ChronosClient')
        fake_client = fake_client_class(servers=[])
        chronos_tools.disable_job(job={}, client=fake_client)
        fake_client.update.assert_called_once_with({"disabled": True})

    def test_delete_job(self):
        fake_job_to_delete = copy.deepcopy(self.fake_config_dict)
        fake_job_to_delete["name"] = 'fake_job'
        fake_client_class = mock.Mock(spec='chronos.ChronosClient')
        fake_client = fake_client_class(servers=[])
        chronos_tools.delete_job(job=fake_job_to_delete, client=fake_client)
        fake_client.delete.assert_called_once_with('fake_job')

    def test_create_job(self):
        fake_client_class = mock.Mock(spec='chronos.ChronosClient')
        fake_client = fake_client_class(servers=[])
        chronos_tools.create_job(job=self.fake_config_dict, client=fake_client)
        fake_client.add.assert_called_once_with(self.fake_config_dict)
