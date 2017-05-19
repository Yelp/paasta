# Copyright 2015-2016 Yelp Inc.
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
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import datetime

import mock
from mock import Mock
from pytest import raises

from paasta_tools import chronos_tools
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import sort_dicts
from paasta_tools.utils import SystemPaastaConfig


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
        'disk': 1234.5,
        'disabled': False,
        'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
        'schedule_time_zone': 'Zulu',
        'monitoring': fake_monitoring_info,
        'data': {},
        'dependencies': {},
        'deploy': {},
        'lb_extras': {},
        'port': None,
        'smartstack': {},
        'vip': None,
    }
    fake_branch_dict = {
        'desired_state': 'start',
        'docker_image': 'paasta-%s-%s' % (fake_service, fake_cluster),
    }

    fake_chronos_job_config = chronos_tools.ChronosJobConfig(
        service=fake_service,
        cluster=fake_cluster,
        instance=fake_job_name,
        config_dict=fake_config_dict,
        branch_dict=fake_branch_dict,
    )

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
    fake_invalid_chronos_job_config = chronos_tools.ChronosJobConfig(service=fake_service,
                                                                     cluster=fake_cluster,
                                                                     instance=fake_job_name,
                                                                     config_dict=fake_invalid_config_dict,
                                                                     branch_dict=fake_branch_dict,
                                                                     )
    fake_config_file = {
        fake_job_name: fake_config_dict,
        'bad_job': fake_invalid_config_dict,
    }

    fake_dependent_job_config_dict = copy.deepcopy(fake_config_dict)
    fake_dependent_job_config_dict.pop("schedule")
    fake_dependent_job_config_dict["parents"] = ["test-service.parent1", "test-service.parent2"]
    fake_dependent_chronos_job_config = chronos_tools.ChronosJobConfig(
        service=fake_service,
        cluster=fake_cluster,
        instance=fake_job_name,
        config_dict=fake_dependent_job_config_dict,
        branch_dict=fake_branch_dict,
    )

    def test_chronos_config_object_normal(self):
        fake_json_contents = {
            'user': 'fake_user',
            'password': 'fake_password',
            'url': 'fake_host'
        }
        fake_config = chronos_tools.ChronosConfig(fake_json_contents)
        assert fake_config.get_username() == 'fake_user'
        assert fake_config.get_password() == 'fake_password'
        assert fake_config.get_url() == 'fake_host'

    def test_chronos_config_object_no_user(self):
        fake_json_contents = {
            'password': 'fake_password',
        }
        fake_config = chronos_tools.ChronosConfig(fake_json_contents)
        with raises(chronos_tools.ChronosNotConfigured):
            fake_config.get_username()

    def test_chronos_config_object_no_password(self):
        fake_json_contents = {
            'user': 'fake_user',
        }
        fake_config = chronos_tools.ChronosConfig(fake_json_contents)
        with raises(chronos_tools.ChronosNotConfigured):
            fake_config.get_password()

    def test_chronos_config_object_no_url(self):
        fake_json_contents = {
            'user': 'fake_user',
        }
        fake_config = chronos_tools.ChronosConfig(fake_json_contents)
        with raises(chronos_tools.ChronosNotConfigured):
            fake_config.get_url()

    def test_get_chronos_client(self):
        with mock.patch('chronos.connect', autospec=True) as mock_connect:
            fake_config = chronos_tools.ChronosConfig(
                {'user': 'test', 'password': 'pass', 'url': ['some_fake_host']})
            chronos_tools.get_chronos_client(fake_config)
            assert mock_connect.call_count == 1

    def test_compose_job_id(self):
        actual = chronos_tools.compose_job_id('service', 'instance')
        assert actual == 'service instance'

    def test_decompose_job_id_without_hashes(self):
        actual = chronos_tools.decompose_job_id('service instance')
        assert actual == ('service', 'instance')

    def test_decompose_job_id_with_tmp(self):
        actual = chronos_tools.decompose_job_id('tmp service instance')
        assert actual == ('service', 'instance')

    def test_decompose_job_id_wrong_tmp_identifier(self):
        with raises(chronos_tools.InvalidJobNameError):
            chronos_tools.decompose_job_id('foo service instance')

    def test_decompose_job_id_invalid_length(self):
        with raises(chronos_tools.InvalidJobNameError):
            chronos_tools.decompose_job_id('service instance baz')

    def test_read_chronos_jobs_for_service(self):
        fake_soa_dir = '/tmp/'
        expected_chronos_conf_file = 'chronos-penguin'
        with mock.patch(
            'paasta_tools.chronos_tools.load_deployments_json', autospec=True,
        ) as mock_load_deployments_json, mock.patch(
            'service_configuration_lib.read_extra_service_information', autospec=True,
        ) as mock_read_extra_service_information:
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
        with mock.patch(
            'paasta_tools.chronos_tools.load_deployments_json', autospec=True,
        ) as mock_load_deployments_json, mock.patch(
            'paasta_tools.chronos_tools.read_chronos_jobs_for_service', autospec=True,
        ) as mock_read_chronos_jobs_for_service:
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
            assert actual.config_dict == self.fake_chronos_job_config.config_dict

    def test_load_chronos_job_config_can_ignore_deployments(self):
        fake_soa_dir = '/tmp/'
        with mock.patch(
            'paasta_tools.chronos_tools.load_deployments_json', autospec=True,
        ) as mock_load_deployments_json, mock.patch(
            'paasta_tools.chronos_tools.read_chronos_jobs_for_service', autospec=True,
        ) as mock_read_chronos_jobs_for_service:
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
            assert actual.config_dict == self.fake_chronos_job_config.config_dict

    def test_load_chronos_job_config_unknown_job(self):
        with mock.patch(
            'paasta_tools.chronos_tools.read_chronos_jobs_for_service', autospec=True,
        ) as mock_read_chronos_jobs_for_service:
            mock_read_chronos_jobs_for_service.return_value = []
            with raises(NoConfigurationForServiceError) as exc:
                chronos_tools.load_chronos_job_config(service='fake_service',
                                                      instance='fake_job',
                                                      cluster='fake_cluster',
                                                      soa_dir='fake_dir')
            mock_read_chronos_jobs_for_service.assert_called_once_with('fake_service',
                                                                       'fake_cluster',
                                                                       soa_dir='fake_dir')
            assert str(exc.value) == 'No job named "fake_job" in config file chronos-fake_cluster.yaml'

    def test_get_bounce_method_in_config(self):
        expected = self.fake_config_dict['bounce_method']
        actual = self.fake_chronos_job_config.get_bounce_method()
        assert actual == expected

    def test_get_bounce_method_default(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        actual = fake_conf.get_bounce_method()
        assert actual == 'graceful'

    def test_get_cpus_in_config(self):
        expected = self.fake_monitoring_info
        actual = self.fake_chronos_job_config.get_monitoring()
        assert actual == expected

    def test_get_epsilon_default(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        actual = fake_conf.get_epsilon()
        assert actual == 'PT60S'

    def test_get_epsilon(self):
        fake_epsilon = 'fake_epsilon'
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={
                'epsilon': fake_epsilon,
            },
            branch_dict={},
        )
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

    def test_format_chronos_job_dict_uses_time_parser(self):
        fake_cmd = 'foo bar baz'
        fake_config_dict = {
            'bounce_method': 'graceful',
            'cmd': fake_cmd,
            'epsilon': 'PT30M',
            'retries': 5,
            'cpus': 5.5,
            'mem': 1024.4,
            'disk': 1234.5,
            'disabled': True,
            'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
            'schedule_time_zone': 'Zulu',
            'monitoring': {},
        }
        fake_docker_url = 'fake_docker_image_url'
        fake_docker_volumes = ['fake_docker_volume']
        fake_constraints = []
        dummy_config = SystemPaastaConfig({}, '/tmp/foo.cfg')

        expected = 'parsed_time'
        with mock.patch(
            'paasta_tools.chronos_tools.parse_time_variables', autospec=True, return_value=expected
        ) as mock_parse_time_variables:
            fake_chronos_job_config = chronos_tools.ChronosJobConfig(
                service='fake_service',
                cluster='fake_cluster',
                instance='fake_job',
                config_dict=fake_config_dict,
                branch_dict={},
            )
            fake_chronos_job_config.format_chronos_job_dict(
                fake_docker_url,
                fake_docker_volumes,
                dummy_config.get_dockercfg_location(),
                fake_constraints
            )
            mock_parse_time_variables.assert_called_with(fake_cmd)

    def test_get_owner(self):
        fake_owner = 'fake_team'
        with mock.patch('paasta_tools.monitoring_tools.get_team', autospec=True) as mock_get_team:
            mock_get_team.return_value = fake_owner
            actual = self.fake_chronos_job_config.get_owner()
            assert actual == fake_owner

    def test_get_shell_without_args_specified(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'args': ['a', 'b']},
            branch_dict={},
        )
        actual = fake_conf.get_shell()
        assert actual is False

    def test_get_shell_when_args_present(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_shell() is True

    def test_get_env(self):
        input_env = {'foo': 'bar', 'biz': 'baz'}
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'env': input_env},
            branch_dict={},
        )
        expected_env = [
            {"name": "PAASTA_CLUSTER", "value": "fake_cluster"},
            {"name": "PAASTA_SERVICE", "value": "fake_name"},
            {"name": "PAASTA_INSTANCE", "value": "fake_instance"},
            {"name": "PAASTA_DOCKER_IMAGE", "value": ""},
            {"name": "foo", "value": "bar"},
            {"name": "biz", "value": "baz"},
        ]
        assert sort_dicts(fake_conf.get_env()) == sort_dicts(expected_env)

    def test_get_calculated_constraints_respects_constraints_override(self):
        fake_constraints = [['fake_constraints']]
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'constraints': fake_constraints},
            branch_dict={},
        )
        fake_system_paasta_config = SystemPaastaConfig({}, "/foo")
        actual = fake_conf.get_calculated_constraints(
            system_paasta_config=fake_system_paasta_config
        )
        assert actual == fake_constraints

    def test_get_calculated_constraints_respects_pool(self):
        fake_pool = 'poolname'
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'pool': fake_pool},
            branch_dict={},
        )
        fake_system_paasta_config = SystemPaastaConfig({}, "/foo")
        actual = fake_conf.get_calculated_constraints(
            system_paasta_config=fake_system_paasta_config
        )
        assert actual == [['pool', 'LIKE', 'poolname']]

    def test_get_calculated_constraints_includes_system_blacklist(self):
        fake_pool = 'poolname'
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'pool': fake_pool},
            branch_dict={},
        )
        fake_system_paasta_config = SystemPaastaConfig({
            'deploy_blacklist': [['region', 'foo']]
        }, "/foo")
        actual = fake_conf.get_calculated_constraints(
            system_paasta_config=fake_system_paasta_config
        )
        assert sorted(actual) == sorted([['pool', 'LIKE', 'poolname'], ['region', 'UNLIKE', 'foo']])

    def test_get_calculated_constraints_includes_system_whitelist(self):
        fake_pool = 'poolname'
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'pool': fake_pool},
            branch_dict={},
        )
        fake_system_paasta_config = SystemPaastaConfig({
            'deploy_whitelist': ['region', ['foo']]
        }, "/foo")
        actual = fake_conf.get_calculated_constraints(
            system_paasta_config=fake_system_paasta_config
        )
        assert sorted(actual) == sorted([['pool', 'LIKE', 'poolname'], ['region', 'LIKE', 'foo']])

    def test_get_retries_default(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        actual = fake_conf.get_retries()
        assert actual == 2

    def test_get_retries(self):
        fake_retries = 'fake_retries'
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'retries': fake_retries},
            branch_dict={},
        )
        actual = fake_conf.get_retries()
        assert actual == fake_retries

    def test_get_disabled_default(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        actual = fake_conf.get_disabled()
        assert not actual

    def test_get_disabled(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'disabled': True},
            branch_dict={},
        )
        actual = fake_conf.get_disabled()
        assert actual

    def test_get_schedule(self):
        fake_schedule = 'fake_schedule'
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        actual = fake_conf.get_schedule()
        assert actual == fake_schedule

    def test_get_schedule_interval_in_seconds(self):
        fake_schedule = 'R/2016-10-21T00:30:00Z/P1D'
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        assert fake_conf.get_schedule_interval_in_seconds() == 60 * 60 * 24  # once a day

    def test_get_schedule_interval_in_seconds_if_crontab_format(self):
        fake_schedule = '0 2 * * *'
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        assert fake_conf.get_schedule_interval_in_seconds() == 60 * 60 * 24

    def test_get_schedule_interval_in_seconds_if_no_interval(self):
        fake_schedule = '2016-10-21T00:30:00Z'
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        assert fake_conf.get_schedule_interval_in_seconds() is None

    def test_get_schedule_interval_in_seconds_if_no_schedule(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_schedule_interval_in_seconds() is None

    def test_get_schedule_time_zone(self):
        fake_schedule_time_zone = 'fake_schedule_time_zone'
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'schedule_time_zone': fake_schedule_time_zone},
            branch_dict={},
        )
        actual = fake_conf.get_schedule_time_zone()
        assert actual == fake_schedule_time_zone

    def test_get_parents_ok(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'parents': None},
            branch_dict={},
        )
        assert fake_conf.get_parents() is None

    def test_get_parents_bad(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'parents': ['my-parent']},
            branch_dict={},
        )
        assert fake_conf.get_parents() == ['my-parent']

    def test_check_parents_none(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'parents': None},
            branch_dict={},
        )
        okay, msg = fake_conf.check_parents()
        assert okay is True
        assert msg == ''

    def test_check_parents_all_ok(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'parents': ['service1.instance1', 'service1.instance2']},
            branch_dict={},
        )
        okay, msg = fake_conf.check_parents()
        assert okay is True
        assert msg == ''

    # 'parents' doesn't need to be an array, it can also be a string
    def test_check_parents_scalar_to_array(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'parents': 'service1.instance1'},
            branch_dict={},
        )
        okay, msg = fake_conf.check_parents()
        assert okay is True
        assert msg == ''

    def test_check_parents_one_bad(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'parents': ['service1.instance1', 'service1-instance1']},
            branch_dict={},
        )
        okay, msg = fake_conf.check_parents()
        assert okay is False
        assert msg == 'The job name(s) service1-instance1 is not formatted correctly: expected service.instance'

    def test_check_parents_all_bad(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'parents': ['service1-instance1', 'service1-instance2']},
            branch_dict={},
        )
        okay, msg = fake_conf.check_parents()
        assert okay is False
        assert msg == ('The job name(s) service1-instance1, service1-instance2'
                       ' is not formatted correctly: expected service.instance')

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
        assert msg == 'The specified cpus value "intel" is not a valid float or int.'

    def test_check_mem_valid(self):
        okay, msg = self.fake_chronos_job_config.check_mem()
        assert okay is True
        assert msg == ''

    def test_check_mem_invalid(self):
        okay, msg = self.fake_invalid_chronos_job_config.check_mem()
        assert okay is False
        assert msg == 'The specified mem value "lots" is not a valid float or int.'

    def test_check_disk_valid(self):
        okay, msg = self.fake_chronos_job_config.check_disk()
        assert okay is True
        assert msg == ''

    def test_check_disk_invalid(self):
        okay, msg = self.fake_invalid_chronos_job_config.check_disk()
        assert okay is False
        assert msg == 'The specified disk value "all of it" is not a valid float or int.'

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

    def test_check_schedule_valid_cron(self):
        fake_schedule = '* * * * *'
        chronos_config = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        okay, msg = chronos_config.check_schedule()
        assert okay is True
        assert msg == ''

    def test_check_schedule_invalid_cron_sixth_field(self):
        fake_schedule = '* * * * * *'
        chronos_config = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified schedule "* * * * * *" '
                       'is neither a valid cron schedule nor a valid ISO 8601 schedule')

    def test_check_schedule_invalid_cron_L_field(self):
        fake_schedule = '0 16 L * *'
        chronos_config = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified schedule "0 16 L * *" '
                       'is neither a valid cron schedule nor a valid ISO 8601 schedule')

    def test_check_schedule_invalid_empty_start_time(self):
        fake_schedule = 'R10//PT70S'
        chronos_config = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == 'The specified schedule "%s" does not contain a start time' % fake_schedule

    def test_check_schedule_invalid_start_time_no_t_designator(self):
        fake_start_time = 'now'
        fake_schedule = 'R10/%s/PT70S' % fake_start_time
        chronos_config = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        fake_isodate_exception = (
            "ISO 8601 time designator 'T' missing. Unable to parse datetime string {!r}".format('now')
        )
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified start time "%s" in schedule "%s" does not conform to the ISO 8601 format:\n%s'
                       % (fake_start_time, fake_schedule, fake_isodate_exception))

    def test_check_schedule_invalid_start_time_bad_date(self):
        fake_start_time = 'todayT19:20:30Z'
        fake_schedule = 'R10/%s/PT70S' % fake_start_time
        chronos_config = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        fake_isodate_exception = 'Unrecognised ISO 8601 date format: {!r}'.format('today')
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified start time "%s" in schedule "%s" does not conform to the ISO 8601 format:\n%s'
                       % (fake_start_time, fake_schedule, fake_isodate_exception))

    def test_check_schedule_invalid_start_time_bad_time(self):
        fake_start_time = '1994-02-18Tmorning'
        fake_schedule = 'R10/%s/PT70S' % fake_start_time
        chronos_config = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        fake_isodate_exception = 'Unrecognised ISO 8601 time format: {!r}'.format('morning')
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified start time "%s" in schedule "%s" does not conform to the ISO 8601 format:\n%s'
                       % (fake_start_time, fake_schedule, fake_isodate_exception))

    def test_check_schedule_invalid_empty_interval(self):
        fake_schedule = 'R10/2015-03-25T19:36:35Z/'
        chronos_config = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified interval "" in schedule "%s" does not conform to the ISO 8601 format.'
                       % fake_schedule)

    def test_check_schedule_invalid_interval(self):
        fake_schedule = 'R10/2015-03-25T19:36:35Z/Mondays'
        chronos_config = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified interval "Mondays" in schedule "%s" does not conform to the ISO 8601 format.'
                       % fake_schedule)

    def test_check_schedule_low_interval(self):
        fake_schedule = 'R10/2015-03-25T19:36:35Z/PT10S'
        chronos_config = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == 'Unsupported interval "PT10S": jobs must be run at an interval of > 60 seconds'

    def test_check_schedule_invalid_empty_repeat(self):
        fake_schedule = '/2015-03-25T19:36:35Z/PT70S'
        chronos_config = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified repeat "" in schedule "%s" does not conform to the ISO 8601 format.'
                       % fake_schedule)

    def test_check_schedule_monthly_period(self):
        fake_schedule = 'R1/2015-03-25T19:36:35Z/P1M'
        chronos_config = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        okay, msg = chronos_config.check_schedule()
        assert okay is True

    def test_check_schedule_yearly_period(self):
        fake_schedule = 'R1/2015-03-25T19:36:35Z/P1Y'
        chronos_config = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        okay, msg = chronos_config.check_schedule()
        assert okay is True

    def test_check_schedule_invalid_repeat(self):
        fake_schedule = 'forever/2015-03-25T19:36:35Z/PT70S'
        chronos_config = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'schedule': fake_schedule},
            branch_dict={},
        )
        okay, msg = chronos_config.check_schedule()
        assert okay is False
        assert msg == ('The specified repeat "forever" in schedule "%s" does not conform to the ISO 8601 format.'
                       % fake_schedule)

    def test_check_schedule_time_zone_valid(self):
        okay, msg = self.fake_chronos_job_config.check_schedule_time_zone()
        assert okay is True
        assert msg == ''

    def test_check_schedule_time_zone_valid_empty(self):
        chronos_config = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={'schedule_time_zone': ''},
            branch_dict={},
        )
        okay, msg = chronos_config.check_schedule_time_zone()
        assert okay is True
        assert msg == ''

    def test_check_schedule_time_zone_invalid(self):
        okay, msg = self.fake_invalid_chronos_job_config.check_schedule_time_zone()
        assert okay is True  # FIXME implement the validator
        assert msg == ''  # FIXME implement the validator
        # assert okay is False
        # assert msg == 'The specified time zone "+0200" does not conform to the tz database format.'

    def test_get_desired_state_human_when_stopped(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={'desired_state': 'stop'},
        )
        assert 'Disabled' in fake_conf.get_desired_state_human()

    def test_get_desired_state_human_with_started(self):
        fake_conf = chronos_tools.ChronosJobConfig(
            service='',
            cluster='',
            instance='',
            config_dict={},
            branch_dict={'desired_state': 'start'},
        )
        assert 'Scheduled' in fake_conf.get_desired_state_human()

    def test_check_param_with_check(self):
        with mock.patch(
            'paasta_tools.chronos_tools.ChronosJobConfig.check_cpus', autospec=True,
        ) as mock_check_cpus:
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
        fake_cpus = 0.25
        fake_period = 200000
        fake_burst = 200
        fake_cpu_quota = fake_cpus * fake_period * (100 + fake_burst) / 100

        chronos_job_config = chronos_tools.ChronosJobConfig(
            service=fake_service,
            cluster='',
            instance=fake_job_name,
            config_dict={
                'cmd': fake_command,
                'schedule': fake_schedule,
                'epsilon': 'PT60S',
                'cpus': fake_cpus,
                'cfs_period_us': fake_period,
                'cpu_burst_pct': fake_burst,
            },
            branch_dict={},
        )
        expected = {
            'name': fake_job_name,
            'command': fake_command,
            'schedule': fake_schedule,
            'scheduleTimeZone': None,
            'environmentVariables': mock.ANY,
            'arguments': None,
            'constraints': [],
            'retries': 2,
            'epsilon': fake_epsilon,
            'cpus': fake_cpus,
            'async': False,
            'owner': fake_owner,
            'disabled': False,
            'mem': 1024,
            'disk': 1024,
            'container': {
                'network': 'BRIDGE',
                'volumes': fake_docker_volumes,
                'image': fake_docker_url,
                'type': 'DOCKER',
                'parameters': [
                    {'key': 'memory-swap', 'value': "1024m"},
                    {"key": "cpu-period", "value": "%s" % int(fake_period)},
                    {"key": "cpu-quota", "value": "%s" % int(fake_cpu_quota)},
                    {"key": "label", "value": "paasta_service=test_service"},
                    {"key": "label", "value": "paasta_instance=test_job"},
                ]
            },
            'uris': ['file:///root/.dockercfg', ],
            'shell': True,
        }
        dummy_config = SystemPaastaConfig({}, '/tmp/foo.cfg')
        with mock.patch('paasta_tools.monitoring_tools.get_team', return_value=fake_owner, autospec=True):
            actual = chronos_job_config.format_chronos_job_dict(
                fake_docker_url,
                fake_docker_volumes,
                dummy_config.get_dockercfg_location(),
                []
            )
            assert actual == expected

    def test_format_chronos_job_dict_uses_net(self):
        fake_service = 'test_service'
        fake_job_name = 'test_job'
        fake_owner = 'test_team'
        fake_command = 'echo foo >> /tmp/test_service_log'
        fake_schedule = 'R10/2012-10-01T05:52:00Z/PT1M'
        fake_docker_url = 'fake_docker_image_url'
        fake_docker_volumes = ['fake_docker_volume']

        chronos_job_config = chronos_tools.ChronosJobConfig(
            service=fake_service,
            cluster='',
            instance=fake_job_name,
            config_dict={
                'cmd': fake_command,
                'schedule': fake_schedule,
                'epsilon': 'PT60S',
                'net': 'host',
            },
            branch_dict={},
        )
        dummy_config = SystemPaastaConfig({}, '/tmp/foo.cfg')
        with mock.patch('paasta_tools.monitoring_tools.get_team', return_value=fake_owner, autospec=True):
            result = chronos_job_config.format_chronos_job_dict(
                fake_docker_url,
                fake_docker_volumes,
                dummy_config.get_dockercfg_location(),
                []
            )
            assert result['container']['network'] == 'HOST'

    def test_format_chronos_job_dict_invalid_param(self):
        fake_service = 'test_service'
        fake_job_name = 'test_job'
        fake_command = 'echo foo >> /tmp/test_service_log'
        fake_schedule = 'fake_bad_schedule'
        invalid_config = chronos_tools.ChronosJobConfig(
            service=fake_service,
            cluster='',
            instance=fake_job_name,
            config_dict={
                'cmd': fake_command,
                'schedule': fake_schedule,
                'epsilon': 'PT60S',
            },
            branch_dict={},
        )
        with raises(chronos_tools.InvalidChronosConfigError) as exc:
            invalid_config.format_chronos_job_dict(
                docker_url='',
                docker_volumes=[],
                docker_cfg_location={},
                constraints=[]
            )
        assert ('The specified schedule "%s" is neither a valid '
                'cron schedule nor a valid ISO 8601 schedule' % fake_schedule) in str(exc.value)

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
        with mock.patch('paasta_tools.chronos_tools.get_services_for_cluster',
                        autospec=True,
                        return_value=[],
                        ) as get_services_for_cluster_patch:
            assert chronos_tools.get_chronos_jobs_for_cluster('mycluster', soa_dir='my_soa_dir') == []
            get_services_for_cluster_patch.assert_called_once_with('mycluster', 'chronos', 'my_soa_dir')

    def test_lookup_chronos_jobs_with_service_and_instance(self):
        fake_client = mock.Mock()
        fake_service = 'fake_service'
        fake_instance = 'fake_instance'
        with mock.patch('paasta_tools.chronos_tools.filter_chronos_jobs', autospec=True) as mock_filter_chronos_jobs:
            chronos_tools.lookup_chronos_jobs(
                client=fake_client,
                service=fake_service,
                instance=fake_instance,
            )
            mock_filter_chronos_jobs.assert_called_once_with(
                jobs=fake_client.list.return_value,
                service=fake_service,
                instance=fake_instance,
                include_disabled=False,
                include_temporary=False,
            )

    def test_get_chronos_status_for_job(self):
        fake_service = 'fakeservice'
        fake_instance = 'fakeinstance'
        fake_client_class = mock.Mock(spec='chronos.ChronosClient')
        fake_client = fake_client_class(servers=[])
        expected_status = "imdead"

        fake_csv = [
            "node,fake_service_instance,fresh,alive",
            "node,%s,fresh,%s" % (chronos_tools.compose_job_id(fake_service, fake_instance), expected_status),
            "node,other_fake_service_instance,fresh,alive",
        ]
        fake_client.scheduler_graph = mock.Mock(return_value="\n".join(fake_csv))

        status = chronos_tools.get_chronos_status_for_job(fake_client, fake_service, fake_instance)
        fake_client.scheduler_graph.assert_called_once_with()
        assert status == expected_status

    def test_filter_chronos_jobs_with_no_filters(self):
        fake_jobs = [
            {
                'name': chronos_tools.compose_job_id(
                    'fake_service',
                    'fake_instance',
                ),
                'disabled': False,
            },
            {
                'name': chronos_tools.compose_job_id(
                    'other_fake_service',
                    'other_fake_instance',
                ),
                'disabled': False,
            },
        ]
        expected = fake_jobs
        actual = chronos_tools.filter_chronos_jobs(
            jobs=fake_jobs,
            service=None,
            instance=None,
            include_disabled=True,
            include_temporary=True,
        )
        assert sort_dicts(actual) == sort_dicts(expected)

    def test_filter_chronos_jobs_with_service_and_instance(self):
        fake_service = 'fake_service'
        fake_instance = 'fake_instance'
        fake_jobs = [
            {
                'name': chronos_tools.compose_job_id(
                    'fake_service',
                    'fake_instance',
                ),
                'disabled': False,
            },
            {
                'name': chronos_tools.compose_job_id(
                    'other_fake_service',
                    'other_fake_instance',
                ),
                'disabled': False,
            },
        ]
        expected = [fake_jobs[0]]
        actual = chronos_tools.filter_chronos_jobs(
            jobs=fake_jobs,
            service=fake_service,
            instance=fake_instance,
            include_disabled=False,
            include_temporary=True,
        )
        assert sorted(actual) == sorted(expected)

    def test_filter_chronos_jobs_include_disabled(self):
        fake_service = 'fake_service'
        fake_instance = 'fake_instance'
        fake_jobs = [
            {
                'name': chronos_tools.compose_job_id(
                    fake_service,
                    fake_instance,
                ),
                'disabled': False,
            },
            {
                'name': chronos_tools.compose_job_id(
                    fake_service,
                    fake_instance,
                ),
                'disabled': False,
            },
            {
                'name': chronos_tools.compose_job_id(
                    fake_service,
                    fake_instance,
                ),
                'disabled': True,
            },
            {
                'name': chronos_tools.compose_job_id(
                    'some_other_service',
                    'some_other_instance',
                ),
                'disabled': False,
            },
        ]
        expected = [fake_jobs[0], fake_jobs[1], fake_jobs[2]]
        actual = chronos_tools.filter_chronos_jobs(
            jobs=fake_jobs,
            service=fake_service,
            instance=fake_instance,
            include_disabled=True,
            include_temporary=True,
        )
        assert sort_dicts(actual) == sort_dicts(expected)

    def test_filter_chronos_jobs_skips_non_paasta_job_id(self):
        fake_jobs = [
            {
                'name': 'some non-paasta job',
                'disabled': False,
            },
        ]
        actual = chronos_tools.filter_chronos_jobs(
            jobs=fake_jobs,
            service='whatever',
            instance='whatever',
            include_disabled=False,
            include_temporary=True,
        )
        # The main thing here is that InvalidJobNameError is not raised.
        assert actual == []

    def test_create_complete_config(self):
        fake_owner = 'test_team'
        fake_config_hash = 'fake_config_hash'
        with mock.patch(
            'paasta_tools.chronos_tools.load_system_paasta_config', autospec=True,
        ) as load_system_paasta_config_patch, mock.patch(
            'paasta_tools.chronos_tools.load_chronos_job_config',
            autospec=True, return_value=self.fake_chronos_job_config,
        ), mock.patch(
            'paasta_tools.monitoring_tools.get_team', return_value=fake_owner, autospec=True,
        ), mock.patch(
            'paasta_tools.chronos_tools.get_config_hash', return_value=fake_config_hash, autospec=True,
        ):
            load_system_paasta_config_patch.return_value.get_volumes = mock.Mock(return_value=[])
            load_system_paasta_config_patch.return_value.get_docker_registry = mock.Mock(return_value='fake_registry')
            load_system_paasta_config_patch.return_value.get_dockercfg_location = \
                mock.Mock(return_value='file:///root/.dockercfg')
            actual = chronos_tools.create_complete_config('fake-service', 'fake-job')
            expected = {
                'arguments': None,
                'description': fake_config_hash,
                'constraints': [['pool', 'LIKE', 'default']],
                'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
                'async': False,
                'cpus': 5.5,
                'scheduleTimeZone': 'Zulu',
                'environmentVariables': mock.ANY,
                'retries': 5,
                'disabled': False,
                'name': 'fake-service fake-job',
                'command': '/bin/sleep 40',
                'epsilon': 'PT30M',
                'container': {
                    'network': 'BRIDGE',
                    'volumes': [],
                    'image': "fake_registry/paasta-test-service-penguin",
                    'type': 'DOCKER',
                    'parameters': [
                        {'key': 'memory-swap', 'value': '1025m'},
                        {"key": "cpu-period", "value": '100000'},
                        {"key": "cpu-quota", "value": '5500000'},
                        {"key": "label", "value": "paasta_service=test-service"},
                        {"key": "label", "value": "paasta_instance=test"},
                    ],
                },
                'uris': ['file:///root/.dockercfg', ],
                'mem': 1024.4,
                'disk': 1234.5,
                'owner': fake_owner,
                'shell': True,
            }
            assert actual == expected

    def test_create_complete_config_understands_parents(self):
        fake_owner = 'test_team'
        fake_config_hash = 'fake_config_hash'
        with mock.patch(
            'paasta_tools.chronos_tools.load_system_paasta_config', autospec=True,
        ) as load_system_paasta_config_patch, mock.patch(
            'paasta_tools.chronos_tools.load_chronos_job_config',
            autospec=True, return_value=self.fake_dependent_chronos_job_config,
        ), mock.patch(
            'paasta_tools.monitoring_tools.get_team', return_value=fake_owner, autospec=True,
        ), mock.patch(
            'paasta_tools.chronos_tools.get_config_hash', return_value=fake_config_hash, autospec=True,
        ):
            load_system_paasta_config_patch.return_value.get_volumes = mock.Mock(return_value=[])
            load_system_paasta_config_patch.return_value.get_docker_registry = mock.Mock(return_value='fake_registry')
            load_system_paasta_config_patch.return_value.get_dockercfg_location = \
                mock.Mock(return_value='file:///root/.dockercfg')
            actual = chronos_tools.create_complete_config('fake-service', 'fake-job')
            assert actual["parents"] == ['test-service parent1', 'test-service parent2']
            assert "schedule" not in actual

    def test_create_complete_config_considers_disabled(self):
        fake_owner = 'test_team'
        with mock.patch(
            'paasta_tools.chronos_tools.load_system_paasta_config', autospec=True,
        ) as load_system_paasta_config_patch, mock.patch(
            'paasta_tools.chronos_tools.load_chronos_job_config',
            autospec=True, return_value=self.fake_chronos_job_config,
        ) as load_chronos_job_config_patch, mock.patch(
            'paasta_tools.monitoring_tools.get_team', return_value=fake_owner, autospec=True,
        ):
            load_system_paasta_config_patch.return_value.get_volumes = mock.Mock(return_value=[])
            load_system_paasta_config_patch.return_value.get_docker_registry = mock.Mock(return_value='fake_registry')
            load_system_paasta_config_patch.return_value.get_dockercfg_location = \
                mock.Mock(return_value='file:///root/.dockercfg')
            first_description = chronos_tools.create_complete_config('fake-service', 'fake-job')['description']

            stopped_job_config = chronos_tools.ChronosJobConfig(
                service=self.fake_service,
                cluster=self.fake_cluster,
                instance=self.fake_job_name,
                config_dict=self.fake_config_dict,
                branch_dict={
                    'desired_state': 'stop',
                    'docker_image': 'paasta-%s-%s' % (self.fake_service, self.fake_cluster),
                }
            )
            load_chronos_job_config_patch.return_value = stopped_job_config
            second_description = chronos_tools.create_complete_config('fake-service', 'fake-job')['description']

            assert first_description != second_description

    def test_create_complete_config_desired_state_start(self):
        fake_owner = 'test_team'
        fake_chronos_job_config = chronos_tools.ChronosJobConfig(
            service=self.fake_service,
            cluster='',
            instance=self.fake_job_name,
            config_dict=self.fake_config_dict,
            branch_dict={
                'desired_state': 'start',
                'docker_image': 'fake_image'
            },
        )
        fake_config_hash = 'fake_config_hash'
        with mock.patch(
            'paasta_tools.chronos_tools.load_system_paasta_config', autospec=True,
        ) as load_system_paasta_config_patch, mock.patch(
            'paasta_tools.chronos_tools.load_chronos_job_config',
            autospec=True, return_value=fake_chronos_job_config,
        ), mock.patch(
            'paasta_tools.monitoring_tools.get_team', return_value=fake_owner, autospec=True,
        ), mock.patch(
            'paasta_tools.chronos_tools.get_config_hash', return_value=fake_config_hash, autospec=True,
        ):
            load_system_paasta_config_patch.return_value.get_volumes = mock.Mock(return_value=[])
            load_system_paasta_config_patch.return_value.get_docker_registry = mock.Mock(return_value='fake_registry')
            load_system_paasta_config_patch.return_value.get_dockercfg_location = \
                mock.Mock(return_value='file:///root/.dockercfg')
            actual = chronos_tools.create_complete_config('fake_service', 'fake_job')
            expected = {
                'arguments': None,
                'description': fake_config_hash,
                'constraints': [['pool', 'LIKE', 'default']],
                'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
                'async': False,
                'cpus': 5.5,
                'scheduleTimeZone': 'Zulu',
                'environmentVariables': mock.ANY,
                'retries': 5,
                'disabled': False,
                'name': 'fake_service fake_job',
                'command': '/bin/sleep 40',
                'epsilon': 'PT30M',
                'container': {
                    'network': 'BRIDGE',
                    'volumes': [],
                    'image': "fake_registry/fake_image",
                    'type': 'DOCKER',
                    'parameters': [
                        {'key': 'memory-swap', 'value': '1025m'},
                        {"key": "cpu-period", "value": '100000'},
                        {"key": "cpu-quota", "value": '5500000'},
                        {"key": "label", "value": "paasta_service=test-service"},
                        {"key": "label", "value": "paasta_instance=test"},
                    ],
                },
                'uris': ['file:///root/.dockercfg', ],
                'mem': 1024.4,
                'disk': 1234.5,
                'owner': fake_owner,
                'shell': True,
            }
            assert actual == expected

    def test_create_complete_config_desired_state_stop(self):
        fake_owner = 'test@test.com'
        fake_chronos_job_config = chronos_tools.ChronosJobConfig(
            service=self.fake_service,
            cluster='',
            instance=self.fake_job_name,
            config_dict=self.fake_config_dict,
            branch_dict={
                'desired_state': 'stop',
                'docker_image': 'fake_image'
            },
        )
        fake_config_hash = 'fake_config_hash'
        with mock.patch(
            'paasta_tools.chronos_tools.load_system_paasta_config', autospec=True,
        ) as load_system_paasta_config_patch, mock.patch(
            'paasta_tools.chronos_tools.load_chronos_job_config',
            autospec=True, return_value=fake_chronos_job_config,
        ), mock.patch(
            'paasta_tools.monitoring_tools.get_team', return_value=fake_owner, autospec=True,
        ), mock.patch(
            'paasta_tools.chronos_tools.get_config_hash', return_value=fake_config_hash, autospec=True,
        ):
            load_system_paasta_config_patch.return_value.get_volumes = mock.Mock(return_value=[])
            load_system_paasta_config_patch.return_value.get_docker_registry = mock.Mock(return_value='fake_registry')
            load_system_paasta_config_patch.return_value.get_dockercfg_location = \
                mock.Mock(return_value='file:///root/.dockercfg')
            actual = chronos_tools.create_complete_config('fake_service', 'fake_job')
            expected = {
                'arguments': None,
                'description': fake_config_hash,
                'constraints': [['pool', 'LIKE', 'default']],
                'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
                'async': False,
                'cpus': 5.5,
                'scheduleTimeZone': 'Zulu',
                'environmentVariables': mock.ANY,
                'retries': 5,
                'disabled': True,
                'name': 'fake_service fake_job',
                'command': '/bin/sleep 40',
                'epsilon': 'PT30M',
                'container': {
                    'network': 'BRIDGE',
                    'volumes': [],
                    'image': "fake_registry/fake_image",
                    'type': 'DOCKER',
                    'parameters': [
                        {'key': 'memory-swap', 'value': '1025m'},
                        {"key": "cpu-period", "value": '100000'},
                        {"key": "cpu-quota", "value": '5500000'},
                        {"key": "label", "value": "paasta_service=test-service"},
                        {"key": "label", "value": "paasta_instance=test"},
                    ],
                },
                'uris': ['file:///root/.dockercfg', ],
                'mem': 1024.4,
                'disk': 1234.5,
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
            service=self.fake_service,
            cluster='',
            instance=self.fake_job_name,
            config_dict=fake_instance_config,
            branch_dict={
                'desired_state': 'stop',
                'docker_image': 'fake_image'
            },
        )
        fake_config_hash = 'fake_config_hash'
        with mock.patch(
            'paasta_tools.chronos_tools.load_system_paasta_config', autospec=True,
        ) as load_system_paasta_config_patch, mock.patch(
            'paasta_tools.chronos_tools.load_chronos_job_config',
            autospec=True, return_value=fake_chronos_job_config,
        ), mock.patch(
            'paasta_tools.monitoring_tools.get_team', return_value=fake_owner, autospec=True,
        ), mock.patch(
            'paasta_tools.chronos_tools.get_config_hash', return_value=fake_config_hash, autospec=True,
        ):
            load_system_paasta_config_patch.return_value.get_volumes = mock.Mock(return_value=fake_system_volumes)
            load_system_paasta_config_patch.return_value.get_docker_registry = mock.Mock(return_value='fake_registry')
            load_system_paasta_config_patch.return_value.get_dockercfg_location = \
                mock.Mock(return_value='file:///root/.dockercfg')
            actual = chronos_tools.create_complete_config('fake_service', 'fake_job')
            expected = {
                'description': fake_config_hash,
                'arguments': None,
                'constraints': [['pool', 'LIKE', 'default']],
                'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
                'async': False,
                'cpus': 5.5,
                'scheduleTimeZone': 'Zulu',
                'environmentVariables': mock.ANY,
                'retries': 5,
                'disabled': True,
                'name': 'fake_service fake_job',
                'command': '/bin/sleep 40',
                'epsilon': 'PT30M',
                'container': {
                    'network': 'BRIDGE',
                    'volumes': sort_dicts(fake_system_volumes + fake_extra_volumes),
                    'image': "fake_registry/fake_image",
                    'type': 'DOCKER',
                    'parameters': [
                        {'key': 'memory-swap', 'value': '1025m'},
                        {"key": "cpu-period", "value": '100000'},
                        {"key": "cpu-quota", "value": '5500000'},
                        {"key": "label", "value": "paasta_service=test-service"},
                        {"key": "label", "value": "paasta_instance=test"},
                    ],
                },
                'uris': ['file:///root/.dockercfg', ],
                'mem': 1024.4,
                'disk': 1234.5,
                'owner': fake_owner,
                'shell': True,
            }
            assert actual == expected

    def test_wait_for_job(self):
        fake_config = chronos_tools.ChronosConfig(
            {'user': 'test', 'password': 'pass', 'url': ['some_fake_host']})
        client = chronos_tools.get_chronos_client(fake_config)

        # only provide the right response on the third attempt
        client.list = Mock(side_effect=[[], [], [{'name': 'foo'}]])

        with mock.patch('paasta_tools.chronos_tools.sleep', autospec=True) as mock_sleep:
            assert chronos_tools.wait_for_job(client, 'foo')
            assert mock_sleep.call_count == 2

    def test_parse_time_variables_parses_shortdate(self):
        input_time = datetime.datetime(2012, 3, 14)
        test_input = 'ls %(shortdate-1)s foo'
        expected = 'ls 2012-03-13 foo'
        actual = chronos_tools.parse_time_variables(input_string=test_input, parse_time=input_time)
        assert actual == expected

    def test_parse_time_variables_parses_percent(self):
        input_time = datetime.datetime(2012, 3, 14)
        test_input = './mycommand --date %(shortdate-1)s --format foo/logs/%%L/%%Y/%%m/%%d/'
        expected = './mycommand --date 2012-03-13 --format foo/logs/%L/%Y/%m/%d/'
        actual = chronos_tools.parse_time_variables(input_string=test_input, parse_time=input_time)
        assert actual == expected

    def test_check_cmd_escaped_percent(self):
        test_input = './mycommand --date %(shortdate-1)s --format foo/logs/%%L/%%Y/%%m/%%d/'
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'cmd': test_input},
            branch_dict={},
        )
        okay, msg = fake_conf.check_cmd()
        assert okay is True
        assert msg == ''

    def test_check_cmd_unescaped_percent(self):
        test_input = './mycommand --date %(shortdate-1)s --format foo/logs/%L/%Y/%m/%d/'
        fake_conf = chronos_tools.ChronosJobConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'cmd': test_input},
            branch_dict={},
        )
        okay, msg = fake_conf.check_cmd()
        assert okay is False
        assert './mycommand --date %(shortdate-1)s --format foo/logs/%L/%Y/%m/%d/' in msg

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

    def test_update_job(self):
        fake_client_class = mock.Mock(spec='chronos.ChronosClient')
        fake_client = fake_client_class(servers=[])
        chronos_tools.update_job(job=self.fake_config_dict, client=fake_client)
        fake_client.update.assert_called_once_with(self.fake_config_dict)

    def test_check_format_job_short(self):
        assert chronos_tools.check_parent_format("foo") is False

    def test_check_format_job_long(self):
        assert chronos_tools.check_parent_format("foo.bar.baz") is False

    def test_check_format_job_ok(self):
        assert chronos_tools.check_parent_format("foo.bar") is True

    @mock.patch('paasta_tools.chronos_tools.lookup_chronos_jobs', autospec=True)
    @mock.patch('paasta_tools.chronos_tools.get_chronos_client', autospec=True)
    @mock.patch('paasta_tools.chronos_tools.load_chronos_config', autospec=True)
    def test_get_jobs_for_service_instance_returns_one(
        self,
        mock_load_chronos_config,
        mock_get_chronos_client, mock_lookup_chronos_jobs,
    ):
        mock_matching_jobs = [{'name': 'service instance'}]
        mock_lookup_chronos_jobs.return_value = mock_matching_jobs
        mock_load_chronos_config.return_value = {}
        matching = chronos_tools.get_jobs_for_service_instance('service', 'instance')
        assert matching == mock_matching_jobs

    def test_filter_non_temporary_jobs(self):
        fake_jobs = [
            {
                'name': 'tmp-2016-04-09T064121354622 example_service mesosstage_robjreruntest'
            },
            {
                'name': 'example_service mesosstage_robjreruntest'
            }
        ]
        expected = [
            {
                'name': 'example_service mesosstage_robjreruntest'
            }
        ]
        assert chronos_tools.filter_non_temporary_chronos_jobs(fake_jobs) == expected

    def test_uses_time_variables_false(self):
        fake_chronos_job_config = copy.deepcopy(self.fake_chronos_job_config)
        fake_chronos_job_config.config_dict['cmd'] = '/usr/bin/printf hello'
        assert not chronos_tools.uses_time_variables(fake_chronos_job_config)

    def test_uses_time_variables_true(self):
        fake_chronos_job_config = copy.deepcopy(self.fake_chronos_job_config)
        fake_chronos_job_config.config_dict['cmd'] = '/usr/bin/printf %(shortdate)s'
        assert chronos_tools.uses_time_variables(fake_chronos_job_config)

    @mock.patch('paasta_tools.mesos_tools.get_local_slave_state', autospec=True)
    def test_chronos_services_running_here(self, mock_get_local_slave_state):
        id_1 = "ct:1494968280000:0:my-test-service my_test_date_interpolation:"
        id_2 = "ct:1494968700000:0:my-test-service my_fail_occasionally:"
        id_3 = "ct:1494957600000:0:my-test-service my_long_running:"
        mock_get_local_slave_state.return_value = {
            'frameworks': [
                {
                    'name': 'chronos',
                    'executors': [
                        {'id': id_1, 'resources': {}, 'tasks': [{'state': 'TASK_RUNNING'}]},
                        {'id': id_2, 'resources': {}, 'tasks': [{'state': 'TASK_STAGED'}]},
                        {'id': id_3, 'resources': {}, 'tasks': [{'state': 'TASK_RUNNING'}]},
                    ]
                },
                {
                    'name': 'marathon-1111111',
                    'executors': [
                        {'id': 'marathon.service', 'resources': {'ports': '[111-111]'},
                            'tasks': [{'state': 'TASK_RUNNING'}]},
                    ]
                },
            ]
        }
        expected = [('my-test-service', 'my_test_date_interpolation', None),
                    ('my-test-service', 'my_long_running', None),
                    ]
        actual = chronos_tools.chronos_services_running_here()
        mock_get_local_slave_state.assert_called_once_with(hostname=None)
        assert expected == actual
