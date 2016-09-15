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
import contextlib

import marathon
import mock
from marathon import MarathonHttpError
from marathon.models import MarathonApp
from mock import patch
from pytest import raises

from paasta_tools import marathon_tools
from paasta_tools.marathon_serviceinit import desired_state_human
from paasta_tools.mesos_tools import NoSlavesAvailableError
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DeploymentsJson
from paasta_tools.utils import SystemPaastaConfig


class TestMarathonTools:

    fake_marathon_app_config = marathon_tools.MarathonServiceConfig(
        service='servicename',
        cluster='clustername',
        instance='instancename',
        config_dict={
            'instances': 3,
            'cpus': 1,
            'mem': 100,
            'nerve_ns': 'fake_nerve_ns',
        },
        branch_dict={
            'docker_image': 'test_docker:1.0',
            'desired_state': 'start',
            'force_bounce': None,
        },
    )
    fake_srv_config = {
        'data': {},
        'deploy': {},
        'deployed_to': ['another-box'],
        'lb_extras': {},
        'monitoring': {},
        'runs_on': ['some-box'],
        'port': None,
        'smartstack': {},
        'vip': None,
    }
    fake_docker_registry = 'remote_registry.com'
    fake_marathon_config = marathon_tools.MarathonConfig({
        'cluster': 'test_cluster',
        'url': 'http://test_url',
        'user': 'admin',
        'pass': 'admin_pass',
        'docker_registry': fake_docker_registry,
        'docker_volumes': [
            {
                'hostPath': '/var/data/a',
                'containerPath': '/etc/a',
                'mode': 'RO',
            },
            {
                'hostPath': '/var/data/b',
                'containerPath': '/etc/b',
                'mode': 'RW',
            },
        ],
    })
    fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()

    def test_load_marathon_service_config_happy_path(self):
        fake_name = 'jazz'
        fake_instance = 'solo'
        fake_cluster = 'amnesia'
        fake_dir = '/nail/home/sanfran'
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.load_deployments_json', autospec=True),
            mock.patch('service_configuration_lib.read_service_configuration', autospec=True),
            mock.patch('service_configuration_lib.read_extra_service_information', autospec=True),
            mock.patch('paasta_tools.marathon_tools.deep_merge_dictionaries', autospec=True),
        ) as (
            mock_load_deployments_json,
            mock_read_service_configuration,
            mock_read_extra_service_information,
            _,
        ):
            mock_read_extra_service_information.return_value = {fake_instance: {}}
            marathon_tools.load_marathon_service_config(
                fake_name,
                fake_instance,
                fake_cluster,
                soa_dir=fake_dir,
            )
            assert mock_read_service_configuration.call_count == 1
            assert mock_read_extra_service_information.call_count == 1
            mock_load_deployments_json.assert_called_once_with(fake_name, soa_dir=fake_dir)

    def test_load_marathon_service_config_bails_with_no_config(self):
        fake_name = 'jazz'
        fake_instance = 'solo'
        fake_cluster = 'amnesia'
        fake_dir = '/nail/home/sanfran'
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.load_deployments_json', autospec=True),
            mock.patch('service_configuration_lib.read_service_configuration', autospec=True),
            mock.patch('service_configuration_lib.read_extra_service_information', autospec=True),
        ) as (
            mock_load_deployments_json,
            mock_read_service_configuration,
            mock_read_extra_service_information,
        ):
            mock_read_extra_service_information.return_value = {}
            with raises(marathon_tools.NoConfigurationForServiceError):
                marathon_tools.load_marathon_service_config(
                    fake_name,
                    fake_instance,
                    fake_cluster,
                    soa_dir=fake_dir,
                )

    def test_read_service_config(self):
        fake_name = 'jazz'
        fake_instance = 'solo'
        fake_cluster = 'amnesia'
        fake_dir = '/nail/home/sanfran'
        config_copy = self.fake_marathon_app_config.config_dict.copy()

        expected = marathon_tools.MarathonServiceConfig(
            service=fake_name,
            cluster='',
            instance=fake_instance,
            config_dict=dict(
                self.fake_srv_config.items() +
                self.fake_marathon_app_config.config_dict.items()
            ),
            branch_dict={},
        )

        with contextlib.nested(
            mock.patch(
                'service_configuration_lib.read_service_configuration',
                autospec=True,
                return_value=self.fake_srv_config,
            ),
            mock.patch(
                'service_configuration_lib.read_extra_service_information',
                autospec=True,
                return_value={fake_instance: config_copy},
            ),
        ) as (
            read_service_configuration_patch,
            read_extra_info_patch,
        ):
            actual = marathon_tools.load_marathon_service_config(
                fake_name,
                fake_instance,
                fake_cluster,
                load_deployments=False,
                soa_dir=fake_dir,
            )
            assert expected.service == actual.service
            assert expected.instance == actual.instance
            assert expected.config_dict == actual.config_dict
            assert expected.branch_dict == actual.branch_dict

            assert read_service_configuration_patch.call_count == 1
            read_service_configuration_patch.assert_any_call(fake_name, soa_dir=fake_dir)
            assert read_extra_info_patch.call_count == 1
            read_extra_info_patch.assert_any_call(fake_name, "marathon-amnesia", soa_dir=fake_dir)

    def test_read_service_config_and_deployments(self):
        fake_name = 'jazz'
        fake_instance = 'solo'
        fake_cluster = 'amnesia'
        fake_dir = '/nail/home/sanfran'
        fake_docker = 'no_docker:9.9'
        config_copy = self.fake_marathon_app_config.config_dict.copy()

        fake_branch_dict = {'desired_state': 'stop', 'force_bounce': '12345', 'docker_image': fake_docker},
        deployments_json_mock = mock.Mock(
            spec=DeploymentsJson,
            get_branch_dict=mock.Mock(return_value=fake_branch_dict),
        )

        with contextlib.nested(
            mock.patch(
                'service_configuration_lib.read_service_configuration',
                autospec=True,
                return_value=self.fake_srv_config,
            ),
            mock.patch(
                'service_configuration_lib.read_extra_service_information',
                autospec=True,
                return_value={fake_instance: config_copy},
            ),
            mock.patch(
                'paasta_tools.marathon_tools.load_deployments_json',
                autospec=True,
                return_value=deployments_json_mock,
            ),
        ) as (
            read_service_configuration_patch,
            read_extra_info_patch,
            load_deployments_json_patch,
        ):
            expected = marathon_tools.MarathonServiceConfig(
                service=fake_name,
                cluster='',
                instance=fake_instance,
                config_dict=dict(
                    self.fake_srv_config.items() +
                    self.fake_marathon_app_config.config_dict.items()
                ),
                branch_dict=fake_branch_dict,
            )
            actual = marathon_tools.load_marathon_service_config(
                fake_name,
                fake_instance,
                fake_cluster,
                load_deployments=True,
                soa_dir=fake_dir,
            )
            assert expected.service == actual.service
            assert expected.instance == actual.instance
            assert expected.config_dict == actual.config_dict
            assert expected.branch_dict == actual.branch_dict

            deployments_json_mock.get_branch_dict.assert_called_once_with(fake_name, 'paasta-amnesia.solo')
            assert read_service_configuration_patch.call_count == 1
            read_service_configuration_patch.assert_any_call(fake_name, soa_dir=fake_dir)
            assert read_extra_info_patch.call_count == 1
            read_extra_info_patch.assert_any_call(fake_name, "marathon-amnesia", soa_dir=fake_dir)

    def test_load_marathon_config(self):
        expected = {'foo': 'bar'}
        from_file = {'marathon_config': {'foo': 'bar'}}
        file_mock = mock.MagicMock(spec=file)
        with contextlib.nested(
            mock.patch('paasta_tools.utils.open', create=True, return_value=file_mock),
            mock.patch('json.load', autospec=True, return_value=from_file),
            mock.patch('os.path.isdir', autospec=True, return_value=True),
            mock.patch('os.access', autospec=True, return_value=True),
            mock.patch('paasta_tools.utils.get_readable_files_in_glob', autospec=True,
                       return_value=['/some/fake/dir/some_file.json']),
        ) as (
            open_file_patch,
            json_patch,
            isdir_patch,
            access_patch,
            get_readable_files_patch,
        ):
            assert marathon_tools.load_marathon_config() == expected
            open_file_patch.assert_called()
            json_patch.assert_called_with(file_mock.__enter__())

    def test_load_marathon_config_path_dne(self):
        expected = {}
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.open', create=True, side_effect=IOError(2, 'a', 'b')),
            mock.patch('os.path.isdir', autospec=True, return_value=True),
            mock.patch('os.access', autospec=True, return_value=True),
        ) as (
            open_patch,
            isdir_patch,
            access_patch,
        ):
            assert marathon_tools.load_marathon_config() == expected

    def test_get_all_namespaces_for_service(self):
        name = 'vvvvvv'
        soa_dir = '^_^'
        t1_dict = {'hollo': 'werld', 'smark': 'stact'}
        t2_dict = {'vataman': 'witir', 'sin': 'chaps'}
        fake_smartstack = {
            'smartstack': {'t1': t1_dict, 't2': t2_dict},
        }
        expected = [('vvvvvv.t2', t2_dict), ('vvvvvv.t1', t1_dict)]
        expected_short = [('t2', t2_dict), ('t1', t1_dict)]
        with mock.patch('service_configuration_lib.read_service_configuration', autospec=True,
                        return_value=fake_smartstack) as read_service_configuration_patch:
            actual = marathon_tools.get_all_namespaces_for_service(name, soa_dir)
            read_service_configuration_patch.assert_any_call(name, soa_dir)
            assert sorted(expected) == sorted(actual)

            actual_short = marathon_tools.get_all_namespaces_for_service(name, soa_dir, False)
            read_service_configuration_patch.assert_any_call(name, soa_dir)
            assert sorted(expected_short) == sorted(actual_short)

    def test_get_all_namespaces(self):
        soa_dir = 'carbon'
        namespaces = [[('aluminum', {'hydrogen': 1}), ('potassium', {'helium': 2})],
                      [('uranium', {'lithium': 3}), ('gold', {'boron': 5})]]
        expected = [('uranium', {'lithium': 3}), ('gold', {'boron': 5}),
                    ('aluminum', {'hydrogen': 1}), ('potassium', {'helium': 2})]
        with contextlib.nested(
            mock.patch('os.path.abspath', autospec=True, return_value='oxygen'),
            mock.patch('os.listdir', autospec=True, return_value=['rid1', 'rid2']),
            mock.patch('paasta_tools.marathon_tools.get_all_namespaces_for_service',
                       autospec=True,
                       side_effect=lambda a, b: namespaces.pop())
        ) as (
            abspath_patch,
            listdir_patch,
            get_namespaces_patch,
        ):
            actual = marathon_tools.get_all_namespaces(soa_dir)
            assert expected == actual
            abspath_patch.assert_called_once_with(soa_dir)
            listdir_patch.assert_called_once_with('oxygen')
            get_namespaces_patch.assert_any_call('rid1', soa_dir)
            get_namespaces_patch.assert_any_call('rid2', soa_dir)
            assert get_namespaces_patch.call_count == 2

    def test_get_proxy_port_for_instance(self):
        name = 'thats_no_moon'
        instance = 'thats_a_space_station'
        cluster = 'shot_line'
        soa_dir = 'drink_up'
        namespace = 'thirsty_mock'
        fake_port = 1234567890
        fake_nerve = marathon_tools.ServiceNamespaceConfig({'proxy_port': fake_port})
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                       autospec=True, return_value=namespace),
            mock.patch('paasta_tools.marathon_tools.load_service_namespace_config',
                       autospec=True, return_value=fake_nerve)
        ) as (
            read_ns_patch,
            read_config_patch
        ):
            actual = marathon_tools.get_proxy_port_for_instance(name, instance, cluster, soa_dir)
            assert fake_port == actual
            read_ns_patch.assert_called_once_with(name, instance, cluster, soa_dir)
            read_config_patch.assert_called_once_with(name, namespace, soa_dir)

    def test_get_proxy_port_for_instance_defaults_to_none(self):
        name = 'thats_no_moon'
        instance = 'thats_a_space_station'
        cluster = 'shot_line'
        soa_dir = 'drink_up'
        namespace = 'thirsty_mock'
        expected = None
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance',
                       autospec=True, return_value=namespace),
            mock.patch('paasta_tools.marathon_tools.load_service_namespace_config',
                       autospec=True, return_value={})
        ) as (
            read_ns_patch,
            read_config_patch
        ):
            actual = marathon_tools.get_proxy_port_for_instance(name, instance, cluster, soa_dir)
            assert expected == actual
            read_ns_patch.assert_called_once_with(name, instance, cluster, soa_dir)
            read_config_patch.assert_called_once_with(name, namespace, soa_dir)

    def test_read_service_namespace_config_exists(self):
        name = 'eman'
        namespace = 'ecapseman'
        soa_dir = 'rid_aos'
        mode = 'http'
        fake_healthcheck_mode = 'http'
        fake_uri = 'energy'
        fake_timeout = -10103
        fake_port = 777
        fake_retries = 9001
        fake_discover = 'myhabitat'
        fake_advertise = ['red', 'blue']
        fake_info = {
            'healthcheck_mode': fake_healthcheck_mode,
            'healthcheck_uri': fake_uri,
            'healthcheck_timeout_s': fake_timeout,
            'proxy_port': fake_port,
            'timeout_connect_ms': 192,
            'timeout_server_ms': 291,
            'timeout_client_ms': 912,
            'updown_timeout_s': 293,
            'retries': fake_retries,
            'mode': mode,
            'routes': [
                {
                    'source': 'oregon',
                    'destinations': ['indiana']
                },
                {
                    'source': 'florida', 'destinations': ['miami', 'beach']
                },
            ],
            'discover': fake_discover,
            'advertise': fake_advertise,
            'extra_advertise': {
                'alpha': ['beta'],
                'gamma': ['delta', 'epsilon'],
            },
            'extra_healthcheck_headers': {
                'Host': 'example.com'
            },
        }
        fake_config = {
            'smartstack': {
                namespace: fake_info,
            },
        }
        expected = {
            'healthcheck_mode': fake_healthcheck_mode,
            'healthcheck_uri': fake_uri,
            'healthcheck_timeout_s': fake_timeout,
            'proxy_port': fake_port,
            'timeout_connect_ms': 192,
            'timeout_server_ms': 291,
            'timeout_client_ms': 912,
            'updown_timeout_s': 293,
            'retries': fake_retries,
            'mode': mode,
            'routes': [
                ('oregon', 'indiana'), ('florida', 'miami'), ('florida', 'beach')
            ],
            'discover': fake_discover,
            'advertise': fake_advertise,
            'extra_advertise': [
                ('alpha', 'beta'), ('gamma', 'delta'), ('gamma', 'epsilon')
            ],
            'extra_healthcheck_headers': {
                'Host': 'example.com'
            },
        }
        with mock.patch('service_configuration_lib.read_service_configuration',
                        autospec=True,
                        return_value=fake_config) as read_service_configuration_patch:
            actual = marathon_tools.load_service_namespace_config(name, namespace, soa_dir)
            read_service_configuration_patch.assert_called_once_with(name, soa_dir)
            assert sorted(actual) == sorted(expected)

    def test_read_service_namespace_config_no_mode_with_no_smartstack(self):
        name = 'eman'
        namespace = 'ecapseman'
        soa_dir = 'rid_aos'
        fake_config = {}
        with mock.patch('service_configuration_lib.read_service_configuration',
                        autospec=True,
                        return_value=fake_config) as read_service_configuration_patch:
            actual = marathon_tools.load_service_namespace_config(name, namespace, soa_dir)
            read_service_configuration_patch.assert_called_once_with(name, soa_dir)
            assert actual.get('mode') is None

    def test_read_service_namespace_config_no_mode_with_smartstack(self):
        name = 'eman'
        namespace = 'ecapseman'
        soa_dir = 'rid_aos'
        fake_config = {
            'smartstack': {
                namespace: {'proxy_port': 9001},
            },
        }
        with mock.patch('service_configuration_lib.read_service_configuration',
                        autospec=True,
                        return_value=fake_config) as read_service_configuration_patch:
            actual = marathon_tools.load_service_namespace_config(name, namespace, soa_dir)
            read_service_configuration_patch.assert_called_once_with(name, soa_dir)
            assert actual.get('mode') == 'http'

    def test_read_service_namespace_config_no_file(self):
        name = 'a_man'
        namespace = 'a_boat'
        soa_dir = 'an_adventure'

        with mock.patch('service_configuration_lib.read_service_configuration',
                        side_effect=Exception) as read_service_configuration_patch:
            with raises(Exception):
                marathon_tools.load_service_namespace_config(name, namespace, soa_dir)
            read_service_configuration_patch.assert_called_once_with(name, soa_dir)

    @mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True)
    def test_read_namespace_for_service_instance_has_value_with_nerve_ns(self, load_config_patch):
        name = 'dont_worry'
        instance = 'im_a_professional'
        cluster = 'andromeda'
        namespace = 'spacename'
        soa_dir = 'dirdirdir'
        load_config_patch.return_value = marathon_tools.MarathonServiceConfig(
            service=name,
            cluster=cluster,
            instance=instance,
            config_dict={'nerve_ns': namespace},
            branch_dict={},
        )
        actual = marathon_tools.read_namespace_for_service_instance(name, instance, cluster, soa_dir)
        assert actual == namespace
        load_config_patch.assert_called_once_with(name, instance, cluster, load_deployments=False, soa_dir=soa_dir)

    @mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True)
    def test_read_namespace_for_service_instance_has_value(self, load_config_patch):
        name = 'dont_worry'
        instance = 'im_a_professional'
        cluster = 'andromeda'
        namespace = 'spacename'
        soa_dir = 'dirdirdir'
        load_config_patch.return_value = marathon_tools.MarathonServiceConfig(
            service=name,
            cluster=cluster,
            instance=instance,
            config_dict={'registration_namespaces': [namespace, 'something else']},
            branch_dict={},
        )
        actual = marathon_tools.read_namespace_for_service_instance(name, instance, cluster, soa_dir)
        assert actual == namespace
        load_config_patch.assert_called_once_with(name, instance, cluster, load_deployments=False, soa_dir=soa_dir)

    @mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True)
    def test_read_all_namespaces_for_service_instance_has_value(self, load_config_patch):
        name = 'dont_worry'
        instance = 'im_a_professional'
        cluster = 'andromeda'
        namespaces = ['spacename', 'spaceiscool', 'rocketsarecooler']
        soa_dir = 'dirdirdir'
        load_config_patch.return_value = marathon_tools.MarathonServiceConfig(
            service=name,
            cluster=cluster,
            instance=instance,
            config_dict={'registration_namespaces': namespaces},
            branch_dict={},
        )
        actual_registrations = marathon_tools.read_all_namespaces_for_service_instance(name, instance, cluster, soa_dir)
        assert set(actual_registrations) == set(namespaces)
        load_config_patch.assert_called_once_with(name, instance, cluster, load_deployments=False, soa_dir=soa_dir)

    @mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True)
    def test_read_namespace_for_service_instance_no_value(self, load_config_patch):
        name = 'wall_light'
        instance = 'ceiling_light'
        cluster = 'no_light'
        soa_dir = 'warehouse_light'
        load_config_patch.return_value = marathon_tools.MarathonServiceConfig(
            service=name,
            cluster=cluster,
            instance=instance,
            config_dict={'aaaaaaaa': ['bbbbbbbbbb']},
            branch_dict={},
        )

        actual = marathon_tools.read_namespace_for_service_instance(name, instance, cluster, soa_dir)
        assert actual == instance
        load_config_patch.assert_called_once_with(name, instance, cluster, load_deployments=False, soa_dir=soa_dir)

    @mock.patch('paasta_tools.marathon_tools.get_local_slave_state', autospec=True)
    def test_marathon_services_running_here(self, mock_get_local_slave_state):
        id_1 = 'klingon.ships.detected.249qwiomelht4jioewglkemr.someuuid'
        id_2 = 'fire.photon.torpedos.jtgriemot5yhtwe94.someuuid'
        id_3 = 'dota.axe.cleave.482u9jyoi4wed.someuuid'
        id_4 = 'mesos.deployment.is.hard.someuuid'
        id_5 = 'how.to.fake.data.someuuid'
        ports_1 = '[111-111]'
        ports_2 = '[222-222]'
        ports_3 = '[333-333]'
        ports_4 = '[444-444]'
        ports_5 = '[555-555]'
        mock_get_local_slave_state.return_value = {
            'frameworks': [
                {
                    'executors': [
                        {'id': id_1, 'resources': {'ports': ports_1},
                            'tasks': [{u'state': u'TASK_RUNNING'}]},
                        {'id': id_2, 'resources': {'ports': ports_2}, 'tasks': [{u'state': u'TASK_RUNNING'}]}
                    ],
                    'name': 'marathon-1111111'
                },
                {
                    'executors': [
                        {'id': id_3, 'resources': {'ports': ports_3}, 'tasks': [{u'state': u'TASK_RUNNING'}]},
                        {'id': id_4, 'resources': {'ports': ports_4}, 'tasks': [{u'state': u'TASK_RUNNING'}]},
                    ],
                    'name': 'marathon-3145jgreoifd'
                },
                {
                    'executors': [
                        {'id': id_5, 'resources': {'ports': ports_5}, 'tasks': [{u'state': u'TASK_STAGED'}]},
                    ],
                    'name': 'marathon-754rchoeurcho'
                },
                {
                    'executors': [
                        {'id': 'bunk', 'resources': {'ports': '[65-65]'}, 'tasks': [{u'state': u'TASK_RUNNING'}]},
                    ],
                    'name': 'super_bunk'
                }
            ]
        }
        expected = [('klingon', 'ships', 111),
                    ('fire', 'photon', 222),
                    ('dota', 'axe', 333),
                    ('mesos', 'deployment', 444)]
        actual = marathon_tools.marathon_services_running_here()
        mock_get_local_slave_state.assert_called_once_with()
        assert expected == actual

    def test_get_marathon_services_running_here_for_nerve(self):
        cluster = 'edelweiss'
        soa_dir = 'the_sound_of_music'
        fake_marathon_services = [('no_test', 'left_behind', 1111),
                                  ('no_docstrings', 'forever_abandoned', 2222)]
        namespaces = [['dos'], ['uno']]
        nerve_dicts = [marathon_tools.ServiceNamespaceConfig({'binary': 1, 'proxy_port': 6666}),
                       marathon_tools.ServiceNamespaceConfig({'clock': 0, 'proxy_port': 6666})]
        expected = [('no_test.uno', {'clock': 0, 'port': 1111, 'proxy_port': 6666}),
                    ('no_docstrings.dos', {'binary': 1, 'port': 2222, 'proxy_port': 6666})]
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.marathon_services_running_here',
                       autospec=True,
                       return_value=fake_marathon_services),
            mock.patch('paasta_tools.marathon_tools.read_all_namespaces_for_service_instance',
                       autospec=True,
                       side_effect=lambda a, b, c, d: namespaces.pop()),
            mock.patch('paasta_tools.marathon_tools.load_service_namespace_config',
                       autospec=True,
                       side_effect=lambda a, b, c: nerve_dicts.pop()),
        ) as (
            mara_srvs_here_patch,
            get_namespace_patch,
            read_ns_config_patch,
        ):
            actual = marathon_tools.get_marathon_services_running_here_for_nerve(cluster, soa_dir)
            assert expected == actual
            mara_srvs_here_patch.assert_called_once_with()
            get_namespace_patch.assert_any_call('no_test', 'left_behind', cluster, soa_dir)
            get_namespace_patch.assert_any_call('no_docstrings', 'forever_abandoned', cluster, soa_dir)
            assert get_namespace_patch.call_count == 2
            read_ns_config_patch.assert_any_call('no_test', 'uno', soa_dir)
            read_ns_config_patch.assert_any_call('no_docstrings', 'dos', soa_dir)
            assert read_ns_config_patch.call_count == 2

    def test_get_marathon_services_running_here_for_nerve_multiple_namespaces(self):
        cluster = 'edelweiss'
        soa_dir = 'the_sound_of_music'
        fake_marathon_services = [('no_test', 'left_behind', 1111),
                                  ('no_docstrings', 'forever_abandoned', 2222)]
        namespaces = [['quatro'], ['uno', 'dos', 'tres']]
        nerve_dicts = {
            ('no_test', 'uno'): marathon_tools.ServiceNamespaceConfig({'proxy_port': 6666}),
            ('no_test', 'dos'): marathon_tools.ServiceNamespaceConfig({'proxy_port': 6667}),
            ('no_test', 'tres'): marathon_tools.ServiceNamespaceConfig({'proxy_port': 6668}),
            ('no_docstrings', 'quatro'): marathon_tools.ServiceNamespaceConfig({'proxy_port': 6669})
        }
        expected = [('no_test.uno', {'port': 1111, 'proxy_port': 6666}),
                    ('no_test.dos', {'port': 1111, 'proxy_port': 6667}),
                    ('no_test.tres', {'port': 1111, 'proxy_port': 6668}),
                    ('no_docstrings.quatro', {'port': 2222, 'proxy_port': 6669})]
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.marathon_services_running_here',
                       autospec=True,
                       return_value=fake_marathon_services),
            mock.patch('paasta_tools.marathon_tools.read_all_namespaces_for_service_instance',
                       autospec=True,
                       side_effect=lambda a, b, c, d: namespaces.pop()),
            mock.patch('paasta_tools.marathon_tools.load_service_namespace_config',
                       autospec=True,
                       side_effect=lambda a, b, c: nerve_dicts.pop((a, b))),
        ) as (
            mara_srvs_here_patch,
            get_namespace_patch,
            read_ns_config_patch,
        ):
            actual = marathon_tools.get_marathon_services_running_here_for_nerve(cluster, soa_dir)
            assert expected == actual
            mara_srvs_here_patch.assert_called_once_with()
            get_namespace_patch.assert_any_call('no_test', 'left_behind', cluster, soa_dir)
            get_namespace_patch.assert_any_call('no_docstrings', 'forever_abandoned', cluster, soa_dir)
            assert get_namespace_patch.call_count == 2
            read_ns_config_patch.assert_any_call('no_test', 'uno', soa_dir)
            read_ns_config_patch.assert_any_call('no_test', 'dos', soa_dir)
            read_ns_config_patch.assert_any_call('no_test', 'tres', soa_dir)
            read_ns_config_patch.assert_any_call('no_docstrings', 'quatro', soa_dir)
            assert read_ns_config_patch.call_count == 4

    def test_get_marathon_services_running_here_for_nerve_when_not_in_smartstack(self):
        cluster = 'edelweiss'
        soa_dir = 'the_sound_of_music'
        fake_marathon_services = [('no_test', 'left_behind', 1111),
                                  ('no_docstrings', 'forever_abandoned', 2222)]
        namespaces = [['dos'], ['uno']]
        nerve_dicts = [marathon_tools.ServiceNamespaceConfig({'binary': 1}),
                       marathon_tools.ServiceNamespaceConfig({'clock': 0, 'proxy_port': 6666})]
        expected = [('no_test.uno', {'clock': 0, 'port': 1111, 'proxy_port': 6666})]
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.marathon_services_running_here',
                       autospec=True,
                       return_value=fake_marathon_services),
            mock.patch('paasta_tools.marathon_tools.read_all_namespaces_for_service_instance',
                       autospec=True,
                       side_effect=lambda a, b, c, d: namespaces.pop()),
            mock.patch('paasta_tools.marathon_tools.load_service_namespace_config',
                       autospec=True,
                       side_effect=lambda a, b, c: nerve_dicts.pop()),
        ) as (
            mara_srvs_here_patch,
            get_namespace_patch,
            read_ns_config_patch,
        ):
            actual = marathon_tools.get_marathon_services_running_here_for_nerve(cluster, soa_dir)
            assert expected == actual
            mara_srvs_here_patch.assert_called_once_with()
            get_namespace_patch.assert_any_call('no_test', 'left_behind', cluster, soa_dir)
            get_namespace_patch.assert_any_call('no_docstrings', 'forever_abandoned', cluster, soa_dir)
            assert get_namespace_patch.call_count == 2
            read_ns_config_patch.assert_any_call('no_test', 'uno', soa_dir)
            read_ns_config_patch.assert_any_call('no_docstrings', 'dos', soa_dir)
            assert read_ns_config_patch.call_count == 2

    def test_get_marathon_services_running_here_for_nerve_when_get_cluster_raises_custom_exception(self):
        cluster = None
        soa_dir = 'the_sound_of_music'
        with contextlib.nested(
            mock.patch(
                'paasta_tools.marathon_tools.load_system_paasta_config',
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.marathon_tools.marathon_services_running_here',
                autospec=True,
                return_value=[],
            ),
        ) as (
            load_system_paasta_config_patch,
            marathon_services_running_here_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster \
                = mock.Mock(side_effect=marathon_tools.PaastaNotConfiguredError)
            actual = marathon_tools.get_marathon_services_running_here_for_nerve(cluster, soa_dir)
            assert actual == []

    def test_get_marathon_services_running_here_for_nerve_when_paasta_not_configured(self):
        cluster = None
        soa_dir = 'the_sound_of_music'
        with contextlib.nested(
            mock.patch(
                'paasta_tools.marathon_tools.load_system_paasta_config',
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.marathon_tools.marathon_services_running_here',
                autospec=True,
                return_value=[],
            ),
        ) as (
            load_system_paasta_config_patch,
            marathon_services_running_here_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster \
                = mock.Mock(side_effect=marathon_tools.PaastaNotConfiguredError)
            actual = marathon_tools.get_marathon_services_running_here_for_nerve(cluster, soa_dir)
            assert actual == []

    def test_get_marathon_services_running_here_for_nerve_when_get_cluster_raises_other_exception(self):
        cluster = None
        soa_dir = 'the_sound_of_music'
        with contextlib.nested(
            mock.patch(
                'paasta_tools.marathon_tools.load_system_paasta_config',
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.marathon_tools.marathon_services_running_here',
                autospec=True,
                return_value=[],
            ),
        ) as (
            load_system_paasta_config_patch,
            marathon_services_running_here_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(side_effect=Exception)
            with raises(Exception):
                marathon_tools.get_marathon_services_running_here_for_nerve(cluster, soa_dir)

    def test_get_classic_service_information_for_nerve(self):
        with contextlib.nested(
            mock.patch('service_configuration_lib.read_port', return_value=101),
            mock.patch('paasta_tools.marathon_tools.load_service_namespace_config', autospec=True,
                       return_value={'ten': 10}),
        ) as (
            read_port_patch,
            namespace_config_patch,
        ):
            info = marathon_tools.get_classic_service_information_for_nerve('no_water', 'we_are_the_one')
            assert info == ('no_water.main', {'ten': 10, 'port': 101})

    def test_get_classic_services_that_run_here(self):
        with contextlib.nested(
            mock.patch(
                'service_configuration_lib.services_that_run_here',
                autospec=True,
                return_value={'d', 'c'},
            ),
            mock.patch(
                'os.listdir',
                autospec=True,
                return_value=['b', 'a']
            ),
            mock.patch(
                'os.path.exists',
                autospec=True,
                side_effect=lambda x: x in (
                    '/etc/nerve/puppet_services.d',
                    '/etc/nerve/puppet_services.d/a'
                )
            ),
        ) as (
            services_that_run_here_patch,
            listdir_patch,
            exists_patch,
        ):
            services = marathon_tools.get_classic_services_that_run_here()
            assert services == ['a', 'c', 'd']
            services_that_run_here_patch.assert_called_once_with()
            listdir_patch.assert_called_once_with(marathon_tools.PUPPET_SERVICE_DIR)

    def test_get_classic_services_running_here_for_nerve(self):
        with contextlib.nested(
            mock.patch(
                'paasta_tools.marathon_tools.get_classic_services_that_run_here',
                autospec=True,
                side_effect=lambda: ['a', 'b', 'c']
            ),
            mock.patch(
                'paasta_tools.marathon_tools.get_all_namespaces_for_service',
                autospec=True,
                side_effect=lambda x, y, full_name: [('foo', {})]
            ),
            mock.patch(
                'paasta_tools.marathon_tools._namespaced_get_classic_service_information_for_nerve',
                autospec=True,
                side_effect=lambda x, y, _: (compose_job_id(x, y), {})
            ),
        ):
            assert marathon_tools.get_classic_services_running_here_for_nerve('baz') == [
                ('a.foo', {}), ('b.foo', {}), ('c.foo', {}),
            ]

    def test_get_services_running_here_for_nerve(self):
        cluster = 'plentea'
        soa_dir = 'boba'
        fake_marathon_services = [('never', 'again'), ('will', 'he')]
        fake_classic_services = [('walk', 'on'), ('his', 'feet')]
        expected = fake_marathon_services + fake_classic_services
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_marathon_services_running_here_for_nerve',
                       autospec=True,
                       return_value=fake_marathon_services),
            mock.patch('paasta_tools.marathon_tools.get_classic_services_running_here_for_nerve',
                       autospec=True,
                       return_value=fake_classic_services),
        ) as (
            marathon_patch,
            classic_patch
        ):
            actual = marathon_tools.get_services_running_here_for_nerve(cluster, soa_dir)
            assert expected == actual
            marathon_patch.assert_called_once_with(cluster, soa_dir)
            classic_patch.assert_called_once_with(soa_dir)

    def test_format_marathon_app_dict(self):
        fake_url = 'dockervania_from_konami'
        fake_volumes = [
            {
                'hostPath': '/var/data/a',
                'containerPath': '/etc/a',
                'mode': 'RO',
            },
            {
                'hostPath': '/var/data/b',
                'containerPath': '/etc/b',
                'mode': 'RW',
            },
        ]
        fake_mem = 1000000000000000000000
        fake_env = {'FAKEENV': 'FAKEVALUE'}
        expected_env = {
            'FAKEENV': 'FAKEVALUE',
            'PAASTA_CLUSTER': '',
            'PAASTA_INSTANCE': 'yes_i_can',
            'PAASTA_SERVICE': 'can_you_dig_it',
            'PAASTA_DOCKER_IMAGE': '',
        }
        fake_cpus = .42
        fake_disk = 1234.5
        fake_instances = 101
        fake_cmd = None
        fake_args = ['arg1', 'arg2']
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({
            'mode': 'http',
            'healthcheck_uri': '/health',
            'discover': 'habitat',
        })
        fake_healthchecks = [
            {
                "protocol": "HTTP",
                "path": "/health",
                "gracePeriodSeconds": 3,
                "intervalSeconds": 10,
                "portIndex": 0,
                "timeoutSeconds": 10,
                "maxConsecutiveFailures": 3
            },
        ]
        fake_period = 200000
        fake_burst = 200
        fake_cpu_quota = fake_cpus * fake_period * (100 + fake_burst) / 100

        expected_conf = {
            'id': mock.ANY,
            'container': {
                'docker': {
                    'image': fake_url,
                    'network': 'BRIDGE',
                    'portMappings': [
                        {
                            'containerPort': marathon_tools.CONTAINER_PORT,
                            'hostPort': 0,
                            'protocol': 'tcp',
                        },
                    ],
                    'parameters': [
                        {'key': 'memory-swap', 'value': "%sm" % int(fake_mem)},
                        {"key": "cpu-period", "value": "%s" % int(fake_period)},
                        {"key": "cpu-quota", "value": "%s" % int(fake_cpu_quota)},
                    ]
                },
                'type': 'DOCKER',
                'volumes': fake_volumes,
            },
            'constraints': [["habitat", "GROUP_BY", "1"], ["pool", "LIKE", "default"]],
            'uris': ['file:///root/.dockercfg', ],
            'mem': fake_mem,
            'env': expected_env,
            'cpus': fake_cpus,
            'disk': fake_disk,
            'instances': fake_instances,
            'cmd': fake_cmd,
            'args': fake_args,
            'health_checks': fake_healthchecks,
            'backoff_factor': 2,
            'backoff_seconds': mock.ANY,
            'max_launch_delay_seconds': 300,
            'accepted_resource_roles': ['ads'],
        }
        config = marathon_tools.MarathonServiceConfig(
            service='can_you_dig_it',
            cluster='',
            instance='yes_i_can',
            config_dict={
                'env': fake_env,
                'mem': fake_mem,
                'cpus': fake_cpus,
                'disk': fake_disk,
                'instances': fake_instances,
                'cmd': fake_cmd,
                'args': fake_args,
                'cfs_period_us': fake_period,
                'cpu_burst_pct': fake_burst,
                'healthcheck_grace_period_seconds': 3,
                'healthcheck_interval_seconds': 10,
                'healthcheck_timeout_seconds': 10,
                'healthcheck_max_consecutive_failures': 3,
                'accepted_resource_roles': ['ads'],
            },
            branch_dict={'desired_state': 'start'}
        )

        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_mesos_slaves_grouped_by_attribute', autospec=True,
                       return_value={'fake_region': [{}]}),
            mock.patch('paasta_tools.marathon_tools.get_slaves', autospec=True, return_value=[{}]),
            mock.patch('paasta_tools.marathon_tools.filter_mesos_slaves_by_blacklist',
                       autospec=True, return_value=[{}]),
            mock.patch('paasta_tools.marathon_tools.get_docker_url', autospec=True, return_value=fake_url),
            mock.patch('paasta_tools.marathon_tools.load_service_namespace_config', autospec=True,
                       return_value=fake_service_namespace_config),
            mock.patch('paasta_tools.marathon_tools.load_system_paasta_config', autospec=True,
                       return_value=mock.Mock(get_volumes=mock.Mock(return_value=fake_volumes),
                                              get_dockercfg_location=mock.Mock(
                                                  return_value='file:///root/.dockercfg'))),
        ) as (
            _,
            _,
            _,
            _,
            _,
            _,
        ):
            actual = config.format_marathon_app_dict()
            assert actual == expected_conf

            # Assert that the complete config can be inserted into the MarathonApp model
            assert MarathonApp(**actual)

    def test_instances_is_zero_when_desired_state_is_stop(self):
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='',
            instance='fake_instance',
            config_dict={'instances': 10},
            branch_dict={'desired_state': 'stop'},
        )
        assert fake_conf.get_instances() == 0

    def test_get_bounce_method_in_config(self):
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='',
            instance='fake_instance',
            config_dict={'bounce_method': 'aaargh'},
            branch_dict={})
        assert fake_conf.get_bounce_method() == 'aaargh'

    def test_get_bounce_method_default(self):
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_bounce_method() == 'crossover'

    def test_get_bounce_health_params_in_config(self):
        fake_param = 'fake_param'
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'bounce_health_params': fake_param},
            branch_dict={},
        )
        assert fake_conf.get_bounce_health_params(mock.Mock()) == fake_param

    def test_get_bounce_health_params_default_when_not_in_smartstack(self):
        fake_service_namespace_config = mock.Mock(is_in_smartstack=mock.Mock(return_value=False))
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_bounce_health_params(fake_service_namespace_config) == {}

    def test_get_bounce_health_params_default_when_in_smartstack(self):
        fake_service_namespace_config = mock.Mock(is_in_smartstack=mock.Mock(return_value=True))
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_bounce_health_params(fake_service_namespace_config) == {'check_haproxy': True}

    def test_get_drain_method_in_config(self):
        fake_param = 'fake_param'
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'drain_method': fake_param},
            branch_dict={},
        )
        assert fake_conf.get_drain_method(mock.Mock()) == fake_param

    def test_get_drain_method_default_when_not_in_smartstack(self):
        fake_service_namespace_config = mock.Mock(is_in_smartstack=mock.Mock(return_value=False))
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_drain_method(fake_service_namespace_config) == 'noop'

    def test_get_drain_method_default_when_in_smartstack(self):
        fake_service_namespace_config = mock.Mock(is_in_smartstack=mock.Mock(return_value=True))
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_drain_method(fake_service_namespace_config) == 'hacheck'

    def test_get_drain_method_params_in_config(self):
        fake_param = 'fake_param'
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'drain_method_params': fake_param},
            branch_dict={},
        )
        assert fake_conf.get_drain_method_params(mock.Mock()) == fake_param

    def test_get_drain_method_params_default_when_not_in_smartstack(self):
        fake_service_namespace_config = mock.Mock(is_in_smartstack=mock.Mock(return_value=False))
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_drain_method_params(fake_service_namespace_config) == {}

    def test_get_drain_method_params_default_when_in_smartstack(self):
        fake_service_namespace_config = mock.Mock(is_in_smartstack=mock.Mock(return_value=True))
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_drain_method_params(fake_service_namespace_config) == {'delay': 60}

    def test_get_instances_in_config(self):
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'instances': -10},
            branch_dict={'desired_state': 'start'},
        )
        assert fake_conf.get_instances() == -10

    def test_get_instances_default(self):
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        assert fake_conf.get_instances() == 1

    def test_get_instances_respects_false(self):
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'instances': False},
            branch_dict={'desired_state': 'start'},
        )
        assert fake_conf.get_instances() == 0

    def test_get_calculated_constraints_in_config_override_all_others(self):
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'constraints': [['something', 'GROUP_BY']], 'extra_constraints': [['ignore', 'this']]},
            branch_dict={},
        )
        with mock.patch(
            'paasta_tools.marathon_tools.get_mesos_slaves_grouped_by_attribute',
            autospec=True,
        ) as get_slaves_patch:
            assert fake_conf.get_calculated_constraints(fake_service_namespace_config) == [['something', 'GROUP_BY']]
            assert get_slaves_patch.call_count == 0

    def test_get_calculated_constraints_default(self):
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_mesos_slaves_grouped_by_attribute', autospec=True),
            mock.patch('paasta_tools.marathon_tools.get_slaves', autospec=True, return_value=[{}]),
            mock.patch('paasta_tools.marathon_tools.filter_mesos_slaves_by_blacklist',
                       autospec=True, return_value=[{}]),
        ) as (
            get_slaves_patch,
            _,
            _,
        ):
            get_slaves_patch.return_value = {'fake_region': [{}]}
            expected_constraints = [
                ["region", "GROUP_BY", "1"],
                ["pool", "LIKE", "default"],
            ]
            assert fake_conf.get_calculated_constraints(fake_service_namespace_config) == expected_constraints

    def test_get_calculated_constraints_stringifies(self):
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'extra_constraints': [['foo', 1]]},
            branch_dict={},
        )
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_mesos_slaves_grouped_by_attribute', autospec=True),
            mock.patch('paasta_tools.marathon_tools.get_slaves', autospec=True, return_value=[{}]),
            mock.patch('paasta_tools.marathon_tools.filter_mesos_slaves_by_blacklist',
                       autospec=True, return_value=[{}]),
        ) as (
            get_slaves_patch,
            _,
            _,
        ):
            get_slaves_patch.return_value = {'fake_region': {}}
            expected_constraints = [
                ["foo", "1"],
                ["region", "GROUP_BY", "1"],
                ["pool", "LIKE", "default"],
            ]
            assert fake_conf.get_calculated_constraints(fake_service_namespace_config) == expected_constraints

    def test_get_calculated_constraints_extra_constraints(self):
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'extra_constraints': [['extra', 'constraint']]},
            branch_dict={},
        )
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_mesos_slaves_grouped_by_attribute', autospec=True),
            mock.patch('paasta_tools.marathon_tools.get_slaves', autospec=True, return_value=[{}]),
            mock.patch('paasta_tools.marathon_tools.filter_mesos_slaves_by_blacklist',
                       autospec=True, return_value=[{}]),
        ) as (
            get_slaves_patch,
            _,
            _,
        ):
            get_slaves_patch.return_value = {'fake_region': {}}
            expected_constraints = [
                ['extra', 'constraint'],
                ["region", "GROUP_BY", "1"],
                ["pool", "LIKE", "default"],
            ]
            assert fake_conf.get_calculated_constraints(fake_service_namespace_config) == expected_constraints

    def test_get_calculated_constraints_from_discover(self):
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({
            'mode': 'http',
            'healthcheck_uri': '/status',
            'discover': 'habitat',
        })
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_mesos_slaves_grouped_by_attribute', autospec=True),
            mock.patch('paasta_tools.marathon_tools.get_slaves', autospec=True, return_value=[{}]),
            mock.patch('paasta_tools.marathon_tools.filter_mesos_slaves_by_blacklist',
                       autospec=True, return_value=[{}]),
        ) as (
            get_slaves_patch,
            _,
            _,
        ):
            get_slaves_patch.return_value = {'fake_region': {}, 'fake_other_region': {}}
            expected_constraints = [
                ["habitat", "GROUP_BY", "2"],
                ["pool", "LIKE", "default"],
            ]
            assert fake_conf.get_calculated_constraints(fake_service_namespace_config) == expected_constraints
            get_slaves_patch.assert_called_once_with([{}], 'habitat')

    def test_get_calculated_constraints_respects_deploy_blacklist(self):
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()
        fake_deploy_blacklist = [["region", "fake_blacklisted_region"]]
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'deploy_blacklist': fake_deploy_blacklist},
            branch_dict={},
        )
        expected_constraints = [
            ["region", "GROUP_BY", "1"],
            ["region", "UNLIKE", "fake_blacklisted_region"],
            ["pool", "LIKE", "default"],
        ]
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_mesos_slaves_grouped_by_attribute', autospec=True),
            mock.patch('paasta_tools.marathon_tools.get_slaves', autospec=True, return_value=[{}]),
            mock.patch('paasta_tools.marathon_tools.filter_mesos_slaves_by_blacklist',
                       autospec=True, return_value=[{}]),
        ) as (
            get_slaves_patch,
            _,
            _,
        ):
            get_slaves_patch.return_value = {'fake_region': {}}
            assert fake_conf.get_calculated_constraints(fake_service_namespace_config) == expected_constraints
            get_slaves_patch.assert_called_once_with([{}], 'region')

    def test_get_calculated_constraints_respects_deploy_whitelist(self):
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()
        fake_deploy_whitelist = ["region", ["fake_whitelisted_region"]]
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'deploy_whitelist': fake_deploy_whitelist},
            branch_dict={},
        )
        expected_constraints = [
            ["region", "GROUP_BY", "1"],
            ["region", "LIKE", "fake_whitelisted_region"],
            ["pool", "LIKE", "default"],
        ]
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_mesos_slaves_grouped_by_attribute', autospec=True),
            mock.patch('paasta_tools.marathon_tools.get_slaves', autospec=True, return_value=[{}]),
            mock.patch('paasta_tools.marathon_tools.filter_mesos_slaves_by_blacklist',
                       autospec=True, return_value=[{}]),
        ) as (
            get_slaves_patch,
            _,
            _,
        ):
            get_slaves_patch.return_value = {'fake_region': {}}
            assert fake_conf.get_calculated_constraints(fake_service_namespace_config) == expected_constraints
            get_slaves_patch.assert_called_once_with([{}], 'region')

    def test_instance_config_getters_in_config(self):
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'monitoring': 'test'},
            branch_dict={},
        )
        assert fake_conf.get_monitoring() == 'test'

    def test_get_marathon_client(self):
        fake_url = "nothing_for_me_to_do_but_dance"
        fake_user = "the_boogie"
        fake_passwd = "is_for_real"
        with mock.patch('paasta_tools.marathon_tools.MarathonClient', autospec=True) as client_patch:
            marathon_tools.get_marathon_client(fake_url, fake_user, fake_passwd)
            client_patch.assert_called_once_with(fake_url, fake_user, fake_passwd, timeout=30, session=mock.ANY)
            args, kwargs = client_patch.call_args
            assert 'User-Agent' in kwargs['session'].headers
            assert 'PaaSTA' in kwargs['session'].headers['User-Agent']

    def test_list_all_marathon_app_ids(self):
        fakeapp1 = mock.Mock(id='/fake_app1')
        fakeapp2 = mock.Mock(id='/fake_app2')
        apps = [fakeapp1, fakeapp2]
        list_apps_mock = mock.Mock(return_value=apps)
        fake_client = mock.Mock(list_apps=list_apps_mock)
        expected_apps = ['fake_app1', 'fake_app2']
        assert marathon_tools.list_all_marathon_app_ids(fake_client) == expected_apps

    def test_is_app_id_running_true(self):
        fake_id = 'fake_app1'
        fake_all_marathon_app_ids = ['fake_app1', 'fake_app2']
        fake_client = mock.Mock()
        with mock.patch(
            'paasta_tools.marathon_tools.list_all_marathon_app_ids',
            autospec=True,
            return_value=fake_all_marathon_app_ids,
        ) as list_all_marathon_app_ids_patch:
            assert marathon_tools.is_app_id_running(fake_id, fake_client) is True
            list_all_marathon_app_ids_patch.assert_called_once_with(fake_client)

    def test_is_app_id_running_false(self):
        fake_id = 'fake_app3'
        fake_all_marathon_app_ids = ['fake_app1', 'fake_app2']
        fake_client = mock.Mock()
        with mock.patch(
            'paasta_tools.marathon_tools.list_all_marathon_app_ids',
            autospec=True,
            return_value=fake_all_marathon_app_ids,
        ) as list_all_marathon_app_ids_patch:
            assert marathon_tools.is_app_id_running(fake_id, fake_client) is False
            list_all_marathon_app_ids_patch.assert_called_once_with(fake_client)

    def test_is_app_id_running_handles_leading_slashes(self):
        fake_id = '/fake_app1'
        fake_all_marathon_app_ids = ['fake_app1', 'fake_app2']
        fake_client = mock.Mock()
        with mock.patch(
            'paasta_tools.marathon_tools.list_all_marathon_app_ids',
            autospec=True,
            return_value=fake_all_marathon_app_ids,
        ) as list_all_marathon_app_ids_patch:
            assert marathon_tools.is_app_id_running(fake_id, fake_client) is True
            list_all_marathon_app_ids_patch.assert_called_once_with(fake_client)

    @patch('paasta_tools.marathon_tools.MarathonClient.list_tasks')
    def test_app_has_tasks_exact(self, patch_list_tasks):
        fake_client = mock.Mock()
        fake_client.list_tasks = patch_list_tasks
        patch_list_tasks.return_value = [{}, {}, {}]
        assert marathon_tools.app_has_tasks(fake_client, 'fake_app', 3) is True
        assert marathon_tools.app_has_tasks(fake_client, 'fake_app', 3, exact_matches_only=True) is True

    @patch('paasta_tools.marathon_tools.MarathonClient.list_tasks')
    def test_app_has_tasks_less(self, patch_list_tasks):
        fake_client = mock.Mock()
        fake_client.list_tasks = patch_list_tasks
        patch_list_tasks.return_value = [{}, {}, {}]
        assert marathon_tools.app_has_tasks(fake_client, 'fake_app', 2) is True
        assert marathon_tools.app_has_tasks(fake_client, 'fake_app', 2, exact_matches_only=True) is False

    @patch('paasta_tools.marathon_tools.MarathonClient.list_tasks')
    def test_app_has_tasks_more(self, patch_list_tasks):
        fake_client = mock.Mock()
        fake_client.list_tasks = patch_list_tasks
        patch_list_tasks.return_value = [{}, {}, {}]
        assert marathon_tools.app_has_tasks(fake_client, 'fake_app', 4) is False
        assert marathon_tools.app_has_tasks(fake_client, 'fake_app', 4, exact_matches_only=True) is False

    @patch('paasta_tools.marathon_tools.MarathonClient.list_tasks')
    def test_add_leading_slash(self, patch_list_tasks):
        fake_client = mock.Mock()
        fake_client.list_tasks = patch_list_tasks
        marathon_tools.app_has_tasks(fake_client, 'fake_app', 4)
        assert patch_list_tasks.called_with('/fake_app')

    def test_get_code_sha_from_dockerurl(self):
        fake_docker_url = 'docker-paasta.yelpcorp.com:443/services-cieye:paasta-93340779404579'
        actual = marathon_tools.get_code_sha_from_dockerurl(fake_docker_url)
        assert actual == 'git93340779'
        assert len(actual) == 11

    def test_get_config_hash(self):
        test_input = {'foo': 'bar'}
        actual = marathon_tools.get_config_hash(test_input)
        expected = 'config94232c5b'
        assert actual == expected
        assert len(actual) == 14

    def test_id_changes_when_force_bounce(self):
        fake_name = 'fakeapp'
        fake_instance = 'fakeinstance'
        fake_url = 'dockervania_from_konami'
        fake_cluster = 'fake_cluster'
        fake_system_paasta_config = SystemPaastaConfig({
            'cluster': fake_cluster,
            'volumes': [],
            'docker_registry': 'fake_registry'
        }, '/fake/dir/')

        fake_service_config_1 = marathon_tools.MarathonServiceConfig(
            service=fake_name,
            cluster='fake_cluster',
            instance=fake_instance,
            config_dict=self.fake_marathon_app_config.config_dict,
            branch_dict={
                'desired_state': 'start',
                'force_bounce': '88888',
            },
        )

        fake_service_config_2 = marathon_tools.MarathonServiceConfig(
            service=fake_name,
            cluster='fake_cluster',
            instance=fake_instance,
            config_dict=self.fake_marathon_app_config.config_dict,
            branch_dict={
                'desired_state': 'start',
                'force_bounce': '99999',
            },
        )

        fake_service_config_3 = marathon_tools.MarathonServiceConfig(
            service=fake_name,
            cluster='fake_cluster',
            instance=fake_instance,
            config_dict=self.fake_marathon_app_config.config_dict,
            branch_dict={
                'desired_state': 'stop',
                'force_bounce': '99999',
            },
        )

        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.load_system_paasta_config',
                       autospec=True, return_value=fake_system_paasta_config),
            mock.patch('paasta_tools.marathon_tools.get_docker_url', autospec=True, return_value=fake_url),
            mock.patch('paasta_tools.marathon_tools.load_service_namespace_config', autospec=True,
                       return_value=self.fake_service_namespace_config),
            mock.patch('paasta_tools.marathon_tools.get_mesos_slaves_grouped_by_attribute',
                       autospec=True, return_value={'fake_region': {}}),
            mock.patch('paasta_tools.marathon_tools.get_slaves', autospec=True, return_value=[{}]),
            mock.patch('paasta_tools.marathon_tools.filter_mesos_slaves_by_blacklist',
                       autospec=True, return_value=[{}]),
        ) as (
            load_system_paasta_config_patch,
            docker_url_patch,
            _,
            _,
            _,
            _
        ):
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(return_value=fake_cluster)
            first_id = fake_service_config_1.format_marathon_app_dict()['id']
            first_id_2 = fake_service_config_1.format_marathon_app_dict()['id']
            # just for sanity, make sure that the app_id is idempotent.
            assert first_id == first_id_2

            second_id = fake_service_config_2.format_marathon_app_dict()['id']
            assert first_id != second_id

            third_id = fake_service_config_3.format_marathon_app_dict()['id']
            assert second_id == third_id

    def test_get_routing_constraints_no_slaves(self):
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_slaves', autospec=True, return_value=[])
        ):
            fake_service_config = marathon_tools.MarathonServiceConfig(
                service='fake_name',
                cluster='fake_cluster',
                instance='fake_instance',
                config_dict=self.fake_marathon_app_config.config_dict,
                branch_dict={
                    'desired_state': 'stop',
                    'force_bounce': '99999',
                },
            )
            with raises(NoSlavesAvailableError) as e:
                fake_service_config.get_routing_constraints(self.fake_service_namespace_config)
                assert e.value.message == (
                    "No suitable slaves could be found in the cluster for fake_name.fake_instance"
                    "There are 0 total slaves in the cluster, but after filtering"
                    " those available to the app according to the constraints set"
                    " by the deploy_blacklist and deploy_whitelist, there are 0"
                    " available."
                )

    def test_get_routing_constraints_no_slaves_after_filter(self):
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_slaves', autospec=True, return_value=[{}]),
            mock.patch('paasta_tools.marathon_tools.filter_mesos_slaves_by_blacklist',
                       autospec=True, return_value=[]),
        ) as (
            _,
            _
        ):
            fake_service_config = marathon_tools.MarathonServiceConfig(
                service='fake_name',
                cluster='fake_cluster',
                instance='fake_instance',
                config_dict=self.fake_marathon_app_config.config_dict,
                branch_dict={
                    'desired_state': 'stop',
                    'force_bounce': '99999',
                },
            )
            with raises(NoSlavesAvailableError) as e:
                fake_service_config.get_routing_constraints(self.fake_service_namespace_config)
                assert e.value.message == (
                    "No suitable slaves could be found in the cluster for fake_name.fake_instance"
                    "There are 1 total slaves in the cluster, but after filtering"
                    " those available to the app according to the constraints set"
                    " by the deploy_blacklist and deploy_whitelist, there are 0"
                    " available."
                )

    def test_get_expected_instance_count_for_namespace(self):
        service = 'red'
        namespace = 'rojo'
        soa_dir = 'que_esta'
        fake_instances = [(service, 'blue'), (service, 'green')]
        fake_srv_config = marathon_tools.MarathonServiceConfig(
            service=service,
            cluster='fake_cluster',
            instance='blue',
            config_dict={'nerve_ns': 'rojo', 'instances': 11},
            branch_dict={},
        )

        def config_helper(name, inst, cluster, soa_dir=None):
            if inst == 'blue':
                return fake_srv_config
            else:
                return marathon_tools.MarathonServiceConfig(
                    service=service,
                    cluster='fake_cluster',
                    instance='green',
                    config_dict={'nerve_ns': 'amarillo'},
                    branch_dict={},
                )

        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_service_instance_list',
                       autospec=True,
                       return_value=fake_instances),
            mock.patch('paasta_tools.marathon_tools.load_marathon_service_config',
                       autospec=True,
                       side_effect=config_helper),
        ) as (
            inst_list_patch,
            read_config_patch,
        ):
            actual = marathon_tools.get_expected_instance_count_for_namespace(
                service,
                namespace,
                cluster='fake_cluster',
                soa_dir=soa_dir,
            )
            assert actual == 11
            inst_list_patch.assert_called_once_with(service,
                                                    cluster='fake_cluster',
                                                    instance_type='marathon',
                                                    soa_dir=soa_dir)
            read_config_patch.assert_any_call(service, 'blue', 'fake_cluster', soa_dir=soa_dir)
            read_config_patch.assert_any_call(service, 'green', 'fake_cluster', soa_dir=soa_dir)

    def test_get_matching_appids(self):
        apps = [
            mock.Mock(id='/fake--service.fake--instance.bouncingold'),
            mock.Mock(id='/fake--service.fake--instance.bouncingnew'),
            mock.Mock(id='/fake--service.other--instance.bla'),
            mock.Mock(id='/other--service'),
            mock.Mock(id='/fake--service.fake--instance--with--suffix.something'),
            mock.Mock(id='/prefixed--fake--service.fake--instance.something'),
        ]

        list_apps_mock = mock.Mock(return_value=apps)
        fake_client = mock.Mock(list_apps=list_apps_mock)
        expected = [
            '/fake--service.fake--instance.bouncingold',
            '/fake--service.fake--instance.bouncingnew',
        ]
        actual = marathon_tools.get_matching_appids('fake_service', 'fake_instance', fake_client)
        assert actual == expected

    def test_get_healthcheck_cmd_happy(self):
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'healthcheck_cmd': 'test_cmd'},
            branch_dict={},
        )
        actual = fake_conf.get_healthcheck_cmd()
        assert actual == 'test_cmd'

    def test_get_healthcheck_cmd_raises_when_unset(self):
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='fake_name',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        )
        with raises(marathon_tools.InvalidInstanceConfig) as exc:
            fake_conf.get_healthcheck_cmd()
        assert "healthcheck mode 'cmd' requires a healthcheck_cmd to run" in str(exc.value)

    def test_get_healthcheck_for_instance_http(self):
        fake_service = 'fake_service'
        fake_namespace = 'fake_namespace'
        fake_hostname = 'fake_hostname'
        fake_random_port = 666

        fake_path = '/fake_path'
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
            service=fake_service,
            cluster='fake_cluster',
            instance=fake_namespace,
            config_dict={},
            branch_dict={},
        )
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({
            'mode': 'http',
            'healthcheck_uri': fake_path,
        })
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.load_marathon_service_config',
                       autospec=True,
                       return_value=fake_marathon_service_config),
            mock.patch('paasta_tools.marathon_tools.load_service_namespace_config',
                       autospec=True,
                       return_value=fake_service_namespace_config),
            mock.patch('socket.getfqdn', autospec=True, return_value=fake_hostname),
        ) as (
            read_config_patch,
            load_service_namespace_config_patch,
            hostname_patch
        ):
            expected = ('http', 'http://%s:%d%s' % (fake_hostname, fake_random_port, fake_path))
            actual = marathon_tools.get_healthcheck_for_instance(
                fake_service, fake_namespace, fake_marathon_service_config, fake_random_port)
            assert expected == actual

    def test_get_healthcheck_for_instance_tcp(self):
        fake_service = 'fake_service'
        fake_namespace = 'fake_namespace'
        fake_hostname = 'fake_hostname'
        fake_random_port = 666

        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
            service=fake_service,
            cluster='fake_cluster',
            instance=fake_namespace,
            config_dict={},
            branch_dict={},
        )
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({
            'mode': 'tcp',
        })
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.load_marathon_service_config',
                       autospec=True,
                       return_value=fake_marathon_service_config),
            mock.patch('paasta_tools.marathon_tools.load_service_namespace_config',
                       autospec=True,
                       return_value=fake_service_namespace_config),
            mock.patch('socket.getfqdn', autospec=True, return_value=fake_hostname),
        ) as (
            read_config_patch,
            load_service_namespace_config_patch,
            hostname_patch
        ):
            expected = ('tcp', 'tcp://%s:%d' % (fake_hostname, fake_random_port))
            actual = marathon_tools.get_healthcheck_for_instance(
                fake_service, fake_namespace, fake_marathon_service_config, fake_random_port)
            assert expected == actual

    def test_get_healthcheck_for_instance_cmd(self):
        fake_service = 'fake_service'
        fake_namespace = 'fake_namespace'
        fake_hostname = 'fake_hostname'
        fake_random_port = 666
        fake_cmd = '/bin/fake_command'
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
            service=fake_service,
            cluster='fake_cluster',
            instance=fake_namespace,
            config_dict={
                'healthcheck_mode': 'cmd',
                'healthcheck_cmd': fake_cmd
            },
            branch_dict={},
        )
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({})
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.load_marathon_service_config',
                       autospec=True,
                       return_value=fake_marathon_service_config),
            mock.patch('paasta_tools.marathon_tools.load_service_namespace_config',
                       autospec=True,
                       return_value=fake_service_namespace_config),
            mock.patch('socket.getfqdn', autospec=True, return_value=fake_hostname),
        ) as (
            read_config_patch,
            load_service_namespace_config_patch,
            hostname_patch
        ):
            expected = ('cmd', fake_cmd)
            actual = marathon_tools.get_healthcheck_for_instance(
                fake_service, fake_namespace, fake_marathon_service_config, fake_random_port)
            assert expected == actual

    def test_get_healthcheck_for_instance_other(self):
        fake_service = 'fake_service'
        fake_namespace = 'fake_namespace'
        fake_hostname = 'fake_hostname'
        fake_random_port = 666
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
            service=fake_service,
            cluster='fake_cluster',
            instance=fake_namespace,
            config_dict={
                'healthcheck_mode': None,
            },
            branch_dict={},
        )
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({})
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.load_marathon_service_config',
                       autospec=True,
                       return_value=fake_marathon_service_config),
            mock.patch('paasta_tools.marathon_tools.load_service_namespace_config',
                       autospec=True,
                       return_value=fake_service_namespace_config),
            mock.patch('socket.getfqdn', autospec=True, return_value=fake_hostname),
        ) as (
            read_config_patch,
            load_service_namespace_config_patch,
            hostname_patch
        ):
            expected = (None, None)
            actual = marathon_tools.get_healthcheck_for_instance(
                fake_service, fake_namespace, fake_marathon_service_config, fake_random_port)
            assert expected == actual

    def test_get_healthcheck_for_instance_custom_soadir(self):
        fake_service = 'fake_service'
        fake_namespace = 'fake_namespace'
        fake_hostname = 'fake_hostname'
        fake_random_port = 666
        fake_soadir = '/fake/soadir'
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
            service=fake_service,
            cluster='fake_cluster',
            instance=fake_namespace,
            config_dict={
                'healthcheck_mode': None,
            },
            branch_dict={},
        )
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({})
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.load_marathon_service_config',
                       autospec=True,
                       return_value=fake_marathon_service_config),
            mock.patch('paasta_tools.marathon_tools.load_service_namespace_config',
                       autospec=True,
                       return_value=fake_service_namespace_config),
            mock.patch('socket.getfqdn', autospec=True, return_value=fake_hostname),
        ) as (
            read_config_patch,
            load_service_namespace_config_patch,
            hostname_patch
        ):
            expected = (None, None)
            actual = marathon_tools.get_healthcheck_for_instance(
                fake_service, fake_namespace, fake_marathon_service_config, fake_random_port, soa_dir=fake_soadir)
            assert expected == actual
            load_service_namespace_config_patch.assert_called_once_with(fake_service, fake_namespace, fake_soadir)


class TestMarathonServiceConfig(object):

    def test_repr(self):
        actual = repr(marathon_tools.MarathonServiceConfig('foo', 'bar', '', {'baz': 'baz'}, {'bubble': 'gum'}))
        expected = """MarathonServiceConfig('foo', 'bar', '', {'baz': 'baz'}, {'bubble': 'gum'})"""
        assert actual == expected

    def test_get_healthcheck_mode_default(self):
        namespace_config = marathon_tools.ServiceNamespaceConfig({})
        marathon_config = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={},
            branch_dict={},
        )
        assert marathon_config.get_healthcheck_mode(namespace_config) is None

    def test_get_healthcheck_mode_default_from_namespace_config(self):
        namespace_config = marathon_tools.ServiceNamespaceConfig({'proxy_port': 1234})
        marathon_config = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={},
            branch_dict={},
        )
        assert marathon_config.get_healthcheck_mode(namespace_config) == 'http'

    def test_get_healthcheck_mode_valid(self):
        namespace_config = marathon_tools.ServiceNamespaceConfig({})
        marathon_config = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={'healthcheck_mode': 'tcp'},
            branch_dict={},
        )
        assert marathon_config.get_healthcheck_mode(namespace_config) == 'tcp'

    def test_get_healthcheck_mode_invalid(self):
        namespace_config = marathon_tools.ServiceNamespaceConfig({})
        marathon_config = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={'healthcheck_mode': 'udp'},
            branch_dict={},
        )
        with raises(marathon_tools.InvalidMarathonHealthcheckMode):
            marathon_config.get_healthcheck_mode(namespace_config)

    def test_get_healthcheck_mode_explicit_none(self):
        namespace_config = marathon_tools.ServiceNamespaceConfig({})
        marathon_config = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={'healthcheck_mode': None},
            branch_dict={},
        )
        assert marathon_config.get_healthcheck_mode(namespace_config) is None

    def test_get_healthchecks_http_overrides(self):
        fake_path = '/mycoolstatus'
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
            service='service',
            instance='instance',
            cluster='cluster',
            config_dict={
                "healthcheck_mode": "http",  # Actually the default here, but I want to be specific.
                "healthcheck_uri": fake_path,
                "healthcheck_grace_period_seconds": 70,
                "healthcheck_interval_seconds": 12,
                "healthcheck_timeout_seconds": 13,
                "healthcheck_max_consecutive_failures": 7,
            },
            branch_dict={},
        )
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({
            'mode': 'http',
            'healthcheck_uri': fake_path,
        })
        expected = [
            {
                "protocol": "HTTP",
                "path": fake_path,
                "gracePeriodSeconds": 70,
                "intervalSeconds": 12,
                "portIndex": 0,
                "timeoutSeconds": 13,
                "maxConsecutiveFailures": 7,
            },
        ]

        actual = fake_marathon_service_config.get_healthchecks(fake_service_namespace_config)

        assert actual == expected

    def test_get_healthchecks_http_defaults(self):
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={},
            branch_dict={},
        )
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({'mode': 'http'})
        expected = [
            {
                "protocol": "HTTP",
                "path": '/status',
                "gracePeriodSeconds": 60,
                "intervalSeconds": 10,
                "portIndex": 0,
                "timeoutSeconds": 10,
                "maxConsecutiveFailures": 30
            },
        ]
        actual = fake_marathon_service_config.get_healthchecks(fake_service_namespace_config)
        assert actual == expected

    def test_get_healthchecks_tcp(self):
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={},
            branch_dict={},
        )
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({'mode': 'tcp'})
        expected = [
            {
                "protocol": "TCP",
                "gracePeriodSeconds": 60,
                "intervalSeconds": 10,
                "portIndex": 0,
                "timeoutSeconds": 10,
                "maxConsecutiveFailures": 30
            },
        ]
        actual = fake_marathon_service_config.get_healthchecks(fake_service_namespace_config)
        assert actual == expected

    def test_get_healthchecks_cmd(self):
        fake_command = '/fake_cmd'
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={'healthcheck_mode': 'cmd', 'healthcheck_cmd': fake_command},
            branch_dict={},
        )
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()
        expected = [
            {
                "protocol": "COMMAND",
                "command": {"value": fake_command},
                "gracePeriodSeconds": 60,
                "intervalSeconds": 10,
                "timeoutSeconds": 10,
                "maxConsecutiveFailures": 30
            },
        ]
        actual = fake_marathon_service_config.get_healthchecks(fake_service_namespace_config)
        assert actual == expected

    def test_get_healthchecks_cmd_overrides_timeout(self):
        fake_command = '/bin/fake_command'
        fake_timeout = 4
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={
                'healthcheck_mode': 'cmd',
                'healthcheck_timeout_seconds': fake_timeout,
                'healthcheck_cmd': fake_command},
            branch_dict={},
        )
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()
        expected = [
            {
                "protocol": "COMMAND",
                "command": {"value": fake_command},
                "gracePeriodSeconds": 60,
                "intervalSeconds": 10,
                "timeoutSeconds": fake_timeout,
                "maxConsecutiveFailures": 30
            },
        ]
        actual = fake_marathon_service_config.get_healthchecks(fake_service_namespace_config)
        assert actual == expected

    def test_get_healthchecks_empty(self):
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={},
            branch_dict={},
        )
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({})
        assert fake_marathon_service_config.get_healthchecks(fake_service_namespace_config) == []

    def test_get_healthchecks_invalid_mode(self):
        marathon_config = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={'healthcheck_mode': 'none'},
            branch_dict={},
        )
        namespace_config = marathon_tools.ServiceNamespaceConfig({})
        with raises(marathon_tools.InvalidMarathonHealthcheckMode):
            marathon_config.get_healthchecks(namespace_config)

    def test_get_backoff_seconds_scales_up(self):
        marathon_config = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={'instances': 100},
            branch_dict={},
        )
        assert marathon_config.get_backoff_seconds() == 1

    def test_get_backoff_seconds_scales_down(self):
        marathon_config = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={'instances': 1},
            branch_dict={},
        )
        assert marathon_config.get_backoff_seconds() == 10

    def test_get_backoff_doesnt_devide_by_zero(self):
        marathon_config = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={'instances': 0},
            branch_dict={},
        )
        assert marathon_config.get_backoff_seconds() == 1

    def test_get_accepted_resource_roles_default(self):
        marathon_config = marathon_tools.MarathonServiceConfig(
            service='service',
            instance='instance',
            cluster='cluster',
            config_dict={},
            branch_dict={},
        )
        assert marathon_config.get_accepted_resource_roles() is None

    def test_get_accepted_resource_roles(self):
        marathon_config = marathon_tools.MarathonServiceConfig(
            service='service',
            instance='instance',
            cluster='cluster',
            config_dict={"accepted_resource_roles": ["ads"]},
            branch_dict={},
        )
        assert marathon_config.get_accepted_resource_roles() == ["ads"]

    def test_get_desired_state_human(self):
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={},
            branch_dict={'desired_state': 'stop'},
        )
        assert 'Stopped' in desired_state_human(fake_conf.get_desired_state(), fake_conf.get_instances())

    def test_get_desired_state_human_started_with_instances(self):
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={'instances': 42},
            branch_dict={'desired_state': 'start'},
        )
        assert 'Started' in desired_state_human(fake_conf.get_desired_state(), fake_conf.get_instances())

    def test_get_desired_state_human_with_0_instances(self):
        fake_conf = marathon_tools.MarathonServiceConfig(
            service='service',
            cluster='cluster',
            instance='instance',
            config_dict={'instances': 0},
            branch_dict={'desired_state': 'start'},
        )
        assert 'Stopped' in desired_state_human(fake_conf.get_desired_state(), fake_conf.get_instances())


class TestServiceNamespaceConfig(object):

    def test_get_mode_default(self):
        assert marathon_tools.ServiceNamespaceConfig().get_mode() is None

    def test_get_mode_default_when_port_specified(self):
        config = {'proxy_port': 1234}
        assert marathon_tools.ServiceNamespaceConfig(config).get_mode() == 'http'

    def test_get_mode_valid(self):
        config = {'mode': 'tcp'}
        assert marathon_tools.ServiceNamespaceConfig(config).get_mode() == 'tcp'

    def test_get_mode_invalid(self):
        config = {'mode': 'paasta'}
        with raises(marathon_tools.InvalidSmartstackMode):
            marathon_tools.ServiceNamespaceConfig(config).get_mode()

    def test_get_healthcheck_uri_default(self):
        assert marathon_tools.ServiceNamespaceConfig().get_healthcheck_uri() == '/status'

    def test_get_discover_default(self):
        assert marathon_tools.ServiceNamespaceConfig().get_discover() == 'region'


def test_deformat_job_id():
    expected = ('ser_vice', 'in_stance', 'git_hash', 'config_hash')
    assert marathon_tools.deformat_job_id('ser--vice.in--stance.git--hash.config--hash') == expected


def test_format_marathon_app_dict_no_smartstack():
    service = "service"
    instance = "instance"
    fake_job_id = "service.instance.some.hash"
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service=service,
        cluster='clustername',
        instance=instance,
        config_dict={},
        branch_dict={'docker_image': 'abcdef'},
    )
    fake_system_paasta_config = SystemPaastaConfig({
        'volumes': [],
        'docker_registry': 'fake_docker_registry:443'
    }, '/fake/dir/')
    fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()

    with contextlib.nested(
        mock.patch(
            'paasta_tools.marathon_tools.load_service_namespace_config',
            return_value=fake_service_namespace_config,
        ),
        mock.patch('paasta_tools.marathon_tools.format_job_id', return_value=fake_job_id),
        mock.patch('paasta_tools.marathon_tools.load_system_paasta_config', return_value=fake_system_paasta_config),
        mock.patch('paasta_tools.marathon_tools.get_slaves', autospec=True, return_value=[{}]),
        mock.patch('paasta_tools.marathon_tools.filter_mesos_slaves_by_blacklist', autospec=True, return_value=[{}]),
        mock.patch('paasta_tools.marathon_tools.get_mesos_slaves_grouped_by_attribute',
                   autospec=True, return_value={'fake_region': [{}]}),
        mock.patch('paasta_tools.marathon_tools.load_system_paasta_config', return_value=fake_system_paasta_config),
    ) as (
        mock_load_service_namespace_config,
        mock_format_job_id,
        _,
        _,
        _,
        _,
        _,
    ):
        actual = fake_marathon_service_config.format_marathon_app_dict()
        expected = {
            'container': {
                'docker': {
                    'portMappings': [{'protocol': 'tcp', 'containerPort': 8888, 'hostPort': 0}],
                    'image': 'fake_docker_registry:443/abcdef',
                    'network': 'BRIDGE',
                    'parameters': [
                        {'key': 'memory-swap', 'value': '1024m'},
                        {"key": "cpu-period", "value": '100000'},
                        {"key": "cpu-quota", "value": '250000'},
                    ]
                },
                'type': 'DOCKER',
                'volumes': [],
            },
            'instances': 1,
            'mem': 1024,
            'cmd': None,
            'args': [],
            'backoff_factor': 2,
            'backoff_seconds': mock.ANY,
            'max_launch_delay_seconds': 300,
            'cpus': 0.25,
            'disk': 1024.0,
            'uris': ['file:///root/.dockercfg'],
            'health_checks': [],
            'env': mock.ANY,
            'id': fake_job_id,
            'constraints': [["region", "GROUP_BY", "1"], ["pool", "LIKE", "default"]],
        }
        assert actual == expected

        # Assert that the complete config can be inserted into the MarathonApp model
        assert MarathonApp(**actual)


def test_format_marathon_app_dict_with_smartstack():
    service = "service"
    instance = "instance"
    fake_job_id = "service.instance.some.hash"
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service=service,
        cluster='clustername',
        instance=instance,
        config_dict={},
        branch_dict={'docker_image': 'abcdef'},
    )
    fake_system_paasta_config = SystemPaastaConfig({
        'volumes': [],
        'docker_registry': 'fake_docker_registry:443'
    }, '/fake/dir/')
    fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({'proxy_port': 9001})

    with contextlib.nested(
        mock.patch(
            'paasta_tools.marathon_tools.load_service_namespace_config',
            return_value=fake_service_namespace_config,
        ),
        mock.patch('paasta_tools.marathon_tools.format_job_id', return_value=fake_job_id),
        mock.patch('paasta_tools.marathon_tools.get_mesos_slaves_grouped_by_attribute',
                   autospec=True, return_value={'fake_region': {}}),
        mock.patch('paasta_tools.marathon_tools.get_slaves', autospec=True, return_value=[{}]),
        mock.patch('paasta_tools.marathon_tools.filter_mesos_slaves_by_blacklist', autospec=True, return_value=[{}]),
        mock.patch('paasta_tools.marathon_tools.load_system_paasta_config', return_value=fake_system_paasta_config),
    ) as (
        mock_load_service_namespace_config,
        mock_format_job_id,
        mock_system_paasta_config,
        _,
        _,
        _
    ):
        actual = fake_marathon_service_config.format_marathon_app_dict()
        expected = {
            'container': {
                'docker': {
                    'portMappings': [{'protocol': 'tcp', 'containerPort': 8888, 'hostPort': 0}],
                    'image': 'fake_docker_registry:443/abcdef',
                    'network': 'BRIDGE',
                    'parameters': [
                        {'key': 'memory-swap', 'value': '1024m'},
                        {"key": "cpu-period", "value": '100000'},
                        {"key": "cpu-quota", "value": '250000'},
                    ]
                },
                'type': 'DOCKER',
                'volumes': [],
            },
            'instances': 1,
            'mem': 1024,
            'cmd': None,
            'args': [],
            'backoff_factor': 2,
            'backoff_seconds': mock.ANY,
            'max_launch_delay_seconds': 300,
            'cpus': 0.25,
            'disk': 1024.0,
            'uris': ['file:///root/.dockercfg'],
            'health_checks': [
                {
                    'portIndex': 0,
                    'protocol': 'HTTP',
                    'timeoutSeconds': 10,
                    'intervalSeconds': 10,
                    'gracePeriodSeconds': 60,
                    'maxConsecutiveFailures': 30,
                    'path': '/status',
                }
            ],
            'env': mock.ANY,
            'id': fake_job_id,
            'constraints': [["region", "GROUP_BY", "1"], ["pool", "LIKE", "default"]],
        }
        assert actual == expected

        # Assert that the complete config can be inserted into the MarathonApp model
        assert MarathonApp(**actual)


def test_format_marathon_app_dict_utilizes_net():
    service_name = "service"
    instance_name = "instance"
    fake_job_id = "service.instance.some.hash"
    fake_system_volumes = [
        {
            "containerPath": "/system",
            "hostPath": "/system",
            "mode": "RO"
        }
    ]
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service=service_name,
        cluster='clustername',
        instance=instance_name,
        config_dict={'net': 'host'},
        branch_dict={'docker_image': 'abcdef'},
    )
    fake_system_paasta_config = SystemPaastaConfig({
        'volumes': fake_system_volumes,
        'docker_registry': 'fake_docker_registry:443'
    }, '/fake/dir/')
    fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()

    with contextlib.nested(
        mock.patch(
            'paasta_tools.marathon_tools.load_service_namespace_config',
            return_value=fake_service_namespace_config,
        ),
        mock.patch('paasta_tools.marathon_tools.format_job_id', return_value=fake_job_id),
        mock.patch('paasta_tools.marathon_tools.load_system_paasta_config', return_value=fake_system_paasta_config),
        mock.patch('paasta_tools.marathon_tools.get_mesos_slaves_grouped_by_attribute',
                   autospec=True, return_value={'fake_region': {}}),
        mock.patch('paasta_tools.marathon_tools.get_slaves', autospec=True, return_value=[{}]),
        mock.patch('paasta_tools.marathon_tools.filter_mesos_slaves_by_blacklist', autospec=True, return_value=[{}]),
    ) as (
        mock_load_service_namespace_config,
        mock_format_job_id,
        mock_system_paasta_config,
        _,
        _,
        _,
    ):
        assert fake_marathon_service_config.format_marathon_app_dict()['container']['docker']['network'] == 'HOST'


def test_format_marathon_app_dict_utilizes_extra_volumes():
    service_name = "service"
    instance_name = "instance"
    fake_job_id = "service.instance.some.hash"
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
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service=service_name,
        cluster='clustername',
        instance=instance_name,
        config_dict={'extra_volumes': fake_extra_volumes},
        branch_dict={'docker_image': 'abcdef'},
    )
    fake_system_paasta_config = SystemPaastaConfig({
        'volumes': fake_system_volumes,
        'docker_registry': 'fake_docker_registry:443'
    }, '/fake/dir/')
    fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()

    with contextlib.nested(
        mock.patch(
            'paasta_tools.marathon_tools.load_service_namespace_config',
            return_value=fake_service_namespace_config,
        ),
        mock.patch('paasta_tools.marathon_tools.format_job_id', return_value=fake_job_id),
        mock.patch('paasta_tools.marathon_tools.load_system_paasta_config', return_value=fake_system_paasta_config),
        mock.patch('paasta_tools.marathon_tools.get_mesos_slaves_grouped_by_attribute',
                   autospec=True, return_value={'fake_region': {}}),
        mock.patch('paasta_tools.marathon_tools.get_slaves', autospec=True, return_value=[{}]),
        mock.patch('paasta_tools.marathon_tools.filter_mesos_slaves_by_blacklist', autospec=True, return_value=[{}]),
    ) as (
        mock_load_service_namespace_config,
        mock_format_job_id,
        mock_system_paasta_config,
        _,
        _,
        _,
    ):
        actual = fake_marathon_service_config.format_marathon_app_dict()
        expected = {
            'container': {
                'docker': {
                    'portMappings': [{'protocol': 'tcp', 'containerPort': 8888, 'hostPort': 0}],
                    'image': 'fake_docker_registry:443/abcdef',
                    'network': 'BRIDGE',
                    'parameters': [
                        {'key': 'memory-swap', 'value': '1024m'},
                        {"key": "cpu-period", "value": '100000'},
                        {"key": "cpu-quota", "value": '250000'},
                    ]
                },
                'type': 'DOCKER',
                'volumes': fake_system_volumes + fake_extra_volumes,
            },
            'instances': 1,
            'mem': 1024.0,
            'cmd': None,
            'args': [],
            'cpus': 0.25,
            'disk': 1024.0,
            'uris': ['file:///root/.dockercfg'],
            'backoff_factor': 2,
            'backoff_seconds': mock.ANY,
            'max_launch_delay_seconds': 300,
            'health_checks': [],
            'env': mock.ANY,
            'id': fake_job_id,
            'constraints': [["region", "GROUP_BY", "1"], ["pool", "LIKE", "default"]],
        }
        assert actual == expected

        # Assert that the complete config can be inserted into the MarathonApp model
        assert MarathonApp(**actual)


def test_kill_tasks_passes_through():
    fake_client = mock.Mock()
    marathon_tools.kill_task(client=fake_client, app_id='app_id', task_id='task_id', scale=True)
    fake_client.kill_task.assert_called_once_with(scale=True, task_id='task_id', app_id='app_id')


def test_kill_tasks_passes_catches_fewer_than_error():
    fake_client = mock.Mock()
    bad_fake_response = mock.Mock()
    bad_fake_response.status_code = 422
    bad_fake_response.json.return_value = {
        "message": "Object is not valid",
        "errors": [{"attribute": "instances", "error": "must be greater than or equal to 0"}],
    }
    fake_client.kill_task.side_effect = MarathonHttpError(response=bad_fake_response)

    actual = marathon_tools.kill_task(client=fake_client, app_id='app_id', task_id='task_id', scale=True)
    fake_client.kill_task.assert_called_once_with(scale=True, task_id='task_id', app_id='app_id')
    assert actual == []


def test_kill_tasks_passes_catches_already_dead_task():
    fake_client = mock.Mock()
    bad_fake_response = mock.Mock()
    bad_fake_response.status_code = 404
    bad_fake_response.json.return_value = {
        "message": "Task 'foo' does not exist",
        "errors": [],
    }
    fake_client.kill_task.side_effect = MarathonHttpError(response=bad_fake_response)

    actual = marathon_tools.kill_task(client=fake_client, app_id='app_id', task_id='task_id', scale=True)
    fake_client.kill_task.assert_called_once_with(scale=True, task_id='task_id', app_id='app_id')
    assert actual == []


def test_create_complete_config():
    mock_format_marathon_app_dict = mock.Mock()
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True,
                   return_value=mock.Mock(format_marathon_app_dict=mock_format_marathon_app_dict)),
        mock.patch('paasta_tools.marathon_tools.load_system_paasta_config', autospec=True),
    ) as (
        mock_load_marathon_service_config,
        _,
    ):
        marathon_tools.create_complete_config('service', 'instance', soa_dir=mock.Mock())
        mock_format_marathon_app_dict.assert_called_once_with()


def test_marathon_config_key_errors():
    fake_marathon_config = marathon_tools.MarathonConfig({})
    with raises(marathon_tools.MarathonNotConfigured):
        fake_marathon_config.get_url()
    with raises(marathon_tools.MarathonNotConfigured):
        fake_marathon_config.get_username()
    with raises(marathon_tools.MarathonNotConfigured):
        fake_marathon_config.get_password()


def test_marathon_service_config_copy():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'elem': 'test'},
        branch_dict={'elem2': 'test2'},
    )
    fake_marathon_service_config_2 = fake_marathon_service_config.copy()
    assert fake_marathon_service_config is not fake_marathon_service_config_2
    assert fake_marathon_service_config == fake_marathon_service_config_2


def test_marathon_service_config_get_healthchecks_invalid_type():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={},
        branch_dict={},
    )
    with mock.patch.object(marathon_tools.MarathonServiceConfig, 'get_healthcheck_mode', autospec=True,
                           return_value='fake-mode'):
        with raises(marathon_tools.InvalidMarathonHealthcheckMode):
            fake_marathon_service_config.get_healthchecks(mock.Mock())


def test_marathon_service_config_get_desired_state_human_invalid_desired_state():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={},
        branch_dict={},
    )
    with mock.patch.object(marathon_tools.MarathonServiceConfig, 'get_desired_state', autospec=True,
                           return_value='fake-state'):
        fake_desired_state = desired_state_human(fake_marathon_service_config.get_desired_state(),
                                                 fake_marathon_service_config.get_instances())
        assert 'Unknown (desired_state: fake-state)' in fake_desired_state


def test_read_namespace_for_service_instance_no_cluster():
    mock_get_cluster = mock.Mock()
    with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.load_marathon_service_config',
                       autospec=True),
            mock.patch('paasta_tools.marathon_tools.load_system_paasta_config', autospec=True,
                       return_value=mock.Mock(get_cluster=mock_get_cluster)),
    ) as (
        _,
        _,
    ):
        marathon_tools.read_namespace_for_service_instance(mock.Mock(), mock.Mock())
        mock_get_cluster.assert_called_once_with()


def test_get_app_queue_status():
    fake_app_id = 'fake_app_id'
    fake_delay = 100
    client = mock.create_autospec(marathon.MarathonClient)
    queue_item = mock.create_autospec(marathon.models.queue.MarathonQueueItem)
    queue_item.app = mock.Mock(id="/%s" % fake_app_id)
    queue_item.delay = mock.Mock(overdue=False, time_left_seconds=fake_delay)
    client.list_queue.return_value = [queue_item]

    is_overdue, delay_seconds = marathon_tools.get_app_queue_status(client, fake_app_id)
    assert delay_seconds == fake_delay
    assert is_overdue is False


def test_is_task_healthy():
    mock_hcrs = [mock.Mock(alive=False), mock.Mock(alive=False)]
    mock_task = mock.Mock(health_check_results=mock_hcrs)
    assert not marathon_tools.is_task_healthy(mock_task)

    mock_hcrs = []
    mock_task = mock.Mock(health_check_results=mock_hcrs)
    assert not marathon_tools.is_task_healthy(mock_task)

    mock_hcrs = [mock.Mock(alive=True), mock.Mock(alive=True)]
    mock_task = mock.Mock(health_check_results=mock_hcrs)
    assert marathon_tools.is_task_healthy(mock_task)

    mock_hcrs = [mock.Mock(alive=True), mock.Mock(alive=False)]
    mock_task = mock.Mock(health_check_results=mock_hcrs)
    assert not marathon_tools.is_task_healthy(mock_task)

    mock_hcrs = [mock.Mock(alive=True), mock.Mock(alive=False)]
    mock_task = mock.Mock(health_check_results=mock_hcrs)
    assert marathon_tools.is_task_healthy(mock_task, require_all=False)

    mock_hcrs = [mock.Mock(alive=False), mock.Mock(alive=False)]
    mock_task = mock.Mock(health_check_results=mock_hcrs)
    assert not marathon_tools.is_task_healthy(mock_task, require_all=False)

    mock_hcrs = []
    mock_task = mock.Mock(health_check_results=mock_hcrs)
    assert marathon_tools.is_task_healthy(mock_task, default_healthy=True)
