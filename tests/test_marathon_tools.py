import contextlib

from marathon.models import MarathonApp
import mock
from mock import patch
from pytest import raises

import marathon_tools
from utils import compose_job_id
from utils import DeploymentsJson
from utils import SystemPaastaConfig


class TestMarathonTools:

    fake_marathon_job_config = marathon_tools.MarathonServiceConfig(
        'servicename',
        'instancename',
        {
            'instances': 3,
            'cpus': 1,
            'mem': 100,
            'nerve_ns': 'fake_nerve_ns',
        },
        {
            'docker_image': 'test_docker:1.0',
            'desired_state': 'start',
            'force_bounce': None,
        }
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
    }, '/some/fake/path/fake_file.json')
    fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()

    def test_load_marathon_service_config_happy_path(self):
        fake_name = 'jazz'
        fake_instance = 'solo'
        fake_cluster = 'amnesia'
        fake_dir = '/nail/home/sanfran'
        with contextlib.nested(
            mock.patch('marathon_tools.load_deployments_json', autospec=True),
            mock.patch('service_configuration_lib.read_service_configuration', autospec=True),
            mock.patch('service_configuration_lib.read_extra_service_information', autospec=True),
        ) as (
            mock_load_deployments_json,
            mock_read_service_configuration,
            mock_read_extra_service_information,
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
            mock.patch('marathon_tools.load_deployments_json', autospec=True),
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
        config_copy = self.fake_marathon_job_config.config_dict.copy()

        expected = marathon_tools.MarathonServiceConfig(
            fake_name,
            fake_instance,
            dict(
                self.fake_srv_config.items() +
                self.fake_marathon_job_config.config_dict.items()
            ),
            {},
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
            assert expected.service_name == actual.service_name
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
        config_copy = self.fake_marathon_job_config.config_dict.copy()

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
                'marathon_tools.load_deployments_json',
                autospec=True,
                return_value=deployments_json_mock,
            ),
        ) as (
            read_service_configuration_patch,
            read_extra_info_patch,
            load_deployments_json_patch,
        ):
            expected = marathon_tools.MarathonServiceConfig(
                fake_name,
                fake_instance,
                dict(
                    self.fake_srv_config.items() +
                    self.fake_marathon_job_config.config_dict.items()
                ),
                fake_branch_dict,
            )
            actual = marathon_tools.load_marathon_service_config(
                fake_name,
                fake_instance,
                fake_cluster,
                load_deployments=True,
                soa_dir=fake_dir,
            )
            assert expected.service_name == actual.service_name
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
        file_mock = mock.MagicMock(spec=file)
        with contextlib.nested(
            mock.patch('marathon_tools.open', create=True, return_value=file_mock),
            mock.patch('json.load', autospec=True, return_value=expected)
        ) as (
            open_file_patch,
            json_patch
        ):
            assert marathon_tools.load_marathon_config() == expected
            open_file_patch.assert_called_once_with('/etc/paasta/marathon.json')
            json_patch.assert_called_once_with(file_mock.__enter__())

    def test_load_marathon_config_path_dne(self):
        fake_path = '/var/dir_of_fake'
        with contextlib.nested(
            mock.patch('marathon_tools.open', create=True, side_effect=IOError(2, 'a', 'b')),
        ) as (
            open_patch,
        ):
            with raises(marathon_tools.PaastaNotConfiguredError) as excinfo:
                marathon_tools.load_marathon_config(fake_path)
            assert str(excinfo.value) == "Could not load marathon config file b: a"

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
            mock.patch('marathon_tools.get_all_namespaces_for_service',
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
            mock.patch('marathon_tools.read_namespace_for_service_instance',
                       autospec=True, return_value=namespace),
            mock.patch('marathon_tools.load_service_namespace_config',
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
            mock.patch('marathon_tools.read_namespace_for_service_instance',
                       autospec=True, return_value=namespace),
            mock.patch('marathon_tools.load_service_namespace_config',
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
        fake_uri = 'energy'
        fake_timeout = -10103
        fake_port = 777
        fake_retries = 9001
        fake_discover = 'myhabitat'
        fake_advertise = ['red', 'blue']
        fake_info = {
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

    @mock.patch('service_configuration_lib.read_extra_service_information', autospec=True)
    def test_read_namespace_for_service_instance_has_value(self, read_info_patch):
        name = 'dont_worry'
        instance = 'im_a_professional'
        cluster = 'andromeda'
        namespace = 'spacename'
        soa_dir = 'dirdirdir'
        read_info_patch.return_value = {instance: {'nerve_ns': namespace}}
        actual = marathon_tools.read_namespace_for_service_instance(name, instance, cluster, soa_dir)
        assert actual == namespace
        read_info_patch.assert_called_once_with(name, 'marathon-%s' % cluster, soa_dir)

    @mock.patch('service_configuration_lib.read_extra_service_information', autospec=True)
    def test_read_namespace_for_service_instance_no_value(self, read_info_patch):
        name = 'wall_light'
        instance = 'ceiling_light'
        cluster = 'no_light'
        soa_dir = 'warehouse_light'
        read_info_patch.return_value = {instance: {'aaaaaaaa': ['bbbbbbbb']}}
        actual = marathon_tools.read_namespace_for_service_instance(name, instance, cluster, soa_dir)
        assert actual == instance
        read_info_patch.assert_called_once_with(name, 'marathon-%s' % cluster, soa_dir)

    @mock.patch('marathon_tools.fetch_local_slave_state', autospec=True)
    def test_marathon_services_running_here(self, mock_fetch_local_slave_state):
        id_1 = 'klingon.ships.detected.249qwiomelht4jioewglkemr'
        id_2 = 'fire.photon.torpedos.jtgriemot5yhtwe94'
        id_3 = 'dota.axe.cleave.482u9jyoi4wed'
        id_4 = 'mesos.deployment.is.hard'
        id_5 = 'how.to.fake.data'
        ports_1 = '[111-111]'
        ports_2 = '[222-222]'
        ports_3 = '[333-333]'
        ports_4 = '[444-444]'
        ports_5 = '[555-555]'
        mock_fetch_local_slave_state.return_value = {
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
        mock_fetch_local_slave_state.assert_called_once_with()
        assert expected == actual

    def test_get_marathon_services_running_here_for_nerve(self):
        cluster = 'edelweiss'
        soa_dir = 'the_sound_of_music'
        fake_marathon_services = [('no_test', 'left_behind', 1111),
                                  ('no_docstrings', 'forever_abandoned', 2222)]
        namespaces = ['dos', 'uno']
        nerve_dicts = [marathon_tools.ServiceNamespaceConfig({'binary': 1, 'proxy_port': 6666}),
                       marathon_tools.ServiceNamespaceConfig({'clock': 0, 'proxy_port': 6666})]
        expected = [('no_test.uno', {'clock': 0, 'port': 1111, 'proxy_port': 6666}),
                    ('no_docstrings.dos', {'binary': 1, 'port': 2222, 'proxy_port': 6666})]
        with contextlib.nested(
            mock.patch('marathon_tools.marathon_services_running_here',
                       autospec=True,
                       return_value=fake_marathon_services),
            mock.patch('marathon_tools.read_namespace_for_service_instance',
                       autospec=True,
                       side_effect=lambda a, b, c, d: namespaces.pop()),
            mock.patch('marathon_tools.load_service_namespace_config',
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

    def test_get_marathon_services_running_here_for_nerve_when_not_in_smartstack(self):
        cluster = 'edelweiss'
        soa_dir = 'the_sound_of_music'
        fake_marathon_services = [('no_test', 'left_behind', 1111),
                                  ('no_docstrings', 'forever_abandoned', 2222)]
        namespaces = ['dos', 'uno']
        nerve_dicts = [marathon_tools.ServiceNamespaceConfig({'binary': 1}),
                       marathon_tools.ServiceNamespaceConfig({'clock': 0, 'proxy_port': 6666})]
        expected = [('no_test.uno', {'clock': 0, 'port': 1111, 'proxy_port': 6666})]
        with contextlib.nested(
            mock.patch('marathon_tools.marathon_services_running_here',
                       autospec=True,
                       return_value=fake_marathon_services),
            mock.patch('marathon_tools.read_namespace_for_service_instance',
                       autospec=True,
                       side_effect=lambda a, b, c, d: namespaces.pop()),
            mock.patch('marathon_tools.load_service_namespace_config',
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
                'marathon_tools.load_system_paasta_config',
                autospec=True,
            ),
            mock.patch(
                'marathon_tools.marathon_services_running_here',
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
                'marathon_tools.load_system_paasta_config',
                autospec=True,
            ),
            mock.patch(
                'marathon_tools.marathon_services_running_here',
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
                'marathon_tools.load_system_paasta_config',
                autospec=True,
            ),
            mock.patch(
                'marathon_tools.marathon_services_running_here',
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
            mock.patch('marathon_tools.load_service_namespace_config', autospec=True,
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
                return_value={'d', 'c'}
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
                'marathon_tools.get_classic_services_that_run_here',
                autospec=True,
                side_effect=lambda: ['a', 'b', 'c']
            ),
            mock.patch(
                'marathon_tools.get_all_namespaces_for_service',
                autospec=True,
                side_effect=lambda x, y, full_name: [('foo', {})]
            ),
            mock.patch(
                'marathon_tools._namespaced_get_classic_service_information_for_nerve',
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
            mock.patch('marathon_tools.get_marathon_services_running_here_for_nerve',
                       autospec=True,
                       return_value=fake_marathon_services),
            mock.patch('marathon_tools.get_classic_services_running_here_for_nerve',
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
        fake_id = marathon_tools.format_job_id('can_you_dig_it', 'yes_i_can')
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
        fake_cpus = .42
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

        expected_conf = {
            'id': fake_id,
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
                },
                'type': 'DOCKER',
                'volumes': fake_volumes,
            },
            'constraints': [["habitat", "GROUP_BY", "1"]],
            'uris': ['file:///root/.dockercfg', ],
            'mem': fake_mem,
            'env': fake_env,
            'cpus': fake_cpus,
            'instances': fake_instances,
            'cmd': fake_cmd,
            'args': fake_args,
            'health_checks': fake_healthchecks,
            'backoff_seconds': 1,
            'backoff_factor': 2,
        }
        config = marathon_tools.MarathonServiceConfig(
            'can_you_dig_it',
            'yes_i_can',
            {
                'env': fake_env,
                'mem': fake_mem,
                'cpus': fake_cpus,
                'instances': fake_instances,
                'cmd': fake_cmd,
                'args': fake_args,
                'healthcheck_grace_period_seconds': 3,
                'healthcheck_interval_seconds':  10,
                'healthcheck_timeout_seconds':  10,
                'healthcheck_max_consecutive_failures': 3,
            },
            {'desired_state': 'start'}
        )

        with mock.patch('marathon_tools.get_mesos_slaves_grouped_by_attribute', autospec=True) as get_slaves_patch:
            get_slaves_patch.return_value = {'fake_region': {}}
            actual = config.format_marathon_app_dict(fake_id, fake_url, fake_volumes,
                                                     fake_service_namespace_config)
            assert actual == expected_conf

            # Assert that the complete config can be inserted into the MarathonApp model
            assert MarathonApp(**actual)

    def test_instances_is_zero_when_desired_state_is_stop(self):
        fake_conf = marathon_tools.MarathonServiceConfig(
            'fake_name',
            'fake_instance',
            {'instances': 10},
            {'desired_state': 'stop'},
        )
        assert fake_conf.get_instances() == 0

    def test_get_bounce_method_in_config(self):
        fake_conf = marathon_tools.MarathonServiceConfig('fake_name', 'fake_instance', {'bounce_method': 'aaargh'}, {})
        assert fake_conf.get_bounce_method() == 'aaargh'

    def test_get_bounce_method_default(self):
        fake_conf = marathon_tools.MarathonServiceConfig('fake_name', 'fake_instance', {}, {})
        assert fake_conf.get_bounce_method() == 'crossover'

    def test_get_bounce_health_params_in_config(self):
        fake_param = 'fake_param'
        fake_conf = marathon_tools.MarathonServiceConfig(
            'fake_name', 'fake_instance', {'bounce_health_params': fake_param}, {})
        assert fake_conf.get_bounce_health_params(mock.Mock()) == fake_param

    def test_get_bounce_health_params_default_when_not_in_smartstack(self):
        fake_service_namespace_config = mock.Mock(is_in_smartstack=mock.Mock(return_value=False))
        fake_conf = marathon_tools.MarathonServiceConfig('fake_name', 'fake_instance', {}, {})
        assert fake_conf.get_bounce_health_params(fake_service_namespace_config) == {}

    def test_get_bounce_health_params_default_when_in_smartstack(self):
        fake_service_namespace_config = mock.Mock(is_in_smartstack=mock.Mock(return_value=True))
        fake_conf = marathon_tools.MarathonServiceConfig('fake_name', 'fake_instance', {}, {})
        assert fake_conf.get_bounce_health_params(fake_service_namespace_config) == {'check_haproxy': True}

    def test_get_drain_method_in_config(self):
        fake_param = 'fake_param'
        fake_conf = marathon_tools.MarathonServiceConfig(
            'fake_name', 'fake_instance', {'drain_method': fake_param}, {})
        assert fake_conf.get_drain_method(mock.Mock()) == fake_param

    def test_get_drain_method_default_when_not_in_smartstack(self):
        fake_service_namespace_config = mock.Mock(is_in_smartstack=mock.Mock(return_value=False))
        fake_conf = marathon_tools.MarathonServiceConfig('fake_name', 'fake_instance', {}, {})
        assert fake_conf.get_drain_method(fake_service_namespace_config) == 'noop'

    def test_get_drain_method_default_when_in_smartstack(self):
        fake_service_namespace_config = mock.Mock(is_in_smartstack=mock.Mock(return_value=True))
        fake_conf = marathon_tools.MarathonServiceConfig('fake_name', 'fake_instance', {}, {})
        assert fake_conf.get_drain_method(fake_service_namespace_config) == 'hacheck'

    def test_get_drain_method_params_in_config(self):
        fake_param = 'fake_param'
        fake_conf = marathon_tools.MarathonServiceConfig(
            'fake_name', 'fake_instance', {'drain_method_params': fake_param}, {})
        assert fake_conf.get_drain_method_params(mock.Mock()) == fake_param

    def test_get_drain_method_params_default_when_not_in_smartstack(self):
        fake_service_namespace_config = mock.Mock(is_in_smartstack=mock.Mock(return_value=False))
        fake_conf = marathon_tools.MarathonServiceConfig('fake_name', 'fake_instance', {}, {})
        assert fake_conf.get_drain_method_params(fake_service_namespace_config) == {}

    def test_get_drain_method_params_default_when_in_smartstack(self):
        fake_service_namespace_config = mock.Mock(is_in_smartstack=mock.Mock(return_value=True))
        fake_conf = marathon_tools.MarathonServiceConfig('fake_name', 'fake_instance', {}, {})
        assert fake_conf.get_drain_method_params(fake_service_namespace_config) == {'delay': 30}

    def test_get_instances_in_config(self):
        fake_conf = marathon_tools.MarathonServiceConfig(
            'fake_name',
            'fake_instance',
            {'instances': -10},
            {'desired_state': 'start'},
        )
        assert fake_conf.get_instances() == -10

    def test_get_instances_default(self):
        fake_conf = marathon_tools.MarathonServiceConfig('fake_name', 'fake_instance', {}, {})
        assert fake_conf.get_instances() == 1

    def test_get_instances_respects_false(self):
        fake_conf = marathon_tools.MarathonServiceConfig(
            'fake_name',
            'fake_instance',
            {'instances': False},
            {'desired_state': 'start'},
        )
        assert fake_conf.get_instances() == 0

    def test_get_constraints_in_config(self):
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()
        fake_conf = marathon_tools.MarathonServiceConfig('fake_name', 'fake_instance', {'constraints': 'so_many_walls'},
                                                         {})
        with mock.patch('marathon_tools.get_mesos_slaves_grouped_by_attribute', autospec=True) as get_slaves_patch:
            assert fake_conf.get_constraints(fake_service_namespace_config) == 'so_many_walls'
            assert get_slaves_patch.call_count == 0

    def test_get_constraints_default(self):
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()
        fake_conf = marathon_tools.MarathonServiceConfig('fake_name', 'fake_instance', {}, {})
        with mock.patch('marathon_tools.get_mesos_slaves_grouped_by_attribute', autospec=True) as get_slaves_patch:
            get_slaves_patch.return_value = {'fake_region': {}}
            assert fake_conf.get_constraints(fake_service_namespace_config) == [["region", "GROUP_BY", "1"]]

    def test_get_constraints_from_discover(self):
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({
            'mode': 'http',
            'healthcheck_uri': '/status',
            'discover': 'habitat',
        })
        fake_conf = marathon_tools.MarathonServiceConfig('fake_name', 'fake_instance', {}, {})
        with mock.patch('marathon_tools.get_mesos_slaves_grouped_by_attribute', autospec=True) as get_slaves_patch:
            get_slaves_patch.return_value = {'fake_region': {}, 'fake_other_region': {}}
            assert fake_conf.get_constraints(fake_service_namespace_config) == [["habitat", "GROUP_BY", "2"]]
            get_slaves_patch.assert_called_once_with('habitat')

    def test_instance_config_getters_in_config(self):
        fake_conf = marathon_tools.MarathonServiceConfig('fake_name', 'fake_instance', {'monitoring': 'test'}, {})
        assert fake_conf.get_monitoring() == 'test'

    def test_get_marathon_client(self):
        fake_url = "nothing_for_me_to_do_but_dance"
        fake_user = "the_boogie"
        fake_passwd = "is_for_real"
        with mock.patch('marathon_tools.MarathonClient', autospec=True) as client_patch:
            marathon_tools.get_marathon_client(fake_url, fake_user, fake_passwd)
            client_patch.assert_called_once_with(fake_url, fake_user, fake_passwd, timeout=30)

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
            'marathon_tools.list_all_marathon_app_ids',
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
            'marathon_tools.list_all_marathon_app_ids',
            autospec=True,
            return_value=fake_all_marathon_app_ids,
        ) as list_all_marathon_app_ids_patch:
            assert marathon_tools.is_app_id_running(fake_id, fake_client) is False
            list_all_marathon_app_ids_patch.assert_called_once_with(fake_client)

    @patch('marathon_tools.MarathonClient.list_tasks')
    def test_app_has_tasks_exact(self, patch_list_tasks):
        fake_client = mock.Mock()
        fake_client.list_tasks = patch_list_tasks
        patch_list_tasks.return_value = [{}, {}, {}]
        assert marathon_tools.app_has_tasks(fake_client, 'fake_app', 3) is True

    @patch('marathon_tools.MarathonClient.list_tasks')
    def test_app_has_tasks_less(self, patch_list_tasks):
        fake_client = mock.Mock()
        fake_client.list_tasks = patch_list_tasks
        patch_list_tasks.return_value = [{}, {}, {}]
        assert marathon_tools.app_has_tasks(fake_client, 'fake_app', 2) is True

    @patch('marathon_tools.MarathonClient.list_tasks')
    def test_app_has_tasks_more(self, patch_list_tasks):
        fake_client = mock.Mock()
        fake_client.list_tasks = patch_list_tasks
        patch_list_tasks.return_value = [{}, {}, {}]
        assert marathon_tools.app_has_tasks(fake_client, 'fake_app', 4) is False

    def test_get_app_id(self):
        fake_name = 'fakeapp'
        fake_instance = 'fakeinstance'
        fake_url = 'fake_url'
        fake_hash = 'CONFIGHASH'
        fake_code_sha = 'CODESHA'
        fake_config = {}
        fake_cluster = 'fake_cluster'

        with contextlib.nested(
            mock.patch('marathon_tools.load_system_paasta_config', autospec=True),
            mock.patch('marathon_tools.load_marathon_service_config', autospec=True,
                       return_value=self.fake_marathon_job_config),
            mock.patch('marathon_tools.get_docker_url', autospec=True, return_value=fake_url),
            mock.patch.object(self.fake_marathon_job_config, 'format_marathon_app_dict',
                              return_value=fake_config, autospec=True),
            mock.patch('marathon_tools.get_config_hash', autospec=True, return_value=fake_hash),
            mock.patch('marathon_tools.get_code_sha_from_dockerurl', autospec=True, return_value=fake_code_sha),
            mock.patch('marathon_tools.load_service_namespace_config', autospec=True,
                       return_value=self.fake_service_namespace_config)
        ) as (
            load_system_paasta_config_patch,
            read_service_config_patch,
            docker_url_patch,
            format_marathon_app_dict_patch,
            hash_patch,
            code_sha_patch,
            SNC_load_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(return_value=fake_cluster)
            assert marathon_tools.get_app_id(
                fake_name,
                fake_instance,
                self.fake_marathon_config
            ) == 'fakeapp.fakeinstance.CODESHA.CONFIGHASH'
            read_service_config_patch.assert_called_once_with(fake_name, fake_instance, fake_cluster,
                                                              soa_dir='/nail/etc/services')
            hash_patch.assert_called_once_with(fake_config, force_bounce=None)
            code_sha_patch.assert_called_once_with(fake_url)
            SNC_load_patch.assert_called_once_with(fake_name, 'fake_nerve_ns')

    def test_get_code_sha_from_dockerurl(self):
        fake_docker_url = 'docker-paasta.yelpcorp.com:443/services-cieye:paasta-93340779404579'
        actual = marathon_tools.get_code_sha_from_dockerurl(fake_docker_url)
        assert actual == 'git93340779'
        assert len(actual) == 11

    def test_get_config_hash(self):
        test_input = {'foo': 'bar'}
        actual = marathon_tools.get_config_hash(test_input)
        expected = 'configdd63dafc'
        assert actual == expected
        assert len(actual) == 14

    def test_id_changes_when_force_bounce_or_desired_state_changes(self):
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
            fake_name,
            fake_instance,
            self.fake_marathon_job_config.config_dict,
            {
                'desired_state': 'start',
                'force_bounce': '88888',
            }
        )

        fake_service_config_2 = marathon_tools.MarathonServiceConfig(
            fake_name,
            fake_instance,
            self.fake_marathon_job_config.config_dict,
            {
                'desired_state': 'start',
                'force_bounce': '99999',
            }
        )

        fake_service_config_3 = marathon_tools.MarathonServiceConfig(
            fake_name,
            fake_instance,
            self.fake_marathon_job_config.config_dict,
            {
                'desired_state': 'stop',
                'force_bounce': '99999',
            }
        )

        with contextlib.nested(
            mock.patch('marathon_tools.load_system_paasta_config',
                       autospec=True, return_value=fake_system_paasta_config),
            mock.patch('marathon_tools.load_marathon_service_config', autospec=True),
            mock.patch('marathon_tools.get_docker_url', autospec=True, return_value=fake_url),
            mock.patch('marathon_tools.load_service_namespace_config', autospec=True,
                       return_value=self.fake_service_namespace_config),
            mock.patch('marathon_tools.get_mesos_slaves_grouped_by_attribute',
                       autospec=True, return_value={'fake_region': {}})
        ) as (
            load_system_paasta_config_patch,
            read_service_config_patch,
            docker_url_patch,
            _,
            __,
        ):
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(return_value=fake_cluster)
            read_service_config_patch.return_value = fake_service_config_1
            first_id = marathon_tools.get_app_id(fake_name, fake_instance, self.fake_marathon_config)
            first_id_2 = marathon_tools.get_app_id(fake_name, fake_instance, self.fake_marathon_config)
            # just for sanity, make sure that get_app_id is idempotent.
            assert first_id == first_id_2

            read_service_config_patch.return_value = fake_service_config_2
            second_id = marathon_tools.get_app_id(fake_name, fake_instance, self.fake_marathon_config)
            assert first_id != second_id

            read_service_config_patch.return_value = fake_service_config_3
            third_id = marathon_tools.get_app_id(fake_name, fake_instance, self.fake_marathon_config)
            assert second_id != third_id

    def test_get_expected_instance_count_for_namespace(self):
        service_name = 'red'
        namespace = 'rojo'
        soa_dir = 'que_esta'
        fake_instances = [(service_name, 'blue'), (service_name, 'green')]
        fake_srv_config = marathon_tools.MarathonServiceConfig(
            service_name=service_name,
            instance='blue',
            config_dict={'nerve_ns': 'rojo', 'instances': 11},
            branch_dict={},
        )

        def config_helper(name, inst, cluster, soa_dir=None):
            if inst == 'blue':
                return fake_srv_config
            else:
                return marathon_tools.MarathonServiceConfig(service_name, 'green', {'nerve_ns': 'amarillo'}, {})

        with contextlib.nested(
            mock.patch('marathon_tools.get_service_instance_list',
                       autospec=True,
                       return_value=fake_instances),
            mock.patch('marathon_tools.load_marathon_service_config',
                       autospec=True,
                       side_effect=config_helper),
        ) as (
            inst_list_patch,
            read_config_patch,
        ):
            actual = marathon_tools.get_expected_instance_count_for_namespace(
                service_name,
                namespace,
                cluster='fake_cluster',
                soa_dir=soa_dir,
            )
            assert actual == 11
            inst_list_patch.assert_called_once_with(service_name,
                                                    cluster='fake_cluster',
                                                    instance_type='marathon',
                                                    soa_dir=soa_dir)
            read_config_patch.assert_any_call(service_name, 'blue', 'fake_cluster', soa_dir=soa_dir)
            read_config_patch.assert_any_call(service_name, 'green', 'fake_cluster', soa_dir=soa_dir)

    def test_get_matching_appids(self):
        fakeapp1 = mock.Mock(id='/fake--service.fake--instance---bouncingold')
        fakeapp2 = mock.Mock(id='/fake--service.fake--instance---bouncingnew')
        fakeapp3 = mock.Mock(id='/fake--service.other--instance--bla')
        fakeapp4 = mock.Mock(id='/other--service')
        apps = [fakeapp1, fakeapp2, fakeapp3, fakeapp4]
        list_apps_mock = mock.Mock(return_value=apps)
        fake_client = mock.Mock(list_apps=list_apps_mock)
        expected = [
            '/fake--service.fake--instance---bouncingold',
            '/fake--service.fake--instance---bouncingnew',
        ]
        actual = marathon_tools.get_matching_appids('fake_service', 'fake_instance', fake_client)
        assert actual == expected

    def test_get_healthcheck_for_instance_http(self):
        fake_service = 'fake_service'
        fake_namespace = 'fake_namespace'
        fake_hostname = 'fake_hostname'
        fake_random_port = 666

        fake_path = '/fake_path'
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(fake_service, fake_namespace, {}, {})
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({
            'mode': 'http',
            'healthcheck_uri': fake_path,
        })
        with contextlib.nested(
            mock.patch('marathon_tools.load_marathon_service_config',
                       autospec=True,
                       return_value=fake_marathon_service_config),
            mock.patch('marathon_tools.load_service_namespace_config',
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

        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(fake_service, fake_namespace, {}, {})
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({
            'mode': 'tcp',
        })
        with contextlib.nested(
            mock.patch('marathon_tools.load_marathon_service_config',
                       autospec=True,
                       return_value=fake_marathon_service_config),
            mock.patch('marathon_tools.load_service_namespace_config',
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
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(fake_service, fake_namespace, {
            'healthcheck_mode': 'cmd',
            'healthcheck_cmd': fake_cmd
        }, {})
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({})
        with contextlib.nested(
            mock.patch('marathon_tools.load_marathon_service_config',
                       autospec=True,
                       return_value=fake_marathon_service_config),
            mock.patch('marathon_tools.load_service_namespace_config',
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
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(fake_service, fake_namespace, {
            'healthcheck_mode': None,
        }, {})
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({})
        with contextlib.nested(
            mock.patch('marathon_tools.load_marathon_service_config',
                       autospec=True,
                       return_value=fake_marathon_service_config),
            mock.patch('marathon_tools.load_service_namespace_config',
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


class TestMarathonServiceConfig(object):

    def test_repr(self):
        actual = repr(marathon_tools.MarathonServiceConfig('foo', 'bar', {'baz': 'baz'}, {'bubble': 'gum'}))
        expected = """MarathonServiceConfig('foo', 'bar', {'baz': 'baz'}, {'bubble': 'gum'})"""
        assert actual == expected

    def test_get_healthcheck_mode_default(self):
        namespace_config = marathon_tools.ServiceNamespaceConfig({})
        marathon_config = marathon_tools.MarathonServiceConfig("service", "instance", {}, {})
        assert marathon_config.get_healthcheck_mode(namespace_config) is None

    def test_get_healthcheck_mode_default_from_namespace_config(self):
        namespace_config = marathon_tools.ServiceNamespaceConfig({'proxy_port': 1234})
        marathon_config = marathon_tools.MarathonServiceConfig("service", "instance", {}, {})
        assert marathon_config.get_healthcheck_mode(namespace_config) == 'http'

    def test_get_healthcheck_mode_valid(self):
        namespace_config = marathon_tools.ServiceNamespaceConfig({})
        marathon_config = marathon_tools.MarathonServiceConfig("service", "instance", {'healthcheck_mode': 'tcp'}, {})
        assert marathon_config.get_healthcheck_mode(namespace_config) == 'tcp'

    def test_get_healthcheck_mode_invalid(self):
        namespace_config = marathon_tools.ServiceNamespaceConfig({})
        marathon_config = marathon_tools.MarathonServiceConfig("service", "instance", {'healthcheck_mode': 'udp'}, {})
        with raises(marathon_tools.InvalidMarathonHealthcheckMode):
            marathon_config.get_healthcheck_mode(namespace_config)

    def test_get_healthchecks_http_overrides(self):
        fake_path = '/mycoolstatus'
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
            "service",
            "instance",
            {
                "healthcheck_mode": "http",  # Actually the default here, but I want to be specific.
                "healthcheck_uri": fake_path,
                "healthcheck_grace_period_seconds": 70,
                "healthcheck_interval_seconds": 12,
                "healthcheck_timeout_seconds": 13,
                "healthcheck_max_consecutive_failures": 7,
            },
            {},
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
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig("service", "instance", {}, {})
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({'mode': 'http'})
        expected = [
            {
                "protocol": "HTTP",
                "path": '/status',
                "gracePeriodSeconds": 60,
                "intervalSeconds": 10,
                "portIndex": 0,
                "timeoutSeconds": 10,
                "maxConsecutiveFailures": 6
            },
        ]
        actual = fake_marathon_service_config.get_healthchecks(fake_service_namespace_config)
        assert actual == expected

    def test_get_healthchecks_tcp(self):
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig("service", "instance", {}, {})
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({'mode': 'tcp'})
        expected = [
            {
                "protocol": "TCP",
                "gracePeriodSeconds": 60,
                "intervalSeconds": 10,
                "portIndex": 0,
                "timeoutSeconds": 10,
                "maxConsecutiveFailures": 6
            },
        ]
        actual = fake_marathon_service_config.get_healthchecks(fake_service_namespace_config)
        assert actual == expected

    def test_get_healthchecks_cmd(self):
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
            "service", "instance", {'healthcheck_mode': 'cmd'}, {})
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()
        expected_cmd = "paasta_execute_docker_command --mesos-id \"$MESOS_TASK_ID\" --cmd /bin/true --timeout '10'"
        expected = [
            {
                "protocol": "COMMAND",
                "command": {"value": expected_cmd},
                "gracePeriodSeconds": 60,
                "intervalSeconds": 10,
                "timeoutSeconds": 10,
                "maxConsecutiveFailures": 6
            },
        ]
        actual = fake_marathon_service_config.get_healthchecks(fake_service_namespace_config)
        assert actual == expected

    def test_get_healthchecks_cmd_quotes(self):
        fake_command = '/bin/fake_command with spaces'
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
            "service", "instance", {'healthcheck_mode': 'cmd', 'healthcheck_cmd': fake_command}, {})
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()
        expected_cmd = "paasta_execute_docker_command " \
            "--mesos-id \"$MESOS_TASK_ID\" --cmd '%s' --timeout '10'" % fake_command
        expected = [
            {
                "protocol": "COMMAND",
                "command": {"value": expected_cmd},
                "gracePeriodSeconds": 60,
                "intervalSeconds": 10,
                "timeoutSeconds": 10,
                "maxConsecutiveFailures": 6
            },
        ]
        actual = fake_marathon_service_config.get_healthchecks(fake_service_namespace_config)
        assert actual == expected

    def test_get_healthchecks_cmd_overrides(self):
        fake_command = '/bin/fake_command'
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
            "service", "instance", {'healthcheck_mode': 'cmd', 'healthcheck_cmd': fake_command}, {})
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()
        expected_cmd = "paasta_execute_docker_command " \
            "--mesos-id \"$MESOS_TASK_ID\" --cmd %s --timeout '10'" % fake_command
        expected = [
            {
                "protocol": "COMMAND",
                "command": {"value": expected_cmd},
                "gracePeriodSeconds": 60,
                "intervalSeconds": 10,
                "timeoutSeconds": 10,
                "maxConsecutiveFailures": 6
            },
        ]
        actual = fake_marathon_service_config.get_healthchecks(fake_service_namespace_config)
        assert actual == expected

    def test_get_healthchecks_cmd_overrides_timeout(self):
        fake_command = '/bin/fake_command'
        fake_timeout = 4
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
            "service",
            "instance",
            {'healthcheck_mode': 'cmd', 'healthcheck_timeout_seconds': fake_timeout, 'healthcheck_cmd': fake_command},
            {}
        )
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()
        expected_cmd = "paasta_execute_docker_command " \
            "--mesos-id \"$MESOS_TASK_ID\" --cmd %s --timeout '%s'" % (fake_command, fake_timeout)
        expected = [
            {
                "protocol": "COMMAND",
                "command": {"value": expected_cmd},
                "gracePeriodSeconds": 60,
                "intervalSeconds": 10,
                "timeoutSeconds": fake_timeout,
                "maxConsecutiveFailures": 6
            },
        ]
        actual = fake_marathon_service_config.get_healthchecks(fake_service_namespace_config)
        assert actual == expected

    def test_get_healthchecks_empty(self):
        fake_marathon_service_config = marathon_tools.MarathonServiceConfig("service", "instance", {}, {})
        fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({})
        assert fake_marathon_service_config.get_healthchecks(fake_service_namespace_config) == []

    def test_get_healthchecks_invalid_mode(self):
        marathon_config = marathon_tools.MarathonServiceConfig("service", "instance", {'healthcheck_mode': 'none'}, {})
        namespace_config = marathon_tools.ServiceNamespaceConfig({})
        with raises(marathon_tools.InvalidMarathonHealthcheckMode):
            marathon_config.get_healthchecks(namespace_config)


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


def test_create_complete_config_no_smartstack():
    service_name = "service"
    instance_name = "instance"
    fake_job_id = "service.instance.some.hash"
    fake_marathon_config = marathon_tools.MarathonConfig({}, 'fake_file.json')
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service_name,
        instance_name,
        {},
        {'docker_image': 'abcdef'},
    )
    fake_system_paasta_config = SystemPaastaConfig({
        'volumes': [],
        'docker_registry': 'fake_docker_registry:443'
    }, '/fake/dir/')
    fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig()
    fake_cluster = "clustername"

    with contextlib.nested(
        mock.patch('marathon_tools.load_marathon_service_config', return_value=fake_marathon_service_config),
        mock.patch('marathon_tools.load_service_namespace_config', return_value=fake_service_namespace_config),
        mock.patch('marathon_tools.format_job_id', return_value=fake_job_id),
        mock.patch('marathon_tools.load_system_paasta_config', return_value=fake_system_paasta_config),
        mock.patch('marathon_tools.get_mesos_slaves_grouped_by_attribute',
                   autospec=True, return_value={'fake_region': {}})
    ) as (
        mock_load_marathon_service_config,
        mock_load_service_namespace_config,
        mock_format_job_id,
        mock_system_paasta_config,
        _,
    ):
        mock_system_paasta_config.return_value.get_cluster = mock.Mock(return_value=fake_cluster)
        actual = marathon_tools.create_complete_config(service_name, instance_name, fake_marathon_config)
        expected = {
            'container': {
                'docker': {
                    'portMappings': [{'protocol': 'tcp', 'containerPort': 8888, 'hostPort': 0}],
                    'image': 'fake_docker_registry:443/abcdef',
                    'network': 'BRIDGE'
                },
                'type': 'DOCKER',
                'volumes': [],
            },
            'instances': 1,
            'mem': 1024,
            'cmd': None,
            'args': [],
            'backoff_factor': 2,
            'cpus': 0.25,
            'uris': ['file:///root/.dockercfg'],
            'backoff_seconds': 1,
            'health_checks': [],
            'env': {},
            'id': fake_job_id,
            'constraints': [["region", "GROUP_BY", "1"]],
        }
        assert actual == expected

        # Assert that the complete config can be inserted into the MarathonApp model
        assert MarathonApp(**actual)


def test_create_complete_config_with_smartstack():
    service_name = "service"
    instance_name = "instance"
    fake_job_id = "service.instance.some.hash"
    fake_marathon_config = marathon_tools.MarathonConfig({}, 'fake_file.json')
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service_name,
        instance_name,
        {},
        {'docker_image': 'abcdef'},
    )
    fake_system_paasta_config = SystemPaastaConfig({
        'volumes': [],
        'docker_registry': 'fake_docker_registry:443'
    }, '/fake/dir/')
    fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({'proxy_port': 9001})
    fake_cluster = "clustername"

    with contextlib.nested(
        mock.patch('marathon_tools.load_marathon_service_config', return_value=fake_marathon_service_config),
        mock.patch('marathon_tools.load_service_namespace_config', return_value=fake_service_namespace_config),
        mock.patch('marathon_tools.format_job_id', return_value=fake_job_id),
        mock.patch('marathon_tools.load_system_paasta_config', return_value=fake_system_paasta_config),
        mock.patch('marathon_tools.get_mesos_slaves_grouped_by_attribute',
                   autospec=True, return_value={'fake_region': {}})
    ) as (
        mock_load_marathon_service_config,
        mock_load_service_namespace_config,
        mock_format_job_id,
        mock_system_paasta_config,
        _,
    ):
        mock_system_paasta_config.return_value.get_cluster = mock.Mock(return_value=fake_cluster)
        actual = marathon_tools.create_complete_config(service_name, instance_name, fake_marathon_config)
        expected = {
            'container': {
                'docker': {
                    'portMappings': [{'protocol': 'tcp', 'containerPort': 8888, 'hostPort': 0}],
                    'image': 'fake_docker_registry:443/abcdef',
                    'network': 'BRIDGE'
                },
                'type': 'DOCKER',
                'volumes': [],
            },
            'instances': 1,
            'mem': 1024,
            'cmd': None,
            'args': [],
            'backoff_factor': 2,
            'cpus': 0.25,
            'uris': ['file:///root/.dockercfg'],
            'backoff_seconds': 1,
            'health_checks': [
                {
                    'portIndex': 0,
                    'protocol': 'HTTP',
                    'timeoutSeconds': 10,
                    'intervalSeconds': 10,
                    'gracePeriodSeconds': 60,
                    'maxConsecutiveFailures': 6,
                    'path': '/status',
                }
            ],
            'env': {},
            'id': fake_job_id,
            'constraints': [["region", "GROUP_BY", "1"]],
        }
        assert actual == expected

        # Assert that the complete config can be inserted into the MarathonApp model
        assert MarathonApp(**actual)
