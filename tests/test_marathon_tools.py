import contextlib

from marathon.models import MarathonApp
import mock
import pycurl
from pytest import raises

import marathon_tools


class TestMarathonTools:

    fake_marathon_job_config = {
        'instances': 3,
        'cpus': 1,
        'mem': 100,
        'docker_image': 'test_docker:1.0',
        'branch': 'master',
        'desired_state': 'start',
        'force_bounce': None,
    }
    fake_srv_config = {
        'runs_on': ['some-box'],
        'deployed_on': ['another-box'],
    }
    fake_docker_registry = 'remote_registry.com'
    fake_marathon_config = {
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
    }

    def test_get_deployments_json(self):
        file_mock = mock.MagicMock(spec=file)
        fake_filedata = '239jiogrefnb iqu23t4ren'
        file_mock.read = mock.Mock(return_value=fake_filedata)
        fake_path = '/etc/nope.json'
        fake_dir = '/var/dir_of_fake'
        fake_json = {
            'no_srv:blaster': {
                'docker_image': 'test_rocker:9.9',
                'desired_state': 'start',
                'force_bounce': None,
            },
            'dont_care:about': {
                'docker_image': 'this:guy',
                'desired_state': 'stop',
                'force_bounce': '12345',
            },
        }
        with contextlib.nested(
            mock.patch('marathon_tools.open', create=True, return_value=file_mock),
            mock.patch('os.path.join', return_value=fake_path),
            mock.patch('os.path.exists', return_value=True),
            mock.patch('json.loads', return_value=fake_json),
        ) as (
            open_patch,
            join_patch,
            exists_patch,
            json_patch
        ):
            actual = marathon_tools._get_deployments_json(fake_dir)
            join_patch.assert_called_once_with(fake_dir, 'deployments.json')
            exists_patch.assert_called_once_with(fake_path)
            open_patch.assert_called_once_with(fake_path)
            file_mock.read.assert_called_once_with()
            json_patch.assert_called_once_with(fake_filedata)
            assert actual == fake_json

    def test_get_docker_from_branch(self):
        fake_srv = 'no_srv'
        fake_branch = 'blaster'
        fake_dir = '/var/dir_of_fake'
        fake_json = {
            'v1': {
                'no_srv:blaster': {
                    'docker_image': 'test_rocker:9.9',
                    'desired_state': 'start',
                    'force_bounce': None,
                },
                'dont_care:about': {
                    'docker_image': 'this:guy',
                    'desired_state': 'stop',
                    'force_bounce': '12345',
                },
            },
        }
        with mock.patch("marathon_tools._get_deployments_json", return_value=fake_json):
            actual = marathon_tools.get_docker_from_branch(fake_srv, fake_branch, fake_dir)
            assert actual == 'test_rocker:9.9'

    def test_get_force_bounce_from_branch(self):
        fake_dir = '/var/dir_of_fake'
        fake_json = {
            'v1': {
                'no_srv:blaster': {
                    'docker_image': 'test_rocker:9.9',
                    'desired_state': 'start',
                    'force_bounce': None,
                },
                'no_soap:radio': {
                    'docker_image': 'this:guy',
                    'desired_state': 'stop',
                    'force_bounce': '12345',
                },
            },
        }
        with mock.patch("marathon_tools._get_deployments_json", return_value=fake_json):
            assert None is marathon_tools.get_force_bounce_from_branch('no_srv', 'blaster', fake_dir)
            assert '12345' == marathon_tools.get_force_bounce_from_branch('no_soap', 'radio', fake_dir)

    def test_get_desired_state_from_branch(self):
        fake_dir = '/var/dir_of_fake'
        fake_json = {
            'v1': {
                'no_srv:blaster': {
                    'docker_image': 'test_rocker:9.9',
                    'desired_state': 'start',
                    'force_bounce': None,
                },
                'no_soap:radio': {
                    'docker_image': 'this:guy',
                    'desired_state': 'stop',
                    'force_bounce': '12345',
                },
            },
        }
        with mock.patch("marathon_tools._get_deployments_json", return_value=fake_json):
            assert 'start' == marathon_tools.get_desired_state_from_branch('no_srv', 'blaster', fake_dir)
            assert 'stop' == marathon_tools.get_desired_state_from_branch('no_soap', 'radio', fake_dir)

    def test_get_deployed_images(self):
        fake_json = {
            'no_srv:blaster': {
                'docker_image': 'test_rocker:9.9',
                'desired_state': 'start',
                'force_bounce': None,
            },
            'dont_care:about': {
                'docker_image': 'this:guy',
                'desired_state': 'stop',
                'force_bounce': '12345',
            },
        }
        with mock.patch("marathon_tools._get_deployments_json", return_value=fake_json):
            actual = marathon_tools.get_deployed_images()
            expected = set(['test_rocker:9.9'])
            assert actual == expected

    def test_read_monitoring_config(self):
        fake_name = 'partial'
        fake_fname = 'acronyms'
        fake_path = 'ever_patched'
        fake_soa_dir = '/nail/cte/oas'
        fake_dict = {'e': 'quail', 'v': 'snail'}
        with contextlib.nested(
            mock.patch('os.path.abspath', return_value=fake_path),
            mock.patch('os.path.join', return_value=fake_fname),
            mock.patch('service_configuration_lib.read_monitoring', return_value=fake_dict)
        ) as (
            abspath_patch,
            join_patch,
            read_monitoring_patch
        ):
            actual = marathon_tools.read_monitoring_config(fake_name, fake_soa_dir)
            assert fake_dict == actual
            abspath_patch.assert_called_once_with(fake_soa_dir)
            join_patch.assert_called_once_with(fake_path, fake_name, 'monitoring.yaml')
            read_monitoring_patch.assert_called_once_with(fake_fname)

    def test_read_service_config(self):
        fake_name = 'jazz'
        fake_instance = 'solo'
        fake_cluster = 'amnesia'
        fake_dir = '/nail/home/sanfran'
        fake_docker = 'no_docker:9.9'
        config_copy = self.fake_marathon_job_config.copy()
        del config_copy['docker_image']

        def conf_helper(name, filename, soa_dir="AAAAAAAAA"):
            if filename == 'marathon-amnesia':
                return {fake_instance: config_copy}
            elif filename == 'service':
                return self.fake_srv_config
            else:
                raise Exception('read_service_config tried to access invalid filename %s' % filename)

        expected = dict(self.fake_srv_config.items() + self.fake_marathon_job_config.items())
        expected['docker_image'] = fake_docker
        expected['desired_state'] = 'stop'
        expected['force_bounce'] = '12345'

        with contextlib.nested(
            mock.patch('service_configuration_lib.read_extra_service_information',
                       side_effect=conf_helper),
            mock.patch('marathon_tools.get_docker_from_branch',
                       return_value=fake_docker),
            mock.patch('marathon_tools.get_desired_state_from_branch',
                       return_value='stop'),
            mock.patch('marathon_tools.get_force_bounce_from_branch',
                       return_value='12345')
        ) as (
            read_extra_info_patch,
            get_docker_patch,
            get_desired_state_patch,
            get_force_bounce_patch,
        ):
            actual = marathon_tools.read_service_config(fake_name, fake_instance,
                                                        fake_cluster, fake_dir)
            assert expected == actual
            read_extra_info_patch.assert_any_call(fake_name, "service", soa_dir=fake_dir)
            read_extra_info_patch.assert_any_call(fake_name, "marathon-amnesia", soa_dir=fake_dir)
            assert read_extra_info_patch.call_count == 2
            get_docker_patch.assert_called_once_with(fake_name, self.fake_marathon_job_config['branch'],
                                                     fake_dir)

    def test_get_service_instance_list(self):
        fake_name = 'hint'
        fake_instance_1 = 'unsweet'
        fake_instance_2 = 'water'
        fake_cluster = '16floz'
        fake_dir = '/nail/home/hipster'
        fake_job_config = {fake_instance_1: self.fake_marathon_job_config,
                           fake_instance_2: self.fake_marathon_job_config}
        expected = [(fake_name, fake_instance_2), (fake_name, fake_instance_1)]
        with mock.patch('service_configuration_lib.read_extra_service_information',
                        return_value=fake_job_config) as read_extra_info_patch:
            actual = marathon_tools.get_service_instance_list(fake_name, fake_cluster, fake_dir)
            assert cmp(expected, actual) == 0
            read_extra_info_patch.assert_called_once_with(fake_name, "marathon-16floz", soa_dir=fake_dir)

    def test_get_config(self):
        expected = 'end_of_the_line'
        file_mock = mock.MagicMock(spec=file)
        with contextlib.nested(
            mock.patch('marathon_tools.open', create=True, return_value=file_mock),
            mock.patch('json.loads', return_value=expected)
        ) as (
            open_file_patch,
            json_patch
        ):
            assert marathon_tools.get_config() == expected
            open_file_patch.assert_called_once_with('/etc/paasta_tools/marathon_config.json')
            file_mock.read.assert_called_once_with()
            json_patch.assert_called_once_with(file_mock.read())

    def test_get_cluster(self):
        fake_config = {
            'cluster': 'peanut',
        }
        expected = 'peanut'
        with mock.patch(
            'marathon_tools.get_config',
            return_value=fake_config,
        ):
            actual = marathon_tools.get_cluster()
            assert actual == expected

    def test_get_cluster_dne(self):
        fake_config = {}
        with mock.patch(
            'marathon_tools.get_config',
            return_value=fake_config,
        ):
            with raises(marathon_tools.NoMarathonClusterFoundException):
                marathon_tools.get_cluster()

    def test_get_cluster_other_exception(self):
        with mock.patch(
            'marathon_tools.get_config',
            side_effect=SyntaxError,
        ):
            with raises(SyntaxError):
                marathon_tools.get_cluster()

    def test_list_clusters_no_service(self):
        with contextlib.nested(
            mock.patch('service_configuration_lib.read_services_configuration'),
            mock.patch('marathon_tools.get_clusters_deployed_to'),
        ) as (
            mock_read_services,
            mock_get_clusters_deployed_to,
        ):
            mock_read_services.return_value = {'service1': 'config'}
            mock_get_clusters_deployed_to.return_value = ['cluster1', 'cluster2']
            actual = marathon_tools.list_clusters()
            expected = ['cluster1', 'cluster2']
            assert actual == expected

    def test_list_clusters_with_service(self):
        with contextlib.nested(
            mock.patch('service_configuration_lib.read_services_configuration'),
            mock.patch('marathon_tools.get_clusters_deployed_to'),
        ) as (
            mock_read_services,
            mock_get_clusters_deployed_to,
        ):
            fake_service = 'fake_service'
            mock_read_services.return_value = {fake_service: 'config', 'fake_service2': 'config'}
            mock_get_clusters_deployed_to.return_value = ['cluster1', 'cluster2']
            actual = marathon_tools.list_clusters(fake_service)
            expected = ['cluster1', 'cluster2']
            assert actual == expected
            mock_get_clusters_deployed_to.assert_called_once_with(fake_service)

    def test_get_clusters_deployed_to(self):
        service = 'fake_service'
        fake_marathon_filenames = ['marathon-cluster1.yaml', 'marathon-cluster2.yaml',
                                   'marathon-SHARED.yaml', 'marathon-cluster3.yaml']
        expected = ['cluster1', 'cluster2', 'cluster3']
        with contextlib.nested(
            mock.patch('os.path.isdir'),
            mock.patch('glob.glob'),
        ) as (
            mock_isdir,
            mock_glob
        ):
            mock_isdir.return_value = True
            mock_glob.return_value = fake_marathon_filenames
            actual = marathon_tools.get_clusters_deployed_to(service)
            assert expected == actual

    def test_list_all_marathon_instance_for_service(self):
        service = 'fake_service'
        clusters = ['fake_cluster']
        mock_instances = [(service, 'instance1'), (service, 'instance2')]
        expected = set(['instance1', 'instance2'])
        with contextlib.nested(
            mock.patch('marathon_tools.list_clusters'),
            mock.patch('marathon_tools.get_service_instance_list'),
        ) as (
            mock_list_clusters,
            mock_service_instance_list,
        ):
            mock_list_clusters.return_value = clusters
            mock_service_instance_list.return_value = mock_instances
            actual = marathon_tools.list_all_marathon_instances_for_service(service)
            assert actual == expected
            mock_list_clusters.assert_called_once_with(service)
            mock_service_instance_list.assert_called_once_with(service, clusters[0])

    def test_get_all_namespaces_for_service(self):
        name = 'vvvvvv'
        soa_dir = '^_^'
        t1_dict = {'hollo': 'werld', 'smark': 'stact'}
        t2_dict = {'vataman': 'witir', 'sin': 'chaps'}
        fake_smartstack = {'t1': t1_dict, 't2': t2_dict}
        expected = [('vvvvvv.t2', t2_dict), ('vvvvvv.t1', t1_dict)]
        with mock.patch('service_configuration_lib.read_extra_service_information',
                        return_value=fake_smartstack) as read_extra_patch:
            actual = marathon_tools.get_all_namespaces_for_service(name, soa_dir)
            assert expected == actual
            read_extra_patch.assert_called_once_with(name, 'smartstack', soa_dir)

    def test_get_marathon_services_for_cluster(self):
        cluster = 'honey_bunches_of_oats'
        soa_dir = 'completely_wholesome'
        instances = [['this_is_testing', 'all_the_things'], ['my_nerf_broke']]
        expected = ['my_nerf_broke', 'this_is_testing', 'all_the_things']
        with contextlib.nested(
            mock.patch('os.path.abspath', return_value='chex_mix'),
            mock.patch('os.listdir', return_value=['dir1', 'dir2']),
            mock.patch('marathon_tools.get_service_instance_list',
                       side_effect=lambda a, b, c: instances.pop())
        ) as (
            abspath_patch,
            listdir_patch,
            get_instances_patch,
        ):
            actual = marathon_tools.get_marathon_services_for_cluster(cluster, soa_dir)
            assert expected == actual
            abspath_patch.assert_called_once_with(soa_dir)
            listdir_patch.assert_called_once_with('chex_mix')
            get_instances_patch.assert_any_call('dir1', cluster, soa_dir)
            get_instances_patch.assert_any_call('dir2', cluster, soa_dir)
            assert get_instances_patch.call_count == 2

    def test_get_all_namespaces(self):
        soa_dir = 'carbon'
        namespaces = [[('aluminum', {'hydrogen': 1}), ('potassium', {'helium': 2})],
                      [('uranium', {'lithium': 3}), ('gold', {'boron': 5})]]
        expected = [('uranium', {'lithium': 3}), ('gold', {'boron': 5}),
                    ('aluminum', {'hydrogen': 1}), ('potassium', {'helium': 2})]
        with contextlib.nested(
            mock.patch('os.path.abspath', return_value='oxygen'),
            mock.patch('os.listdir', return_value=['rid1', 'rid2']),
            mock.patch('marathon_tools.get_all_namespaces_for_service',
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
        fake_nerve = {'proxy_port': fake_port}
        with contextlib.nested(
            mock.patch('marathon_tools.read_namespace_for_service_instance', return_value=namespace),
            mock.patch('marathon_tools.read_service_namespace_config', return_value=fake_nerve)
        ) as (
            read_ns_patch,
            read_config_patch
        ):
            actual = marathon_tools.get_proxy_port_for_instance(name, instance, cluster, soa_dir)
            assert fake_port == actual
            read_ns_patch.assert_called_once_with(name, instance, cluster, soa_dir)
            read_config_patch.assert_called_once_with(name, namespace, soa_dir)

    def test_get_mode_for_instance_present(self):
        name = 'stage_env'
        instance = 'in_aws'
        cluster = 'thats_crazy'
        soa_dir = 'the_future'
        namespace = 'is_here'
        fake_mode = 'banana'
        fake_nerve = {'mode': fake_mode}
        with contextlib.nested(
            mock.patch('marathon_tools.read_namespace_for_service_instance', return_value=namespace),
            mock.patch('marathon_tools.read_service_namespace_config', return_value=fake_nerve)
        ) as (
            read_ns_patch,
            read_config_patch
        ):
            actual = marathon_tools.get_mode_for_instance(name, instance, cluster, soa_dir)
            assert fake_mode == actual
            read_ns_patch.assert_called_once_with(name, instance, cluster, soa_dir)
            read_config_patch.assert_called_once_with(name, namespace, soa_dir)

    def test_get_mode_for_instance_default(self):
        name = 'stage_env'
        instance = 'in_aws'
        cluster = 'thats_crazy'
        soa_dir = 'the_future'
        namespace = 'is_here'
        expected = 'http'
        with contextlib.nested(
            mock.patch('marathon_tools.read_namespace_for_service_instance', return_value=namespace),
            mock.patch('marathon_tools.read_service_namespace_config', return_value={})
        ) as (
            read_ns_patch,
            read_config_patch
        ):
            actual = marathon_tools.get_mode_for_instance(name, instance, cluster, soa_dir)
            assert expected == actual
            read_ns_patch.assert_called_once_with(name, instance, cluster, soa_dir)
            read_config_patch.assert_called_once_with(name, namespace, soa_dir)

    def test_read_service_namespace_config_exists(self):
        name = 'eman'
        namespace = 'ecapseman'
        soa_dir = 'rid_aos'
        fake_uri = 'energy'
        fake_mode = 'ZTP'
        fake_timeout = -10103
        fake_port = 777
        fake_retries = 9001
        fake_info = {'healthcheck_uri': fake_uri, 'healthcheck_timeout_s': fake_timeout,
                     'proxy_port': fake_port,
                     'timeout_connect_ms': 192, 'timeout_server_ms': 291,
                     'timeout_client_ms': 912, 'retries': fake_retries, 'mode': fake_mode,
                     'routes': [{'source': 'oregon', 'destinations': ['indiana']},
                                {'source': 'florida', 'destinations': ['miami', 'beach']}]}
        fake_config = {namespace: fake_info}
        expected = {'healthcheck_uri': fake_uri, 'healthcheck_timeout_s': fake_timeout,
                    'proxy_port': fake_port,
                    'timeout_connect_ms': 192, 'timeout_server_ms': 291,
                    'timeout_client_ms': 912, 'retries': fake_retries, 'mode': fake_mode,
                    'routes': [('oregon', 'indiana'), ('florida', 'miami'), ('florida', 'beach')]}
        with mock.patch('service_configuration_lib.read_extra_service_information',
                        return_value=fake_config) as read_extra_patch:
            assert marathon_tools.read_service_namespace_config(name, namespace, soa_dir) == expected
            read_extra_patch.assert_called_once_with(name, 'smartstack', soa_dir)

    def test_read_service_namespace_config_no_file(self):
        name = 'a_man'
        namespace = 'a_boat'
        soa_dir = 'an_adventure'

        def raiser(a, b, c):
            raise Exception
        with mock.patch('service_configuration_lib.read_extra_service_information',
                        side_effect=raiser) as read_extra_patch:
            assert marathon_tools.read_service_namespace_config(name, namespace, soa_dir) == {}
            read_extra_patch.assert_called_once_with(name, 'smartstack', soa_dir)

    @mock.patch('service_configuration_lib.read_extra_service_information')
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

    @mock.patch('service_configuration_lib.read_extra_service_information')
    def test_read_namespace_for_service_instance_no_value(self, read_info_patch):
        name = 'wall_light'
        instance = 'ceiling_light'
        cluster = 'no_light'
        soa_dir = 'warehouse_light'
        read_info_patch.return_value = {instance: {'aaaaaaaa': ['bbbbbbbb']}}
        actual = marathon_tools.read_namespace_for_service_instance(name, instance, cluster, soa_dir)
        assert actual == instance
        read_info_patch.assert_called_once_with(name, 'marathon-%s' % cluster, soa_dir)

    @mock.patch('marathon_tools.StringIO', return_value=mock.Mock(getvalue=mock.Mock()))
    @mock.patch('pycurl.Curl', return_value=mock.Mock(setopt=mock.Mock(), perform=mock.Mock()))
    @mock.patch('json.loads')
    def test_marathon_services_running_on(self, json_load_patch, curl_patch, stringio_patch):
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
        hostname = 'io-dev.oiio.io'
        port = 123456789
        timeout = -99

        stringio_patch.return_value.getvalue.return_value = 'curl_into_a_corner'
        json_load_patch.return_value = {
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
        actual = marathon_tools.marathon_services_running_on(hostname, port, timeout)
        curl_patch.return_value.setopt.assert_any_call(pycurl.URL,
                                                       'http://%s:%s/state.json' % (hostname, port))
        curl_patch.return_value.setopt.assert_any_call(pycurl.TIMEOUT, timeout)
        curl_patch.return_value.setopt.assert_any_call(pycurl.WRITEFUNCTION,
                                                       stringio_patch.return_value.write)
        json_load_patch.assert_called_once_with(stringio_patch.return_value.getvalue.return_value)
        assert expected == actual

    @mock.patch('marathon_tools.marathon_services_running_on', return_value='chipotle')
    def test_marathon_services_running_here(self, mesos_on_patch):
        port = 808
        timeout = 9999
        assert marathon_tools.marathon_services_running_here(port, timeout) == 'chipotle'
        mesos_on_patch.assert_called_once_with(port=port, timeout_s=timeout)

    def test_get_marathon_services_running_here_for_nerve(self):
        cluster = 'edelweiss'
        soa_dir = 'the_sound_of_music'
        fake_marathon_services = [('no_test', 'left_behind', 1111),
                                  ('no_docstrings', 'forever_abandoned', 2222)]
        namespaces = ['dos', 'uno']
        nerve_dicts = [{'binary': 1}, {'clock': 0}]
        expected = [('no_test.uno', {'clock': 0, 'port': 1111}),
                    ('no_docstrings.dos', {'binary': 1, 'port': 2222})]
        with contextlib.nested(
            mock.patch('marathon_tools.marathon_services_running_here',
                       return_value=fake_marathon_services),
            mock.patch('marathon_tools.read_namespace_for_service_instance',
                       side_effect=lambda a, b, c, d: namespaces.pop()),
            mock.patch('marathon_tools.read_service_namespace_config',
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
                'marathon_tools.get_cluster',
                side_effect=marathon_tools.NoMarathonClusterFoundException,
            ),
            mock.patch(
                'marathon_tools.marathon_services_running_here',
                return_value=[],
            ),
        ) as (
            get_cluster_patch,
            marathon_services_running_here_patch,
        ):
            actual = marathon_tools.get_marathon_services_running_here_for_nerve(cluster, soa_dir)
            assert actual == []

    def test_get_marathon_services_running_here_for_nerve_when_get_cluster_raises_other_exception(self):
        cluster = None
        soa_dir = 'the_sound_of_music'
        with contextlib.nested(
            mock.patch(
                'marathon_tools.get_cluster',
                side_effect=Exception,
            ),
            mock.patch(
                'marathon_tools.marathon_services_running_here',
                return_value=[],
            ),
        ) as (
            get_cluster_patch,
            marathon_services_running_here_patch,
        ):
            with raises(Exception):
                marathon_tools.get_marathon_services_running_here_for_nerve(cluster, soa_dir)

    def test_get_classic_service_information_for_nerve(self):
        with contextlib.nested(
            mock.patch('service_configuration_lib.read_port', return_value=101),
            mock.patch('marathon_tools.read_service_namespace_config', return_value={'ten': 10}),
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
                return_value=['d', 'c']
            ),
            mock.patch(
                'os.listdir',
                return_value=['b', 'a']
            ),
        ) as (
            services_that_run_here_patch,
            listdir_patch,
        ):
            services = marathon_tools.get_classic_services_that_run_here()
            assert services == ['a', 'b', 'c', 'd']
            services_that_run_here_patch.assert_called_once_with()
            listdir_patch.assert_called_once_with(marathon_tools.PUPPET_SERVICE_DIR)

    def test_get_classic_services_running_here_for_nerve(self):
        with contextlib.nested(
            mock.patch(
                'marathon_tools.get_classic_services_that_run_here',
                side_effect=lambda: ['a', 'b', 'c']
            ),
            mock.patch(
                'marathon_tools.get_classic_service_information_for_nerve',
                side_effect=lambda x, _: '%s.foo' % x
            ),
        ):
            assert marathon_tools.get_classic_services_running_here_for_nerve('baz') == [
                'a.foo', 'b.foo', 'c.foo',
            ]

    def test_get_services_running_here_for_nerve(self):
        cluster = 'plentea'
        soa_dir = 'boba'
        fake_marathon_services = [('never', 'again'), ('will', 'he')]
        fake_classic_services = [('walk', 'on'), ('his', 'feet')]
        expected = fake_marathon_services + fake_classic_services
        with contextlib.nested(
            mock.patch('marathon_tools.get_marathon_services_running_here_for_nerve',
                       return_value=fake_marathon_services),
            mock.patch('marathon_tools.get_classic_services_running_here_for_nerve',
                       return_value=fake_classic_services),
        ) as (
            marathon_patch,
            classic_patch
        ):
            actual = marathon_tools.get_services_running_here_for_nerve(cluster, soa_dir)
            assert expected == actual
            marathon_patch.assert_called_once_with(cluster, soa_dir)
            classic_patch.assert_called_once_with(soa_dir)

    def test_get_mesos_leader(self):
        expected = 'mesos.master.yelpcorp.com'
        fake_master = 'false.authority.yelpcorp.com'
        fake_curl = mock.Mock(setopt=mock.Mock(), perform=mock.Mock(),
                              getinfo=mock.Mock(return_value='http://%s:999' % expected))
        with mock.patch('pycurl.Curl', return_value=fake_curl) as curl_patch:
            assert marathon_tools.get_mesos_leader(fake_master) == expected
            curl_patch.assert_called_once_with()
            fake_curl.setopt.assert_any_call(pycurl.URL, 'http://%s:5050/redirect' % fake_master)
            fake_curl.setopt.assert_any_call(pycurl.HEADER, True)
            fake_curl.setopt.assert_any_call(pycurl.WRITEFUNCTION, mock.ANY)
            assert fake_curl.setopt.call_count == 3
            fake_curl.perform.assert_called_once_with()
            fake_curl.getinfo.assert_called_once_with(pycurl.REDIRECT_URL)

    def test_is_mesos_leader(self):
        fake_host = 'toast.host.roast'
        with mock.patch('marathon_tools.get_mesos_leader', return_value=fake_host) as get_leader_patch:
            assert marathon_tools.is_mesos_leader(fake_host)
            get_leader_patch.assert_called_once_with(fake_host)

    def test_compose_job_id_full(self):
        fake_name = 'someone_scheduled_docker_image'
        fake_id = 'docker_isnt_deployed'
        fake_instance = 'then_who_was_job'
        spacer = marathon_tools.ID_SPACER
        expected = '%s%s%s%s%s' % (fake_name.replace('_', '--'), spacer, fake_id.replace('_', '--'),
                                   spacer, fake_instance.replace('_', '--'))
        assert marathon_tools.compose_job_id(fake_name, fake_id, fake_instance) == expected

    def test_format_marathon_app_dict(self):
        fake_id = marathon_tools.compose_job_id('can_you_dig_it', 'yes_i_can')
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
        fake_cpus = .42
        fake_instances = 101
        fake_args = ['arg1', 'arg2']
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
                            'containerPort': 8888,
                            'hostPort': 0,
                            'protocol': 'tcp',
                        },
                    ],
                },
                'type': 'DOCKER',
                'volumes': fake_volumes,
            },
            'constraints': [],
            'uris': ['file:///root/.dockercfg', ],
            'mem': fake_mem,
            'cpus': fake_cpus,
            'instances': fake_instances,
            'args': fake_args,
            'health_checks': fake_healthchecks,
            'backoff_seconds': 1,
            'backoff_factor': 2,
        }
        with contextlib.nested(
            mock.patch('marathon_tools.get_mem', return_value=fake_mem),
            mock.patch('marathon_tools.get_cpus', return_value=fake_cpus),
            mock.patch('marathon_tools.get_constraints', return_value=[]),
            mock.patch('marathon_tools.get_instances', return_value=fake_instances),
            mock.patch('marathon_tools.get_args', return_value=fake_args),
        ) as (
            get_mem_patch,
            get_cpus_patch,
            get_constraints_patch,
            get_instances_patch,
            get_args_patch,
        ):
            actual = marathon_tools.format_marathon_app_dict(fake_id, fake_url, fake_volumes,
                                                             self.fake_marathon_job_config, fake_healthchecks)
            get_mem_patch.assert_called_once_with(self.fake_marathon_job_config)
            get_cpus_patch.assert_called_once_with(self.fake_marathon_job_config)
            get_constraints_patch.assert_called_once_with(self.fake_marathon_job_config)
            get_instances_patch.assert_called_once_with(self.fake_marathon_job_config)
            get_args_patch.assert_called_once_with(self.fake_marathon_job_config)
            assert actual == expected_conf

            # Assert that the complete config can be inserted into the MarathonApp model
            assert MarathonApp(**actual)

    def test_instances_is_zero_when_desired_state_is_stop(self):
        fake_id = marathon_tools.compose_job_id('can_you_dig_it', 'yes_i_can')
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

        fake_marathon_job_config = dict(self.fake_marathon_job_config)
        fake_marathon_job_config['desired_state'] = 'stop'

        config = marathon_tools.format_marathon_app_dict(fake_id, fake_url, fake_volumes,
                                                         fake_marathon_job_config, [])
        assert config['instances'] == 0

    def test_get_bounce_method_in_config(self):
        fake_method = 'aaargh'
        fake_conf = {'bounce_method': fake_method}
        assert marathon_tools.get_bounce_method(fake_conf) == fake_method

    def test_get_bounce_method_default(self):
        assert marathon_tools.get_bounce_method({}) == 'upthendown'

    def test_get_instances_in_config(self):
        fake_conf = {'instances': -10, 'desired_state': 'start'}
        assert marathon_tools.get_instances(fake_conf) == -10

    def test_get_instances_default(self):
        assert marathon_tools.get_instances({'desired_state': 'start'}) == 1

    def test_get_instances_respects_false(self):
        fake_conf = {'instances': False, 'desired_state': 'start'}
        assert marathon_tools.get_instances(fake_conf) == 0

    def test_get_constraints_in_config(self):
        fake_conf = {'constraints': 'so_many_walls'}
        assert marathon_tools.get_constraints(fake_conf) == 'so_many_walls'

    def test_get_constraints_default(self):
        assert marathon_tools.get_constraints({}) is None

    def test_get_cpus_in_config(self):
        fake_conf = {'cpus': -5}
        assert marathon_tools.get_cpus(fake_conf) == -5

    def test_get_cpus_in_config_float(self):
        fake_conf = {'cpus': .66}
        assert marathon_tools.get_cpus(fake_conf) == .66

    def test_get_cpus_default(self):
        assert marathon_tools.get_cpus({}) == .25

    def test_get_mem_in_config(self):
        fake_conf = {'mem': -999}
        assert marathon_tools.get_mem(fake_conf) == -999

    def test_get_mem_default(self):
        assert marathon_tools.get_mem({}) == 1000

    def test_get_args_default(self):
        assert marathon_tools.get_args({}) == []

    def test_get_args_in_config(self):
        fake_conf = {'args': ['arg1', 'arg2']}
        assert marathon_tools.get_args(fake_conf) == ['arg1', 'arg2']

    def test_get_force_bounce(self):
        fake_conf = {'force_bounce': 'blurp'}
        assert marathon_tools.get_force_bounce(fake_conf) == 'blurp'

    def test_get_desired_state(self):
        fake_conf = {'desired_state': 'stop'}
        assert marathon_tools.get_desired_state(fake_conf) == 'stop'

    def test_get_docker_url_no_error(self):
        fake_registry = "im.a-real.vm"
        fake_image = "and-i-can-run:1.0"
        expected = "%s/%s" % (fake_registry, fake_image)
        assert marathon_tools.get_docker_url(fake_registry, fake_image) == expected

    def test_get_docker_url_with_no_docker_image(self):
        with raises(marathon_tools.NoDockerImageError):
            marathon_tools.get_docker_url('fake_registry', None)

    def test_get_marathon_client(self):
        fake_url = "nothing_for_me_to_do_but_dance"
        fake_user = "the_boogie"
        fake_passwd = "is_for_real"
        with mock.patch('marathon_tools.MarathonClient') as client_patch:
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
            return_value=fake_all_marathon_app_ids,
        ) as list_all_marathon_app_ids_patch:
            assert marathon_tools.is_app_id_running(fake_id, fake_client) is False
            list_all_marathon_app_ids_patch.assert_called_once_with(fake_client)

    def test_get_app_id(self):
        fake_name = 'fakeapp'
        fake_instance = 'fakeinstance'
        fake_url = 'fake_url'
        fake_hash = 'CONFIGHASH'
        fake_code_sha = 'CODESHA'
        fake_config = {}

        with contextlib.nested(
            mock.patch('marathon_tools.read_service_config',
                       return_value=self.fake_marathon_job_config),
            mock.patch('marathon_tools.get_docker_url', return_value=fake_url),
            mock.patch('marathon_tools.format_marathon_app_dict',
                       return_value=fake_config),
            mock.patch('marathon_tools.get_config_hash', return_value=fake_hash),
            mock.patch('marathon_tools.get_code_sha_from_dockerurl', return_value=fake_code_sha),
        ) as (
            read_service_config_patch,
            docker_url_patch,
            format_marathon_app_dict_patch,
            hash_patch,
            code_sha_patch,
        ):
            assert marathon_tools.get_app_id(
                fake_name,
                fake_instance,
                self.fake_marathon_config
            ) == 'fakeapp.fakeinstance.CODESHA.CONFIGHASH'
            read_service_config_patch.assert_called_once_with(fake_name, fake_instance, soa_dir='/nail/etc/services')
            hash_patch.assert_called_once_with(fake_config, force_bounce=None)
            code_sha_patch.assert_called_once_with(fake_url)

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

        fake_service_config_1 = dict(self.fake_marathon_job_config)
        fake_service_config_1['desired_state'] = 'start'
        fake_service_config_1['force_bounce'] = '88888'

        fake_service_config_2 = dict(self.fake_marathon_job_config)
        fake_service_config_2['desired_state'] = 'start'
        fake_service_config_2['force_bounce'] = '99999'

        fake_service_config_3 = dict(self.fake_marathon_job_config)
        fake_service_config_3['desired_state'] = 'stop'
        fake_service_config_3['force_bounce'] = '99999'

        with contextlib.nested(
            mock.patch('marathon_tools.read_service_config'),
            mock.patch('marathon_tools.get_docker_url', return_value=fake_url),
        ) as (
            read_service_config_patch,
            docker_url_patch,
        ):
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
        fake_srv_config = {'nerve_ns': 'rojo'}

        def config_helper(name, inst, soa_dir=None):
            if inst == 'blue':
                return fake_srv_config
            else:
                return {'nerve_ns': 'amarillo'}

        with contextlib.nested(
            mock.patch('marathon_tools.get_service_instance_list',
                       return_value=fake_instances),
            mock.patch('marathon_tools.read_service_config',
                       side_effect=config_helper),
            mock.patch('marathon_tools.get_instances',
                       return_value=11)
        ) as (
            inst_list_patch,
            read_config_patch,
            get_inst_patch
        ):
            actual = marathon_tools.get_expected_instance_count_for_namespace(service_name, namespace, soa_dir)
            assert actual == 11
            inst_list_patch.assert_called_once_with(service_name, soa_dir=soa_dir)
            read_config_patch.assert_any_call(service_name, 'blue', soa_dir=soa_dir)
            read_config_patch.assert_any_call(service_name, 'green', soa_dir=soa_dir)
            get_inst_patch.assert_called_once_with(fake_srv_config)

    def test_get_healthchecks_http_overrides(self):
        fake_path = '/mycoolstatus'
        fake_config = {
            'mode': 'http',
            'healthcheck_uri': fake_path,
        }
        expected = [
            {
                "protocol": "HTTP",
                "path": fake_path,
                "gracePeriodSeconds": 60,
                "intervalSeconds": 10,
                "portIndex": 0,
                "timeoutSeconds": 10,
                "maxConsecutiveFailures": 6
            },
        ]
        with mock.patch('marathon_tools.read_service_namespace_config') as mock_read_service_namespace_config:
            mock_read_service_namespace_config.return_value = fake_config
            actual = marathon_tools.get_healthchecks('fake_service', 'fake_instance')
            assert mock_read_service_namespace_config.called_once_with('fake_service', 'fake_instance')
            assert actual == expected

    def test_get_healthchecks_http_defaults(self):
        fake_config = {}
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
        with mock.patch('marathon_tools.read_service_namespace_config') as mock_read_service_namespace_config:
            mock_read_service_namespace_config.return_value = fake_config
            actual = marathon_tools.get_healthchecks('fake_service', 'fake_instance')
            assert actual == expected

    def test_get_healthchecks_tcp(self):
        fake_config = {'mode': 'tcp'}
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
        with mock.patch('marathon_tools.read_service_namespace_config') as mock_read_service_namespace_config:
            mock_read_service_namespace_config.return_value = fake_config
            actual = marathon_tools.get_healthchecks('fake_service', 'fake_instance')
            assert actual == expected

    def test_get_healthchecks_other(self):
        fake_config = {'mode': 'other'}
        with mock.patch('marathon_tools.read_service_namespace_config') as mock_read_service_namespace_config:
            mock_read_service_namespace_config.return_value = fake_config
            with raises(marathon_tools.InvalidSmartstackMode):
                marathon_tools.get_healthchecks('fake_service', 'fake_instance')

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
