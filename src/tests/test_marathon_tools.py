import marathon_tools
import contextlib
import mock
import pycurl


class TestMarathonTools:

    fake_marathon_job_config = {
        'instances': 3,
        'cpus': 1,
        'mem': 100,
        'docker_image': 'test_docker:1.0',
        'branch': 'master',
        'iteration': 'testin',
    }
    fake_srv_config = {
        'runs_on': ['some-box'],
        'deployed_on': ['another-box'],
    }

    def test_get_deployments_json(self):
        file_mock = mock.MagicMock(spec=file)
        fake_filedata = '239jiogrefnb iqu23t4ren'
        file_mock.read = mock.Mock(return_value=fake_filedata)
        fake_path = '/etc/nope.json'
        fake_dir = '/var/dir_of_fake'
        fake_json = {'no_srv:blaster': 'test_rocker:9.9', 'dont_care:about': 'this:guy'}
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
        fake_json = {'no_srv:blaster': 'test_rocker:9.9', 'dont_care:about': 'this:guy'}
        with mock.patch("marathon_tools._get_deployments_json", return_value=fake_json):
            actual = marathon_tools.get_docker_from_branch(fake_srv, fake_branch, fake_dir)
            assert actual == 'test_rocker:9.9'

    def test_get_deployed_images(self):
        fake_json = {'no_srv:blaster': 'test_rocker:9.9', 'dont_care:about': 'this:guy'}
        with mock.patch("marathon_tools._get_deployments_json", return_value=fake_json):
            actual = marathon_tools.get_deployed_images()
            expected = set(['test_rocker:9.9', 'this:guy'])
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
        with contextlib.nested(
            mock.patch('service_configuration_lib.read_extra_service_information',
                       side_effect=conf_helper),
            mock.patch('marathon_tools.get_docker_from_branch',
                       return_value=fake_docker)
        ) as (
            read_extra_info_patch,
            get_docker_patch
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
            open_file_patch.assert_called_once_with('/etc/service_deployment_tools/marathon_config.json')
            file_mock.read.assert_called_once_with()
            json_patch.assert_called_once_with(file_mock.read())

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
        json_load_patch.return_value = {'frameworks': [
                                            {'executors': [
                                                {'id': id_1, 'resources': {'ports': ports_1}, 'tasks': [{u'state': u'TASK_RUNNING'}]},
                                                {'id': id_2, 'resources': {'ports': ports_2}, 'tasks': [{u'state': u'TASK_RUNNING'}]}],
                                             'name': 'marathon-1111111'},
                                            {'executors': [
                                                {'id': id_3, 'resources': {'ports': ports_3}, 'tasks': [{u'state': u'TASK_RUNNING'}]},
                                                {'id': id_4, 'resources': {'ports': ports_4}, 'tasks': [{u'state': u'TASK_RUNNING'}]}],
                                             'name': 'marathon-3145jgreoifd'},
                                            {'executors': [
                                                {'id': id_5, 'resources': {'ports': ports_5}, 'tasks': [{u'state': u'TASK_STAGED'}]}],
                                             'name': 'marathon-754rchoeurcho'},
                                            {'executors': [
                                                {'id': 'bunk', 'resources': {'ports': '[65-65]'}, 'tasks': [{u'state': u'TASK_RUNNING'}]}],
                                             'name': 'super_bunk'}
                                        ]}
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

    def test_create_complete_config(self):
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
        fake_cpus = -1
        fake_instances = 101
        fake_args = ['arg1', 'arg2']
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
            'args': fake_args
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
            get_args_patch
        ):
            actual = marathon_tools.create_complete_config(fake_id, fake_url, fake_volumes,
                                                           self.fake_marathon_job_config)
            assert actual == expected_conf
            get_mem_patch.assert_called_once_with(self.fake_marathon_job_config)
            get_cpus_patch.assert_called_once_with(self.fake_marathon_job_config)
            get_constraints_patch.assert_called_once_with(self.fake_marathon_job_config)
            get_instances_patch.assert_called_once_with(self.fake_marathon_job_config)
            get_args_patch.assert_called_once_with(self.fake_marathon_job_config)

    def test_get_bounce_method_in_config(self):
        fake_method = 'aaargh'
        fake_conf = {'bounce_method': fake_method}
        assert marathon_tools.get_bounce_method(fake_conf) == fake_method

    def test_get_bounce_method_default(self):
        assert marathon_tools.get_bounce_method({}) == 'brutal'

    def test_get_instances_in_config(self):
        fake_conf = {'instances': -10}
        assert marathon_tools.get_instances(fake_conf) == -10

    def test_get_instances_default(self):
        assert marathon_tools.get_instances({}) == 1

    def test_get_constraints_in_config(self):
        fake_conf = {'constraints': 'so_many_walls'}
        assert marathon_tools.get_constraints(fake_conf) == 'so_many_walls'

    def test_get_constraints_default(self):
        assert marathon_tools.get_constraints({}) is None

    def test_get_cpus_in_config(self):
        fake_conf = {'cpus': -5}
        assert marathon_tools.get_cpus(fake_conf) == -5

    def test_get_cpus_default(self):
        assert marathon_tools.get_cpus({}) == 1

    def test_get_mem_in_config(self):
        fake_conf = {'mem': -999}
        assert marathon_tools.get_mem(fake_conf) == -999

    def test_get_mem_default(self):
        assert marathon_tools.get_mem({}) == 100

    def test_get_args_default(self):
        assert marathon_tools.get_args({}) == []

    def test_get_args_in_config(self):
        fake_conf = {'args': ['arg1', 'arg2']}
        assert marathon_tools.get_args(fake_conf) == ['arg1', 'arg2']

    def test_get_docker_url_no_error(self):
        fake_registry = "im.a-real.vm"
        fake_image = "and-i-can-run:1.0"
        fake_curl = mock.Mock()
        fake_stringio = mock.Mock(getvalue=mock.Mock(return_value='483af83b81ee93ac930d'))
        expected = "%s/%s" % (fake_registry, fake_image)
        with contextlib.nested(
            mock.patch('pycurl.Curl', return_value=fake_curl),
            mock.patch('marathon_tools.StringIO', return_value=fake_stringio)
        ) as (
            pycurl_patch,
            stringio_patch
        ):
            assert marathon_tools.get_docker_url(fake_registry, fake_image) == expected
            fake_curl.setopt.assert_any_call(pycurl.URL,
                                             'http://%s/v1/repositories/%s/tags/%s' % (
                                                    fake_registry,
                                                    fake_image.split(':')[0],
                                                    fake_image.split(':')[1]))
            fake_curl.setopt.assert_any_call(pycurl.WRITEFUNCTION, fake_stringio.write)

            assert fake_curl.setopt.call_count == 3
            fake_curl.setopt.assert_any_call(pycurl.TIMEOUT, 30)
            fake_curl.perform.assert_called_once_with()
            fake_stringio.getvalue.assert_called_once_with()

    def test_get_docker_url_has_error(self):
        fake_registry = "youre.just.virtual"
        fake_image = "just-a-shadow-of-reality:0.9"
        fake_curl = mock.Mock()
        fake_stringio = mock.Mock(getvalue=mock.Mock(return_value='all the errors ever'))
        expected = ""
        with contextlib.nested(
            mock.patch('pycurl.Curl', return_value=fake_curl),
            mock.patch('marathon_tools.StringIO', return_value=fake_stringio)
        ) as (
            pycurl_patch,
            stringio_patch
        ):
            assert marathon_tools.get_docker_url(fake_registry, fake_image) == expected
            fake_curl.setopt.assert_any_call(pycurl.URL,
                                             'http://%s/v1/repositories/%s/tags/%s' % (
                                                    fake_registry,
                                                    fake_image.split(':')[0],
                                                    fake_image.split(':')[1]))
            fake_curl.setopt.assert_any_call(pycurl.WRITEFUNCTION, fake_stringio.write)
            assert fake_curl.setopt.call_count == 3
            fake_curl.setopt.assert_any_call(pycurl.TIMEOUT, 30)
            fake_curl.perform.assert_called_once_with()
            fake_stringio.getvalue.assert_called_once_with()

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
        with mock.patch('marathon_tools.list_all_marathon_app_ids', return_value=fake_all_marathon_app_ids) as list_all_marathon_app_ids_patch:
            assert marathon_tools.is_app_id_running(fake_id, fake_client) is True
            list_all_marathon_app_ids_patch.assert_called_once_with(fake_client)

    def test_is_app_id_running_false(self):
        fake_id = 'fake_app3'
        fake_all_marathon_app_ids = ['fake_app1', 'fake_app2']
        fake_client = mock.Mock()
        with mock.patch('marathon_tools.list_all_marathon_app_ids', return_value=fake_all_marathon_app_ids) as list_all_marathon_app_ids_patch:
            assert marathon_tools.is_app_id_running(fake_id, fake_client) is False
            list_all_marathon_app_ids_patch.assert_called_once_with(fake_client)
