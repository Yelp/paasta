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
        'iteration': 'testin',
    }
    fake_srv_config = {
        'runs_on': ['some-box'],
        'deployed_on': ['another-box'],
    }

    def test_read_service_config(self):
        fake_name = 'jazz'
        fake_instance = 'solo'
        fake_cluster = 'amnesia'
        fake_dir = '/nail/home/sanfran'

        def conf_helper(name, filename, soa_dir="AAAAAAAAA"):
            if filename == 'marathon-amnesia':
                return {fake_instance: self.fake_marathon_job_config}
            elif filename == 'service':
                return self.fake_srv_config
            else:
                raise Exception('read_service_config tried to access invalid filename %s' % filename)

        expected = dict(self.fake_srv_config.items() + self.fake_marathon_job_config.items())
        with mock.patch('service_configuration_lib.read_extra_service_information',
                        side_effect=conf_helper) as read_extra_info_patch:
            actual = marathon_tools.read_service_config(fake_name, fake_instance,
                                                        fake_cluster, fake_dir)
            assert expected == actual
            read_extra_info_patch.assert_any_call(fake_name, "service", soa_dir=fake_dir)
            read_extra_info_patch.assert_any_call(fake_name, "marathon-amnesia", soa_dir=fake_dir)
            assert read_extra_info_patch.call_count == 2

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

    def test_brutal_bounce(self):
        old_ids = ["bbounce", "the_best_bounce_method"]
        new_config = {"now_featuring": "no_gracefuls", "guaranteed": "or_your_money_back", 'id': 'none'}
        fake_client = mock.MagicMock(delete_app=mock.Mock(), create_app=mock.Mock())
        marathon_tools.brutal_bounce(old_ids, new_config, fake_client)
        for oid in old_ids:
            fake_client.delete_app.assert_any_call(oid)
        assert fake_client.delete_app.call_count == len(old_ids)
        fake_client.create_app.assert_called_once_with(**new_config)

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

    def test_read_service_namespace_config_exists(self):
        name = 'eman'
        namespace = 'ecapseman'
        soa_dir = 'rid_aos'
        fake_uri = 'energy'
        fake_timeout = -10103
        fake_port = 777
        fake_info = {'healthcheck_uri': fake_uri, 'healthcheck_timeout_s': fake_timeout,
                     'proxy_port': fake_port,
                     'routes': [{'source': 'oregon', 'destinations': ['indiana']},
                                {'source': 'florida', 'destinations': ['miami', 'beach']}]}
        fake_config = {namespace: fake_info}
        expected = {'healthcheck_uri': fake_uri, 'healthcheck_timeout_s': fake_timeout,
                    'proxy_port': fake_port,
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

    @mock.patch('pycurl.Curl', return_value=mock.Mock(setopt=mock.Mock(), perform=mock.Mock()))
    @mock.patch('json.loads')
    def test_marathon_services_running_on(self, json_load_patch, curl_patch):
        id_1 = 'klingon.ships.detected.249qwiomelht4jioewglkemr'
        id_2 = 'fire.photon.torpedos.jtgriemot5yhtwe94'
        id_3 = 'dota.axe.cleave.482u9jyoi4wed'
        id_4 = 'mesos.deployment.is.hard'
        ports_1 = '[111-111]'
        ports_2 = '[222-222]'
        ports_3 = '[333-333]'
        ports_4 = '[444-444]'
        hostname = 'io-dev.oiio.io'
        port = 123456789
        timeout = -99

        curl_patch.return_value.perform.return_value = 'curl_into_a_corner'
        json_load_patch.return_value = {'frameworks': [
                                            {'executors': [
                                                {'id': id_1, 'resources': {'ports': ports_1}},
                                                {'id': id_2, 'resources': {'ports': ports_2}}],
                                             'name': 'marathon-1111111'},
                                            {'executors': [
                                                {'id': id_3, 'resources': {'ports': ports_3}},
                                                {'id': id_4, 'resources': {'ports': ports_4}}],
                                             'name': 'marathon-3145jgreoifd'},
                                            {'executors': [
                                                {'id': 'bunk', 'resources': {'ports': '[65-65]'}}],
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
        json_load_patch.assert_called_once_with(curl_patch.return_value.perform.return_value)
        assert expected == actual

    @mock.patch('marathon_tools.marathon_services_running_on', return_value='chipotle')
    def test_marathon_services_running_here(self, mesos_on_patch):
        port = 808
        timeout = 9999
        assert marathon_tools.marathon_services_running_here(port, timeout) == 'chipotle'
        mesos_on_patch.assert_called_once_with(port=port, timeout_s=timeout)

    def test_get_services_running_here_for_nerve(self):
        cluster = 'edelweiss'
        soa_dir = 'the_sound_of_music'
        fake_marathon_services = [('no_test', 'left_behind', 1111),
                                  ('no_docstrings', 'forever_abandoned', 2222)]
        fake_normal_services = [('no_water'), ('no_life')]
        fake_port_files = ['trop', 'prot']
        fake_ports = [101, 202]
        namespaces = ['dos', 'uno']
        nerve_dicts = [{'ten': 10}, {'nine': 9}, {'binary': 1}, {'clock': 0}]
        expected = [('no_test.uno', {'clock': 0, 'port': 1111}),
                    ('no_docstrings.dos', {'binary': 1, 'port': 2222}),
                    ('no_water.main', {'nine': 9, 'port': 202}),
                    ('no_water', {'nine': 9, 'port': 202}),
                    ('no_life.main', {'ten': 10, 'port': 101}),
                    ('no_life', {'ten': 10, 'port': 101})]
        with contextlib.nested(
            mock.patch('marathon_tools.marathon_services_running_here',
                       return_value=fake_marathon_services),
            mock.patch('service_configuration_lib.services_that_run_here',
                       return_value=fake_normal_services),
            mock.patch('marathon_tools.read_namespace_for_service_instance',
                       side_effect=lambda a, b, c, d: namespaces.pop()),
            mock.patch('marathon_tools.read_service_namespace_config',
                       side_effect=lambda a, b, c: nerve_dicts.pop()),
            mock.patch('os.path.join',
                       side_effect=lambda a, b, c: fake_port_files.pop()),
            mock.patch('service_configuration_lib.read_port',
                       side_effect=lambda a: fake_ports.pop())
        ) as (
            mara_srvs_here_patch,
            norm_srvs_here_patch,
            get_namespace_patch,
            read_ns_config_patch,
            join_patch,
            read_port_patch,
        ):
            actual = marathon_tools.get_services_running_here_for_nerve(cluster, soa_dir)
            assert expected == actual
            mara_srvs_here_patch.assert_called_once_with()
            norm_srvs_here_patch.assert_called_once_with()
            get_namespace_patch.assert_any_call('no_test', 'left_behind', cluster, soa_dir)
            get_namespace_patch.assert_any_call('no_docstrings', 'forever_abandoned', cluster, soa_dir)
            assert get_namespace_patch.call_count == 2
            read_ns_config_patch.assert_any_call('no_test', 'uno', soa_dir)
            read_ns_config_patch.assert_any_call('no_docstrings', 'dos', soa_dir)
            read_ns_config_patch.assert_any_call('no_water', 'main', soa_dir)
            read_ns_config_patch.assert_any_call('no_life', 'main', soa_dir)
            assert read_ns_config_patch.call_count == 4

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
            assert fake_curl.setopt.call_count == 2
            fake_curl.perform.assert_called_once_with()
            fake_curl.getinfo.assert_called_once_with(pycurl.REDIRECT_URL)

    def test_is_mesos_leader(self):
        fake_host = 'toast.host.roast'
        with mock.patch('marathon_tools.get_mesos_leader', return_value=fake_host) as get_leader_patch:
            assert marathon_tools.is_mesos_leader(fake_host)
            get_leader_patch.assert_called_once_with(fake_host)
