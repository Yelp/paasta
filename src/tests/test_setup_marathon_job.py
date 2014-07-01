#!/usr/bin/env python

import setup_marathon_job, marathon_tools
import mock
import contextlib
import sys

class TestSetupMarathonJob:

    fake_docker_image = 'test_docker:1.0'
    fake_marathon_job_config = {
        'instances': 3,
        'cpus': 1,
        'mem': 100,
        'docker_image': fake_docker_image,
        'iteration': 'testin',
    }
    fake_docker_registry = 'remote_registry.com'
    fake_marathon_config = {
        'cluster': 'test_cluster',
        'url': 'http://test_url',
        'user': 'admin',
        'pass': 'admin_pass',
        'docker_registry': fake_docker_registry,
        'docker_options': ['-v', 'you_wish_that_meant_verbose'],
        'executor': 'exeggutor',
    }
    fake_args = mock.MagicMock(
        service_name='what_is_love',
        instance_name='bby_dont_hurt_me',
        soa_dir='no_more',
        verbose=False,
    )


    def test_main_success(self):
        fake_client = mock.MagicMock()
        with contextlib.nested(
            mock.patch('setup_marathon_job.parse_args', return_value=self.fake_args),
            mock.patch('setup_marathon_job.get_main_marathon_config', return_value=self.fake_marathon_config),
            mock.patch('setup_marathon_job.get_marathon_client', return_value=fake_client),
            mock.patch('marathon_tools.read_srv_config', return_value=self.fake_marathon_job_config),
            mock.patch('setup_marathon_job.setup_service', return_value=True),
            mock.patch('sys.exit'),
        ) as (
            args_patch,
            main_conf_patch,
            client_patch,
            read_conf_patch,
            setup_patch,
            exit_patch,
        ):
            setup_marathon_job.main()
            args_patch.assert_called_once()
            main_conf_patch.assert_called_once()
            client_patch.assert_called_once_with(
                    self.fake_marathon_config['url'],
                    self.fake_marathon_config['user'],
                    self.fake_marathon_config['pass'])
            read_conf_patch.assert_called_once_with(
                    self.fake_args.service_name,
                    self.fake_args.instance_name,
                    self.fake_marathon_config['cluster'],
                    self.fake_args.soa_dir)
            setup_patch.assert_called_once_with(
                    self.fake_args.service_name,
                    self.fake_args.instance_name,
                    fake_client,
                    self.fake_marathon_config,
                    self.fake_marathon_job_config)
            exit_patch.assert_called_once_with(0)

    def test_main_failure(self):
        fake_client = mock.MagicMock()
        with contextlib.nested(
            mock.patch('setup_marathon_job.parse_args', return_value=self.fake_args),
            mock.patch('setup_marathon_job.get_main_marathon_config', return_value=self.fake_marathon_config),
            mock.patch('setup_marathon_job.get_marathon_client', return_value=fake_client),
            mock.patch('marathon_tools.read_srv_config', return_value=self.fake_marathon_job_config),
            mock.patch('setup_marathon_job.setup_service', return_value=False),
            mock.patch('sys.exit'),
        ) as (
            args_patch,
            main_conf_patch,
            client_patch,
            read_conf_patch,
            setup_patch,
            exit_patch,
        ):
            setup_marathon_job.main()
            args_patch.assert_called_once()
            main_conf_patch.assert_called_once()
            client_patch.assert_called_once_with(
                    self.fake_marathon_config['url'],
                    self.fake_marathon_config['user'],
                    self.fake_marathon_config['pass'])
            read_conf_patch.assert_called_once_with(
                    self.fake_args.service_name,
                    self.fake_args.instance_name,
                    self.fake_marathon_config['cluster'],
                    self.fake_args.soa_dir)
            setup_patch.assert_called_once_with(
                    self.fake_args.service_name,
                    self.fake_args.instance_name,
                    fake_client,
                    self.fake_marathon_config,
                    self.fake_marathon_job_config)
            exit_patch.assert_called_once_with(1)

    def test_setup_service_srv_already_exists(self):
        fake_name = 'if_trees_could_talk'
        fake_instance = 'would_they_scream'
        fake_client = mock.MagicMock(get_app=mock.Mock(return_value=True))
        fake_complete = { 'seven': 'full', 'eight': 'frightened', 'nine': 'eaten' }
        fake_url = 'docker:///what_is_a_test'
        spacer = setup_marathon_job.ID_TKN
        full_id = fake_name + spacer + fake_instance + spacer + self.fake_marathon_job_config['iteration']
        with contextlib.nested(
            mock.patch('setup_marathon_job.get_docker_url', return_value=fake_url),
            mock.patch('setup_marathon_job.create_complete_config', return_value=fake_complete)
        ) as (
            url_patch,
            config_patch,
        ):
            assert setup_marathon_job.setup_service(fake_name, fake_instance, fake_client,
                                             self.fake_marathon_config, self.fake_marathon_job_config)
            url_patch.assert_called_once_with(self.fake_docker_registry, self.fake_docker_image)
            config_patch.assert_called_once_with(full_id, fake_url,
                                                 self.fake_marathon_config['docker_options'],
                                                 self.fake_marathon_config['executor'],
                                                 self.fake_marathon_job_config)
            fake_client.get_app.assert_called_once_with(full_id)

    def test_setup_service_srv_does_not_exist(self):
        fake_name = 'if_talk_was_cheap'
        fake_instance = 'psychatrists_would_be_broke'
        fake_client = mock.MagicMock(get_app=mock.Mock(side_effect=KeyError))
        fake_complete = { 'do': 'you', 'even': 'dota' }
        fake_url = 'docker:///a_miserable_pile_of_mocks'
        fake_bounce = 'trampoline'
        spacer = setup_marathon_job.ID_TKN
        full_id = fake_name + spacer + fake_instance + spacer + self.fake_marathon_job_config['iteration']
        with contextlib.nested(
            mock.patch('setup_marathon_job.get_docker_url', return_value=fake_url),
            mock.patch('setup_marathon_job.create_complete_config', return_value=fake_complete),
            mock.patch('setup_marathon_job.deploy_service', return_value=False),
            mock.patch('setup_marathon_job.get_bounce_method', return_value=fake_bounce)
        ) as (
            url_patch,
            config_patch,
            deploy_patch,
            bounce_patch,
        ):
            assert not setup_marathon_job.setup_service(fake_name, fake_instance, fake_client,
                                             self.fake_marathon_config, self.fake_marathon_job_config)
            url_patch.assert_called_once_with(self.fake_docker_registry, self.fake_docker_image)
            config_patch.assert_called_once_with(full_id, fake_url,
                                                 self.fake_marathon_config['docker_options'],
                                                 self.fake_marathon_config['executor'],
                                                 self.fake_marathon_job_config)
            fake_client.get_app.assert_called_once_with(full_id)
            bounce_patch.assert_called_once_with(self.fake_marathon_job_config)
            deploy_patch.assert_called_once_with(full_id, fake_complete, fake_client,
                                                 bounce_method=fake_bounce)

    def test_deploy_service_unknown_bounce(self):
        fake_bounce = 'WHEEEEEEEEEEEEEEEE'
        fake_name = 'whoa'
        fake_instance = 'the_earth_is_tiny'
        fake_id = fake_name + setup_marathon_job.ID_TKN + fake_instance
        fake_apps = [mock.Mock(id=fake_id), mock.Mock(id=fake_id + '2')]
        fake_client = mock.MagicMock(list_apps=mock.Mock(return_value=fake_apps))
        assert not setup_marathon_job.deploy_service(fake_id, self.fake_marathon_job_config,
                                                     fake_client, fake_bounce)
        fake_client.list_apps.assert_called_once()
        fake_client.create_app.assert_not_called()

    def test_deploy_service_known_bounce_brutal(self):
        fake_bounce = 'brutal'
        fake_name = 'how_many_strings'
        fake_instance = 'will_i_need_to_think_of'
        fake_id = fake_name + setup_marathon_job.ID_TKN + fake_instance
        fake_apps = [mock.Mock(id=fake_id), mock.Mock(id=fake_id + '2')]
        fake_client = mock.MagicMock(list_apps=mock.Mock(return_value=fake_apps))
        with mock.patch('marathon_tools.brutal_bounce', return_value=True) as brutal_patch:
            assert setup_marathon_job.deploy_service(fake_id, self.fake_marathon_job_config,
                                                     fake_client, fake_bounce)
            fake_client.list_apps.assert_called_once()
            fake_client.create_app.assert_not_called()
            brutal_patch.assert_called_once_with([fake_id, fake_id+'2'],
                                                  self.fake_marathon_job_config,
                                                  fake_client)

    def test_deploy_service_no_old_ids(self):
        fake_bounce = 'trogdor'
        fake_name = '20x6'
        fake_instance = 'hsr_forever'
        fake_id = fake_name + setup_marathon_job.ID_TKN + fake_instance
        fake_apps = [mock.Mock(id='fake_id'), mock.Mock(id='ahahahahaahahahaha')]
        fake_client = mock.MagicMock(list_apps=mock.Mock(return_value=fake_apps))
        assert setup_marathon_job.deploy_service(fake_id, self.fake_marathon_job_config,
                                                 fake_client, fake_bounce)
        fake_client.list_apps.assert_called_once()
        fake_client.create_app.assert_called_once_with(**self.fake_marathon_job_config)

    def test_create_complete_config(self):
        fake_id = 'can_you_dig_it' + setup_marathon_job.ID_TKN + 'yes_i_can'
        fake_url = 'docker:///dockervania_from_konami'
        fake_options = self.fake_marathon_config['docker_options']
        executor = 'exeggcute'
        fake_ports = [1111, 2222]
        fake_mem = 1000000000000000000000
        fake_cpus = -1
        fake_instances = 101
        expected_conf = { 'id': fake_id,
                          'cmd': '/bin/true',
                          'container': {'image': fake_url, 'options': fake_options},
                          'executor': executor,
                          'ports': fake_ports,
                          'mem': fake_mem,
                          'cpus': fake_cpus,
                          'instances': fake_instances}
        with contextlib.nested(
            mock.patch('setup_marathon_job.get_ports', return_value=fake_ports),
            mock.patch('setup_marathon_job.get_mem', return_value=fake_mem),
            mock.patch('setup_marathon_job.get_cpus', return_value=fake_cpus),
            mock.patch('setup_marathon_job.get_constraints', return_value=[]),
            mock.patch('setup_marathon_job.get_instances', return_value=fake_instances),
        ) as (
            port_patch,
            mem_patch,
            cpu_patch,
            constraint_patch,
            instances_patch,
        ):
            actual = setup_marathon_job.create_complete_config(fake_id, fake_url, fake_options,
                                                               executor, self.fake_marathon_job_config)
            assert actual == expected_conf
            port_patch.assert_called_once_with(self.fake_marathon_job_config)
            mem_patch.assert_called_once_with(self.fake_marathon_job_config)
            cpu_patch.assert_called_once_with(self.fake_marathon_job_config)
            constraint_patch.assert_called_once_with(self.fake_marathon_job_config)
            instances_patch.assert_called_once_with(self.fake_marathon_job_config)

    def test_get_marathon_client(self):
        fake_url = "docker:///nothing_for_me_to_do_but_dance"
        fake_user = "the_boogie"
        fake_passwd = "is_for_real"
        with mock.patch('setup_marathon_job.MarathonClient') as client_patch:
            setup_marathon_job.get_marathon_client(fake_url, fake_user, fake_passwd)
            client_patch.assert_called_once_with(fake_url, fake_user, fake_passwd)

    def test_get_bounce_method_in_config(self):
        fake_method = 'aaargh'
        fake_conf = {'bounce_method' : fake_method}
        assert setup_marathon_job.get_bounce_method(fake_conf) == fake_method

    def test_get_bounce_method_default(self):
        assert setup_marathon_job.get_bounce_method({}) == 'brutal'

    def test_get_instances_in_config(self):
        fake_conf = {'instances' : -10}
        assert setup_marathon_job.get_instances(fake_conf) == -10

    def test_get_instances_default(self):
        assert setup_marathon_job.get_instances({}) == 1

    def test_get_constraints_in_config(self):
        fake_conf = {'constraints': 'so_many_walls'}
        assert setup_marathon_job.get_constraints(fake_conf) == 'so_many_walls'

    def test_get_constraints_default(self):
        assert setup_marathon_job.get_constraints({}) is None

    def test_get_cpus_in_config(self):
        fake_conf = {'cpus' : -5}
        assert setup_marathon_job.get_cpus(fake_conf) == -5

    def test_get_cpus_default(self):
        assert setup_marathon_job.get_cpus({}) == 1

    def test_get_mem_in_config(self):
        fake_conf = {'mem' : -999}
        assert setup_marathon_job.get_mem(fake_conf) == -999

    def test_get_mem_default(self):
        assert setup_marathon_job.get_mem({}) == 100

    def test_get_ports_in_config(self):
        fake_conf = {'num_ports' : 10 }
        assert setup_marathon_job.get_ports(fake_conf) == [0 for i in range(10)]

    def test_get_ports_default(self):
        assert setup_marathon_job.get_ports({}) == [0]

    def test_get_docker_url(self):
        fake_registry = "im.a-real.vm"
        fake_image = "and-i-can-run:1.0"
        expected = "docker:///" + fake_registry + "/" + fake_image
        assert setup_marathon_job.get_docker_url(fake_registry, fake_image) == expected

    def test_get_marathon_config(self):
        fake_conf = {'oh_no': 'im_a_ghost'}
        with mock.patch('marathon_tools.get_config', return_value=fake_conf) as get_patch:
            assert setup_marathon_job.get_main_marathon_config() == fake_conf
            get_patch.assert_called_once()
