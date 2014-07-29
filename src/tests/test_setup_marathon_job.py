#!/usr/bin/env python

import setup_marathon_job
from service_deployment_tools import marathon_tools
import mock
import contextlib
import pycurl


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
    }
    fake_args = mock.MagicMock(
        service_instance='what_is_love.bby_dont_hurt_me',
        soa_dir='no_more',
        verbose=False,
    )

    def test_main_success(self):
        fake_client = mock.MagicMock()
        with contextlib.nested(
            mock.patch('setup_marathon_job.parse_args', return_value=self.fake_args),
            mock.patch('setup_marathon_job.get_main_marathon_config', return_value=self.fake_marathon_config),
            mock.patch('setup_marathon_job.get_marathon_client', return_value=fake_client),
            mock.patch('service_deployment_tools.marathon_tools.read_service_config',
                       return_value=self.fake_marathon_job_config),
            mock.patch('setup_marathon_job.setup_service', return_value=(0, 'it_is_finished')),
            mock.patch('setup_marathon_job.send_sensu_event'),
            mock.patch('sys.exit'),
        ) as (
            parse_args_patch,
            get_main_conf_patch,
            get_client_patch,
            read_service_conf_patch,
            setup_service_patch,
            sensu_patch,
            sys_exit_patch,
        ):
            setup_marathon_job.main()
            parse_args_patch.assert_called_once_with()
            get_main_conf_patch.assert_called_once_with()
            get_client_patch.assert_called_once_with(
                    self.fake_marathon_config['url'],
                    self.fake_marathon_config['user'],
                    self.fake_marathon_config['pass'])
            read_service_conf_patch.assert_called_once_with(
                    self.fake_args.service_instance.split('.')[0],
                    self.fake_args.service_instance.split('.')[1],
                    self.fake_marathon_config['cluster'],
                    self.fake_args.soa_dir)
            setup_service_patch.assert_called_once_with(
                    self.fake_args.service_instance.split('.')[0],
                    self.fake_args.service_instance.split('.')[1],
                    fake_client,
                    self.fake_marathon_config,
                    self.fake_marathon_job_config)
            sys_exit_patch.assert_called_once_with(0)

    def test_main_failure(self):
        fake_client = mock.MagicMock()
        with contextlib.nested(
            mock.patch('setup_marathon_job.parse_args', return_value=self.fake_args),
            mock.patch('setup_marathon_job.get_main_marathon_config', return_value=self.fake_marathon_config),
            mock.patch('setup_marathon_job.get_marathon_client', return_value=fake_client),
            mock.patch('service_deployment_tools.marathon_tools.read_service_config',
                       return_value=self.fake_marathon_job_config),
            mock.patch('setup_marathon_job.setup_service', return_value=(1, 'NEVER')),
            mock.patch('setup_marathon_job.send_sensu_event'),
            mock.patch('sys.exit'),
        ) as (
            parse_args_patch,
            get_main_conf_patch,
            get_client_patch,
            read_service_conf_patch,
            setup_service_patch,
            sensu_patch,
            sys_exit_patch,
        ):
            setup_marathon_job.main()
            parse_args_patch.assert_called_once_with()
            get_main_conf_patch.assert_called_once_with()
            get_client_patch.assert_called_once_with(
                    self.fake_marathon_config['url'],
                    self.fake_marathon_config['user'],
                    self.fake_marathon_config['pass'])
            read_service_conf_patch.assert_called_once_with(
                    self.fake_args.service_instance.split('.')[0],
                    self.fake_args.service_instance.split('.')[1],
                    self.fake_marathon_config['cluster'],
                    self.fake_args.soa_dir)
            setup_service_patch.assert_called_once_with(
                    self.fake_args.service_instance.split('.')[0],
                    self.fake_args.service_instance.split('.')[1],
                    fake_client,
                    self.fake_marathon_config,
                    self.fake_marathon_job_config)
            sys_exit_patch.assert_called_once_with(1)

    def test_send_sensu_event(self):
        name = 'internal_naming'
        instance = 'whats_taters'
        soa_dir = 'potatoes'
        status = 7654
        output = "boil em, mash em, stick em in a stew",
        fake_monitor_conf = {
            'team': 'zero',
            'runbook': 'y/koobnur',
            'notification_email': '44@yelp.com'
        }
        expected_name = 'setup_marathon_job.%s.%s' % (name, instance)
        with contextlib.nested(
            mock.patch('os.path.abspath', return_value='black_sheep'),
            mock.patch('os.path.join', return_value='actually_albino'),
            mock.patch('service_configuration_lib.read_monitoring', return_value=fake_monitor_conf),
            mock.patch('pysensu_yelp.send_event')
        ) as (
            abs_path_patch,
            join_path_patch,
            read_monitoring_patch,
            send_event_patch
        ):
            setup_marathon_job.send_sensu_event(name, instance, soa_dir, status, output)
            abs_path_patch.assert_called_once_with(soa_dir)
            join_path_patch.assert_called_once_with('black_sheep', name, 'monitoring.yaml')
            read_monitoring_patch.assert_called_once_with('actually_albino')
            send_event_patch.assert_called_once_with(expected_name, 'y/koobnur', status, output, 'zero',
                                                     notification_email='44@yelp.com')

    def test_setup_service_srv_already_exists(self):
        fake_name = 'if_trees_could_talk'
        fake_instance = 'would_they_scream'
        fake_client = mock.MagicMock(get_app=mock.Mock(return_value=True))
        fake_complete = {'seven': 'full', 'eight': 'frightened', 'nine': 'eaten'}
        fake_url = 'docker:///what_is_a_test'
        full_id = marathon_tools.compose_job_id(fake_name, fake_instance,
                                                self.fake_marathon_job_config['iteration'])
        with contextlib.nested(
            mock.patch('setup_marathon_job.get_docker_url', return_value=fake_url),
            mock.patch('setup_marathon_job.create_complete_config', return_value=fake_complete),
            mock.patch('service_deployment_tools.marathon_tools.compose_job_id', return_value=full_id),
        ) as (
            docker_url_patch,
            create_config_patch,
            compose_id_patch,
        ):
            assert setup_marathon_job.setup_service(fake_name, fake_instance, fake_client,
                                                    self.fake_marathon_config,
                                                    self.fake_marathon_job_config)
            docker_url_patch.assert_called_once_with(self.fake_docker_registry, self.fake_docker_image)
            compose_id_patch.assert_called_once_with(fake_name, fake_instance,
                                                     self.fake_marathon_job_config['iteration'])
            create_config_patch.assert_called_once_with(full_id, fake_url,
                                                        self.fake_marathon_config['docker_options'],
                                                        self.fake_marathon_job_config)
            fake_client.get_app.assert_called_once_with(full_id)

    def test_setup_service_srv_does_not_exist(self):
        fake_name = 'if_talk_was_cheap'
        fake_instance = 'psychatrists_would_be_broke'
        fake_client = mock.MagicMock(get_app=mock.Mock(side_effect=KeyError))
        fake_complete = {'do': 'you', 'even': 'dota'}
        fake_url = 'docker:///a_miserable_pile_of_mocks'
        fake_bounce = 'trampoline'
        full_id = marathon_tools.compose_job_id(fake_name, fake_instance,
                                                self.fake_marathon_job_config['iteration'])
        with contextlib.nested(
            mock.patch('setup_marathon_job.get_docker_url', return_value=fake_url),
            mock.patch('setup_marathon_job.create_complete_config', return_value=fake_complete),
            mock.patch('setup_marathon_job.deploy_service', return_value=False),
            mock.patch('setup_marathon_job.get_bounce_method', return_value=fake_bounce),
            mock.patch('service_deployment_tools.marathon_tools.compose_job_id', return_value=full_id),
        ) as (
            docker_url_patch,
            create_config_patch,
            deploy_service_patch,
            get_bounce_patch,
            compose_id_patch,
        ):
            assert not setup_marathon_job.setup_service(fake_name, fake_instance, fake_client,
                                                        self.fake_marathon_config,
                                                        self.fake_marathon_job_config)
            docker_url_patch.assert_called_once_with(self.fake_docker_registry, self.fake_docker_image)
            compose_id_patch.assert_called_once_with(fake_name, fake_instance,
                                                     self.fake_marathon_job_config['iteration'])
            create_config_patch.assert_called_once_with(full_id, fake_url,
                                                        self.fake_marathon_config['docker_options'],
                                                        self.fake_marathon_job_config)
            fake_client.get_app.assert_called_once_with(full_id)
            get_bounce_patch.assert_called_once_with(self.fake_marathon_job_config)
            deploy_service_patch.assert_called_once_with(full_id, fake_complete, fake_client,
                                                         bounce_method=fake_bounce)

    def test_deploy_service_unknown_bounce(self):
        fake_bounce = 'WHEEEEEEEEEEEEEEEE'
        fake_name = 'whoa'
        fake_instance = 'the_earth_is_tiny'
        fake_id = marathon_tools.compose_job_id(fake_name, fake_instance)
        fake_apps = [mock.Mock(id=fake_id), mock.Mock(id=('%s2' % fake_id))]
        fake_client = mock.MagicMock(list_apps=mock.Mock(return_value=fake_apps))
        expected = (1, 'bounce_method not recognized: %s' % fake_bounce)
        actual = setup_marathon_job.deploy_service(fake_id, self.fake_marathon_job_config,
                                                   fake_client, fake_bounce)
        assert expected == actual
        fake_client.list_apps.assert_called_once_with()
        assert fake_client.create_app.call_count == 0

    def test_deploy_service_known_bounce_brutal(self):
        fake_bounce = 'brutal'
        fake_name = 'how_many_strings'
        fake_instance = 'will_i_need_to_think_of'
        fake_id = marathon_tools.compose_job_id(fake_name, fake_instance)
        fake_apps = [mock.Mock(id=fake_id), mock.Mock(id=('%s2' % fake_id))]
        fake_client = mock.MagicMock(list_apps=mock.Mock(return_value=fake_apps))
        with mock.patch('service_deployment_tools.bounce_lib.brutal_bounce', return_value=True) as brutal_bounce_patch:
            assert setup_marathon_job.deploy_service(fake_id, self.fake_marathon_job_config,
                                                     fake_client, fake_bounce)
            fake_client.list_apps.assert_called_once_with()
            assert fake_client.create_app.call_count == 0
            brutal_bounce_patch.assert_called_once_with([fake_id, '%s2' % fake_id],
                                                        self.fake_marathon_job_config,
                                                        fake_client)

    def test_deploy_service_no_old_ids(self):
        fake_bounce = 'trogdor'
        fake_name = '20x6'
        fake_instance = 'hsr_forever'
        fake_id = marathon_tools.compose_job_id(fake_name, fake_instance)
        fake_apps = [mock.Mock(id='fake_id'), mock.Mock(id='ahahahahaahahahaha')]
        fake_client = mock.MagicMock(list_apps=mock.Mock(return_value=fake_apps))
        expected = (0, 'Service deployed.')
        actual = setup_marathon_job.deploy_service(fake_id, self.fake_marathon_job_config,
                                                   fake_client, fake_bounce)
        assert expected == actual
        fake_client.list_apps.assert_called_once_with()
        fake_client.create_app.assert_called_once_with(**self.fake_marathon_job_config)

    def test_create_complete_config(self):
        fake_id = marathon_tools.compose_job_id('can_you_dig_it', 'yes_i_can')
        fake_url = 'docker:///dockervania_from_konami'
        fake_options = self.fake_marathon_config['docker_options']
        fake_ports = [1111, 2222]
        fake_mem = 1000000000000000000000
        fake_cpus = -1
        fake_instances = 101
        expected_conf = {'id': fake_id,
                         'container': {'image': fake_url, 'options': fake_options},
                         'constraints': [],
                         'uris': [],
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
            get_port_patch,
            get_mem_patch,
            get_cpus_patch,
            get_constraints_patch,
            get_instances_patch,
        ):
            actual = setup_marathon_job.create_complete_config(fake_id, fake_url, fake_options,
                                                               self.fake_marathon_job_config)
            assert actual == expected_conf
            get_port_patch.assert_called_once_with(self.fake_marathon_job_config)
            get_mem_patch.assert_called_once_with(self.fake_marathon_job_config)
            get_cpus_patch.assert_called_once_with(self.fake_marathon_job_config)
            get_constraints_patch.assert_called_once_with(self.fake_marathon_job_config)
            get_instances_patch.assert_called_once_with(self.fake_marathon_job_config)

    def test_get_marathon_client(self):
        fake_url = "docker:///nothing_for_me_to_do_but_dance"
        fake_user = "the_boogie"
        fake_passwd = "is_for_real"
        with mock.patch('setup_marathon_job.MarathonClient') as client_patch:
            setup_marathon_job.get_marathon_client(fake_url, fake_user, fake_passwd)
            client_patch.assert_called_once_with(fake_url, fake_user, fake_passwd)

    def test_get_bounce_method_in_config(self):
        fake_method = 'aaargh'
        fake_conf = {'bounce_method': fake_method}
        assert setup_marathon_job.get_bounce_method(fake_conf) == fake_method

    def test_get_bounce_method_default(self):
        assert setup_marathon_job.get_bounce_method({}) == 'brutal'

    def test_get_instances_in_config(self):
        fake_conf = {'instances': -10}
        assert setup_marathon_job.get_instances(fake_conf) == -10

    def test_get_instances_default(self):
        assert setup_marathon_job.get_instances({}) == 1

    def test_get_constraints_in_config(self):
        fake_conf = {'constraints': 'so_many_walls'}
        assert setup_marathon_job.get_constraints(fake_conf) == 'so_many_walls'

    def test_get_constraints_default(self):
        assert setup_marathon_job.get_constraints({}) is None

    def test_get_cpus_in_config(self):
        fake_conf = {'cpus': -5}
        assert setup_marathon_job.get_cpus(fake_conf) == -5

    def test_get_cpus_default(self):
        assert setup_marathon_job.get_cpus({}) == 1

    def test_get_mem_in_config(self):
        fake_conf = {'mem': -999}
        assert setup_marathon_job.get_mem(fake_conf) == -999

    def test_get_mem_default(self):
        assert setup_marathon_job.get_mem({}) == 100

    def test_get_ports_in_config(self):
        fake_conf = {'num_ports': 10}
        assert setup_marathon_job.get_ports(fake_conf) == [0 for i in range(10)]

    def test_get_ports_default(self):
        assert setup_marathon_job.get_ports({}) == [0]

    def test_get_docker_url_no_error(self):
        fake_registry = "im.a-real.vm"
        fake_image = "and-i-can-run:1.0"
        fake_curl = mock.Mock()
        fake_stringio = mock.Mock(getvalue=mock.Mock(return_value='483af83b81ee93ac930d'))
        expected = "docker:///%s/%s" % (fake_registry, fake_image)
        with contextlib.nested(
            mock.patch('pycurl.Curl', return_value=fake_curl),
            mock.patch('setup_marathon_job.StringIO', return_value=fake_stringio)
        ) as (
            pycurl_patch,
            stringio_patch
        ):
            assert setup_marathon_job.get_docker_url(fake_registry, fake_image) == expected
            fake_curl.setopt.assert_any_call(pycurl.URL,
                                             'http://%s/v1/repositories/%s/tags/%s' % (
                                                    fake_registry,
                                                    fake_image.split(':')[0],
                                                    fake_image.split(':')[1]))
            fake_curl.setopt.assert_any_call(pycurl.WRITEFUNCTION, fake_stringio.write)
            assert fake_curl.setopt.call_count == 2
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
            mock.patch('setup_marathon_job.StringIO', return_value=fake_stringio)
        ) as (
            pycurl_patch,
            stringio_patch
        ):
            assert setup_marathon_job.get_docker_url(fake_registry, fake_image) == expected
            fake_curl.setopt.assert_any_call(pycurl.URL,
                                             'http://%s/v1/repositories/%s/tags/%s' % (
                                                    fake_registry,
                                                    fake_image.split(':')[0],
                                                    fake_image.split(':')[1]))
            fake_curl.setopt.assert_any_call(pycurl.WRITEFUNCTION, fake_stringio.write)
            assert fake_curl.setopt.call_count == 2
            fake_curl.perform.assert_called_once_with()
            fake_stringio.getvalue.assert_called_once_with()

    def test_get_marathon_config(self):
        fake_conf = {'oh_no': 'im_a_ghost'}
        with mock.patch('service_deployment_tools.marathon_tools.get_config', return_value=fake_conf) as get_conf_patch:
            assert setup_marathon_job.get_main_marathon_config() == fake_conf
            get_conf_patch.assert_called_once_with()
