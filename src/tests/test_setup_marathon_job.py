#!/usr/bin/env python

import setup_marathon_job
import marathon
from service_deployment_tools import marathon_tools
import mock
import contextlib


class TestSetupMarathonJob:

    fake_docker_image = 'test_docker:1.0'
    fake_marathon_job_config = {
        'instances': 3,
        'cpus': 1,
        'mem': 100,
        'docker_image': fake_docker_image,
        'iteration': 'testin',
        'nerve_ns': 'aaaaugh',
        'bounce_method': 'brutal'
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
            mock.patch('setup_marathon_job.send_event'),
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
            mock.patch('setup_marathon_job.send_event'),
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

    def test_send_event(self):
        fake_service_name = 'fake_service'
        fake_instance_name = 'fake_instance'
        fake_status = '42'
        fake_output = 'The http port is not open'
        fake_team = 'fake_team'
        fake_tip = 'fake_tip'
        fake_notification_email = 'fake@notify'
        fake_irc = '#fake'
        fake_soa_dir = '/fake/soa/dir'
        fake_cluster = 'fake_cluster'
        expected_runbook = 'y/rb-marathon'
        expected_check_name = 'setup_marathon_job.%s.%s' % (fake_service_name, fake_instance_name)
        expected_kwargs = {
            'tip': fake_tip,
            'notification_email': fake_notification_email,
            'irc_channels': fake_irc,
            'alert_after': '6m',
            'check_every': '2m',
            'realert_every': -1,
            'source': 'mesos-fake_cluster'
        }
        with contextlib.nested(
            mock.patch("service_deployment_tools.monitoring_tools.get_team",
                       return_value=fake_team),
            mock.patch("service_deployment_tools.monitoring_tools.get_tip",
                       return_value=fake_tip),
            mock.patch("service_deployment_tools.monitoring_tools.get_notification_email",
                       return_value=fake_notification_email),
            mock.patch("service_deployment_tools.monitoring_tools.get_irc_channels",
                       return_value=fake_irc),
            mock.patch("pysensu_yelp.send_event"),
            mock.patch('service_deployment_tools.marathon_tools.get_cluster',
                       return_value=fake_cluster)
        ) as (
            monitoring_tools_get_team_patch,
            monitoring_tools_get_tip_patch,
            monitoring_tools_get_notification_email_patch,
            monitoring_tools_get_irc_patch,
            pysensu_yelp_send_event_patch,
            cluster_patch,
        ):
            setup_marathon_job.send_event(fake_service_name,
                                          fake_instance_name,
                                          fake_soa_dir,
                                          fake_status,
                                          fake_output)
            monitoring_tools_get_team_patch.assert_called_once_with('marathon', fake_service_name,
                                                                    fake_instance_name, fake_soa_dir)
            monitoring_tools_get_tip_patch.assert_called_once_with('marathon', fake_service_name,
                                                                   fake_instance_name, fake_soa_dir)
            monitoring_tools_get_notification_email_patch.assert_called_once_with('marathon', fake_service_name,
                                                                                  fake_instance_name, fake_soa_dir)
            monitoring_tools_get_irc_patch.assert_called_once_with('marathon', fake_service_name,
                                                                   fake_instance_name, fake_soa_dir)
            pysensu_yelp_send_event_patch.assert_called_once_with(expected_check_name, expected_runbook, fake_status,
                                                                  fake_output, fake_team, **expected_kwargs)
            cluster_patch.assert_called_once_with()

    def test_setup_service_srv_already_exists(self):
        fake_name = 'if_trees_could_talk'
        fake_instance = 'would_they_scream'
        fake_client = mock.MagicMock(get_app=mock.Mock(return_value=True))
        fake_complete = {'seven': 'full', 'eight': 'frightened', 'nine': 'eaten'}
        fake_url = 'docker:///what_is_a_test'
        fake_hash = '4d5e6f'
        full_id = marathon_tools.compose_job_id(fake_name, fake_instance,
                                                self.fake_marathon_job_config['iteration'])
        with contextlib.nested(
            mock.patch('service_deployment_tools.marathon_tools.get_docker_url', return_value=fake_url),
            mock.patch('service_deployment_tools.marathon_tools.create_complete_config',
                       return_value=fake_complete),
            mock.patch('service_deployment_tools.marathon_tools.get_config_hash',
                       return_value=fake_hash),
            mock.patch('service_deployment_tools.marathon_tools.compose_job_id', return_value=full_id),
        ) as (
            docker_url_patch,
            create_config_patch,
            hash_patch,
            compose_id_patch,
        ):
            status, output = setup_marathon_job.setup_service(fake_name, fake_instance, fake_client,
                                                              self.fake_marathon_config,
                                                              self.fake_marathon_job_config)
            docker_url_patch.assert_called_once_with(self.fake_docker_registry, self.fake_docker_image)
            compose_id_patch.assert_any_call(fake_name, fake_instance, fake_hash)
            compose_id_patch.assert_any_call(fake_name, fake_instance)
            assert compose_id_patch.call_count == 2
            create_config_patch.assert_called_once_with(full_id, fake_url,
                                                        self.fake_marathon_config['docker_options'],
                                                        self.fake_marathon_job_config)
            fake_client.get_app.assert_called_once_with(full_id)
            hash_patch.assert_called_once_with(fake_complete)

    def test_setup_service_srv_does_not_exist(self):
        fake_name = 'if_talk_was_cheap'
        fake_instance = 'psychatrists_would_be_broke'
        fake_response = mock.Mock(json=mock.Mock(return_value={'message': 'test'}))
        fake_client = mock.MagicMock(get_app=mock.Mock(
                        side_effect=marathon.exceptions.NotFoundError(fake_response)))
        fake_complete = {'do': 'you', 'even': 'dota'}
        fake_url = 'docker:///a_miserable_pile_of_mocks'
        fake_bounce = 'trampoline'
        fake_hash = '1a2b3c'
        full_id = marathon_tools.compose_job_id(fake_name, fake_instance,
                                                self.fake_marathon_job_config['iteration'])
        with contextlib.nested(
            mock.patch('service_deployment_tools.marathon_tools.get_docker_url', return_value=fake_url),
            mock.patch('service_deployment_tools.marathon_tools.create_complete_config',
                       return_value=fake_complete),
            mock.patch('service_deployment_tools.marathon_tools.get_config_hash',
                       return_value=fake_hash),
            mock.patch('setup_marathon_job.deploy_service', return_value=(111, 'Never')),
            mock.patch('service_deployment_tools.marathon_tools.get_bounce_method', return_value=fake_bounce),
            mock.patch('service_deployment_tools.marathon_tools.compose_job_id', return_value=full_id),
        ) as (
            docker_url_patch,
            create_config_patch,
            hash_patch,
            deploy_service_patch,
            get_bounce_patch,
            compose_id_patch,
        ):
            status, output = setup_marathon_job.setup_service(fake_name, fake_instance, fake_client,
                                                              self.fake_marathon_config,
                                                              self.fake_marathon_job_config)
            assert status == 111
            assert output == 'Never'
            docker_url_patch.assert_called_once_with(self.fake_docker_registry, self.fake_docker_image)
            compose_id_patch.assert_any_call(fake_name, fake_instance)
            compose_id_patch.assert_any_call(fake_name, fake_instance, fake_hash)
            assert compose_id_patch.call_count == 2
            create_config_patch.assert_called_once_with(full_id, fake_url,
                                                        self.fake_marathon_config['docker_options'],
                                                        self.fake_marathon_job_config)
            fake_client.get_app.assert_called_once_with(full_id)
            get_bounce_patch.assert_called_once_with(self.fake_marathon_job_config)
            deploy_service_patch.assert_called_once_with(full_id, fake_complete, fake_client,
                                                         self.fake_marathon_job_config['nerve_ns'],
                                                         fake_bounce)
            hash_patch.assert_called_once_with(fake_complete)

    def test_deploy_service_unknown_bounce(self):
        fake_bounce = 'WHEEEEEEEEEEEEEEEE'
        fake_name = 'whoa'
        fake_instance = 'the_earth_is_tiny'
        fake_namespace = 'not_really'
        fake_id = marathon_tools.compose_job_id(fake_name, fake_instance)
        fake_apps = [mock.Mock(id=fake_id), mock.Mock(id=('%s2' % fake_id))]
        fake_client = mock.MagicMock(list_apps=mock.Mock(return_value=fake_apps))
        expected = (1, 'bounce_method not recognized: %s' % fake_bounce)
        actual = setup_marathon_job.deploy_service(fake_id, self.fake_marathon_job_config,
                                                   fake_client, fake_namespace, fake_bounce)
        assert expected == actual
        fake_client.list_apps.assert_called_once_with()
        assert fake_client.create_app.call_count == 0

    def test_deploy_service_known_bounce_brutal(self):
        fake_bounce = 'brutal'
        fake_name = 'how_many_strings'
        fake_instance = 'will_i_need_to_think_of'
        fake_namespace = 'over_9000'
        fake_id = marathon_tools.compose_job_id(fake_name, fake_instance)
        fake_apps = [mock.Mock(id=fake_id), mock.Mock(id=('%s2' % fake_id))]
        fake_client = mock.MagicMock(list_apps=mock.Mock(return_value=fake_apps))
        with mock.patch('service_deployment_tools.bounce_lib.brutal_bounce',
                        return_value=True) as brutal_bounce_patch:
            assert setup_marathon_job.deploy_service(fake_id, self.fake_marathon_job_config,
                                                     fake_client, fake_namespace, fake_bounce)
            fake_client.list_apps.assert_called_once_with()
            assert fake_client.create_app.call_count == 0
            brutal_bounce_patch.assert_called_once_with([fake_id, '%s2' % fake_id],
                                                        self.fake_marathon_job_config,
                                                        fake_client, fake_namespace)

    def test_deploy_service_known_bounce_crossover(self):
        fake_bounce = 'crossover'
        fake_name = 'friday_night_tox'
        fake_instance = 'a_new_bloodsport'
        fake_namespace = 'featuring_snakes'
        fake_id = marathon_tools.compose_job_id(fake_name, fake_instance)
        fake_apps = [mock.Mock(id=fake_id), mock.Mock(id=('%s2' % fake_id))]
        fake_client = mock.MagicMock(list_apps=mock.Mock(return_value=fake_apps))
        with mock.patch('service_deployment_tools.bounce_lib.crossover_bounce',
                        return_value=True) as crossover_bounce_patch:
            assert setup_marathon_job.deploy_service(fake_id, self.fake_marathon_job_config,
                                                     fake_client, fake_namespace, fake_bounce)
            fake_client.list_apps.assert_called_once_with()
            assert fake_client.create_app.call_count == 0
            crossover_bounce_patch.assert_called_once_with([fake_id, '%s2' % fake_id],
                                                           self.fake_marathon_job_config,
                                                           fake_client, fake_namespace)

    def test_deploy_service_no_old_ids(self):
        fake_bounce = 'trogdor'
        fake_name = '20x6'
        fake_instance = 'hsr_forever'
        fake_namespace = 'its_comin_back'
        fake_id = marathon_tools.compose_job_id(fake_name, fake_instance)
        fake_apps = [mock.Mock(id='fake_id'), mock.Mock(id='ahahahahaahahahaha')]
        fake_client = mock.MagicMock(list_apps=mock.Mock(return_value=fake_apps))
        expected = (0, 'Service deployed.')
        with mock.patch('service_deployment_tools.bounce_lib.create_app_lock',
                        spec=contextlib.contextmanager) as app_lock_patch:
            actual = setup_marathon_job.deploy_service(fake_id, self.fake_marathon_job_config,
                                                       fake_client, fake_namespace, fake_bounce)
            assert expected == actual
            fake_client.list_apps.assert_called_once_with()
            fake_client.create_app.assert_called_once_with(**self.fake_marathon_job_config)
            app_lock_patch.assert_called_once_with()

    def test_get_marathon_client(self):
        fake_url = "docker:///nothing_for_me_to_do_but_dance"
        fake_user = "the_boogie"
        fake_passwd = "is_for_real"
        with mock.patch('setup_marathon_job.MarathonClient') as client_patch:
            setup_marathon_job.get_marathon_client(fake_url, fake_user, fake_passwd)
            client_patch.assert_called_once_with(fake_url, fake_user, fake_passwd, timeout=20)

    def test_get_marathon_config(self):
        fake_conf = {'oh_no': 'im_a_ghost'}
        with mock.patch('service_deployment_tools.marathon_tools.get_config', return_value=fake_conf) as get_conf_patch:
            assert setup_marathon_job.get_main_marathon_config() == fake_conf
            get_conf_patch.assert_called_once_with()
