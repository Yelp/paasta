#!/usr/bin/env python

import contextlib
import mock

import marathon

from pytest import raises
from paasta_tools import marathon_tools, bounce_lib
from paasta_tools.bounce_lib import list_bounce_methods
import setup_marathon_job


class TestSetupMarathonJob:

    fake_docker_image = 'test_docker:1.0'
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        'servicename',
        'instancename',
        {
            'instances': 3,
            'cpus': 1,
            'mem': 100,
            'docker_image': fake_docker_image,
            'nerve_ns': 'aaaaugh',
            'bounce_method': 'brutal'
        },
        {},
    )
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
    fake_args = mock.MagicMock(
        service_instance='what_is_love.bby_dont_hurt_me',
        soa_dir='no_more',
        verbose=False,
    )

    def test_main_success(self):
        fake_client = mock.MagicMock()
        with contextlib.nested(
            mock.patch(
                'setup_marathon_job.parse_args',
                return_value=self.fake_args,
                autospec=True,
            ),
            mock.patch(
                'setup_marathon_job.get_main_marathon_config',
                return_value=self.fake_marathon_config,
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.marathon_tools.get_marathon_client',
                return_value=fake_client,
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.marathon_tools.load_marathon_service_config',
                return_value=self.fake_marathon_service_config,
                autospec=True,
            ),
            mock.patch(
                'setup_marathon_job.setup_service',
                return_value=(0, 'it_is_finished'),
                autospec=True,
            ),
            mock.patch(
                'setup_marathon_job.marathon_tools.get_cluster',
                return_value=self.fake_marathon_config['cluster'],
                autospec=True,
            ),
            mock.patch('setup_marathon_job.send_event', autospec=True,),
            mock.patch('sys.exit', autospec=True),
        ) as (
            parse_args_patch,
            get_main_conf_patch,
            get_client_patch,
            read_service_conf_patch,
            setup_service_patch,
            get_cluster_patch,
            sensu_patch,
            sys_exit_patch,
        ):
            setup_marathon_job.main()
            parse_args_patch.assert_called_once_with()
            get_main_conf_patch.assert_called_once_with()
            get_client_patch.assert_called_once_with(
                self.fake_marathon_config['url'],
                self.fake_marathon_config['user'],
                self.fake_marathon_config['pass'],
            )
            read_service_conf_patch.assert_called_once_with(
                self.fake_args.service_instance.split('.')[0],
                self.fake_args.service_instance.split('.')[1],
                self.fake_marathon_config['cluster'],
                soa_dir=self.fake_args.soa_dir,
            )
            setup_service_patch.assert_called_once_with(
                self.fake_args.service_instance.split('.')[0],
                self.fake_args.service_instance.split('.')[1],
                fake_client,
                self.fake_marathon_config,
                self.fake_marathon_service_config,
            )
            sys_exit_patch.assert_called_once_with(0)

    def test_main_failure(self):
        fake_client = mock.MagicMock()
        with contextlib.nested(
            mock.patch(
                'setup_marathon_job.parse_args',
                return_value=self.fake_args,
                autospec=True,
            ),
            mock.patch(
                'setup_marathon_job.get_main_marathon_config',
                return_value=self.fake_marathon_config,
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.marathon_tools.get_marathon_client',
                return_value=fake_client,
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.marathon_tools.load_marathon_service_config',
                return_value=self.fake_marathon_service_config,
                autospec=True,
            ),
            mock.patch(
                'setup_marathon_job.setup_service',
                return_value=(1, 'NEVER'),
                autospec=True,
            ),
            mock.patch(
                'setup_marathon_job.marathon_tools.get_cluster',
                return_value=self.fake_marathon_config['cluster'],
                autospec=True,
            ),
            mock.patch('setup_marathon_job.send_event', autospec=True),
            mock.patch('sys.exit', autospec=True),
        ) as (
            parse_args_patch,
            get_main_conf_patch,
            get_client_patch,
            read_service_conf_patch,
            setup_service_patch,
            get_cluster_patch,
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
                soa_dir=self.fake_args.soa_dir)
            setup_service_patch.assert_called_once_with(
                self.fake_args.service_instance.split('.')[0],
                self.fake_args.service_instance.split('.')[1],
                fake_client,
                self.fake_marathon_config,
                self.fake_marathon_service_config)
            sys_exit_patch.assert_called_once_with(0)

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
        expected_runbook = 'http://y/paasta-troubleshooting'
        expected_check_name = 'setup_marathon_job.%s.%s' % (
            fake_service_name, fake_instance_name)
        expected_kwargs = {
            'tip': fake_tip,
            'notification_email': fake_notification_email,
            'irc_channels': fake_irc,
            'alert_after': '5m',
            'check_every': '1m',
            'realert_every': -1,
            'source': 'paasta-fake_cluster',
        }
        with contextlib.nested(
            mock.patch(
                "paasta_tools.monitoring_tools.get_team",
                return_value=fake_team,
                autospec=True,
            ),
            mock.patch(
                "paasta_tools.monitoring_tools.get_tip",
                return_value=fake_tip,
                autospec=True,
            ),
            mock.patch(
                "paasta_tools.monitoring_tools.get_notification_email",
                return_value=fake_notification_email,
                autospec=True,
            ),
            mock.patch(
                "paasta_tools.monitoring_tools.get_irc_channels",
                return_value=fake_irc,
                autospec=True,
            ),
            mock.patch("pysensu_yelp.send_event", autospec=True),
            mock.patch(
                'paasta_tools.marathon_tools.get_cluster',
                return_value=fake_cluster,
                autospec=True,
            )
        ) as (
            get_team_patch,
            get_tip_patch,
            get_notification_email_patch,
            get_irc_patch,
            pysensu_yelp_send_event_patch,
            cluster_patch,
        ):
            setup_marathon_job.send_event(fake_service_name,
                                          fake_instance_name,
                                          fake_soa_dir,
                                          fake_status,
                                          fake_output)
            get_team_patch.assert_called_once_with(
                'marathon',
                fake_service_name,
                fake_instance_name,
                fake_soa_dir,
            )
            get_tip_patch.assert_called_once_with(
                'marathon',
                fake_service_name,
                fake_instance_name,
                fake_soa_dir
            )
            get_notification_email_patch.assert_called_once_with(
                'marathon',
                fake_service_name,
                fake_instance_name,
                fake_soa_dir
            )
            get_irc_patch.assert_called_once_with(
                'marathon',
                fake_service_name,
                fake_instance_name,
                fake_soa_dir
            )
            pysensu_yelp_send_event_patch.assert_called_once_with(
                expected_check_name,
                expected_runbook,
                fake_status,
                fake_output,
                fake_team,
                **expected_kwargs
            )
            cluster_patch.assert_called_once_with()

    def test_do_bounce(self):
        fake_bounce_func_return = {
            'create_app': True,
            'tasks_to_kill': [mock.Mock(app_id='fake_task_to_kill_1')],
            'apps_to_kill': ['fake_app_to_kill'],
        }
        fake_bounce_func = mock.create_autospec(
            bounce_lib.brutal_bounce,
            return_value=fake_bounce_func_return,
        )
        fake_config = {'instances': 5}
        fake_new_app_running = False
        fake_happy_new_tasks = ['fake_one', 'fake_two', 'fake_three']
        fake_old_app_tasks = []
        fake_service_name = 'fake_service'
        fake_serviceinstance = 'fake_service.fake_instance'
        fake_cluster = 'fake_cluster'
        fake_instance_name = 'fake_instance'
        fake_bounce_method = 'fake_bounce_method'
        fake_marathon_jobid = 'fake_marathon_jobid'
        fake_client = mock.create_autospec(
            marathon.MarathonClient
        )
        expected_new_task_count = fake_config["instances"] - len(fake_happy_new_tasks)
        expected_kill_task_count = len(fake_bounce_func_return['apps_to_kill'])

        with contextlib.nested(
            mock.patch('setup_marathon_job._log', autospec=True),
            mock.patch('setup_marathon_job.bounce_lib.create_marathon_app', autospec=True),
        ) as (mock_log, mock_create_marathon_app):
            setup_marathon_job.do_bounce(fake_bounce_func, fake_config, fake_new_app_running,
                                         fake_happy_new_tasks, fake_old_app_tasks, fake_service_name,
                                         fake_bounce_method, fake_serviceinstance, fake_cluster, fake_instance_name,
                                         fake_marathon_jobid, fake_client)
            assert mock_log.call_count == 3
            first_logged_line = mock_log.mock_calls[0][2]["line"]
            assert '%s new tasks' % expected_new_task_count in first_logged_line
            assert '%s to kill' % expected_kill_task_count in first_logged_line

            assert mock_create_marathon_app.call_count == 1

            assert fake_client.kill_task.call_count == len(fake_bounce_func_return["tasks_to_kill"])

    def test_setup_service_srv_already_exists(self):
        fake_name = 'if_trees_could_talk'
        fake_instance = 'would_they_scream'
        fake_client = mock.MagicMock(get_app=mock.Mock(return_value=True))
        full_id = marathon_tools.compose_job_id(fake_name, fake_instance)
        fake_complete = {
            'seven': 'full',
            'eight': 'frightened',
            'nine': 'eaten',
            'id': full_id,
        }
        with contextlib.nested(
            mock.patch(
                'paasta_tools.marathon_tools.create_complete_config',
                return_value=fake_complete,
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.marathon_tools.load_marathon_config',
                return_value=self.fake_marathon_config,
                autospec=True,
            ),
            mock.patch(
                'setup_marathon_job.deploy_service',
                autospec=True,
            ),
        ) as (
            create_config_patch,
            get_config_patch,
            deploy_service_patch,
        ):
            setup_marathon_job.setup_service(
                fake_name,
                fake_instance,
                fake_client,
                self.fake_marathon_config,
                self.fake_marathon_service_config
            )
            create_config_patch.assert_called_once_with(
                fake_name,
                fake_instance,
                self.fake_marathon_config,
            )
            assert deploy_service_patch.call_count == 1

    def test_setup_service_srv_does_not_exist(self):
        fake_name = 'if_talk_was_cheap'
        fake_instance = 'psychatrists_would_be_broke'
        fake_response = mock.Mock(
            json=mock.Mock(return_value={'message': 'test'}))
        fake_client = mock.MagicMock(get_app=mock.Mock(
            side_effect=marathon.exceptions.NotFoundError(fake_response)))
        full_id = marathon_tools.compose_job_id(fake_name, fake_instance, 'oogabooga')
        fake_complete = {
            'do': 'you', 'even': 'dota', 'id': full_id,
            'docker_image': 'fake_docker_registry/fake_docker_image',
        }
        fake_bounce = 'trampoline'
        with contextlib.nested(
            mock.patch(
                'paasta_tools.marathon_tools.create_complete_config',
                return_value=fake_complete,
                autospec=True,
            ),
            mock.patch(
                'setup_marathon_job.deploy_service',
                return_value=(111, 'Never'),
                autospec=True,
            ),
            mock.patch.object(
                self.fake_marathon_service_config,
                'get_bounce_method',
                return_value=fake_bounce,
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.marathon_tools.load_marathon_service_config',
                return_value=self.fake_marathon_service_config,
                autospec=True,
            ),
        ) as (
            create_config_patch,
            deploy_service_patch,
            get_bounce_patch,
            read_service_conf_patch,
        ):
            status, output = setup_marathon_job.setup_service(
                fake_name,
                fake_instance,
                fake_client,
                self.fake_marathon_config,
                self.fake_marathon_service_config,
            )
            assert status == 111
            assert output == 'Never'

            create_config_patch.assert_called_once_with(
                fake_name,
                fake_instance,
                self.fake_marathon_config
            )
            get_bounce_patch.assert_called_once_with()
            deploy_service_patch.assert_called_once_with(
                fake_name,
                fake_instance,
                full_id,
                fake_complete,
                fake_client,
                fake_bounce,
                self.fake_marathon_service_config.get_nerve_namespace(),
                self.fake_marathon_service_config.get_bounce_health_params(),
            )

    def test_setup_service_srv_complete_config_raises(self):
        fake_name = 'test_service'
        fake_instance = 'test_instance'
        with mock.patch(
            'setup_marathon_job.marathon_tools.create_complete_config',
            side_effect=marathon_tools.NoDockerImageError,
        ):
            status, output = setup_marathon_job.setup_service(
                fake_name,
                fake_instance,
                None,
                None,
                None
            )
            assert status == 1
            expected = 'Docker image for test_service.test_instance not in'
            assert expected in output

    def test_deploy_service_unknown_bounce(self):
        fake_bounce = 'WHEEEEEEEEEEEEEEEE'
        fake_name = 'whoa'
        fake_instance = 'the_earth_is_tiny'
        fake_id = marathon_tools.compose_job_id(fake_name, fake_instance)
        fake_apps = [mock.Mock(id=fake_id, tasks=[]), mock.Mock(id=('%s2' % fake_id), tasks=[])]
        fake_client = mock.MagicMock(
            list_apps=mock.Mock(return_value=fake_apps))
        fake_config = {'id': fake_id, 'instances': 2}

        errormsg = 'ERROR: bounce_method not recognized: %s. Must be one of (%s)' % \
            (fake_bounce, ', '.join(list_bounce_methods()))
        expected = (1, errormsg)

        with contextlib.nested(
            mock.patch('setup_marathon_job._log', autospec=True),
            mock.patch(
                'paasta_tools.setup_marathon_job.marathon_tools.get_cluster',
                return_value='fake_cluster',
                autospec=True
            ),
        ) as (mock_log, mock_get_cluster):
            actual = setup_marathon_job.deploy_service(
                fake_name,
                fake_instance,
                fake_id,
                fake_config,
                fake_client,
                fake_bounce,
                nerve_ns=fake_instance,
                bounce_health_params={},
            )
            assert mock_log.call_count == 1
        assert expected == actual
        fake_client.list_apps.assert_called_once_with(embed_failures=True)
        assert fake_client.create_app.call_count == 0

    def test_deploy_service_known_bounce(self):
        fake_bounce = 'areallygoodbouncestrategy'
        fake_name = 'how_many_strings'
        fake_instance = 'will_i_need_to_think_of'
        fake_id = marathon_tools.compose_job_id(fake_name, fake_instance, tag='blah')
        fake_config = {'id': fake_id, 'instances': 2}

        old_app_id = ('%s2' % fake_id)
        old_task = mock.Mock(id="old_task_id", app_id=old_app_id)
        old_app = mock.Mock(id=old_app_id, tasks=[old_task])

        fake_client = mock.MagicMock(
            list_apps=mock.Mock(return_value=[old_app]),
            kill_task=mock.Mock(spec=lambda app_id, id, scale=False: None),
        )

        fake_bounce_func = mock.create_autospec(
            bounce_lib.brutal_bounce,
            return_value={
                "create_app": True,
                "tasks_to_kill": [old_task],
                "apps_to_kill": [old_app_id],
            }
        )

        with contextlib.nested(
            mock.patch(
                'paasta_tools.bounce_lib.get_bounce_method_func',
                return_value=fake_bounce_func,
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.bounce_lib.bounce_lock_zookeeper',
                autospec=True
            ),
            mock.patch(
                'paasta_tools.bounce_lib.get_happy_tasks',
                autospec=True,
                side_effect=lambda x, _, __, **kwargs: x,
            ),
            mock.patch('paasta_tools.bounce_lib.kill_old_ids', autospec=True),
            mock.patch('paasta_tools.bounce_lib.create_marathon_app', autospec=True),
            mock.patch('setup_marathon_job._log', autospec=True),
            mock.patch(
                'paasta_tools.setup_marathon_job.marathon_tools.get_cluster',
                return_value='fake_cluster',
                autospec=True
            ),
        ) as (_, _, _, kill_old_ids_patch, create_marathon_app_patch, mock_log, mock_get_cluster):
            result = setup_marathon_job.deploy_service(
                fake_name,
                fake_instance,
                fake_id,
                fake_config,
                fake_client,
                fake_bounce,
                nerve_ns=fake_instance,
                bounce_health_params={},
            )
            assert result[0] == 0, "Expected successful result; got (%d, %s)" % result
            fake_client.list_apps.assert_called_once_with(embed_failures=True)
            assert fake_client.create_app.call_count == 0
            fake_bounce_func.assert_called_once_with(
                new_config=fake_config,
                new_app_running=False,
                happy_new_tasks=[],
                old_app_tasks={old_app.id: set([old_task])},
            )

            fake_client.kill_task.assert_called_once_with(old_app.id, old_task.id, scale=True)
            create_marathon_app_patch.assert_called_once_with(fake_config['id'], fake_config, fake_client)
            kill_old_ids_patch.assert_called_once_with([old_app_id], fake_client)
            # We should call _log 5 times:
            # 1. bounce starts
            # 2. create new app
            # 3. killing old tasks
            # 4. remove old apps
            # 5. bounce finishes
            assert mock_log.call_count == 5

    def test_deploy_service_already_bouncing(self):
        fake_bounce = 'areallygoodbouncestrategy'
        fake_name = 'how_many_strings'
        fake_instance = 'will_i_need_to_think_of'
        fake_id = marathon_tools.compose_job_id(fake_name, fake_instance, tag='blah')
        fake_config = {'id': fake_id, 'instances': 2}

        old_app_id = ('%s2' % fake_id)
        old_task = mock.Mock(id="old_task_id", app_id=old_app_id)
        old_app = mock.Mock(id=old_app_id, tasks=[old_task])

        fake_client = mock.MagicMock(
            list_apps=mock.Mock(return_value=[old_app]),
            kill_task=mock.Mock(spec=lambda app_id, id, scale=False: None),
        )

        fake_bounce_func = mock.create_autospec(
            bounce_lib.brutal_bounce,
            return_value={
                "create_app": True,
                "tasks_to_kill": [old_task],
                "apps_to_kill": [old_app_id],
            }
        )

        fake_short_id = marathon_tools.remove_tag_from_job_id(fake_id)

        with contextlib.nested(
            mock.patch(
                'paasta_tools.bounce_lib.get_bounce_method_func',
                return_value=fake_bounce_func,
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.bounce_lib.bounce_lock_zookeeper',
                side_effect=bounce_lib.LockHeldException,
                autospec=True
            ),
            mock.patch(
                'paasta_tools.bounce_lib.get_happy_tasks',
                autospec=True,
                side_effect=lambda x, _, __, **kwargs: x,
            ),
            mock.patch('setup_marathon_job._log', autospec=True),
            mock.patch(
                'paasta_tools.setup_marathon_job.marathon_tools.get_cluster',
                return_value='fake_cluster',
                autospec=True
            ),
        ) as (_, _, _, _, _):
            result = setup_marathon_job.deploy_service(
                fake_name,
                fake_instance,
                fake_id,
                fake_config,
                fake_client,
                fake_bounce,
                nerve_ns=fake_instance,
                bounce_health_params={},
            )
            assert result == (1, "Instance %s is already being bounced." % fake_short_id)

    def test_deploy_service_logs_exceptions(self):
        fake_bounce = 'WHEEEEEEEEEEEEEEEE'
        fake_name = 'whoa'
        fake_instance = 'the_earth_is_tiny'
        fake_id = marathon_tools.compose_job_id(fake_name, fake_instance)
        fake_apps = [mock.Mock(id=fake_id, tasks=[]), mock.Mock(id=('%s2' % fake_id), tasks=[])]
        fake_client = mock.MagicMock(
            list_apps=mock.Mock(return_value=fake_apps))
        fake_config = {'id': fake_id, 'instances': 2}

        with contextlib.nested(
            mock.patch('setup_marathon_job._log', autospec=True),
            mock.patch('setup_marathon_job.bounce_lib.get_bounce_method_func', side_effect=IOError('foo')),
            mock.patch(
                'paasta_tools.setup_marathon_job.marathon_tools.get_cluster',
                return_value='fake_cluster',
                autospec=True
            ),
        ) as (mock_log, mock_bounce,  mock_get_cluster):
            with raises(IOError):
                setup_marathon_job.deploy_service(
                    fake_name,
                    fake_instance,
                    fake_id,
                    fake_config,
                    fake_client,
                    fake_bounce,
                    nerve_ns=fake_instance,
                    bounce_health_params={},
                )
            assert mock_log.call_count == 1

    def test_get_marathon_config(self):
        fake_conf = {'oh_no': 'im_a_ghost'}
        with mock.patch(
            'paasta_tools.marathon_tools.load_marathon_config',
            return_value=fake_conf,
            autospec=True
        ) as get_conf_patch:
            assert setup_marathon_job.get_main_marathon_config() == fake_conf
            get_conf_patch.assert_called_once_with()
