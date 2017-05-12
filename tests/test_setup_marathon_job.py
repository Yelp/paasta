#!/usr/bin/env python
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

import marathon
import mock
from pytest import raises

from paasta_tools import bounce_lib
from paasta_tools import long_running_service_tools
from paasta_tools import marathon_tools
from paasta_tools import setup_marathon_job
from paasta_tools import utils
from paasta_tools.bounce_lib import list_bounce_methods
from paasta_tools.bounce_lib import LockHeldException
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import paasta_print


class TestSetupMarathonJob:

    fake_docker_image = 'test_docker:1.0'
    fake_cluster = 'fake_test_cluster'
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='servicename',
        cluster='clustername',
        instance='instancename',
        config_dict={
            'instances': 3,
            'cpus': 1,
            'mem': 100,
            'docker_image': fake_docker_image,
            'nerve_ns': 'aaaaugh',
            'bounce_method': 'brutal'
        },
        branch_dict={},
    )
    fake_docker_registry = 'remote_registry.com'
    fake_marathon_config = marathon_tools.MarathonConfig({
        'url': 'http://test_url',
        'user': 'admin',
        'password': 'admin_pass',
    })
    fake_args = mock.MagicMock(
        service_instance_list=['what_is_love.bby_dont_hurt_me'],
        soa_dir='no_more',
        verbose=False,
    )
    fake_service_namespace_config = long_running_service_tools.ServiceNamespaceConfig({
        'mode': 'http'
    })

    def test_main_success(self):
        fake_client = mock.MagicMock()
        with mock.patch(
            'paasta_tools.setup_marathon_job.parse_args',
            return_value=self.fake_args,
            autospec=True,
        ) as parse_args_patch, mock.patch(
            'paasta_tools.setup_marathon_job.get_main_marathon_config',
            return_value=self.fake_marathon_config,
            autospec=True,
        ) as get_main_conf_patch, mock.patch(
            'paasta_tools.marathon_tools.get_marathon_client',
            return_value=fake_client,
            autospec=True,
        ) as get_client_patch, mock.patch(
            'paasta_tools.marathon_tools.load_marathon_service_config',
            return_value=self.fake_marathon_service_config,
            autospec=True,
        ) as read_service_conf_patch, mock.patch(
            'paasta_tools.setup_marathon_job.setup_service',
            return_value=(0, 'it_is_finished', None),
            autospec=True,
        ) as setup_service_patch, mock.patch(
            'paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True,
        ) as load_system_paasta_config_patch, mock.patch(
            'paasta_tools.setup_marathon_job.send_event', autospec=True,
        ), mock.patch(
            'sys.exit', autospec=True,
        ) as sys_exit_patch, mock.patch(
            'paasta_tools.setup_marathon_job.get_draining_hosts', autospec=True,
        ), mock.patch(
            'paasta_tools.marathon_tools.get_all_marathon_apps', autospec=True,
        ) as get_all_marathon_apps_patch, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.bounce_lock_zookeeper', autospec=True,
        ):
            mock_apps = mock.Mock()
            get_all_marathon_apps_patch.return_value = mock_apps
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(return_value=self.fake_cluster)
            setup_marathon_job.main()
            parse_args_patch.assert_called_once_with()
            get_main_conf_patch.assert_called_once_with()
            get_client_patch.assert_called_once_with(
                self.fake_marathon_config.get_url(),
                self.fake_marathon_config.get_username(),
                self.fake_marathon_config.get_password(),
            )
            read_service_conf_patch.assert_called_once_with(
                decompose_job_id(self.fake_args.service_instance_list[0])[0],
                decompose_job_id(self.fake_args.service_instance_list[0])[1],
                self.fake_cluster,
                soa_dir=self.fake_args.soa_dir,
            )
            setup_service_patch.assert_called_once_with(
                decompose_job_id(self.fake_args.service_instance_list[0])[0],
                decompose_job_id(self.fake_args.service_instance_list[0])[1],
                fake_client,
                self.fake_marathon_service_config,
                mock_apps,
                'no_more',
            )
            sys_exit_patch.assert_called_once_with(0)

    def test_main_failure(self):
        fake_client = mock.MagicMock()
        with mock.patch(
            'paasta_tools.setup_marathon_job.parse_args',
            return_value=self.fake_args,
            autospec=True,
        ) as parse_args_patch, mock.patch(
            'paasta_tools.setup_marathon_job.get_main_marathon_config',
            return_value=self.fake_marathon_config,
            autospec=True,
        ) as get_main_conf_patch, mock.patch(
            'paasta_tools.marathon_tools.get_marathon_client',
            return_value=fake_client,
            autospec=True,
        ) as get_client_patch, mock.patch(
            'paasta_tools.marathon_tools.load_marathon_service_config',
            return_value=self.fake_marathon_service_config,
            autospec=True,
        ) as read_service_conf_patch, mock.patch(
            'paasta_tools.setup_marathon_job.setup_service',
            return_value=(1, 'NEVER', None),
            autospec=True,
        ) as setup_service_patch, mock.patch(
            'paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True,
        ) as load_system_paasta_config_patch, mock.patch(
            'paasta_tools.setup_marathon_job.send_event', autospec=True,
        ), mock.patch(
            'sys.exit', autospec=True,
        ) as sys_exit_patch, mock.patch(
            'paasta_tools.marathon_tools.get_all_marathon_apps', autospec=True,
        ) as get_all_marathon_apps_patch, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.bounce_lock_zookeeper', autospec=True,
        ):
            mock_apps = mock.Mock()
            get_all_marathon_apps_patch.return_value = mock_apps
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(return_value=self.fake_cluster)
            setup_marathon_job.main()
            parse_args_patch.assert_called_once_with()
            get_main_conf_patch.assert_called_once_with()
            get_client_patch.assert_called_once_with(
                self.fake_marathon_config.get_url(),
                self.fake_marathon_config.get_username(),
                self.fake_marathon_config.get_password())
            read_service_conf_patch.assert_called_once_with(
                decompose_job_id(self.fake_args.service_instance_list[0])[0],
                decompose_job_id(self.fake_args.service_instance_list[0])[1],
                self.fake_cluster,
                soa_dir=self.fake_args.soa_dir)
            setup_service_patch.assert_called_once_with(
                decompose_job_id(self.fake_args.service_instance_list[0])[0],
                decompose_job_id(self.fake_args.service_instance_list[0])[1],
                fake_client,
                self.fake_marathon_service_config,
                mock_apps,
                'no_more',
            )
            sys_exit_patch.assert_called_once_with(0)

    def test_main_exits_if_no_deployments_yet(self):
        fake_client = mock.MagicMock()
        with mock.patch(
            'paasta_tools.setup_marathon_job.parse_args',
            return_value=self.fake_args,
            autospec=True,
        ) as parse_args_patch, mock.patch(
            'paasta_tools.setup_marathon_job.get_main_marathon_config',
            return_value=self.fake_marathon_config,
            autospec=True,
        ) as get_main_conf_patch, mock.patch(
            'paasta_tools.marathon_tools.get_marathon_client',
            return_value=fake_client,
            autospec=True,
        ) as get_client_patch, mock.patch(
            'paasta_tools.marathon_tools.load_marathon_service_config',
            side_effect=NoDeploymentsAvailable(),
            autospec=True,
        ) as read_service_conf_patch, mock.patch(
            'paasta_tools.setup_marathon_job.setup_service',
            return_value=(1, 'NEVER'),
            autospec=True,
        ), mock.patch(
            'paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True,
        ) as load_system_paasta_config_patch, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.bounce_lock_zookeeper', autospec=True,
        ):
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(return_value=self.fake_cluster)
            with raises(SystemExit) as exc_info:
                setup_marathon_job.main()
            parse_args_patch.assert_called_once_with()
            get_main_conf_patch.assert_called_once_with()
            get_client_patch.assert_called_once_with(
                self.fake_marathon_config.get_url(),
                self.fake_marathon_config.get_username(),
                self.fake_marathon_config.get_password())
            read_service_conf_patch.assert_called_once_with(
                decompose_job_id(self.fake_args.service_instance_list[0])[0],
                decompose_job_id(self.fake_args.service_instance_list[0])[1],
                self.fake_cluster,
                soa_dir=self.fake_args.soa_dir)
            assert exc_info.value.code == 0

    def test_send_event(self):
        fake_service = 'fake_service'
        fake_instance = 'fake_instance'
        fake_status = '42'
        fake_output = 'The http port is not open'
        fake_soa_dir = ''
        expected_check_name = 'setup_marathon_job.%s' % compose_job_id(fake_service, fake_instance)
        with mock.patch(
            "paasta_tools.monitoring_tools.send_event", autospec=True,
        ) as send_event_patch, mock.patch(
            "paasta_tools.marathon_tools.load_marathon_service_config", autospec=True,
        ) as load_marathon_service_config_patch, mock.patch(
            "paasta_tools.setup_marathon_job.load_system_paasta_config", autospec=True,
        ) as load_system_paasta_config_patch:
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
            load_marathon_service_config_patch.return_value.get_monitoring.return_value = {}

            setup_marathon_job.send_event(
                fake_service,
                fake_instance,
                fake_soa_dir,
                fake_status,
                fake_output
            )

            send_event_patch.assert_called_once_with(
                fake_service,
                expected_check_name,
                {'alert_after': '10m', 'check_every': '10s'},
                fake_status,
                fake_output,
                fake_soa_dir
            )
            load_marathon_service_config_patch.assert_called_once_with(
                fake_service,
                fake_instance,
                load_system_paasta_config_patch.return_value.get_cluster.return_value,
                load_deployments=False,
                soa_dir=fake_soa_dir,
            )

    def test_do_bounce_when_create_app_and_new_app_not_running_but_already_created(self):
        """ Note that this is possible if two bounces are running at the same time
        because we get the list of marathon apps outside of any locking"""
        fake_bounce_func_return = {
            'create_app': True,
            'tasks_to_drain': [mock.Mock(app_id='fake_task_to_kill_1')],
        }
        fake_bounce_func = mock.create_autospec(
            bounce_lib.brutal_bounce,
            return_value=fake_bounce_func_return,
        )
        fake_config = {'instances': 5}
        fake_new_app_running = False
        fake_happy_new_tasks = ['fake_one', 'fake_two', 'fake_three']
        fake_old_app_live_happy_tasks = {}
        fake_old_app_live_unhappy_tasks = {}
        fake_old_app_draining_tasks = {}
        fake_old_app_at_risk_tasks = {}
        fake_service = 'fake_service'
        fake_serviceinstance = 'fake_service.fake_instance'
        self.fake_cluster = 'fake_cluster'
        fake_instance = 'fake_instance'
        fake_bounce_method = 'fake_bounce_method'
        fake_drain_method = mock.Mock()
        fake_marathon_jobid = 'fake.marathon.jobid'
        fake_client = mock.create_autospec(
            marathon.MarathonClient
        )
        expected_new_task_count = fake_config["instances"] - len(fake_happy_new_tasks)

        with mock.patch(
            'paasta_tools.setup_marathon_job._log', autospec=True
        ) as mock_log, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.create_marathon_app', autospec=True
        ) as mock_create_marathon_app, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.kill_old_ids', autospec=True
        ) as mock_kill_old_ids:
            mock_bad_response = mock.Mock(status_code=409,
                                          headers={'content-type': 'application/json'})
            mock_create_marathon_app.side_effect = marathon.exceptions.MarathonHttpError(mock_bad_response)
            setup_marathon_job.do_bounce(
                bounce_func=fake_bounce_func,
                drain_method=fake_drain_method,
                config=fake_config,
                new_app_running=fake_new_app_running,
                happy_new_tasks=fake_happy_new_tasks,
                old_app_live_happy_tasks=fake_old_app_live_happy_tasks,
                old_app_live_unhappy_tasks=fake_old_app_live_unhappy_tasks,
                old_app_draining_tasks=fake_old_app_draining_tasks,
                old_app_at_risk_tasks=fake_old_app_at_risk_tasks,
                service=fake_service,
                bounce_method=fake_bounce_method,
                serviceinstance=fake_serviceinstance,
                cluster=self.fake_cluster,
                instance=fake_instance,
                marathon_jobid=fake_marathon_jobid,
                client=fake_client,
                soa_dir='fake_soa_dir',
            )
            assert mock_log.call_count == 2
            first_logged_line = mock_log.mock_calls[0][2]["line"]
            assert '%s new tasks' % expected_new_task_count in first_logged_line
            second_logged_line = mock_log.mock_calls[1][2]["line"]
            assert 'creating new app with app_id %s' % fake_marathon_jobid in second_logged_line

            assert mock_create_marathon_app.call_count == 1
            assert fake_client.kill_task.call_count == 0
            assert fake_drain_method.drain.call_count == 0
            assert mock_kill_old_ids.call_count == 0

    def test_do_bounce_when_create_app_and_new_app_not_running(self):
        fake_bounce_func_return = {
            'create_app': True,
            'tasks_to_drain': [mock.Mock(app_id='fake_task_to_kill_1')],
        }
        fake_bounce_func = mock.create_autospec(
            bounce_lib.brutal_bounce,
            return_value=fake_bounce_func_return,
        )
        fake_config = {'instances': 5}
        fake_new_app_running = False
        fake_happy_new_tasks = ['fake_one', 'fake_two', 'fake_three']
        fake_old_app_live_happy_tasks = {}
        fake_old_app_live_unhappy_tasks = {}
        fake_old_app_draining_tasks = {}
        fake_old_app_at_risk_tasks = {}
        fake_service = 'fake_service'
        fake_serviceinstance = 'fake_service.fake_instance'
        self.fake_cluster = 'fake_cluster'
        fake_instance = 'fake_instance'
        fake_bounce_method = 'fake_bounce_method'
        fake_drain_method = mock.Mock(is_safe_to_kill=lambda t: False)
        fake_marathon_jobid = 'fake.marathon.jobid'
        fake_client = mock.create_autospec(marathon.MarathonClient)
        expected_new_task_count = fake_config["instances"] - len(fake_happy_new_tasks)
        expected_drain_task_count = len(fake_bounce_func_return['tasks_to_drain'])

        with mock.patch(
            'paasta_tools.setup_marathon_job._log', autospec=True,
        ) as mock_log, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.create_marathon_app', autospec=True,
        ) as mock_create_marathon_app, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.kill_old_ids', autospec=True,
        ) as mock_kill_old_ids:
            setup_marathon_job.do_bounce(
                bounce_func=fake_bounce_func,
                drain_method=fake_drain_method,
                config=fake_config,
                new_app_running=fake_new_app_running,
                happy_new_tasks=fake_happy_new_tasks,
                old_app_live_happy_tasks=fake_old_app_live_happy_tasks,
                old_app_live_unhappy_tasks=fake_old_app_live_unhappy_tasks,
                old_app_draining_tasks=fake_old_app_draining_tasks,
                old_app_at_risk_tasks=fake_old_app_at_risk_tasks,
                service=fake_service,
                bounce_method=fake_bounce_method,
                serviceinstance=fake_serviceinstance,
                cluster=self.fake_cluster,
                instance=fake_instance,
                marathon_jobid=fake_marathon_jobid,
                client=fake_client,
                soa_dir='fake_soa_dir',
            )
            assert mock_log.call_count == 3
            first_logged_line = mock_log.mock_calls[0][2]["line"]
            assert '%s new tasks' % expected_new_task_count in first_logged_line
            second_logged_line = mock_log.mock_calls[1][2]["line"]
            assert 'creating new app with app_id %s' % fake_marathon_jobid in second_logged_line
            third_logged_line = mock_log.mock_calls[2][2]["line"]
            assert 'draining %s old tasks' % expected_drain_task_count in third_logged_line

            assert mock_create_marathon_app.call_count == 1
            assert fake_client.kill_task.call_count == 0
            assert fake_drain_method.drain.call_count == len(fake_bounce_func_return["tasks_to_drain"])
            assert mock_kill_old_ids.call_count == 0

            # test failure from marathon raised
            mock_bad_response = mock.Mock(status_code=500,
                                          headers={'content-type': 'application/json'})
            mock_create_marathon_app.side_effect = marathon.exceptions.MarathonHttpError(mock_bad_response)
            with raises(marathon.exceptions.MarathonHttpError):
                setup_marathon_job.do_bounce(
                    bounce_func=fake_bounce_func,
                    drain_method=fake_drain_method,
                    config=fake_config,
                    new_app_running=fake_new_app_running,
                    happy_new_tasks=fake_happy_new_tasks,
                    old_app_live_happy_tasks=fake_old_app_live_happy_tasks,
                    old_app_live_unhappy_tasks=fake_old_app_live_unhappy_tasks,
                    old_app_draining_tasks=fake_old_app_draining_tasks,
                    old_app_at_risk_tasks=fake_old_app_at_risk_tasks,
                    service=fake_service,
                    bounce_method=fake_bounce_method,
                    serviceinstance=fake_serviceinstance,
                    cluster=self.fake_cluster,
                    instance=fake_instance,
                    marathon_jobid=fake_marathon_jobid,
                    client=fake_client,
                    soa_dir='fake_soa_dir',
                )

    def test_do_bounce_when_create_app_and_new_app_running(self):
        fake_task_to_drain = mock.Mock(app_id='fake_app_to_kill_1')
        fake_bounce_func_return = {
            'create_app': True,
            'tasks_to_drain': [fake_task_to_drain],
        }
        fake_bounce_func = mock.create_autospec(
            bounce_lib.brutal_bounce,
            return_value=fake_bounce_func_return,
        )
        fake_config = {'instances': 5}
        fake_new_app_running = True
        fake_happy_new_tasks = ['fake_one', 'fake_two', 'fake_three']
        fake_old_app_live_happy_tasks = {'fake_app_to_kill_1': {fake_task_to_drain}}
        fake_old_app_live_unhappy_tasks = {'fake_app_to_kill_1': set()}
        fake_old_app_draining_tasks = {'fake_app_to_kill_1': set()}
        fake_old_app_at_risk_tasks = {'fake_app_to_kill_1': set()}
        fake_service = 'fake_service'
        fake_serviceinstance = 'fake_service.fake_instance'
        self.fake_cluster = 'fake_cluster'
        fake_instance = 'fake_instance'
        fake_bounce_method = 'fake_bounce_method'
        fake_drain_method = mock.Mock(is_safe_to_kill=lambda t: False)
        fake_marathon_jobid = 'fake.marathon.jobid'
        fake_client = mock.create_autospec(marathon.MarathonClient)
        expected_new_task_count = fake_config["instances"] - len(fake_happy_new_tasks)
        expected_drain_task_count = len(fake_bounce_func_return['tasks_to_drain'])

        with mock.patch(
            'paasta_tools.setup_marathon_job._log', autospec=True,
        ) as mock_log, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.create_marathon_app', autospec=True,
        ) as mock_create_marathon_app, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.kill_old_ids', autospec=True,
        ) as mock_kill_old_ids:
            setup_marathon_job.do_bounce(
                bounce_func=fake_bounce_func,
                drain_method=fake_drain_method,
                config=fake_config,
                new_app_running=fake_new_app_running,
                happy_new_tasks=fake_happy_new_tasks,
                old_app_live_happy_tasks=fake_old_app_live_happy_tasks,
                old_app_live_unhappy_tasks=fake_old_app_live_unhappy_tasks,
                old_app_draining_tasks=fake_old_app_draining_tasks,
                old_app_at_risk_tasks=fake_old_app_at_risk_tasks,
                service=fake_service,
                bounce_method=fake_bounce_method,
                serviceinstance=fake_serviceinstance,
                cluster=self.fake_cluster,
                instance=fake_instance,
                marathon_jobid=fake_marathon_jobid,
                client=fake_client,
                soa_dir='fake_soa_dir',
            )
            first_logged_line = mock_log.mock_calls[0][2]["line"]
            assert '%s new tasks' % expected_new_task_count in first_logged_line
            second_logged_line = mock_log.mock_calls[1][2]["line"]
            assert 'draining %s old tasks' % expected_drain_task_count in second_logged_line
            assert mock_log.call_count == 2

            assert mock_create_marathon_app.call_count == 0
            assert fake_client.kill_task.call_count == 0
            assert mock_kill_old_ids.call_count == 0
            assert fake_drain_method.drain.call_count == len(fake_bounce_func_return["tasks_to_drain"])

    def test_do_bounce_when_tasks_to_drain(self):
        fake_task_to_drain = mock.Mock(app_id='fake_app_to_kill_1')
        fake_bounce_func_return = {
            'create_app': False,
            'tasks_to_drain': [fake_task_to_drain],
        }
        fake_bounce_func = mock.create_autospec(
            bounce_lib.brutal_bounce,
            return_value=fake_bounce_func_return,
        )
        fake_config = {'instances': 5}
        fake_new_app_running = True
        fake_happy_new_tasks = ['fake_one', 'fake_two', 'fake_three']
        fake_old_app_live_happy_tasks = {'fake_app_to_kill_1': {fake_task_to_drain}}
        fake_old_app_live_unhappy_tasks = {'fake_app_to_kill_1': set()}
        fake_old_app_draining_tasks = {'fake_app_to_kill_1': set()}
        fake_old_app_at_risk_tasks = {'fake_app_to_kill_1': set()}
        fake_service = 'fake_service'
        fake_serviceinstance = 'fake_service.fake_instance'
        self.fake_cluster = 'fake_cluster'
        fake_instance = 'fake_instance'
        fake_bounce_method = 'fake_bounce_method'
        fake_drain_method = mock.Mock(is_safe_to_kill=lambda t: False)
        fake_marathon_jobid = 'fake.marathon.jobid'
        fake_client = mock.create_autospec(marathon.MarathonClient)
        expected_new_task_count = fake_config["instances"] - len(fake_happy_new_tasks)
        expected_drain_task_count = len(fake_bounce_func_return['tasks_to_drain'])

        with mock.patch(
            'paasta_tools.setup_marathon_job._log', autospec=True,
        ) as mock_log, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.create_marathon_app', autospec=True,
        ) as mock_create_marathon_app, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.kill_old_ids', autospec=True,
        ) as mock_kill_old_ids:
            setup_marathon_job.do_bounce(
                bounce_func=fake_bounce_func,
                drain_method=fake_drain_method,
                config=fake_config,
                new_app_running=fake_new_app_running,
                happy_new_tasks=fake_happy_new_tasks,
                old_app_live_happy_tasks=fake_old_app_live_happy_tasks,
                old_app_live_unhappy_tasks=fake_old_app_live_unhappy_tasks,
                old_app_draining_tasks=fake_old_app_draining_tasks,
                old_app_at_risk_tasks=fake_old_app_at_risk_tasks,
                service=fake_service,
                bounce_method=fake_bounce_method,
                serviceinstance=fake_serviceinstance,
                cluster=self.fake_cluster,
                instance=fake_instance,
                marathon_jobid=fake_marathon_jobid,
                client=fake_client,
                soa_dir='fake_soa_dir',
            )
            # assert mock_log.call_count == 3
            first_logged_line = mock_log.mock_calls[0][2]["line"]
            assert '%s new tasks' % expected_new_task_count in first_logged_line
            second_logged_line = mock_log.mock_calls[1][2]["line"]
            assert 'draining %s old tasks with app_id %s' % (expected_drain_task_count, 'fake_app_to_kill_1') \
                in second_logged_line

            assert mock_create_marathon_app.call_count == 0
            assert fake_client.kill_task.call_count == 0
            assert mock_kill_old_ids.call_count == 0
            assert fake_drain_method.drain.call_count == expected_drain_task_count

    def test_do_bounce_when_apps_to_kill(self):
        fake_bounce_func_return = {
            'create_app': False,
            'tasks_to_drain': [],
        }
        fake_bounce_func = mock.create_autospec(
            bounce_lib.brutal_bounce,
            return_value=fake_bounce_func_return,
        )
        fake_config = {'instances': 5}
        fake_new_app_running = True
        fake_happy_new_tasks = ['fake_one', 'fake_two', 'fake_three']
        fake_old_app_live_happy_tasks = {'fake_app_to_kill_1': set()}
        fake_old_app_live_unhappy_tasks = {'fake_app_to_kill_1': set()}
        fake_old_app_draining_tasks = {'fake_app_to_kill_1': set()}
        fake_old_app_at_risk_tasks = {'fake_app_to_kill_1': set()}
        fake_service = 'fake_service'
        fake_serviceinstance = 'fake_service.fake_instance'
        self.fake_cluster = 'fake_cluster'
        fake_instance = 'fake_instance'
        fake_bounce_method = 'fake_bounce_method'
        fake_drain_method = mock.Mock()
        fake_marathon_jobid = 'fake.marathon.jobid'
        fake_client = mock.create_autospec(marathon.MarathonClient)
        expected_new_task_count = fake_config["instances"] - len(fake_happy_new_tasks)

        with mock.patch(
            'paasta_tools.setup_marathon_job._log', autospec=True,
        ) as mock_log, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.create_marathon_app', autospec=True,
        ) as mock_create_marathon_app, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.kill_old_ids', autospec=True,
        ) as mock_kill_old_ids:
            setup_marathon_job.do_bounce(
                bounce_func=fake_bounce_func,
                drain_method=fake_drain_method,
                config=fake_config,
                new_app_running=fake_new_app_running,
                happy_new_tasks=fake_happy_new_tasks,
                old_app_live_happy_tasks=fake_old_app_live_happy_tasks,
                old_app_live_unhappy_tasks=fake_old_app_live_unhappy_tasks,
                old_app_draining_tasks=fake_old_app_draining_tasks,
                old_app_at_risk_tasks=fake_old_app_at_risk_tasks,
                service=fake_service,
                bounce_method=fake_bounce_method,
                serviceinstance=fake_serviceinstance,
                cluster=self.fake_cluster,
                instance=fake_instance,
                marathon_jobid=fake_marathon_jobid,
                client=fake_client,
                soa_dir='fake_soa_dir',
            )
            assert mock_log.call_count == 3
            first_logged_line = mock_log.mock_calls[0][2]["line"]
            assert '%s new tasks' % expected_new_task_count in first_logged_line

            second_logged_line = mock_log.mock_calls[1][2]["line"]
            assert 'removing old unused apps with app_ids: %s' % 'fake_app_to_kill_1' in second_logged_line

            assert mock_create_marathon_app.call_count == 0
            assert fake_client.kill_task.call_count == len(fake_bounce_func_return["tasks_to_drain"])
            assert mock_kill_old_ids.call_count == 1

            third_logged_line = mock_log.mock_calls[2][2]["line"]
            assert '%s bounce on %s finish' % (fake_bounce_method, fake_serviceinstance) in third_logged_line
            assert 'Now running %s' % fake_marathon_jobid in third_logged_line

    def test_do_bounce_when_nothing_to_do(self):
        fake_bounce_func_return = {
            'create_app': False,
            'tasks_to_drain': [],
        }
        fake_bounce_func = mock.create_autospec(
            bounce_lib.brutal_bounce,
            return_value=fake_bounce_func_return,
        )

        fake_config = {'instances': 3}
        fake_new_app_running = True
        fake_happy_new_tasks = ['fake_one', 'fake_two', 'fake_three']
        fake_old_app_live_happy_tasks = {}
        fake_old_app_live_unhappy_tasks = {}
        fake_old_app_draining_tasks = {}
        fake_old_app_at_risk_tasks = {}
        fake_service = 'fake_service'
        fake_serviceinstance = 'fake_service.fake_instance'
        self.fake_cluster = 'fake_cluster'
        fake_instance = 'fake_instance'
        fake_bounce_method = 'fake_bounce_method'
        fake_drain_method = mock.Mock()
        fake_marathon_jobid = 'fake.marathon.jobid'
        fake_client = mock.create_autospec(marathon.MarathonClient)

        with mock.patch(
            'paasta_tools.setup_marathon_job._log', autospec=True,
        ) as mock_log, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.create_marathon_app', autospec=True,
        ) as mock_create_marathon_app, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.kill_old_ids', autospec=True,
        ) as mock_kill_old_ids:
            setup_marathon_job.do_bounce(
                bounce_func=fake_bounce_func,
                drain_method=fake_drain_method,
                config=fake_config,
                new_app_running=fake_new_app_running,
                happy_new_tasks=fake_happy_new_tasks,
                old_app_live_happy_tasks=fake_old_app_live_happy_tasks,
                old_app_live_unhappy_tasks=fake_old_app_live_unhappy_tasks,
                old_app_draining_tasks=fake_old_app_draining_tasks,
                old_app_at_risk_tasks=fake_old_app_at_risk_tasks,
                service=fake_service,
                bounce_method=fake_bounce_method,
                serviceinstance=fake_serviceinstance,
                cluster=self.fake_cluster,
                instance=fake_instance,
                marathon_jobid=fake_marathon_jobid,
                client=fake_client,
                soa_dir='fake_soa_dir',
            )
            assert mock_log.call_count == 0
            assert mock_create_marathon_app.call_count == 0
            assert fake_drain_method.drain.call_count == 0
            assert mock_kill_old_ids.call_count == 0

    def test_do_bounce_when_all_old_tasks_are_unhappy(self):
        old_tasks = [mock.Mock(id=id, app_id='old_app') for id in ['old_one', 'old_two', 'old_three']]
        fake_bounce_func_return = {
            'create_app': False,
            'tasks_to_drain': old_tasks,
        }
        fake_bounce_func = mock.create_autospec(
            bounce_lib.brutal_bounce,
            return_value=fake_bounce_func_return,
        )

        fake_config = {'instances': 3}
        fake_new_app_running = True
        fake_happy_new_tasks = ['fake_one', 'fake_two', 'fake_three']
        fake_old_app_live_happy_tasks = {'old_app': set()}
        fake_old_app_live_unhappy_tasks = {'old_app': set(old_tasks)}
        fake_old_app_draining_tasks = {'old_app': set()}
        fake_old_app_at_risk_tasks = {'old_app': set()}
        fake_service = 'fake_service'
        fake_serviceinstance = 'fake_service.fake_instance'
        self.fake_cluster = 'fake_cluster'
        fake_instance = 'fake_instance'
        fake_bounce_method = 'fake_bounce_method'
        fake_drain_method = mock.Mock()
        fake_drain_method.is_safe_to_kill.return_value = False
        fake_marathon_jobid = 'fake.marathon.jobid'
        fake_client = mock.create_autospec(marathon.MarathonClient)

        with mock.patch(
            'paasta_tools.setup_marathon_job._log', autospec=True,
        ), mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.create_marathon_app', autospec=True,
        ), mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.kill_old_ids', autospec=True,
        ) as mock_kill_old_ids:
            setup_marathon_job.do_bounce(
                bounce_func=fake_bounce_func,
                drain_method=fake_drain_method,
                config=fake_config,
                new_app_running=fake_new_app_running,
                happy_new_tasks=fake_happy_new_tasks,
                old_app_live_happy_tasks=fake_old_app_live_happy_tasks,
                old_app_live_unhappy_tasks=fake_old_app_live_unhappy_tasks,
                old_app_draining_tasks=fake_old_app_draining_tasks,
                old_app_at_risk_tasks=fake_old_app_at_risk_tasks,
                service=fake_service,
                bounce_method=fake_bounce_method,
                serviceinstance=fake_serviceinstance,
                cluster=self.fake_cluster,
                instance=fake_instance,
                marathon_jobid=fake_marathon_jobid,
                client=fake_client,
                soa_dir='fake_soa_dir',
            )

            # Since the old tasks are all unhappy, we should drain all of them
            assert fake_drain_method.drain.call_count == 3
            # But since they haven't drained yet, we should not kill the app.
            assert mock_kill_old_ids.call_count == 0

    def test_deploy_service_scale_up(self):
        fake_service = 'fake_service'
        fake_instance = 'fake_instance'
        fake_jobid = 'fake_jobid'
        fake_config = {
            'id': 'some_id',
            'instances': 5,
        }
        fake_client = mock.MagicMock(scale_app=mock.Mock())
        fake_bounce_method = 'bounce'
        fake_drain_method_name = 'drain'
        fake_drain_method_params = {}
        fake_nerve_ns = 'nerve'
        fake_bounce_health_params = {}
        fake_soa_dir = '/soa/dir'
        fake_marathon_apps = mock.Mock()
        with mock.patch(
            'paasta_tools.setup_marathon_job._log', autospec=True,
        ), mock.patch(
            'paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True,
        ) as mock_load_system_paasta_config, mock.patch(
            'paasta_tools.setup_marathon_job.marathon_tools.get_matching_apps', autospec=True,
        ) as mock_get_matching_apps, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.get_happy_tasks', autospec=True,
        ) as mock_get_happy_tasks, mock.patch(
            'paasta_tools.setup_marathon_job.drain_lib.get_drain_method', autospec=True,
        ) as mock_get_drain_method, mock.patch(
            'paasta_tools.setup_marathon_job.get_draining_hosts', autospec=True,
        ), mock.patch(
            'paasta_tools.mesos_maintenance.get_draining_hosts', autospec=True,
        ):
            mock_load_system_paasta_config.return_value = mock.MagicMock(
                get_cluster=mock.Mock(return_value='fake_cluster'))
            mock_get_matching_apps.return_value = [mock.Mock(id='/some_id', instances=1, tasks=[])]
            mock_get_happy_tasks.return_value = []
            mock_get_drain_method.return_value = mock.Mock(is_draining=mock.Mock(return_value=False))
            setup_marathon_job.deploy_service(
                service=fake_service,
                instance=fake_instance,
                marathon_jobid=fake_jobid,
                config=fake_config,
                client=fake_client,
                marathon_apps=fake_marathon_apps,
                bounce_method=fake_bounce_method,
                drain_method_name=fake_drain_method_name,
                drain_method_params=fake_drain_method_params,
                nerve_ns=fake_nerve_ns,
                bounce_health_params=fake_bounce_health_params,
                soa_dir=fake_soa_dir,
            )
            mock_get_matching_apps.assert_called_with(fake_service, fake_instance, fake_marathon_apps)
            fake_client.scale_app.assert_called_once_with(
                app_id='/some_id',
                instances=5,
                force=True,
            )

    def test_deploy_service_scale_up_at_risk_hosts(self):
        fake_service = 'fake_service'
        fake_instance = 'fake_instance'
        fake_jobid = 'fake_jobid'
        fake_config = {
            'id': 'some_id',
            'instances': 5,
        }
        fake_client = mock.MagicMock(scale_app=mock.Mock())
        fake_bounce_method = 'bounce'
        fake_drain_method_name = 'drain'
        fake_drain_method_params = {}
        fake_nerve_ns = 'nerve'
        fake_bounce_health_params = {}
        fake_soa_dir = '/soa/dir'
        fake_marathon_apps = mock.Mock()
        with mock.patch(
            'paasta_tools.setup_marathon_job._log', autospec=True,
        ), mock.patch(
            'paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True,
        ) as mock_load_system_paasta_config, mock.patch(
            'paasta_tools.setup_marathon_job.marathon_tools.get_matching_apps', autospec=True,
        ) as mock_get_matching_apps, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.get_happy_tasks', autospec=True,
        ) as mock_get_happy_tasks, mock.patch(
            'paasta_tools.setup_marathon_job.drain_lib.get_drain_method', autospec=True,
        ) as mock_get_drain_method, mock.patch(
            'paasta_tools.setup_marathon_job.get_draining_hosts', autospec=True,
        ) as mock_get_draining_hosts, mock.patch(
            'paasta_tools.mesos_maintenance.get_draining_hosts', autospec=True,
        ) as mock_mt_get_draining_hosts:
            mock_load_system_paasta_config.return_value = mock.MagicMock(
                get_cluster=mock.Mock(return_value='fake_cluster'))
            mock_get_draining_hosts.return_value = ['fake-host1', 'fake-host2']
            mock_mt_get_draining_hosts.return_value = ['fake-host1', 'fake-host2']
            tasks = [
                mock.Mock(host='fake-host1'),
                mock.Mock(host='fake-host2'),
                mock.Mock(host='fake-host3'),
                mock.Mock(host='fake-host4'),
                mock.Mock(host='fake-host5'),
            ]
            mock_get_matching_apps.return_value = [mock.Mock(id='/some_id', instances=1, tasks=tasks)]
            mock_get_happy_tasks.return_value = []
            mock_get_drain_method.return_value = mock.Mock(is_draining=mock.Mock(return_value=False))
            setup_marathon_job.deploy_service(
                service=fake_service,
                instance=fake_instance,
                marathon_jobid=fake_jobid,
                config=fake_config,
                client=fake_client,
                marathon_apps=fake_marathon_apps,
                bounce_method=fake_bounce_method,
                drain_method_name=fake_drain_method_name,
                drain_method_params=fake_drain_method_params,
                nerve_ns=fake_nerve_ns,
                bounce_health_params=fake_bounce_health_params,
                soa_dir=fake_soa_dir,
            )
            fake_client.scale_app.assert_called_once_with(
                app_id='/some_id',
                instances=7,
                force=True,
            )

    def test_deploy_service_scale_down(self):
        fake_service = 'fake_service'
        fake_instance = 'fake_instance'
        fake_jobid = 'fake_jobid'
        fake_config = {
            'id': 'some_id',
            'instances': 1,
        }
        fake_client = mock.MagicMock()
        fake_bounce_method = 'bounce'
        fake_drain_method_name = 'drain'
        fake_drain_method_params = {}
        fake_nerve_ns = 'nerve'
        fake_bounce_health_params = {}
        fake_soa_dir = '/soa/dir'
        fake_marathon_apps = mock.Mock()
        with mock.patch(
            'paasta_tools.setup_marathon_job._log', autospec=True,
        ), mock.patch(
            'paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True,
        ) as mock_load_system_paasta_config, mock.patch(
            'paasta_tools.setup_marathon_job.marathon_tools.get_matching_apps', autospec=True,
        ) as mock_get_matching_apps, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.get_happy_tasks', autospec=True,
        ) as mock_get_happy_tasks, mock.patch(
            'paasta_tools.setup_marathon_job.drain_lib.get_drain_method', autospec=True,
        ) as mock_get_drain_method, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.get_bounce_method_func', autospec=True,
        ), mock.patch(
            'paasta_tools.setup_marathon_job.do_bounce', autospec=True,
        ) as mock_do_bounce, mock.patch(
            'paasta_tools.setup_marathon_job.get_draining_hosts', autospec=True,
        ), mock.patch(
            'paasta_tools.mesos_maintenance.get_draining_hosts', autospec=True,
        ):
            mock_load_system_paasta_config.return_value = mock.MagicMock(
                get_cluster=mock.Mock(return_value='fake_cluster'))
            tasks = [
                mock.Mock(hostname='fake-host1'),
                mock.Mock(hostname='fake-host2'),
                mock.Mock(hostname='fake-host3'),
                mock.Mock(hostname='fake-host4'),
                mock.Mock(hostname='fake-host5'),
            ]
            mock_get_matching_apps.return_value = [mock.Mock(id='/some_id', instances=5, tasks=tasks)]
            mock_get_happy_tasks.return_value = tasks
            mock_get_drain_method.return_value = mock.Mock(is_draining=mock.Mock(return_value=False))
            setup_marathon_job.deploy_service(
                service=fake_service,
                instance=fake_instance,
                marathon_jobid=fake_jobid,
                config=fake_config,
                client=fake_client,
                marathon_apps=fake_marathon_apps,
                bounce_method=fake_bounce_method,
                drain_method_name=fake_drain_method_name,
                drain_method_params=fake_drain_method_params,
                nerve_ns=fake_nerve_ns,
                bounce_health_params=fake_bounce_health_params,
                soa_dir=fake_soa_dir,
            )
            assert mock_do_bounce.call_args[1]['old_app_live_happy_tasks']['/some_id'] < set(tasks)
            assert len(mock_do_bounce.call_args[1]['old_app_live_happy_tasks']['/some_id']) == 4

    def test_deploy_service_scale_down_doesnt_undrain_scaling_tasks(self):
        fake_service = 'fake_service'
        fake_instance = 'fake_instance'
        fake_jobid = 'fake_jobid'
        fake_config = {
            'id': 'some_id',
            'instances': 3,
        }
        fake_client = mock.MagicMock()
        fake_bounce_method = 'bounce'
        fake_drain_method_name = 'drain'
        fake_drain_method_params = {}
        fake_nerve_ns = 'nerve'
        fake_bounce_health_params = {}
        fake_soa_dir = '/soa/dir'
        fake_marathon_apps = mock.Mock()
        with mock.patch(
            'paasta_tools.setup_marathon_job._log', autospec=True,
        ), mock.patch(
            'paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True,
        ) as mock_load_system_paasta_config, mock.patch(
            'paasta_tools.setup_marathon_job.marathon_tools.get_matching_apps', autospec=True,
        ) as mock_get_matching_apps, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.get_happy_tasks', autospec=True,
        ) as mock_get_happy_tasks, mock.patch(
            'paasta_tools.setup_marathon_job.drain_lib.get_drain_method', autospec=True,
        ) as mock_get_drain_method, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.get_bounce_method_func', autospec=True,
        ), mock.patch(
            'paasta_tools.setup_marathon_job.do_bounce', autospec=True,
        ) as mock_do_bounce, mock.patch(
            'paasta_tools.setup_marathon_job.get_draining_hosts', autospec=True,
        ), mock.patch(
            'paasta_tools.mesos_maintenance.get_draining_hosts', autospec=True,
        ):
            mock_stop_draining = mock.MagicMock()

            mock_load_system_paasta_config.return_value = mock.MagicMock(
                get_cluster=mock.Mock(return_value='fake_cluster'))
            tasks = [
                mock.Mock(host='fake-host1'),
                mock.Mock(host='fake-host2'),
                mock.Mock(host='fake-host3'),
                mock.Mock(host='fake-host4'),
                mock.Mock(host='fake-host5'),
            ]
            mock_get_matching_apps.return_value = [mock.Mock(id='/some_id', instances=5, tasks=tasks)]

            mock_get_happy_tasks.return_value = tasks
            # this drain method gives us 1 healthy task (fake-host1) and 4 draining tasks (fake-host[2-5])
            mock_get_drain_method.return_value = mock.Mock(is_draining=lambda x: x.host != 'fake-host1',
                                                           stop_draining=mock_stop_draining,)
            setup_marathon_job.deploy_service(
                service=fake_service,
                instance=fake_instance,
                marathon_jobid=fake_jobid,
                config=fake_config,
                client=fake_client,
                marathon_apps=fake_marathon_apps,
                bounce_method=fake_bounce_method,
                drain_method_name=fake_drain_method_name,
                drain_method_params=fake_drain_method_params,
                nerve_ns=fake_nerve_ns,
                bounce_health_params=fake_bounce_health_params,
                soa_dir=fake_soa_dir,
            )
            assert mock_do_bounce.call_args[1]['old_app_draining_tasks']['/some_id'] < set(tasks[1:])
            assert len(mock_do_bounce.call_args[1]['old_app_draining_tasks']['/some_id']) == 2
            # we don't bounce happy tasks when draining tasks are available
            assert mock_do_bounce.call_args[1]['old_app_live_happy_tasks']['/some_id'] == set()
            # we only stopped draining the tasks we aren't scaling down
            assert mock_stop_draining.call_count == 3

    def test_setup_service_srv_already_exists(self):
        fake_name = 'if_trees_could_talk'
        fake_instance = 'would_they_scream'
        fake_client = mock.MagicMock(get_app=mock.Mock(return_value=True))
        full_id = marathon_tools.format_job_id(fake_name, fake_instance)
        fake_complete = {
            'seven': 'full',
            'eight': 'frightened',
            'nine': 'eaten',
            'id': full_id,
        }
        with mock.patch.object(
            self.fake_marathon_service_config,
            'format_marathon_app_dict',
            return_value=fake_complete,
            autospec=True,
        ) as format_marathon_app_dict_patch, mock.patch(
            'paasta_tools.marathon_tools.load_marathon_config',
            return_value=self.fake_marathon_config,
            autospec=True,
        ), mock.patch(
            'paasta_tools.marathon_tools.load_service_namespace_config',
            return_value=mock.MagicMock(),
            autospec=True,
        ), mock.patch(
            'paasta_tools.setup_marathon_job.deploy_service',
            autospec=True,
        ) as deploy_service_patch, mock.patch(
            'paasta_tools.setup_marathon_job.get_draining_hosts',
            autospec=True,
        ):
            setup_marathon_job.setup_service(
                service=fake_name,
                instance=fake_instance,
                client=fake_client,
                service_marathon_config=self.fake_marathon_service_config,
                marathon_apps=None,
                soa_dir=None,
            )
            format_marathon_app_dict_patch.assert_called_once_with()
            assert deploy_service_patch.call_count == 1

    def test_setup_service_srv_does_not_exist(self):
        fake_name = 'if_talk_was_cheap'
        fake_instance = 'psychatrists_would_be_broke'
        fake_response = mock.Mock(
            json=mock.Mock(return_value={'message': 'test'}))
        fake_response.headers = {'content-type': 'application/json'}
        fake_client = mock.MagicMock(get_app=mock.Mock(
            side_effect=marathon.exceptions.NotFoundError(fake_response)))
        full_id = marathon_tools.format_job_id(fake_name, fake_instance, 'oogabooga', 'bananafanafofooga')
        fake_complete = {
            'do': 'you', 'even': 'dota', 'id': full_id,
            'docker_image': 'fake_docker_registry/fake_docker_image',
        }
        fake_bounce = 'trampoline'
        fake_drain_method = 'noop'
        fake_drain_method_params = {}
        fake_bounce_margin_factor = 0.5
        with mock.patch(
            'paasta_tools.setup_marathon_job.deploy_service',
            return_value=(111, 'Never'),
            autospec=True,
        ) as deploy_service_patch, mock.patch.object(
            self.fake_marathon_service_config,
            'get_bounce_method',
            return_value=fake_bounce,
            autospec=True,
        ) as get_bounce_patch, mock.patch.object(
            self.fake_marathon_service_config,
            'get_drain_method',
            return_value=fake_drain_method,
            autospec=True,
        ) as get_drain_method_patch, mock.patch.object(
            self.fake_marathon_service_config,
            'get_drain_method_params',
            return_value=fake_drain_method_params,
            autospec=True,
        ), mock.patch.object(
            self.fake_marathon_service_config,
            'format_marathon_app_dict',
            return_value=fake_complete,
            autospec=True,
        ) as format_marathon_app_dict_patch, mock.patch(
            'paasta_tools.marathon_tools.load_marathon_service_config',
            return_value=self.fake_marathon_service_config,
            autospec=True,
        ), mock.patch(
            'paasta_tools.marathon_tools.load_service_namespace_config',
            return_value=self.fake_service_namespace_config,
            autospec=True,
        ) as read_namespace_conf_patch, mock.patch.object(
            self.fake_marathon_service_config,
            'get_bounce_margin_factor',
            return_value=fake_bounce_margin_factor,
            autospec=True,
        ) as get_bounce_margin_factor_patch, mock.patch(
            'paasta_tools.setup_marathon_job.get_draining_hosts',
            autospec=True,
        ):
            mock_marathon_apps = mock.Mock()
            status, output = setup_marathon_job.setup_service(
                service=fake_name,
                instance=fake_instance,
                client=fake_client,
                marathon_apps=mock_marathon_apps,
                service_marathon_config=self.fake_marathon_service_config,
                soa_dir=None,
            )
            assert status == 111
            assert output == 'Never'

            get_bounce_patch.assert_called_once_with()
            get_bounce_margin_factor_patch.assert_called_once_with()
            format_marathon_app_dict_patch.assert_called_once_with()
            get_drain_method_patch.assert_called_once_with(read_namespace_conf_patch.return_value)
            deploy_service_patch.assert_called_once_with(
                service=fake_name,
                instance=fake_instance,
                marathon_jobid=full_id,
                config=fake_complete,
                client=fake_client,
                marathon_apps=mock_marathon_apps,
                bounce_method=fake_bounce,
                drain_method_name=fake_drain_method,
                drain_method_params=fake_drain_method_params,
                nerve_ns=self.fake_marathon_service_config.get_nerve_namespace(),
                bounce_health_params=self.fake_marathon_service_config.get_bounce_health_params(
                    read_namespace_conf_patch.return_value),
                soa_dir=None,
                bounce_margin_factor=fake_bounce_margin_factor,
            )

    def test_setup_service_srv_complete_config_raises(self):
        fake_name = 'test_service'
        fake_instance = 'test_instance'
        with mock.patch.object(
            self.fake_marathon_service_config,
            'format_marathon_app_dict',
            side_effect=NoDockerImageError,
        ):
            status, output, bounce_again = setup_marathon_job.setup_service(
                service=fake_name,
                instance=fake_instance,
                client=None,
                service_marathon_config=self.fake_marathon_service_config,
                marathon_apps=None,
                soa_dir=None,
            )
            assert status == 1
            expected = 'Docker image for test_service.test_instance not in'
            assert expected in output
            assert bounce_again is None

    def test_setup_service_nerve_ns(self):
        fake_service = 'fake_service'
        fake_instance = 'fake_instance'
        fake_cluster = 'fake_cluster'
        fake_nerve_ns = 'fake_nerve_ns'

        fake_msc = marathon_tools.MarathonServiceConfig(
            service=fake_service,
            cluster=fake_cluster,
            instance=fake_instance,
            config_dict={
                'instances': 3,
                'cpus': 1,
                'mem': 100,
                'docker_image': self.fake_docker_image,
                'nerve_ns': 'fake_nerve_ns',
                'bounce_method': 'brutal'
            },
            branch_dict={},
        )

        with mock.patch(
            'paasta_tools.setup_marathon_job.deploy_service', autospec=True,
        ), mock.patch(
            'paasta_tools.marathon_tools.load_service_namespace_config', autospec=True,
        ) as mock_load_service_namespace_config, mock.patch(
            'paasta_tools.marathon_tools.load_system_paasta_config', autospec=True,
        ), mock.patch.object(
            fake_msc, 'format_marathon_app_dict', return_value={'id': 'blurpadurp'},
        ):
            setup_marathon_job.setup_service(
                service=fake_service,
                instance=fake_instance,
                client=None,
                service_marathon_config=fake_msc,
                marathon_apps=None,
                soa_dir=None,
            )

            mock_load_service_namespace_config.assert_called_once_with(
                service=fake_service,
                namespace=fake_nerve_ns,
                soa_dir=mock.ANY,
            )

    def test_deploy_service_unknown_drain_method(self):
        fake_bounce = 'exists'
        fake_drain_method = 'doesntexist'
        fake_name = 'whoa'
        fake_instance = 'the_earth_is_tiny'
        fake_id = marathon_tools.format_job_id(fake_name, fake_instance)
        fake_marathon_apps = [mock.Mock(id=fake_id, tasks=[]), mock.Mock(id=('%s2' % fake_id), tasks=[])]
        fake_client = mock.MagicMock(
            list_apps=mock.Mock(return_value=fake_marathon_apps))
        fake_config = {'id': fake_id, 'instances': 2}

        errormsg = 'ERROR: drain_method not recognized: doesntexist. Must be one of (exists1, exists2)'
        expected = (1, errormsg, None)

        with mock.patch(
            'paasta_tools.setup_marathon_job._log', autospec=True,
        ) as mock_log, mock.patch(
            'paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True,
        ) as mock_load_system_paasta_config, mock.patch(
            'paasta_tools.drain_lib._drain_methods', autospec=None,
            new={'exists1': mock.Mock(), 'exists2': mock.Mock()},
        ), mock.patch(
            'paasta_tools.setup_marathon_job.get_draining_hosts', autospec=True,
        ):
            mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
            actual = setup_marathon_job.deploy_service(
                service=fake_name,
                instance=fake_instance,
                marathon_jobid=fake_id,
                config=fake_config,
                client=fake_client,
                marathon_apps=fake_marathon_apps,
                bounce_method=fake_bounce,
                drain_method_name=fake_drain_method,
                drain_method_params={},
                nerve_ns=fake_instance,
                bounce_health_params={},
                soa_dir='fake_soa_dir',
            )
            assert mock_log.call_count == 1
        assert expected == actual

    def test_deploy_service_unknown_bounce(self):
        fake_bounce = 'WHEEEEEEEEEEEEEEEE'
        fake_drain_method = 'noop'
        fake_name = 'whoa'
        fake_instance = 'the_earth_is_tiny'
        fake_id = marathon_tools.format_job_id(fake_name, fake_instance)
        fake_marathon_apps = [mock.Mock(id=fake_id, tasks=[]), mock.Mock(id=('%s2' % fake_id), tasks=[])]
        fake_client = mock.MagicMock(
            list_apps=mock.Mock(return_value=fake_marathon_apps))
        fake_config = {'id': fake_id, 'instances': 2}

        errormsg = 'ERROR: bounce_method not recognized: %s. Must be one of (%s)' % \
            (fake_bounce, ', '.join(list_bounce_methods()))
        expected = (1, errormsg, None)

        with mock.patch(
            'paasta_tools.setup_marathon_job._log', autospec=True,
        ) as mock_log, mock.patch(
            'paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True,
        ) as mock_load_system_paasta_config, mock.patch(
            'paasta_tools.setup_marathon_job.get_draining_hosts', autospec=True,
        ):
            mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
            actual = setup_marathon_job.deploy_service(
                service=fake_name,
                instance=fake_instance,
                marathon_jobid=fake_id,
                config=fake_config,
                client=fake_client,
                marathon_apps=fake_marathon_apps,
                bounce_method=fake_bounce,
                drain_method_name=fake_drain_method,
                drain_method_params={},
                nerve_ns=fake_instance,
                bounce_health_params={},
                soa_dir='fake_soa_dir',
            )
            assert mock_log.call_count == 1
        assert expected == actual
        assert fake_client.create_app.call_count == 0

    def test_deploy_service_known_bounce(self):
        fake_bounce = 'areallygoodbouncestrategy'
        fake_drain_method_name = 'noop'
        fake_name = 'how_many_strings'
        fake_instance = 'will_i_need_to_think_of'
        fake_id = marathon_tools.format_job_id(fake_name, fake_instance, 'git11111111', 'config11111111')
        fake_config = {'id': fake_id, 'instances': 2}

        old_app_id = marathon_tools.format_job_id(fake_name, fake_instance, 'git22222222', 'config22222222')
        old_task_to_drain = mock.Mock(id="old_task_to_drain", app_id=old_app_id)
        old_task_is_draining = mock.Mock(id="old_task_is_draining", app_id=old_app_id)
        old_task_dont_drain = mock.Mock(id="old_task_dont_drain", app_id=old_app_id)

        old_app = mock.Mock(id="/%s" % old_app_id, tasks=[old_task_to_drain, old_task_is_draining, old_task_dont_drain])

        fake_client = mock.MagicMock(  # pragma: no branch (only used for interface)
            list_apps=mock.Mock(return_value=[old_app]),
            kill_given_tasks=mock.Mock(
                spec=lambda task_ids, scale=False: None,
            ),
        )

        fake_bounce_func = mock.create_autospec(
            bounce_lib.brutal_bounce,
            return_value={
                "create_app": True,
                "tasks_to_drain": [old_task_to_drain],
            }
        )

        fake_drain_method = mock.Mock(is_draining=lambda t: t is old_task_is_draining, is_safe_to_kill=lambda t: True)

        with mock.patch(
            'paasta_tools.bounce_lib.get_bounce_method_func',
            return_value=fake_bounce_func,
            autospec=True,
        ), mock.patch(
            'paasta_tools.bounce_lib.get_happy_tasks',
            autospec=True,
            side_effect=lambda x, _, __, ___, **kwargs: x.tasks,
        ), mock.patch(
            'paasta_tools.bounce_lib.kill_old_ids', autospec=True,
        ) as kill_old_ids_patch, mock.patch(
            'paasta_tools.bounce_lib.create_marathon_app', autospec=True,
        ) as create_marathon_app_patch, mock.patch(
            'paasta_tools.setup_marathon_job._log', autospec=True,
        ) as mock_log, mock.patch(
            'paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True,
        ) as mock_load_system_paasta_config, mock.patch(
            'paasta_tools.drain_lib.get_drain_method', return_value=fake_drain_method, autospec=True,
        ), mock.patch(
            'paasta_tools.setup_marathon_job.get_draining_hosts', autospec=True,
        ):
            mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
            result = setup_marathon_job.deploy_service(
                service=fake_name,
                instance=fake_instance,
                marathon_jobid=fake_id,
                config=fake_config,
                client=fake_client,
                marathon_apps=[old_app],
                bounce_method=fake_bounce,
                drain_method_name=fake_drain_method_name,
                drain_method_params={},
                nerve_ns=fake_instance,
                bounce_health_params={},
                soa_dir='fake_soa_dir',
            )
            assert result[0] == 0, "Expected successful result; got (%d, %s)" % result
            assert fake_client.create_app.call_count == 0
            fake_bounce_func.assert_called_once_with(
                new_config=fake_config,
                new_app_running=False,
                happy_new_tasks=[],
                old_app_live_happy_tasks={old_app.id: {old_task_to_drain, old_task_dont_drain}},
                old_app_live_unhappy_tasks={old_app.id: set()},
                margin_factor=1,
            )

            assert fake_drain_method.drain.call_count == 2
            fake_drain_method.drain.assert_any_call(old_task_is_draining)
            fake_drain_method.drain.assert_any_call(old_task_to_drain)

            assert fake_client.kill_given_tasks.call_count == 1
            assert {old_task_to_drain.id, old_task_is_draining.id} == set(
                fake_client.kill_given_tasks.call_args[1]['task_ids'])
            assert fake_client.kill_given_tasks.call_args[1]['scale'] is True

            create_marathon_app_patch.assert_called_once_with(fake_config['id'], fake_config, fake_client)
            assert kill_old_ids_patch.call_count == 0

            # We should call _log 5 times:
            # 1. bounce starts
            # 2. create new app
            # 3. draining old tasks
            # 4. remove old apps
            # 5. bounce finishes

            assert mock_log.call_count == 5

    def test_deploy_service_lock_exceptions(self):
        fake_bounce = 'WHEEEEEEEEEEEEEEEE'
        fake_drain_method = 'noop'
        fake_name = 'whoa'
        fake_instance = 'the_earth_is_tiny'
        fake_id = marathon_tools.format_job_id(fake_name, fake_instance)
        fake_apps = [mock.Mock(id=fake_id, tasks=[]), mock.Mock(id=('%s2' % fake_id), tasks=[])]
        fake_client = mock.MagicMock(
            list_apps=mock.Mock(return_value=fake_apps))
        fake_config = {'id': fake_id, 'instances': 2}

        with mock.patch(
            'paasta_tools.setup_marathon_job._log', autospec=True,
        ) as mock_log, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.get_bounce_method_func',
            side_effect=LockHeldException, autospec=True,
        ), mock.patch(
            'paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True,
        ) as mock_load_system_paasta_config, mock.patch(
            'paasta_tools.setup_marathon_job.get_draining_hosts', autospec=True,
        ):
            mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
            ret = setup_marathon_job.deploy_service(
                service=fake_name,
                instance=fake_instance,
                marathon_jobid=fake_id,
                config=fake_config,
                client=fake_client,
                marathon_apps=fake_apps,
                bounce_method=fake_bounce,
                drain_method_name=fake_drain_method,
                drain_method_params={},
                nerve_ns=fake_instance,
                bounce_health_params={},
                soa_dir='fake_soa_dir',
            )
            assert ret[0] == 0

            logged_line = mock_log.mock_calls[0][2]["line"]
            assert logged_line.startswith("Failed to get lock to create marathon app for %s.%s" % (fake_name,
                                                                                                   fake_instance))

    def test_deploy_service_logs_exceptions(self):
        fake_bounce = 'WHEEEEEEEEEEEEEEEE'
        fake_drain_method = 'noop'
        fake_name = 'whoa'
        fake_instance = 'the_earth_is_tiny'
        fake_id = marathon_tools.format_job_id(fake_name, fake_instance)
        fake_apps = [mock.Mock(id=fake_id, tasks=[]), mock.Mock(id=('%s2' % fake_id), tasks=[])]
        fake_client = mock.MagicMock(
            list_apps=mock.Mock(return_value=fake_apps))
        fake_config = {'id': fake_id, 'instances': 2}

        with mock.patch(
            'paasta_tools.setup_marathon_job._log', autospec=True,
        ) as mock_log, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.get_bounce_method_func',
            side_effect=OSError('foo'), autospec=True,
        ), mock.patch(
            'paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True,
        ) as mock_load_system_paasta_config, mock.patch(
            'paasta_tools.setup_marathon_job.get_draining_hosts', autospec=True,
        ):
            mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
            with raises(OSError):
                setup_marathon_job.deploy_service(
                    service=fake_name,
                    instance=fake_instance,
                    marathon_jobid=fake_id,
                    config=fake_config,
                    client=fake_client,
                    marathon_apps=fake_apps,
                    bounce_method=fake_bounce,
                    drain_method_name=fake_drain_method,
                    drain_method_params={},
                    nerve_ns=fake_instance,
                    bounce_health_params={},
                    soa_dir='fake_soa_dir',
                )

            logged_line = mock_log.mock_calls[0][2]["line"]
            assert logged_line.startswith("Exception raised during deploy of service whoa:\nTraceback")
            assert "OSError: foo" in logged_line

    def test_get_marathon_config(self):
        fake_conf = {'oh_no': 'im_a_ghost'}
        with mock.patch(
            'paasta_tools.marathon_tools.load_marathon_config',
            return_value=fake_conf,
            autospec=True
        ) as get_conf_patch:
            assert setup_marathon_job.get_main_marathon_config() == fake_conf
            get_conf_patch.assert_called_once_with()

    def test_deploy_marathon_service(self):
        with mock.patch(
            'paasta_tools.setup_marathon_job.setup_service', autospec=True,
        ) as mock_setup_service, mock.patch(
            'paasta_tools.setup_marathon_job.bounce_lib.bounce_lock_zookeeper', autospec=True,
            side_effect=bounce_lib.LockHeldException,
        ):
            mock_client = mock.Mock()
            mock_marathon_config = {}
            mock_marathon_apps = []
            ret = setup_marathon_job.deploy_marathon_service('something',
                                                             'main',
                                                             mock_client,
                                                             'fake_soa',
                                                             mock_marathon_config,
                                                             mock_marathon_apps)
            assert not mock_setup_service.called
            assert ret == (0, None)


class TestGetOldHappyUnhappyDrainingTasks(object):
    def fake_task(self, state, happiness):
        return mock.Mock(_drain_state=state, _happiness=happiness)

    def fake_drain_method(self):
        return mock.Mock(is_draining=lambda t: t._drain_state == 'down')

    def fake_get_happy_tasks(self, app, service, nerve_ns, system_paasta_config, **kwargs):
        return [t for t in app.tasks if t._happiness == 'happy']

    def test_get_tasks_by_state_empty(self):
        fake_name = 'whoa'
        fake_instance = 'the_earth_is_tiny'
        fake_id = marathon_tools.format_job_id(fake_name, fake_instance)

        fake_apps = [
            mock.Mock(id=fake_id, tasks=[]),
            mock.Mock(id=('%s2' % fake_id), tasks=[])
        ]
        fake_system_paasta_config = utils.SystemPaastaConfig({}, "/fake/configs")

        expected_live_happy_tasks = {
            fake_apps[0].id: set(),
            fake_apps[1].id: set(),
        }
        expected_live_unhappy_tasks = {
            fake_apps[0].id: set(),
            fake_apps[1].id: set(),
        }
        expected_draining_tasks = {
            fake_apps[0].id: set(),
            fake_apps[1].id: set(),
        }
        expected_at_risk_tasks = {
            fake_apps[0].id: set(),
            fake_apps[1].id: set(),
        }

        with mock.patch(
            'paasta_tools.bounce_lib.get_happy_tasks', side_effect=self.fake_get_happy_tasks, autospec=True,
        ), mock.patch(
            'paasta_tools.setup_marathon_job.get_draining_hosts', autospec=True,
        ):
            actual = setup_marathon_job.get_tasks_by_state(
                other_apps=fake_apps,
                drain_method=self.fake_drain_method(),
                service=fake_name,
                nerve_ns=fake_instance,
                bounce_health_params={},
                system_paasta_config=fake_system_paasta_config,
                log_deploy_error=None,
            )
        actual_live_happy_tasks, actual_live_unhappy_tasks, actual_draining_tasks, actual_at_risk_tasks = actual
        assert actual_live_happy_tasks == expected_live_happy_tasks
        assert actual_live_unhappy_tasks == expected_live_unhappy_tasks
        assert actual_draining_tasks == expected_draining_tasks
        assert actual_at_risk_tasks == expected_at_risk_tasks

    def test_get_tasks_by_state_not_empty(self):
        fake_name = 'whoa'
        fake_instance = 'the_earth_is_tiny'
        fake_id = marathon_tools.format_job_id(fake_name, fake_instance)

        fake_apps = [
            mock.Mock(
                id=fake_id,
                tasks=[
                    self.fake_task('up', 'happy'),
                    self.fake_task('up', 'unhappy'),
                    self.fake_task('down', 'unhappy'),
                ],
            ),
            mock.Mock(
                id=('%s2' % fake_id),
                tasks=[
                    self.fake_task('up', 'happy'),
                    self.fake_task('up', 'unhappy'),
                    self.fake_task('down', 'unhappy'),
                ],
            ),
        ]

        fake_system_paasta_config = utils.SystemPaastaConfig({}, "/fake/configs")

        expected_live_happy_tasks = {
            fake_apps[0].id: {fake_apps[0].tasks[0]},
            fake_apps[1].id: {fake_apps[1].tasks[0]},
        }
        expected_live_unhappy_tasks = {
            fake_apps[0].id: {fake_apps[0].tasks[1]},
            fake_apps[1].id: {fake_apps[1].tasks[1]},
        }
        expected_draining_tasks = {
            fake_apps[0].id: {fake_apps[0].tasks[2]},
            fake_apps[1].id: {fake_apps[1].tasks[2]},
        }
        expected_at_risk_tasks = {
            fake_apps[0].id: set(),
            fake_apps[1].id: set(),
        }

        with mock.patch(
            'paasta_tools.bounce_lib.get_happy_tasks', side_effect=self.fake_get_happy_tasks, autospec=True,
        ), mock.patch(
            'paasta_tools.setup_marathon_job.get_draining_hosts', autospec=True,
        ):
            actual = setup_marathon_job.get_tasks_by_state(
                other_apps=fake_apps,
                drain_method=self.fake_drain_method(),
                service=fake_name,
                nerve_ns=fake_instance,
                bounce_health_params={},
                system_paasta_config=fake_system_paasta_config,
                log_deploy_error=None,
            )
        actual_live_happy_tasks, actual_live_unhappy_tasks, actual_draining_tasks, actual_at_risk_tasks = actual
        assert actual_live_happy_tasks == expected_live_happy_tasks
        assert actual_live_unhappy_tasks == expected_live_unhappy_tasks
        assert actual_draining_tasks == expected_draining_tasks
        assert actual_at_risk_tasks == expected_at_risk_tasks


class TestDrainTasksAndFindTasksToKill(object):
    def test_catches_exception_during_drain(self):
        tasks_to_drain = {mock.Mock(id='to_drain')}
        already_draining_tasks = set()
        at_risk_tasks = set()
        fake_drain_method = mock.Mock(
            drain=mock.Mock(side_effect=Exception('Hello')),
        )

        def _paasta_print(line, level=None):
            paasta_print(line)
        fake_log_bounce_action = mock.Mock(side_effect=_paasta_print)

        setup_marathon_job.drain_tasks_and_find_tasks_to_kill(
            tasks_to_drain=tasks_to_drain,
            already_draining_tasks=already_draining_tasks,
            drain_method=fake_drain_method,
            log_bounce_action=fake_log_bounce_action,
            bounce_method='fake',
            at_risk_tasks=at_risk_tasks,
        )

        fake_log_bounce_action.assert_any_call(
            line="fake bounce killing task to_drain due to exception when draining: Hello",
        )

    def test_catches_exception_during_is_safe_to_kill(self):
        tasks_to_drain = {mock.Mock(id='to_drain')}
        already_draining_tasks = set()
        at_risk_tasks = set()
        fake_drain_method = mock.Mock(
            is_safe_to_kill=mock.Mock(side_effect=Exception('Hello')),
        )
        fake_log_bounce_action = mock.Mock()

        setup_marathon_job.drain_tasks_and_find_tasks_to_kill(
            tasks_to_drain=tasks_to_drain,
            already_draining_tasks=already_draining_tasks,
            drain_method=fake_drain_method,
            log_bounce_action=fake_log_bounce_action,
            bounce_method='fake',
            at_risk_tasks=at_risk_tasks,
        )

        fake_log_bounce_action.assert_called_with(
            line='fake bounce killing task to_drain due to exception in is_safe_to_kill: Hello',
        )


def test_undrain_tasks():
    all_tasks = [mock.Mock(id="task%d" % x) for x in range(5)]
    to_undrain = all_tasks[:4]
    leave_draining = all_tasks[2:]
    fake_drain_method = mock.Mock(
        stop_draining=mock.Mock(side_effect=Exception('Hello')),
    )
    fake_log_deploy_error = mock.Mock()

    setup_marathon_job.undrain_tasks(
        to_undrain=to_undrain,
        leave_draining=leave_draining,
        drain_method=fake_drain_method,
        log_deploy_error=fake_log_deploy_error,
    )

    assert fake_drain_method.stop_draining.call_count == 2
    fake_drain_method.stop_draining.assert_any_call(to_undrain[0])
    fake_drain_method.stop_draining.assert_any_call(to_undrain[1])
    assert fake_log_deploy_error.call_count == 2
