#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
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
from pytest import raises

from paasta_tools import bounce_lib
from paasta_tools import marathon_tools
from paasta_tools import setup_marathon_job
from paasta_tools import utils
from paasta_tools.bounce_lib import list_bounce_methods
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError


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
    }, '/fake/fake_file.json')
    fake_args = mock.MagicMock(
        service_instance='what_is_love.bby_dont_hurt_me',
        soa_dir='no_more',
        verbose=False,
    )
    fake_service_namespace_config = marathon_tools.ServiceNamespaceConfig({
        'mode': 'http'
    })

    def test_main_success(self):
        fake_client = mock.MagicMock()
        with contextlib.nested(
            mock.patch(
                'paasta_tools.setup_marathon_job.parse_args',
                return_value=self.fake_args,
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.setup_marathon_job.get_main_marathon_config',
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
                'paasta_tools.setup_marathon_job.setup_service',
                return_value=(0, 'it_is_finished'),
                autospec=True,
            ),
            mock.patch('paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.send_event', autospec=True),
            mock.patch('sys.exit', autospec=True),
        ) as (
            parse_args_patch,
            get_main_conf_patch,
            get_client_patch,
            read_service_conf_patch,
            setup_service_patch,
            load_system_paasta_config_patch,
            sensu_patch,
            sys_exit_patch,
        ):
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
                decompose_job_id(self.fake_args.service_instance)[0],
                decompose_job_id(self.fake_args.service_instance)[1],
                self.fake_cluster,
                soa_dir=self.fake_args.soa_dir,
            )
            setup_service_patch.assert_called_once_with(
                decompose_job_id(self.fake_args.service_instance)[0],
                decompose_job_id(self.fake_args.service_instance)[1],
                fake_client,
                self.fake_marathon_config,
                self.fake_marathon_service_config,
                'no_more',
            )
            sys_exit_patch.assert_called_once_with(0)

    def test_main_failure(self):
        fake_client = mock.MagicMock()
        with contextlib.nested(
            mock.patch(
                'paasta_tools.setup_marathon_job.parse_args',
                return_value=self.fake_args,
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.setup_marathon_job.get_main_marathon_config',
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
                'paasta_tools.setup_marathon_job.setup_service',
                return_value=(1, 'NEVER'),
                autospec=True,
            ),
            mock.patch('paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.send_event', autospec=True),
            mock.patch('sys.exit', autospec=True),
        ) as (
            parse_args_patch,
            get_main_conf_patch,
            get_client_patch,
            read_service_conf_patch,
            setup_service_patch,
            load_system_paasta_config_patch,
            sensu_patch,
            sys_exit_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(return_value=self.fake_cluster)
            setup_marathon_job.main()
            parse_args_patch.assert_called_once_with()
            get_main_conf_patch.assert_called_once_with()
            get_client_patch.assert_called_once_with(
                self.fake_marathon_config.get_url(),
                self.fake_marathon_config.get_username(),
                self.fake_marathon_config.get_password())
            read_service_conf_patch.assert_called_once_with(
                decompose_job_id(self.fake_args.service_instance)[0],
                decompose_job_id(self.fake_args.service_instance)[1],
                self.fake_cluster,
                soa_dir=self.fake_args.soa_dir)
            setup_service_patch.assert_called_once_with(
                decompose_job_id(self.fake_args.service_instance)[0],
                decompose_job_id(self.fake_args.service_instance)[1],
                fake_client,
                self.fake_marathon_config,
                self.fake_marathon_service_config,
                'no_more',
            )
            sys_exit_patch.assert_called_once_with(0)

    def test_main_exits_if_no_deployments_yet(self):
        fake_client = mock.MagicMock()
        with contextlib.nested(
            mock.patch(
                'paasta_tools.setup_marathon_job.parse_args',
                return_value=self.fake_args,
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.setup_marathon_job.get_main_marathon_config',
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
                side_effect=NoDeploymentsAvailable(),
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.setup_marathon_job.setup_service',
                return_value=(1, 'NEVER'),
                autospec=True,
            ),
            mock.patch('paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True),
        ) as (
            parse_args_patch,
            get_main_conf_patch,
            get_client_patch,
            read_service_conf_patch,
            setup_service_patch,
            load_system_paasta_config_patch,
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
                decompose_job_id(self.fake_args.service_instance)[0],
                decompose_job_id(self.fake_args.service_instance)[1],
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
        with contextlib.nested(
            mock.patch("paasta_tools.monitoring_tools.send_event", autospec=True),
            mock.patch("paasta_tools.marathon_tools.load_marathon_service_config", autospec=True),
            mock.patch("paasta_tools.setup_marathon_job.load_system_paasta_config", autospec=True),
        ) as (
            send_event_patch,
            load_marathon_service_config_patch,
            load_system_paasta_config_patch,
        ):
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

    def test_send_bounce_keepalive(self):
        fake_service = 'fake_service'
        fake_instance = 'fake_instance'
        fake_cluster = 'fake_cluster'
        fake_soa_dir = ''
        expected_check_name = 'paasta_bounce_progress.%s' % compose_job_id(fake_service, fake_instance)
        with contextlib.nested(
            mock.patch("paasta_tools.monitoring_tools.send_event", autospec=True),
            mock.patch("paasta_tools.marathon_tools.load_marathon_service_config", autospec=True),
        ) as (
            send_event_patch,
            load_marathon_service_config_patch,
        ):
            load_marathon_service_config_patch.return_value.get_monitoring.return_value = {}
            setup_marathon_job.send_sensu_bounce_keepalive(
                service=fake_service,
                instance=fake_instance,
                cluster=fake_cluster,
                soa_dir=fake_soa_dir,
            )
            send_event_patch.assert_called_once_with(
                service=fake_service,
                check_name=expected_check_name,
                overrides=mock.ANY,
                status=0,
                output=mock.ANY,
                soa_dir=fake_soa_dir,
                ttl='1h',
            )
            load_marathon_service_config_patch.assert_called_once_with(
                service=fake_service,
                instance=fake_instance,
                cluster=fake_cluster,
                load_deployments=False,
                soa_dir=fake_soa_dir,
            )

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
        fake_service = 'fake_service'
        fake_serviceinstance = 'fake_service.fake_instance'
        self.fake_cluster = 'fake_cluster'
        fake_instance = 'fake_instance'
        fake_bounce_method = 'fake_bounce_method'
        fake_drain_method = mock.Mock(is_safe_to_kill=lambda t: False)
        fake_marathon_jobid = 'fake.marathon.jobid'
        fake_client = mock.create_autospec(
            marathon.MarathonClient
        )
        expected_new_task_count = fake_config["instances"] - len(fake_happy_new_tasks)
        expected_drain_task_count = len(fake_bounce_func_return['tasks_to_drain'])

        with contextlib.nested(
            mock.patch('paasta_tools.setup_marathon_job._log', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.create_marathon_app', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.kill_old_ids', autospec=True),
        ) as (mock_log, mock_create_marathon_app, mock_kill_old_ids):
            setup_marathon_job.do_bounce(
                bounce_func=fake_bounce_func,
                drain_method=fake_drain_method,
                config=fake_config,
                new_app_running=fake_new_app_running,
                happy_new_tasks=fake_happy_new_tasks,
                old_app_live_happy_tasks=fake_old_app_live_happy_tasks,
                old_app_live_unhappy_tasks=fake_old_app_live_unhappy_tasks,
                old_app_draining_tasks=fake_old_app_draining_tasks,
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
        fake_old_app_live_happy_tasks = {'fake_app_to_kill_1': set([fake_task_to_drain])}
        fake_old_app_live_unhappy_tasks = {'fake_app_to_kill_1': set()}
        fake_old_app_draining_tasks = {'fake_app_to_kill_1': set()}
        fake_service = 'fake_service'
        fake_serviceinstance = 'fake_service.fake_instance'
        self.fake_cluster = 'fake_cluster'
        fake_instance = 'fake_instance'
        fake_bounce_method = 'fake_bounce_method'
        fake_drain_method = mock.Mock(is_safe_to_kill=lambda t: False)
        fake_marathon_jobid = 'fake.marathon.jobid'
        fake_client = mock.create_autospec(
            marathon.MarathonClient
        )
        expected_new_task_count = fake_config["instances"] - len(fake_happy_new_tasks)
        expected_drain_task_count = len(fake_bounce_func_return['tasks_to_drain'])

        with contextlib.nested(
            mock.patch('paasta_tools.setup_marathon_job._log', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.create_marathon_app', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.kill_old_ids', autospec=True),
        ) as (mock_log, mock_create_marathon_app, mock_kill_old_ids):
            setup_marathon_job.do_bounce(
                bounce_func=fake_bounce_func,
                drain_method=fake_drain_method,
                config=fake_config,
                new_app_running=fake_new_app_running,
                happy_new_tasks=fake_happy_new_tasks,
                old_app_live_happy_tasks=fake_old_app_live_happy_tasks,
                old_app_live_unhappy_tasks=fake_old_app_live_unhappy_tasks,
                old_app_draining_tasks=fake_old_app_draining_tasks,
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
        fake_old_app_live_happy_tasks = {'fake_app_to_kill_1': set([fake_task_to_drain])}
        fake_old_app_live_unhappy_tasks = {'fake_app_to_kill_1': set([])}
        fake_old_app_draining_tasks = {'fake_app_to_kill_1': set([])}
        fake_service = 'fake_service'
        fake_serviceinstance = 'fake_service.fake_instance'
        self.fake_cluster = 'fake_cluster'
        fake_instance = 'fake_instance'
        fake_bounce_method = 'fake_bounce_method'
        fake_drain_method = mock.Mock(is_safe_to_kill=lambda t: False)
        fake_marathon_jobid = 'fake.marathon.jobid'
        fake_client = mock.create_autospec(
            marathon.MarathonClient
        )
        expected_new_task_count = fake_config["instances"] - len(fake_happy_new_tasks)
        expected_drain_task_count = len(fake_bounce_func_return['tasks_to_drain'])

        with contextlib.nested(
            mock.patch('paasta_tools.setup_marathon_job._log', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.create_marathon_app', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.kill_old_ids', autospec=True),
        ) as (mock_log, mock_create_marathon_app, mock_kill_old_ids):
            setup_marathon_job.do_bounce(
                bounce_func=fake_bounce_func,
                drain_method=fake_drain_method,
                config=fake_config,
                new_app_running=fake_new_app_running,
                happy_new_tasks=fake_happy_new_tasks,
                old_app_live_happy_tasks=fake_old_app_live_happy_tasks,
                old_app_live_unhappy_tasks=fake_old_app_live_unhappy_tasks,
                old_app_draining_tasks=fake_old_app_draining_tasks,
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

        with contextlib.nested(
            mock.patch('paasta_tools.setup_marathon_job._log', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.create_marathon_app', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.kill_old_ids', autospec=True),
        ) as (mock_log, mock_create_marathon_app, mock_kill_old_ids):
            setup_marathon_job.do_bounce(
                bounce_func=fake_bounce_func,
                drain_method=fake_drain_method,
                config=fake_config,
                new_app_running=fake_new_app_running,
                happy_new_tasks=fake_happy_new_tasks,
                old_app_live_happy_tasks=fake_old_app_live_happy_tasks,
                old_app_live_unhappy_tasks=fake_old_app_live_unhappy_tasks,
                old_app_draining_tasks=fake_old_app_draining_tasks,
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

        with contextlib.nested(
            mock.patch('paasta_tools.setup_marathon_job._log', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.create_marathon_app', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.kill_old_ids', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.send_sensu_bounce_keepalive', autospec=True),
        ) as (
            mock_log,
            mock_create_marathon_app,
            mock_kill_old_ids,
            mock_send_sensu_bounce_keepalive,
        ):
            setup_marathon_job.do_bounce(
                bounce_func=fake_bounce_func,
                drain_method=fake_drain_method,
                config=fake_config,
                new_app_running=fake_new_app_running,
                happy_new_tasks=fake_happy_new_tasks,
                old_app_live_happy_tasks=fake_old_app_live_happy_tasks,
                old_app_live_unhappy_tasks=fake_old_app_live_unhappy_tasks,
                old_app_draining_tasks=fake_old_app_draining_tasks,
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
            # When doing nothing, we need to send the keepalive heartbeat to Sensu
            mock_send_sensu_bounce_keepalive.assert_called_once_with(
                service=fake_service,
                instance=fake_instance,
                cluster=self.fake_cluster,
                soa_dir='fake_soa_dir',
            )

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
        fake_old_app_live_happy_tasks = {'old_app': set([])}
        fake_old_app_live_unhappy_tasks = {'old_app': set(old_tasks)}
        fake_old_app_draining_tasks = {'old_app': set([])}
        fake_service = 'fake_service'
        fake_serviceinstance = 'fake_service.fake_instance'
        self.fake_cluster = 'fake_cluster'
        fake_instance = 'fake_instance'
        fake_bounce_method = 'fake_bounce_method'
        fake_drain_method = mock.Mock()
        fake_drain_method.is_safe_to_kill.return_value = False
        fake_marathon_jobid = 'fake.marathon.jobid'
        fake_client = mock.create_autospec(
            marathon.MarathonClient
        )

        with contextlib.nested(
            mock.patch('paasta_tools.setup_marathon_job._log', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.create_marathon_app', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.kill_old_ids', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.send_sensu_bounce_keepalive', autospec=True),
        ) as (
            mock_log,
            mock_create_marathon_app,
            mock_kill_old_ids,
            mock_send_sensu_bounce_keepalive,
        ):
            setup_marathon_job.do_bounce(
                bounce_func=fake_bounce_func,
                drain_method=fake_drain_method,
                config=fake_config,
                new_app_running=fake_new_app_running,
                happy_new_tasks=fake_happy_new_tasks,
                old_app_live_happy_tasks=fake_old_app_live_happy_tasks,
                old_app_live_unhappy_tasks=fake_old_app_live_unhappy_tasks,
                old_app_draining_tasks=fake_old_app_draining_tasks,
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
        with contextlib.nested(
            mock.patch('paasta_tools.setup_marathon_job._log', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.marathon_tools.get_matching_apps', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.get_happy_tasks', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.drain_lib.get_drain_method', autospec=True),
        ) as (
            mock_log,
            mock_load_system_paasta_config,
            mock_get_matching_apps,
            mock_get_happy_tasks,
            mock_get_drain_method,
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
                bounce_method=fake_bounce_method,
                drain_method_name=fake_drain_method_name,
                drain_method_params=fake_drain_method_params,
                nerve_ns=fake_nerve_ns,
                bounce_health_params=fake_bounce_health_params,
                soa_dir=fake_soa_dir,
            )
            fake_client.scale_app.assert_called_once_with(
                app_id='/some_id',
                instances=5,
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
        with contextlib.nested(
            mock.patch('paasta_tools.setup_marathon_job._log', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.marathon_tools.get_matching_apps', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.get_happy_tasks', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.drain_lib.get_drain_method', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.get_bounce_method_func', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.bounce_lock_zookeeper', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.do_bounce', autospec=True),
        ) as (
            mock_log,
            mock_load_system_paasta_config,
            mock_get_matching_apps,
            mock_get_happy_tasks,
            mock_get_drain_method,
            mock_get_bounce_method_func,
            mock_bounce_lock_zookeeper,
            mock_do_bounce,
        ):
            mock_load_system_paasta_config.return_value = mock.MagicMock(
                get_cluster=mock.Mock(return_value='fake_cluster'))
            mock_get_matching_apps.return_value = [mock.Mock(id='/some_id', instances=5, tasks=range(5))]
            mock_get_happy_tasks.return_value = range(5)
            mock_get_drain_method.return_value = mock.Mock(is_draining=mock.Mock(return_value=False))
            setup_marathon_job.deploy_service(
                service=fake_service,
                instance=fake_instance,
                marathon_jobid=fake_jobid,
                config=fake_config,
                client=fake_client,
                bounce_method=fake_bounce_method,
                drain_method_name=fake_drain_method_name,
                drain_method_params=fake_drain_method_params,
                nerve_ns=fake_nerve_ns,
                bounce_health_params=fake_bounce_health_params,
                soa_dir=fake_soa_dir,
            )
            assert mock_do_bounce.call_args[1]['old_app_live_happy_tasks']['/some_id'] < set(range(5))
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
        with contextlib.nested(
            mock.patch('paasta_tools.setup_marathon_job._log', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.marathon_tools.get_matching_apps', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.get_happy_tasks', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.drain_lib.get_drain_method', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.get_bounce_method_func', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.bounce_lock_zookeeper', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.do_bounce', autospec=True),
        ) as (
            mock_log,
            mock_load_system_paasta_config,
            mock_get_matching_apps,
            mock_get_happy_tasks,
            mock_get_drain_method,
            mock_get_bounce_method_func,
            mock_bounce_lock_zookeeper,
            mock_do_bounce,
        ):
            mock_stop_draining = mock.MagicMock()

            mock_load_system_paasta_config.return_value = mock.MagicMock(
                get_cluster=mock.Mock(return_value='fake_cluster'))
            mock_get_matching_apps.return_value = [mock.Mock(id='/some_id', instances=5, tasks=range(5))]
            mock_get_happy_tasks.return_value = range(5)
            # this drain method gives us 1 healthy task (0) and 4 draining tasks (1, 2, 3, 4)
            mock_get_drain_method.return_value = mock.Mock(is_draining=lambda x: x != 0,
                                                           stop_draining=mock_stop_draining,)
            setup_marathon_job.deploy_service(
                service=fake_service,
                instance=fake_instance,
                marathon_jobid=fake_jobid,
                config=fake_config,
                client=fake_client,
                bounce_method=fake_bounce_method,
                drain_method_name=fake_drain_method_name,
                drain_method_params=fake_drain_method_params,
                nerve_ns=fake_nerve_ns,
                bounce_health_params=fake_bounce_health_params,
                soa_dir=fake_soa_dir,
            )
            assert mock_do_bounce.call_args[1]['old_app_draining_tasks']['/some_id'] < set([1, 2, 3, 4])
            assert len(mock_do_bounce.call_args[1]['old_app_draining_tasks']['/some_id']) == 2
            # we don't bounce happy tasks when draining tasks are available
            assert mock_do_bounce.call_args[1]['old_app_live_happy_tasks']['/some_id'] == set([])
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
        with contextlib.nested(
            mock.patch.object(
                self.fake_marathon_service_config,
                'format_marathon_app_dict',
                return_value=fake_complete,
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.marathon_tools.load_marathon_config',
                return_value=self.fake_marathon_config,
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.setup_marathon_job.deploy_service',
                autospec=True,
            ),
        ) as (
            format_marathon_app_dict_patch,
            get_config_patch,
            deploy_service_patch,
        ):
            setup_marathon_job.setup_service(
                service=fake_name,
                instance=fake_instance,
                client=fake_client,
                marathon_config=self.fake_marathon_config,
                service_marathon_config=self.fake_marathon_service_config,
                soa_dir=None,
            )
            format_marathon_app_dict_patch.assert_called_once_with()
            assert deploy_service_patch.call_count == 1

    def test_setup_service_srv_does_not_exist(self):
        fake_name = 'if_talk_was_cheap'
        fake_instance = 'psychatrists_would_be_broke'
        fake_response = mock.Mock(
            json=mock.Mock(return_value={'message': 'test'}))
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
        with contextlib.nested(
            mock.patch(
                'paasta_tools.setup_marathon_job.deploy_service',
                return_value=(111, 'Never'),
                autospec=True,
            ),
            mock.patch.object(
                self.fake_marathon_service_config,
                'get_bounce_method',
                return_value=fake_bounce,
                autospec=True,
            ),
            mock.patch.object(
                self.fake_marathon_service_config,
                'get_drain_method',
                return_value=fake_drain_method,
                autospec=True,
            ),
            mock.patch.object(
                self.fake_marathon_service_config,
                'get_drain_method_params',
                return_value=fake_drain_method_params,
                autospec=True,
            ),
            mock.patch.object(
                self.fake_marathon_service_config,
                'format_marathon_app_dict',
                return_value=fake_complete,
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.marathon_tools.load_marathon_service_config',
                return_value=self.fake_marathon_service_config,
                autospec=True,
            ),
            mock.patch(
                'paasta_tools.marathon_tools.load_service_namespace_config',
                return_value=self.fake_service_namespace_config,
                autospec=True,
            ),
        ) as (
            deploy_service_patch,
            get_bounce_patch,
            get_drain_method_patch,
            get_drain_method_params_patch,
            format_marathon_app_dict_patch,
            read_service_conf_patch,
            read_namespace_conf_patch,
        ):
            status, output = setup_marathon_job.setup_service(
                service=fake_name,
                instance=fake_instance,
                client=fake_client,
                marathon_config=self.fake_marathon_config,
                service_marathon_config=self.fake_marathon_service_config,
                soa_dir=None,
            )
            assert status == 111
            assert output == 'Never'

            get_bounce_patch.assert_called_once_with()
            format_marathon_app_dict_patch.assert_called_once_with()
            get_drain_method_patch.assert_called_once_with(read_namespace_conf_patch.return_value)
            deploy_service_patch.assert_called_once_with(
                service=fake_name,
                instance=fake_instance,
                marathon_jobid=full_id,
                config=fake_complete,
                client=fake_client,
                bounce_method=fake_bounce,
                drain_method_name=fake_drain_method,
                drain_method_params=fake_drain_method_params,
                nerve_ns=self.fake_marathon_service_config.get_nerve_namespace(),
                bounce_health_params=self.fake_marathon_service_config.get_bounce_health_params(
                    read_namespace_conf_patch.return_value),
                soa_dir=None,
            )

    def test_setup_service_srv_complete_config_raises(self):
        fake_name = 'test_service'
        fake_instance = 'test_instance'
        with mock.patch.object(
            self.fake_marathon_service_config,
            'format_marathon_app_dict',
            side_effect=NoDockerImageError,
        ):
            status, output = setup_marathon_job.setup_service(
                service=fake_name,
                instance=fake_instance,
                client=None,
                marathon_config=None,
                service_marathon_config=self.fake_marathon_service_config,
                soa_dir=None,
            )
            assert status == 1
            expected = 'Docker image for test_service.test_instance not in'
            assert expected in output

    def test_deploy_service_unknown_drain_method(self):
        fake_bounce = 'exists'
        fake_drain_method = 'doesntexist'
        fake_name = 'whoa'
        fake_instance = 'the_earth_is_tiny'
        fake_id = marathon_tools.format_job_id(fake_name, fake_instance)
        fake_apps = [mock.Mock(id=fake_id, tasks=[]), mock.Mock(id=('%s2' % fake_id), tasks=[])]
        fake_client = mock.MagicMock(
            list_apps=mock.Mock(return_value=fake_apps))
        fake_config = {'id': fake_id, 'instances': 2}

        errormsg = 'ERROR: drain_method not recognized: doesntexist. Must be one of (exists1, exists2)'
        expected = (1, errormsg)

        with contextlib.nested(
            mock.patch('paasta_tools.setup_marathon_job._log', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True),
            mock.patch(
                'paasta_tools.drain_lib._drain_methods',
                new={'exists1': mock.Mock(), 'exists2': mock.Mock()},
            )
        ) as (mock_log, mock_load_system_paasta_config, mock_drain_methods):
            mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
            actual = setup_marathon_job.deploy_service(
                service=fake_name,
                instance=fake_instance,
                marathon_jobid=fake_id,
                config=fake_config,
                client=fake_client,
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
        fake_apps = [mock.Mock(id=fake_id, tasks=[]), mock.Mock(id=('%s2' % fake_id), tasks=[])]
        fake_client = mock.MagicMock(
            list_apps=mock.Mock(return_value=fake_apps))
        fake_config = {'id': fake_id, 'instances': 2}

        errormsg = 'ERROR: bounce_method not recognized: %s. Must be one of (%s)' % \
            (fake_bounce, ', '.join(list_bounce_methods()))
        expected = (1, errormsg)

        with contextlib.nested(
            mock.patch('paasta_tools.setup_marathon_job._log', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True),
        ) as (mock_log, mock_load_system_paasta_config):
            mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
            actual = setup_marathon_job.deploy_service(
                service=fake_name,
                instance=fake_instance,
                marathon_jobid=fake_id,
                config=fake_config,
                client=fake_client,
                bounce_method=fake_bounce,
                drain_method_name=fake_drain_method,
                drain_method_params={},
                nerve_ns=fake_instance,
                bounce_health_params={},
                soa_dir='fake_soa_dir',
            )
            assert mock_log.call_count == 1
        assert expected == actual
        fake_client.list_apps.assert_called_once_with(embed_failures=True)
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

        fake_client = mock.MagicMock(
            list_apps=mock.Mock(return_value=[old_app]),
            kill_given_tasks=mock.Mock(spec=lambda task_ids, scale=False: None),
        )

        fake_bounce_func = mock.create_autospec(
            bounce_lib.brutal_bounce,
            return_value={
                "create_app": True,
                "tasks_to_drain": [old_task_to_drain],
            }
        )

        fake_drain_method = mock.Mock(is_draining=lambda t: t is old_task_is_draining, is_safe_to_kill=lambda t: True)

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
                side_effect=lambda x, _, __, ___, **kwargs: x.tasks,
            ),
            mock.patch('paasta_tools.bounce_lib.kill_old_ids', autospec=True),
            mock.patch('paasta_tools.bounce_lib.create_marathon_app', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job._log', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.drain_lib.get_drain_method', return_value=fake_drain_method),
        ) as (_, _, _, kill_old_ids_patch, create_marathon_app_patch, mock_log, mock_load_system_paasta_config, _):
            mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
            result = setup_marathon_job.deploy_service(
                service=fake_name,
                instance=fake_instance,
                marathon_jobid=fake_id,
                config=fake_config,
                client=fake_client,
                bounce_method=fake_bounce,
                drain_method_name=fake_drain_method_name,
                drain_method_params={},
                nerve_ns=fake_instance,
                bounce_health_params={},
                soa_dir='fake_soa_dir',
            )
            assert result[0] == 0, "Expected successful result; got (%d, %s)" % result
            fake_client.list_apps.assert_called_once_with(embed_failures=True)
            assert fake_client.create_app.call_count == 0
            fake_bounce_func.assert_called_once_with(
                new_config=fake_config,
                new_app_running=False,
                happy_new_tasks=[],
                old_app_live_happy_tasks={old_app.id: set([old_task_to_drain, old_task_dont_drain])},
                old_app_live_unhappy_tasks={old_app.id: set()},
            )

            assert fake_drain_method.drain.call_count == 2
            fake_drain_method.drain.assert_any_call(old_task_is_draining)
            fake_drain_method.drain.assert_any_call(old_task_to_drain)

            assert fake_client.kill_given_tasks.call_count == 1
            assert set([old_task_to_drain.id, old_task_is_draining.id]) == set(
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

    def test_deploy_service_already_bouncing(self):
        fake_bounce = 'areallygoodbouncestrategy'
        fake_drain_method = 'noop'
        fake_name = 'how_many_strings'
        fake_instance = 'will_i_need_to_think_of'
        fake_id = marathon_tools.format_job_id(fake_name, fake_instance, 'gityourmom', 'configyourdad')
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
                "tasks_to_drain": [old_task],
            }
        )

        fake_short_id = marathon_tools.format_job_id(fake_name, fake_instance)

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
                side_effect=lambda x, _, __, **kwargs: x.tasks,
            ),
            mock.patch('paasta_tools.setup_marathon_job._log', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True),
        ) as (_, _, _, _, mock_load_system_paasta_config):
            mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
            result = setup_marathon_job.deploy_service(
                service=fake_name,
                instance=fake_instance,
                marathon_jobid=fake_id,
                config=fake_config,
                client=fake_client,
                bounce_method=fake_bounce,
                drain_method_name=fake_drain_method,
                drain_method_params={},
                nerve_ns=fake_instance,
                bounce_health_params={},
                soa_dir='fake_soa_dir',
            )
            assert result == (1, "Instance %s is already being bounced." % fake_short_id)

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

        with contextlib.nested(
            mock.patch('paasta_tools.setup_marathon_job._log', autospec=True),
            mock.patch('paasta_tools.setup_marathon_job.bounce_lib.get_bounce_method_func', side_effect=IOError('foo')),
            mock.patch('paasta_tools.setup_marathon_job.load_system_paasta_config', autospec=True),
        ) as (mock_log, mock_bounce, mock_load_system_paasta_config):
            mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
            with raises(IOError):
                setup_marathon_job.deploy_service(
                    service=fake_name,
                    instance=fake_instance,
                    marathon_jobid=fake_id,
                    config=fake_config,
                    client=fake_client,
                    bounce_method=fake_bounce,
                    drain_method_name=fake_drain_method,
                    drain_method_params={},
                    nerve_ns=fake_instance,
                    bounce_health_params={},
                    soa_dir='fake_soa_dir',
                )
            assert fake_name in mock_log.mock_calls[0][2]["line"]
            assert 'Traceback' in mock_log.mock_calls[1][2]["line"]

    def test_get_marathon_config(self):
        fake_conf = {'oh_no': 'im_a_ghost'}
        with mock.patch(
            'paasta_tools.marathon_tools.load_marathon_config',
            return_value=fake_conf,
            autospec=True
        ) as get_conf_patch:
            assert setup_marathon_job.get_main_marathon_config() == fake_conf
            get_conf_patch.assert_called_once_with()


class TestGetOldHappyUnhappyDrainingTasks(object):
    def fake_task(self, state, happiness):
        return mock.Mock(_drain_state=state, _happiness=happiness)

    def fake_drain_method(self):
        return mock.Mock(is_draining=lambda t: t._drain_state == 'down')

    def fake_get_happy_tasks(self, app, service, nerve_ns, system_paasta_config, **kwargs):
        return [t for t in app.tasks if t._happiness == 'happy']

    def test_get_old_happy_unhappy_draining_tasks_empty(self):
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

        with mock.patch('paasta_tools.bounce_lib.get_happy_tasks', side_effect=self.fake_get_happy_tasks):
            actual = setup_marathon_job.get_old_happy_unhappy_draining_tasks(
                fake_apps,
                self.fake_drain_method(),
                service=fake_name,
                nerve_ns=fake_instance,
                bounce_health_params={},
                system_paasta_config=fake_system_paasta_config,
            )
        actual_live_happy_tasks, actual_live_unhappy_tasks, actual_draining_tasks = actual
        assert actual_live_happy_tasks == expected_live_happy_tasks
        assert actual_live_unhappy_tasks == expected_live_unhappy_tasks
        assert actual_draining_tasks == expected_draining_tasks

    def test_get_old_happy_unhappy_draining_tasks_not_empty(self):
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
            fake_apps[0].id: set([fake_apps[0].tasks[0]]),
            fake_apps[1].id: set([fake_apps[1].tasks[0]]),
        }
        expected_live_unhappy_tasks = {
            fake_apps[0].id: set([fake_apps[0].tasks[1]]),
            fake_apps[1].id: set([fake_apps[1].tasks[1]]),
        }
        expected_draining_tasks = {
            fake_apps[0].id: set([fake_apps[0].tasks[2]]),
            fake_apps[1].id: set([fake_apps[1].tasks[2]]),
        }

        with mock.patch('paasta_tools.bounce_lib.get_happy_tasks', side_effect=self.fake_get_happy_tasks):
            actual = setup_marathon_job.get_old_happy_unhappy_draining_tasks(
                fake_apps,
                self.fake_drain_method(),
                service=fake_name,
                nerve_ns=fake_instance,
                bounce_health_params={},
                system_paasta_config=fake_system_paasta_config,
            )
        actual_live_happy_tasks, actual_live_unhappy_tasks, actual_draining_tasks = actual
        assert actual_live_happy_tasks == expected_live_happy_tasks
        assert actual_live_unhappy_tasks == expected_live_unhappy_tasks
        assert actual_draining_tasks == expected_draining_tasks
