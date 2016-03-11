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
import copy

import mock
from pysensu_yelp import Status
from pytest import raises

from paasta_tools import chronos_tools
from paasta_tools import setup_chronos_job
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import NoDeploymentsAvailable


class TestSetupChronosJob:

    fake_docker_image = 'test_docker:1.0'
    fake_client = mock.MagicMock()

    fake_service = 'test_service'
    fake_instance = 'test'
    fake_cluster = 'fake_test_cluster'
    fake_config_dict = {
        'name': 'test_service test gitsha config',
        'description': 'This is a test Chronos job.',
        'command': '/bin/sleep 40',
        'bounce_method': 'graceful',
        'epsilon': 'PT30M',
        'retries': 5,
        'owner': 'test@test.com',
        'async': False,
        'cpus': 5.5,
        'mem': 1024.4,
        'disk': 2048.5,
        'disabled': 'true',
        'schedule': 'R/2015-03-25T19:36:35Z/PT5M',
        'schedule_time_zone': 'Zulu',
    }
    fake_branch_dict = {
        'docker_image': 'paasta-%s-%s' % (fake_service, fake_cluster),
    }
    fake_chronos_job_config = chronos_tools.ChronosJobConfig(
        service=fake_service,
        cluster=fake_cluster,
        instance=fake_instance,
        config_dict=fake_config_dict,
        branch_dict=fake_branch_dict,
    )

    fake_docker_registry = 'remote_registry.com'
    fake_args = mock.MagicMock(
        service_instance=compose_job_id(fake_service, fake_instance),
        soa_dir='no_more',
        verbose=False,
    )

    def test_main_success(self):
        expected_status = 0
        expected_output = 'it_is_finished'
        fake_complete_job_config = {'foo': 'bar'}
        with contextlib.nested(
            mock.patch('paasta_tools.setup_chronos_job.parse_args',
                       return_value=self.fake_args,
                       autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_config', autospec=True),
            mock.patch('paasta_tools.chronos_tools.get_chronos_client',
                       return_value=self.fake_client,
                       autospec=True),
            mock.patch('paasta_tools.chronos_tools.create_complete_config',
                       return_value=fake_complete_job_config,
                       autospec=True),
            mock.patch('paasta_tools.setup_chronos_job.setup_job',
                       return_value=(expected_status, expected_output),
                       autospec=True),
            mock.patch('paasta_tools.setup_chronos_job.send_event', autospec=True),
            mock.patch('paasta_tools.setup_chronos_job.load_system_paasta_config', autospec=True),
            mock.patch('sys.exit', autospec=True),
        ) as (
            parse_args_patch,
            load_chronos_config_patch,
            get_client_patch,
            create_complete_config_patch,
            setup_job_patch,
            send_event_patch,
            load_system_paasta_config_patch,
            sys_exit_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster = mock.MagicMock(return_value=self.fake_cluster)
            setup_chronos_job.main()

            parse_args_patch.assert_called_once_with()
            get_client_patch.assert_called_once_with(load_chronos_config_patch.return_value)
            setup_job_patch.assert_called_once_with(
                service=self.fake_service,
                instance=self.fake_instance,
                complete_job_config=fake_complete_job_config,
                client=self.fake_client,
                cluster=self.fake_cluster,
            )
            send_event_patch.assert_called_once_with(
                service=self.fake_service,
                instance=self.fake_instance,
                soa_dir=self.fake_args.soa_dir,
                status=expected_status,
                output=expected_output,
            )
            sys_exit_patch.assert_called_once_with(0)

    def test_main_no_deployments(self):
        with contextlib.nested(
            mock.patch('paasta_tools.setup_chronos_job.parse_args',
                       return_value=self.fake_args,
                       autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_config', autospec=True),
            mock.patch('paasta_tools.chronos_tools.get_chronos_client',
                       return_value=self.fake_client,
                       autospec=True),
            mock.patch('paasta_tools.chronos_tools.create_complete_config',
                       return_value={},
                       autospec=True,
                       side_effect=NoDeploymentsAvailable),
            mock.patch('paasta_tools.setup_chronos_job.setup_job',
                       return_value=(0, 'it_is_finished'),
                       autospec=True),
            mock.patch('paasta_tools.setup_chronos_job.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.setup_chronos_job.send_event', autospec=True),
        ) as (
            parse_args_patch,
            load_chronos_config_patch,
            get_client_patch,
            load_chronos_job_config_patch,
            setup_job_patch,
            load_system_paasta_config_patch,
            send_event_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster = mock.MagicMock(return_value=self.fake_cluster)
            with raises(SystemExit) as excinfo:
                setup_chronos_job.main()
            assert excinfo.value.code == 0

    def test_main_bad_chronos_job_config_notifies_user(self):
        with contextlib.nested(
            mock.patch('paasta_tools.setup_chronos_job.parse_args',
                       return_value=self.fake_args,
                       autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_config', autospec=True),
            mock.patch('paasta_tools.chronos_tools.get_chronos_client',
                       return_value=self.fake_client,
                       autospec=True),
            mock.patch('paasta_tools.chronos_tools.create_complete_config',
                       autospec=True,
                       side_effect=chronos_tools.UnknownChronosJobError('test bad configuration')),
            mock.patch('paasta_tools.setup_chronos_job.setup_job',
                       return_value=(0, 'it_is_finished'),
                       autospec=True),
            mock.patch('paasta_tools.setup_chronos_job.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.setup_chronos_job.send_event', autospec=True),
        ) as (
            parse_args_patch,
            load_chronos_config_patch,
            get_client_patch,
            load_chronos_job_config_patch,
            setup_job_patch,
            load_system_paasta_config_patch,
            send_event_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster = mock.MagicMock(return_value=self.fake_cluster)
            with raises(SystemExit) as excinfo:
                setup_chronos_job.main()
            assert excinfo.value.code == 0
            expected_error_msg = (
                "Could not read chronos configuration file for %s in cluster %s\nError was: test bad configuration"
                % (compose_job_id(self.fake_service, self.fake_instance), self.fake_cluster)
            )
            send_event_patch.assert_called_once_with(
                service=self.fake_service,
                instance=self.fake_instance,
                soa_dir=self.fake_args.soa_dir,
                status=Status.CRITICAL,
                output=expected_error_msg
            )

    def test_setup_job_new_app_with_no_previous_jobs(self):
        fake_existing_jobs = []
        with contextlib.nested(
            mock.patch('paasta_tools.setup_chronos_job.bounce_chronos_job', autospec=True, return_value=(0, 'ok')),
            mock.patch('paasta_tools.chronos_tools.lookup_chronos_jobs',
                       autospec=True),
            mock.patch('paasta_tools.chronos_tools.sort_jobs',
                       autospec=True,
                       return_value=fake_existing_jobs),
            mock.patch('paasta_tools.chronos_tools.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_job_config',
                       autospec=True,
                       return_value=self.fake_chronos_job_config),
        ) as (
            mock_bounce_chronos_job,
            lookup_chronos_jobs_patch,
            sort_jobs_patch,
            load_system_paasta_config_patch,
            load_chronos_job_config_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster.return_value = self.fake_cluster
            load_system_paasta_config_patch.return_value.get_volumes.return_value = []
            complete_config = chronos_tools.create_complete_config(
                service=self.fake_service,
                job_name=self.fake_instance,
                soa_dir=self.fake_args.soa_dir,
            )
            actual = setup_chronos_job.setup_job(
                service=self.fake_service,
                instance=self.fake_instance,
                complete_job_config=complete_config,
                client=self.fake_client,
                cluster=self.fake_cluster,
            )
            mock_bounce_chronos_job.assert_called_once_with(
                service=self.fake_service,
                instance=self.fake_instance,
                cluster=self.fake_cluster,
                job_to_update=complete_config,
                client=self.fake_client,
            )
            assert actual == mock_bounce_chronos_job.return_value

    def test_setup_job_with_previously_enabled_job(self):
        fake_existing_job = {
            'name': 'fake_job',
            'disabled': False,
        }
        with contextlib.nested(
            mock.patch('paasta_tools.setup_chronos_job.bounce_chronos_job', autospec=True, return_value=(0, 'ok')),
            mock.patch('paasta_tools.chronos_tools.lookup_chronos_jobs',
                       autospec=True),
            mock.patch('paasta_tools.chronos_tools.sort_jobs',
                       autospec=True,
                       return_value=[fake_existing_job]),
            mock.patch('paasta_tools.chronos_tools.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_job_config',
                       autospec=True, return_value=self.fake_chronos_job_config),
        ) as (
            mock_bounce_chronos_job,
            mock_lookup_chronos_jobs,
            mock_sort_jobs,
            load_system_paasta_config_patch,
            load_chronos_job_config_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster.return_value = self.fake_cluster
            load_system_paasta_config_patch.return_value.get_volumes.return_value = []
            complete_config = chronos_tools.create_complete_config(
                service=self.fake_service,
                job_name=self.fake_instance,
                soa_dir=self.fake_args.soa_dir
            )
            actual = setup_chronos_job.setup_job(
                service=self.fake_service,
                instance=self.fake_instance,
                complete_job_config=complete_config,
                client=self.fake_client,
                cluster=self.fake_cluster,
            )
            mock_bounce_chronos_job.assert_called_once_with(
                service=self.fake_service,
                instance=self.fake_instance,
                cluster=self.fake_cluster,
                job_to_update=complete_config,
                client=self.fake_client,
            )
            assert mock_lookup_chronos_jobs.called
            assert actual == mock_bounce_chronos_job.return_value

    def test_setup_job_does_nothing_with_only_existing_app(self):
        fake_existing_job = copy.deepcopy(self.fake_config_dict)
        with contextlib.nested(
            mock.patch('paasta_tools.setup_chronos_job.bounce_chronos_job', autospec=True, return_value=(0, 'ok')),
            mock.patch('paasta_tools.chronos_tools.lookup_chronos_jobs',
                       autospec=True, return_value=[fake_existing_job]),
            mock.patch('paasta_tools.chronos_tools.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_job_config',
                       autospec=True, return_value=self.fake_chronos_job_config),
        ) as (
            mock_bounce_chronos_job,
            mock_lookup_chronos_jobs,
            load_system_paasta_config_patch,
            load_chronos_job_config_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster.return_value = self.fake_cluster
            complete_config = copy.deepcopy(self.fake_config_dict)
            # Force the complete_config's name to match the return value of
            # lookup_chronos_jobs to simulate that they have the same name
            complete_config["name"] = fake_existing_job["name"]
            actual = setup_chronos_job.setup_job(
                service=self.fake_service,
                instance=self.fake_instance,
                complete_job_config=complete_config,
                client=self.fake_client,
                cluster=self.fake_cluster,
            )
            mock_bounce_chronos_job.assert_called_once_with(
                service=self.fake_service,
                instance=self.fake_instance,
                cluster=self.fake_cluster,
                job_to_update=None,
                client=self.fake_client,
            )
            assert mock_lookup_chronos_jobs.called
            assert actual == mock_bounce_chronos_job.return_value

    def test_send_event(self):
        fake_status = '42'
        fake_output = 'something went wrong'
        fake_soa_dir = ''
        expected_check_name = 'setup_chronos_job.%s' % compose_job_id(self.fake_service, self.fake_instance)
        with contextlib.nested(
            mock.patch("paasta_tools.monitoring_tools.send_event", autospec=True),
            mock.patch("paasta_tools.chronos_tools.load_chronos_job_config", autospec=True),
            mock.patch("paasta_tools.setup_chronos_job.load_system_paasta_config", autospec=True),
        ) as (
            mock_send_event,
            mock_load_chronos_job_config,
            mock_load_system_paasta_config,
        ):
            mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
            mock_load_chronos_job_config.return_value.get_monitoring.return_value = {}

            setup_chronos_job.send_event(
                service=self.fake_service,
                instance=self.fake_instance,
                soa_dir=fake_soa_dir,
                status=fake_status,
                output=fake_output,
            )
            mock_send_event.assert_called_once_with(
                service=self.fake_service,
                check_name=expected_check_name,
                overrides={'alert_after': '10m', 'check_every': '10s'},
                status=fake_status,
                output=fake_output,
                soa_dir=fake_soa_dir
            )
            mock_load_chronos_job_config.assert_called_once_with(
                service=self.fake_service,
                instance=self.fake_instance,
                cluster=mock_load_system_paasta_config.return_value.get_cluster.return_value,
                soa_dir=fake_soa_dir,
            )

    def test_bounce_chronos_job_takes_actions(self):
        fake_job_to_update = {'name': 'job_to_update'}
        with contextlib.nested(
            mock.patch("paasta_tools.setup_chronos_job._log", autospec=True),
            mock.patch("paasta_tools.chronos_tools.update_job", autospec=True),
        ) as (
            mock_log,
            mock_update_job,
        ):
            setup_chronos_job.bounce_chronos_job(
                service=self.fake_service,
                instance=self.fake_instance,
                cluster=self.fake_cluster,
                job_to_update=fake_job_to_update,
                client=self.fake_client,
            )
            mock_log.assert_any_call(
                line=mock.ANY,
                level='debug',
                instance=self.fake_instance,
                cluster=self.fake_cluster,
                component='deploy',
                service=self.fake_service,
            )
            mock_log.assert_any_call(
                line="Updated Chronos job: job_to_update",
                level='event',
                instance=self.fake_instance,
                cluster=self.fake_cluster,
                component='deploy',
                service=self.fake_service,
            )
            mock_update_job.assert_called_once_with(job=fake_job_to_update, client=self.fake_client)

    def test_bounce_chronos_job_doesnt_log_when_nothing_to_do(self):
        with contextlib.nested(
            mock.patch("paasta_tools.setup_chronos_job._log", autospec=True),
            mock.patch("paasta_tools.chronos_tools.update_job", autospec=True),
        ) as (
            mock_log,
            mock_update_job,
        ):
            setup_chronos_job.bounce_chronos_job(
                service=self.fake_service,
                instance=self.fake_instance,
                cluster=self.fake_cluster,
                job_to_update=None,
                client=self.fake_client,
            )
            assert not mock_log.called
            assert not mock_update_job.called
