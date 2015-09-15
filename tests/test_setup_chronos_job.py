#!/usr/bin/env python

import contextlib
import mock

from pysensu_yelp import Status
from pytest import raises

from paasta_tools import chronos_tools
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import compose_job_id
import setup_chronos_job


class TestSetupChronosJob:

    fake_docker_image = 'test_docker:1.0'
    fake_cluster = 'fake_test_cluster'

    fake_service_name = 'test_service'
    fake_job_name = 'test'
    fake_cluster = 'penguin'
    fake_config_dict = {
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
        'docker_image': 'paasta-%s-%s' % (fake_service_name, fake_cluster),
    }
    fake_chronos_job_config = chronos_tools.ChronosJobConfig(fake_service_name,
                                                             fake_job_name,
                                                             fake_config_dict,
                                                             fake_branch_dict)

    fake_docker_registry = 'remote_registry.com'
    fake_args = mock.MagicMock(
        service_instance=compose_job_id(fake_service_name, fake_job_name),
        soa_dir='no_more',
        verbose=False,
    )

    def test_main_success(self):
        fake_client = mock.MagicMock()
        expected_status = 0
        expected_output = 'it_is_finished'
        fake_complete_job_config = {'foo': 'bar'}
        with contextlib.nested(
            mock.patch('setup_chronos_job.parse_args',
                       return_value=self.fake_args,
                       autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_config', autospec=True),
            mock.patch('paasta_tools.chronos_tools.get_chronos_client',
                       return_value=fake_client,
                       autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_job_config',
                       return_value=self.fake_chronos_job_config,
                       autospec=True),
            mock.patch('paasta_tools.chronos_tools.create_complete_config',
                       return_value=fake_complete_job_config,
                       autospec=True),
            mock.patch('setup_chronos_job.setup_job',
                       return_value=(expected_status, expected_output),
                       autospec=True),
            mock.patch('setup_chronos_job.send_event', autospec=True),
            mock.patch('setup_chronos_job.load_system_paasta_config', autospec=True),
            mock.patch('sys.exit', autospec=True),
        ) as (
            parse_args_patch,
            load_chronos_config_patch,
            get_client_patch,
            load_chronos_job_config_patch,
            create_complete_config_patch,
            setup_job_patch,
            send_event_patch,
            load_system_paasta_config_patch,
            sys_exit_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster.return_value = self.fake_cluster
            setup_chronos_job.main()

            parse_args_patch.assert_called_once_with()
            get_client_patch.assert_called_once_with(load_chronos_config_patch.return_value)
            load_chronos_job_config_patch.assert_called_once_with(
                self.fake_service_name,
                self.fake_job_name,
                self.fake_cluster,
                soa_dir=self.fake_args.soa_dir,
            )
            setup_job_patch.assert_called_once_with(
                self.fake_service_name,
                self.fake_job_name,
                self.fake_chronos_job_config,
                fake_complete_job_config,
                fake_client,
                self.fake_cluster,
            )
            send_event_patch.assert_called_once_with(
                self.fake_service_name,
                self.fake_job_name,
                self.fake_args.soa_dir,
                expected_status,
                expected_output,
            )
            sys_exit_patch.assert_called_once_with(0)

    def test_main_no_deployments(self):
        fake_client = mock.MagicMock()
        with contextlib.nested(
            mock.patch('setup_chronos_job.parse_args',
                       return_value=self.fake_args,
                       autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_config', autospec=True),
            mock.patch('paasta_tools.chronos_tools.get_chronos_client',
                       return_value=fake_client,
                       autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_job_config',
                       return_value=self.fake_chronos_job_config,
                       autospec=True,
                       side_effect=NoDeploymentsAvailable),
            mock.patch('setup_chronos_job.setup_job',
                       return_value=(0, 'it_is_finished'),
                       autospec=True),
            mock.patch('setup_chronos_job.load_system_paasta_config', autospec=True),
            mock.patch('setup_chronos_job.send_event', autospec=True),
        ) as (
            parse_args_patch,
            load_chronos_config_patch,
            get_client_patch,
            load_chronos_job_config_patch,
            setup_job_patch,
            load_system_paasta_config_patch,
            send_event_patch
        ):
            load_system_paasta_config_patch.return_value.get_cluster = mock.MagicMock(return_value=self.fake_cluster)
            with raises(SystemExit) as excinfo:
                setup_chronos_job.main()
            assert excinfo.value.code == 0

    def test_main_bad_chronos_job_config_notifies_user(self):
        fake_client = mock.MagicMock()
        with contextlib.nested(
            mock.patch('setup_chronos_job.parse_args',
                       return_value=self.fake_args,
                       autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_config', autospec=True),
            mock.patch('paasta_tools.chronos_tools.get_chronos_client',
                       return_value=fake_client,
                       autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_job_config',
                       return_value=self.fake_chronos_job_config,
                       autospec=True,
                       side_effect=chronos_tools.InvalidChronosConfigError('test bad configuration')),
            mock.patch('setup_chronos_job.setup_job',
                       return_value=(0, 'it_is_finished'),
                       autospec=True),
            mock.patch('setup_chronos_job.load_system_paasta_config', autospec=True),
            mock.patch('setup_chronos_job.send_event', autospec=True),
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
                % (compose_job_id(self.fake_service_name, self.fake_job_name), self.fake_cluster)
            )
            send_event_patch.assert_called_once_with(
                self.fake_service_name,
                self.fake_job_name,
                self.fake_args.soa_dir,
                Status.CRITICAL,
                expected_error_msg
            )

    def test_setup_job_new_app(self):
        fake_client = mock.MagicMock()
        fake_existing_jobs = []
        with contextlib.nested(
            mock.patch('setup_chronos_job._setup_new_job', autospec=True, return_value=(0, 'ok')),
            mock.patch('paasta_tools.chronos_tools.lookup_chronos_jobs',
                       autospec=True,
                       return_value=fake_existing_jobs),
            mock.patch('paasta_tools.chronos_tools.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_job_config',
                       autospec=True,
                       return_value=self.fake_chronos_job_config),
        ) as (
            setup_new_job_patch,
            lookup_chronos_jobs_patch,
            load_system_paasta_config_patch,
            load_chronos_job_config_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster.return_value = self.fake_cluster
            complete_config = chronos_tools.create_complete_config(
                self.fake_service_name,
                self.fake_job_name,
                self.fake_args.soa_dir
            )
            actual = setup_chronos_job.setup_job(
                self.fake_service_name,
                self.fake_job_name,
                self.fake_chronos_job_config,
                complete_config,
                fake_client,
                self.fake_cluster,
            )
            setup_new_job_patch.assert_called_once_with(
                complete_config['name'],
                fake_existing_jobs,
                complete_config,
                fake_client
            )
            assert actual == setup_new_job_patch.return_value

    def test_setup_job_new_app_with_previous(self):
        fake_client = mock.MagicMock()
        fake_existing_job = {
            'name': 'fake_job'
        }
        with contextlib.nested(
            mock.patch('setup_chronos_job._setup_existing_job', autospec=True, return_value=(0, 'ok')),
            mock.patch('paasta_tools.chronos_tools.lookup_chronos_jobs',
                       autospec=True, return_value=[fake_existing_job]),
            mock.patch('paasta_tools.chronos_tools.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_job_config',
                       autospec=True, return_value=self.fake_chronos_job_config),
        ) as (
            setup_existing_job_patch,
            lookup_chronos_jobs_patch,
            load_system_paasta_config_patch,
            load_chronos_job_config_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster.return_value = self.fake_cluster
            complete_config = chronos_tools.create_complete_config(
                self.fake_service_name,
                self.fake_job_name,
                self.fake_args.soa_dir
            )
            actual = setup_chronos_job.setup_job(
                self.fake_service_name,
                self.fake_job_name,
                self.fake_chronos_job_config,
                complete_config,
                fake_client,
                self.fake_cluster,
            )
            setup_existing_job_patch.assert_called_once_with(
                complete_config['name'],
                fake_existing_job,
                complete_config,
                fake_client
            )
            assert actual == setup_existing_job_patch.return_value

    def test_setup_job_brutal_bounce(self):
        fake_client = mock.MagicMock()
        fake_existing_jobs = []
        with contextlib.nested(
            mock.patch('setup_chronos_job.restart_chronos_job', autospec=True),
            mock.patch('paasta_tools.chronos_tools.lookup_chronos_jobs',
                       autospec=True,
                       return_value=fake_existing_jobs),
            mock.patch('paasta_tools.chronos_tools.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_job_config',
                       autospec=True,
                       return_value=self.fake_chronos_job_config),
            mock.patch('setup_chronos_job.chronos_tools.ChronosJobConfig.get_bounce_method',
                       autospec=True,
                       return_value='brutal'),
        ) as (
            restart_chronos_job_patch,
            lookup_chronos_jobs_patch,
            load_system_paasta_config_patch,
            load_chronos_job_config_patch,
            get_bounce_method_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster.return_value = self.fake_cluster
            complete_config = chronos_tools.create_complete_config(
                self.fake_service_name,
                self.fake_job_name,
                self.fake_args.soa_dir
            )
            actual = setup_chronos_job.setup_job(
                self.fake_service_name,
                self.fake_job_name,
                self.fake_chronos_job_config,
                complete_config,
                fake_client,
                self.fake_cluster,
            )
            restart_chronos_job_patch.assert_called_once_with(
                self.fake_service_name,
                self.fake_job_name,
                complete_config['name'],
                fake_client,
                self.fake_cluster,
                fake_existing_jobs,
                complete_config
            )
            assert actual == (0, "Job '%s' bounced using the 'brutal' method" % complete_config['name'])

    def test_setup_new_job(self):
        fake_client = mock.MagicMock()
        fake_chronos_job_config = chronos_tools.ChronosJobConfig(
            self.fake_service_name,
            self.fake_job_name,
            self.fake_config_dict,
            {
                'desired_state': 'start',
                'docker_image': 'fake_image'
            }
        )
        fake_existing_jobs = []

        with contextlib.nested(
            mock.patch('paasta_tools.chronos_tools.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_job_config',
                       autospec=True, return_value=fake_chronos_job_config),
            mock.patch('paasta_tools.chronos_tools.get_code_sha_from_dockerurl', autospec=True, return_value="sha"),
            mock.patch('paasta_tools.chronos_tools.get_config_hash', autospec=True, return_value="hash"),
        ) as (
            load_system_paasta_config_patch,
            load_chronos_job_config_patch,
            code_sha_patch,
            config_hash_patch,
        ):
            fake_complete_config = chronos_tools.create_complete_config(
                self.fake_service_name,
                self.fake_job_name,
                self.fake_args.soa_dir
            )
            actual = setup_chronos_job._setup_new_job(
                fake_complete_config['name'],
                fake_existing_jobs,
                fake_complete_config,
                fake_client
            )
            assert actual == (0, "Deployed job '%s'" % fake_complete_config['name'])
            fake_client.add.assert_called_once_with(fake_complete_config)

    def test_setup_new_job_disable_old_versions(self):
        fake_client = mock.MagicMock()
        fake_existing_job = {
            'name': 'fake_job',
            'disabled': False,
            'mem': 1337,
        }
        fake_existing_job_disabled = {
            'name': 'fake_job',
            'disabled': True,
            'mem': 1337,
        }
        fake_new_job = {
            'name': 'fake_job',
            'disabled': False,
            'mem': 9001,
        }

        actual = setup_chronos_job._setup_new_job(
            fake_new_job['name'],
            [fake_existing_job],
            fake_new_job,
            fake_client,
        )
        assert fake_existing_job['disabled'] is True
        fake_client.update.assert_called_once_with(fake_existing_job_disabled)
        fake_client.add.assert_called_once_with(fake_new_job)
        assert actual == (0, "Deployed job '%s'" % fake_new_job['name'])

    def test_setup_existing_job_start(self):
        fake_client = mock.MagicMock()
        fake_state = 'start'
        fake_chronos_job_config = chronos_tools.ChronosJobConfig(
            self.fake_service_name,
            self.fake_job_name,
            self.fake_config_dict,
            {
                'desired_state': fake_state,
                'docker_image': 'fake_image'
            }
        )

        with contextlib.nested(
            mock.patch('paasta_tools.chronos_tools.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_job_config',
                       autospec=True, return_value=fake_chronos_job_config),
            mock.patch('paasta_tools.chronos_tools.get_code_sha_from_dockerurl', autospec=True, return_value="sha"),
            mock.patch('paasta_tools.chronos_tools.get_config_hash', autospec=True, return_value="hash"),
        ) as (
            load_system_paasta_config_patch,
            load_chronos_job_config_patch,
            code_sha_patch,
            config_hash_patch,
        ):
            fake_complete_config = chronos_tools.create_complete_config(
                self.fake_service_name,
                self.fake_job_name,
                self.fake_args.soa_dir
            )
            fake_existing_job = fake_complete_config.copy()
            # Simulate the job being stopped
            fake_existing_job['disabled'] = True

            actual = setup_chronos_job._setup_existing_job(
                fake_complete_config['name'],
                fake_existing_job,
                fake_complete_config,
                fake_client
            )
            assert actual == (0, "Enabled job '%s'" % fake_complete_config['name'])
            fake_client.update.assert_called_once_with(fake_complete_config)

    def test_setup_existing_job_stop(self):
        fake_client = mock.MagicMock()
        fake_state = 'stop'
        fake_chronos_job_config = chronos_tools.ChronosJobConfig(
            self.fake_service_name,
            self.fake_job_name,
            self.fake_config_dict,
            {
                'desired_state': fake_state,
                'docker_image': 'fake_image'
            }
        )

        with contextlib.nested(
            mock.patch('paasta_tools.chronos_tools.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_job_config',
                       autospec=True, return_value=fake_chronos_job_config),
            mock.patch('paasta_tools.chronos_tools.get_code_sha_from_dockerurl', autospec=True, return_value="sha"),
            mock.patch('paasta_tools.chronos_tools.get_config_hash', autospec=True, return_value="hash"),
        ) as (
            load_system_paasta_config_patch,
            load_chronos_job_config_patch,
            code_sha_patch,
            config_hash_patch,
        ):
            fake_complete_config = chronos_tools.create_complete_config(
                self.fake_service_name,
                self.fake_job_name,
                self.fake_args.soa_dir
            )
            fake_existing_job = fake_complete_config.copy()
            # Pretend the job is already running
            fake_existing_job['disabled'] = False

            actual = setup_chronos_job._setup_existing_job(
                fake_complete_config['name'],
                fake_existing_job,
                fake_complete_config,
                fake_client
            )
            assert actual == (0, "Disabled job '%s'" % fake_complete_config['name'])
            fake_client.update.assert_called_once_with(fake_complete_config)

    def test_setup_existing_job_noop(self):
        fake_client = mock.MagicMock()
        fake_state = 'start'
        fake_chronos_job_config = chronos_tools.ChronosJobConfig(
            self.fake_service_name,
            self.fake_job_name,
            self.fake_config_dict,
            {
                'desired_state': fake_state,
                'docker_image': 'fake_image'
            }
        )

        with contextlib.nested(
            mock.patch('paasta_tools.chronos_tools.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.chronos_tools.load_chronos_job_config',
                       autospec=True, return_value=fake_chronos_job_config),
            mock.patch('paasta_tools.chronos_tools.get_code_sha_from_dockerurl', autospec=True, return_value="sha"),
            mock.patch('paasta_tools.chronos_tools.get_config_hash', autospec=True, return_value="hash"),
        ) as (
            load_system_paasta_config_patch,
            load_chronos_job_config_patch,
            code_sha_patch,
            config_hash_patch,
        ):
            fake_complete_config = chronos_tools.create_complete_config(
                self.fake_service_name,
                self.fake_job_name,
                self.fake_args.soa_dir
            )
            fake_existing_job = fake_complete_config.copy()
            # Pretend the job is already running
            fake_existing_job['disabled'] = False

            actual = setup_chronos_job._setup_existing_job(
                fake_complete_config['name'],
                fake_existing_job,
                fake_complete_config,
                fake_client
            )
            assert actual == (0, "Job '%s' state is already set to '%s'" % (fake_complete_config['name'], fake_state))
            assert fake_client.update.call_count == 0
