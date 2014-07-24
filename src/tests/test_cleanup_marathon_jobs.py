#!/usr/bin/env python

import cleanup_marathon_jobs
import mock
import contextlib


class TestCleanupMarathonJobs:

    cleanup_marathon_jobs.log = mock.Mock()
    fake_docker_registry = 'http://del.icio.us/'
    fake_marathon_config = {
        'cluster': 'mess',
        'url': 'http://mess_url',
        'user': 'namnin',
        'pass': 'pass_nememim',
        'docker_registry': fake_docker_registry,
        'docker_options': ['-v', 'vvvvvv'],
    }
    fake_marathon_client = mock.Mock()

    def test_main(self):
        soa_dir = 'paasta_maaaachine'
        fake_args = mock.Mock(verbose=False, soa_dir=soa_dir)
        with contextlib.nested(
            mock.patch('cleanup_marathon_jobs.parse_args', return_value=fake_args),
            mock.patch('service_deployment_tools.marathon_tools.get_config',
                       return_value=self.fake_marathon_config),
            mock.patch('cleanup_marathon_jobs.get_marathon_client', return_value=self.fake_marathon_client),
            mock.patch('cleanup_marathon_jobs.cleanup_apps')
        ) as (
            args_patch,
            config_patch,
            client_patch,
            cleanup_patch
        ):
            cleanup_marathon_jobs.main()
            args_patch.assert_called_once_with()
            config_patch.assert_called_once_with()
            client_patch.assert_called_once_with(self.fake_marathon_config['url'],
                                                 self.fake_marathon_config['user'],
                                                 self.fake_marathon_config['pass'])
            cleanup_patch.assert_called_once_with(self.fake_marathon_client, soa_dir)

    def test_cleanup_apps(self):
        from contextlib import contextmanager
        soa_dir = 'not_really_a_dir'
        fake_app_list = [('fake-app', 'one', '1'), ('really-fake', 'two', '2')]
        fake_app_ids = [mock.Mock(id='fake-app.one.1'), mock.Mock(id='really-fake.two.2'),
                        mock.Mock(id='not-here.oh.no')]
        self.fake_marathon_client.list_apps = mock.Mock(return_value=fake_app_ids)
        with contextlib.nested(
            mock.patch('service_deployment_tools.marathon_tools.get_marathon_services_for_cluster',
                       return_value=fake_app_list),
            mock.patch('service_deployment_tools.marathon_tools.compose_job_id',
                       side_effect=lambda a, b, c: '%s.%s.%s' % (a, b, c)),
            mock.patch('service_deployment_tools.marathon_tools.remove_iteration_from_job_id',
                       return_value='a_location'),
            mock.patch('service_deployment_tools.marathon_tools.bounce_lock', spec=contextmanager)
        ) as (
            get_srvs_patch,
            compose_patch,
            remove_patch,
            bounce_patch
        ):
            cleanup_marathon_jobs.cleanup_apps(self.fake_marathon_client, soa_dir)
            get_srvs_patch.assert_called_once_with(soa_dir=soa_dir, include_iteration=True)
            compose_patch.assert_any_call('fake-app', 'one', '1')
            compose_patch.assert_any_call('really-fake', 'two', '2')
            assert compose_patch.call_count == 2
            remove_patch.assert_called_once_with('not-here.oh.no')
            bounce_patch.assert_called_once_with('a_location')

    def test_get_marathon_client(self):
        with mock.patch('cleanup_marathon_jobs.MarathonClient', return_value='tt1') as client_patch:
            actual = cleanup_marathon_jobs.get_marathon_client('a1', 'b2', 'c3')
            assert actual == 'tt1'
            client_patch.assert_called_once_with('a1', 'b2', 'c3')
