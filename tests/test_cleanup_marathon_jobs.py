#!/usr/bin/env python
import cleanup_marathon_jobs
import mock
import contextlib

from pytest import raises


class TestCleanupMarathonJobs:

    cleanup_marathon_jobs.log = mock.Mock()
    fake_docker_registry = 'http://del.icio.us/'
    fake_marathon_config = {
        'cluster': 'mess',
        'url': 'http://mess_url',
        'user': 'namnin',
        'pass': 'pass_nememim',
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
    fake_marathon_client = mock.Mock()

    def test_main(self):
        soa_dir = 'paasta_maaaachine'
        fake_args = mock.Mock(verbose=False, soa_dir=soa_dir)
        with contextlib.nested(
            mock.patch('cleanup_marathon_jobs.parse_args', return_value=fake_args),
            mock.patch('cleanup_marathon_jobs.cleanup_apps')
        ) as (
            args_patch,
            cleanup_patch
        ):
            cleanup_marathon_jobs.main()
            args_patch.assert_called_once_with()
            cleanup_patch.assert_called_once_with(soa_dir)

    def test_cleanup_apps(self):
        soa_dir = 'not_really_a_dir'
        expected_apps = [('present', 'away'), ('on-app', 'off')]
        fake_app_ids = [mock.Mock(id='present.away.gone'), mock.Mock(id='on-app.off.stop'),
                        mock.Mock(id='not-here.oh.no')]
        self.fake_marathon_client.list_apps = mock.Mock(return_value=fake_app_ids)
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_marathon_services_for_cluster',
                       return_value=expected_apps, autospec=True),
            mock.patch('paasta_tools.marathon_tools.load_marathon_config',
                       autospec=True,
                       return_value=self.fake_marathon_config),
            mock.patch('paasta_tools.marathon_tools.get_marathon_client', autospec=True,
                       return_value=self.fake_marathon_client),
            mock.patch('cleanup_marathon_jobs.delete_app', autospec=True),
        ) as (
            get_marathon_services_for_cluster_patch,
            config_patch,
            client_patch,
            delete_patch,
        ):
            cleanup_marathon_jobs.cleanup_apps(soa_dir)
            config_patch.assert_called_once_with()
            get_marathon_services_for_cluster_patch.assert_called_once_with(soa_dir=soa_dir)
            client_patch.assert_called_once_with(self.fake_marathon_config['url'],
                                                 self.fake_marathon_config['user'],
                                                 self.fake_marathon_config['pass'])
            delete_patch.assert_called_once_with('not-here.oh.no', self.fake_marathon_client)

    def test_cleanup_apps_doesnt_delete_unknown_apps(self):
        soa_dir = 'not_really_a_dir'
        expected_apps = [('present', 'away'), ('on-app', 'off')]
        fake_app_ids = [mock.Mock(id='non_conforming_app')]
        self.fake_marathon_client.list_apps = mock.Mock(return_value=fake_app_ids)
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_marathon_services_for_cluster',
                       return_value=expected_apps, autospec=True),
            mock.patch('paasta_tools.marathon_tools.load_marathon_config',
                       autospec=True,
                       return_value=self.fake_marathon_config),
            mock.patch('paasta_tools.marathon_tools.get_marathon_client', autospec=True,
                       return_value=self.fake_marathon_client),
            mock.patch('cleanup_marathon_jobs.delete_app', autospec=True),
        ) as (
            get_marathon_services_for_cluster_patch,
            config_patch,
            client_patch,
            delete_patch,
        ):
            cleanup_marathon_jobs.cleanup_apps(soa_dir)
            assert delete_patch.call_count == 0

    def test_delete_app(self):
        app_id = 'example--service.main.git93340779.configddb38a65'
        client = self.fake_marathon_client
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_cluster', autospec=True),
            mock.patch('paasta_tools.bounce_lib.bounce_lock_zookeeper', autospec=True),
            mock.patch('paasta_tools.bounce_lib.delete_marathon_app', autospec=True),
            mock.patch('cleanup_marathon_jobs._log', autospec=True),
        ) as (
            mock_get_cluster,
            mock_bounce_lock_zookeeper,
            mock_delete_marathon_app,
            mock_log,
        ):
            mock_get_cluster.return_value = 'fake_cluster'
            cleanup_marathon_jobs.delete_app(app_id, client)
            mock_delete_marathon_app.assert_called_once_with(app_id, client)
            mock_get_cluster.assert_called_once_with()
            expected_log_line = (
                'Deleted stale marathon job that looks lost: ' +
                app_id
            )
            mock_log.assert_called_once_with(
                instance='main',
                service_name='example_service',
                level='event',
                component='deploy',
                cluster='fake_cluster',
                line=expected_log_line,
            )

    def test_delete_app_throws_exception(self):
        app_id = 'example--service.main.git93340779.configddb38a65'
        client = self.fake_marathon_client

        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_cluster', return_value='fake_cluster', autospec=True),
            mock.patch('paasta_tools.bounce_lib.bounce_lock_zookeeper', autospec=True),
            mock.patch('paasta_tools.bounce_lib.delete_marathon_app', side_effect=ValueError('foo')),
            mock.patch('cleanup_marathon_jobs._log', autospec=True),
        ) as (
            mock_get_cluster,
            mock_bounce_lock_zookeeper,
            mock_delete_marathon_app,
            mock_log,
        ):
            with raises(ValueError):
                cleanup_marathon_jobs.delete_app(app_id, client)
            assert 'Traceback' in mock_log.mock_calls[0][2]["line"]


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
