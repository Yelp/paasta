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
        from contextlib import contextmanager
        soa_dir = 'not_really_a_dir'
        expected_apps = [('present', 'away'), ('on-app', 'off')]
        fake_app_ids = [mock.Mock(id='present.away.gone'), mock.Mock(id='on-app.off.stop'),
                        mock.Mock(id='not-here.oh.no')]
        self.fake_marathon_client.list_apps = mock.Mock(return_value=fake_app_ids)
        with contextlib.nested(
            mock.patch(
                'paasta_tools.marathon_tools.get_marathon_services_for_cluster',
                return_value=expected_apps
            ),
            mock.patch('paasta_tools.marathon_tools.MarathonConfig.read',
                       return_value=self.fake_marathon_config),
            mock.patch('paasta_tools.bounce_lib.bounce_lock_zookeeper', spec=contextmanager),
            mock.patch('paasta_tools.marathon_tools.get_marathon_client', return_value=self.fake_marathon_client),
            mock.patch('paasta_tools.bounce_lib.delete_marathon_app'),
        ) as (
            get_marathon_services_for_cluster_patch,
            config_patch,
            bounce_patch,
            client_patch,
            delete_patch,
        ):
            cleanup_marathon_jobs.cleanup_apps(soa_dir)
            config_patch.assert_called_once_with()
            get_marathon_services_for_cluster_patch.assert_called_once_with(soa_dir=soa_dir)
            client_patch.assert_called_once_with(self.fake_marathon_config['url'],
                                                 self.fake_marathon_config['user'],
                                                 self.fake_marathon_config['pass'])
            bounce_patch.assert_called_once_with('not-here.oh')
            delete_patch.assert_called_once_with('not-here.oh.no', self.fake_marathon_client)

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
