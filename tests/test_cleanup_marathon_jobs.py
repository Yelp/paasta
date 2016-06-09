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
import contextlib

import mock
from pytest import raises

from paasta_tools import cleanup_marathon_jobs
from paasta_tools import marathon_tools


class TestCleanupMarathonJobs:

    cleanup_marathon_jobs.log = mock.Mock()
    fake_docker_registry = 'http://del.icio.us/'
    fake_cluster = 'fake_test_cluster'
    fake_marathon_config = marathon_tools.MarathonConfig({
        'url': 'http://mess_url',
        'user': 'namnin',
        'password': 'pass_nememim',
    })
    fake_marathon_client = mock.Mock()

    def test_main(self):
        soa_dir = 'paasta_maaaachine'
        fake_args = mock.Mock(verbose=False, soa_dir=soa_dir)
        with contextlib.nested(
            mock.patch('paasta_tools.cleanup_marathon_jobs.parse_args', return_value=fake_args),
            mock.patch('paasta_tools.cleanup_marathon_jobs.cleanup_apps')
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
        fake_app_ids = [mock.Mock(id='present.away.gone.wtf'), mock.Mock(id='on-app.off.stop.jrm'),
                        mock.Mock(id='not-here.oh.no.weirdo')]
        self.fake_marathon_client.list_apps = mock.Mock(return_value=fake_app_ids)
        with contextlib.nested(
            mock.patch('paasta_tools.cleanup_marathon_jobs.get_services_for_cluster',
                       return_value=expected_apps, autospec=True),
            mock.patch('paasta_tools.marathon_tools.load_marathon_config',
                       autospec=True,
                       return_value=self.fake_marathon_config),
            mock.patch('paasta_tools.marathon_tools.get_marathon_client', autospec=True,
                       return_value=self.fake_marathon_client),
            mock.patch('paasta_tools.cleanup_marathon_jobs.delete_app', autospec=True),
        ) as (
            get_services_for_cluster_patch,
            config_patch,
            client_patch,
            delete_patch,
        ):
            cleanup_marathon_jobs.cleanup_apps(soa_dir)
            config_patch.assert_called_once_with()
            get_services_for_cluster_patch.assert_called_once_with(instance_type='marathon', soa_dir=soa_dir)
            client_patch.assert_called_once_with(self.fake_marathon_config.get_url(),
                                                 self.fake_marathon_config.get_username(),
                                                 self.fake_marathon_config.get_password())
            delete_patch.assert_called_once_with(
                app_id='not-here.oh.no.weirdo',
                client=self.fake_marathon_client,
                soa_dir=soa_dir,
            )

    def test_cleanup_apps_doesnt_delete_unknown_apps(self):
        soa_dir = 'not_really_a_dir'
        expected_apps = [('present', 'away'), ('on-app', 'off')]
        fake_app_ids = [mock.Mock(id='non_conforming_app')]
        self.fake_marathon_client.list_apps = mock.Mock(return_value=fake_app_ids)
        with contextlib.nested(
            mock.patch('paasta_tools.cleanup_marathon_jobs.get_services_for_cluster',
                       return_value=expected_apps, autospec=True),
            mock.patch('paasta_tools.marathon_tools.load_marathon_config',
                       autospec=True,
                       return_value=self.fake_marathon_config),
            mock.patch('paasta_tools.marathon_tools.get_marathon_client', autospec=True,
                       return_value=self.fake_marathon_client),
            mock.patch('paasta_tools.cleanup_marathon_jobs.delete_app', autospec=True),
        ) as (
            get_services_for_cluster_patch,
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
            mock.patch('paasta_tools.cleanup_marathon_jobs.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.bounce_lib.bounce_lock_zookeeper', autospec=True),
            mock.patch('paasta_tools.bounce_lib.delete_marathon_app', autospec=True),
            mock.patch('paasta_tools.cleanup_marathon_jobs._log', autospec=True),
            mock.patch('paasta_tools.cleanup_marathon_jobs.send_event', autospec=True)
        ) as (
            mock_load_system_paasta_config,
            mock_bounce_lock_zookeeper,
            mock_delete_marathon_app,
            mock_log,
            mock_send_sensu_event,
        ):
            mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
            cleanup_marathon_jobs.delete_app(app_id, client, 'fake_soa_dir')
            mock_delete_marathon_app.assert_called_once_with(app_id, client)
            mock_load_system_paasta_config.return_value.get_cluster.assert_called_once_with()
            expected_log_line = (
                'Deleted stale marathon job that looks lost: ' +
                app_id
            )
            mock_log.assert_called_once_with(
                instance='main',
                service='example_service',
                level='event',
                component='deploy',
                cluster='fake_cluster',
                line=expected_log_line,
            )
            assert mock_send_sensu_event.call_count == 3

    def test_delete_app_throws_exception(self):
        app_id = 'example--service.main.git93340779.configddb38a65'
        client = self.fake_marathon_client

        with contextlib.nested(
            mock.patch('paasta_tools.cleanup_marathon_jobs.load_system_paasta_config', autospec=True),
            mock.patch('paasta_tools.bounce_lib.bounce_lock_zookeeper', autospec=True),
            mock.patch('paasta_tools.bounce_lib.delete_marathon_app', side_effect=ValueError('foo')),
            mock.patch('paasta_tools.cleanup_marathon_jobs._log', autospec=True),
        ) as (
            mock_load_system_paasta_config,
            mock_bounce_lock_zookeeper,
            mock_delete_marathon_app,
            mock_log,
        ):
            with raises(ValueError):
                cleanup_marathon_jobs.delete_app(app_id, client, 'fake_soa_dir')
            assert 'example_service' in mock_log.mock_calls[0][2]["line"]
            assert 'Traceback' in mock_log.mock_calls[1][2]["line"]


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
