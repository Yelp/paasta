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
import mock
from pytest import raises

from paasta_tools import cleanup_marathon_jobs
from paasta_tools.util.config_loading import SystemPaastaConfig


class TestCleanupMarathonJobs:

    cleanup_marathon_jobs.log = mock.Mock()
    fake_docker_registry = "http://del.icio.us/"
    fake_cluster = "fake_test_cluster"
    fake_system_config = SystemPaastaConfig(
        {
            "marathon_servers": [
                {"url": "http://mess_url", "user": "namnin", "password": "pass_nememim"}
            ]
        },
        directory="/fake/etc/paasta",
    )
    fake_marathon_client = mock.Mock()
    fake_marathon_clients = mock.Mock(
        get_all_clients=mock.Mock(return_value=[fake_marathon_client])
    )

    def test_main(self):
        soa_dir = "paasta_maaaachine"
        with mock.patch(
            "paasta_tools.cleanup_marathon_jobs.cleanup_apps", autospec=True
        ) as cleanup_patch:
            cleanup_marathon_jobs.main(("--soa-dir", soa_dir))
            cleanup_patch.assert_called_once_with(
                soa_dir, kill_threshold=0.5, force=False
            )

    def test_cleanup_apps(self):
        soa_dir = "not_really_a_dir"
        expected_apps = [("present", "away"), ("on-app", "off")]
        fake_app_ids = [
            mock.Mock(id="present.away.gone.wtf"),
            mock.Mock(id="on-app.off.stop.jrm"),
            mock.Mock(id="not-here.oh.no.weirdo"),
        ]
        self.fake_marathon_client.list_apps = mock.Mock(return_value=fake_app_ids)
        with mock.patch(
            "paasta_tools.cleanup_marathon_jobs.get_services_for_cluster",
            return_value=expected_apps,
            autospec=True,
        ) as get_services_for_cluster_patch, mock.patch(
            "paasta_tools.cleanup_marathon_jobs.load_system_paasta_config",
            autospec=True,
            return_value=self.fake_system_config,
        ) as config_patch, mock.patch(
            "paasta_tools.marathon_tools.get_marathon_clients",
            autospec=True,
            return_value=self.fake_marathon_clients,
        ) as clients_patch, mock.patch(
            "paasta_tools.cleanup_marathon_jobs.delete_app", autospec=True
        ) as delete_patch:
            cleanup_marathon_jobs.cleanup_apps(soa_dir)
            config_patch.assert_called_once_with()
            get_services_for_cluster_patch.assert_called_once_with(
                instance_type="marathon", soa_dir=soa_dir
            )
            clients_patch.assert_called_once_with(mock.ANY)
            delete_patch.assert_called_once_with(
                app_id="not-here.oh.no.weirdo",
                client=self.fake_marathon_client,
                soa_dir=soa_dir,
            )

    def test_cleanup_apps_dont_kill_everything(self):
        soa_dir = "not_really_a_dir"
        expected_apps = []
        fake_app_ids = [
            mock.Mock(id="present.away.gone.wtf"),
            mock.Mock(id="on-app.off.stop.jrm"),
            mock.Mock(id="not-here.oh.no.weirdo"),
        ]
        self.fake_marathon_client.list_apps = mock.Mock(return_value=fake_app_ids)
        with mock.patch(
            "paasta_tools.cleanup_marathon_jobs.get_services_for_cluster",
            return_value=expected_apps,
            autospec=True,
        ) as get_services_for_cluster_patch, mock.patch(
            "paasta_tools.cleanup_marathon_jobs.load_system_paasta_config",
            autospec=True,
            return_value=self.fake_system_config,
        ) as config_patch, mock.patch(
            "paasta_tools.marathon_tools.get_marathon_clients",
            autospec=True,
            return_value=self.fake_marathon_clients,
        ) as clients_patch, mock.patch(
            "paasta_tools.cleanup_marathon_jobs.delete_app", autospec=True
        ) as delete_patch:
            with raises(cleanup_marathon_jobs.DontKillEverythingError):
                cleanup_marathon_jobs.cleanup_apps(soa_dir)
            config_patch.assert_called_once_with()
            get_services_for_cluster_patch.assert_called_once_with(
                instance_type="marathon", soa_dir=soa_dir
            )
            clients_patch.assert_called_once_with(mock.ANY)

            assert delete_patch.call_count == 0

    def test_cleanup_apps_force(self):
        soa_dir = "not_really_a_dir"
        expected_apps = []
        fake_app_ids = [
            mock.Mock(id="present.away.gone.wtf"),
            mock.Mock(id="on-app.off.stop.jrm"),
            mock.Mock(id="not-here.oh.no.weirdo"),
        ]
        self.fake_marathon_client.list_apps = mock.Mock(return_value=fake_app_ids)
        with mock.patch(
            "paasta_tools.cleanup_marathon_jobs.get_services_for_cluster",
            return_value=expected_apps,
            autospec=True,
        ) as get_services_for_cluster_patch, mock.patch(
            "paasta_tools.cleanup_marathon_jobs.load_system_paasta_config",
            autospec=True,
            return_value=self.fake_system_config,
        ) as config_patch, mock.patch(
            "paasta_tools.marathon_tools.get_marathon_clients",
            autospec=True,
            return_value=self.fake_marathon_clients,
        ) as clients_patch, mock.patch(
            "paasta_tools.cleanup_marathon_jobs.delete_app", autospec=True
        ) as delete_patch:
            cleanup_marathon_jobs.cleanup_apps(soa_dir, force=True)
            config_patch.assert_called_once_with()
            get_services_for_cluster_patch.assert_called_once_with(
                instance_type="marathon", soa_dir=soa_dir
            )
            clients_patch.assert_called_once_with(mock.ANY)
            assert delete_patch.call_count == 3

    def test_cleanup_apps_doesnt_delete_unknown_apps(self):
        soa_dir = "not_really_a_dir"
        expected_apps = [("present", "away"), ("on-app", "off")]
        fake_app_ids = [mock.Mock(id="non_conforming_app")]
        self.fake_marathon_client.list_apps = mock.Mock(return_value=fake_app_ids)
        with mock.patch(
            "paasta_tools.cleanup_marathon_jobs.get_services_for_cluster",
            return_value=expected_apps,
            autospec=True,
        ), mock.patch(
            "paasta_tools.cleanup_marathon_jobs.load_system_paasta_config",
            autospec=True,
            return_value=self.fake_system_config,
        ), mock.patch(
            "paasta_tools.marathon_tools.get_marathon_clients",
            autospec=True,
            return_value=self.fake_marathon_clients,
        ), mock.patch(
            "paasta_tools.cleanup_marathon_jobs.delete_app", autospec=True
        ) as delete_patch:
            cleanup_marathon_jobs.cleanup_apps(soa_dir)
            assert delete_patch.call_count == 0

    def test_delete_app(self):
        app_id = "example--service.main.git93340779.configddb38a65"
        client = self.fake_marathon_client
        with mock.patch(
            "paasta_tools.cleanup_marathon_jobs.load_system_paasta_config",
            autospec=True,
        ) as mock_load_system_paasta_config, mock.patch(
            "paasta_tools.bounce_lib.bounce_lock_zookeeper", autospec=True
        ), mock.patch(
            "paasta_tools.bounce_lib.delete_marathon_app", autospec=True
        ) as mock_delete_marathon_app, mock.patch(
            "paasta_tools.cleanup_marathon_jobs._log", autospec=True
        ) as mock_log, mock.patch(
            "paasta_tools.cleanup_marathon_jobs.send_event", autospec=True
        ) as mock_send_sensu_event:
            mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(
                return_value="fake_cluster"
            )
            cleanup_marathon_jobs.delete_app(app_id, client, "fake_soa_dir")
            mock_delete_marathon_app.assert_called_once_with(app_id, client)
            mock_load_system_paasta_config.return_value.get_cluster.assert_called_once_with()
            expected_log_line = "Deleted stale marathon job that looks lost: " + app_id
            mock_log.assert_called_once_with(
                instance="main",
                service="example_service",
                level="event",
                component="deploy",
                cluster="fake_cluster",
                line=expected_log_line,
            )
            assert mock_send_sensu_event.call_count == 2

    def test_delete_app_throws_exception(self):
        app_id = "example--service.main.git93340779.configddb38a65"
        client = self.fake_marathon_client

        with mock.patch(
            "paasta_tools.cleanup_marathon_jobs.load_system_paasta_config",
            autospec=True,
        ), mock.patch(
            "paasta_tools.bounce_lib.bounce_lock_zookeeper", autospec=True
        ), mock.patch(
            "paasta_tools.bounce_lib.delete_marathon_app",
            side_effect=ValueError("foo"),
            autospec=True,
        ), mock.patch(
            "paasta_tools.cleanup_marathon_jobs._log", autospec=True
        ) as mock_log:
            with raises(ValueError):
                cleanup_marathon_jobs.delete_app(app_id, client, "fake_soa_dir")
            assert "example_service" in mock_log.mock_calls[0][2]["line"]
            assert "Traceback" in mock_log.mock_calls[1][2]["line"]


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
