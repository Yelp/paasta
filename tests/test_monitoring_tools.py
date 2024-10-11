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
import pysensu_yelp
import pytest

from paasta_tools import long_running_service_tools
from paasta_tools import monitoring_tools
from paasta_tools.utils import compose_job_id


class TestMonitoring_Tools:

    general_page = True
    fake_general_service_config = {
        "team": "general_test_team",
        "runbook": "y/general_test_runbook",
        "tip": "general_test_tip",
        "notification_email": "general_test_notification_email",
        "page": general_page,
    }

    empty_service_config = long_running_service_tools.LongRunningServiceConfig(
        service="myservicename",
        cluster="mycluster",
        instance="myinstance",
        config_dict={},
        branch_dict=None,
    )
    job_page = False
    fake_marathon_job_config = long_running_service_tools.LongRunningServiceConfig(
        service="myservicename",
        cluster="myclustername",
        instance="myinstance",
        config_dict={
            "team": "job_test_team",
            "runbook": "y/job_test_runbook",
            "tip": "job_test_tip",
            "notification_email": "job_test_notification_email",
            "page": job_page,
        },
        branch_dict=None,
    )
    empty_job_config = {}
    monitor_page = True
    fake_monitor_config = {
        "team": "monitor_test_team",
        "runbook": "y/monitor_test_runbook",
        "tip": "monitor_test_tip",
        "notification_email": "monitor_test_notification_email",
        "page": monitor_page,
    }
    empty_monitor_config = {}
    framework = "fake_framework"
    overrides = {}
    instance = "fake_instance"
    service = "fake_service"
    soa_dir = "/fake/soa/dir"

    def test_get_team(self):
        with mock.patch(
            "paasta_tools.monitoring_tools.__get_monitoring_config_value", autospec=True
        ) as get_monitoring_config_value_patch:
            monitoring_tools.get_team(self.overrides, self.service, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with(
                "team", self.overrides, self.service, self.soa_dir
            )

    def test_get_runbook(self):
        with mock.patch(
            "paasta_tools.monitoring_tools.__get_monitoring_config_value", autospec=True
        ) as get_monitoring_config_value_patch:
            monitoring_tools.get_runbook(self.overrides, self.service, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with(
                "runbook", self.overrides, self.service, self.soa_dir
            )

    def test_get_tip(self):
        with mock.patch(
            "paasta_tools.monitoring_tools.__get_monitoring_config_value", autospec=True
        ) as get_monitoring_config_value_patch:
            monitoring_tools.get_tip(self.overrides, self.service, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with(
                "tip", self.overrides, self.service, self.soa_dir
            )

    def test_get_notification_email(self):
        with mock.patch(
            "paasta_tools.monitoring_tools.__get_monitoring_config_value", autospec=True
        ) as get_monitoring_config_value_patch:
            monitoring_tools.get_notification_email(
                self.overrides, self.service, self.soa_dir
            )
            get_monitoring_config_value_patch.assert_called_once_with(
                "notification_email", self.overrides, self.service, self.soa_dir
            )

    def test_get_page(self):
        with mock.patch(
            "paasta_tools.monitoring_tools.__get_monitoring_config_value", autospec=True
        ) as get_monitoring_config_value_patch:
            monitoring_tools.get_page(self.overrides, self.service, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with(
                "page", self.overrides, self.service, self.soa_dir
            )

    def test_get_alert_after(self):
        with mock.patch(
            "paasta_tools.monitoring_tools.__get_monitoring_config_value", autospec=True
        ) as get_monitoring_config_value_patch:
            monitoring_tools.get_alert_after(self.overrides, self.service, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with(
                "alert_after", self.overrides, self.service, self.soa_dir
            )

    def test_get_realert_every(self):
        with mock.patch(
            "paasta_tools.monitoring_tools.__get_monitoring_config_value", autospec=True
        ) as get_monitoring_config_value_patch:
            monitoring_defaults = mock.Mock()
            monitoring_tools.get_realert_every(
                self.overrides, self.service, self.soa_dir, monitoring_defaults
            )
            get_monitoring_config_value_patch.assert_called_once_with(
                "realert_every",
                self.overrides,
                self.service,
                self.soa_dir,
                monitoring_defaults,
            )

    def test_get_check_every(self):
        with mock.patch(
            "paasta_tools.monitoring_tools.__get_monitoring_config_value", autospec=True
        ) as get_monitoring_config_value_patch:
            monitoring_tools.get_check_every(self.overrides, self.service, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with(
                "check_every", self.overrides, self.service, self.soa_dir
            )

    def test_get_irc_channels(self):
        with mock.patch(
            "paasta_tools.monitoring_tools.__get_monitoring_config_value", autospec=True
        ) as get_monitoring_config_value_patch:
            monitoring_tools.get_irc_channels(
                self.overrides, self.service, self.soa_dir
            )
            get_monitoring_config_value_patch.assert_called_once_with(
                "irc_channels", self.overrides, self.service, self.soa_dir
            )

    def test_get_slack_channels(self):
        with mock.patch(
            "paasta_tools.monitoring_tools.__get_monitoring_config_value", autospec=True
        ) as get_monitoring_config_value_patch:
            monitoring_tools.get_slack_channels(
                self.overrides, self.service, self.soa_dir
            )
            get_monitoring_config_value_patch.assert_called_once_with(
                "slack_channels", self.overrides, self.service, self.soa_dir
            )

    def test_get_dependencies(self):
        with mock.patch(
            "paasta_tools.monitoring_tools.__get_monitoring_config_value", autospec=True
        ) as get_monitoring_config_value_patch:
            monitoring_tools.get_dependencies(
                self.overrides, self.service, self.soa_dir
            )
            get_monitoring_config_value_patch.assert_called_once_with(
                "dependencies", self.overrides, self.service, self.soa_dir
            )

    def test_get_ticket(self):
        with mock.patch(
            "paasta_tools.monitoring_tools.__get_monitoring_config_value", autospec=True
        ) as get_monitoring_config_value_patch:
            monitoring_tools.get_ticket(self.overrides, self.service, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with(
                "ticket", self.overrides, self.service, self.soa_dir
            )

    def test_get_project(self):
        with mock.patch(
            "paasta_tools.monitoring_tools.__get_monitoring_config_value", autospec=True
        ) as get_monitoring_config_value_patch:
            monitoring_tools.get_project(self.overrides, self.service, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with(
                "project", self.overrides, self.service, self.soa_dir
            )

    def test_get_monitoring_config_value_with_monitor_config(self):
        expected = "monitor_test_team"
        with mock.patch(
            "paasta_tools.monitoring_tools._cached_read_service_configuration",
            autospec=True,
            return_value=self.fake_general_service_config,
        ) as service_configuration_lib_patch, mock.patch(
            "paasta_tools.monitoring_tools.read_monitoring_config",
            autospec=True,
            return_value=self.fake_monitor_config,
        ) as read_monitoring_patch, mock.patch(
            "paasta_tools.monitoring_tools.load_system_paasta_config", autospec=True
        ) as load_system_paasta_config_patch:
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(
                return_value="fake_cluster"
            )
            actual = monitoring_tools.get_team(
                self.overrides, self.service, self.soa_dir
            )
            assert expected == actual
            service_configuration_lib_patch.assert_called_once_with(
                self.service, soa_dir=self.soa_dir
            )
            read_monitoring_patch.assert_called_once_with(
                self.service, soa_dir=self.soa_dir
            )

    def test_get_monitoring_config_value_with_service_config(self):
        expected = "general_test_team"
        with mock.patch(
            "paasta_tools.monitoring_tools._cached_read_service_configuration",
            autospec=True,
            return_value=self.fake_general_service_config,
        ) as service_configuration_lib_patch, mock.patch(
            "paasta_tools.monitoring_tools.read_monitoring_config",
            autospec=True,
            return_value=self.empty_monitor_config,
        ) as read_monitoring_patch, mock.patch(
            "paasta_tools.monitoring_tools.load_system_paasta_config", autospec=True
        ) as load_system_paasta_config_patch:
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(
                return_value="fake_cluster"
            )
            actual = monitoring_tools.get_team(
                self.overrides, self.service, self.soa_dir
            )
            assert expected == actual
            service_configuration_lib_patch.assert_called_once_with(
                self.service, soa_dir=self.soa_dir
            )
            read_monitoring_patch.assert_called_once_with(
                self.service, soa_dir=self.soa_dir
            )

    def test_get_monitoring_config_value_with_defaults(self):
        expected = None
        with mock.patch(
            "paasta_tools.monitoring_tools._cached_read_service_configuration",
            autospec=True,
            return_value=self.empty_job_config,
        ) as service_configuration_lib_patch, mock.patch(
            "paasta_tools.monitoring_tools.read_monitoring_config",
            autospec=True,
            return_value=self.empty_monitor_config,
        ) as read_monitoring_patch, mock.patch(
            "paasta_tools.monitoring_tools.load_system_paasta_config", autospec=True
        ) as load_system_paasta_config_patch:
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(
                return_value="fake_cluster"
            )
            actual = monitoring_tools.get_team(
                self.overrides, self.service, self.soa_dir
            )
            assert expected == actual
            service_configuration_lib_patch.assert_called_once_with(
                self.service, soa_dir=self.soa_dir
            )
            read_monitoring_patch.assert_called_once_with(
                self.service, soa_dir=self.soa_dir
            )

    def test_send_event(self):
        fake_service = "fake_service"
        fake_monitoring_overrides = {}
        fake_check_name = "fake_check_name"
        fake_status = "42"
        fake_output = "The http port is not open"
        fake_team = "fake_team"
        fake_tip = "fake_tip"
        fake_notification_email = "fake@notify"
        fake_irc = "#fake"
        fake_slack = "#fake_slack"
        fake_soa_dir = "/fake/soa/dir"
        self.fake_cluster = "fake_cluster"
        fake_sensu_host = "fake_sensu_host"
        fake_sensu_port = 12345
        expected_runbook = "http://y/paasta-troubleshooting"
        expected_check_name = fake_check_name
        expected_kwargs = {
            "name": expected_check_name,
            "runbook": expected_runbook,
            "status": fake_status,
            "output": fake_output,
            "team": fake_team,
            "page": True,
            "tip": fake_tip,
            "notification_email": fake_notification_email,
            "check_every": "1m",
            "realert_every": -1,
            "alert_after": "5m",
            "irc_channels": fake_irc,
            "slack_channels": fake_slack,
            "ticket": False,
            "project": None,
            "priority": None,
            "source": "paasta-fake_cluster",
            "tags": [],
            "ttl": None,
            "sensu_host": fake_sensu_host,
            "sensu_port": fake_sensu_port,
            "component": None,
            "description": None,
        }
        with mock.patch(
            "paasta_tools.monitoring_tools.get_team",
            return_value=fake_team,
            autospec=True,
        ) as get_team_patch, mock.patch(
            "paasta_tools.monitoring_tools.get_tip",
            return_value=fake_tip,
            autospec=True,
        ) as get_tip_patch, mock.patch(
            "paasta_tools.monitoring_tools.get_notification_email",
            return_value=fake_notification_email,
            autospec=True,
        ) as get_notification_email_patch, mock.patch(
            "paasta_tools.monitoring_tools.get_irc_channels",
            return_value=fake_irc,
            autospec=True,
        ) as get_irc_patch, mock.patch(
            "paasta_tools.monitoring_tools.get_slack_channels",
            return_value=fake_slack,
            autospec=True,
        ) as get_slack_patch, mock.patch(
            "paasta_tools.monitoring_tools.get_ticket",
            return_value=False,
            autospec=True,
        ), mock.patch(
            "paasta_tools.monitoring_tools.get_project",
            return_value=None,
            autospec=True,
        ), mock.patch(
            "paasta_tools.monitoring_tools.get_page", return_value=True, autospec=True
        ) as get_page_patch, mock.patch(
            "paasta_tools.monitoring_tools.get_priority",
            return_value=None,
            autospec=True,
        ), mock.patch(
            "paasta_tools.monitoring_tools.get_tags", return_value=[], autospec=True
        ), mock.patch(
            "paasta_tools.monitoring_tools.get_component",
            return_value=None,
            autospec=True,
        ), mock.patch(
            "paasta_tools.monitoring_tools.get_description",
            return_value=None,
            autospec=True,
        ), mock.patch(
            "pysensu_yelp.send_event", autospec=True
        ) as pysensu_yelp_send_event_patch, mock.patch(
            "paasta_tools.monitoring_tools.load_system_paasta_config", autospec=True
        ) as load_system_paasta_config_patch:
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(
                return_value=self.fake_cluster
            )
            load_system_paasta_config_patch.return_value.get_sensu_host = mock.Mock(
                return_value=fake_sensu_host
            )
            load_system_paasta_config_patch.return_value.get_sensu_port = mock.Mock(
                return_value=fake_sensu_port
            )

            monitoring_tools.send_event(
                fake_service,
                fake_check_name,
                fake_monitoring_overrides,
                fake_status,
                fake_output,
                fake_soa_dir,
            )

            get_team_patch.assert_called_once_with(
                fake_monitoring_overrides, fake_service, fake_soa_dir
            )
            get_tip_patch.assert_called_once_with(
                fake_monitoring_overrides, fake_service, fake_soa_dir
            )
            get_notification_email_patch.assert_called_once_with(
                fake_monitoring_overrides, fake_service, fake_soa_dir
            )
            get_irc_patch.assert_called_once_with(
                fake_monitoring_overrides, fake_service, fake_soa_dir
            )
            get_slack_patch.assert_called_once_with(
                fake_monitoring_overrides, fake_service, fake_soa_dir
            )
            get_page_patch.assert_called_once_with(
                fake_monitoring_overrides, fake_service, fake_soa_dir
            )
            pysensu_yelp_send_event_patch.assert_called_once_with(**expected_kwargs)
            load_system_paasta_config_patch.return_value.get_cluster.assert_called_once_with()

    def test_send_event_sensu_host_is_None(self):
        fake_service = "fake_service"
        fake_monitoring_overrides = {}
        fake_check_name = "fake_check_name"
        fake_status = "42"
        fake_output = "The http port is not open"
        fake_soa_dir = "/fake/soa/dir"
        self.fake_cluster = "fake_cluster"
        fake_sensu_port = 12345

        with mock.patch(
            "paasta_tools.monitoring_tools.get_team", autospec=True
        ), mock.patch(
            "paasta_tools.monitoring_tools.get_tip", autospec=True
        ), mock.patch(
            "paasta_tools.monitoring_tools.get_notification_email", autospec=True
        ), mock.patch(
            "paasta_tools.monitoring_tools.get_irc_channels", autospec=True
        ), mock.patch(
            "paasta_tools.monitoring_tools.get_ticket", autospec=True
        ), mock.patch(
            "paasta_tools.monitoring_tools.get_project", autospec=True
        ), mock.patch(
            "paasta_tools.monitoring_tools.get_page", autospec=True
        ), mock.patch(
            "pysensu_yelp.send_event", autospec=True
        ) as pysensu_yelp_send_event_patch, mock.patch(
            "paasta_tools.monitoring_tools.load_system_paasta_config", autospec=True
        ) as load_system_paasta_config_patch:
            load_system_paasta_config_patch.return_value.get_sensu_host = mock.Mock(
                return_value=None
            )
            load_system_paasta_config_patch.return_value.get_sensu_port = mock.Mock(
                return_value=fake_sensu_port
            )

            monitoring_tools.send_event(
                fake_service,
                fake_check_name,
                fake_monitoring_overrides,
                fake_status,
                fake_output,
                fake_soa_dir,
            )

            assert pysensu_yelp_send_event_patch.call_count == 0

    def test_read_monitoring_config(self):
        fake_name = "partial"
        fake_fname = "acronyms"
        fake_path = "ever_patched"
        fake_soa_dir = "/nail/cte/oas"
        fake_dict = {"e": "quail", "v": "snail"}
        with mock.patch(
            "os.path.abspath", autospec=True, return_value=fake_path
        ) as abspath_patch, mock.patch(
            "os.path.join", autospec=True, return_value=fake_fname
        ) as join_patch, mock.patch(
            "service_configuration_lib.read_monitoring",
            autospec=True,
            return_value=fake_dict,
        ) as read_monitoring_patch:
            actual = monitoring_tools.read_monitoring_config(fake_name, fake_soa_dir)
            assert fake_dict == actual
            abspath_patch.assert_called_once_with(fake_soa_dir)
            join_patch.assert_called_once_with(fake_path, fake_name, "monitoring.yaml")
            read_monitoring_patch.assert_called_once_with(fake_fname)


def test_list_teams():
    fake_team_data = {
        "team_data": {
            "red_jaguars": {
                "pagerduty_api_key": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "pages_slack_channel": "red_jaguars_pages",
                "notifications_slack_channel": "red_jaguars_notifications",
                "notification_email": "red_jaguars+alert@yelp.com",
                "project": "REDJAGS",
            },
            "blue_barracudas": {
                "pagerduty_api_key": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "pages_slack_channel": "blue_barracudas_pages",
            },
        }
    }
    expected = {"red_jaguars", "blue_barracudas"}
    with mock.patch(
        "paasta_tools.monitoring_tools._load_sensu_team_data",
        autospec=True,
        return_value=fake_team_data,
    ):
        actual = monitoring_tools.list_teams()
    assert actual == expected


def test_send_event_users_monitoring_tools_send_event_properly(instance_config):
    fake_status = "999999"
    fake_output = "YOU DID IT"
    fake_description = "SOME CONTEXT"
    instance_config.get_monitoring.return_value = {"fake_key": "fake_value"}

    expected_check_name = (
        "check_paasta_services_replication.%s" % instance_config.job_id
    )
    with mock.patch(
        "paasta_tools.monitoring_tools.send_event", autospec=True
    ) as send_event_patch, mock.patch(
        "paasta_tools.monitoring_tools._log", autospec=True
    ), mock.patch(
        "paasta_tools.monitoring_tools.get_runbook",
        autospec=True,
        return_value="y/runbook",
    ):
        monitoring_tools.send_replication_event(
            instance_config=instance_config,
            status=fake_status,
            output=fake_output,
            description=fake_description,
            dry_run=True,
        )
        send_event_patch.assert_called_once_with(
            service=instance_config.service,
            check_name=expected_check_name,
            overrides={
                "fake_key": "fake_value",
                "runbook": mock.ANY,
                "tip": mock.ANY,
                "alert_after": "2m",
                "check_every": "1m",
                "description": fake_description,
            },
            status=fake_status,
            output=fake_output,
            soa_dir=instance_config.soa_dir,
            cluster=instance_config.cluster,
            dry_run=True,
        )


def test_send_replication_event_users_monitoring_tools_send_event_properly(
    instance_config,
):
    fake_status = "999999"
    fake_output = "YOU DID IT"
    fake_description = "SOME CONTEXT"
    instance_config.get_monitoring.return_value = {"fake_key": "fake_value"}

    expected_check_name = (
        "check_paasta_services_replication.%s" % instance_config.job_id
    )
    with mock.patch(
        "paasta_tools.monitoring_tools.send_event", autospec=True
    ) as send_event_patch, mock.patch(
        "paasta_tools.monitoring_tools._log", autospec=True
    ), mock.patch(
        "paasta_tools.monitoring_tools.get_runbook",
        autospec=True,
        return_value="y/runbook",
    ):
        monitoring_tools.send_replication_event(
            instance_config=instance_config,
            status=fake_status,
            output=fake_output,
            description=fake_description,
            dry_run=True,
        )
        send_event_patch.assert_called_once_with(
            service=instance_config.service,
            check_name=expected_check_name,
            overrides={
                "fake_key": "fake_value",
                "runbook": mock.ANY,
                "tip": mock.ANY,
                "alert_after": "2m",
                "check_every": "1m",
                "description": fake_description,
            },
            status=fake_status,
            output=fake_output,
            soa_dir=instance_config.soa_dir,
            cluster=instance_config.cluster,
            dry_run=True,
        )


def test_send_replication_event_users_monitoring_tools_send_event_respects_alert_after(
    instance_config,
):
    fake_status = "999999"
    fake_output = "YOU DID IT"
    fake_description = "SOME CONTEXT"
    instance_config.get_monitoring.return_value = {"alert_after": "666m"}
    expected_check_name = (
        "check_paasta_services_replication.%s" % instance_config.job_id
    )
    with mock.patch(
        "paasta_tools.monitoring_tools.send_event", autospec=True
    ) as send_event_patch, mock.patch(
        "paasta_tools.monitoring_tools._log", autospec=True
    ), mock.patch(
        "paasta_tools.monitoring_tools.get_runbook",
        autospec=True,
        return_value="y/runbook",
    ):
        monitoring_tools.send_replication_event(
            instance_config=instance_config,
            status=fake_status,
            output=fake_output,
            description=fake_description,
            dry_run=True,
        )
        send_event_patch.call_count == 1
        send_event_patch.assert_called_once_with(
            service=instance_config.service,
            check_name=expected_check_name,
            overrides={
                "runbook": mock.ANY,
                "tip": mock.ANY,
                "alert_after": "666m",
                "check_every": "1m",
                "description": fake_description,
            },
            status=fake_status,
            output=fake_output,
            soa_dir=instance_config.soa_dir,
            cluster=instance_config.cluster,
            dry_run=True,
        )


@pytest.fixture
def instance_config():
    service = "fake_service"
    instance = "fake_instance"
    job_id = compose_job_id(service, instance)
    mock_instance_config = mock.Mock(
        service=service,
        instance=instance,
        cluster="fake_cluster",
        soa_dir="fake_soa_dir",
        job_id=job_id,
    )
    mock_instance_config.get_replication_crit_percentage.return_value = 90
    mock_instance_config.get_registrations.return_value = [job_id]
    mock_instance_config.get_pool.return_value = "fake_pool"
    return mock_instance_config


def test_check_replication_for_instance_ok_when_expecting_zero(
    instance_config,
):
    expected_replication_count = 0
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = {
        "fake_provider": {
            "fake_region": {"test.main": 1, "test.three": 4, "test.four": 8}
        }
    }

    with mock.patch(
        "paasta_tools.monitoring_tools.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        monitoring_tools.check_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            replication_checker=mock_smartstack_replication_checker,
            dry_run=True,
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.OK,
            output=mock.ANY,
            description=mock.ANY,
            dry_run=True,
        )


def test_check_replication_for_instance_crit_when_absent(instance_config):
    expected_replication_count = 8
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = {
        "fake_provider": {
            "fake_region": {"test.two": 1, "test.three": 4, "test.four": 8}
        }
    }
    with mock.patch(
        "paasta_tools.monitoring_tools.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        monitoring_tools.check_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            replication_checker=mock_smartstack_replication_checker,
            dry_run=True,
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
            description=mock.ANY,
            dry_run=True,
        )


def test_check_replication_for_instance_crit_when_zero_replication(
    instance_config,
):
    expected_replication_count = 8
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = {
        "fake_provider": {
            "fake_region": {
                "fake_service.fake_instance": 0,
                "test.main": 8,
                "test.fully_replicated": 8,
            }
        }
    }
    with mock.patch(
        "paasta_tools.monitoring_tools.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        monitoring_tools.check_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            replication_checker=mock_smartstack_replication_checker,
            dry_run=True,
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
            description=mock.ANY,
            dry_run=True,
        )
        _, send_replication_event_kwargs = mock_send_replication_event.call_args
        alert_output = send_replication_event_kwargs["output"]
        assert (
            f"{instance_config.job_id} has 0/8 replicas in fake_region"
        ) in alert_output
        assert (
            "paasta status -s {} -i {} -c {} -vv".format(
                instance_config.service,
                instance_config.instance,
                instance_config.cluster,
            )
        ) in send_replication_event_kwargs["description"]


def test_check_replication_for_instance_crit_when_low_replication(
    instance_config,
):
    expected_replication_count = 8
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = {
        "fake_provider": {
            "fake_region": {
                "test.canary": 1,
                "fake_service.fake_instance": 4,
                "test.fully_replicated": 8,
            }
        }
    }
    with mock.patch(
        "paasta_tools.monitoring_tools.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        monitoring_tools.check_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            replication_checker=mock_smartstack_replication_checker,
            dry_run=True,
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
            description=mock.ANY,
            dry_run=True,
        )
        _, send_replication_event_kwargs = mock_send_replication_event.call_args
        alert_output = send_replication_event_kwargs["output"]
        assert (
            f"{instance_config.job_id} has 4/8 replicas in fake_region"
        ) in alert_output
        assert (
            "paasta status -s {} -i {} -c {} -vv".format(
                instance_config.service,
                instance_config.instance,
                instance_config.cluster,
            )
        ) in send_replication_event_kwargs["description"]


def test_check_replication_for_instance_ok_with_enough_replication(
    instance_config,
):
    expected_replication_count = 8
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = {
        "fake_provider": {
            "fake_region": {
                "test.canary": 1,
                "test.low_replication": 4,
                "fake_service.fake_instance": 8,
            }
        }
    }
    with mock.patch(
        "paasta_tools.monitoring_tools.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        monitoring_tools.check_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            replication_checker=mock_smartstack_replication_checker,
            dry_run=True,
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.OK,
            output=mock.ANY,
            description=mock.ANY,
            dry_run=True,
        )
        _, send_replication_event_kwargs = mock_send_replication_event.call_args
        alert_output = send_replication_event_kwargs["output"]
        assert (
            "{} has 8/8 replicas in fake_region according to fake_provider (OK: 100.0%)".format(
                instance_config.job_id
            )
        ) in alert_output


def test_check_replication_for_instance_ok_with_enough_replication_multilocation(
    instance_config,
):
    expected_replication_count = 2
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = {
        "fake_provider": {
            "fake_region": {"fake_service.fake_instance": 1},
            "fake_other_region": {"fake_service.fake_instance": 1},
        }
    }
    with mock.patch(
        "paasta_tools.monitoring_tools.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        monitoring_tools.check_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            replication_checker=mock_smartstack_replication_checker,
            dry_run=True,
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.OK,
            output=mock.ANY,
            description=mock.ANY,
            dry_run=True,
        )
        _, send_replication_event_kwargs = mock_send_replication_event.call_args
        alert_output = send_replication_event_kwargs["output"]
        assert (
            f"{instance_config.job_id} has 1/1 replicas in fake_region"
        ) in alert_output
        assert (
            f"{instance_config.job_id} has 1/1 replicas in fake_other_region"
        ) in alert_output


def test_check_replication_for_instance_crit_when_low_replication_multilocation(
    instance_config,
):
    expected_replication_count = 2
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = {
        "fake_provider": {
            "fake_region": {"fake_service.fake_instance": 1},
            "fake_other_region": {"fake_service.fake_instance": 0},
        }
    }
    with mock.patch(
        "paasta_tools.monitoring_tools.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        monitoring_tools.check_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            replication_checker=mock_smartstack_replication_checker,
            dry_run=True,
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
            description=mock.ANY,
            dry_run=True,
        )
        _, send_replication_event_kwargs = mock_send_replication_event.call_args
        alert_output = send_replication_event_kwargs["output"]
        assert (
            f"{instance_config.job_id} has 1/1 replicas in fake_region"
        ) in alert_output
        assert (
            f"{instance_config.job_id} has 0/1 replicas in fake_other_region"
        ) in alert_output
        assert (
            "paasta status -s {} -i {} -c {} -vv".format(
                instance_config.service,
                instance_config.instance,
                instance_config.cluster,
            )
        ) in send_replication_event_kwargs["description"]


def test_check_replication_for_instance_crit_when_zero_replication_multilocation(
    instance_config,
):
    expected_replication_count = 2
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = {
        "fake_provider": {
            "fake_region": {"fake_service.fake_instance": 0},
            "fake_other_region": {"fake_service.fake_instance": 0},
        }
    }
    with mock.patch(
        "paasta_tools.monitoring_tools.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        monitoring_tools.check_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            replication_checker=mock_smartstack_replication_checker,
            dry_run=True,
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
            description=mock.ANY,
            dry_run=True,
        )
        _, send_replication_event_kwargs = mock_send_replication_event.call_args
        alert_output = send_replication_event_kwargs["output"]
        assert (
            f"{instance_config.job_id} has 0/1 replicas in fake_region"
        ) in alert_output
        assert (
            f"{instance_config.job_id} has 0/1 replicas in fake_other_region"
        ) in alert_output
        assert (
            "paasta status -s {} -i {} -c {} -vv".format(
                instance_config.service,
                instance_config.instance,
                instance_config.cluster,
            )
        ) in send_replication_event_kwargs["description"]


def test_check_replication_for_instance_crit_when_missing_replication_multilocation(
    instance_config,
):
    expected_replication_count = 2
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = {
        "fake_provider": {
            "fake_region": {"test.main": 0},
            "fake_other_region": {"test.main": 0},
        }
    }
    with mock.patch(
        "paasta_tools.monitoring_tools.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        monitoring_tools.check_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            replication_checker=mock_smartstack_replication_checker,
            dry_run=True,
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
            description=mock.ANY,
            dry_run=True,
        )
        _, send_replication_event_kwargs = mock_send_replication_event.call_args
        alert_output = send_replication_event_kwargs["output"]
        assert (
            f"{instance_config.job_id} has 0/1 replicas in fake_region"
        ) in alert_output
        assert (
            f"{instance_config.job_id} has 0/1 replicas in fake_other_region"
        ) in alert_output


def test_check_replication_for_instance_crit_when_no_smartstack_info(
    instance_config,
):
    expected_replication_count = 2
    mock_smartstack_replication_checker = mock.Mock()
    mock_smartstack_replication_checker.get_replication_for_instance.return_value = {
        "fake_provider": {}
    }
    with mock.patch(
        "paasta_tools.monitoring_tools.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        monitoring_tools.check_replication_for_instance(
            instance_config=instance_config,
            expected_count=expected_replication_count,
            replication_checker=mock_smartstack_replication_checker,
            dry_run=True,
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output=mock.ANY,
            description=mock.ANY,
            dry_run=True,
        )
        _, send_replication_event_kwargs = mock_send_replication_event.call_args
        alert_output = send_replication_event_kwargs["output"]
        assert (
            f"{instance_config.job_id} has no fake_provider replication info."
        ) in alert_output


def test_emit_replication_metrics(instance_config):
    with mock.patch(
        "paasta_tools.monitoring_tools.yelp_meteorite", autospec=True
    ) as mock_yelp_meteorite:
        mock_smartstack_replication_info = {
            "fake_provider": {
                "fake_region_1": {
                    "fake_service.fake_instance": 2,
                    "other_service.other_instance": 5,
                },
                "fake_region_2": {"fake_service.fake_instance": 4},
            }
        }
        mock_gauges = {
            "paasta.service.available_backends": mock.Mock(),
            "paasta.service.critical_backends": mock.Mock(),
            "paasta.service.expected_backends": mock.Mock(),
        }
        expected_dims = {
            "paasta_service": "fake_service",
            "paasta_cluster": "fake_cluster",
            "paasta_instance": "fake_instance",
            "paasta_pool": "fake_pool",
            "service_discovery_provider": "fake_provider",
        }

        mock_yelp_meteorite.create_gauge.side_effect = lambda name, dims: mock_gauges[
            name
        ]
        monitoring_tools.emit_replication_metrics(
            mock_smartstack_replication_info,
            instance_config,
            expected_count=10,
        )

        mock_yelp_meteorite.create_gauge.assert_has_calls(
            [
                mock.call("paasta.service.available_backends", expected_dims),
                mock.call("paasta.service.critical_backends", expected_dims),
                mock.call("paasta.service.expected_backends", expected_dims),
            ]
        )
        mock_gauges["paasta.service.available_backends"].set.assert_called_once_with(6)
        mock_gauges["paasta.service.critical_backends"].set.assert_called_once_with(9)
        mock_gauges["paasta.service.expected_backends"].set.assert_called_once_with(10)


def test_emit_replication_metrics_dry_run(instance_config):
    with mock.patch(
        "paasta_tools.monitoring_tools.yelp_meteorite", autospec=True
    ) as mock_yelp_meteorite:
        mock_smartstack_replication_info = {
            "fake_provider": {
                "fake_region_1": {
                    "fake_service.fake_instance": 2,
                    "other_service.other_instance": 5,
                },
                "fake_region_2": {"fake_service.fake_instance": 4},
            }
        }
        monitoring_tools.emit_replication_metrics(
            mock_smartstack_replication_info,
            instance_config,
            expected_count=10,
            dry_run=True,
        )
        mock_yelp_meteorite.create_gauge.call_count = 0


def test_check_replication_for_instance_emits_metrics(instance_config):
    with mock.patch(
        "paasta_tools.monitoring_tools.send_replication_event", autospec=True
    ), mock.patch(
        "paasta_tools.monitoring_tools.yelp_meteorite", autospec=True
    ), mock.patch(
        "paasta_tools.monitoring_tools.emit_replication_metrics", autospec=True
    ) as mock_emit_replication_metrics:
        mock_smartstack_replication_checker = mock.Mock()
        mock_smartstack_replication_checker.get_replication_for_instance.return_value = {
            "fake_provider": {"fake_region": {"fake_service.fake_instance": 10}}
        }

        monitoring_tools.check_replication_for_instance(
            instance_config=instance_config,
            expected_count=10,
            replication_checker=mock_smartstack_replication_checker,
            dry_run=True,
        )
        mock_emit_replication_metrics.assert_called_once_with(
            mock_smartstack_replication_checker.get_replication_for_instance.return_value,
            instance_config,
            10,
            dry_run=True,
        )


def test_send_replication_event_if_under_replication_handles_0_expected(
    instance_config,
):
    with mock.patch(
        "paasta_tools.monitoring_tools.send_replication_event", autospec=True
    ) as mock_send_event:
        monitoring_tools.send_replication_event_if_under_replication(
            instance_config=instance_config,
            expected_count=0,
            num_available=0,
            dry_run=True,
        )
        mock_send_event.assert_called_once_with(
            instance_config=instance_config,
            status=0,
            output=mock.ANY,
            description=mock.ANY,
            dry_run=True,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert (
            "{} has 0/0 replicas available (threshold: 90%)".format(
                instance_config.job_id
            )
        ) in alert_output


def test_send_replication_event_if_under_replication_good(instance_config):
    with mock.patch(
        "paasta_tools.monitoring_tools.send_replication_event", autospec=True
    ) as mock_send_event:
        monitoring_tools.send_replication_event_if_under_replication(
            instance_config=instance_config,
            expected_count=100,
            num_available=100,
            dry_run=True,
        )
        mock_send_event.assert_called_once_with(
            instance_config=instance_config,
            status=0,
            output=mock.ANY,
            description=mock.ANY,
            dry_run=True,
        )
        _, send_event_kwargs = mock_send_event.call_args
        alert_output = send_event_kwargs["output"]
        assert (
            "{} has 100/100 replicas available (threshold: 90%)".format(
                instance_config.job_id
            )
        ) in alert_output


def test_send_replication_event_if_under_replication_critical(instance_config):
    with mock.patch(
        "paasta_tools.monitoring_tools.send_replication_event", autospec=True
    ) as mock_send_event:
        monitoring_tools.send_replication_event_if_under_replication(
            instance_config=instance_config,
            expected_count=100,
            num_available=89,
            dry_run=True,
        )
        mock_send_event.assert_called_once_with(
            instance_config=instance_config,
            status=2,
            output=mock.ANY,
            description=mock.ANY,
            dry_run=True,
        )
        _, send_event_kwargs = mock_send_event.call_args
        assert (
            "{} has 89/100 replicas available (threshold: 90%)".format(
                instance_config.job_id
            )
        ) in send_event_kwargs["output"]
        assert (
            "paasta status -s {} -i {} -c {} -vv".format(
                instance_config.service,
                instance_config.instance,
                instance_config.cluster,
            )
        ) in send_event_kwargs["description"]
