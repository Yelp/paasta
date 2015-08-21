import contextlib
import mock

import chronos_tools
import marathon_tools
import monitoring_tools


class TestMonitoring_Tools:

    general_page = True
    fake_general_service_config = {
        'team': 'general_test_team',
        'runbook': 'y/general_test_runbook',
        'tip': 'general_test_tip',
        'notification_email': 'general_test_notification_email',
        'page': general_page
    }

    empty_service_config = marathon_tools.MarathonServiceConfig('myservicename', 'myinstance', {}, {})
    job_page = False
    fake_marathon_job_config = marathon_tools.MarathonServiceConfig(
        'myservicename',
        'myinstance',
        {
            'team': 'job_test_team',
            'runbook': 'y/job_test_runbook',
            'tip': 'job_test_tip',
            'notification_email': 'job_test_notification_email',
            'page': job_page
        },
        {},
    )
    fake_chronos_job_config = chronos_tools.ChronosJobConfig(
        'myservicename',
        'myinstance',
        {
            'team': 'job_test_team',
            'runbook': 'y/job_test_runbook',
            'tip': 'job_test_tip',
            'notification_email': 'job_test_notification_email',
            'page': job_page
        },
        {},
    )
    empty_job_config = {}
    monitor_page = True
    fake_monitor_config = {
        'team': 'monitor_test_team',
        'runbook': 'y/monitor_test_runbook',
        'tip': 'monitor_test_tip',
        'notification_email': 'monitor_test_notification_email',
        'page': monitor_page
    }
    empty_monitor_config = {}
    framework = 'fake_framework'
    overrides = {}
    instance = 'fake_instance'
    service_name = 'fake_service'
    soa_dir = '/fake/soa/dir'

    def test_get_team(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_team(self.overrides, self.service_name, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with('team', self.overrides, self.service_name,
                                                                      self.soa_dir)

    def test_get_runbook(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_runbook(self.overrides, self.service_name, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with('runbook', self.overrides, self.service_name,
                                                                      self.soa_dir)

    def test_get_tip(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_tip(self.overrides, self.service_name, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with('tip', self.overrides, self.service_name,
                                                                      self.soa_dir)

    def test_get_notification_email(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_notification_email(self.overrides, self.service_name, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with('notification_email', self.overrides,
                                                                      self.service_name, self.soa_dir)

    def test_get_page(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_page(self.overrides, self.service_name, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with('page', self.overrides, self.service_name,
                                                                      self.soa_dir)

    def test_get_alert_after(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_alert_after(self.overrides, self.service_name, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with('alert_after', self.overrides, self.service_name,
                                                                      self.soa_dir)

    def test_get_realert_every(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_realert_every(self.overrides, self.service_name, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with('realert_every', self.overrides,
                                                                      self.service_name, self.soa_dir)

    def test_get_check_every(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_check_every(self.overrides, self.service_name, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with('check_every', self.overrides, self.service_name,
                                                                      self.soa_dir)

    def test_get_irc_channels(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_irc_channels(self.overrides, self.service_name, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with('irc_channels', self.overrides, self.service_name,
                                                                      self.soa_dir)

    def test_get_dependencies(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_dependencies(self.overrides, self.service_name, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with('dependencies', self.overrides, self.service_name,
                                                                      self.soa_dir)

    def test_get_ticket(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_ticket(self.overrides, self.service_name, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with('ticket', self.overrides, self.service_name,
                                                                      self.soa_dir)

    def test_get_project(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_project(self.overrides, self.service_name, self.soa_dir)
            get_monitoring_config_value_patch.assert_called_once_with('project', self.overrides, self.service_name,
                                                                      self.soa_dir)

    def test_get_monitoring_config_value_with_monitor_config(self):
        expected = 'monitor_test_team'
        with contextlib.nested(
            mock.patch('service_configuration_lib.read_service_configuration', autospec=True,
                       return_value=self.fake_general_service_config),
            mock.patch('monitoring_tools.read_monitoring_config',
                       autospec=True, return_value=self.fake_monitor_config),
            mock.patch('monitoring_tools.load_system_paasta_config', autospec=True),
        ) as (
            service_configuration_lib_patch,
            read_monitoring_patch,
            load_system_paasta_config_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
            actual = monitoring_tools.get_team(self.overrides, self.service_name, self.soa_dir)
            assert expected == actual
            service_configuration_lib_patch.assert_called_once_with(self.service_name, soa_dir=self.soa_dir)
            read_monitoring_patch.assert_called_once_with(self.service_name, soa_dir=self.soa_dir)

    def test_get_monitoring_config_value_with_service_config(self):
        expected = 'general_test_team'
        with contextlib.nested(
            mock.patch('service_configuration_lib.read_service_configuration', autospec=True,
                       return_value=self.fake_general_service_config),
            mock.patch('monitoring_tools.read_monitoring_config',
                       autospec=True, return_value=self.empty_monitor_config),
            mock.patch('monitoring_tools.load_system_paasta_config', autospec=True),
        ) as (
            service_configuration_lib_patch,
            read_monitoring_patch,
            load_system_paasta_config_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
            actual = monitoring_tools.get_team(self.overrides, self.service_name, self.soa_dir)
            assert expected == actual
            service_configuration_lib_patch.assert_called_once_with(self.service_name, soa_dir=self.soa_dir)
            read_monitoring_patch.assert_called_once_with(self.service_name, soa_dir=self.soa_dir)

    def test_get_monitoring_config_value_with_defaults(self):
        expected = None
        with contextlib.nested(
            mock.patch('service_configuration_lib.read_service_configuration', autospec=True,
                       return_value=self.empty_job_config),
            mock.patch('monitoring_tools.read_monitoring_config',
                       autospec=True, return_value=self.empty_monitor_config),
            mock.patch('monitoring_tools.load_system_paasta_config', autospec=True),
        ) as (
            service_configuration_lib_patch,
            read_monitoring_patch,
            load_system_paasta_config_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(return_value='fake_cluster')
            actual = monitoring_tools.get_team(self.overrides, self.service_name, self.soa_dir)
            assert expected == actual
            service_configuration_lib_patch.assert_called_once_with(self.service_name, soa_dir=self.soa_dir)
            read_monitoring_patch.assert_called_once_with(self.service_name, soa_dir=self.soa_dir)

    def test_get_team_email_address_uses_override_if_specified(self):
        fake_email = 'fake_email'
        with contextlib.nested(
            mock.patch('monitoring_tools.__get_monitoring_config_value', autospec=True),
        ) as (
            mock_get_monitoring_config_value,
        ):
            mock_get_monitoring_config_value.return_value = 'fake_email'
            actual = monitoring_tools.get_team_email_address('fake_service', {'notification_email': fake_email})
            assert actual == fake_email

    def test_get_team_email_address_uses_instance_config_if_specified(self):
        expected = 'fake_email'
        with contextlib.nested(
            mock.patch('monitoring_tools.__get_monitoring_config_value', autospec=True),
        ) as (
            mock_get_monitoring_config_value,
        ):
            mock_get_monitoring_config_value.return_value = 'fake_email'
            actual = monitoring_tools.get_team_email_address('fake_service')
            assert actual == expected

    def test_get_team_email_address_uses_team_data_as_last_resort(self):
        expected = 'team_data_email'
        with contextlib.nested(
            mock.patch('monitoring_tools.__get_monitoring_config_value', autospec=True),
            mock.patch('monitoring_tools.get_sensu_team_data', autospec=True),
            mock.patch('monitoring_tools.get_team', autospec=True),
        ) as (
            mock_get_monitoring_config_value,
            mock_get_sensu_team_data,
            mock_get_team,
        ):
            mock_get_team.return_value = 'test_team'
            mock_get_monitoring_config_value.return_value = False
            mock_get_sensu_team_data.return_value = {
                'notification_email': expected
            }
            actual = monitoring_tools.get_team_email_address('fake_service')
            assert actual == expected

    def test_get_team_email_address_returns_none_if_not_available(self):
        with contextlib.nested(
            mock.patch('monitoring_tools.__get_monitoring_config_value', autospec=True),
            mock.patch('monitoring_tools.get_sensu_team_data', autospec=True),
            mock.patch('monitoring_tools.get_team', autospec=True),
        ) as (
            mock_get_monitoring_config_value,
            mock_get_sensu_team_data,
            mock_get_team,
        ):
            mock_get_team.return_value = 'test_team'
            mock_get_monitoring_config_value.return_value = False
            mock_get_sensu_team_data.return_value = {}
            actual = monitoring_tools.get_team_email_address('fake_service')
            assert actual is None

    def test_send_event(self):
        fake_service_name = 'fake_service'
        fake_monitoring_overrides = {}
        fake_check_name = 'fake_check_name'
        fake_status = '42'
        fake_output = 'The http port is not open'
        fake_team = 'fake_team'
        fake_tip = 'fake_tip'
        fake_notification_email = 'fake@notify'
        fake_irc = '#fake'
        fake_soa_dir = '/fake/soa/dir'
        self.fake_cluster = 'fake_cluster'
        expected_runbook = 'http://y/paasta-troubleshooting'
        expected_check_name = fake_check_name
        expected_kwargs = {
            'tip': fake_tip,
            'notification_email': fake_notification_email,
            'irc_channels': fake_irc,
            'project': None,
            'ticket': False,
            'page': True,
            'alert_after': '5m',
            'check_every': '1m',
            'realert_every': -1,
            'source': 'paasta-fake_cluster',
        }
        with contextlib.nested(
            mock.patch(
                "monitoring_tools.get_team",
                return_value=fake_team,
                autospec=True,
            ),
            mock.patch(
                "monitoring_tools.get_tip",
                return_value=fake_tip,
                autospec=True,
            ),
            mock.patch(
                "monitoring_tools.get_notification_email",
                return_value=fake_notification_email,
                autospec=True,
            ),
            mock.patch(
                "monitoring_tools.get_irc_channels",
                return_value=fake_irc,
                autospec=True,
            ),
            mock.patch(
                "monitoring_tools.get_ticket",
                return_value=False,
                autospec=True,
            ),
            mock.patch(
                "monitoring_tools.get_project",
                return_value=None,
                autospec=True,
            ),
            mock.patch(
                "monitoring_tools.get_page",
                return_value=True,
                autospec=True,
            ),
            mock.patch("pysensu_yelp.send_event", autospec=True),
            mock.patch('monitoring_tools.load_system_paasta_config', autospec=True),
        ) as (
            get_team_patch,
            get_tip_patch,
            get_notification_email_patch,
            get_irc_patch,
            get_ticket_patch,
            get_project_patch,
            get_page_patch,
            pysensu_yelp_send_event_patch,
            load_system_paasta_config_patch,
        ):
            load_system_paasta_config_patch.return_value.get_cluster = mock.Mock(return_value=self.fake_cluster)
            monitoring_tools.send_event(
                fake_service_name,
                fake_check_name,
                fake_monitoring_overrides,
                fake_status,
                fake_output,
                fake_soa_dir
            )
            get_team_patch.assert_called_once_with(
                fake_monitoring_overrides,
                fake_service_name,
                fake_soa_dir,
            )
            get_tip_patch.assert_called_once_with(
                fake_monitoring_overrides,
                fake_service_name,
                fake_soa_dir
            )
            get_notification_email_patch.assert_called_once_with(
                fake_monitoring_overrides,
                fake_service_name,
                fake_soa_dir
            )
            get_irc_patch.assert_called_once_with(
                fake_monitoring_overrides,
                fake_service_name,
                fake_soa_dir
            )
            get_page_patch.assert_called_once_with(
                fake_monitoring_overrides,
                fake_service_name,
                fake_soa_dir
            )
            pysensu_yelp_send_event_patch.assert_called_once_with(
                expected_check_name,
                expected_runbook,
                fake_status,
                fake_output,
                fake_team,
                **expected_kwargs
            )
            load_system_paasta_config_patch.return_value.get_cluster.assert_called_once_with()

    def test_read_monitoring_config(self):
        fake_name = 'partial'
        fake_fname = 'acronyms'
        fake_path = 'ever_patched'
        fake_soa_dir = '/nail/cte/oas'
        fake_dict = {'e': 'quail', 'v': 'snail'}
        with contextlib.nested(
            mock.patch('os.path.abspath', autospec=True, return_value=fake_path),
            mock.patch('os.path.join', autospec=True, return_value=fake_fname),
            mock.patch('service_configuration_lib.read_monitoring', autospec=True, return_value=fake_dict)
        ) as (
            abspath_patch,
            join_patch,
            read_monitoring_patch
        ):
            actual = monitoring_tools.read_monitoring_config(fake_name, fake_soa_dir)
            assert fake_dict == actual
            abspath_patch.assert_called_once_with(fake_soa_dir)
            join_patch.assert_called_once_with(fake_path, fake_name, 'monitoring.yaml')
            read_monitoring_patch.assert_called_once_with(fake_fname)
