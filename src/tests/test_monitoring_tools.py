import contextlib
import mock
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
    empty_service_config = {}
    job_page = False
    fake_job_config = {
        'team': 'job_test_team',
        'runbook': 'y/job_test_runbook',
        'tip': 'job_test_tip',
        'notification_email': 'job_test_notification_email',
        'page': job_page
    }
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
    instance_name = 'fake_instance'
    service_name = 'fake_service'

    def test_get_team(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_team(self.framework, self.service_name, self.instance_name)
            get_monitoring_config_value_patch.assert_called_once_with('team', self.framework, self.service_name, self.instance_name)

    def test_get_runbook(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_runbook(self.framework, self.service_name, self.instance_name)
            get_monitoring_config_value_patch.assert_called_once_with('runbook', self.framework, self.service_name, self.instance_name)

    def test_get_tip(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_tip(self.framework, self.service_name, self.instance_name)
            get_monitoring_config_value_patch.assert_called_once_with('tip', self.framework, self.service_name, self.instance_name)

    def test_get_notification_email(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_notification_email(self.framework, self.service_name, self.instance_name)
            get_monitoring_config_value_patch.assert_called_once_with('notification_email', self.framework, self.service_name, self.instance_name)

    def test_get_page(self):
        with mock.patch('monitoring_tools.__get_monitoring_config_value') as get_monitoring_config_value_patch:
            monitoring_tools.get_page(self.framework, self.service_name, self.instance_name)
            get_monitoring_config_value_patch.assert_called_once_with('page', self.framework, self.service_name, self.instance_name)

    def test_get_monitoring_config_value_with_job_config(self):
        expected = 'job_test_team'
        with contextlib.nested(
            mock.patch('service_configuration_lib.read_service_configuration',
                       return_value=self.fake_general_service_config),
            mock.patch('marathon_tools.read_service_config', return_value=self.fake_job_config),
            mock.patch('marathon_tools.read_monitoring_config', return_value=self.fake_monitor_config),
        ) as (
            service_configuration_lib_patch,
            read_service_patch,
            read_monitoring_patch
        ):
            actual = monitoring_tools.get_team(self.framework, self.service_name, self.instance_name)
            assert expected == actual
            service_configuration_lib_patch.assert_called_once_with(self.service_name)
            read_service_patch.assert_called_once_with(self.service_name, self.instance_name)
            read_monitoring_patch.assert_called_once_with(self.service_name)

    def test_get_monitoring_config_value_with_monitor_config(self):
        expected = 'monitor_test_team'
        with contextlib.nested(
            mock.patch('service_configuration_lib.read_service_configuration',
                       return_value=self.fake_general_service_config),
            mock.patch('marathon_tools.read_service_config', return_value=self.empty_job_config),
            mock.patch('marathon_tools.read_monitoring_config', return_value=self.fake_monitor_config),
        ) as (
            service_configuration_lib_patch,
            read_service_patch,
            read_monitoring_patch
        ):
            actual = monitoring_tools.get_team(self.framework, self.service_name, self.instance_name)
            assert expected == actual
            service_configuration_lib_patch.assert_called_once_with(self.service_name)
            read_service_patch.assert_called_once_with(self.service_name, self.instance_name)
            read_monitoring_patch.assert_called_once_with(self.service_name)

    def test_get_monitoring_config_value_with_service_config(self):
        expected = 'general_test_team'
        with contextlib.nested(
            mock.patch('service_configuration_lib.read_service_configuration',
                       return_value=self.fake_general_service_config),
            mock.patch('marathon_tools.read_service_config', return_value=self.empty_job_config),
            mock.patch('marathon_tools.read_monitoring_config', return_value=self.empty_monitor_config),
        ) as (
            service_configuration_lib_patch,
            read_service_patch,
            read_monitoring_patch
        ):
            actual = monitoring_tools.get_team(self.framework, self.service_name, self.instance_name)
            assert expected == actual
            service_configuration_lib_patch.assert_called_once_with(self.service_name)
            read_service_patch.assert_called_once_with(self.service_name, self.instance_name)
            read_monitoring_patch.assert_called_once_with(self.service_name)

    def test_get_monitoring_config_value_with_defaults(self):
        expected = False
        with contextlib.nested(
            mock.patch('service_configuration_lib.read_service_configuration',
                       return_value=self.empty_job_config),
            mock.patch('marathon_tools.read_service_config', return_value=self.empty_job_config),
            mock.patch('marathon_tools.read_monitoring_config', return_value=self.empty_monitor_config),
        ) as (
            service_configuration_lib_patch,
            read_service_patch,
            read_monitoring_patch
        ):
            actual = monitoring_tools.get_team(self.framework, self.service_name, self.instance_name)
            assert expected == actual
            service_configuration_lib_patch.assert_called_once_with(self.service_name)
            read_service_patch.assert_called_once_with(self.service_name, self.instance_name)
            read_monitoring_patch.assert_called_once_with(self.service_name)
