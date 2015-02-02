#!/usr/bin/env python

import contextlib
import datetime

import marathon
import mesos
import mock

from paasta_tools import paasta_serviceinit
from paasta_tools.paasta_cli.utils import PaastaColors


class TestPaastaServiceinit:

    def test_validate_service_instance_valid(self):
        mock_services = [('service1', 'main'), ('service2', 'main')]
        my_service = 'service1'
        my_instance = 'main'
        fake_cluster = 'fake_cluster'
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_marathon_services_for_cluster', return_value=mock_services)
        ) as (
            get_marathon_services_patch,
        ):
            assert paasta_serviceinit.validate_service_instance(my_service, my_instance, fake_cluster) is True
            get_marathon_services_patch.assert_called_once_with(fake_cluster)

    def test_validate_service_instance_invalid(self):
        mock_services = [('service1', 'main'), ('service2', 'main')]
        my_service = 'bad_service'
        my_instance = 'main'
        fake_cluster = 'fake_cluster'
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.get_marathon_services_for_cluster', return_value=mock_services),
            mock.patch('sys.exit'),
        ) as (
            get_marathon_services_patch,
            sys_exit_patch,
        ):
            assert paasta_serviceinit.validate_service_instance(my_service, my_instance, fake_cluster) is True
            sys_exit_patch.assert_called_once_with(3)
            get_marathon_services_patch.assert_called_once_with(fake_cluster)

    def test_start_marathon_job(self):
        client = mock.create_autospec(marathon.MarathonClient)
        service = 'my_service'
        instance = 'my_instance'
        app_id = 'mock_app_id'
        normal_instance_count = 5
        paasta_serviceinit.start_marathon_job(service, instance, app_id, normal_instance_count, client)
        client.scale_app.assert_called_once_with(app_id, instances=normal_instance_count, force=True)

    def test_stop_marathon_job(self):
        client = mock.create_autospec(marathon.MarathonClient)
        service = 'my_service'
        instance = 'my_instance'
        app_id = 'mock_app_id'
        paasta_serviceinit.stop_marathon_job(service, instance, app_id, client)
        client.scale_app.assert_called_once_with(app_id, instances=0, force=True)


class TestPaastaServiceStatus:
    def test_status_marathon_job_verbose(self):
        client = mock.create_autospec(marathon.MarathonClient)
        app = mock.create_autospec(marathon.models.app.MarathonApp)
        client.get_app.return_value = app
        service = 'my_service'
        instance = 'my_instance'
        with contextlib.nested(
            mock.patch('paasta_tools.paasta_serviceinit.marathon_tools.get_matching_appids'),
            mock.patch('paasta_tools.paasta_serviceinit.get_verbose_status_of_marathon_app'),
        ) as (
            mock_get_matching_appids,
            mock_get_verbose_app,
        ):
            mock_get_matching_appids.return_value = ['/app1']
            mock_get_verbose_app.return_value = 'fake_return'
            actual = paasta_serviceinit.status_marathon_job_verbose(service, instance, client)
            mock_get_matching_appids.assert_called_once_with(service, instance, client)
            mock_get_verbose_app.assert_called_once_with(app)
            assert 'fake_return' in actual

    def test_get_verbose_status_of_marathon_app(self):
        fake_app = mock.create_autospec(marathon.models.app.MarathonApp)
        fake_app.version = '2015-01-15T05:30:49.862Z'
        fake_app.id = '/fake--service'
        fake_task = mock.create_autospec(marathon.models.app.MarathonTask)
        fake_task.id = 'fake_task_id'
        fake_task.host = 'fake_deployed_host'
        fake_task.staged_at = datetime.datetime.fromtimestamp(0)
        fake_app.tasks = [fake_task]
        actual = paasta_serviceinit.get_verbose_status_of_marathon_app(fake_app)
        assert 'fake_task_id' in actual
        assert '/fake--service' in actual
        assert 'App created: 2015-01-15 05:30:49' in actual
        assert 'fake_deployed_host' in actual

    def test_status_marathon_job_when_running(self):
        client = mock.create_autospec(marathon.MarathonClient)
        app = mock.create_autospec(marathon.models.app.MarathonApp)
        client.get_app.return_value = app
        service = 'my_service'
        instance = 'my_instance'
        app_id = 'mock_app_id'
        normal_instance_count = 5
        mock_tasks_running = 5
        app.tasks_running = mock_tasks_running
        app.deployments = []
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.is_app_id_running', return_value=True),
        ) as (
            is_app_id_running_patch,
        ):
            paasta_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
            is_app_id_running_patch.assert_called_once_with(app_id, client)

    def tests_status_marathon_job_when_running_running_no_tasks(self):
        client = mock.create_autospec(marathon.MarathonClient)
        app = mock.create_autospec(marathon.models.app.MarathonApp)
        client.get_app.return_value = app
        service = 'my_service'
        instance = 'my_instance'
        app_id = 'mock_app_id'
        normal_instance_count = 5
        mock_tasks_running = 0
        app.tasks_running = mock_tasks_running
        app.deployments = []
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.is_app_id_running', return_value=True),
        ) as (
            is_app_id_running_patch,
        ):
            paasta_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
            is_app_id_running_patch.assert_called_once_with(app_id, client)

    def tests_status_marathon_job_when_running_not_running(self):
        client = mock.create_autospec(marathon.MarathonClient)
        service = 'my_service'
        instance = 'my_instance'
        app_id = 'mock_app_id'
        normal_instance_count = 5
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.is_app_id_running', return_value=True),
        ) as (
            is_app_id_running_patch,
        ):
            paasta_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
            is_app_id_running_patch.assert_called_once_with(app_id, client)

    def tests_status_marathon_job_when_running_running_tasks_with_deployments(self):
        client = mock.create_autospec(marathon.MarathonClient)
        app = mock.create_autospec(marathon.models.app.MarathonApp)
        client.get_app.return_value = app
        service = 'my_service'
        instance = 'my_instance'
        app_id = 'mock_app_id'
        normal_instance_count = 5
        mock_tasks_running = 0
        app.tasks_running = mock_tasks_running
        app.deployments = ['test_deployment']
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.is_app_id_running', return_value=True),
        ) as (
            is_app_id_running_patch,
        ):
            output = paasta_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
            is_app_id_running_patch.assert_called_once_with(app_id, client)
            assert 'Deploying' in output

    def test_status_smartstack_backends_verbose(self):
        service = 'my_service'
        instance = 'my_instance'
        cluster = 'fake_cluster'
        actual = paasta_serviceinit.status_smartstack_backends_verbose(service, instance, cluster)
        assert None is actual

    def test_status_smartstack_backends_different_nerve_ns(self):
        service = 'my_service'
        instance = 'my_instance'
        cluster = 'fake_cluster'
        different_ns = 'other_instance'
        normal_count = 10
        with mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance') as read_ns_mock:
            read_ns_mock.return_value = different_ns
            actual = paasta_serviceinit.status_smartstack_backends(service, instance, normal_count, cluster)
            assert "is announced in the" in actual
            assert different_ns in actual

    def test_status_smartstack_backends_working(self):
        service = 'my_service'
        instance = 'my_instance'
        service_instance = "%s.%s" % (service, instance)
        cluster = 'fake_cluster'
        normal_count = 10
        fake_up_backends = 11
        with contextlib.nested(
            mock.patch('paasta_tools.paasta_serviceinit.get_replication_for_services'),
            mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance'),
            mock.patch('paasta_tools.paasta_serviceinit.haproxy_backend_report'),
        ) as (
            get_replication_for_services_patch,
            read_ns_patch,
            backend_report_patch,
        ):
            read_ns_patch.return_value = instance
            backend_report_patch.return_value = "fake_report"
            get_replication_for_services_patch.return_value = {service_instance: fake_up_backends}
            actual = paasta_serviceinit.status_smartstack_backends(service, instance, normal_count, cluster)
            backend_report_patch.assert_called_once_with(normal_count, fake_up_backends)
            assert "Smartstack: fake_report" in actual

    def test_haproxy_backend_report_healthy(self):
        normal_count = 10
        actual_count = 11
        status = paasta_serviceinit.haproxy_backend_report(normal_count, actual_count)
        assert "Healthy" in status

    def test_haproxy_backend_report_warning(self):
        normal_count = 10
        actual_count = 1
        status = paasta_serviceinit.haproxy_backend_report(normal_count, actual_count)
        assert "Warning" in status

    def test_haproxy_backend_report_critical(self):
        normal_count = 10
        actual_count = 0
        status = paasta_serviceinit.haproxy_backend_report(normal_count, actual_count)
        assert "Critical" in status

    def test_status_mesos_tasks_verbose(self):
        with contextlib.nested(
            mock.patch('paasta_tools.paasta_serviceinit.get_running_mesos_tasks_for_service'),
            mock.patch('paasta_tools.paasta_serviceinit.get_non_running_mesos_tasks_for_service'),
        ) as (
            get_running_mesos_tasks_for_service_patch,
            get_non_running_mesos_tasks_for_service_patch,
        ):
            get_running_mesos_tasks_for_service_patch.return_value = []
            get_non_running_mesos_tasks_for_service_patch.return_value = []
            actual = paasta_serviceinit.status_mesos_tasks_verbose('fake_service', 'fake_instance')
            assert 'Running Tasks' in actual
            assert 'Non-Running Tasks' in actual

    def test_status_mesos_tasks_working(self):
        with mock.patch('paasta_tools.paasta_serviceinit.get_running_mesos_tasks_for_service') as mock_tasks:
            mock_tasks.return_value = [
                {'id': 1}, {'id': 2}
            ]
            normal_count = 2
            actual = paasta_serviceinit.status_mesos_tasks('unused', 'unused', normal_count)
            assert 'Healthy' in actual

    def test_status_mesos_tasks_warning(self):
        with mock.patch('paasta_tools.paasta_serviceinit.get_running_mesos_tasks_for_service') as mock_tasks:
            mock_tasks.return_value = [
                {'id': 1}, {'id': 2}
            ]
            normal_count = 4
            actual = paasta_serviceinit.status_mesos_tasks('unused', 'unused', normal_count)
            assert 'Warning' in actual

    def test_status_mesos_tasks_critical(self):
        with mock.patch('paasta_tools.paasta_serviceinit.get_running_mesos_tasks_for_service') as mock_tasks:
            mock_tasks.return_value = []
            normal_count = 10
            actual = paasta_serviceinit.status_mesos_tasks('unused', 'unused', normal_count)
            assert 'Critical' in actual

    def test_get_cpu_usage_good(self):
        fake_task = mock.create_autospec(mesos.cli.task.Task)
        fake_task.cpu_limit = .35
        fake_duration = 100
        fake_task.stats = {
            'cpus_system_time_secs': 2.5,
            'cpus_user_time_secs': 0.0,
        }
        fake_task.__getitem__.return_value = [{
               'state': 'TASK_RUNNING',
               'timestamp': int(datetime.datetime.now().strftime('%s')) - fake_duration,
        }]
        actual = paasta_serviceinit.get_cpu_usage(fake_task)
        assert '10.0%' == actual

    def test_get_cpu_usage_bad(self):
        fake_task = mock.create_autospec(mesos.cli.task.Task)
        fake_task.cpu_limit = 1.1
        fake_duration = 100
        fake_task.stats = {
            'cpus_system_time_secs': 50.0,
            'cpus_user_time_secs': 50.0,
        }
        fake_task.__getitem__.return_value = [{
            'state': 'TASK_RUNNING',
            'timestamp': int(datetime.datetime.now().strftime('%s')) - fake_duration,
        }]
        actual = paasta_serviceinit.get_cpu_usage(fake_task)
        assert PaastaColors.red('100.0%') in actual

    def test_get_mem_usage_good(self):
        fake_task = mock.create_autospec(mesos.cli.task.Task)
        fake_task.rss = 1024 * 1024 * 10
        fake_task.mem_limit = fake_task.rss * 10
        actual = paasta_serviceinit.get_mem_usage(fake_task)
        assert actual == '10/100MB'

    def test_get_mem_usage_bad(self):
        fake_task = mock.create_autospec(mesos.cli.task.Task)
        fake_task.rss = 1024 * 1024 * 100
        fake_task.mem_limit = fake_task.rss
        actual = paasta_serviceinit.get_mem_usage(fake_task)
        assert actual == PaastaColors.red('100/100MB')


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
