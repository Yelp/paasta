#!/usr/bin/env python

import contextlib
import datetime
import re

import marathon
import mesos
import mock

from paasta_tools import marathon_tools, paasta_serviceinit
from paasta_tools.utils import PaastaColors


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
        cluster = 'my_cluster'
        service = 'my_service'
        instance = 'my_instance'
        app_id = 'mock_app_id'
        normal_instance_count = 5
        paasta_serviceinit.start_marathon_job(service, instance, app_id, normal_instance_count, client, cluster)
        client.scale_app.assert_called_once_with(app_id, instances=normal_instance_count, force=True)

    def test_stop_marathon_job(self):
        client = mock.create_autospec(marathon.MarathonClient)
        cluster = 'my_cluster'
        service = 'my_service'
        instance = 'my_instance'
        app_id = 'mock_app_id'
        paasta_serviceinit.stop_marathon_job(service, instance, app_id, client, cluster)
        client.scale_app.assert_called_once_with(app_id, instances=0, force=True)


class TestPaastaServiceStatus:
    def test_get_bouncing_status(self):
        with contextlib.nested(
            mock.patch('paasta_tools.paasta_serviceinit.marathon_tools.get_matching_appids', autospec=True),
        ) as (
            mock_get_matching_appids,
        ):
            mock_get_matching_appids.return_value = ['a', 'b']
            mock_config = marathon_tools.MarathonServiceConfig(
                'fake_service',
                'fake_instance',
                {'bounce_method': 'fake_bounce'},
                {},
            )
            actual = paasta_serviceinit.get_bouncing_status('fake_service', 'fake_instance', 'unused', mock_config)
            assert 'fake_bounce' in actual
            assert 'Bouncing' in actual

    def test_status_desired_state(self):
        with contextlib.nested(
            mock.patch('paasta_tools.paasta_serviceinit.get_bouncing_status', autospec=True),
            mock.patch('paasta_tools.paasta_serviceinit.get_desired_state_human', autospec=True),
        ) as (
            mock_get_bouncing_status,
            mock_get_desired_state_human,
        ):
            mock_get_bouncing_status.return_value = 'Bouncing (fake_bounce)'
            mock_get_desired_state_human.return_value = 'Started'
            actual = paasta_serviceinit.status_desired_state('fake_service', 'fake_instance', 'unused', 'unused')
            assert 'Started' in actual
            assert 'Bouncing' in actual

    def test_get_desired_state(self):
        fake_config = marathon_tools.MarathonServiceConfig(
            'fake_service',
            'fake_instance',
            {},
            {},
        )

        actual = paasta_serviceinit.get_desired_state_human(fake_config)
        assert 'Started' in actual

    def test_status_marathon_job_verbose(self):
        client = mock.create_autospec(marathon.MarathonClient)
        app = mock.create_autospec(marathon.models.app.MarathonApp)
        client.get_app.return_value = app
        service = 'my_service'
        instance = 'my_instance'
        task = mock.Mock()
        with contextlib.nested(
            mock.patch('paasta_tools.paasta_serviceinit.marathon_tools.get_matching_appids'),
            mock.patch('paasta_tools.paasta_serviceinit.get_verbose_status_of_marathon_app'),
        ) as (
            mock_get_matching_appids,
            mock_get_verbose_app,
        ):
            mock_get_matching_appids.return_value = ['/app1']
            mock_get_verbose_app.return_value = ([task], 'fake_return')
            tasks, out = paasta_serviceinit.status_marathon_job_verbose(service, instance, client)
            mock_get_matching_appids.assert_called_once_with(service, instance, client)
            mock_get_verbose_app.assert_called_once_with(app)
            assert tasks == [task]
            assert 'fake_return' in out

    def test_get_verbose_status_of_marathon_app(self):
        fake_app = mock.create_autospec(marathon.models.app.MarathonApp)
        fake_app.version = '2015-01-15T05:30:49.862Z'
        fake_app.id = '/fake--service'
        fake_task = mock.create_autospec(marathon.models.app.MarathonTask)
        fake_task.id = 'fake_task_id'
        fake_task.host = 'fake_deployed_host'
        fake_task.ports = [6666]
        fake_task.staged_at = datetime.datetime.fromtimestamp(0)
        fake_app.tasks = [fake_task]
        tasks, out = paasta_serviceinit.get_verbose_status_of_marathon_app(fake_app)
        assert 'fake_task_id' in out
        assert '/fake--service' in out
        assert 'App created: 2015-01-14 21:30:49' in out
        assert 'fake_deployed_host:6666' in out
        assert tasks == [fake_task]

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

    def test_pretty_print_haproxy_backend(self):
        pass

    def test_status_smartstack_backends_verbose_multiple_apps(self):
        service = 'my_service'
        instance = 'my_instance'
        cluster = 'fake_cluster'

        good_task = mock.Mock()
        bad_task = mock.Mock()
        other_task = mock.Mock()
        haproxy_backends_by_task = {
            good_task: {'status': 'UP', 'lastchg': '1', 'last_chk': 'OK',
                        'check_code': '200', 'svname': 'ipaddress1:1001_hostname1',
                        'check_status': 'L7OK', 'check_duration': 1},
            bad_task: {'status': 'UP', 'lastchg': '1', 'last_chk': 'OK',
                       'check_code': '200', 'svname': 'ipaddress2:1002_hostname2',
                       'check_status': 'L7OK', 'check_duration': 1},
        }
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance', autospec=True),
            mock.patch('paasta_tools.paasta_serviceinit.get_backends', autospec=True),
            mock.patch('paasta_tools.paasta_serviceinit.match_backends_and_tasks', autospec=True),
        ) as (
            mock_read_namespace_for_service_instance,
            mock_get_backends,
            mock_match_backends_and_tasks,
        ):
            mock_read_namespace_for_service_instance.return_value = instance
            mock_get_backends.return_value = haproxy_backends_by_task.values()
            mock_match_backends_and_tasks.return_value = [
                (haproxy_backends_by_task[good_task], good_task),
                (haproxy_backends_by_task[bad_task], None),
                (None, other_task),
            ]
            tasks = [good_task, other_task]
            actual = paasta_serviceinit.status_smartstack_backends_verbose(service, instance, cluster, tasks)
            assert re.search(r"%s[^\n]*hostname1:1001" % re.escape(PaastaColors.DEFAULT), actual)
            assert re.search(r"%s[^\n]*hostname2:1002" % re.escape(PaastaColors.GREY), actual)

    def test_status_smartstack_backends_verbose_emphasizes_maint_instances(self):
        service = 'my_service'
        instance = 'my_instance'
        cluster = 'fake_cluster'

        good_task = mock.Mock()
        other_task = mock.Mock()
        fake_backend = {'status': 'MAINT', 'lastchg': '1', 'last_chk': 'OK',
                        'check_code': '200', 'svname': 'ipaddress1:1001_hostname1',
                        'check_status': 'L7OK', 'check_duration': 1}
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance', autospec=True),
            mock.patch('paasta_tools.paasta_serviceinit.get_backends', autospec=True),
            mock.patch('paasta_tools.paasta_serviceinit.match_backends_and_tasks', autospec=True),
        ) as (
            mock_read_namespace_for_service_instance,
            mock_get_backends,
            mock_match_backends_and_tasks,
        ):
            mock_read_namespace_for_service_instance.return_value = instance
            mock_get_backends.return_value = [fake_backend]
            mock_match_backends_and_tasks.return_value = [
                (fake_backend, good_task),
            ]
            tasks = [good_task, other_task]
            actual = paasta_serviceinit.status_smartstack_backends_verbose(service, instance, cluster, tasks)
            assert PaastaColors.red('MAINT') in actual

    def test_status_smartstack_backends_verbose_demphasizes_maint_instances_for_unrelated_tasks(self):
        service = 'my_service'
        instance = 'my_instance'
        cluster = 'fake_cluster'

        good_task = mock.Mock()
        other_task = mock.Mock()
        fake_backend = {'status': 'MAINT', 'lastchg': '1', 'last_chk': 'OK',
                        'check_code': '200', 'svname': 'ipaddress1:1001_hostname1',
                        'check_status': 'L7OK', 'check_duration': 1}
        with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance', autospec=True),
            mock.patch('paasta_tools.paasta_serviceinit.get_backends', autospec=True),
            mock.patch('paasta_tools.paasta_serviceinit.match_backends_and_tasks', autospec=True),
        ) as (
            mock_read_namespace_for_service_instance,
            mock_get_backends,
            mock_match_backends_and_tasks,
        ):
            mock_read_namespace_for_service_instance.return_value = instance
            mock_get_backends.return_value = [fake_backend]
            mock_match_backends_and_tasks.return_value = [
                (fake_backend, None),
            ]
            tasks = [good_task, other_task]
            actual = paasta_serviceinit.status_smartstack_backends_verbose(service, instance, cluster, tasks)
            assert PaastaColors.red('MAINT') not in actual
            assert re.search(r"%s[^\n]*hostname1:1001" % re.escape(PaastaColors.GREY), actual)

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
            mock.patch('paasta_tools.paasta_serviceinit.get_running_tasks_from_active_frameworks'),
            mock.patch('paasta_tools.paasta_serviceinit.get_non_running_tasks_from_active_frameworks'),
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
        with mock.patch('paasta_tools.paasta_serviceinit.get_running_tasks_from_active_frameworks') as mock_tasks:
            mock_tasks.return_value = [
                {'id': 1}, {'id': 2}
            ]
            normal_count = 2
            actual = paasta_serviceinit.status_mesos_tasks('unused', 'unused', normal_count)
            assert 'Healthy' in actual

    def test_status_mesos_tasks_warning(self):
        with mock.patch('paasta_tools.paasta_serviceinit.get_running_tasks_from_active_frameworks') as mock_tasks:
            mock_tasks.return_value = [
                {'id': 1}, {'id': 2}
            ]
            normal_count = 4
            actual = paasta_serviceinit.status_mesos_tasks('unused', 'unused', normal_count)
            assert 'Warning' in actual

    def test_status_mesos_tasks_critical(self):
        with mock.patch('paasta_tools.paasta_serviceinit.get_tasks_from_active_frameworks') as mock_tasks:
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

    def test_get_cpu_usage_handles_missing_stats(self):
        fake_task = mock.create_autospec(mesos.cli.task.Task)
        fake_task.cpu_limit = 1.1
        fake_duration = 100
        fake_task.stats = {}
        fake_task.__getitem__.return_value = [{
            'state': 'TASK_RUNNING',
            'timestamp': int(datetime.datetime.now().strftime('%s')) - fake_duration,
        }]
        actual = paasta_serviceinit.get_cpu_usage(fake_task)
        assert "0.0%" in actual

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

    def test_get_mem_usage_divide_by_zero(self):
        fake_task = mock.create_autospec(mesos.cli.task.Task)
        fake_task.rss = 1024 * 1024 * 10
        fake_task.mem_limit = 0
        actual = paasta_serviceinit.get_mem_usage(fake_task)
        assert actual == "Undef"

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
