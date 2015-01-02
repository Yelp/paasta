#!/usr/bin/env python

import contextlib

import marathon
import mock

import paasta_tools.paasta_serviceinit


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
            assert paasta_tools.paasta_serviceinit.validate_service_instance(my_service, my_instance, fake_cluster) is True
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
            assert paasta_tools.paasta_serviceinit.validate_service_instance(my_service, my_instance, fake_cluster) is True
            sys_exit_patch.assert_called_once_with(3)
            get_marathon_services_patch.assert_called_once_with(fake_cluster)

    def test_start_marathon_job(self):
        client = mock.create_autospec(marathon.MarathonClient)
        service = 'my_service'
        instance = 'my_instance'
        app_id = 'mock_app_id'
        normal_instance_count = 5
        paasta_tools.paasta_serviceinit.start_marathon_job(service, instance, app_id, normal_instance_count, client)
        client.scale_app.assert_called_once_with(app_id, instances=normal_instance_count, force=True)

    def test_stop_marathon_job(self):
        client = mock.create_autospec(marathon.MarathonClient)
        service = 'my_service'
        instance = 'my_instance'
        app_id = 'mock_app_id'
        paasta_tools.paasta_serviceinit.stop_marathon_job(service, instance, app_id, client)
        client.scale_app.assert_called_once_with(app_id, instances=0, force=True)


class TestPaastaServiceStatus:
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
            paasta_tools.paasta_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
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
            paasta_tools.paasta_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
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
            paasta_tools.paasta_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
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
            output = paasta_tools.paasta_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
            is_app_id_running_patch.assert_called_once_with(app_id, client)
            assert 'Deploying' in output

    def test_status_smartstack_backends_different_nerve_ns(self):
        service = 'my_service'
        instance = 'my_instance'
        cluster = 'fake_cluster'
        different_ns = 'other_instance'
        normal_count = 10
        with mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance') as read_ns_mock:
            read_ns_mock.return_value = different_ns
            actual = paasta_tools.paasta_serviceinit.status_smartstack_backends(service, instance, normal_count, cluster)
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
            actual = paasta_tools.paasta_serviceinit.status_smartstack_backends(service, instance, normal_count, cluster)
            backend_report_patch.assert_called_once_with(normal_count, fake_up_backends)
            assert "Smartstack: fake_report" in actual

    def test_haproxy_backend_report_healthy(self):
        normal_count = 10
        actual_count = 11
        status = paasta_tools.paasta_serviceinit.haproxy_backend_report(normal_count, actual_count)
        assert "Healthy" in status

    def test_haproxy_backend_report_warning(self):
        normal_count = 10
        actual_count = 1
        status = paasta_tools.paasta_serviceinit.haproxy_backend_report(normal_count, actual_count)
        assert "Warning" in status

    def test_haproxy_backend_report_critical(self):
        normal_count = 10
        actual_count = 0
        status = paasta_tools.paasta_serviceinit.haproxy_backend_report(normal_count, actual_count)
        assert "Critical" in status

    def test_status_mesos_tasks_working(self):
        with mock.patch('paasta_tools.paasta_serviceinit.get_running_mesos_tasks_for_service') as mock_tasks:
            mock_tasks.return_value = [
                {'id': 1}, {'id': 2}
            ]
            normal_count = 2
            actual = paasta_tools.paasta_serviceinit.status_mesos_tasks('unused', 'unused', normal_count)
            assert 'Healthy' in actual

    def test_status_mesos_tasks_warning(self):
        with mock.patch('paasta_tools.paasta_serviceinit.get_running_mesos_tasks_for_service') as mock_tasks:
            mock_tasks.return_value = [
                {'id': 1}, {'id': 2}
            ]
            normal_count = 4
            actual = paasta_tools.paasta_serviceinit.status_mesos_tasks('unused', 'unused', normal_count)
            assert 'Warning' in actual

    def test_status_mesos_tasks_critical(self):
        with mock.patch('paasta_tools.paasta_serviceinit.get_running_mesos_tasks_for_service') as mock_tasks:
            mock_tasks.return_value = []
            normal_count = 10
            actual = paasta_tools.paasta_serviceinit.status_mesos_tasks('unused', 'unused', normal_count)
            assert 'Critical' in actual

    def test_main(self):
        pass

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
