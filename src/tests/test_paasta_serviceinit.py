#!/usr/bin/env python

import contextlib
from StringIO import StringIO

import marathon
import mock

import service_deployment_tools.paasta_serviceinit


class TestPaastaServiceinit:

    def test_validate_service_instance_valid(self):
        mock_services = [('service1', 'main'), ('service2', 'main')]
        my_service = 'service1'
        my_instance = 'main'
        fake_cluster = 'fake_cluster'
        with contextlib.nested(
            mock.patch('service_deployment_tools.marathon_tools.get_marathon_services_for_cluster', return_value=mock_services)
        ) as (
            get_marathon_services_patch,
        ):
            assert service_deployment_tools.paasta_serviceinit.validate_service_instance(my_service, my_instance, fake_cluster) is True
            get_marathon_services_patch.assert_called_once_with(fake_cluster)

    def test_validate_service_instance_invalid(self):
        mock_services = [('service1', 'main'), ('service2', 'main')]
        my_service = 'bad_service'
        my_instance = 'main'
        fake_cluster = 'fake_cluster'
        with contextlib.nested(
            mock.patch('service_deployment_tools.marathon_tools.get_marathon_services_for_cluster', return_value=mock_services),
            mock.patch('sys.exit'),
        ) as (
            get_marathon_services_patch,
            sys_exit_patch
        ):
            assert service_deployment_tools.paasta_serviceinit.validate_service_instance(my_service, my_instance, fake_cluster) is True
            get_marathon_services_patch.assert_called_once_with(fake_cluster)
            sys_exit_patch.assert_called_once_with(3)

    def test_start_marathon_job(self):
        client = mock.create_autospec(marathon.MarathonClient)
        service = 'my_service'
        instance = 'my_instance'
        app_id = 'mock_app_id'
        normal_instance_count = 5
        service_deployment_tools.paasta_serviceinit.start_marathon_job(service, instance, app_id, normal_instance_count, client)
        client.scale_app.assert_called_once_with(app_id, instances=normal_instance_count)

    def test_stop_marathon_job(self):
        client = mock.create_autospec(marathon.MarathonClient)
        service = 'my_service'
        instance = 'my_instance'
        app_id = 'mock_app_id'
        service_deployment_tools.paasta_serviceinit.stop_marathon_job(service, instance, app_id, client)
        client.scale_app.assert_called_once_with(app_id, instances=0)


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
            mock.patch('service_deployment_tools.marathon_tools.is_app_id_running', return_value=True),
            mock.patch('sys.exit'),
        ) as (
            is_app_id_running_patch,
            sys_exit_patch,
        ):
            service_deployment_tools.paasta_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
            is_app_id_running_patch.assert_called_once_with(app_id, client)
            sys_exit_patch.assert_called_once_with(0)

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
            mock.patch('service_deployment_tools.marathon_tools.is_app_id_running', return_value=True),
            mock.patch('sys.exit'),
        ) as (
            is_app_id_running_patch,
            sys_exit_patch,
        ):
            service_deployment_tools.paasta_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
            is_app_id_running_patch.assert_called_once_with(app_id, client)
            sys_exit_patch.assert_called_once_with(1)

    def tests_status_marathon_job_when_running_not_running(self):
        client = mock.create_autospec(marathon.MarathonClient)
        service = 'my_service'
        instance = 'my_instance'
        app_id = 'mock_app_id'
        normal_instance_count = 5
        with contextlib.nested(
            mock.patch('service_deployment_tools.marathon_tools.is_app_id_running', return_value=True),
            mock.patch('sys.exit'),
        ) as (
            is_app_id_running_patch,
            sys_exit_patch,
        ):
            service_deployment_tools.paasta_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
            is_app_id_running_patch.assert_called_once_with(app_id, client)
            sys_exit_patch.assert_called_once_with(0)

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
            mock.patch('service_deployment_tools.marathon_tools.is_app_id_running', return_value=True),
            mock.patch('sys.exit'),
            mock.patch('sys.stdout', new_callable=StringIO)
        ) as (
            is_app_id_running_patch,
            sys_exit_patch,
            std_out_patch,
        ):
            service_deployment_tools.paasta_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
            is_app_id_running_patch.assert_called_once_with(app_id, client)
            sys_exit_patch.assert_called_once_with(1)
            output = std_out_patch.getvalue()
            assert 'Deploying' in output

    def test_main(self):
        pass

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
