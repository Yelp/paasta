#!/usr/bin/env python

import contextlib
import datetime
import re

import marathon
import mesos
import mock

from paasta_tools import marathon_tools, marathon_serviceinit
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import PaastaColors


def test_validate_service_instance_valid():
    mock_services = [('service1', 'main'), ('service2', 'main')]
    my_service = 'service1'
    my_instance = 'main'
    fake_cluster = 'fake_cluster'
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_serviceinit.get_services_for_cluster',
                   autospec=True,
                   return_value=mock_services),
    ) as (
        get_services_for_cluster_patch,
    ):
        assert marathon_serviceinit.validate_service_instance(my_service, my_instance, fake_cluster) is True
        get_services_for_cluster_patch.assert_called_once_with(cluster=fake_cluster, instance_type='marathon')


def test_validate_service_instance_invalid():
    mock_services = [('service1', 'main'), ('service2', 'main')]
    my_service = 'bad_service'
    my_instance = 'main'
    fake_cluster = 'fake_cluster'
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_serviceinit.get_services_for_cluster',
                   autospec=True,
                   return_value=mock_services),
        mock.patch('sys.exit'),
    ) as (
        get_services_for_cluster_patch,
        sys_exit_patch,
    ):
        assert marathon_serviceinit.validate_service_instance(my_service, my_instance, fake_cluster) is True
        sys_exit_patch.assert_called_once_with(3)
        get_services_for_cluster_patch.assert_called_once_with(cluster=fake_cluster, instance_type='marathon')


def test_start_marathon_job():
    client = mock.create_autospec(marathon.MarathonClient)
    cluster = 'my_cluster'
    service = 'my_service'
    instance = 'my_instance'
    app_id = 'mock_app_id'
    normal_instance_count = 5
    marathon_serviceinit.start_marathon_job(service, instance, app_id, normal_instance_count, client, cluster)
    client.scale_app.assert_called_once_with(app_id, instances=normal_instance_count, force=True)


def test_stop_marathon_job():
    client = mock.create_autospec(marathon.MarathonClient)
    cluster = 'my_cluster'
    service = 'my_service'
    instance = 'my_instance'
    app_id = 'mock_app_id'
    marathon_serviceinit.stop_marathon_job(service, instance, app_id, client, cluster)
    client.scale_app.assert_called_once_with(app_id, instances=0, force=True)


def test_get_bouncing_status():
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_serviceinit.marathon_tools.get_matching_appids', autospec=True),
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
        actual = marathon_serviceinit.get_bouncing_status('fake_service', 'fake_instance', 'unused', mock_config)
        assert 'fake_bounce' in actual
        assert 'Bouncing' in actual


def test_status_desired_state():
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_serviceinit.get_bouncing_status', autospec=True),
        mock.patch('paasta_tools.marathon_serviceinit.get_desired_state_human', autospec=True),
    ) as (
        mock_get_bouncing_status,
        mock_get_desired_state_human,
    ):
        mock_get_bouncing_status.return_value = 'Bouncing (fake_bounce)'
        mock_get_desired_state_human.return_value = 'Started'
        actual = marathon_serviceinit.status_desired_state('fake_service', 'fake_instance', 'unused', 'unused')
        assert 'Started' in actual
        assert 'Bouncing' in actual


def test_get_desired_state():
    fake_config = marathon_tools.MarathonServiceConfig(
        'fake_service',
        'fake_instance',
        {},
        {},
    )

    actual = marathon_serviceinit.get_desired_state_human(fake_config)
    assert 'Started' in actual


def test_status_marathon_job_verbose():
    client = mock.create_autospec(marathon.MarathonClient)
    app = mock.create_autospec(marathon.models.app.MarathonApp)
    client.get_app.return_value = app
    service = 'my_service'
    instance = 'my_instance'
    task = mock.Mock()
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_serviceinit.marathon_tools.get_matching_appids'),
        mock.patch('paasta_tools.marathon_serviceinit.get_verbose_status_of_marathon_app'),
    ) as (
        mock_get_matching_appids,
        mock_get_verbose_app,
    ):
        mock_get_matching_appids.return_value = ['/app1']
        mock_get_verbose_app.return_value = ([task], 'fake_return')
        tasks, out = marathon_serviceinit.status_marathon_job_verbose(service, instance, client)
        mock_get_matching_appids.assert_called_once_with(service, instance, client)
        mock_get_verbose_app.assert_called_once_with(app)
        assert tasks == [task]
        assert 'fake_return' in out


def test_get_verbose_status_of_marathon_app():
    fake_app = mock.create_autospec(marathon.models.app.MarathonApp)
    fake_app.version = '2015-01-15T05:30:49.862Z'
    fake_app.id = '/fake--service'
    fake_task = mock.create_autospec(marathon.models.app.MarathonTask)
    fake_task.id = 'fake_task_id'
    fake_task.host = 'fake_deployed_host'
    fake_task.ports = [6666]
    fake_task.staged_at = datetime.datetime.fromtimestamp(0)
    fake_app.tasks = [fake_task]
    tasks, out = marathon_serviceinit.get_verbose_status_of_marathon_app(fake_app)
    assert 'fake_task_id' in out
    assert '/fake--service' in out
    assert 'App created: 2015-01-14 21:30:49' in out
    assert 'fake_deployed_host:6666' in out
    assert tasks == [fake_task]


def test_status_marathon_job_when_running():
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
        marathon_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
        is_app_id_running_patch.assert_called_once_with(app_id, client)


def tests_status_marathon_job_when_running_running_no_tasks():
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
        marathon_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
        is_app_id_running_patch.assert_called_once_with(app_id, client)


def tests_status_marathon_job_when_running_not_running():
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
        marathon_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
        is_app_id_running_patch.assert_called_once_with(app_id, client)


def tests_status_marathon_job_when_running_running_tasks_with_deployments():
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
        output = marathon_serviceinit.status_marathon_job(service, instance, app_id, normal_instance_count, client)
        is_app_id_running_patch.assert_called_once_with(app_id, client)
        assert 'Deploying' in output


def test_pretty_print_haproxy_backend():
    pass


def test_status_smartstack_backends_normal():
    service = 'my_service'
    instance = 'my_instance'
    service_instance = "%s.%s" % (service, instance)

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
        mock.patch('paasta_tools.marathon_tools.load_service_namespace_config', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance'),
        mock.patch('paasta_tools.marathon_serviceinit.get_mesos_slaves_grouped_by_attribute'),
        mock.patch('paasta_tools.marathon_serviceinit.get_backends', autospec=True),
        mock.patch('paasta_tools.marathon_serviceinit.match_backends_and_tasks', autospec=True),
    ) as (
        mock_load_service_namespace_config,
        mock_read_ns,
        mock_get_mesos_slaves_grouped_by_attribute,
        mock_get_backends,
        mock_match_backends_and_tasks,
    ):
        mock_load_service_namespace_config.return_value.get_discover.return_value = 'fake_discover'
        mock_read_ns.return_value = instance
        mock_get_backends.return_value = haproxy_backends_by_task.values()
        mock_match_backends_and_tasks.return_value = [
            (haproxy_backends_by_task[good_task], good_task),
            (haproxy_backends_by_task[bad_task], None),
            (None, other_task),
        ]
        tasks = [good_task, other_task]
        mock_get_mesos_slaves_grouped_by_attribute.return_value = {'fake_location1': ['fakehost1']}
        actual = marathon_serviceinit.status_smartstack_backends(
            service,
            instance,
            cluster,
            tasks,
            len(haproxy_backends_by_task),
            None,
            False,
        )
        mock_get_backends.assert_called_once_with(service_instance, synapse_host_port='fakehost1:3212')
        assert "fake_location1" in actual
        assert "Healthy" in actual


def test_status_smartstack_backends_different_nerve_ns():
    service = 'my_service'
    instance = 'my_instance'
    cluster = 'fake_cluster'
    different_ns = 'other_instance'
    normal_count = 10
    tasks = mock.Mock()
    with mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance') as read_ns_mock:
        read_ns_mock.return_value = different_ns
        actual = marathon_serviceinit.status_smartstack_backends(
            service,
            instance,
            cluster,
            tasks,
            normal_count,
            None,
            False,
        )
        assert "is announced in the" in actual
        assert different_ns in actual


def test_status_smartstack_backends_no_smartstack_replication_info():
    service = 'my_service'
    instance = 'my_instance'
    service_instance = "%s.%s" % (service, instance)
    cluster = 'fake_cluster'
    tasks = mock.Mock()
    normal_count = 10
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.load_service_namespace_config', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance'),
        mock.patch('paasta_tools.marathon_serviceinit.get_mesos_slaves_grouped_by_attribute'),
    ) as (
        mock_load_service_namespace_config,
        mock_read_ns,
        mock_get_mesos_slaves_grouped_by_attribute,
    ):
        mock_load_service_namespace_config.return_value.get_discover.return_value = 'fake_discover'
        mock_read_ns.return_value = instance
        mock_get_mesos_slaves_grouped_by_attribute.return_value = {}
        actual = marathon_serviceinit.status_smartstack_backends(
            service,
            instance,
            cluster,
            tasks,
            normal_count,
            None,
            False,
        )
        assert "%s is NOT in smartstack" % service_instance in actual


def test_status_smartstack_backends_multiple_locations():
    service = 'my_service'
    instance = 'my_instance'
    service_instance = "%s.%s" % (service, instance)
    cluster = 'fake_cluster'
    good_task = mock.Mock()
    other_task = mock.Mock()
    fake_backend = {'status': 'UP', 'lastchg': '1', 'last_chk': 'OK',
                    'check_code': '200', 'svname': 'ipaddress1:1001_hostname1',
                    'check_status': 'L7OK', 'check_duration': 1}
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.load_service_namespace_config', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance'),
        mock.patch('paasta_tools.marathon_serviceinit.get_mesos_slaves_grouped_by_attribute'),
        mock.patch('paasta_tools.marathon_serviceinit.get_backends', autospec=True),
        mock.patch('paasta_tools.marathon_serviceinit.match_backends_and_tasks', autospec=True),
    ) as (
        mock_load_service_namespace_config,
        mock_read_ns,
        mock_get_mesos_slaves_grouped_by_attribute,
        mock_get_backends,
        mock_match_backends_and_tasks,
    ):
        mock_load_service_namespace_config.return_value.get_discover.return_value = 'fake_discover'
        mock_read_ns.return_value = instance
        mock_get_backends.return_value = [fake_backend]
        mock_match_backends_and_tasks.return_value = [
            (fake_backend, good_task),
        ]
        tasks = [good_task, other_task]
        mock_get_mesos_slaves_grouped_by_attribute.return_value = {
            'fake_location1': ['fakehost1'],
            'fake_location2': ['fakehost2'],
        }
        actual = marathon_serviceinit.status_smartstack_backends(
            service,
            instance,
            cluster,
            tasks,
            len(mock_get_backends.return_value),
            None,
            False,
        )
        mock_get_backends.assert_any_call(service_instance, synapse_host_port='fakehost1:3212')
        mock_get_backends.assert_any_call(service_instance, synapse_host_port='fakehost2:3212')
        assert "fake_location1 - %s" % PaastaColors.green('Healthy') in actual
        assert "fake_location2 - %s" % PaastaColors.green('Healthy') in actual


def test_status_smartstack_backends_multiple_locations_expected_count():
    service = 'my_service'
    instance = 'my_instance'
    service_instance = "%s.%s" % (service, instance)
    cluster = 'fake_cluster'
    normal_count = 10

    good_task = mock.Mock()
    other_task = mock.Mock()
    fake_backend = {'status': 'UP', 'lastchg': '1', 'last_chk': 'OK',
                    'check_code': '200', 'svname': 'ipaddress1:1001_hostname1',
                    'check_status': 'L7OK', 'check_duration': 1}
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.load_service_namespace_config', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance'),
        mock.patch('paasta_tools.marathon_serviceinit.get_mesos_slaves_grouped_by_attribute'),
        mock.patch('paasta_tools.marathon_serviceinit.get_backends', autospec=True),
        mock.patch('paasta_tools.marathon_serviceinit.match_backends_and_tasks', autospec=True),
        mock.patch('paasta_tools.marathon_serviceinit.haproxy_backend_report', autospec=True),
    ) as (
        mock_load_service_namespace_config,
        mock_read_ns,
        mock_get_mesos_slaves_grouped_by_attribute,
        mock_get_backends,
        mock_match_backends_and_tasks,
        mock_haproxy_backend_report,
    ):
        mock_load_service_namespace_config.return_value.get_discover.return_value = 'fake_discover'
        mock_read_ns.return_value = instance
        mock_get_backends.return_value = [fake_backend]
        mock_match_backends_and_tasks.return_value = [
            (fake_backend, good_task),
        ]
        tasks = [good_task, other_task]
        mock_get_mesos_slaves_grouped_by_attribute.return_value = {
            'fake_location1': ['fakehost1'],
            'fake_location2': ['fakehost2'],
        }
        marathon_serviceinit.status_smartstack_backends(
            service,
            instance,
            cluster,
            tasks,
            normal_count,
            None,
            False,
        )
        mock_get_backends.assert_any_call(service_instance, synapse_host_port='fakehost1:3212')
        mock_get_backends.assert_any_call(service_instance, synapse_host_port='fakehost2:3212')
        expected_count_per_location = int(
            normal_count / len(mock_get_mesos_slaves_grouped_by_attribute.return_value))
        mock_haproxy_backend_report.assert_any_call(expected_count_per_location, 1)


def test_status_smartstack_backends_verbose_multiple_apps():
    service = 'my_service'
    instance = 'my_instance'
    service_instance = "%s.%s" % (service, instance)
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
        mock.patch('paasta_tools.marathon_tools.load_service_namespace_config', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance'),
        mock.patch('paasta_tools.marathon_serviceinit.get_mesos_slaves_grouped_by_attribute'),
        mock.patch('paasta_tools.marathon_serviceinit.get_backends', autospec=True),
        mock.patch('paasta_tools.marathon_serviceinit.match_backends_and_tasks', autospec=True),
    ) as (
        mock_load_service_namespace_config,
        mock_read_ns,
        mock_get_mesos_slaves_grouped_by_attribute,
        mock_get_backends,
        mock_match_backends_and_tasks,
    ):
        mock_load_service_namespace_config.return_value.get_discover.return_value = 'fake_discover'
        mock_read_ns.return_value = instance
        mock_get_backends.return_value = haproxy_backends_by_task.values()
        mock_match_backends_and_tasks.return_value = [
            (haproxy_backends_by_task[good_task], good_task),
            (haproxy_backends_by_task[bad_task], None),
            (None, other_task),
        ]
        tasks = [good_task, other_task]
        mock_get_mesos_slaves_grouped_by_attribute.return_value = {'fake_location1': ['fakehost1']}
        actual = marathon_serviceinit.status_smartstack_backends(
            service,
            instance,
            cluster,
            tasks,
            len(haproxy_backends_by_task),
            None,
            True,
        )
        mock_get_backends.assert_called_once_with(service_instance, synapse_host_port='fakehost1:3212')
        assert "fake_location1" in actual
        assert re.search(r"%s[^\n]*hostname1:1001" % re.escape(PaastaColors.DEFAULT), actual)
        assert re.search(r"%s[^\n]*hostname2:1002" % re.escape(PaastaColors.GREY), actual)


def test_status_smartstack_backends_verbose_multiple_locations():
    service = 'my_service'
    instance = 'my_instance'
    service_instance = "%s.%s" % (service, instance)
    cluster = 'fake_cluster'
    good_task = mock.Mock()
    other_task = mock.Mock()
    fake_backend = {'status': 'UP', 'lastchg': '1', 'last_chk': 'OK',
                    'check_code': '200', 'svname': 'ipaddress1:1001_hostname1',
                    'check_status': 'L7OK', 'check_duration': 1}
    fake_other_backend = {'status': 'UP', 'lastchg': '1', 'last_chk': 'OK',
                          'check_code': '200', 'svname': 'ipaddress1:1002_hostname2',
                          'check_status': 'L7OK', 'check_duration': 1}
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.load_service_namespace_config', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance'),
        mock.patch('paasta_tools.marathon_serviceinit.get_mesos_slaves_grouped_by_attribute'),
        mock.patch('paasta_tools.marathon_serviceinit.get_backends', autospec=True,
                   side_effect=[[fake_backend], [fake_other_backend]]),
        mock.patch('paasta_tools.marathon_serviceinit.match_backends_and_tasks',
                   autospec=True, side_effect=[[(fake_backend, good_task)], [(fake_other_backend, good_task)]]),
    ) as (
        mock_load_service_namespace_config,
        mock_read_ns,
        mock_get_mesos_slaves_grouped_by_attribute,
        mock_get_backends,
        mock_match_backends_and_tasks,
    ):
        mock_load_service_namespace_config.return_value.get_discover.return_value = 'fake_discover'
        mock_read_ns.return_value = instance
        tasks = [good_task, other_task]
        mock_get_mesos_slaves_grouped_by_attribute.return_value = {
            'fake_location1': ['fakehost1'],
            'fake_location2': ['fakehost2'],
        }
        actual = marathon_serviceinit.status_smartstack_backends(
            service,
            instance,
            cluster,
            tasks,
            1,
            None,
            True,
        )
        mock_get_backends.assert_any_call(service_instance, synapse_host_port='fakehost1:3212')
        mock_get_backends.assert_any_call(service_instance, synapse_host_port='fakehost2:3212')
        assert "fake_location1 - %s" % PaastaColors.green('Healthy') in actual
        assert re.search(r"%s[^\n]*hostname1:1001" % re.escape(PaastaColors.DEFAULT), actual)
        assert "fake_location2 - %s" % PaastaColors.green('Healthy') in actual
        assert re.search(r"%s[^\n]*hostname2:1002" % re.escape(PaastaColors.DEFAULT), actual)


def test_status_smartstack_backends_verbose_emphasizes_maint_instances():
    service = 'my_service'
    instance = 'my_instance'
    cluster = 'fake_cluster'
    normal_count = 10
    good_task = mock.Mock()
    other_task = mock.Mock()
    fake_backend = {'status': 'MAINT', 'lastchg': '1', 'last_chk': 'OK',
                    'check_code': '200', 'svname': 'ipaddress1:1001_hostname1',
                    'check_status': 'L7OK', 'check_duration': 1}
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.load_service_namespace_config', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance'),
        mock.patch('paasta_tools.marathon_serviceinit.get_mesos_slaves_grouped_by_attribute'),
        mock.patch('paasta_tools.marathon_serviceinit.get_backends', autospec=True),
        mock.patch('paasta_tools.marathon_serviceinit.match_backends_and_tasks', autospec=True),
    ) as (
        mock_load_service_namespace_config,
        mock_read_ns,
        mock_get_mesos_slaves_grouped_by_attribute,
        mock_get_backends,
        mock_match_backends_and_tasks,
    ):
        mock_load_service_namespace_config.return_value.get_discover.return_value = 'fake_discover'
        mock_read_ns.return_value = instance
        mock_get_backends.return_value = [fake_backend]
        mock_match_backends_and_tasks.return_value = [
            (fake_backend, good_task),
        ]
        tasks = [good_task, other_task]
        mock_get_mesos_slaves_grouped_by_attribute.return_value = {'fake_location1': ['fakehost1']}
        actual = marathon_serviceinit.status_smartstack_backends(
            service,
            instance,
            cluster,
            tasks,
            normal_count,
            None,
            True,
        )
        assert PaastaColors.red('MAINT') in actual


def test_status_smartstack_backends_verbose_demphasizes_maint_instances_for_unrelated_tasks():
    service = 'my_service'
    instance = 'my_instance'
    cluster = 'fake_cluster'
    normal_count = 10
    good_task = mock.Mock()
    other_task = mock.Mock()
    fake_backend = {'status': 'MAINT', 'lastchg': '1', 'last_chk': 'OK',
                    'check_code': '200', 'svname': 'ipaddress1:1001_hostname1',
                    'check_status': 'L7OK', 'check_duration': 1}
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_tools.load_service_namespace_config', autospec=True),
        mock.patch('paasta_tools.marathon_tools.read_namespace_for_service_instance'),
        mock.patch('paasta_tools.marathon_serviceinit.get_mesos_slaves_grouped_by_attribute'),
        mock.patch('paasta_tools.marathon_serviceinit.get_backends', autospec=True),
        mock.patch('paasta_tools.marathon_serviceinit.match_backends_and_tasks', autospec=True),
    ) as (
        mock_load_service_namespace_config,
        mock_read_ns,
        mock_get_mesos_slaves_grouped_by_attribute,
        mock_get_backends,
        mock_match_backends_and_tasks,
    ):
        mock_load_service_namespace_config.return_value.get_discover.return_value = 'fake_discover'
        mock_read_ns.return_value = instance
        mock_get_backends.return_value = [fake_backend]
        mock_match_backends_and_tasks.return_value = [
            (fake_backend, None),
        ]
        tasks = [good_task, other_task]
        mock_get_mesos_slaves_grouped_by_attribute.return_value = {'fake_location1': ['fakehost1']}
        actual = marathon_serviceinit.status_smartstack_backends(
            service,
            instance,
            cluster,
            tasks,
            normal_count,
            None,
            True,
        )
        assert PaastaColors.red('MAINT') not in actual
        assert re.search(r"%s[^\n]*hostname1:1001" % re.escape(PaastaColors.GREY), actual)


def test_haproxy_backend_report_healthy():
    normal_count = 10
    actual_count = 11
    status = marathon_serviceinit.haproxy_backend_report(normal_count, actual_count)
    assert "Healthy" in status


def test_haproxy_backend_report_warning():
    normal_count = 10
    actual_count = 1
    status = marathon_serviceinit.haproxy_backend_report(normal_count, actual_count)
    assert "Warning" in status


def test_haproxy_backend_report_critical():
    normal_count = 10
    actual_count = 0
    status = marathon_serviceinit.haproxy_backend_report(normal_count, actual_count)
    assert "Critical" in status


def test_status_mesos_tasks_verbose():
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_serviceinit.get_running_tasks_from_active_frameworks'),
        mock.patch('paasta_tools.marathon_serviceinit.get_non_running_tasks_from_active_frameworks'),
    ) as (
        get_running_mesos_tasks_for_service_patch,
        get_non_running_mesos_tasks_for_service_patch,
    ):
        get_running_mesos_tasks_for_service_patch.return_value = []
        get_non_running_mesos_tasks_for_service_patch.return_value = []
        actual = marathon_serviceinit.status_mesos_tasks_verbose('fake_service', 'fake_instance')
        assert 'Running Tasks' in actual
        assert 'Non-Running Tasks' in actual


def test_status_mesos_tasks_working():
    with mock.patch('paasta_tools.marathon_serviceinit.get_running_tasks_from_active_frameworks') as mock_tasks:
        mock_tasks.return_value = [
            {'id': 1}, {'id': 2}
        ]
        normal_count = 2
        actual = marathon_serviceinit.status_mesos_tasks('unused', 'unused', normal_count)
        assert 'Healthy' in actual


def test_status_mesos_tasks_warning():
    with mock.patch('paasta_tools.marathon_serviceinit.get_running_tasks_from_active_frameworks') as mock_tasks:
        mock_tasks.return_value = [
            {'id': 1}, {'id': 2}
        ]
        normal_count = 4
        actual = marathon_serviceinit.status_mesos_tasks('unused', 'unused', normal_count)
        assert 'Warning' in actual


def test_status_mesos_tasks_critical():
    with mock.patch('paasta_tools.marathon_serviceinit.get_tasks_from_active_frameworks') as mock_tasks:
        mock_tasks.return_value = []
        normal_count = 10
        actual = marathon_serviceinit.status_mesos_tasks('unused', 'unused', normal_count)
        assert 'Critical' in actual


def test_get_cpu_usage_good():
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
    actual = marathon_serviceinit.get_cpu_usage(fake_task)
    assert '10.0%' == actual


def test_get_cpu_usage_bad():
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
    actual = marathon_serviceinit.get_cpu_usage(fake_task)
    assert PaastaColors.red('100.0%') in actual


def test_get_cpu_usage_handles_missing_stats():
    fake_task = mock.create_autospec(mesos.cli.task.Task)
    fake_task.cpu_limit = 1.1
    fake_duration = 100
    fake_task.stats = {}
    fake_task.__getitem__.return_value = [{
        'state': 'TASK_RUNNING',
        'timestamp': int(datetime.datetime.now().strftime('%s')) - fake_duration,
    }]
    actual = marathon_serviceinit.get_cpu_usage(fake_task)
    assert "0.0%" in actual


def test_get_mem_usage_good():
    fake_task = mock.create_autospec(mesos.cli.task.Task)
    fake_task.rss = 1024 * 1024 * 10
    fake_task.mem_limit = fake_task.rss * 10
    actual = marathon_serviceinit.get_mem_usage(fake_task)
    assert actual == '10/100MB'


def test_get_mem_usage_bad():
    fake_task = mock.create_autospec(mesos.cli.task.Task)
    fake_task.rss = 1024 * 1024 * 100
    fake_task.mem_limit = fake_task.rss
    actual = marathon_serviceinit.get_mem_usage(fake_task)
    assert actual == PaastaColors.red('100/100MB')


def test_get_mem_usage_divide_by_zero():
    fake_task = mock.create_autospec(mesos.cli.task.Task)
    fake_task.rss = 1024 * 1024 * 10
    fake_task.mem_limit = 0
    actual = marathon_serviceinit.get_mem_usage(fake_task)
    assert actual == "Undef"


def test_perform_command_handles_no_docker_and_doesnt_raise():
    fake_service = 'fake_service'
    fake_instance = 'fake_instance'
    fake_cluster = 'fake_cluster'
    soa_dir = 'fake_soa_dir'
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_serviceinit.marathon_tools.load_marathon_config', autospec=True),
        mock.patch('paasta_tools.marathon_serviceinit.validate_service_instance', autospec=True),
        mock.patch('paasta_tools.marathon_serviceinit.marathon_tools.load_marathon_service_config', autospec=True),
        mock.patch('paasta_tools.marathon_serviceinit.marathon_tools.get_app_id', autospec=True),
    ) as (
        mock_load_marathon_config,
        mock_validate_service_instance,
        mock_load_marathon_service_config,
        mock_get_app_id,
    ):
        mock_get_app_id.side_effect = NoDockerImageError()
        actual = marathon_serviceinit.perform_command(
            'start', fake_service, fake_instance, fake_cluster, False, soa_dir)
        assert actual == 1


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
