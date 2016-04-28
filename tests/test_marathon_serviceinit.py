#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
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
import datetime
import re

import marathon
import mock

from paasta_tools import marathon_serviceinit
from paasta_tools import marathon_tools
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import remove_ansi_escape_sequences
from paasta_tools.utils import SystemPaastaConfig


fake_marathon_job_config = marathon_tools.MarathonServiceConfig(
    service='servicename',
    cluster='clustername',
    instance='instancename',
    config_dict={
        'instances': 3,
        'cpus': 1,
        'mem': 100,
        'disk': 512,
        'nerve_ns': 'fake_nerve_ns',
    },
    branch_dict={
        'docker_image': 'test_docker:1.0',
        'desired_state': 'start',
        'force_bounce': None,
    },
)


def test_start_marathon_job():
    client = mock.create_autospec(marathon.MarathonClient)
    cluster = 'my_cluster'
    service = 'my_service'
    instance = 'my_instance'
    app_id = 'mock_app_id'
    normal_instance_count = 5
    with mock.patch('paasta_tools.marathon_serviceinit._log'):
        marathon_serviceinit.start_marathon_job(service, instance, app_id, normal_instance_count, client, cluster)
    client.scale_app.assert_called_once_with(app_id, instances=normal_instance_count, force=True)


def test_stop_marathon_job():
    client = mock.create_autospec(marathon.MarathonClient)
    cluster = 'my_cluster'
    service = 'my_service'
    instance = 'my_instance'
    app_id = 'mock_app_id'
    with mock.patch('paasta_tools.marathon_serviceinit._log'):
        marathon_serviceinit.stop_marathon_job(service, instance, app_id, client, cluster)
    client.scale_app.assert_called_once_with(app_id, instances=0, force=True)


def test_scale_marathon_job():
    client = mock.create_autospec(marathon.MarathonClient)
    cluster = 'my_cluster'
    service = 'my_service'
    instance = 'my_instance'
    app_id = 'mock_app_id'
    delta = -1
    with mock.patch('paasta_tools.marathon_serviceinit._log'):
        marathon_serviceinit.scale_marathon_job(service, instance, app_id, delta, client, cluster)
    client.scale_app.assert_called_once_with(app_id, delta=-1, force=True)


def test_get_bouncing_status():
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_serviceinit.marathon_tools.get_matching_appids', autospec=True),
    ) as (
        mock_get_matching_appids,
    ):
        mock_get_matching_appids.return_value = ['a', 'b']
        mock_config = marathon_tools.MarathonServiceConfig(
            service='fake_service',
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={'bounce_method': 'fake_bounce'},
            branch_dict={},
        )
        actual = marathon_serviceinit.get_bouncing_status('fake_service', 'fake_instance', 'unused', mock_config)
        assert 'fake_bounce' in actual
        assert 'Bouncing' in actual


def test_status_desired_state():
    with mock.patch(
        'paasta_tools.marathon_serviceinit.get_bouncing_status',
        autospec=True,
    ) as mock_get_bouncing_status:
        mock_get_bouncing_status.return_value = 'Bouncing (fake_bounce)'
        fake_complete_config = mock.Mock()
        fake_complete_config.get_desired_state_human = mock.Mock(return_value='Started')
        actual = marathon_serviceinit.status_desired_state(
            'fake_service',
            'fake_instance',
            'unused',
            fake_complete_config,
        )
        assert 'Started' in actual
        assert 'Bouncing' in actual


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
        mock.patch('paasta_tools.marathon_tools.is_app_id_running', return_value=True),
    ) as (
        mock_get_matching_appids,
        mock_get_verbose_app,
        mock_is_app_id_running,
    ):
        mock_get_matching_appids.return_value = ['/app1']
        mock_get_verbose_app.return_value = ([task], 'fake_return')
        tasks, out = marathon_serviceinit.status_marathon_job_verbose(service, instance, client)
        mock_is_app_id_running.assert_called_once_with('/app1', client)
        mock_get_matching_appids.assert_called_once_with(service, instance, client)
        mock_get_verbose_app.assert_called_once_with(app)
        assert tasks == [task]
        assert 'fake_return' in out


def test_status_marathon_job_verbose_when_not_running():
    client = mock.create_autospec(marathon.MarathonClient)
    app = mock.create_autospec(marathon.models.app.MarathonApp)
    client.get_app.return_value = app
    service = 'my_service'
    instance = 'my_instance'
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_serviceinit.marathon_tools.get_matching_appids'),
        mock.patch('paasta_tools.marathon_serviceinit.get_verbose_status_of_marathon_app'),
        mock.patch('paasta_tools.marathon_tools.is_app_id_running', return_value=False),
    ) as (
        mock_get_matching_appids,
        mock_get_verbose_app,
        mock_is_app_id_running,
    ):
        mock_get_matching_appids.return_value = ['/app1']
        tasks, out = marathon_serviceinit.status_marathon_job_verbose(service, instance, client)
        mock_is_app_id_running.assert_called_once_with('/app1', client)
        assert not mock_get_verbose_app.called
        assert tasks == []
        assert 'not running' in out


def test_get_verbose_status_of_marathon_app():
    fake_app = mock.create_autospec(marathon.models.app.MarathonApp)
    fake_app.version = '2015-01-15T05:30:49.862Z'
    fake_app.id = '/fake--service'
    fake_task = mock.create_autospec(marathon.models.app.MarathonTask)
    fake_task.id = 'fake_task_id'
    fake_task.host = 'fake_deployed_host'
    fake_task.ports = [6666]
    fake_task.staged_at = datetime.datetime.fromtimestamp(0)
    fake_task.health_check_results = []
    fake_app.tasks = [fake_task]
    tasks, out = marathon_serviceinit.get_verbose_status_of_marathon_app(fake_app)
    assert 'fake_task_id' in out
    assert '/fake--service' in out
    assert 'App created: 2015-01-15 05:30:49' in out
    assert 'fake_deployed_host:6666' in out
    assert tasks == [fake_task]


def test_get_verbose_status_of_marathon_app_column_alignment():
    fake_app = mock.create_autospec(marathon.models.app.MarathonApp)
    fake_app.version = '2015-01-15T05:30:49.862Z'
    fake_app.id = '/fake--service'

    fake_task1 = mock.create_autospec(marathon.models.app.MarathonTask)
    fake_task1.id = 'fake_task1_id'
    fake_task1.host = 'fake_deployed_host'
    fake_task1.ports = [6666]
    fake_task1.staged_at = datetime.datetime.fromtimestamp(0)
    fake_task1.health_check_results = []

    fake_task2 = mock.create_autospec(marathon.models.app.MarathonTask)
    fake_task2.id = 'fake_task2_id'
    fake_task2.host = 'fake_deployed_host_with_a_really_long_name'
    fake_task2.ports = [6666]
    fake_task2.staged_at = datetime.datetime.fromtimestamp(0)
    fake_task2.health_check_results = []

    fake_app.tasks = [fake_task1, fake_task2]
    tasks, out = marathon_serviceinit.get_verbose_status_of_marathon_app(fake_app)
    (_, _, _, headers_line, task1_line, task2_line) = out.split('\n')
    assert headers_line.index('Host deployed to') == task1_line.index('fake_deployed_host')
    assert headers_line.index('Host deployed to') == task2_line.index('fake_deployed_host_with_a_really_long_name')
    assert headers_line.index('Deployed at what localtime') == task1_line.index('1970-01-01T00:00')
    assert headers_line.index('Deployed at what localtime') == task2_line.index('1970-01-01T00:00')


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


def test_format_haproxy_backend_row():
    actual = marathon_serviceinit.format_haproxy_backend_row(
        backend={
            'svname': '169.254.123.1:1234_host1',
            'status': 'UP',
            'check_status': 'L7OK',
            'check_code': '200',
            'check_duration': 4,
            'lastchg': 0
        },
        is_correct_instance=True,
    )
    expected = (
        '      host1:1234',
        'L7OK/200 in 4ms',
        'now',
        PaastaColors.default('UP'),
    )
    assert actual == expected


def test_status_smartstack_backends_normal():
    service = 'my_service'
    instance = 'my_instance'
    service_instance = compose_job_id(service, instance)

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
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            tasks=tasks,
            expected_count=len(haproxy_backends_by_task),
            soa_dir=None,
            verbose=False,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        mock_get_backends.assert_called_once_with(
            service_instance,
            synapse_host='fakehost1',
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
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
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            tasks=tasks,
            expected_count=normal_count,
            soa_dir=None,
            verbose=False,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        assert "is announced in the" in actual
        assert different_ns in actual


def test_status_smartstack_backends_no_smartstack_replication_info():
    service = 'my_service'
    instance = 'my_instance'
    service_instance = compose_job_id(service, instance)
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
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            tasks=tasks,
            expected_count=normal_count,
            soa_dir=None,
            verbose=False,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        assert "%s is NOT in smartstack" % service_instance in actual


def test_status_smartstack_backends_multiple_locations():
    service = 'my_service'
    instance = 'my_instance'
    service_instance = compose_job_id(service, instance)
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
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            tasks=tasks,
            expected_count=len(mock_get_backends.return_value),
            soa_dir=None,
            verbose=False,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        mock_get_backends.assert_any_call(
            service_instance,
            synapse_host='fakehost1',
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        mock_get_backends.assert_any_call(
            service_instance,
            synapse_host='fakehost2',
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        assert "fake_location1 - %s" % PaastaColors.green('Healthy') in actual
        assert "fake_location2 - %s" % PaastaColors.green('Healthy') in actual


def test_status_smartstack_backends_multiple_locations_expected_count():
    service = 'my_service'
    instance = 'my_instance'
    service_instance = compose_job_id(service, instance)
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
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            tasks=tasks,
            expected_count=normal_count,
            soa_dir=None,
            verbose=False,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        mock_get_backends.assert_any_call(
            service_instance,
            synapse_host='fakehost1',
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        mock_get_backends.assert_any_call(
            service_instance,
            synapse_host='fakehost2',
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        expected_count_per_location = int(
            normal_count / len(mock_get_mesos_slaves_grouped_by_attribute.return_value))
        mock_haproxy_backend_report.assert_any_call(expected_count_per_location, 1)


def test_status_smartstack_backends_verbose_multiple_apps():
    service = 'my_service'
    instance = 'my_instance'
    service_instance = compose_job_id(service, instance)
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
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            tasks=tasks,
            expected_count=len(haproxy_backends_by_task),
            soa_dir=None,
            verbose=True,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        mock_get_backends.assert_called_once_with(
            service_instance,
            synapse_host='fakehost1',
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        assert "fake_location1" in actual
        assert "hostname1:1001" in actual
        assert re.search(r"%s[^\n]*hostname2:1002" % re.escape(PaastaColors.GREY), actual)


def test_status_smartstack_backends_verbose_multiple_locations():
    service = 'my_service'
    instance = 'my_instance'
    service_instance = compose_job_id(service, instance)
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
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            tasks=tasks,
            expected_count=1,
            soa_dir=None,
            verbose=True,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        mock_get_backends.assert_any_call(
            service_instance,
            synapse_host='fakehost1',
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        mock_get_backends.assert_any_call(
            service_instance,
            synapse_host='fakehost2',
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        mock_get_mesos_slaves_grouped_by_attribute.assert_called_once_with(
            attribute='fake_discover',
            blacklist=[],
        )
        assert "fake_location1 - %s" % PaastaColors.green('Healthy') in actual
        assert "hostname1:1001" in actual
        assert "fake_location2 - %s" % PaastaColors.green('Healthy') in actual
        assert "hostname2:1002" in actual


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
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            tasks=tasks,
            expected_count=normal_count,
            soa_dir=None,
            verbose=True,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
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
            service=service,
            instance=instance,
            cluster=cluster,
            job_config=fake_marathon_job_config,
            tasks=tasks,
            expected_count=normal_count,
            soa_dir=None,
            verbose=True,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )
        assert PaastaColors.red('MAINT') not in actual
        assert re.search(r"%s[^\n]*hostname1:1001" % re.escape(PaastaColors.GREY), actual)


def test_haproxy_backend_report_healthy():
    normal_count = 10
    actual_count = 11
    status = marathon_serviceinit.haproxy_backend_report(normal_count, actual_count)
    assert "Healthy" in status


def test_haproxy_backend_report_critical():
    normal_count = 10
    actual_count = 1
    status = marathon_serviceinit.haproxy_backend_report(normal_count, actual_count)
    assert "Critical" in status


def test_get_short_task_id():
    task_id = 'service.instance.githash.confighash.uuid'
    assert marathon_serviceinit.get_short_task_id(task_id) == 'uuid'


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
    with mock.patch('paasta_tools.marathon_serviceinit.get_running_tasks_from_active_frameworks') as mock_tasks:
        mock_tasks.return_value = []
        normal_count = 10
        actual = marathon_serviceinit.status_mesos_tasks('unused', 'unused', normal_count)
        assert 'Critical' in actual


def test_perform_command_handles_no_docker_and_doesnt_raise():
    fake_service = 'fake_service'
    fake_instance = 'fake_instance'
    fake_cluster = 'fake_cluster'
    soa_dir = 'fake_soa_dir'
    with contextlib.nested(
        mock.patch('paasta_tools.marathon_serviceinit.marathon_tools.load_marathon_config', autospec=True),
        mock.patch('paasta_tools.marathon_serviceinit.marathon_tools.load_marathon_service_config', autospec=True,
                   return_value=mock.Mock(format_marathon_app_dict=mock.Mock(side_effect=NoDockerImageError))),
        mock.patch('paasta_tools.marathon_serviceinit.load_system_paasta_config', autospec=True,
                   return_value=SystemPaastaConfig({}, "/fake/config")),
    ) as (
        mock_load_marathon_config,
        mock_load_marathon_service_config,
        mock_load_system_paasta_config,
    ):
        actual = marathon_serviceinit.perform_command(
            'start', fake_service, fake_instance, fake_cluster, False, soa_dir)
        assert actual == 1


def test_pretty_print_smartstack_backends_for_locations_verbose():
    hosts_grouped_by_location = {'place1': ['host1'], 'place2': ['host2'], 'place3': ['host3']}
    host_ip_mapping = {
        'host1': '169.254.123.1',
        'host2': '169.254.123.2',
        'host3': '169.254.123.3',
    }
    tasks = [
        mock.Mock(host='host1', ports=[1234]),
        mock.Mock(host='host2', ports=[1234]),
        mock.Mock(host='host3', ports=[1234])
    ]
    backends = {
        'host1': {
            'svname': '169.254.123.1:1234_host1',
            'status': 'UP',
            'check_status': 'L7OK',
            'check_code': '200',
            'check_duration': 4,
            'lastchg': 0
        },
        'host2': {
            'svname': '169.254.123.2:1234_host2',
            'status': 'UP',
            'check_status': 'L7OK',
            'check_code': '200',
            'check_duration': 4,
            'lastchg': 0
        },
        'host3': {
            'svname': '169.254.123.3:1234_host3',
            'status': 'UP',
            'check_status': 'L7OK',
            'check_code': '200',
            'check_duration': 4,
            'lastchg': 0
        },
    }
    with contextlib.nested(
        mock.patch(
            'paasta_tools.marathon_serviceinit.get_backends', autospec=True,
            side_effect=lambda _, synapse_host, synapse_port, synapse_haproxy_url_format: [backends[synapse_host]]
        ),
        mock.patch('socket.gethostbyname', side_effect=lambda name: host_ip_mapping[name], autospec=True),
    ) as (
        mock_get_backends,
        mock_gethostbyname,
    ):
        actual = marathon_serviceinit.pretty_print_smartstack_backends_for_locations(
            service_instance='fake_service.fake_instance',
            tasks=tasks,
            locations=hosts_grouped_by_location,
            expected_count=3,
            verbose=True,
            synapse_port=123456,
            synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
        )

        colorstripped_actual = [remove_ansi_escape_sequences(l) for l in actual]
        assert colorstripped_actual == [
            '      Name        LastCheck        LastChange  Status',
            '    place1 - Healthy - in haproxy with (1/1) total backends UP in this namespace.',
            '      host1:1234  L7OK/200 in 4ms  now         UP',
            '    place2 - Healthy - in haproxy with (1/1) total backends UP in this namespace.',
            '      host2:1234  L7OK/200 in 4ms  now         UP',
            '    place3 - Healthy - in haproxy with (1/1) total backends UP in this namespace.',
            '      host3:1234  L7OK/200 in 4ms  now         UP',
        ]

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
