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
import asynctest
import marathon
import mock
from pyramid import testing
from pytest import raises

from paasta_tools import marathon_tools
from paasta_tools.api import settings
from paasta_tools.api.views import instance
from paasta_tools.api.views.exception import ApiFailure


@mock.patch('paasta_tools.api.views.instance.marathon_job_status', autospec=True)
@mock.patch('paasta_tools.api.views.instance.marathon_tools.get_matching_appids', autospec=True)
@mock.patch('paasta_tools.api.views.instance.marathon_tools.load_marathon_service_config', autospec=True)
@mock.patch('paasta_tools.api.views.instance.validate_service_instance', autospec=True)
@mock.patch('paasta_tools.api.views.instance.get_actual_deployments', autospec=True)
def test_instances_status_marathon(
    mock_get_actual_deployments,
    mock_validate_service_instance,
    mock_load_marathon_service_config,
    mock_get_matching_appids,
    mock_marathon_job_status,
):
    settings.cluster = 'fake_cluster'
    mock_get_actual_deployments.return_value = {
        'fake_cluster.fake_instance': 'GIT_SHA',
        'fake_cluster.fake_instance2': 'GIT_SHA',
        'fake_cluster2.fake_instance': 'GIT_SHA',
        'fake_cluster2.fake_instance2': 'GIT_SHA',
    }
    mock_validate_service_instance.return_value = 'marathon'

    settings.marathon_clients = mock.Mock()

    mock_get_matching_appids.return_value = ['a', 'b']
    mock_service_config = marathon_tools.MarathonServiceConfig(
        service='fake_service',
        cluster='fake_cluster',
        instance='fake_instance',
        config_dict={'bounce_method': 'fake_bounce'},
        branch_dict=None,
    )
    mock_load_marathon_service_config.return_value = mock_service_config
    mock_marathon_job_status.return_value = 'fake_marathon_status'

    request = testing.DummyRequest()
    request.swagger_data = {'service': 'fake_service', 'instance': 'fake_instance'}

    response = instance.instance_status(request)
    assert response['marathon']['bounce_method'] == 'fake_bounce'
    assert response['marathon']['desired_state'] == 'start'


@mock.patch('paasta_tools.chronos_tools.load_chronos_config', autospec=True)
@mock.patch('paasta_tools.api.views.instance.chronos_tools.get_chronos_client', autospec=True)
@mock.patch('paasta_tools.api.views.instance.validate_service_instance', autospec=True)
@mock.patch('paasta_tools.api.views.instance.get_actual_deployments', autospec=True)
@mock.patch('paasta_tools.chronos_serviceinit.status_chronos_jobs', autospec=True)
def test_chronos_instance_status(
    mock_status_chronos_jobs,
    mock_get_actual_deployments,
    mock_validate_service_instance,
    mock_get_chronos_client,
    mock_load_chronos_config,
):
    settings.cluster = 'fake_cluster'
    mock_get_actual_deployments.return_value = {
        'fake_cluster.fake_instance': 'GIT_SHA',
        'fake_cluster.fake_instance2': 'GIT_SHA',
        'fake_cluster2.fake_instance': 'GIT_SHA',
        'fake_cluster2.fake_instance2': 'GIT_SHA',
    }
    mock_validate_service_instance.return_value = 'chronos'

    request = testing.DummyRequest()
    request.swagger_data = {'service': 'fake_service', 'instance': 'fake_instance'}

    instance.instance_status(request)
    assert mock_status_chronos_jobs.called


@mock.patch('paasta_tools.api.views.instance.adhoc_instance_status', autospec=True)
@mock.patch('paasta_tools.api.views.instance.validate_service_instance', autospec=True)
@mock.patch('paasta_tools.api.views.instance.get_actual_deployments', autospec=True)
def test_instances_status_adhoc(
    mock_get_actual_deployments,
    mock_validate_service_instance,
    mock_adhoc_instance_status,
):
    settings.cluster = 'fake_cluster'
    mock_get_actual_deployments.return_value = {
        'fake_cluster.fake_instance': 'GIT_SHA',
        'fake_cluster.fake_instance2': 'GIT_SHA',
        'fake_cluster2.fake_instance': 'GIT_SHA',
        'fake_cluster2.fake_instance2': 'GIT_SHA',
    }
    mock_validate_service_instance.return_value = 'adhoc'
    mock_adhoc_instance_status.return_value = {}

    request = testing.DummyRequest()
    request.swagger_data = {'service': 'fake_service', 'instance': 'fake_instance'}

    response = instance.instance_status(request)
    assert mock_adhoc_instance_status.called
    assert response == {
        'service': 'fake_service',
        'instance': 'fake_instance',
        'git_sha': 'GIT_SHA',
        'adhoc': {},
    }


@mock.patch('paasta_tools.api.views.instance.get_running_tasks_from_frameworks', autospec=True)
@mock.patch('paasta_tools.api.views.instance.marathon_tools.is_app_id_running', autospec=True)
def test_marathon_job_status_verbose(
    mock_is_app_id_running,
    mock_get_running_tasks_from_frameworks,
):
    mock_tasks = [
        mock.Mock(slave=asynctest.CoroutineMock(
            return_value={'hostname': 'host1'},
            func=asynctest.CoroutineMock(),  # https://github.com/notion/a_sync/pull/40
        )),
        mock.Mock(slave=asynctest.CoroutineMock(
            return_value={'hostname': 'host1'},
            func=asynctest.CoroutineMock(),  # https://github.com/notion/a_sync/pull/40
        )),
        mock.Mock(slave=asynctest.CoroutineMock(
            return_value={'hostname': 'host2'},
            func=asynctest.CoroutineMock(),  # https://github.com/notion/a_sync/pull/40
        )),
    ]
    mock_get_running_tasks_from_frameworks.return_value = mock_tasks
    mock_is_app_id_running.return_value = True

    app = mock.create_autospec(marathon.models.app.MarathonApp)
    app.instances = 5
    app.tasks_running = 5
    app.deployments = []
    app.id = 'mock_app_id'

    client = mock.create_autospec(marathon.MarathonClient)
    client.get_app.return_value = app

    job_config = mock.create_autospec(marathon_tools.MarathonServiceConfig)
    job_config.format_marathon_app_dict.return_value = {'id': 'mock_app_id'}
    job_config.get_instances.return_value = 5

    mstatus = {}
    instance.marathon_job_status(mstatus, client, job_config, verbose=True)
    expected = {
        'deploy_status': 'Running',
        'running_instance_count': 5,
        'expected_instance_count': 5,
        'app_id': 'mock_app_id',
    }
    expected_slaves = ['host2', 'host1']
    slaves = mstatus.pop('slaves')
    assert len(slaves) == len(expected_slaves) and sorted(slaves) == sorted(expected_slaves)
    assert mstatus == expected


@mock.patch('paasta_tools.api.views.instance.add_executor_info', autospec=True)
@mock.patch('paasta_tools.api.views.instance.add_slave_info', autospec=True)
@mock.patch('paasta_tools.api.views.instance.instance_status', autospec=True)
@mock.patch('paasta_tools.api.views.instance.get_tasks_from_app_id', autospec=True)
def test_instance_tasks(mock_get_tasks_from_app_id, mock_instance_status, mock_add_slave_info, mock_add_executor_info):
    mock_request = mock.Mock(swagger_data={'task_id': '123', 'slave_hostname': 'host1'})
    mock_instance_status.return_value = {'marathon': {'app_id': 'app1'}}

    mock_task_1 = mock.Mock()
    mock_task_2 = mock.Mock()
    mock_get_tasks_from_app_id.return_value = [mock_task_1, mock_task_2]
    ret = instance.instance_tasks(mock_request)
    assert not mock_add_slave_info.called
    assert not mock_add_executor_info.called

    mock_request = mock.Mock(swagger_data={'task_id': '123', 'slave_hostname': 'host1', 'verbose': True})
    ret = instance.instance_tasks(mock_request)
    mock_add_executor_info.assert_has_calls([mock.call(mock_task_1), mock.call(mock_task_2)])
    mock_add_slave_info.assert_has_calls([
        mock.call(mock_add_executor_info.return_value),
        mock.call(mock_add_executor_info.return_value),
    ])
    expected = [
        mock_add_slave_info.return_value._Task__items,
        mock_add_slave_info.return_value._Task__items,
    ]

    def ids(l):
        return {id(x) for x in l}
    assert len(ret) == len(expected) and ids(expected) == ids(ret)

    mock_instance_status.return_value = {'chronos': {}}
    with raises(ApiFailure):
        ret = instance.instance_tasks(mock_request)


@mock.patch('paasta_tools.api.views.instance.add_executor_info', autospec=True)
@mock.patch('paasta_tools.api.views.instance.add_slave_info', autospec=True)
@mock.patch('paasta_tools.api.views.instance.instance_status', autospec=True)
@mock.patch('paasta_tools.api.views.instance.get_task', autospec=True)
def test_instance_task(mock_get_task, mock_instance_status, mock_add_slave_info, mock_add_executor_info):
    mock_request = mock.Mock(swagger_data={'task_id': '123', 'slave_hostname': 'host1'})
    mock_instance_status.return_value = {'marathon': {'app_id': 'app1'}}

    mock_task_1 = mock.Mock()
    mock_get_task.return_value = mock_task_1
    ret = instance.instance_task(mock_request)
    assert not mock_add_slave_info.called
    assert not mock_add_executor_info.called
    assert ret == mock_task_1._Task__items

    mock_request = mock.Mock(swagger_data={'task_id': '123', 'slave_hostname': 'host1', 'verbose': True})
    ret = instance.instance_task(mock_request)
    mock_add_slave_info.assert_called_with(mock_task_1)
    mock_add_executor_info.assert_called_with(mock_add_slave_info.return_value)
    expected = mock_add_executor_info.return_value._Task__items
    assert ret == expected

    mock_instance_status.return_value = {'chronos': {}}
    with raises(ApiFailure):
        ret = instance.instance_task(mock_request)


@mock.patch('paasta_tools.api.views.instance.marathon_tools.get_app_queue', autospec=True)
@mock.patch('paasta_tools.api.views.instance.marathon_tools.load_marathon_service_config', autospec=True)
def test_instance_delay(mock_load_config, mock_get_app_queue):
    mock_unused_offers = mock.Mock()
    mock_unused_offers.last_unused_offers = [
        {
            'reason': ['foo', 'bar'],
        },
        {
            'reason': ['bar', 'baz'],
        },
        {
            'reason': [],
        },
    ]
    mock_get_app_queue.return_value = mock_unused_offers

    mock_config = mock.Mock()
    mock_config.format_marathon_app_dict = lambda: {'id': 'foo'}
    mock_load_config.return_value = mock_config

    request = testing.DummyRequest()
    request.swagger_data = {'service': 'fake_service', 'instance': 'fake_instance'}

    response = instance.instance_delay(request)
    assert response['foo'] == 1
    assert response['bar'] == 2
    assert response['baz'] == 1


def test_add_executor_info():
    mock_mesos_task = mock.Mock()
    mock_executor = {
        'tasks': [mock_mesos_task],
        'some': 'thing',
        'completed_tasks': [mock_mesos_task],
        'queued_tasks': [mock_mesos_task],
    }
    mock_task = mock.Mock(
        _Task__items={'a': 'thing'},
        executor=asynctest.CoroutineMock(
            return_value=mock_executor,
            func=asynctest.CoroutineMock(),  # https://github.com/notion/a_sync/pull/40
        ),
    )
    ret = instance.add_executor_info(mock_task)
    expected = {
        'a': 'thing',
        'executor': {'some': 'thing'},
    }
    assert ret._Task__items == expected
    with raises(KeyError):
        ret._Task__items['executor']['completed_tasks']
    with raises(KeyError):
        ret._Task__items['executor']['tasks']
    with raises(KeyError):
        ret._Task__items['executor']['queued_tasks']


def test_add_slave_info():
    mock_slave = asynctest.CoroutineMock(
        return_value=mock.Mock(_MesosSlave__items={'some': 'thing'}),
        func=asynctest.CoroutineMock(),  # https://github.com/notion/a_sync/pull/40
    )
    mock_task = mock.Mock(
        _Task__items={'a': 'thing'},
        slave=mock_slave,
    )
    expected = {
        'a': 'thing',
        'slave': {'some': 'thing'},
    }
    assert instance.add_slave_info(mock_task)._Task__items == expected


@mock.patch('paasta_tools.api.views.instance.tron_tools.get_tron_dashboard_for_cluster', autospec=True)
@mock.patch('paasta_tools.api.views.instance.tron_tools.TronClient', autospec=True)
@mock.patch('paasta_tools.api.views.instance.tron_tools.get_tron_client', autospec=True)
@mock.patch('paasta_tools.api.views.instance.validate_service_instance', autospec=True)
def test_tron_instance_status(
    mock_validate_service_instance,
    mock_get_tron_client,
    mock_tron_client,
    mock_get_tron_dashboard_for_cluster,
):
    settings.cluster = 'fake_cluster'
    mock_validate_service_instance.return_value = 'tron'
    mock_client = mock_tron_client('fake_url')
    mock_get_tron_client.return_value = mock_client
    mock_client.get_job_content.return_value = {
        'status': 'fake_status',
        'scheduler': {
            'type': 'daily',
            'value': '1 2 3',
        },
    }
    mock_client.get_action_run.return_value = {
        'state': 'fake_state',
        'start_time': 'fake_start_time',
        'raw_command': 'fake_raw_command',
        'command': 'fake_command',
        'stdout': ['fake_stdout'],
        'stderr': ['fake_stderr'],
    }
    mock_get_tron_dashboard_for_cluster.return_value = 'http://fake_url/'

    request = testing.DummyRequest()
    request.swagger_data = {'service': 'fake_service', 'instance': 'fake_job.fake_action'}
    response = instance.instance_status(request)
    assert response['tron']['job_name'] == 'fake_job'
    assert response['tron']['job_status'] == 'fake_status'
    assert response['tron']['job_schedule'] == 'daily 1 2 3'
    assert response['tron']['job_url'] == 'http://fake_url/#job/fake_service.fake_job'
    assert response['tron']['action_name'] == 'fake_action'
    assert response['tron']['action_state'] == 'fake_state'
    assert response['tron']['action_raw_command'] == 'fake_raw_command'
    assert response['tron']['action_command'] == 'fake_command'
    assert response['tron']['action_start_time'] == 'fake_start_time'
    assert response['tron']['action_stdout'] == 'fake_stdout'
    assert response['tron']['action_stderr'] == 'fake_stderr'
