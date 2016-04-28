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
from datetime import datetime
from datetime import timedelta

import mock
from kazoo.exceptions import NoNodeError
from pytest import raises

from paasta_tools import autoscaling_lib
from paasta_tools import marathon_tools


def test_get_zookeeper_instances():
    fake_marathon_config = marathon_tools.MarathonServiceConfig(
        service='service',
        instance='instance',
        cluster='cluster',
        config_dict={
            'instances': 5,
            'max_instances': 10,
        },
        branch_dict={},
    )
    with contextlib.nested(
            mock.patch('paasta_tools.utils.KazooClient', autospec=True),
            mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True),
    ) as (
        mock_zk_client,
        _,
    ):
        mock_zk_get = mock.Mock(return_value=(7, None))
        mock_zk_client.return_value = mock.Mock(get=mock_zk_get)
        assert fake_marathon_config.get_instances() == 7
        assert mock_zk_get.call_count == 1


def test_zookeeper_pool():
    with contextlib.nested(
            mock.patch('paasta_tools.utils.KazooClient', autospec=True),
            mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True),
    ) as (
        mock_zk_client,
        _,
    ):
        zk_client = mock.Mock()
        mock_zk_client.return_value = zk_client
        with autoscaling_lib.ZookeeperPool():
            with autoscaling_lib.ZookeeperPool():
                assert zk_client.start.call_count == 1
            assert zk_client.stop.call_count == 0

        assert zk_client.stop.call_count == 1


def test_get_zookeeper_instances_defaults_to_config_no_zk_node():
    fake_marathon_config = marathon_tools.MarathonServiceConfig(
        service='service',
        instance='instance',
        cluster='cluster',
        config_dict={
            'min_instances': 5,
            'max_instances': 10,
        },
        branch_dict={},
    )
    with contextlib.nested(
            mock.patch('paasta_tools.utils.KazooClient', autospec=True),
            mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True),
    ) as (
        mock_zk_client,
        _,
    ):
        mock_zk_client.return_value = mock.Mock(get=mock.Mock(side_effect=NoNodeError))
        assert fake_marathon_config.get_instances() == 5


def test_get_zookeeper_instances_defaults_to_config_out_of_bounds():
    fake_marathon_config = marathon_tools.MarathonServiceConfig(
        service='service',
        instance='instance',
        cluster='cluster',
        config_dict={
            'min_instances': 5,
            'max_instances': 10,
        },
        branch_dict={},
    )
    with contextlib.nested(
            mock.patch('paasta_tools.utils.KazooClient', autospec=True),
            mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True),
    ) as (
        mock_zk_client,
        _,
    ):
        mock_zk_client.return_value = mock.Mock(get=mock.Mock(return_value=(15, None)))
        assert fake_marathon_config.get_instances() == 10
        mock_zk_client.return_value = mock.Mock(get=mock.Mock(return_value=(0, None)))
        assert fake_marathon_config.get_instances() == 5


def test_update_instances_for_marathon_service():
    with contextlib.nested(
            mock.patch('paasta_tools.marathon_tools.load_marathon_service_config', autospec=True),
            mock.patch('paasta_tools.utils.KazooClient', autospec=True),
            mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True),
    ) as (
        mock_load_marathon_service_config,
        mock_zk_client,
        _,
    ):
        zk_client = mock.Mock(get=mock.Mock(side_effect=NoNodeError))
        mock_zk_client.return_value = zk_client
        mock_load_marathon_service_config.return_value = marathon_tools.MarathonServiceConfig(
            service='service',
            instance='instance',
            cluster='cluster',
            config_dict={
                'min_instances': 5,
                'max_instances': 10,
            },
            branch_dict={},
        )
        autoscaling_lib.set_instances_for_marathon_service('service', 'instance', instance_count=8)
        zk_client.set.assert_called_once_with('/autoscaling/service/instance/instances', '8')


def test_compose_autoscaling_zookeeper_root():
    assert autoscaling_lib.compose_autoscaling_zookeeper_root(
        'fake-service', 'fake-instance') == '/autoscaling/fake-service/fake-instance'


def test_get_service_metrics_provider():
    assert autoscaling_lib.get_service_metrics_provider(
        'mesos_cpu') == autoscaling_lib.mesos_cpu_metrics_provider


def test_get_decision_policy():
    assert autoscaling_lib.get_decision_policy('pid') == autoscaling_lib.pid_decision_policy


def test_pid_decision_policy():
    current_time = datetime.now()

    zookeeper_get_payload = {
        'pid_iterm': 0,
        'pid_last_error': 0,
        'pid_last_time': (current_time - timedelta(seconds=600)).strftime('%s'),
    }

    with contextlib.nested(
            mock.patch('paasta_tools.utils.KazooClient', autospec=True,
                       return_value=mock.Mock(get=mock.Mock(
                           side_effect=lambda x: (zookeeper_get_payload[x.split('/')[-1]], None)))),
            mock.patch('paasta_tools.autoscaling_lib.datetime', autospec=True),
            mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True,
                       return_value=mock.Mock(get_zk_hosts=mock.Mock())),
    ) as (
        mock_zk_client,
        mock_datetime,
        _,
    ):
        mock_datetime.now.return_value = current_time
        assert autoscaling_lib.pid_decision_policy('/autoscaling/fake-service/fake-instance', 10, 1, 100, 0.0) == 0
        assert autoscaling_lib.pid_decision_policy('/autoscaling/fake-service/fake-instance', 10, 1, 100, 0.2) == 1
        assert autoscaling_lib.pid_decision_policy('/autoscaling/fake-service/fake-instance', 10, 1, 100, -0.2) == -1
        mock_zk_client.return_value.set.assert_has_calls([
            mock.call('/autoscaling/fake-service/fake-instance/pid_iterm', '0.0'),
            mock.call('/autoscaling/fake-service/fake-instance/pid_last_error', '0.0'),
            mock.call('/autoscaling/fake-service/fake-instance/pid_last_time',
                      '%s' % current_time.strftime('%s')),
        ], any_order=True)


def test_threshold_decision_policy():
    decision_policy_args = {
        'threshold': 0.1,
        'current_instances': 10,
    }
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling_lib.datetime', autospec=True),
        mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True,
                   return_value=mock.Mock(get_zk_hosts=mock.Mock())),
    ) as (
        mock_datetime,
        _,
    ):
        assert autoscaling_lib.threshold_decision_policy(error=0, **decision_policy_args) == 0
        assert autoscaling_lib.threshold_decision_policy(error=0.5, **decision_policy_args) == 1
        assert autoscaling_lib.threshold_decision_policy(error=-0.5, **decision_policy_args) == -1


def test_mesos_cpu_metrics_provider():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={},
        branch_dict={},
    )
    fake_mesos_task = mock.MagicMock(
        stats={
            'cpus_limit': 1.1,
            'cpus_system_time_secs': 240,
            'cpus_user_time_secs': 240,
        },
    )
    fake_mesos_task.__getitem__.return_value = 'fake-service.fake-instance'

    fake_marathon_tasks = [mock.Mock(id='fake-service.fake-instance')]

    with contextlib.nested(
            mock.patch('paasta_tools.utils.KazooClient', autospec=True,
                       return_value=mock.Mock(get=mock.Mock(
                           side_effect=NoNodeError))),
            mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True,
                       return_value=mock.Mock(get_zk_hosts=mock.Mock())),
    ) as (
        mock_zk_client,
        _,
    ):
        with raises(autoscaling_lib.MetricsProviderNoDataError):
            autoscaling_lib.mesos_cpu_metrics_provider(
                fake_marathon_service_config, fake_marathon_tasks, (fake_mesos_task,))
        mock_zk_client.return_value.set.assert_has_calls([
            mock.call('/autoscaling/fake-service/fake-instance/cpu_data', '480.0:fake-service.fake-instance'),
        ], any_order=True)


def test_mesos_cpu_metrics_provider_no_previous_cpu_data():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={},
        branch_dict={},
    )
    fake_mesos_task = mock.MagicMock(
        stats={
            'cpus_limit': 1.1,
            'cpus_system_time_secs': 240,
            'cpus_user_time_secs': 240,
        },
    )
    fake_mesos_task.__getitem__.return_value = 'fake-service.fake-instance'

    fake_marathon_tasks = [mock.Mock(id='fake-service.fake-instance')]

    current_time = datetime.now()

    zookeeper_get_payload = {
        'cpu_last_time': (current_time - timedelta(seconds=600)).strftime('%s'),
        'cpu_data': '0:fake-service.fake-instance',
    }

    with contextlib.nested(
            mock.patch('paasta_tools.utils.KazooClient', autospec=True,
                       return_value=mock.Mock(get=mock.Mock(
                           side_effect=lambda x: (zookeeper_get_payload[x.split('/')[-1]], None)))),
            mock.patch('paasta_tools.autoscaling_lib.datetime', autospec=True),
            mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True,
                       return_value=mock.Mock(get_zk_hosts=mock.Mock())),
    ) as (
        mock_zk_client,
        mock_datetime,
        _,
    ):
        mock_datetime.now.return_value = current_time
        assert autoscaling_lib.mesos_cpu_metrics_provider(
            fake_marathon_service_config, fake_marathon_tasks, (fake_mesos_task,)) == 0.8
        mock_zk_client.return_value.set.assert_has_calls([
            mock.call('/autoscaling/fake-service/fake-instance/cpu_last_time', current_time.strftime('%s')),
            mock.call('/autoscaling/fake-service/fake-instance/cpu_data', '480.0:fake-service.fake-instance'),
        ], any_order=True)


def test_http_metrics_provider():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={},
        branch_dict={},
    )
    fake_marathon_tasks = [mock.Mock(id='fake-service.fake-instance', host='fake_host', ports=[30101])]
    mock_request_result = mock.Mock(json=mock.Mock(return_value={'utilization': '0.5'}))
    with mock.patch('paasta_tools.autoscaling_lib.requests.get', autospec=True, return_value=mock_request_result):
        assert autoscaling_lib.http_metrics_provider(
            fake_marathon_service_config, fake_marathon_tasks, mock.Mock()) == 0.5


def test_http_metrics_provider_no_data():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={},
        branch_dict={},
    )
    fake_marathon_tasks = [mock.Mock(id='fake-service.fake-instance', host='fake_host', ports=[30101])]
    mock_request_result = mock.Mock(json=mock.Mock(return_value='malformed_result'))
    with mock.patch('paasta_tools.autoscaling_lib.requests.get', autospec=True, return_value=mock_request_result):
        with raises(autoscaling_lib.MetricsProviderNoDataError):
            autoscaling_lib.http_metrics_provider(fake_marathon_service_config, fake_marathon_tasks, mock.Mock()) == 0.5


def test_mesos_cpu_metrics_provider_no_data_mesos():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={},
        branch_dict={},
    )
    fake_marathon_tasks = [mock.Mock(id='fake-service.fake-instance')]
    zookeeper_get_payload = {
        'cpu_last_time': '0',
        'cpu_data': '',
    }
    with contextlib.nested(
            mock.patch('paasta_tools.utils.KazooClient', autospec=True,
                       return_value=mock.Mock(get=mock.Mock(
                           side_effect=lambda x: (zookeeper_get_payload[x.split('/')[-1]], None)))),
            mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True,
                       return_value=mock.Mock(get_zk_hosts=mock.Mock())),
    ) as (
        _,
        _,
    ):
        with raises(autoscaling_lib.MetricsProviderNoDataError):
            autoscaling_lib.mesos_cpu_metrics_provider(fake_marathon_service_config, fake_marathon_tasks, [])


def test_autoscale_marathon_instance():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 10},
        branch_dict={},
    )
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling_lib.set_instances_for_marathon_service', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.get_service_metrics_provider', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.get_decision_policy', autospec=True,
                   return_value=mock.Mock(return_value=1)),
        mock.patch.object(marathon_tools.MarathonServiceConfig, 'get_instances', autospec=True, return_value=1),
        mock.patch('paasta_tools.autoscaling_lib._log', autospec=True),
    ) as (
        mock_set_instances_for_marathon_service,
        _,
        _,
        _,
        _,
    ):
        autoscaling_lib.autoscale_marathon_instance(fake_marathon_service_config, [mock.Mock()], [mock.Mock()])
        mock_set_instances_for_marathon_service.assert_called_once_with(
            service='fake-service', instance='fake-instance', instance_count=2)


def test_autoscale_marathon_instance_aborts_when_task_deploying():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 10},
        branch_dict={},
    )
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling_lib.set_instances_for_marathon_service', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.get_service_metrics_provider', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.get_decision_policy', autospec=True,
                   return_value=mock.Mock(return_value=1)),
        mock.patch.object(marathon_tools.MarathonServiceConfig, 'get_instances', autospec=True, return_value=500),
        mock.patch('paasta_tools.autoscaling_lib._log', autospec=True),
    ) as (
        mock_set_instances_for_marathon_service,
        _,
        _,
        _,
        _,
    ):
        autoscaling_lib.autoscale_marathon_instance(fake_marathon_service_config, [mock.Mock()], [mock.Mock()])
        assert not mock_set_instances_for_marathon_service.called


def test_autoscale_services():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 10, 'desired_state': 'start'},
        branch_dict={},
    )
    mock_mesos_tasks = [{'id': 'fake-service.fake-instance'}]
    mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance')]
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling_lib.autoscale_marathon_instance', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.get_marathon_client', autospec=True,
                   return_value=mock.Mock(list_tasks=mock.Mock(return_value=mock_marathon_tasks))),
        mock.patch('paasta_tools.autoscaling_lib.get_running_tasks_from_active_frameworks', autospec=True,
                   return_value=mock_mesos_tasks),
        mock.patch('paasta_tools.autoscaling_lib.load_system_paasta_config', autospec=True,
                   return_value=mock.Mock(get_cluster=mock.Mock())),
        mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True,
                   return_value=mock.Mock(get_zk_hosts=mock.Mock())),
        mock.patch('paasta_tools.autoscaling_lib.get_services_for_cluster', autospec=True,
                   return_value=[('fake-service', 'fake-instance')]),
        mock.patch('paasta_tools.autoscaling_lib.load_marathon_service_config', autospec=True,
                   return_value=fake_marathon_service_config),
        mock.patch('paasta_tools.autoscaling_lib.load_marathon_config', autospec=True),
        mock.patch('paasta_tools.utils.KazooClient', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.create_autoscaling_lock', autospec=True),
    ) as (
        mock_autoscale_marathon_instance,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
    ):
        autoscaling_lib.autoscale_services()
        mock_autoscale_marathon_instance.assert_called_once_with(
            fake_marathon_service_config, mock_marathon_tasks, mock_mesos_tasks)


def test_autoscale_services_bespoke_doesnt_autoscale():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 10, 'desired_state': 'start',
                     'autoscaling': {'decision_policy': 'bespoke'}},
        branch_dict={},
    )
    mock_mesos_tasks = [{'id': 'fake-service.fake-instance'}]
    mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance')]
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling_lib.autoscale_marathon_instance', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.get_marathon_client', autospec=True,
                   return_value=mock.Mock(list_tasks=mock.Mock(return_value=mock_marathon_tasks))),
        mock.patch('paasta_tools.autoscaling_lib.get_running_tasks_from_active_frameworks', autospec=True,
                   return_value=mock_mesos_tasks),
        mock.patch('paasta_tools.autoscaling_lib.load_system_paasta_config', autospec=True,
                   return_value=mock.Mock(get_cluster=mock.Mock())),
        mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True,
                   return_value=mock.Mock(get_zk_hosts=mock.Mock())),
        mock.patch('paasta_tools.autoscaling_lib.get_services_for_cluster', autospec=True,
                   return_value=[('fake-service', 'fake-instance')]),
        mock.patch('paasta_tools.autoscaling_lib.load_marathon_service_config', autospec=True,
                   return_value=fake_marathon_service_config),
        mock.patch('paasta_tools.autoscaling_lib.load_marathon_config', autospec=True),
        mock.patch('paasta_tools.utils.KazooClient', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.create_autoscaling_lock', autospec=True),
    ) as (
        mock_autoscale_marathon_instance,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
    ):
        autoscaling_lib.autoscale_services()
        assert not mock_autoscale_marathon_instance.called
