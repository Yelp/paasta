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
from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib
from datetime import datetime
from datetime import timedelta

import mock
from kazoo.exceptions import NoNodeError
from pytest import raises
from requests.exceptions import Timeout

from paasta_tools import marathon_tools
from paasta_tools.autoscaling import autoscaling_service_lib
from paasta_tools.autoscaling.autoscaling_service_lib import MAX_TASK_DELTA
from paasta_tools.utils import NoDeploymentsAvailable


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
        with autoscaling_service_lib.ZookeeperPool():
            with autoscaling_service_lib.ZookeeperPool():
                assert zk_client.start.call_count == 1
            assert zk_client.stop.call_count == 0

        assert zk_client.stop.call_count == 1


def test_get_zookeeper_instances_defaults_to_max_instances_when_no_zk_node():
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
        assert fake_marathon_config.get_instances() == 10


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
        autoscaling_service_lib.set_instances_for_marathon_service('service', 'instance', instance_count=8)
        zk_client.set.assert_called_once_with('/autoscaling/service/instance/instances', '8')


def test_compose_autoscaling_zookeeper_root():
    assert autoscaling_service_lib.compose_autoscaling_zookeeper_root(
        'fake-service', 'fake-instance') == '/autoscaling/fake-service/fake-instance'


def test_get_service_metrics_provider():
    assert autoscaling_service_lib.get_service_metrics_provider(
        'mesos_cpu') == autoscaling_service_lib.mesos_cpu_metrics_provider


def test_get_decision_policy():
    assert autoscaling_service_lib.get_decision_policy('pid') == autoscaling_service_lib.pid_decision_policy


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
            mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.datetime', autospec=True),
            mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True,
                       return_value=mock.Mock(get_zk_hosts=mock.Mock())),
    ) as (
        mock_zk_client,
        mock_datetime,
        _,
    ):
        mock_datetime.now.return_value = current_time
        assert autoscaling_service_lib.pid_decision_policy('/autoscaling/fake-service/fake-instance',
                                                           10, 1, 100, 0.0) == 0
        assert autoscaling_service_lib.pid_decision_policy('/autoscaling/fake-service/fake-instance',
                                                           10, 1, 100, 0.2) == 1
        assert autoscaling_service_lib.pid_decision_policy('/autoscaling/fake-service/fake-instance',
                                                           10, 1, 100, -0.2) == -1
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
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.datetime', autospec=True),
        mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True,
                   return_value=mock.Mock(get_zk_hosts=mock.Mock())),
    ) as (
        mock_datetime,
        _,
    ):
        assert autoscaling_service_lib.threshold_decision_policy(error=0, **decision_policy_args) == 0
        assert autoscaling_service_lib.threshold_decision_policy(error=0.5, **decision_policy_args) == 1
        assert autoscaling_service_lib.threshold_decision_policy(error=-0.5, **decision_policy_args) == -1


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
        with raises(autoscaling_service_lib.MetricsProviderNoDataError):
            autoscaling_service_lib.mesos_cpu_metrics_provider(
                fake_marathon_service_config, fake_marathon_tasks, (fake_mesos_task,))
        mock_zk_client.return_value.set.assert_has_calls([
            mock.call('/autoscaling/fake-service/fake-instance/cpu_data', '480.0:fake-service.fake-instance'),
        ], any_order=True)


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

    current_time = datetime.now()
    last_time = (current_time - timedelta(seconds=600)).strftime('%s')

    zookeeper_get_payload = {
        'cpu_last_time': last_time,
        'cpu_data': '0:fake-service.fake-instance',
    }

    with contextlib.nested(
            mock.patch('paasta_tools.utils.KazooClient', autospec=True,
                       return_value=mock.Mock(get=mock.Mock(
                           side_effect=lambda x: (zookeeper_get_payload[x.split('/')[-1]], None)))),
            mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.datetime', autospec=True),
            mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True,
                       return_value=mock.Mock(get_zk_hosts=mock.Mock())),
    ) as (
        mock_zk_client,
        mock_datetime,
        _,
    ):
        mock_datetime.now.return_value = current_time
        log_utilization_data = {}
        assert autoscaling_service_lib.mesos_cpu_metrics_provider(
            fake_marathon_service_config,
            fake_marathon_tasks,
            (fake_mesos_task,),
            log_utilization_data=log_utilization_data) == 0.8
        mock_zk_client.return_value.set.assert_has_calls([
            mock.call('/autoscaling/fake-service/fake-instance/cpu_last_time', current_time.strftime('%s')),
            mock.call('/autoscaling/fake-service/fake-instance/cpu_data', '480.0:fake-service.fake-instance'),
        ], any_order=True)
        assert log_utilization_data == {last_time: '0:fake-service.fake-instance',
                                        current_time.strftime('%s'): '480.0:fake-service.fake-instance'}


def test_get_json_body_from_service():
    with mock.patch(
            'paasta_tools.autoscaling.autoscaling_service_lib.requests.get', autospec=True) as mock_request_get:
        mock_request_get.return_value = mock.Mock(json=mock.Mock(return_value=mock.sentinel.json_body))
        assert autoscaling_service_lib.get_json_body_from_service(
            'fake-host', 'fake-port', 'fake-endpoint') == mock.sentinel.json_body
        mock_request_get.assert_called_once_with(
            'http://fake-host:fake-port/fake-endpoint',
            headers={'User-Agent': mock.ANY},
        )


def test_get_http_utilization_for_all_tasks():
    fake_marathon_tasks = [mock.Mock(id='fake-service.fake-instance', host='fake_host', ports=[30101])]
    mock_json_mapper = mock.Mock(return_value=0.5)

    with mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_json_body_from_service', autospec=True):
        assert autoscaling_service_lib.get_http_utilization_for_all_tasks(
            marathon_service_config=mock.Mock(),
            marathon_tasks=fake_marathon_tasks,
            endpoint='fake-endpoint',
            json_mapper=mock_json_mapper,
        ) == 0.5


def test_get_http_utilization_for_all_tasks_timeout():
    fake_marathon_tasks = [mock.Mock(id='fake-service.fake-instance', host='fake_host', ports=[30101])]
    mock_json_mapper = mock.Mock(side_effect=Timeout)

    with mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_json_body_from_service', autospec=True):
        assert autoscaling_service_lib.get_http_utilization_for_all_tasks(
            marathon_service_config=mock.Mock(),
            marathon_tasks=fake_marathon_tasks,
            endpoint='fake-endpoint',
            json_mapper=mock_json_mapper,
        ) == 1.0


def test_get_http_utilization_for_all_tasks_no_data():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={},
        branch_dict={},
    )
    fake_marathon_tasks = [mock.Mock(id='fake-service.fake-instance', host='fake_host', ports=[30101])]
    mock_json_mapper = mock.Mock(side_effect=KeyError('Detailed message'))  # KeyError simulates an invalid response

    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.log.debug', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_json_body_from_service', autospec=True),
    ) as (
        mock_log_debug,
        _,
    ):
        with raises(autoscaling_service_lib.MetricsProviderNoDataError):
            autoscaling_service_lib.get_http_utilization_for_all_tasks(
                fake_marathon_service_config,
                fake_marathon_tasks,
                endpoint='fake-endpoint',
                json_mapper=mock_json_mapper,
            )
        mock_log_debug.assert_called_once_with(
            "Caught excpetion when querying fake-service on fake_host:30101 : 'Detailed message'")


def test_http_metrics_provider():
    fake_marathon_tasks = [mock.Mock(id='fake-service.fake-instance', host='fake_host', ports=[30101])]

    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_json_body_from_service',
        autospec=True,
    ) as mock_get_json_body_from_service:
        mock_get_json_body_from_service.return_value = {'utilization': 0.5}
        assert autoscaling_service_lib.http_metrics_provider(
            marathon_service_config=mock.Mock(),
            marathon_tasks=fake_marathon_tasks,
        ) == 0.5


def test_uwsgi_metrics_provider():
    fake_marathon_tasks = [mock.Mock(id='fake-service.fake-instance', host='fake_host', ports=[30101])]

    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_json_body_from_service',
        autospec=True,
    ) as mock_get_json_body_from_service:
        mock_get_json_body_from_service.return_value = {
            'workers': [
                {'status': 'idle'},
                {'status': 'busy'},
                {'status': 'busy'},
                {'status': 'busy'},
            ],
        }
        assert autoscaling_service_lib.uwsgi_metrics_provider(
            marathon_service_config=mock.Mock(),
            marathon_tasks=fake_marathon_tasks,
        ) == 0.75


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
        with raises(autoscaling_service_lib.MetricsProviderNoDataError):
            autoscaling_service_lib.mesos_cpu_metrics_provider(fake_marathon_service_config, fake_marathon_tasks, [])


def test_autoscale_marathon_instance():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 10},
        branch_dict={},
    )
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.set_instances_for_marathon_service',
                   autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_service_metrics_provider', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_decision_policy', autospec=True,
                   return_value=mock.Mock(return_value=1)),
        mock.patch.object(marathon_tools.MarathonServiceConfig, 'get_instances', autospec=True, return_value=1),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib._log', autospec=True),
    ) as (
        mock_set_instances_for_marathon_service,
        _,
        _,
        _,
        _,
    ):
        autoscaling_service_lib.autoscale_marathon_instance(fake_marathon_service_config, [mock.Mock()], [mock.Mock()])
        mock_set_instances_for_marathon_service.assert_called_once_with(
            service='fake-service', instance='fake-instance', instance_count=2)


def test_autoscale_marathon_instance_up_to_min_instances():
    current_instances = 5
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 10, 'max_instances': 100},
        branch_dict={},
    )
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.set_instances_for_marathon_service',
                   autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_service_metrics_provider', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_decision_policy', autospec=True,
                   return_value=mock.Mock(return_value=-3)),
        mock.patch.object(marathon_tools.MarathonServiceConfig,
                          'get_instances',
                          autospec=True,
                          return_value=current_instances),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib._log', autospec=True),
    ) as (
        mock_set_instances_for_marathon_service,
        _,
        _,
        _,
        _,
    ):
        autoscaling_service_lib.autoscale_marathon_instance(fake_marathon_service_config,
                                                            [mock.Mock()] * 5,
                                                            [mock.Mock()] * 5)
        mock_set_instances_for_marathon_service.assert_called_once_with(
            service='fake-service', instance='fake-instance', instance_count=10)

        # even if we don't find the tasks healthy in marathon we shouldn't be below min_instances
        mock_set_instances_for_marathon_service.reset_mock()
        autoscaling_service_lib.autoscale_marathon_instance(fake_marathon_service_config,
                                                            [mock.Mock()] * (int(5 * (1 - MAX_TASK_DELTA)) - 1),
                                                            [mock.Mock()] * (int(5 * (1 - MAX_TASK_DELTA)) - 1))
        mock_set_instances_for_marathon_service.assert_called_once_with(
            service='fake-service', instance='fake-instance', instance_count=10)


def test_autoscale_marathon_instance_below_min_instances():
    current_instances = 7
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 5, 'max_instances': 10},
        branch_dict={},
    )
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.set_instances_for_marathon_service',
                   autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_service_metrics_provider', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_decision_policy', autospec=True,
                   return_value=mock.Mock(return_value=-3)),
        mock.patch.object(marathon_tools.MarathonServiceConfig,
                          'get_instances',
                          autospec=True,
                          return_value=current_instances),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib._log', autospec=True),
    ) as (
        mock_set_instances_for_marathon_service,
        _,
        _,
        _,
        _,
    ):
        autoscaling_service_lib.autoscale_marathon_instance(fake_marathon_service_config,
                                                            [mock.Mock() for i in xrange(current_instances)],
                                                            [mock.Mock()])
        mock_set_instances_for_marathon_service.assert_called_once_with(
            service='fake-service', instance='fake-instance', instance_count=5)


def test_autoscale_marathon_instance_above_max_instances():
    current_instances = 7
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 5, 'max_instances': 10},
        branch_dict={},
    )
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.set_instances_for_marathon_service',
                   autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_service_metrics_provider', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_decision_policy', autospec=True,
                   return_value=mock.Mock(return_value=5)),
        mock.patch.object(marathon_tools.MarathonServiceConfig,
                          'get_instances',
                          autospec=True,
                          return_value=current_instances),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib._log', autospec=True),
    ) as (
        mock_set_instances_for_marathon_service,
        _,
        _,
        _,
        _,
    ):
        autoscaling_service_lib.autoscale_marathon_instance(fake_marathon_service_config,
                                                            [mock.Mock() for i in xrange(current_instances)],
                                                            [mock.Mock()])
        mock_set_instances_for_marathon_service.assert_called_once_with(
            service='fake-service', instance='fake-instance', instance_count=10)


def test_autoscale_marathon_instance_drastic_downscaling():
    current_instances = 100
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 5, 'max_instances': 100},
        branch_dict={},
    )
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.set_instances_for_marathon_service',
                   autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_service_metrics_provider', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_decision_policy', autospec=True,
                   return_value=mock.Mock(return_value=-50)),
        mock.patch.object(marathon_tools.MarathonServiceConfig,
                          'get_instances',
                          autospec=True,
                          return_value=current_instances),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib._log', autospec=True),
    ) as (
        mock_set_instances_for_marathon_service,
        _,
        _,
        _,
        _,
    ):
        autoscaling_service_lib.autoscale_marathon_instance(fake_marathon_service_config,
                                                            [mock.Mock() for i in xrange(current_instances)],
                                                            [mock.Mock()])
        mock_set_instances_for_marathon_service.assert_called_once_with(
            service='fake-service', instance='fake-instance', instance_count=int(current_instances * 0.7))


def test_autoscale_marathon_with_http_stuff():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 10, 'autoscaling':
                     {
                         'decision_policy': 'pid',
                         'metrics_provider': 'http',
                         'endpoint': '/bogus',
                     },
                     },
        branch_dict={},
    )
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.set_instances_for_marathon_service',
                   autospec=True),
        mock.patch.object(marathon_tools.MarathonServiceConfig, 'get_instances', autospec=True, return_value=1),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib._log', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_http_utilization_for_all_tasks',
                   autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_decision_policy', autospec=True,
                   return_value=mock.Mock(return_value=1)),
    ) as (
        mock_set_instances_for_marathon_service,
        _,
        _,
        mock_get_http_utilization_for_all_tasks,
        _,
    ):
        autoscaling_service_lib.autoscale_marathon_instance(fake_marathon_service_config, [mock.Mock()], [mock.Mock()])
        mock_set_instances_for_marathon_service.assert_called_once_with(
            service='fake-service', instance='fake-instance', instance_count=2)
        assert mock_get_http_utilization_for_all_tasks.called


def test_autoscale_marathon_instance_aborts_when_wrong_number_tasks():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 100},
        branch_dict={},
    )
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.set_instances_for_marathon_service',
                   autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_service_metrics_provider', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_decision_policy', autospec=True,
                   return_value=mock.Mock(return_value=1)),
        mock.patch.object(marathon_tools.MarathonServiceConfig, 'get_instances', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib._log', autospec=True),
    ) as (
        mock_set_instances_for_marathon_service,
        _,
        _,
        mock_get_instances,
        _,
    ):
        # Test all running
        mock_set_instances_for_marathon_service.reset_mock()
        mock_get_instances.return_value = 10
        autoscaling_service_lib.autoscale_marathon_instance(fake_marathon_service_config,
                                                            [mock.Mock()] * 10,
                                                            [mock.Mock()] * 10)
        assert mock_set_instances_for_marathon_service.called

        # Test none running
        mock_set_instances_for_marathon_service.reset_mock()
        mock_get_instances.return_value = 10
        autoscaling_service_lib.autoscale_marathon_instance(fake_marathon_service_config,
                                                            [], [])
        assert not mock_set_instances_for_marathon_service.called

        # Test more instances above threshold
        mock_set_instances_for_marathon_service.reset_mock()
        mock_get_instances.return_value = 10
        autoscaling_service_lib.autoscale_marathon_instance(fake_marathon_service_config,
                                                            [mock.Mock()] * (int(10 * (1 + MAX_TASK_DELTA)) + 1),
                                                            [mock.Mock()] * (int(10 * (1 + MAX_TASK_DELTA)) + 1))
        assert not mock_set_instances_for_marathon_service.called

        # Test more instances below threshold
        mock_set_instances_for_marathon_service.reset_mock()
        mock_get_instances.return_value = 10
        autoscaling_service_lib.autoscale_marathon_instance(fake_marathon_service_config,
                                                            [mock.Mock()] * (int(10 * (1 + MAX_TASK_DELTA)) - 1),
                                                            [mock.Mock()] * (int(10 * (1 + MAX_TASK_DELTA)) - 1))
        assert mock_set_instances_for_marathon_service.called

        # Test fewer below threshold
        mock_set_instances_for_marathon_service.reset_mock()
        mock_get_instances.return_value = 10
        autoscaling_service_lib.autoscale_marathon_instance(fake_marathon_service_config,
                                                            [mock.Mock()] * (int(10 * (1 - MAX_TASK_DELTA)) - 1),
                                                            [mock.Mock()] * (int(10 * (1 - MAX_TASK_DELTA)) - 1))
        assert not mock_set_instances_for_marathon_service.called

        # Test fewer above threshold
        mock_set_instances_for_marathon_service.reset_mock()
        mock_get_instances.return_value = 10
        autoscaling_service_lib.autoscale_marathon_instance(fake_marathon_service_config,
                                                            [mock.Mock()] * (int(10 * (1 - MAX_TASK_DELTA)) + 1),
                                                            [mock.Mock()] * (int(10 * (1 - MAX_TASK_DELTA)) + 1))
        assert mock_set_instances_for_marathon_service.called


def test_autoscale_services_happy_path():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 10, 'desired_state': 'start'},
        branch_dict={},
    )
    mock_mesos_tasks = [{'id': 'fake-service.fake-instance.sha123.sha456'}]
    mock_healthcheck_results = mock.Mock(alive=True)
    mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance.sha123.sha456',
                                     health_check_results=[mock_healthcheck_results])]
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.autoscale_marathon_instance', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_marathon_client', autospec=True,
                   return_value=mock.Mock(list_tasks=mock.Mock(return_value=mock_marathon_tasks))),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_all_running_tasks',
                   autospec=True,
                   return_value=mock_mesos_tasks),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.load_system_paasta_config', autospec=True,
                   return_value=mock.Mock(get_cluster=mock.Mock())),
        mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True,
                   return_value=mock.Mock(get_zk_hosts=mock.Mock())),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_services_for_cluster', autospec=True,
                   return_value=[('fake-service', 'fake-instance')]),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.load_marathon_service_config', autospec=True,
                   return_value=fake_marathon_service_config),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.load_marathon_config', autospec=True),
        mock.patch('paasta_tools.utils.KazooClient', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.create_autoscaling_lock', autospec=True),
        mock.patch('paasta_tools.marathon_tools.MarathonServiceConfig.format_marathon_app_dict', autospec=True),
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
        mock_format_marathon_app_dict,
    ):
        mock_format_marathon_app_dict.return_value = {'id': 'fake-service.fake-instance.sha123.sha456'}
        autoscaling_service_lib.autoscale_services()
        mock_autoscale_marathon_instance.assert_called_once_with(
            fake_marathon_service_config, mock_marathon_tasks, mock_mesos_tasks)


def test_autoscale_services_not_healthy():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 10, 'desired_state': 'start'},
        branch_dict={},
    )
    mock_mesos_tasks = [{'id': 'fake-service.fake-instance.sha123.sha456.uuid'}]
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.autoscale_marathon_instance', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.write_to_log', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_marathon_client', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_all_running_tasks',
                   autospec=True,
                   return_value=mock_mesos_tasks),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.load_system_paasta_config', autospec=True,
                   return_value=mock.Mock(get_cluster=mock.Mock())),
        mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True,
                   return_value=mock.Mock(get_zk_hosts=mock.Mock())),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_services_for_cluster', autospec=True,
                   return_value=[('fake-service', 'fake-instance')]),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.load_marathon_service_config', autospec=True,
                   return_value=fake_marathon_service_config),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.load_marathon_config', autospec=True),
        mock.patch('paasta_tools.utils.KazooClient', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.create_autoscaling_lock', autospec=True),
        mock.patch('paasta_tools.marathon_tools.MarathonServiceConfig.format_marathon_app_dict', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.is_task_healthy', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.is_old_task_missing_healthchecks', autospec=True),
    ) as (
        mock_autoscale_marathon_instance,
        mock_write_to_log,
        mock_marathon_client,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        mock_format_marathon_app_dict,
        mock_is_task_healthy,
        mock_is_old_task_missing_healthchecks,
    ):

        mock_is_task_healthy.return_value = True
        mock_is_old_task_missing_healthchecks.return_value = False
        mock_format_marathon_app_dict.return_value = {'id': 'fake-service.fake-instance.sha123.sha456'}
        # Test healthy task
        mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance.sha123.sha456')]
        mock_health_check = mock.Mock()
        mock_marathon_app = mock.Mock(health_checks=[mock_health_check])
        mock_marathon_client.return_value = mock.Mock(list_tasks=mock.Mock(return_value=mock_marathon_tasks),
                                                      get_app=mock.Mock(return_value=mock_marathon_app))
        autoscaling_service_lib.autoscale_services()
        assert mock_autoscale_marathon_instance.called

        mock_autoscale_marathon_instance.reset_mock()
        # Test unhealthy task
        mock_is_task_healthy.reset_mock()
        mock_is_task_healthy.return_value = False
        mock_is_old_task_missing_healthchecks.return_value = False
        mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance.sha123.sha456')]
        mock_health_check = mock.Mock()
        mock_marathon_app = mock.Mock(health_checks=[mock_health_check])
        mock_marathon_client.return_value = mock.Mock(list_tasks=mock.Mock(return_value=mock_marathon_tasks),
                                                      get_app=mock.Mock(return_value=mock_marathon_app))
        autoscaling_service_lib.autoscale_services()
        assert not mock_autoscale_marathon_instance.called
        mock_write_to_log.assert_called_with(config=fake_marathon_service_config,
                                             line="Caught Exception Couldn't find any healthy marathon tasks")

        mock_autoscale_marathon_instance.reset_mock()
        # Test no healthcheck defined
        mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance.sha123.sha456')]
        mock_health_check = mock.Mock()
        mock_marathon_app = mock.Mock(health_checks=[])
        mock_marathon_client.return_value = mock.Mock(list_tasks=mock.Mock(return_value=mock_marathon_tasks),
                                                      get_app=mock.Mock(return_value=mock_marathon_app))
        autoscaling_service_lib.autoscale_services()
        assert mock_autoscale_marathon_instance.called

        mock_autoscale_marathon_instance.reset_mock()
        # Test unhealthy but old missing hcr
        mock_is_task_healthy.reset_mock()
        mock_is_task_healthy.return_value = False
        mock_is_old_task_missing_healthchecks.return_value = True
        mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance.sha123.sha456')]
        mock_health_check = mock.Mock()
        mock_marathon_app = mock.Mock(health_checks=[mock_health_check])
        mock_marathon_client.return_value = mock.Mock(list_tasks=mock.Mock(return_value=mock_marathon_tasks),
                                                      get_app=mock.Mock(return_value=mock_marathon_app))
        autoscaling_service_lib.autoscale_services()
        assert mock_autoscale_marathon_instance.called


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
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.autoscale_marathon_instance', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_marathon_client', autospec=True,
                   return_value=mock.Mock(list_tasks=mock.Mock(return_value=mock_marathon_tasks))),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_all_running_tasks',
                   autospec=True,
                   return_value=mock_mesos_tasks),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.load_system_paasta_config', autospec=True,
                   return_value=mock.Mock(get_cluster=mock.Mock())),
        mock.patch('paasta_tools.utils.load_system_paasta_config', autospec=True,
                   return_value=mock.Mock(get_zk_hosts=mock.Mock())),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_services_for_cluster', autospec=True,
                   return_value=[('fake-service', 'fake-instance')]),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.load_marathon_service_config', autospec=True,
                   return_value=fake_marathon_service_config),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.load_marathon_config', autospec=True),
        mock.patch('paasta_tools.utils.KazooClient', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.create_autoscaling_lock', autospec=True),
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
        autoscaling_service_lib.autoscale_services()
        assert not mock_autoscale_marathon_instance.called


def test_autoscale_services_ignores_non_deployed_services():
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.get_services_for_cluster', autospec=True,
                   return_value=[('fake-service', 'fake-instance')]),
        mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.load_marathon_service_config', autospec=True,
                   side_effect=NoDeploymentsAvailable),
    ) as (
        _,
        _,
    ):
        configs = autoscaling_service_lib.get_configs_of_services_to_scale(cluster='fake_cluster')
        assert len(configs) == 0, configs


def test_humanize_error_above():
    actual = autoscaling_service_lib.humanize_error(1.0)
    assert actual == "100% overutilized"


def test_humanize_error_below():
    actual = autoscaling_service_lib.humanize_error(-1.0)
    assert actual == "100% underutilized"


def test_humanize_error_equal():
    actual = autoscaling_service_lib.humanize_error(0.0)
    assert actual == "utilization within thresholds"
