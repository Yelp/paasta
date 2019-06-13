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
from datetime import datetime
from datetime import timedelta

import asynctest
import mock
from kazoo.exceptions import NoNodeError
from pytest import raises
from requests.exceptions import Timeout

from paasta_tools import marathon_tools
from paasta_tools.autoscaling import autoscaling_service_lib
from paasta_tools.autoscaling.autoscaling_service_lib import autoscaling_is_paused
from paasta_tools.autoscaling.autoscaling_service_lib import filter_autoscaling_tasks
from paasta_tools.autoscaling.autoscaling_service_lib import MAX_TASK_DELTA
from paasta_tools.autoscaling.autoscaling_service_lib import MetricsProviderNoDataError
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
        branch_dict=None,
    )
    with mock.patch(
        'paasta_tools.utils.KazooClient', autospec=True,
    ) as mock_zk_client, mock.patch(
        'paasta_tools.utils.load_system_paasta_config', autospec=True,
    ):
        mock_zk_get = mock.Mock(return_value=(7, None))
        mock_zk_client.return_value = mock.Mock(get=mock_zk_get)
        assert fake_marathon_config.get_instances() == 7
        assert mock_zk_get.call_count == 1


def test_zookeeper_pool():
    with mock.patch(
        'paasta_tools.utils.KazooClient', autospec=True,
    ) as mock_zk_client, mock.patch(
        'paasta_tools.utils.load_system_paasta_config', autospec=True,
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
        branch_dict=None,
    )
    with mock.patch(
        'paasta_tools.utils.KazooClient', autospec=True,
    ) as mock_zk_client, mock.patch(
        'paasta_tools.utils.load_system_paasta_config', autospec=True,
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
        branch_dict=None,
    )
    with mock.patch(
        'paasta_tools.utils.KazooClient', autospec=True,
    ) as mock_zk_client, mock.patch(
        'paasta_tools.utils.load_system_paasta_config', autospec=True,
    ):
        mock_zk_client.return_value = mock.Mock(get=mock.Mock(return_value=(15, None)))
        assert fake_marathon_config.get_instances() == 10
        mock_zk_client.return_value = mock.Mock(get=mock.Mock(return_value=(0, None)))
        assert fake_marathon_config.get_instances() == 5


def test_update_instances_for_marathon_service():
    with mock.patch(
        'paasta_tools.marathon_tools.load_marathon_service_config', autospec=True,
    ) as mock_load_marathon_service_config, mock.patch(
        'paasta_tools.utils.KazooClient', autospec=True,
    ) as mock_zk_client, mock.patch(
        'paasta_tools.utils.load_system_paasta_config', autospec=True,
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
            branch_dict=None,
        )
        autoscaling_service_lib.set_instances_for_marathon_service('service', 'instance', instance_count=8)
        zk_client.set.assert_called_once_with('/autoscaling/service/instance/instances', '8'.encode('utf8'))


def test_compose_autoscaling_zookeeper_root():
    assert autoscaling_service_lib.compose_autoscaling_zookeeper_root(
        'fake-service', 'fake-instance',
    ) == '/autoscaling/fake-service/fake-instance'


def test_get_service_metrics_provider():
    assert autoscaling_service_lib.get_service_metrics_provider(
        'mesos_cpu',
    ) == autoscaling_service_lib.mesos_cpu_metrics_provider


def test_get_decision_policy():
    assert autoscaling_service_lib.get_decision_policy('proportional') == autoscaling_service_lib.proportional_decision_policy  # NOQA


def test_threshold_decision_policy():
    decision_policy_args = {
        'threshold': 0.1,
        'current_instances': 10,
    }
    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.datetime', autospec=True,
    ), mock.patch(
        'paasta_tools.utils.load_system_paasta_config', autospec=True,
        return_value=mock.Mock(get_zk_hosts=mock.Mock()),
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
        branch_dict=None,
    )
    fake_mesos_task = mock.MagicMock(
        stats=asynctest.CoroutineMock(return_value={
            'cpus_limit': 1.1,
            'cpus_system_time_secs': 240,
            'cpus_user_time_secs': 240,
        }),
    )
    fake_system_paasta_config = mock.MagicMock()
    fake_mesos_task.__getitem__.return_value = 'fake-service.fake-instance'

    fake_marathon_tasks = [mock.Mock(id='fake-service.fake-instance')]

    with mock.patch(
        'paasta_tools.utils.KazooClient', autospec=True,
        return_value=mock.Mock(get=mock.Mock(side_effect=NoNodeError)),
    ) as mock_zk_client, mock.patch(
        'paasta_tools.utils.load_system_paasta_config', autospec=True,
        return_value=mock.Mock(get_zk_hosts=mock.Mock()),
    ):
        with raises(autoscaling_service_lib.MetricsProviderNoDataError):
            autoscaling_service_lib.mesos_cpu_metrics_provider(
                fake_marathon_service_config, fake_system_paasta_config, fake_marathon_tasks, (fake_mesos_task,),
            )
        mock_zk_client.return_value.set.assert_has_calls(
            [
                mock.call(
                    '/autoscaling/fake-service/fake-instance/cpu_data',
                    '480.0:fake-service.fake-instance'.encode('utf8'),
                ),
            ], any_order=True,
        )


def test_mesos_cpu_metrics_provider():
    """
    +------+--------------+--------------+---------+-------+---------------+----------------+
    | inst | prev cputime | elasped_time | cputime | limit | norm_cputime  |  utilization   |
    +------+--------------+--------------+---------+-------+---------------+----------------+
    | 1    |            0 |          600 | 480     | 1.1   | 480/(1.1-0.1) | 480/600 => 0.8 |
    | 2    |          300 |          600 | None    | None  | N/A           | N/A            |
    | 3    |       123456 |          600 | {}      | {}    | N/A           | N/A            |
    | -    |            - |            - | -       | -     | -             | -              |
    | avg  |              |              |         |       |               | 0.8            |
    +------+--------------+--------------+---------+-------+---------------+----------------+
    """
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={},
        branch_dict=None,
    )
    fake_system_paasta_config = mock.MagicMock()
    fake_mesos_task = mock.MagicMock(
        stats=asynctest.CoroutineMock(return_value={
            'cpus_limit': 1.1,
            'cpus_system_time_secs': 240,
            'cpus_user_time_secs': 240,
        }),
    )
    fake_mesos_task_2 = mock.MagicMock(
        stats=asynctest.CoroutineMock(side_effect=Exception),
    )
    fake_mesos_task_3 = mock.MagicMock(
        stats=asynctest.CoroutineMock(return_value={}),
    )
    fake_mesos_task.__getitem__.return_value = 'fake-service.fake-instance'
    fake_mesos_task_2.__getitem__.return_value = 'fake-service.fake-instance2'
    fake_mesos_task_3.__getitem__.return_value = 'fake-service.fake-instance3'

    fake_marathon_tasks = [
        mock.Mock(id='fake-service.fake-instance'),
        mock.Mock(id='fake-service.fake-instance2'),
        mock.Mock(id='fake-service.fake-instance3'),
    ]

    current_time = datetime.now()
    last_time = (current_time - timedelta(seconds=600)).strftime('%s')

    fake_old_utilization_data = ','.join([
        '0:fake-service.fake-instance',
        '300:fake-service.fake-instance2',
        '123456:fake-service.fake-instance3',
    ])

    zookeeper_get_payload = {
        'cpu_last_time': last_time,
        'cpu_data': fake_old_utilization_data,
    }

    with mock.patch(
        'paasta_tools.utils.KazooClient', autospec=True,
        return_value=mock.Mock(get=mock.Mock(
            side_effect=lambda x: (str(zookeeper_get_payload[x.split('/')[-1]]).encode('utf-8'), None),
        )),
    ) as mock_zk_client, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.datetime', autospec=True,
    ) as mock_datetime, mock.patch(
        'paasta_tools.utils.load_system_paasta_config', autospec=True,
        return_value=mock.Mock(get_zk_hosts=mock.Mock()),
    ):
        mock_datetime.now.return_value = current_time
        log_utilization_data = {}
        assert 0.8 == autoscaling_service_lib.mesos_cpu_metrics_provider(
            fake_marathon_service_config,
            fake_system_paasta_config,
            fake_marathon_tasks,
            (fake_mesos_task_2, fake_mesos_task_3, fake_mesos_task),
            log_utilization_data=log_utilization_data,
        )
        mock_zk_client.return_value.set.assert_has_calls(
            [
                mock.call(
                    '/autoscaling/fake-service/fake-instance/cpu_last_time',
                    current_time.strftime('%s').encode('utf8'),
                ),
                mock.call(
                    '/autoscaling/fake-service/fake-instance/cpu_data',
                    '480.0:fake-service.fake-instance'.encode('utf8'),
                ),
            ], any_order=True,
        )
        assert log_utilization_data == {
            last_time: fake_old_utilization_data,
            current_time.strftime('%s'): '480.0:fake-service.fake-instance',
        }

        # test noop mode
        mock_zk_client.return_value.set.reset_mock()
        assert 0.8 == autoscaling_service_lib.mesos_cpu_metrics_provider(
            fake_marathon_service_config,
            fake_system_paasta_config,
            fake_marathon_tasks,
            (fake_mesos_task,),
            log_utilization_data=log_utilization_data,
            noop=True,
        )
        assert not mock_zk_client.return_value.set.called


def test_get_json_body_from_service():
    with mock.patch(
            'paasta_tools.autoscaling.autoscaling_service_lib.requests.get', autospec=True,
    ) as mock_request_get:
        mock_request_get.return_value = mock.Mock(json=mock.Mock(return_value=mock.sentinel.json_body))
        assert autoscaling_service_lib.get_json_body_from_service(
            'fake-host', 'fake-port', 'fake-endpoint',
        ) == mock.sentinel.json_body
        mock_request_get.assert_called_once_with(
            'http://fake-host:fake-port/fake-endpoint',
            headers={'User-Agent': mock.ANY}, timeout=2,
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
        branch_dict=None,
    )
    fake_marathon_tasks = [mock.Mock(id='fake-service.fake-instance', host='fake_host', ports=[30101])]
    # KeyError simulates an invalid response
    mock_json_mapper = mock.Mock(side_effect=KeyError(str('Detailed message')))

    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.log.error', autospec=True,
    ) as mock_log_error, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_json_body_from_service', autospec=True,
    ):
        with raises(autoscaling_service_lib.MetricsProviderNoDataError):
            autoscaling_service_lib.get_http_utilization_for_all_tasks(
                fake_marathon_service_config,
                fake_marathon_tasks,
                endpoint='fake-endpoint',
                json_mapper=mock_json_mapper,
            )
        mock_log_error.assert_called_once_with(
            "Caught exception when querying fake-service on fake_host:30101 : 'Detailed message'",
        )


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
        branch_dict=None,
    )
    fake_system_paasta_config = mock.MagicMock()
    fake_marathon_tasks = [mock.Mock(id='fake-service.fake-instance')]
    zookeeper_get_payload = {
        'cpu_last_time': '0',
        'cpu_data': '',
    }
    with mock.patch(
        'paasta_tools.utils.KazooClient', autospec=True,
        return_value=mock.Mock(get=mock.Mock(
            side_effect=lambda x: (str(zookeeper_get_payload[x.split('/')[-1]]).encode('utf-8'), None),
        )),
    ), mock.patch(
        'paasta_tools.utils.load_system_paasta_config', autospec=True,
        return_value=mock.Mock(get_zk_hosts=mock.Mock()),
    ):
        with raises(autoscaling_service_lib.MetricsProviderNoDataError):
            autoscaling_service_lib.mesos_cpu_metrics_provider(
                fake_marathon_service_config,
                fake_system_paasta_config,
                fake_marathon_tasks,
                [],
            )


def test_autoscale_marathon_instance():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 10},
        branch_dict=None,
    )
    fake_system_paasta_config = mock.MagicMock()
    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.set_instances_for_marathon_service',
        autospec=True,
    ) as mock_set_instances_for_marathon_service, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.json', autospec=True,
    ) as mock_json, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_service_metrics_provider', autospec=True,
        **{'return_value.return_value': 0},
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.create_autoscaling_lock', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_decision_policy', autospec=True,
        return_value=mock.Mock(return_value=1),
    ), mock.patch.object(
        marathon_tools.MarathonServiceConfig, 'get_instances', autospec=True, return_value=1,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib._log', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.yelp_meteorite', autospec=True,
    ) as mock_meteorite:
        autoscaling_service_lib.autoscale_marathon_instance(
            fake_marathon_service_config,
            fake_system_paasta_config,
            [mock.Mock()],
            [mock.Mock()],
        )
        mock_set_instances_for_marathon_service.assert_called_once_with(
            service='fake-service', instance='fake-instance', instance_count=2,
        )
        mock_meteorite.create_gauge.call_count == 3
        mock_json.dumps.call_count == 1


def test_autoscale_marathon_instance_up_to_min_instances():
    current_instances = 5
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 10, 'max_instances': 100},
        branch_dict=None,
    )
    fake_system_paasta_config = mock.MagicMock()
    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.set_instances_for_marathon_service',
        autospec=True,
    ) as mock_set_instances_for_marathon_service, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_service_metrics_provider', autospec=True,
        **{'return_value.return_value': 0},
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.create_autoscaling_lock', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_decision_policy', autospec=True,
        return_value=mock.Mock(return_value=-3),
    ), mock.patch.object(
        marathon_tools.MarathonServiceConfig,
        'get_instances',
        autospec=True,
        return_value=current_instances,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib._log', autospec=True,
    ):
        autoscaling_service_lib.autoscale_marathon_instance(
            fake_marathon_service_config,
            fake_system_paasta_config,
            [mock.Mock()] * 5,
            [mock.Mock()] * 5,
        )
        mock_set_instances_for_marathon_service.assert_called_once_with(
            service='fake-service', instance='fake-instance', instance_count=10,
        )

        # even if we don't find the tasks healthy in marathon we shouldn't be below min_instances
        mock_set_instances_for_marathon_service.reset_mock()
        autoscaling_service_lib.autoscale_marathon_instance(
            fake_marathon_service_config,
            fake_system_paasta_config,
            [mock.Mock()] * (int(5 * (1 - MAX_TASK_DELTA)) - 1),
            [mock.Mock()] * (int(5 * (1 - MAX_TASK_DELTA)) - 1),
        )
        mock_set_instances_for_marathon_service.assert_called_once_with(
            service='fake-service', instance='fake-instance', instance_count=10,
        )


def test_autoscale_marathon_instance_below_min_instances():
    current_instances = 7
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 5, 'max_instances': 10},
        branch_dict=None,
    )
    fake_system_paasta_config = mock.MagicMock()
    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.set_instances_for_marathon_service',
        autospec=True,
    ) as mock_set_instances_for_marathon_service, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_service_metrics_provider', autospec=True,
        **{'return_value.return_value': 0},
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.create_autoscaling_lock', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_decision_policy', autospec=True,
        return_value=mock.Mock(return_value=-3),
    ), mock.patch.object(
        marathon_tools.MarathonServiceConfig,
        'get_instances',
        autospec=True,
        return_value=current_instances,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib._log', autospec=True,
    ):
        autoscaling_service_lib.autoscale_marathon_instance(
            fake_marathon_service_config,
            fake_system_paasta_config,
            [mock.Mock() for i in range(current_instances)],
            [mock.Mock()],
        )
        mock_set_instances_for_marathon_service.assert_called_once_with(
            service='fake-service', instance='fake-instance', instance_count=5,
        )


def test_autoscale_marathon_instance_above_max_instances():
    current_instances = 7
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 5, 'max_instances': 10},
        branch_dict=None,
    )
    fake_system_paasta_config = mock.MagicMock()
    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.set_instances_for_marathon_service',
        autospec=True,
    ) as mock_set_instances_for_marathon_service, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_service_metrics_provider', autospec=True,
        **{'return_value.return_value': 0},
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.create_autoscaling_lock', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_decision_policy', autospec=True,
        return_value=mock.Mock(return_value=5),
    ), mock.patch.object(
        marathon_tools.MarathonServiceConfig,
        'get_instances',
        autospec=True,
        return_value=current_instances,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib._log', autospec=True,
    ):
        autoscaling_service_lib.autoscale_marathon_instance(
            fake_marathon_service_config,
            fake_system_paasta_config,
            [mock.Mock() for i in range(current_instances)],
            [mock.Mock()],
        )
        mock_set_instances_for_marathon_service.assert_called_once_with(
            service='fake-service', instance='fake-instance', instance_count=10,
        )


def test_autoscale_marathon_instance_drastic_downscaling():
    current_instances = 100
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 5, 'max_instances': 100},
        branch_dict=None,
    )
    fake_system_paasta_config = mock.MagicMock()
    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.set_instances_for_marathon_service',
        autospec=True,
    ) as mock_set_instances_for_marathon_service, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_service_metrics_provider', autospec=True,
        **{'return_value.return_value': 0},
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.create_autoscaling_lock', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_decision_policy', autospec=True,
        return_value=mock.Mock(return_value=-50),
    ), mock.patch.object(
        marathon_tools.MarathonServiceConfig,
        'get_instances',
        autospec=True,
        return_value=current_instances,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib._log', autospec=True,
    ):
        autoscaling_service_lib.autoscale_marathon_instance(
            fake_marathon_service_config,
            fake_system_paasta_config,
            [mock.Mock() for i in range(current_instances)],
            [mock.Mock()],
        )
        mock_set_instances_for_marathon_service.assert_called_once_with(
            service='fake-service', instance='fake-instance', instance_count=int(current_instances * 0.7),
        )


def test_autoscale_marathon_with_http_stuff():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={
            'min_instances': 1, 'max_instances': 10, 'autoscaling':
            {
                'decision_policy': 'pid',
                'metrics_provider': 'http',
                'endpoint': '/bogus',
            },
        },
        branch_dict=None,
    )
    fake_system_paasta_config = mock.MagicMock()
    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.set_instances_for_marathon_service',
        autospec=True,
    ) as mock_set_instances_for_marathon_service, mock.patch.object(
        marathon_tools.MarathonServiceConfig, 'get_instances', autospec=True, return_value=1,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib._log', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.create_autoscaling_lock', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_http_utilization_for_all_tasks',
        autospec=True,
        return_value=0,
    ) as mock_get_http_utilization_for_all_tasks, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_decision_policy', autospec=True,
        return_value=mock.Mock(return_value=1),
    ):
        autoscaling_service_lib.autoscale_marathon_instance(
            fake_marathon_service_config,
            fake_system_paasta_config,
            [mock.Mock()],
            [mock.Mock()],
        )
        mock_set_instances_for_marathon_service.assert_called_once_with(
            service='fake-service', instance='fake-instance', instance_count=2,
        )
        assert mock_get_http_utilization_for_all_tasks.called


def test_is_task_data_insufficient():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 100},
        branch_dict=None,
    )
    # Test all running
    ret = autoscaling_service_lib.is_task_data_insufficient(
        fake_marathon_service_config,
        [mock.Mock()] * 10,
        10,
    )
    assert not ret

    # Test none running
    ret = autoscaling_service_lib.is_task_data_insufficient(
        fake_marathon_service_config,
        [],
        10,
    )
    assert ret

    # Test more instances above threshold
    ret = autoscaling_service_lib.is_task_data_insufficient(
        fake_marathon_service_config,
        [mock.Mock()] * (int(10 * (1 + MAX_TASK_DELTA)) + 1),
        10,
    )
    assert ret

    # Test more instances below threshold
    ret = autoscaling_service_lib.is_task_data_insufficient(
        fake_marathon_service_config,
        [mock.Mock()] * (int(10 * (1 + MAX_TASK_DELTA)) - 1),
        10,
    )
    assert not ret

    # Test fewer below threshold
    ret = autoscaling_service_lib.is_task_data_insufficient(
        fake_marathon_service_config,
        [mock.Mock()] * (int(10 * (1 - MAX_TASK_DELTA)) - 1),
        10,
    )
    assert ret

    # Test fewer above threshold
    ret = autoscaling_service_lib.is_task_data_insufficient(
        fake_marathon_service_config,
        [mock.Mock()] * (int(10 * (1 - MAX_TASK_DELTA)) + 1),
        10,
    )
    assert not ret


def test_autoscale_marathon_instance_aborts_when_wrong_number_tasks():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 100},
        branch_dict=None,
    )
    fake_system_paasta_config = mock.MagicMock()
    mock_autoscaling_decision = mock.Mock()
    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.set_instances_for_marathon_service',
        autospec=True,
    ) as mock_set_instances_for_marathon_service, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.is_task_data_insufficient', autospec=True,
    ) as mock_is_task_data_insufficient, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_service_metrics_provider', autospec=True,
        **{'return_value.return_value': 0.0},
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.create_autoscaling_lock', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_decision_policy', autospec=True,
        return_value=mock_autoscaling_decision,
    ), mock.patch.object(
        marathon_tools.MarathonServiceConfig, 'get_instances', autospec=True,
    ) as mock_get_instances, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib._log', autospec=True,
    ):
        # test insufficient data scale up
        mock_set_instances_for_marathon_service.reset_mock()
        mock_is_task_data_insufficient.return_value = True
        mock_get_instances.return_value = 10
        mock_autoscaling_decision.return_value = 1
        autoscaling_service_lib.autoscale_marathon_instance(
            fake_marathon_service_config,
            fake_system_paasta_config,
            [mock.Mock()] * 10,
            [mock.Mock()] * 10,
        )
        assert mock_set_instances_for_marathon_service.called

        # test insufficient data scale down
        mock_set_instances_for_marathon_service.reset_mock()
        mock_is_task_data_insufficient.return_value = True
        mock_get_instances.return_value = 10
        mock_autoscaling_decision.return_value = -1
        autoscaling_service_lib.autoscale_marathon_instance(
            fake_marathon_service_config,
            fake_system_paasta_config,
            [mock.Mock()] * 10,
            [mock.Mock()] * 10,
        )
        assert not mock_set_instances_for_marathon_service.called

        # test sufficient data scale up
        mock_set_instances_for_marathon_service.reset_mock()
        mock_is_task_data_insufficient.return_value = False
        mock_get_instances.return_value = 10
        mock_autoscaling_decision.return_value = 1
        autoscaling_service_lib.autoscale_marathon_instance(
            fake_marathon_service_config,
            fake_system_paasta_config,
            [mock.Mock()] * 10,
            [mock.Mock()] * 10,
        )
        assert mock_set_instances_for_marathon_service.called

        # test sufficient data scale down
        mock_set_instances_for_marathon_service.reset_mock()
        mock_is_task_data_insufficient.return_value = False
        mock_get_instances.return_value = 10
        mock_autoscaling_decision.return_value = -1
        autoscaling_service_lib.autoscale_marathon_instance(
            fake_marathon_service_config,
            fake_system_paasta_config,
            [mock.Mock()] * 10,
            [mock.Mock()] * 10,
        )
        assert mock_set_instances_for_marathon_service.called


def test_autoscale_services_happy_path():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 10, 'desired_state': 'start'},
        branch_dict=None,
    )
    mock_mesos_tasks = [{'id': 'fake-service.fake-instance.sha123.sha456'}]
    mock_healthcheck_results = mock.Mock(alive=True)
    mock_marathon_tasks = [mock.Mock(
        id='fake-service.fake-instance.sha123.sha456',
        health_check_results=[mock_healthcheck_results],
    )]

    mock_app = mock.Mock(
        tasks=mock_marathon_tasks,
        health_checks=[mock.Mock()],
    )
    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.autoscale_marathon_instance', autospec=True,
    ) as mock_autoscale_marathon_instance, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_marathon_clients', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_marathon_servers', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_marathon_apps_with_clients', autospec=True,
        return_value=[(mock_app, mock.Mock())],
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_all_running_tasks',
        autospec=True,
        return_value=mock_mesos_tasks,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.load_system_paasta_config', autospec=True,
        return_value=mock.Mock(get_cluster=mock.Mock()),
    ), mock.patch(
        'paasta_tools.utils.load_system_paasta_config', autospec=True,
        return_value=mock.Mock(get_zk_hosts=mock.Mock()),
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.PaastaServiceConfigLoader.instance_configs', autospec=True,
        return_value=[fake_marathon_service_config],
    ), mock.patch(
        'paasta_tools.utils.KazooClient', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.create_autoscaling_lock', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.autoscaling_is_paused', autospec=True,
    ) as mock_paused, mock.patch(
        'paasta_tools.marathon_tools.MarathonServiceConfig.format_marathon_app_dict', autospec=True,
    ) as mock_format_marathon_app_dict:
        mock_paused.return_value = False
        mock_format_marathon_app_dict.return_value = {'id': 'fake-service.fake-instance.sha123.sha456'}
        autoscaling_service_lib.autoscale_services(services=['fake-service'])
        mock_autoscale_marathon_instance.assert_called_once_with(
            fake_marathon_service_config,
            autoscaling_service_lib.load_system_paasta_config(),
            mock_marathon_tasks,
            mock_mesos_tasks,
        )


def test_autoscale_services_not_healthy():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 10, 'desired_state': 'start'},
        branch_dict=None,
    )
    mock_mesos_tasks = [{'id': 'fake-service.fake-instance.sha123.sha456.uuid'}]
    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.autoscale_marathon_instance', autospec=True,
    ) as mock_autoscale_marathon_instance, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.write_to_log', autospec=True,
    ) as mock_write_to_log, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_marathon_clients', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_marathon_servers', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_marathon_apps_with_clients', autospec=True,
    ) as mock_get_marathon_apps_with_clients, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_all_running_tasks',
        autospec=True,
        return_value=mock_mesos_tasks,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.load_system_paasta_config', autospec=True,
        return_value=mock.Mock(get_cluster=mock.Mock()),
    ), mock.patch(
        'paasta_tools.utils.load_system_paasta_config', autospec=True,
        return_value=mock.Mock(get_zk_hosts=mock.Mock()),
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.PaastaServiceConfigLoader.instance_configs', autospec=True,
        return_value=[fake_marathon_service_config],
    ), mock.patch(
        'paasta_tools.utils.KazooClient', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.create_autoscaling_lock', autospec=True,
    ), mock.patch(
        'paasta_tools.marathon_tools.MarathonServiceConfig.format_marathon_app_dict', autospec=True,
    ) as mock_format_marathon_app_dict, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.autoscaling_is_paused', autospec=True,
    ) as mock_paused, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.is_task_healthy', autospec=True,
    ) as mock_is_task_healthy, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.is_old_task_missing_healthchecks', autospec=True,
    ) as mock_is_old_task_missing_healthchecks:
        mock_paused.return_value = False
        mock_is_task_healthy.return_value = True
        mock_is_old_task_missing_healthchecks.return_value = False
        mock_format_marathon_app_dict.return_value = {'id': 'fake-service.fake-instance.sha123.sha456'}
        # Test healthy task
        mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance.sha123.sha456')]
        mock_health_check = mock.Mock()
        mock_marathon_app = mock.Mock(health_checks=[mock_health_check], tasks=mock_marathon_tasks)
        mock_get_marathon_apps_with_clients.return_value = [(mock_marathon_app, mock.Mock())]

        autoscaling_service_lib.autoscale_services(services=['fake-service'])
        assert mock_autoscale_marathon_instance.called

        mock_autoscale_marathon_instance.reset_mock()
        # Test unhealthy task
        mock_is_task_healthy.reset_mock()
        mock_is_task_healthy.return_value = False
        mock_is_old_task_missing_healthchecks.return_value = False
        mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance.sha123.sha456')]
        mock_health_check = mock.Mock()
        mock_marathon_app = mock.Mock(health_checks=[mock_health_check], tasks=mock_marathon_tasks)
        mock_get_marathon_apps_with_clients.return_value = [(mock_marathon_app, mock.Mock())]
        autoscaling_service_lib.autoscale_services(services=['fake-service'])
        assert not mock_autoscale_marathon_instance.called
        mock_write_to_log.assert_called_with(
            config=fake_marathon_service_config,
            line="Caught Exception Couldn't find any healthy marathon tasks",
            level='debug',
        )

        mock_autoscale_marathon_instance.reset_mock()
        # Test no healthcheck defined
        mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance.sha123.sha456')]
        mock_health_check = mock.Mock()
        mock_marathon_app = mock.Mock(health_checks=[], tasks=mock_marathon_tasks)
        mock_get_marathon_apps_with_clients.return_value = [(mock_marathon_app, mock.Mock())]
        autoscaling_service_lib.autoscale_services(services=['fake-service'])
        assert mock_autoscale_marathon_instance.called

        mock_autoscale_marathon_instance.reset_mock()
        # Test unhealthy but old missing hcr
        mock_is_task_healthy.reset_mock()
        mock_is_task_healthy.return_value = False
        mock_is_old_task_missing_healthchecks.return_value = True
        mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance.sha123.sha456')]
        mock_health_check = mock.Mock()
        mock_marathon_app = mock.Mock(health_checks=[], tasks=mock_marathon_tasks)
        mock_get_marathon_apps_with_clients.return_value = [(mock_marathon_app, mock.Mock())]
        autoscaling_service_lib.autoscale_services(services=['fake-service'])
        assert mock_autoscale_marathon_instance.called


def test_autoscale_services_bespoke_doesnt_autoscale():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={
            'min_instances': 1, 'max_instances': 10, 'desired_state': 'start',
            'autoscaling': {'decision_policy': 'bespoke'},
        },
        branch_dict=None,
    )
    mock_mesos_tasks = [{'id': 'fake-service.fake-instance'}]
    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.autoscale_marathon_instance', autospec=True,
    ) as mock_autoscale_marathon_instance, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_marathon_clients', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_marathon_apps_with_clients', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_all_running_tasks',
        autospec=True,
        return_value=mock_mesos_tasks,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.load_system_paasta_config', autospec=True,
        return_value=mock.Mock(get_cluster=mock.Mock()),
    ), mock.patch(
        'paasta_tools.utils.load_system_paasta_config', autospec=True,
        return_value=mock.Mock(get_zk_hosts=mock.Mock()),
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.PaastaServiceConfigLoader.instance_configs', autospec=True,
        return_value=[fake_marathon_service_config],
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_marathon_servers', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.autoscaling_is_paused', autospec=True,
    ) as mock_paused, mock.patch(
        'paasta_tools.utils.KazooClient', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.create_autoscaling_lock', autospec=True,
    ):
        mock_paused.return_value = False
        autoscaling_service_lib.autoscale_services(services=['fake-service'])
        assert not mock_autoscale_marathon_instance.called


def test_autoscale_services_ignores_non_deployed_services():
    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.PaastaServiceConfigLoader._create_service_config',
        autospec=True,
        side_effect=NoDeploymentsAvailable,
    ):
        configs = autoscaling_service_lib.get_configs_of_services_to_scale(
            cluster='fake_cluster',
            services=['fake-service'],
        )
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


def test_autoscaling_is_paused():
    with mock.patch(
        'paasta_tools.utils.KazooClient', autospec=True,
    ) as mock_zk, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.time', autospec=True,
    ) as mock_time, mock.patch(
        'paasta_tools.utils.load_system_paasta_config', autospec=True,
    ):

        # Pausing expired 100 seconds ago
        mock_zk_get = mock.Mock(return_value=(b'100', None))
        mock_zk.return_value = mock.Mock(get=mock_zk_get)
        mock_time.time = mock.Mock(return_value=200)
        assert not autoscaling_is_paused()

        # Pause set until 300, still has 100 more seconds of pausing
        mock_zk_get.return_value = (b'300', None)
        mock_time.time = mock.Mock(return_value=200)
        assert autoscaling_is_paused()

        # With 0 we should be unpaused
        mock_zk_get.return_value = (b'0', None)
        assert not autoscaling_is_paused()


def test_filter_autoscaling_tasks():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 10, 'desired_state': 'start'},
        branch_dict=None,
    )
    mock_mesos_tasks = [{'id': 'fake-service.fake-instance.sha123.sha456.uuid'}]
    with mock.patch(
        'paasta_tools.marathon_tools.MarathonServiceConfig.format_marathon_app_dict', autospec=True,
    ) as mock_format_marathon_app_dict, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.is_task_healthy', autospec=True,
    ) as mock_is_task_healthy, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.is_old_task_missing_healthchecks', autospec=True,
    ) as mock_is_old_task_missing_healthchecks:
        mock_is_task_healthy.return_value = True
        mock_is_old_task_missing_healthchecks.return_value = False
        mock_format_marathon_app_dict.return_value = {'id': 'fake-service.fake-instance.sha123.sha456'}
        # Test healthy task
        mock_health_check = mock.Mock()
        mock_marathon_app = mock.Mock(
            health_checks=[mock_health_check],
            tasks=[mock.Mock(id='fake-service.fake-instance.sha123.sha456.uuid')],
        )
        ret = autoscaling_service_lib.filter_autoscaling_tasks(
            [mock_marathon_app],
            mock_mesos_tasks,
            fake_marathon_service_config,
        )
        assert ret == ({'fake-service.fake-instance.sha123.sha456.uuid': mock_marathon_app.tasks[0]}, mock_mesos_tasks)

        # Test unhealthy task
        mock_is_task_healthy.reset_mock()
        mock_is_task_healthy.return_value = False
        mock_is_old_task_missing_healthchecks.return_value = False
        mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance.sha123.sha456')]
        mock_health_check = mock.Mock()
        mock_marathon_app = mock.Mock(health_checks=[mock_health_check], tasks=mock_marathon_tasks)
        with raises(autoscaling_service_lib.MetricsProviderNoDataError):
            autoscaling_service_lib.filter_autoscaling_tasks(
                [mock_marathon_app],
                mock_mesos_tasks,
                fake_marathon_service_config,
            )

        # Test no healthcheck defined
        mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance.sha123.sha456.uuid')]
        mock_health_check = mock.Mock()
        mock_marathon_app = mock.Mock(health_checks=[], tasks=mock_marathon_tasks)
        ret = autoscaling_service_lib.filter_autoscaling_tasks(
            [mock_marathon_app],
            mock_mesos_tasks,
            fake_marathon_service_config,
        )
        assert ret == ({'fake-service.fake-instance.sha123.sha456.uuid': mock_marathon_tasks[0]}, mock_mesos_tasks)

        # Test unhealthy but old missing hcr
        mock_is_task_healthy.reset_mock()
        mock_is_task_healthy.return_value = False
        mock_is_old_task_missing_healthchecks.return_value = True
        mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance.sha123.sha456.uuid')]
        mock_health_check = mock.Mock()
        mock_marathon_app = mock.Mock(health_checks=[mock_health_check], tasks=mock_marathon_tasks)
        ret = autoscaling_service_lib.filter_autoscaling_tasks(
            [mock_marathon_app],
            mock_mesos_tasks,
            fake_marathon_service_config,
        )
        assert ret == ({'fake-service.fake-instance.sha123.sha456.uuid': mock_marathon_tasks[0]}, mock_mesos_tasks)


def test_get_utilization():
    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_service_metrics_provider', autospec=True,
    ) as mock_get_service_metrics_provider:
        mock_metrics_provider = mock.Mock()
        mock_get_service_metrics_provider.return_value = mock_metrics_provider
        mock_marathon_tasks = mock.Mock()
        mock_mesos_tasks = mock.Mock()
        mock_marathon_service_config = mock.Mock()
        mock_system_paasta_config = mock.Mock()
        mock_log_utilization_data = mock.Mock()
        mock_autoscaling_params = {
            autoscaling_service_lib.SERVICE_METRICS_PROVIDER_KEY: 'mock_provider',
            'mock_param': 2,
        }
        ret = autoscaling_service_lib.get_utilization(
            marathon_service_config=mock_marathon_service_config,
            system_paasta_config=mock_system_paasta_config,
            marathon_tasks=mock_marathon_tasks,
            mesos_tasks=mock_mesos_tasks,
            log_utilization_data=mock_log_utilization_data,
            autoscaling_params=mock_autoscaling_params,
        )
        mock_metrics_provider.assert_called_with(
            marathon_service_config=mock_marathon_service_config,
            system_paasta_config=mock_system_paasta_config,
            marathon_tasks=mock_marathon_tasks,
            mesos_tasks=mock_mesos_tasks,
            log_utilization_data=mock_log_utilization_data,
            mock_param=2,
            metrics_provider='mock_provider',
        )
        assert ret == mock_metrics_provider.return_value


def test_get_new_instance_count():
    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_decision_policy', autospec=True,
    ) as mock_get_decision_policy, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.compose_autoscaling_zookeeper_root', autospec=True,
    ) as mock_compose_autoscaling_zookeeper_root:
        mock_decision_policy = mock.Mock(return_value=1)
        mock_get_decision_policy.return_value = mock_decision_policy
        mock_marathon_service_config = mock.Mock()
        mock_autoscaling_params = {autoscaling_service_lib.DECISION_POLICY_KEY: 'mock_dp', 'mock_param': 2}
        ret = autoscaling_service_lib.get_new_instance_count(
            utilization=0.7,
            error=0.1,
            autoscaling_params=mock_autoscaling_params,
            current_instances=4,
            marathon_service_config=mock_marathon_service_config,
            num_healthy_instances=4,
        )
        assert mock_decision_policy.called_with(
            error=0.1,
            min_instances=mock_marathon_service_config.get_min_instances(),
            max_instances=mock_marathon_service_config.get_max_instances(),
            current_instances=4,
            zookeeper_path=mock_compose_autoscaling_zookeeper_root.return_value,
            mock_param=2,
        )
        mock_marathon_service_config.limit_instance_count.assert_called_with(5)
        assert ret == mock_marathon_service_config.limit_instance_count.return_value

        # test safe_downscaling_threshold
        mock_decision_policy = mock.Mock(return_value=-4)
        mock_get_decision_policy.return_value = mock_decision_policy
        mock_autoscaling_params = {autoscaling_service_lib.DECISION_POLICY_KEY: 'mock_dp', 'mock_param': 2}
        ret = autoscaling_service_lib.get_new_instance_count(
            utilization=0.7,
            error=0.1,
            autoscaling_params=mock_autoscaling_params,
            current_instances=10,
            marathon_service_config=mock_marathon_service_config,
            num_healthy_instances=10,
        )
        mock_marathon_service_config.limit_instance_count.assert_called_with(7)


def test_get_autoscaling_info():
    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_utilization', autospec=True,
    ) as mock_get_utilization, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.filter_autoscaling_tasks', autospec=True,
    ) as mock_filter_autoscaling_tasks, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_error_from_utilization', autospec=True,
    ) as mock_get_error_from_utilization, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_new_instance_count', autospec=True,
    ) as mock_get_new_instance_count, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.load_system_paasta_config', autospec=True,
        return_value=mock.Mock(get_cluster=mock.Mock()),
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_cached_list_of_running_tasks_from_frameworks',
        autospec=True, return_value=[],
    ) as mock_get_all_running_tasks:
        mock_get_utilization.return_value = 0.80131
        mock_apps_with_clients = [(mock.Mock(name='fake_app'), mock.Mock(name='fake_client'))]
        mock_get_new_instance_count.return_value = 6
        mock_service_config = mock.Mock(
            get_max_instances=mock.Mock(return_value=10),
            get_min_instances=mock.Mock(return_value=2),
            get_desired_state=mock.Mock(return_value='start'),
            get_autoscaling_params=mock.Mock(return_value={
                'myarg': 'param',
                'setpoint': 0.7,
            }),
            get_instances=mock.Mock(return_value=4),
        )
        mock_marathon_task = mock.Mock()
        mock_mesos_tasks = mock.Mock()
        mock_filter_autoscaling_tasks.return_value = ({'id1': mock_marathon_task}, mock_mesos_tasks)
        ret = autoscaling_service_lib.get_autoscaling_info(
            mock_apps_with_clients,
            mock_service_config,
        )
        mock_filter_autoscaling_tasks.assert_called_with(
            [mock_apps_with_clients[0][0]],
            mock_get_all_running_tasks.return_value,
            mock_service_config,
        )
        mock_get_utilization.assert_called_with(
            marathon_service_config=mock_service_config,
            system_paasta_config=autoscaling_service_lib.load_system_paasta_config(),
            autoscaling_params={
                'myarg': 'param',
                'noop': True,
                'setpoint': 0.7,
            },
            log_utilization_data={},
            marathon_tasks=[mock_marathon_task],
            mesos_tasks=mock_mesos_tasks,
        )
        mock_get_error_from_utilization.assert_called_with(
            utilization=mock_get_utilization.return_value,
            setpoint=0.7,
            current_instances=4,
        )
        mock_get_new_instance_count.assert_called_with(
            utilization=mock_get_utilization.return_value,
            error=mock_get_error_from_utilization.return_value,
            autoscaling_params={
                'myarg': 'param',
                'noop': True,
                'setpoint': 0.7,
            },
            current_instances=4,
            marathon_service_config=mock_service_config,
            num_healthy_instances=1,
        )
        expected = autoscaling_service_lib.ServiceAutoscalingInfo(
            current_instances=4,
            max_instances=10,
            min_instances=2,
            current_utilization=0.80131,
            target_instances=6,
        )
        assert ret == expected

        # test missing data
        mock_get_utilization.side_effect = MetricsProviderNoDataError
        ret = autoscaling_service_lib.get_autoscaling_info(
            mock_apps_with_clients,
            mock_service_config,
        )
        expected = autoscaling_service_lib.ServiceAutoscalingInfo(
            current_instances=4,
            max_instances=10,
            min_instances=2,
            current_utilization="Exception",
            target_instances="Exception",
        )
        assert ret == expected

        mock_get_utilization.return_value = 0.80131
        mock_filter_autoscaling_tasks.side_effect = MetricsProviderNoDataError
        ret = autoscaling_service_lib.get_autoscaling_info(
            mock_apps_with_clients,
            mock_service_config,
        )
        expected = autoscaling_service_lib.ServiceAutoscalingInfo(
            current_instances=4,
            max_instances=10,
            min_instances=2,
            current_utilization="Exception",
            target_instances="Exception",
        )
        assert ret == expected

        # test regular service has no autoscaling info
        mock_service_config = mock.Mock(get_max_instances=mock.Mock(return_value=None))
        ret = autoscaling_service_lib.get_autoscaling_info(
            mock_apps_with_clients,
            mock_service_config,
        )
        assert ret is None


def test_serialize_and_deserialize_historical_load():
    fake_data = list(zip(range(0, 50, 1), range(50, 0, -1)))
    assert len(fake_data) == 50
    assert len(fake_data[0]) == 2

    serialized = autoscaling_service_lib.serialize_historical_load(fake_data)
    assert len(serialized) == 50 * autoscaling_service_lib.SIZE_PER_HISTORICAL_LOAD_RECORD
    assert autoscaling_service_lib.deserialize_historical_load(serialized) == fake_data


def test_serialize_historical_load_trims_oldest_data():
    fake_data_long = list(zip(range(0, 63000, 1), range(63000, 0, -1)))
    serialized_long = autoscaling_service_lib.serialize_historical_load(fake_data_long)
    assert len(serialized_long) == 1000000
    deserialized_long = autoscaling_service_lib.deserialize_historical_load(serialized_long)
    assert deserialized_long[0] == (500, 62500)
    assert deserialized_long[-1] == (62999, 1)


@mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.save_historical_load', autospec=True)
@mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.fetch_historical_load', autospec=True, return_value=[])
def test_proportional_decision_policy(mock_save_historical_load, mock_fetch_historical_load):

    common_kwargs = {
        'zookeeper_path': '/test',
        'current_instances': 10,
        'min_instances': 5,
        'max_instances': 15,
        'num_healthy_instances': 10,
        'forecast_policy': 'current',
    }

    # if utilization == setpoint, delta should be 0.
    assert 0 == autoscaling_service_lib.proportional_decision_policy(
        setpoint=0.5,
        utilization=0.5,
        **common_kwargs,
    )

    # if utilization is fairly close to setpoint, delta should be 0.
    assert 0 == autoscaling_service_lib.proportional_decision_policy(
        setpoint=0.5,
        utilization=0.524,  # Just under 0.525 = 0.5 * 1.05
        **common_kwargs,
    )
    assert 0 == autoscaling_service_lib.proportional_decision_policy(
        setpoint=0.5,
        utilization=0.476,  # Just over 0.475 = 0.5 * 0.95
        **common_kwargs,
    )

    assert 1 == autoscaling_service_lib.proportional_decision_policy(
        setpoint=0.5,
        utilization=0.526,  # Just over 0.525 = 0.5 * 1.05
        **common_kwargs,
    )

    assert -1 == autoscaling_service_lib.proportional_decision_policy(
        setpoint=0.5,
        utilization=0.474,  # Just under 0.475 = 0.5 * 0.95
        **common_kwargs,
    )

    # If we're 50% overutilized, scale up by 50%
    assert 5 == autoscaling_service_lib.proportional_decision_policy(
        setpoint=0.5,
        utilization=0.75,
        **common_kwargs,
    )

    # If we're 50% underutilized, scale down by 50%
    assert -5 == autoscaling_service_lib.proportional_decision_policy(
        setpoint=0.5,
        utilization=0.25,
        **common_kwargs,
    )


@mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.save_historical_load', autospec=True)
@mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.fetch_historical_load', autospec=True, return_value=[])
def test_proportional_decision_policy_nonzero_offset(mock_save_historical_load, mock_fetch_historical_load):
    common_kwargs = {
        'zookeeper_path': '/test',
        'current_instances': 10,
        'num_healthy_instances': 10,
        'min_instances': 5,
        'max_instances': 15,
        'forecast_policy': 'current',
        'offset': 0.2,
    }

    # if utilization == setpoint, delta should be 0.
    assert 0 == autoscaling_service_lib.proportional_decision_policy(
        setpoint=0.5,
        utilization=0.5,
        **common_kwargs,
    )

    # if utilization is fairly close to setpoint, delta should be 0.
    assert 0 == autoscaling_service_lib.proportional_decision_policy(
        setpoint=0.5,
        utilization=0.514,  # Just under 0.515 = (0.5 - 0.2) * 1.05 + 0.2
        **common_kwargs,
    )
    assert 0 == autoscaling_service_lib.proportional_decision_policy(
        setpoint=0.5,
        utilization=0.486,  # Just over 0.485 = (0.5 - 0.2) * 0.95 + 0.2
        **common_kwargs,
    )

    assert 1 == autoscaling_service_lib.proportional_decision_policy(
        setpoint=0.5,
        utilization=0.516,  # Just over 0.515 = (0.5 - 0.2) * 1.05 + 0.2
        **common_kwargs,
    )

    assert -1 == autoscaling_service_lib.proportional_decision_policy(
        setpoint=0.5,
        utilization=0.484,  # Just under 0.485 = (0.5 - 0.2) * 0.95 + 0.2
        **common_kwargs,
    )

    # If we're 50% overutilized, scale up by 50%
    assert 5 == autoscaling_service_lib.proportional_decision_policy(
        setpoint=0.5,
        utilization=0.65,
        **common_kwargs,
    )

    # If we're 50% underutilized, scale down by 50%
    assert -5 == autoscaling_service_lib.proportional_decision_policy(
        setpoint=0.5,
        utilization=0.35,
        **common_kwargs,
    )


@mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.save_historical_load', autospec=True)
@mock.patch('paasta_tools.autoscaling.autoscaling_service_lib.fetch_historical_load', autospec=True, return_value=[])
def test_proportional_decision_policy_good_enough(mock_save_historical_load, mock_fetch_historical_load):
    assert 0 == autoscaling_service_lib.proportional_decision_policy(
        zookeeper_path='/test',
        current_instances=100,
        num_healthy_instances=100,
        min_instances=50,
        max_instances=150,
        forecast_policy='current',
        offset=0.0,
        setpoint=0.50,
        utilization=0.54,
        good_enough_window=(0.45, 0.55),
    )

    assert 0 == autoscaling_service_lib.proportional_decision_policy(
        zookeeper_path='/test',
        current_instances=100,
        num_healthy_instances=100,
        min_instances=50,
        max_instances=150,
        forecast_policy='current',
        offset=0.0,
        setpoint=0.50,
        utilization=0.46,
        good_enough_window=(0.45, 0.55),
    )

    # current_instances < min_instances, so scale up.
    assert 25 == autoscaling_service_lib.proportional_decision_policy(
        zookeeper_path='/test',
        current_instances=25,
        num_healthy_instances=25,
        min_instances=50,
        max_instances=150,
        forecast_policy='current',
        offset=0.0,
        setpoint=0.50,
        utilization=0.46,
        good_enough_window=(0.45, 0.55),
    )


def test_filter_autoscaling_tasks_considers_old_versions():
    marathon_apps = [
        mock.Mock(
            tasks=[
                mock.Mock(id='service.instance.gitOLD.configOLD.1', app_id='service.instance.gitOLD.configOLD'),
                mock.Mock(id='service.instance.gitOLD.configOLD.2', app_id='service.instance.gitOLD.configOLD'),
            ],
        ),
        mock.Mock(
            tasks=[
                mock.Mock(id='service.instance.gitNEW.configNEW.3', app_id='service.instance.gitNEW.configNEW'),
                mock.Mock(id='service.instance.gitNEW.configNEW.4', app_id='service.instance.gitNEW.configNEW'),
            ],
        ),
    ]

    all_mesos_tasks = [
        {'id': 'service.instance.gitOLD.configOLD.1'},
        {'id': 'service.instance.gitOLD.configOLD.2'},
        {'id': 'service.instance.gitNEW.configNEW.3'},
        {'id': 'service.instance.gitNEW.configNEW.4'},
    ]

    service_config = marathon_tools.MarathonServiceConfig(
        service='service',
        cluster='cluster',
        instance='instance',
        config_dict={},
        branch_dict=None,
        soa_dir='/soa/dir',
    )

    expected = (
        {x.id: x for x in marathon_apps[0].tasks + marathon_apps[1].tasks},
        all_mesos_tasks,
    )

    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.is_task_healthy',
        return_value=True,
        autospec=True,
    ):
        actual = filter_autoscaling_tasks(marathon_apps, all_mesos_tasks, service_config)

    assert actual == expected


def test_autoscale_service_configs():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 10, 'desired_state': 'start'},
        branch_dict=None,
    )
    mock_mesos_tasks = [{'id': 'fake-service.fake-instance.sha123.sha456'}]
    mock_healthcheck_results = mock.Mock(alive=True)
    mock_marathon_tasks = [mock.Mock(
        id='fake-service.fake-instance.sha123.sha456',
        health_check_results=[mock_healthcheck_results],
    )]

    mock_app = mock.Mock(
        tasks=mock_marathon_tasks,
        health_checks=[mock.Mock()],
    )
    mock_system_paasta_config = mock.Mock(get_cluster=mock.Mock()),
    with mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.autoscale_marathon_instance', autospec=True,
    ) as mock_autoscale_marathon_instance, mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_marathon_clients', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_marathon_servers', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_marathon_apps_with_clients', autospec=True,
        return_value=[(mock_app, mock.Mock())],
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.get_all_running_tasks',
        autospec=True,
        return_value=mock_mesos_tasks,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.ZookeeperPool', autospec=True,
    ), mock.patch(
        'paasta_tools.autoscaling.autoscaling_service_lib.autoscaling_is_paused', autospec=True,
        return_value=False,
    ):
        autoscaling_service_lib.autoscale_service_configs(
            service_configs=[fake_marathon_service_config],
            system_paasta_config=mock_system_paasta_config,
        )
        mock_autoscale_marathon_instance.assert_called_with(
            fake_marathon_service_config,
            mock_system_paasta_config,
            mock_marathon_tasks,
            mock_mesos_tasks,
        )
