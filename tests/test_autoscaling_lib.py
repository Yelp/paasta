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
import contextlib
from datetime import datetime
from datetime import timedelta

import mock
from botocore.exceptions import ClientError
from kazoo.exceptions import NoNodeError
from pytest import raises

from paasta_tools import autoscaling_lib
from paasta_tools import marathon_tools
from paasta_tools.mesos_tools import SlaveTaskCount
from paasta_tools.paasta_metastatus import ResourceInfo
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import TimeoutError


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


def test_autoscale_services_happy_path():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 10, 'desired_state': 'start'},
        branch_dict={},
    )
    mock_mesos_tasks = [{'id': 'fake-service.fake-instance'}]
    mock_healthcheck_results = mock.Mock(alive=True)
    mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance',
                                     health_check_results=[mock_healthcheck_results])]
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


def test_autoscale_services_not_healthy():
    fake_marathon_service_config = marathon_tools.MarathonServiceConfig(
        service='fake-service',
        instance='fake-instance',
        cluster='fake-cluster',
        config_dict={'min_instances': 1, 'max_instances': 10, 'desired_state': 'start'},
        branch_dict={},
    )
    mock_mesos_tasks = [{'id': 'fake-service.fake-instance'}]
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling_lib.autoscale_marathon_instance', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.write_to_log', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.get_marathon_client', autospec=True),
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
    ):

        # Test missing health_check_results
        mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance',
                                         health_check_results=[])]
        mock_marathon_app = mock.Mock()
        mock_marathon_app.health_checks = ["some-healthcheck-definition"]
        mock_marathon_client.return_value = mock.Mock(list_tasks=mock.Mock(return_value=mock_marathon_tasks),
                                                      get_app=mock.Mock(return_value=mock_marathon_app))
        autoscaling_lib.autoscale_services()
        mock_write_to_log.assert_called_with(config=fake_marathon_service_config,
                                             line="Caught Exception Couldn't find any healthy marathon tasks")
        assert not mock_autoscale_marathon_instance.called

        # Test present results but not yet passing
        mock_healthcheck_results = mock.Mock(alive=False)
        mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance',
                                         health_check_results=[mock_healthcheck_results])]
        mock_marathon_app = mock.Mock()
        mock_marathon_app.health_checks = ["some-healthcheck-definition"]
        mock_marathon_client.return_value = mock.Mock(list_tasks=mock.Mock(return_value=mock_marathon_tasks),
                                                      get_app=mock.Mock(return_value=mock_marathon_app))
        autoscaling_lib.autoscale_services()
        mock_write_to_log.assert_called_with(config=fake_marathon_service_config,
                                             line="Caught Exception Couldn't find any healthy marathon tasks")
        assert not mock_autoscale_marathon_instance.called

        # Test no healthcheck defined
        mock_marathon_tasks = [mock.Mock(id='fake-service.fake-instance',
                                         health_check_results=[])]
        mock_marathon_app = mock.Mock()
        mock_marathon_app.health_checks = []
        mock_marathon_client.return_value = mock.Mock(list_tasks=mock.Mock(return_value=mock_marathon_tasks),
                                                      get_app=mock.Mock(return_value=mock_marathon_app))
        autoscaling_lib.autoscale_services()
        mock_write_to_log.assert_called_with(config=fake_marathon_service_config,
                                             line="Caught Exception Couldn't find any healthy marathon tasks")
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


def test_autoscale_services_ignores_non_deployed_services():
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling_lib.get_services_for_cluster', autospec=True,
                   return_value=[('fake-service', 'fake-instance')]),
        mock.patch('paasta_tools.autoscaling_lib.load_marathon_service_config', autospec=True,
                   side_effect=NoDeploymentsAvailable),
    ) as (
        _,
        _,
    ):
        configs = autoscaling_lib.get_configs_of_services_to_scale(cluster='fake_cluster')
        assert len(configs) == 0, configs


def test_humanize_error_above():
    actual = autoscaling_lib.humanize_error(1.0)
    assert actual == "100% overutilized"


def test_humanize_error_below():
    actual = autoscaling_lib.humanize_error(-1.0)
    assert actual == "100% underutilized"


def test_humanize_error_equal():
    actual = autoscaling_lib.humanize_error(0.0)
    assert actual == "utilization within thresholds"


def test_scale_aws_spot_fleet_request():
    with contextlib.nested(
        mock.patch('time.time', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.filter_sfr_slaves', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.drain', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.undrain', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.set_spot_fleet_request_capacity', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.wait_and_terminate', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.get_mesos_state_summary_from_leader', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.get_mesos_task_count_by_slave', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.sort_slaves_to_kill')
    ) as (
        mock_time,
        mock_filter_sfr_slaves,
        mock_drain,
        mock_undrain,
        mock_set_spot_fleet_request_capacity,
        mock_wait_and_terminate,
        mock_get_mesos_state_summary_from_leader,
        mock_get_mesos_task_count_by_slave,
        mock_sort_slaves_to_kill
    ):

        mock_sfr = mock.Mock()
        mock_resource = {'id': 'sfr-blah', 'sfr': mock_sfr, 'region': 'westeros-1', 'pool': 'default'}
        mock_pool_settings = {'drain_timeout': 123}
        mock_set_spot_fleet_request_capacity.return_value = True
        mock_mesos_state = mock.Mock()
        mock_get_mesos_state_summary_from_leader.return_value = mock_mesos_state

        # test no scale
        autoscaling_lib.scale_aws_spot_fleet_request(mock_resource, 4, 4, mock_pool_settings, False)
        assert not mock_set_spot_fleet_request_capacity.called

        # test scale up
        autoscaling_lib.scale_aws_spot_fleet_request(mock_resource, 2, 4, mock_pool_settings, False)
        mock_set_spot_fleet_request_capacity.assert_called_with('sfr-blah', 4, False, region='westeros-1')

        # test scale down
        mock_slave_1 = {'hostname': 'host1', 'instance_id': 'i-blah123',
                        'pid': 'slave(1)@10.1.1.1:5051', 'instance_weight': 1,
                        'ip': '10.1.1.1'}
        mock_slave_2 = {'hostname': 'host2', 'instance_id': 'i-blah456',
                        'pid': 'slave(1)@10.2.2.2:5051', 'instance_weight': 2,
                        'ip': '10.2.2.2'}
        mock_sfr_sorted_slaves_1 = [mock_slave_1, mock_slave_2]
        mock_sfr_sorted_slaves_2 = [mock_slave_2]
        mock_filter_sfr_slaves.return_value = mock_sfr_sorted_slaves_1
        mock_get_mesos_task_count_by_slave.return_value = [mock_slave_2]
        mock_time.return_value = int(1)
        mock_start = (1 + 123) * 1000000000
        terminate_call_1 = mock.call(mock_sfr_sorted_slaves_1[0], 123, False, region='westeros-1')
        terminate_call_2 = mock.call(mock_sfr_sorted_slaves_1[1], 123, False, region='westeros-1')
        drain_call_1 = mock.call(['host1|10.1.1.1'], mock_start, 600 * 1000000000)
        drain_call_2 = mock.call(['host2|10.2.2.2'], mock_start, 600 * 1000000000)
        undrain_call_1 = mock.call(['host1|10.1.1.1'])
        undrain_call_2 = mock.call(['host2|10.2.2.2'])
        set_call_1 = mock.call('sfr-blah', 4, False, region='westeros-1')
        set_call_2 = mock.call('sfr-blah', 2, False, region='westeros-1')
        mock_sort_slaves_to_kill.side_effect = [mock_sfr_sorted_slaves_1, mock_sfr_sorted_slaves_2, []]
        autoscaling_lib.scale_aws_spot_fleet_request(mock_resource, 5, 2, mock_pool_settings, False)
        assert mock_get_mesos_state_summary_from_leader.called
        get_task_count_call_1 = mock.call(mock_mesos_state, pool='default')
        get_task_count_call_2 = mock.call(mock_mesos_state, slaves_list=[mock_slave_2])
        mock_get_mesos_task_count_by_slave.assert_has_calls([get_task_count_call_1, get_task_count_call_2])
        mock_filter_sfr_slaves.assert_called_with(mock_get_mesos_task_count_by_slave.return_value, mock_resource)
        mock_sort_slaves_to_kill.assert_called_with(mock_filter_sfr_slaves.return_value)
        mock_drain.assert_has_calls([drain_call_1, drain_call_2])
        mock_set_spot_fleet_request_capacity.assert_has_calls([set_call_1, set_call_2])
        mock_wait_and_terminate.assert_has_calls([terminate_call_1, terminate_call_2])
        mock_undrain.assert_has_calls([undrain_call_1, undrain_call_2])
        assert mock_get_mesos_task_count_by_slave.call_count == 3
        assert mock_sort_slaves_to_kill.call_count == 3

        # test scale down stop if it would take us below capacity
        mock_slave_1 = {'hostname': 'host1', 'instance_id': 'i-blah123',
                        'pid': 'slave(1)@10.1.1.1:5051', 'instance_weight': 1,
                        'ip': '10.1.1.1'}
        mock_slave_2 = {'hostname': 'host2', 'instance_id': 'i-blah456',
                        'pid': 'slave(1)@10.2.2.2:5051', 'instance_weight': 5,
                        'ip': '10.2.2.2'}
        mock_sfr_sorted_slaves_1 = [mock_slave_1, mock_slave_2]
        mock_filter_sfr_slaves.return_value = mock_sfr_sorted_slaves_1
        mock_sort_slaves_to_kill.side_effect = [mock_sfr_sorted_slaves_1, []]
        autoscaling_lib.scale_aws_spot_fleet_request(mock_resource, 5, 2, mock_pool_settings, False)
        mock_filter_sfr_slaves.assert_called_with(mock_get_mesos_task_count_by_slave.return_value, mock_resource)
        mock_drain.assert_has_calls([drain_call_1, drain_call_2, drain_call_1])
        mock_set_spot_fleet_request_capacity.assert_has_calls([set_call_1, set_call_2, set_call_1])
        mock_wait_and_terminate.assert_has_calls([terminate_call_1, terminate_call_2, terminate_call_1])

        # test we cleanup if a termination fails
        mock_wait_and_terminate.side_effect = ClientError({'Error': {}}, 'blah')
        mock_sfr_sorted_slaves = [{'hostname': 'host1', 'instance_id': 'i-blah123',
                                   'pid': 'slave(1)@10.1.1.1:5051', 'instance_weight': 1,
                                   'ip': '10.1.1.1'}]
        mock_filter_sfr_slaves.return_value = mock_sfr_sorted_slaves
        mock_sort_slaves_to_kill.side_effect = [mock_sfr_sorted_slaves, []]
        autoscaling_lib.scale_aws_spot_fleet_request(mock_resource, 5, 4, mock_pool_settings, False)
        set_call_3 = mock.call('sfr-blah', 5, False, region='westeros-1')
        mock_filter_sfr_slaves.assert_called_with(mock_get_mesos_task_count_by_slave.return_value, mock_resource)
        mock_drain.assert_has_calls([drain_call_1, drain_call_2, drain_call_1, drain_call_1])
        mock_set_spot_fleet_request_capacity.assert_has_calls([set_call_1, set_call_2, set_call_1,
                                                               set_call_1, set_call_3])
        mock_wait_and_terminate.assert_has_calls([terminate_call_1, terminate_call_2,
                                                  terminate_call_1, terminate_call_1])
        mock_undrain.assert_has_calls([undrain_call_1, undrain_call_2, undrain_call_1, undrain_call_1])


def test_autoscale_local_cluster():
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling_lib.load_system_paasta_config', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.get_cluster_metrics_provider', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.get_scaler', autospec=True),
    ) as (
        mock_get_paasta_config,
        mock_get_metrics_provider,
        mock_get_scaler,
    ):

        mock_scaling_resources = {'id1': {'id': 'sfr-blah', 'type': 'sfr', 'pool': 'default'}}
        mock_resource_pool_settings = {'default': {'drain_timeout': 123, 'target_utilization': 0.75}}
        mock_get_cluster_autoscaling_resources = mock.Mock(return_value=mock_scaling_resources)
        mock_get_resource_pool_settings = mock.Mock(return_value=mock_resource_pool_settings)
        mock_get_resources = mock.Mock(get_cluster_autoscaling_resources=mock_get_cluster_autoscaling_resources,
                                       get_resource_pool_settings=mock_get_resource_pool_settings)
        mock_get_paasta_config.return_value = mock_get_resources
        mock_metrics_provider = mock.Mock()
        mock_metrics_provider.return_value = (2, 6)
        mock_get_metrics_provider.return_value = mock_metrics_provider
        mock_scaler = mock.Mock()
        mock_get_scaler.return_value = mock_scaler

        # test scale up
        autoscaling_lib.autoscale_local_cluster()
        assert mock_get_paasta_config.called
        mock_get_metrics_provider.assert_called_with('sfr')
        mock_metrics_provider.assert_called_with('sfr-blah',
                                                 {'id': 'sfr-blah', 'type': 'sfr', 'pool': 'default'},
                                                 {'drain_timeout': 123, 'target_utilization': 0.75})
        mock_get_scaler.assert_called_with('sfr')
        mock_scaler.assert_called_with({'id': 'sfr-blah', 'type': 'sfr', 'pool': 'default'}, 2, 6,
                                       {'drain_timeout': 123, 'target_utilization': 0.75}, False)


def test_get_instances_from_ip():
    mock_instances = []
    ret = autoscaling_lib.get_instances_from_ip('10.1.1.1', mock_instances)
    assert ret == []

    mock_instances = [{'InstanceId': 'i-blah', 'PrivateIpAddress': '10.1.1.1'}]
    ret = autoscaling_lib.get_instances_from_ip('10.1.1.1', mock_instances)
    assert ret == mock_instances


def test_wait_and_terminate():
    with contextlib.nested(
        mock.patch('boto3.client', autospec=True),
        mock.patch('time.sleep', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.is_safe_to_kill'),
    ) as (
        mock_ec2_client,
        _,
        mock_is_safe_to_kill,
    ):
        mock_terminate_instances = mock.Mock()
        mock_ec2_client.return_value = mock.Mock(terminate_instances=mock_terminate_instances)

        mock_is_safe_to_kill.return_value = True
        mock_slave_to_kill = {'ip': '10.1.1.1', 'instance_id': 'i-blah123', 'pid': 'slave(1)@10.1.1.1:5051',
                              'hostname': 'hostblah'}
        autoscaling_lib.wait_and_terminate(mock_slave_to_kill, 600, False, region='westeros-1')
        mock_terminate_instances.assert_called_with(InstanceIds=['i-blah123'], DryRun=False)
        mock_is_safe_to_kill.assert_called_with('hostblah')

        mock_is_safe_to_kill.side_effect = [False, False, True]
        autoscaling_lib.wait_and_terminate(mock_slave_to_kill, 600, False, region='westeros-1')
        assert mock_is_safe_to_kill.call_count == 4


def test_sort_slaves_to_kill():
    # test no slaves
    ret = autoscaling_lib.sort_slaves_to_kill({})
    assert ret == []

    mock_slave_1 = mock.Mock()
    mock_slave_2 = mock.Mock()
    mock_slave_3 = mock.Mock()
    mock_slave_1 = {'task_counts': SlaveTaskCount(count=3, slave=mock_slave_1, chronos_count=0)}
    mock_slave_2 = {'task_counts': SlaveTaskCount(count=2, slave=mock_slave_2, chronos_count=1)}
    mock_slave_3 = {'task_counts': SlaveTaskCount(count=5, slave=mock_slave_3, chronos_count=0)}
    mock_task_count = [mock_slave_1, mock_slave_2, mock_slave_3]
    ret = autoscaling_lib.sort_slaves_to_kill(mock_task_count)
    assert ret == [mock_slave_1, mock_slave_3, mock_slave_2]


def test_get_spot_fleet_instances():
    with contextlib.nested(
        mock.patch('boto3.client', autospec=True),
    ) as (
        mock_ec2_client,
    ):
        mock_instances = mock.Mock()
        mock_sfr = {'ActiveInstances': mock_instances}
        mock_describe_spot_fleet_instances = mock.Mock(return_value=mock_sfr)
        mock_ec2_client.return_value = mock.Mock(describe_spot_fleet_instances=mock_describe_spot_fleet_instances)
        ret = autoscaling_lib.get_spot_fleet_instances('sfr-blah', region='westeros-1')
        assert ret == mock_instances


def test_get_sfr_instance_ips():
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling_lib.describe_instances', autospec=True),
    ) as (
        mock_describe_instances,
    ):
        mock_spot_fleet_instances = [{'InstanceId': 'i-blah1'}, {'InstanceId': 'i-blah2'}]
        mock_sfr = {'ActiveInstances': mock_spot_fleet_instances}
        mock_instances = [{'PrivateIpAddress': '10.1.1.1'}, {'PrivateIpAddress': '10.2.2.2'}]
        mock_describe_instances.return_value = mock_instances
        ret = autoscaling_lib.get_sfr_instance_ips(mock_sfr, region='westeros-1')
        mock_describe_instances.assert_called_with(['i-blah1', 'i-blah2'], region='westeros-1')
        assert ret == ['10.1.1.1', '10.2.2.2']


def test_get_sfr():
    with contextlib.nested(
        mock.patch('boto3.client', autospec=True),
    ) as (
        mock_ec2_client,
    ):
        mock_sfr_config = mock.Mock()
        mock_sfr = {'SpotFleetRequestConfigs': [mock_sfr_config]}
        mock_describe_spot_fleet_requests = mock.Mock(return_value=mock_sfr)
        mock_ec2_client.return_value = mock.Mock(describe_spot_fleet_requests=mock_describe_spot_fleet_requests)
        ret = autoscaling_lib.get_sfr('sfr-blah', region='westeros-1')
        mock_describe_spot_fleet_requests.assert_called_with(SpotFleetRequestIds=['sfr-blah'])
        assert ret == mock_sfr_config

        mock_error = {'Error': {'Code': 'InvalidSpotFleetRequestId.NotFound'}}
        mock_describe_spot_fleet_requests = mock.Mock(side_effect=ClientError(mock_error, 'blah'))
        mock_ec2_client.return_value = mock.Mock(describe_spot_fleet_requests=mock_describe_spot_fleet_requests)
        ret = autoscaling_lib.get_sfr('sfr-blah', region='westeros-1')
        assert ret is None


def test_filter_sfr_slaves():
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling_lib.get_sfr_instance_ips', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.slave_pid_to_ip'),
        mock.patch('paasta_tools.autoscaling_lib.get_instances_from_ip'),
        mock.patch('paasta_tools.autoscaling_lib.describe_instances', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.get_instance_type_weights', autospec=True),
    ) as (
        mock_get_sfr_instance_ips,
        mock_pid_to_ip,
        mock_get_instances_from_ip,
        mock_describe_instances,
        mock_get_instance_type_weights
    ):
        mock_sfr = mock.Mock()
        mock_resource = {'sfr': mock_sfr, 'region': 'westeros-1'}
        mock_get_sfr_instance_ips.return_value = ['10.1.1.1', '10.3.3.3']
        mock_pid_to_ip.side_effect = ['10.1.1.1', '10.2.2.2', '10.3.3.3',
                                      '10.1.1.1', '10.3.3.3', '10.1.1.1', '10.3.3.3']
        mock_get_instances_from_ip.side_effect = [[{'InstanceId': 'i-1'}], [{'InstanceId': 'i-3'}]]
        mock_instances = [{'InstanceId': 'i-1',
                           'InstanceType': 'c4.blah'},
                          {'InstanceId': 'i-2',
                           'InstanceType': 'm4.whatever'},
                          {'InstanceId': 'i-3',
                           'InstanceType': 'm4.whatever'}]
        mock_describe_instances.return_value = mock_instances
        mock_slave_1 = {'task_counts': SlaveTaskCount(slave={'pid': 'slave(1)@10.1.1.1:5051', 'id': '123',
                                                             'hostname': 'host123'},
                                                      count=0, chronos_count=0)}
        mock_slave_2 = {'task_counts': SlaveTaskCount(slave={'pid': 'slave(2)@10.2.2.2:5051', 'id': '456',
                                                             'hostname': 'host456'},
                                                      count=0, chronos_count=0)}
        mock_slave_3 = {'task_counts': SlaveTaskCount(slave={'pid': 'slave(3)@10.3.3.3:5051', 'id': '789',
                                                             'hostname': 'host789'},
                                                      count=0, chronos_count=0)}

        mock_sfr_sorted_slaves = [mock_slave_1, mock_slave_2, mock_slave_3]
        mock_get_instance_call_1 = mock.call('10.1.1.1', mock_instances)
        mock_get_instance_call_3 = mock.call('10.3.3.3', mock_instances)
        mock_get_ip_call_1 = mock.call('slave(1)@10.1.1.1:5051')
        mock_get_ip_call_2 = mock.call('slave(2)@10.2.2.2:5051')
        mock_get_ip_call_3 = mock.call('slave(3)@10.3.3.3:5051')
        mock_get_instance_type_weights.return_value = {'c4.blah': 2, 'm4.whatever': 5}
        ret = autoscaling_lib.filter_sfr_slaves(mock_sfr_sorted_slaves, mock_resource)
        mock_get_sfr_instance_ips.assert_called_with(mock_sfr, region='westeros-1')
        mock_pid_to_ip.assert_has_calls([mock_get_ip_call_1, mock_get_ip_call_2, mock_get_ip_call_3,
                                         mock_get_ip_call_1, mock_get_ip_call_3])
        mock_get_instances_from_ip.assert_has_calls([mock_get_instance_call_1, mock_get_instance_call_3])
        mock_describe_instances.assert_called_with([], region='westeros-1',
                                                   instance_filters=[{'Values': ['10.1.1.1', '10.3.3.3'],
                                                                      'Name': 'private-ip-address'}])
        mock_get_instance_type_weights.assert_called_with(mock_sfr)
        expected = [{'pid': 'slave(1)@10.1.1.1:5051',
                     'instance_id': 'i-1',
                     'id': '123',
                     'instance_type': 'c4.blah',
                     'task_counts': mock_slave_1['task_counts'],
                     'hostname': 'host123',
                     'ip': '10.1.1.1',
                     'instance_weight': 2},
                    {'pid': 'slave(3)@10.3.3.3:5051',
                     'instance_id': 'i-3',
                     'id': '789',
                     'instance_type': 'm4.whatever',
                     'task_counts': mock_slave_3['task_counts'],
                     'hostname': 'host789',
                     'ip': '10.3.3.3',
                     'instance_weight': 5}]
        assert ret == expected


def test_set_spot_fleet_request_capacity():
    with contextlib.nested(
        mock.patch('boto3.client', autospec=True),
        mock.patch('time.sleep', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.get_sfr', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.AWS_SPOT_MODIFY_TIMEOUT', autospec=True)
    ) as (
        mock_ec2_client,
        mock_sleep,
        mock_get_sfr,
        _
    ):
        mock_sleep.side_effect = TimeoutError()
        mock_get_sfr.return_value = {'SpotFleetRequestState': 'modifying'}
        mock_modify_spot_fleet_request = mock.Mock()
        mock_ec2_client.return_value = mock.Mock(modify_spot_fleet_request=mock_modify_spot_fleet_request)
        ret = autoscaling_lib.set_spot_fleet_request_capacity('sfr-blah', 4, False, region='westeros-1')
        assert not mock_modify_spot_fleet_request.called
        assert ret is False

        mock_get_sfr.return_value = {'SpotFleetRequestState': 'active'}
        ret = autoscaling_lib.set_spot_fleet_request_capacity('sfr-blah', 4, False, region='westeros-1')
        mock_modify_spot_fleet_request.assert_called_with(SpotFleetRequestId='sfr-blah',
                                                          TargetCapacity=4,
                                                          ExcessCapacityTerminationPolicy='noTermination')
        assert ret is not None


def test_get_instance_type_weights():
    mock_launch_specs = [{'InstanceType': 'c4.blah',
                          'WeightedCapacity': 123},
                         {'InstanceType': 'm4.whatever',
                          'WeightedCapacity': 456}]
    mock_sfr = {'SpotFleetRequestConfig': {'LaunchSpecifications': mock_launch_specs}}
    ret = autoscaling_lib.get_instance_type_weights(mock_sfr)
    assert ret == {'c4.blah': 123, 'm4.whatever': 456}


def test_describe_instance():
    with contextlib.nested(
        mock.patch('boto3.client', autospec=True),
    ) as (
        mock_ec2_client,
    ):
        mock_instance_1 = mock.Mock()
        mock_instance_2 = mock.Mock()
        mock_instances = {'Reservations': [{'Instances': [mock_instance_1]}, {'Instances': [mock_instance_2]}]}
        mock_describe_instances = mock.Mock(return_value=mock_instances)
        mock_ec2_client.return_value = mock.Mock(describe_instances=mock_describe_instances)
        ret = autoscaling_lib.describe_instances(['i-1', 'i-2'], region='westeros-1', instance_filters=['filter1'])
        mock_describe_instances.assert_called_with(InstanceIds=['i-1', 'i-2'], Filters=['filter1'])
        assert ret == [mock_instance_1, mock_instance_2]

        ret = autoscaling_lib.describe_instances(['i-1', 'i-2'], region='westeros-1')
        mock_describe_instances.assert_called_with(InstanceIds=['i-1', 'i-2'], Filters=[])

        mock_error = {'Error': {'Code': 'InvalidInstanceID.NotFound'}}
        mock_describe_instances.side_effect = ClientError(mock_error, 'blah')
        ret = autoscaling_lib.describe_instances(['i-1', 'i-2'], region='westeros-1')
        assert ret is None


def test_spotfleet_metrics_provider():
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling_lib.get_sfr', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.get_spot_fleet_instances', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.get_sfr_instance_ips', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.get_resource_utilization_by_grouping', autospec=True),
        mock.patch('paasta_tools.autoscaling_lib.slave_pid_to_ip'),
        mock.patch('paasta_tools.autoscaling_lib.get_spot_fleet_delta'),
        mock.patch('paasta_tools.autoscaling_lib.get_mesos_state_from_leader'),
    ) as (
        mock_get_sfr,
        mock_get_spot_fleet_instances,
        mock_get_sfr_instance_ips,
        mock_get_resource_utilization_by_grouping,
        mock_pid_to_ip,
        mock_get_spot_fleet_delta,
        mock_get_mesos_state_from_leader
    ):
        mock_resource = {'pool': 'default',
                         'region': 'westeros-1'}
        mock_mesos_state = {'slaves': [{'id': 'id1',
                                        'attributes': {'pool': 'default'},
                                        'pid': 'pid1'},
                                       {'id': 'id2',
                                        'attributes': {'pool': 'default'},
                                        'pid': 'pid2'}]}
        mock_get_mesos_state_from_leader.return_value = mock_mesos_state
        mock_utilization = {'free': ResourceInfo(cpus=5.0, mem=2048.0, disk=20.0),
                            'total': ResourceInfo(cpus=10.0, mem=4096.0, disk=40.0)}
        mock_get_resource_utilization_by_grouping.return_value = {'default': mock_utilization}
        mock_pid_to_ip.side_effect = ['10.1.1.1', '10.2.2.2']
        mock_get_spot_fleet_instances.return_value = [mock.Mock(), mock.Mock()]
        mock_get_sfr_instance_ips.return_value = ['10.1.1.1', '10.2.2.2']
        mock_get_spot_fleet_delta.return_value = 1, 2
        mock_pool_settings = {}

        mock_get_sfr.return_value = {'SpotFleetRequestState': 'cancelled'}
        ret = autoscaling_lib.spotfleet_metrics_provider('sfr-blah', mock_resource, mock_pool_settings)
        mock_get_sfr.assert_called_with('sfr-blah', region='westeros-1')
        assert not mock_get_spot_fleet_instances.called
        assert ret == (0, 0)

        mock_get_sfr.return_value = None
        ret = autoscaling_lib.spotfleet_metrics_provider('sfr-blah', mock_resource, mock_pool_settings)
        mock_get_sfr.assert_called_with('sfr-blah', region='westeros-1')
        assert not mock_get_spot_fleet_instances.called
        assert ret == (0, 0)

        mock_get_sfr.return_value = {'SpotFleetRequestState': 'active'}
        ret = autoscaling_lib.spotfleet_metrics_provider('sfr-blah', mock_resource, mock_pool_settings)
        mock_get_sfr.assert_called_with('sfr-blah', region='westeros-1')
        mock_get_spot_fleet_instances.assert_called_with('sfr-blah', region='westeros-1')
        expected_get_ip_call = mock_get_sfr.return_value.copy()
        expected_get_ip_call['ActiveInstances'] = mock_get_spot_fleet_instances.return_value
        mock_get_sfr_instance_ips.assert_called_with(expected_get_ip_call, region='westeros-1')
        mock_pid_to_ip.assert_has_calls([mock.call('pid1'), mock.call('pid2')])
        mock_get_resource_utilization_by_grouping.assert_called_with(mock.ANY, mock_mesos_state)
        mock_get_spot_fleet_delta.assert_called_with(mock_resource, float(0.5) - float(0.8))
        assert ret == (1, 2)
