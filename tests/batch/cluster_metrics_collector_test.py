# Copyright 2019 Yelp Inc.
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
import argparse
import socket

import mock
import pytest
from clusterman_metrics import ClustermanMetricsBotoClient

from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.mesos.metrics_generators import ClusterMetric
from clusterman.util import All
from clusterman.util import splay_event_time

try:
    from clusterman.batch.cluster_metrics_collector import ClusterMetricsCollector
    from clusterman.batch.cluster_metrics_collector import METRICS_TO_WRITE
except ImportError:
    pytest.mark.skip('Could not import the batch; are you in a Yelp-y environment?')


@pytest.fixture
def batch(args=None):
    batch = ClusterMetricsCollector()
    args = args or ['--cluster', 'mesos-test']
    parser = argparse.ArgumentParser()
    batch.parse_args(parser)
    batch.options = parser.parse_args(args)
    batch.options.instance_name = 'foo'
    batch.version_checker = mock.Mock(watchers=[])
    return batch


@pytest.fixture
def mock_setup_config():
    with mock.patch('clusterman.batch.cluster_metrics_collector.setup_config', autospec=True) as mock_setup:
        yield mock_setup


@mock.patch('clusterman.batch.cluster_metrics_collector.ClustermanMetricsBotoClient', autospec=True)
@mock.patch('clusterman.batch.cluster_metrics_collector.PoolManager', autospec=True)
@mock.patch('os.listdir')
def test_configure_initial(mock_ls, mock_mesos_pool_manager, mock_client_class, batch, mock_setup_config):
    pools = ['pool-1', 'pool-3', 'pool-4']
    mock_ls.return_value = [f'{p}.mesos' for p in pools[:2]] + [f'{p}.kubernetes' for p in pools[2:]]
    with mock.patch('clusterman.batch.cluster_metrics_collector.load_cluster_pool_config') as mock_pool_config:
        batch.configure_initial()
        assert mock_pool_config.call_count == 3

    assert batch.run_interval == 120
    assert mock_setup_config.call_count == 1
    assert batch.region == 'us-west-2'  # region from cluster configs

    assert mock_client_class.call_args_list == [mock.call(region_name='us-west-2')]
    assert batch.metrics_client == mock_client_class.return_value

    assert len(batch.pools['mesos']) == 2
    assert len(batch.pools['kubernetes']) == 1


def test_write_metrics(batch):
    batch.pool_managers = {
        'pool_A.mesos': mock.Mock(autospec=PoolManager, pool='pool_A', scheduler='mesos'),
        'pool_B.mesos': mock.Mock(autospec=PoolManager, pool='pool_B', scheduler='mesos'),
        'pool_B.kubernetes': mock.Mock(autospec=PoolManager, pool='pool_B', scheduler='kubernetes'),
    }
    batch.pool_managers['pool_A.mesos'].cluster_connector = mock.Mock()
    batch.pool_managers['pool_B.mesos'].cluster_connector = mock.Mock()
    batch.pool_managers['pool_B.kubernetes'].cluster_connector = mock.Mock()
    writer = mock.Mock()

    def metric_generator(manager):
        yield ClusterMetric(
            'allocated',
            manager.cluster_connector.get_resource_allocation('cpus'),
            {'pool': f'{manager.pool}.{manager.scheduler}'},
        )

    batch.write_metrics(writer, metric_generator, pools=All, schedulers=['mesos', 'kubernetes'])

    for pool, manager in batch.pool_managers.items():
        assert manager.cluster_connector.get_resource_allocation.call_args_list == [mock.call('cpus')]

    assert writer.send.call_count == 3

    metric_names = [call[0][0][0] for call in writer.send.call_args_list]
    assert sorted(metric_names) == sorted([
        'allocated|pool=pool_A.mesos',
        'allocated|pool=pool_B.mesos',
        'allocated|pool=pool_B.kubernetes',
    ])


@mock.patch('time.sleep')
@mock.patch('time.time')
@mock.patch(
    'clusterman.batch.cluster_metrics_collector.ClusterMetricsCollector.running',
    new_callable=mock.PropertyMock,
)
@mock.patch('clusterman.batch.cluster_metrics_collector.sensu_checkin', autospec=True)
def test_run(mock_sensu, mock_running, mock_time, mock_sleep, batch):
    mock_running.side_effect = [True, True, True, True, False]
    mock_time.side_effect = [101, 113, 148, 188]
    batch.run_interval = 10
    batch.metrics_client = mock.MagicMock(spec_set=ClustermanMetricsBotoClient)
    batch.pools = {'mesos': ['pool-1', 'pool-2']}

    writer_context = batch.metrics_client.get_writer.return_value
    writer = writer_context.__enter__.return_value

    # modify splay_event_time to avoid any splaying
    def mock_splay_event_time(frequency, key):
        fake_key = mock.Mock(__hash__=lambda x: 0)
        return splay_event_time(frequency, fake_key)

    with mock.patch('clusterman.batch.cluster_metrics_collector.splay_event_time', mock_splay_event_time), \
            mock.patch.object(batch, 'write_metrics', autospec=True) as write_metrics, \
            mock.patch.object(batch, 'reload_watchers', autospec=True) as reload_watchers, \
            mock.patch('clusterman.batch.cluster_metrics_collector.PoolManager', autospec=True), \
            mock.patch('clusterman.batch.cluster_metrics_collector.logger') as mock_logger, \
            mock.patch('clusterman.batch.cluster_metrics_collector.ClusterMetricsCollector.'
                       'initialize_clusterman_metrics_client') as mock_initialize_clusterman_metrics_client:
        def mock_write_metrics(writer, generator, pools, schedulers):
            if mock_time.call_count == 4:
                raise socket.timeout('timed out')
            else:
                return

        reload_watchers.return_value = True
        mock_initialize_clusterman_metrics_client.return_value = None
        write_metrics.side_effect = mock_write_metrics
        batch.run()
        mock_initialize_clusterman_metrics_client.assert_called_with()

        # Writing should have happened 3 times, for each metric type.
        # Each time, we create a new writer context and call write_metrics.
        assert sorted(batch.metrics_client.get_writer.call_args_list) == sorted(
            [
                mock.call(metric_to_write.type, metric_to_write.aggregate_meteorite_dims)
                for metric_to_write in METRICS_TO_WRITE
            ] * 4
        )

        expected_write_metrics_calls = [
            mock.call(writer, metric_to_write.generator, metric_to_write.pools, metric_to_write.schedulers)
            for metric_to_write in METRICS_TO_WRITE] * 4
        assert write_metrics.call_args_list == expected_write_metrics_calls

        assert writer_context.__exit__.call_count == len(METRICS_TO_WRITE) * 4
        assert mock_sensu.call_count == 3
        assert mock_logger.warn.call_count == 4

    assert mock_sleep.call_args_list == [mock.call(9), mock.call(7), mock.call(2), mock.call(2)]
