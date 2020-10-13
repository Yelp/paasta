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
from decimal import Decimal

import arrow
import mock
import pytest
import staticconf

from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.autoscaler.config import AutoscalingConfig
from clusterman.config import POOL_NAMESPACE
from clusterman.exceptions import NoSignalConfiguredException
from clusterman.monitoring_lib import GaugeProtocol
from clusterman.util import ClustermanResources
from clusterman.util import SignalResourceRequest


@pytest.fixture
def run_timestamp():
    return arrow.get(300)


@pytest.fixture(autouse=True)
def mock_logger():
    with mock.patch('clusterman.autoscaler.autoscaler.logger') as mock_logger:
        yield mock_logger


@pytest.fixture(autouse=True)
def pool_configs():
    with staticconf.testing.PatchConfiguration(
        {
            'scaling_limits': {
                'min_capacity': 24,
                'max_capacity': 5000,
                'max_weight_to_add': 200,
                'max_weight_to_remove': 10,
            },
        },
        namespace=POOL_NAMESPACE.format(pool='bar', scheduler='mesos'),
    ):
        yield


@pytest.fixture
def mock_autoscaler():
    autoscaling_config_dict = {
        'default_signal_role': 'clusterman',
        'setpoint': 0.7,
        'target_capacity_margin': 0.1,
    }

    with mock.patch(
        'clusterman.autoscaler.autoscaler.ClustermanMetricsBotoClient',
        autospec=True,
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.PoolManager',
        autospec=True,
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.Autoscaler._get_signal_for_app',
        autospec=True,
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.get_monitoring_client',
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.ExternalSignal',
    ), mock.patch(
        'clusterman.autoscaler.autoscaler.PendingPodsSignal',
    ), staticconf.testing.PatchConfiguration(
        {'autoscaling': autoscaling_config_dict},
    ):
        mock_autoscaler = Autoscaler('mesos-test', 'bar', 'mesos', ['bar'], monitoring_enabled=False)
        mock_autoscaler.pool_manager.cluster_connector = mock.Mock()

    mock_autoscaler.pool_manager.target_capacity = 300
    mock_autoscaler.pool_manager.min_capacity = staticconf.read_int(
        'scaling_limits.min_capacity', namespace=POOL_NAMESPACE.format(pool='bar', scheduler='mesos')
    )
    mock_autoscaler.pool_manager.max_capacity = staticconf.read_int(
        'scaling_limits.max_capacity', namespace=POOL_NAMESPACE.format(pool='bar', scheduler='mesos')
    )
    mock_autoscaler.pool_manager.non_orphan_fulfilled_capacity = 0

    mock_autoscaler.target_capacity_gauge = mock.Mock(spec=GaugeProtocol)
    mock_autoscaler.non_orphan_capacity_gauge = mock.Mock(spec=GaugeProtocol)
    mock_autoscaler.resource_request_gauges = {
        'mem': mock.Mock(spec=GaugeProtocol),
        'cpus': mock.Mock(spec=GaugeProtocol),
        'disk': mock.Mock(spec=GaugeProtocol),
        'gpus': mock.Mock(spec=GaugeProtocol),
    }
    return mock_autoscaler


def test_autoscaler_init_too_many_apps():
    with pytest.raises(NotImplementedError):
        Autoscaler('mesos-test', 'bar', 'mesos', ['app1', 'app2'], monitoring_enabled=False)


@mock.patch('clusterman.autoscaler.autoscaler.ExternalSignal')
@pytest.mark.parametrize('monitoring_enabled', [True, False])
def test_monitoring_enabled(mock_signal, mock_autoscaler, monitoring_enabled):
    mock_autoscaler.monitoring_enabled = monitoring_enabled
    mock_signal.side_effect = Exception('foo')

    with mock.patch('clusterman.util._get_sensu') as mock_get_sensu:
        mock_autoscaler._get_signal_for_app('bar')
        assert mock_get_sensu.return_value.send_event.call_count == (1 if monitoring_enabled else 0)


@pytest.mark.parametrize('signal_response', [
    NoSignalConfiguredException,  # no app signal
    ValueError,  # app signal failed to load
    mock.Mock()  # Custom app signal successful
])
def test_get_signal_for_app(mock_autoscaler, signal_response):
    with mock.patch('clusterman.autoscaler.autoscaler.ExternalSignal') as mock_signal, \
            mock.patch('clusterman.autoscaler.autoscaler.sensu_checkin') as mock_sensu:
        mock_signal.side_effect = signal_response
        signal = mock_autoscaler._get_signal_for_app('bar')
        assert mock_sensu.call_count == (signal_response == ValueError)

    assert signal == (mock_autoscaler.default_signal if isinstance(signal_response, Exception) else signal)


@pytest.mark.parametrize('dry_run', [True, False])
def test_autoscaler_run(dry_run, mock_autoscaler, run_timestamp):
    mock_autoscaler._compute_target_capacity = mock.Mock(return_value=100)
    mock_autoscaler.signal.evaluate.side_effect = ValueError
    resource_request = SignalResourceRequest(cpus=100000)
    mock_autoscaler.default_signal.evaluate.return_value = resource_request
    with mock.patch(
        'clusterman.autoscaler.autoscaler.autoscaling_is_paused',
        return_value=False,
    ), pytest.raises(ValueError):
        mock_autoscaler.run(dry_run=dry_run, timestamp=run_timestamp)

    assert mock_autoscaler.target_capacity_gauge.set.call_args == mock.call(100, {'dry_run': dry_run})
    assert mock_autoscaler._compute_target_capacity.call_args == mock.call(resource_request)
    assert mock_autoscaler.pool_manager.modify_target_capacity.call_count == 1

    assert mock_autoscaler.resource_request_gauges['cpus'].set.call_args == mock.call(
        resource_request.cpus,
        {'dry_run': dry_run},
    )
    assert mock_autoscaler.resource_request_gauges['mem'].set.call_count == 0
    assert mock_autoscaler.resource_request_gauges['disk'].set.call_count == 0


def test_autoscaler_run_paused(mock_autoscaler, run_timestamp):
    mock_autoscaler._compute_target_capacity = mock.Mock(return_value=100)
    mock_autoscaler._is_paused = mock.Mock(return_value=True)

    with mock.patch(
        'clusterman.autoscaler.autoscaler.autoscaling_is_paused',
        return_value=True,
    ):
        mock_autoscaler.run(timestamp=run_timestamp)

    assert mock_autoscaler.signal.evaluate.call_count == 0
    assert mock_autoscaler.target_capacity_gauge.set.call_count == 0
    assert mock_autoscaler._compute_target_capacity.call_count == 0
    assert mock_autoscaler.pool_manager.modify_target_capacity.call_count == 0

    assert mock_autoscaler.resource_request_gauges['cpus'].set.call_count == 0
    assert mock_autoscaler.resource_request_gauges['mem'].set.call_count == 0
    assert mock_autoscaler.resource_request_gauges['disk'].set.call_count == 0


class TestComputeTargetCapacity:

    @pytest.mark.parametrize('resource', ['cpus', 'mem', 'disk', 'gpus'])
    @pytest.mark.parametrize('signal_resource,total_resource,expected_capacity', [
        (None, 1000, 125),
        (767, 1000, 125),  # above setpoint, but within setpoint margin
        (980, 1000, 175),  # above setpoint margin
        (633, 1000, 125),  # below setpoint, but within setpoint margin
        (490, 1000, 87.5),  # below setpoint margin
        (1400, 1000, 250),  # above setpoint margin and total
    ])
    def test_single_resource(self, mock_autoscaler, resource, signal_resource, total_resource, expected_capacity):
        mock_autoscaler.pool_manager.target_capacity = 125
        mock_autoscaler.pool_manager.non_orphan_fulfilled_capacity = 125
        mock_autoscaler.pool_manager.cluster_connector.get_cluster_total_resources.return_value = ClustermanResources(
            cpus=total_resource,
            mem=total_resource,
            disk=total_resource,
            gpus=total_resource,
        )
        new_target_capacity = mock_autoscaler._compute_target_capacity(SignalResourceRequest(
            **{resource: signal_resource},
        ))
        assert new_target_capacity == pytest.approx(expected_capacity)

    def test_empty_request(self, mock_autoscaler):
        new_target_capacity = mock_autoscaler._compute_target_capacity({})
        assert new_target_capacity == mock_autoscaler.pool_manager.target_capacity

    @pytest.mark.parametrize('target_capacity', [0, 125])
    def test_request_zero_resources(self, target_capacity, mock_autoscaler):
        mock_autoscaler.pool_manager.cluster_connector.get_resource_total.return_value = 10
        mock_autoscaler.pool_manager.target_capacity = target_capacity
        mock_autoscaler.pool_manager.non_orphan_fulfilled_capacity = target_capacity

        new_target_capacity = mock_autoscaler._compute_target_capacity(
            SignalResourceRequest(cpus=None, mem=None, disk=0, gpus=0)
        )
        assert new_target_capacity == 0

    def test_current_target_capacity_with_historical_data(self, mock_autoscaler):
        mock_autoscaler.pool_manager.cluster_connector.get_resource_total.return_value = 0
        mock_autoscaler.pool_manager.target_capacity = 0
        mock_autoscaler.pool_manager.non_orphan_fulfilled_capacity = 0
        mock_autoscaler._get_historical_weighted_resource_value = mock.Mock(return_value=ClustermanResources(
            cpus=2, mem=26, disk=0, gpus=0
        ))

        new_target_capacity = mock_autoscaler._compute_target_capacity(
            SignalResourceRequest(cpus=7, mem=400, disk=70, gpus=0),
        )
        assert new_target_capacity == pytest.approx(400 / 26 / 0.7)

    def test_current_target_capacity_no_historical_data(self, mock_autoscaler):
        mock_autoscaler.pool_manager.cluster_connector.get_resource_total.return_value = 0
        mock_autoscaler.pool_manager.target_capacity = 0
        mock_autoscaler.pool_manager.non_orphan_fulfilled_capacity = 0
        mock_autoscaler._get_historical_weighted_resource_value = mock.Mock(return_value=ClustermanResources())

        new_target_capacity = mock_autoscaler._compute_target_capacity(
            SignalResourceRequest(cpus=7, mem=400, disk=70, gpus=0),
        )
        assert new_target_capacity == 1

    def test_non_orphan_fulfilled_capacity_0(self, mock_autoscaler):
        mock_autoscaler.pool_manager.cluster_connector.get_resource_total.return_value = 0
        mock_autoscaler.pool_manager.target_capacity = 1
        mock_autoscaler.pool_manager.non_orphan_fulfilled_capacity = 0

        new_target_capacity = mock_autoscaler._compute_target_capacity(
            SignalResourceRequest(cpus=10, mem=500, disk=1000, gpus=0),
        )
        assert new_target_capacity == mock_autoscaler.pool_manager.target_capacity

    def test_scale_most_constrained_resource(self, mock_autoscaler):
        resource_request = SignalResourceRequest(cpus=500, mem=30000, disk=19000, gpus=0)
        resource_totals = ClustermanResources(cpus=1000, mem=50000, disk=20000, gpus=0)
        mock_autoscaler.pool_manager.non_orphan_fulfilled_capacity = 100
        mock_autoscaler.pool_manager.cluster_connector.get_cluster_total_resources.return_value = resource_totals
        new_target_capacity = mock_autoscaler._compute_target_capacity(resource_request)

        # disk would be the most constrained resource, so we should scale the target_capacity (100) by an amount
        # such that requested/(total*scale_factor) = setpoint
        expected_new_target_capacity = 100 * 19000 / (20000 * 0.7)
        assert new_target_capacity == pytest.approx(expected_new_target_capacity)

    def test_excluded_resources(self, mock_autoscaler):
        resource_request = SignalResourceRequest(cpus=500, mem=30000, disk=19000, gpus=0)
        resource_totals = ClustermanResources(cpus=1000, mem=50000, disk=20000, gpus=0)
        mock_autoscaler.autoscaling_config = AutoscalingConfig(['disk'], 0.7, 0.1)
        mock_autoscaler.pool_manager.non_orphan_fulfilled_capacity = 100
        mock_autoscaler.pool_manager.cluster_connector.get_cluster_total_resources.return_value = resource_totals
        new_target_capacity = mock_autoscaler._compute_target_capacity(resource_request)

        # disk would be the most constrained resource, but it's excluded, so we scale on the next most constrained (mem)
        expected_new_target_capacity = 100 * 30000 / (50000 * 0.7)
        assert new_target_capacity == pytest.approx(expected_new_target_capacity)

    def test_request_mix_of_zeroes_and_nones(self, mock_autoscaler):
        resource_request = SignalResourceRequest(cpus=0, mem=None, disk=None, gpus=None)
        resource_totals = {'cpus': 1000, 'mem': 50000, 'disk': 20000, 'gpus': 0}
        mock_autoscaler.pool_manager.non_orphan_fulfilled_capacity = 100
        mock_autoscaler.pool_manager.cluster_connector.get_resource_total.side_effect = resource_totals.__getitem__
        new_target_capacity = mock_autoscaler._compute_target_capacity(resource_request)

        assert new_target_capacity == 0


def test_get_historical_weighted_resource_value_no_historical_data(mock_autoscaler):
    mock_autoscaler._get_smoothed_non_zero_metadata = mock.Mock(return_value=None)
    assert mock_autoscaler._get_historical_weighted_resource_value() == ClustermanResources()


def test_get_historical_weighted_resource_value(mock_autoscaler):
    mock_autoscaler._get_smoothed_non_zero_metadata = mock.Mock(side_effect=[
        (100, 200, 78),   # historical non_zero_fulfilled_capacity
        (100, 200, 20),   # cpus
        None,             # mem
        (100, 200, 0.1),  # disk
        (100, 200, 1),    # gpus
    ])
    assert mock_autoscaler._get_historical_weighted_resource_value() == ClustermanResources(
        cpus=20 / 78,
        mem=0,
        disk=0.1 / 78,
        gpus=1 / 78,
    )


def test_get_smoothed_non_zero_metadata(mock_autoscaler):
    mock_autoscaler.metrics_client.get_metric_values.return_value = {
        'some_metric': [(100, 5), (110, 7), (120, 40), (130, 23), (136, 0), (140, 41), (150, 0), (160, 0), (170, 0)],
    }
    assert mock_autoscaler._get_smoothed_non_zero_metadata('some_metric', 0, 200, smoothing=3) == (
        120, 140, (40 + 23 + 41) / 3,
    )


def test_get_smoothed_non_zero_metadata_no_data(mock_autoscaler):
    mock_autoscaler.metrics_client.get_metric_values.return_value = {'some_metric': []}
    assert mock_autoscaler._get_smoothed_non_zero_metadata('some_metric', 0, 200, smoothing=3) is None


def test_get_smoothed_non_zero_metadata_all_zero(mock_autoscaler):
    mock_autoscaler.metrics_client.get_metric_values.return_value = {
        'some_metric': [(Decimal('150'), Decimal('0')), (Decimal('160'), Decimal('0')), (Decimal('170'), Decimal('0'))],
    }
    assert mock_autoscaler._get_smoothed_non_zero_metadata('some_metric', 0, 200, smoothing=3) is None
