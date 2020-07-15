import arrow
import mock
import pytest
import staticconf
from clusterman_metrics import APP_METRICS
from clusterman_metrics import METADATA
from clusterman_metrics import SYSTEM_METRICS

from clusterman.exceptions import MetricsError
from clusterman.interfaces.signal import get_metrics_for_signal


@pytest.mark.parametrize('end_time', [arrow.get(3600), arrow.get(10000), arrow.get(35000)])
def test_get_metrics(end_time):

    required_metrics = staticconf.read_list(
        'autoscale_signal.required_metrics',
        namespace='bar.mesos_config',
    )
    metrics_client = mock.Mock()
    metrics_client.get_metric_values.side_effect = [
        {'cpus_allocated': [(1, 2), (3, 4)]},
        {'cpus_allocated': [(5, 6), (7, 8)]},
        {'app1,cost': [(1, 2.5), (3, 4.5)]},
    ]
    metrics = get_metrics_for_signal('foo', 'bar', 'mesos', 'app1', metrics_client, required_metrics, end_time)
    assert metrics_client.get_metric_values.call_args_list == [
        mock.call(
            'cpus_allocated',
            SYSTEM_METRICS,
            end_time.shift(minutes=-10).timestamp,
            end_time.timestamp,
            app_identifier='app1',
            extra_dimensions={'cluster': 'foo', 'pool': 'bar'},
            is_regex=False,
        ),
        mock.call(
            'cpus_allocated',
            SYSTEM_METRICS,
            end_time.shift(minutes=-10).timestamp,
            end_time.timestamp,
            app_identifier='app1',
            extra_dimensions={'cluster': 'foo', 'pool': 'bar.mesos'},
            is_regex=False,
        ),
        mock.call(
            'cost',
            APP_METRICS,
            end_time.shift(minutes=-30).timestamp,
            end_time.timestamp,
            app_identifier='app1',
            extra_dimensions={},
            is_regex=False,
        ),
    ]
    assert 'cpus_allocated' in metrics
    assert 'app1,cost' in metrics


def test_get_metadata_metrics():
    with pytest.raises(MetricsError):
        required_metrics = [{'name': 'total_cpus', 'type': METADATA, 'minute_range': 10}]
        get_metrics_for_signal('foo', 'bar', 'mesos', 'app1', mock.Mock(), required_metrics, arrow.get(0))
