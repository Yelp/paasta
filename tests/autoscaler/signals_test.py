import math
import os

import arrow
import mock
import pytest
import simplejson as json
import staticconf.testing
from clusterman_metrics import APP_METRICS
from clusterman_metrics import METADATA
from clusterman_metrics import SYSTEM_METRICS

from clusterman.autoscaler.signals import ACK
from clusterman.autoscaler.signals import setup_signals_environment
from clusterman.autoscaler.signals import Signal
from clusterman.exceptions import ClustermanSignalError
from clusterman.exceptions import MetricsError
from clusterman.exceptions import NoSignalConfiguredException
from clusterman.exceptions import SignalConnectionError


@pytest.fixture
def mock_signal():
    with mock.patch('clusterman.autoscaler.signals.Signal._connect_to_signal_process'):
        return Signal('foo', 'bar', 'mesos', 'app1', 'bar.mesos_config', mock.Mock(), 'the_signal')


def test_init(mock_signal):
    assert 'cluster' in mock_signal.parameters
    assert mock_signal.parameters['cluster'] == 'foo'
    assert 'pool' in mock_signal.parameters
    assert mock_signal.parameters['pool'] == 'bar'


def test_no_signal_configured():
    with staticconf.testing.MockConfiguration(
        {},
        namespace='bar.mesos_config',
    ), mock.patch(
        'clusterman.autoscaler.signals.Signal._connect_to_signal_process',
    ), pytest.raises(NoSignalConfiguredException):
        return Signal('foo', 'bar', 'mesos', 'app1', 'bar.mesos_config', mock.Mock(), 'the_signal')


@pytest.mark.parametrize('conn_response', [['foo'], [ACK, 'foo']])
def test_evaluate_signal_connection_errors(mock_signal, conn_response):
    mock_signal._get_metrics = mock.Mock(return_value={})
    mock_signal._signal_conn.recv.side_effect = conn_response
    with pytest.raises(SignalConnectionError):
        mock_signal.evaluate(arrow.get(12345678))
    assert mock_signal._signal_conn.send.call_count == len(conn_response)
    assert mock_signal._signal_conn.recv.call_count == len(conn_response)


def test_evaluate_broken_signal(mock_signal):
    mock_signal._get_metrics = mock.Mock(return_value={})
    mock_signal._signal_conn.recv.side_effect = [ACK, ACK, 'error']
    with pytest.raises(ClustermanSignalError):
        mock_signal.evaluate(arrow.get(12345678))


def test_evaluate_restart_dead_signal(mock_signal):
    mock_signal._get_metrics = mock.Mock(return_value={})
    mock_signal._signal_conn.recv.side_effect = [BrokenPipeError, ACK, ACK, '{"Resources": {"cpus": 1}}']
    with mock.patch('clusterman.autoscaler.signals.Signal._connect_to_signal_process') as mock_connect:
        mock_connect.return_value = mock_signal._signal_conn
        assert mock_signal.evaluate(arrow.get(12345678)) == {'cpus': 1}
        assert mock_connect.call_count == 1


@pytest.mark.parametrize('error', [BrokenPipeError, 'error'])
def test_evaluate_restart_dead_signal_fails(mock_signal, error):
    mock_signal._get_metrics = mock.Mock(return_value={})
    mock_signal._signal_conn.recv.side_effect = [BrokenPipeError, ACK, ACK, error]
    with mock.patch('clusterman.autoscaler.signals.Signal._connect_to_signal_process') as mock_connect, \
            pytest.raises(ClustermanSignalError):
        mock_connect.return_value = mock_signal._signal_conn
        mock_signal.evaluate(arrow.get(12345678))
        assert mock_connect.call_count == 1


@mock.patch('clusterman.autoscaler.signals.SOCKET_MESG_SIZE', 2)
@pytest.mark.parametrize('signal_recv', [
    [ACK, ACK, b'{"Resources": {"cpus": 5.2}}'],
    [ACK, b'\x01{"Resources": {"cpus": 5.2}}'],
])
def test_evaluate_signal_sending_message(mock_signal, signal_recv):
    metrics = {'cpus_allocated': [(1234, 3.5), (1235, 6)]}
    mock_signal._get_metrics = mock.Mock(return_value=metrics)
    num_messages = math.ceil(len(json.dumps({'metrics': metrics, 'timestamp': 12345678})) / 2) + 1
    mock_signal._signal_conn = mock.Mock()
    mock_signal._signal_conn.recv.side_effect = signal_recv
    resp = mock_signal.evaluate(arrow.get(12345678))
    assert mock_signal._signal_conn.send.call_count == num_messages
    assert mock_signal._signal_conn.recv.call_count == len(signal_recv)
    assert resp == {'cpus': 5.2}


@pytest.mark.parametrize('end_time', [arrow.get(3600), arrow.get(10000), arrow.get(35000)])
def test_get_metrics(mock_signal, end_time):
    mock_signal.metrics_client.get_metric_values.side_effect = [
        {'cpus_allocated': [(1, 2), (3, 4)]},
        {'cpus_allocated': [(5, 6), (7, 8)]},
        {'app1,cost': [(1, 2.5), (3, 4.5)]},
    ]
    metrics = mock_signal._get_metrics(end_time)
    assert mock_signal.metrics_client.get_metric_values.call_args_list == [
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


def test_get_metadata_metrics(mock_signal):
    with pytest.raises(MetricsError):
        mock_signal.required_metrics = [{'name': 'total_cpus', 'type': METADATA, 'minute_range': 10}]
        mock_signal._get_metrics(arrow.get(0))


def test_setup_signals_namespace():
    fetch_num, signal_num = setup_signals_environment('bar', 'mesos')
    assert sorted(os.environ['CMAN_VERSIONS_TO_FETCH'].split(' ')) == ['master', 'v42']
    assert sorted(os.environ['CMAN_SIGNAL_VERSIONS'].split(' ')) == ['master', 'v42']
    assert sorted(os.environ['CMAN_SIGNAL_NAMESPACES'].split(' ')) == ['bar', 'foo']
    assert sorted(os.environ['CMAN_SIGNAL_NAMES'].split(' ')) == ['BarSignal3', 'DefaultSignal']
    assert sorted(os.environ['CMAN_SIGNAL_APPS'].split(' ')) == ['__default__', 'bar']
    assert os.environ['CMAN_NUM_VERSIONS'] == '2'
    assert os.environ['CMAN_NUM_SIGNALS'] == '2'
    assert os.environ['CMAN_SIGNALS_BUCKET'] == 'the_bucket'
    assert (fetch_num, signal_num) == (2, 2)
