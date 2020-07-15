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
import math
import os

import arrow
import mock
import pytest
import simplejson as json
import staticconf

from clusterman.exceptions import ClustermanSignalError
from clusterman.exceptions import NoSignalConfiguredException
from clusterman.exceptions import SignalConnectionError
from clusterman.signals.external_signal import ACK
from clusterman.signals.external_signal import ExternalSignal
from clusterman.signals.external_signal import setup_signals_environment


@pytest.fixture
def mock_signal():
    with mock.patch('clusterman.signals.external_signal.ExternalSignal._connect_to_signal_process'):
        return ExternalSignal('foo', 'bar', 'mesos', 'app1', 'bar.mesos_config', mock.Mock(), 'the_signal')


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
        'clusterman.signals.external_signal.ExternalSignal._connect_to_signal_process',
    ), pytest.raises(NoSignalConfiguredException):
        return ExternalSignal('foo', 'bar', 'mesos', 'app1', 'bar.mesos_config', mock.Mock(), 'the_signal')


@pytest.mark.parametrize('conn_response', [['foo'], [ACK, 'foo']])
def test_evaluate_signal_connection_errors(mock_signal, conn_response):
    mock_signal._signal_conn.recv.side_effect = conn_response
    with mock.patch(
        'clusterman.signals.external_signal.get_metrics_for_signal', return_value={}
    ), pytest.raises(SignalConnectionError):
        mock_signal.evaluate(arrow.get(12345678))
    assert mock_signal._signal_conn.send.call_count == len(conn_response)
    assert mock_signal._signal_conn.recv.call_count == len(conn_response)


def test_evaluate_broken_signal(mock_signal):
    mock_signal._signal_conn.recv.side_effect = [ACK, ACK, 'error']
    with mock.patch(
        'clusterman.signals.external_signal.get_metrics_for_signal', return_value={}
    ), pytest.raises(ClustermanSignalError):
        mock_signal.evaluate(arrow.get(12345678))


def test_evaluate_restart_dead_signal(mock_signal):
    mock_signal._signal_conn.recv.side_effect = [BrokenPipeError, ACK, ACK, '{"Resources": {"cpus": 1}}']
    with mock.patch(
        'clusterman.signals.external_signal.ExternalSignal._connect_to_signal_process'
    ) as mock_connect, mock.patch(
        'clusterman.signals.external_signal.get_metrics_for_signal', return_value={}
    ):
        mock_connect.return_value = mock_signal._signal_conn
        assert mock_signal.evaluate(arrow.get(12345678)) == {'cpus': 1}
        assert mock_connect.call_count == 1


@pytest.mark.parametrize('error', [BrokenPipeError, 'error'])
def test_evaluate_restart_dead_signal_fails(mock_signal, error):
    mock_signal._get_metrics = mock.Mock(return_value={})
    mock_signal._signal_conn.recv.side_effect = [BrokenPipeError, ACK, ACK, error]
    with mock.patch(
        'clusterman.signals.external_signal.ExternalSignal._connect_to_signal_process'
    ) as mock_connect, mock.patch(
        'clusterman.signals.external_signal.get_metrics_for_signal', return_value={}
    ), pytest.raises(ClustermanSignalError):
        mock_connect.return_value = mock_signal._signal_conn
        mock_signal.evaluate(arrow.get(12345678))
        assert mock_connect.call_count == 1


@mock.patch('clusterman.signals.external_signal.SOCKET_MESG_SIZE', 2)
@pytest.mark.parametrize('signal_recv', [
    [ACK, ACK, b'{"Resources": {"cpus": 5.2}}'],
    [ACK, b'\x01{"Resources": {"cpus": 5.2}}'],
])
def test_evaluate_signal_sending_message(mock_signal, signal_recv):
    metrics = {'cpus_allocated': [(1234, 3.5), (1235, 6)]}
    num_messages = math.ceil(len(json.dumps({'metrics': metrics, 'timestamp': 12345678})) / 2) + 1
    mock_signal._signal_conn = mock.Mock()
    mock_signal._signal_conn.recv.side_effect = signal_recv
    with mock.patch(
        'clusterman.signals.external_signal.get_metrics_for_signal',
        return_value=metrics,
    ):
        resp = mock_signal.evaluate(arrow.get(12345678))
    assert mock_signal._signal_conn.send.call_count == num_messages
    assert mock_signal._signal_conn.recv.call_count == len(signal_recv)
    assert resp == {'cpus': 5.2}


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
