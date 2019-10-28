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
import arrow
import jsonpickle
import mock
import pytest
from clusterman_metrics import SYSTEM_METRICS

from clusterman.simulator.io import read_object_from_compressed_json
from clusterman.simulator.io import write_object_to_compressed_json


@pytest.fixture
def mock_ts_1():
    return {SYSTEM_METRICS: {'metric_1': [(arrow.get(1), 1.0), (arrow.get(2), 2.0), (arrow.get(3), 3.0)]}}


@pytest.yield_fixture
def mock_open():
    with mock.patch('clusterman.simulator.io.gzip') as mgz:
        mock_open_obj = mock.Mock()
        mgz.open.return_value.__enter__ = mock.Mock(return_value=mock_open_obj)
        yield mock_open_obj


def test_write_new_object(mock_ts_1, mock_open):
    write_object_to_compressed_json(mock_ts_1, 'foo')
    assert jsonpickle.decode(mock_open.write.call_args[0][0]) == mock_ts_1


@pytest.mark.parametrize('raw_ts', [True, False])
def test_read_new_object(raw_ts, mock_ts_1, mock_open):
    expected_return = {
        SYSTEM_METRICS: {
            'metric_1': [
                ((t.timestamp, v) if raw_ts else (t, v))
                for t, v in mock_ts_1[SYSTEM_METRICS]['metric_1']
            ]
        }
    }
    mock_open.read.return_value = jsonpickle.encode(expected_return).encode()
    assert read_object_from_compressed_json('foo', raw_timestamps=raw_ts) == expected_return
