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

import mock
import pytest
from botocore.exceptions import EndpointConnectionError

try:
    from clusterman.batch.autoscaler import AutoscalerBatch
except ImportError:
    pytest.mark.skip('Could not import the batch; are you in a Yelp-y environment?')
from clusterman.exceptions import PoolConnectionError


def batch(extra_args=None, mock_sensu=True):
    with mock.patch('clusterman.batch.autoscaler.setup_config'), \
            mock.patch('clusterman.batch.autoscaler.Autoscaler', signal=mock.Mock()):
        batch = AutoscalerBatch()
        args = ['--cluster', 'mesos-test', '--pool', 'bar', '--scheduler', 'mesos'] + (extra_args or [])
        parser = argparse.ArgumentParser()
        batch.parse_args(parser)
        batch.options = parser.parse_args(args)
        batch.options.instance_name = 'foo'
        if mock_sensu:
            batch._do_sensu_checkins = mock.Mock()
        batch.configure_initial()
        batch.version_checker = mock.Mock(watchers=[])
        return batch


@pytest.fixture(autouse=True)
def mock_logger():
    with mock.patch('clusterman.batch.autoscaler.logger', autospec=True) as mock_logger:
        yield mock_logger


@pytest.fixture(autouse=True)
def mock_watcher():
    with mock.patch('staticconf.config.ConfigurationWatcher', autospec=True):
        yield


@pytest.fixture(autouse=True)
def mock_pool_manager():
    with mock.patch('clusterman.batch.autoscaler.PoolManager', autospec=True):
        yield


@mock.patch('time.sleep')
@mock.patch('time.time')
@mock.patch('clusterman.batch.autoscaler.AutoscalerBatch.running', new_callable=mock.PropertyMock)
@mock.patch('clusterman.batch.autoscaler.sensu_checkin', autospec=True)
@pytest.mark.parametrize('dry_run', [True, False])
def test_run_ok(mock_sensu, mock_running, mock_time, mock_sleep, dry_run):
    extra_args = ['--dry-run'] if dry_run else []
    batch_obj = batch(extra_args, mock_sensu=False)
    batch_obj.autoscaler.run_frequency = 600

    mock_running.side_effect = [True, True, True, False]
    mock_time.side_effect = [101, 913, 2000]

    with mock.patch('builtins.hash') as mock_hash, \
            mock.patch.object(batch_obj, 'reload_watchers', autospec=True) as reload_watchers, \
            mock.patch('clusterman.batch.autoscaler.AutoscalerBatch.initialize_autoscaler') \
            as mock_initialize_autoscaler:
        reload_watchers.return_value = True
        mock_initialize_autoscaler.return_value = None
        mock_hash.return_value = 0  # patch hash to ignore splaying
        batch_obj.run()
        mock_initialize_autoscaler.assert_called_with()

    assert batch_obj.autoscaler.run.call_args_list == [mock.call(dry_run=dry_run) for i in range(3)]
    assert mock_sleep.call_args_list == [mock.call(499), mock.call(287), mock.call(400)]
    assert mock_sensu.call_count == 6


@mock.patch('clusterman.batch.autoscaler.AutoscalerBatch.running', new_callable=mock.PropertyMock)
@pytest.mark.parametrize('exc', [PoolConnectionError, EndpointConnectionError(endpoint_url='')])
def test_run_connection_error(mock_running, exc):
    batch_obj = batch()
    batch_obj._autoscale = mock.Mock(side_effect=exc)
    mock_running.side_effect = [True, True, False]

    with mock.patch.object(batch_obj, 'reload_watchers', autospec=True) as reload_watchers:
        reload_watchers.return_value = False
        batch_obj.run()

    # exceptions raised did not prevent subsequent calls to autoscale
    assert batch_obj._autoscale.call_count == 2
