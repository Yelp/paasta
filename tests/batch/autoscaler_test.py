import argparse

import mock
import pytest

try:
    from clusterman.batch.autoscaler import AutoscalerBatch
except ImportError:
    pytest.mark.skip('Could not import the batch; are you in a Yelp-y environment?')


def batch(extra_args=None, mock_sensu=True):
    with mock.patch('clusterman.batch.autoscaler.setup_config'), \
            mock.patch('clusterman.batch.autoscaler.Autoscaler', signal=mock.Mock()):
        batch = AutoscalerBatch()
        args = ['--cluster', 'mesos-test', '--pool', 'bar', '--scheduler', 'mesos'] + extra_args
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
def test_run(mock_sensu, mock_running, mock_time, mock_sleep, dry_run):
    extra_args = ['--dry-run'] if dry_run else []
    batch_obj = batch(extra_args, mock_sensu=False)
    batch_obj.autoscaler.run_frequency = 600

    mock_running.side_effect = [True, True, True, False]
    mock_time.side_effect = [101, 913, 2000]

    with mock.patch('builtins.hash') as mock_hash:
        mock_hash.return_value = 0  # patch hash to ignore splaying
        batch_obj.run()
    assert batch_obj.autoscaler.run.call_args_list == [mock.call(dry_run=dry_run) for i in range(3)]
    assert mock_sleep.call_args_list == [mock.call(499), mock.call(287), mock.call(400)]
    assert mock_sensu.call_count == 6
