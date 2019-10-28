from argparse import Namespace

import mock
import pytest

from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.cli.manage import change_target_capacity
from clusterman.cli.manage import get_target_capacity_value
from clusterman.cli.manage import LOG_TEMPLATE
from clusterman.cli.manage import main
from clusterman.cli.manage import mark_stale


@pytest.fixture
def args():
    return Namespace(
        cluster='foo',
        pool='bar',
        scheduler='mesos',
        target_capacity='123',
        dry_run=False,
        mark_stale=True,
    )


@pytest.fixture
def mock_pool_manager():
    return mock.Mock(cluster='mesos-test', pool='bar', scheduler='mesos', spec=PoolManager)


def test_get_target_capacity_value_min():
    assert get_target_capacity_value('mIN', 'bar', 'mesos') == 3


def test_get_target_capacity_value_max():
    assert get_target_capacity_value('mAx', 'bar', 'mesos') == 345


def test_get_target_capacity_value_number():
    assert get_target_capacity_value('123', 'bar', 'mesos') == 123


def test_get_target_capacity_value_invalid():
    with pytest.raises(ValueError):
        get_target_capacity_value('asdf', 'bar', 'mesos')


@mock.patch('clusterman.cli.manage.ask_for_confirmation')
@mock.patch('clusterman.cli.manage.get_target_capacity_value')
class TestManageMethods:
    @pytest.mark.parametrize('dry_run', [True, False])
    def test_change_target_capacity(
        self,
        mock_target_capacity,
        mock_confirm,
        mock_pool_manager,
        dry_run,
    ):
        args.dry_run = dry_run
        mock_target_capacity.return_value = 123

        change_target_capacity(mock_pool_manager, 123, dry_run)
        assert mock_confirm.call_count == 0 if dry_run else 1
        assert mock_pool_manager.modify_target_capacity.call_args == mock.call(123, dry_run)
        assert mock_pool_manager.modify_target_capacity.call_count == 1
        assert mock_pool_manager.mark_stale.call_count == 0

    def test_abort_change_target_capacity(
        self,
        mock_target_capacity,
        mock_confirm,
        mock_pool_manager,
    ):
        mock_target_capacity.return_value = 123
        mock_confirm.return_value = False

        change_target_capacity(mock_pool_manager, 123, False)
        assert mock_confirm.call_count == 1
        assert mock_pool_manager.modify_target_capacity.call_count == 0
        assert mock_pool_manager.mark_stale.call_count == 0

    @pytest.mark.parametrize('dry_run', [True, False])
    def test_change_mark_stale(
        self,
        mock_target_capacity,
        mock_confirm,
        mock_pool_manager,
        dry_run,
    ):
        args.dry_run = dry_run

        mark_stale(mock_pool_manager, dry_run)
        assert mock_confirm.call_count == 0 if dry_run else 1
        assert mock_pool_manager.mark_stale.call_args == mock.call(dry_run)
        assert mock_pool_manager.modify_target_capacity.call_count == 0
        assert mock_pool_manager.mark_stale.call_count == 1

    def test_abort_mark_stale(
        self,
        mock_target_capacity,
        mock_confirm,
        mock_pool_manager,
    ):
        mock_confirm.return_value = False

        mark_stale(mock_pool_manager.return_value, False)
        assert mock_confirm.call_count == 1
        assert mock_pool_manager.modify_target_capacity.call_count == 0
        assert mock_pool_manager.mark_stale.call_count == 0


@mock.patch('clusterman.cli.manage.log_to_scribe')
@mock.patch('clusterman.cli.manage.PoolManager')
@mock.patch('clusterman.cli.manage.change_target_capacity')
@mock.patch('clusterman.cli.manage.mark_stale')
@mock.patch('clusterman.cli.manage.get_autoscaler_scribe_stream')
def test_main_error(mock_scribe_stream, mock_stale, mock_change, mock_pool_manager, mock_logger, args):
    mock_scribe_stream.return_value = 'scribe_stream'
    mock_stale.return_value = 'marking stale'
    mock_change.return_value = 'changing capacity'
    with pytest.raises(ValueError):
        main(args)


@mock.patch('clusterman.cli.manage.log_to_scribe')
@mock.patch('clusterman.cli.manage.PoolManager')
@mock.patch('clusterman.cli.manage.change_target_capacity')
@mock.patch('clusterman.cli.manage.mark_stale')
@mock.patch('clusterman.cli.manage.get_autoscaler_scribe_stream')
@pytest.mark.parametrize('non_action', ['mark_stale', 'target_capacity'])
def test_main(mock_scribe_stream, mock_stale, mock_change, mock_pool_manager, mock_logger, args, non_action):
    setattr(args, non_action, None)
    mock_scribe_stream.return_value = 'scribe_stream'
    mock_stale.return_value = 'marking stale'
    mock_change.return_value = 'changing capacity'
    main(args)
    if non_action == 'mark_stale':
        assert mock_change.call_count == 1
        assert mock_logger.call_args == mock.call('scribe_stream', f'{LOG_TEMPLATE} changing capacity')
    elif non_action == 'target_capacity':
        assert mock_stale.call_count == 1
        assert mock_logger.call_args == mock.call('scribe_stream', f'{LOG_TEMPLATE} marking stale')
