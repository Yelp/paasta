from __future__ import absolute_import
from __future__ import unicode_literals

import sys
import unittest

import mock
from pytest import raises
from six.moves.queue import Empty

from paasta_tools.deployd.common import ServiceInstance


class FakePyinotify(object):  # pragma: no cover
    class ProcessEvent():
        pass

    @property
    def WatchManager():
        pass

    @property
    def EventsCodes():
        pass

    @property
    def Notifier():
        pass


# This module is only available on linux
# and we will be mocking it in the unit tests anyway
# so this just creates it as a dummy module to prevent
# the ImportError
sys.modules['pyinotify'] = FakePyinotify

from paasta_tools.deployd.master import Inbox  # noqa
from paasta_tools.deployd.master import DeployDaemon  # noqa
from paasta_tools.deployd.master import main  # noqa


class TestInbox(unittest.TestCase):
    def setUp(self):
        self.mock_bounce_q = mock.Mock()
        self.mock_inbox_q = mock.Mock()
        self.inbox = Inbox(self.mock_inbox_q, self.mock_bounce_q)

    def test_run(self):
        with mock.patch(
            'paasta_tools.deployd.master.Inbox.process_inbox', autospec=True,
            side_effect=LoopBreak,
        ) as mock_process:
            with raises(LoopBreak):
                self.inbox.run()
            assert mock_process.called

    def test_process_inbox(self):
        self.mock_inbox_q.get.side_effect = Empty
        self.mock_inbox_q.empty.return_value = True
        self.inbox.to_bounce = {}
        with mock.patch(
            'paasta_tools.deployd.master.Inbox.process_service_instance', autospec=True,
        ) as mock_process_service_instance, mock.patch(
            'paasta_tools.deployd.master.Inbox.process_to_bounce', autospec=True,
        ) as mock_process_to_bounce, mock.patch(
            'time.sleep', autospec=True,
        ):
            self.inbox.process_inbox()
            self.mock_inbox_q.get.assert_called_with(block=False)
            assert not mock_process_service_instance.called
            assert not mock_process_to_bounce.called

            mock_si = mock.Mock()
            self.mock_inbox_q.get.side_effect = None
            self.mock_inbox_q.get.return_value = mock_si
            self.mock_inbox_q.empty.return_value = False
            self.inbox.process_inbox()
            mock_process_service_instance.assert_called_with(self.inbox, mock_si)
            assert not mock_process_to_bounce.called

            self.inbox.to_bounce = {'service.instance': mock_si}
            self.mock_inbox_q.empty.return_value = True
            self.inbox.process_inbox()
            mock_process_service_instance.assert_called_with(self.inbox, mock_si)
            assert mock_process_to_bounce.called

    def test_process_service_instance(self):
        mock_service_instance = mock.Mock(service='universe', instance='c137')
        with mock.patch(
            'paasta_tools.deployd.master.Inbox.should_add_to_bounce', autospec=True,
        ) as mock_should_add_to_bounce:
            mock_should_add_to_bounce.return_value = False
            self.inbox.process_service_instance(mock_service_instance)
            assert self.inbox.to_bounce == {}

            mock_should_add_to_bounce.return_value = True
            self.inbox.process_service_instance(mock_service_instance)
            assert self.inbox.to_bounce == {'universe.c137': mock_service_instance}

    def test_should_add_to_bounce(self):
        mock_service_instance_1 = mock.Mock(bounce_by=10)
        mock_service_instance_2 = mock.Mock(bounce_by=20)
        self.inbox.to_bounce = {'universe.c137': mock_service_instance_1}
        assert not self.inbox.should_add_to_bounce(mock_service_instance_2, 'universe.c137')
        self.inbox.to_bounce = {'universe.c137': mock_service_instance_2}
        assert self.inbox.should_add_to_bounce(mock_service_instance_1, 'universe.c137')
        self.inbox.to_bounce = {}
        assert self.inbox.should_add_to_bounce(mock_service_instance_1, 'universe.c137')

    def test_process_to_bounce(self):
        with mock.patch(
            'time.time', autospec=True,
        ) as mock_time:
            mock_time.return_value = 50
            mock_service_instance_1 = mock.Mock(bounce_by=10)
            mock_service_instance_2 = mock.Mock(bounce_by=60)
            self.inbox.to_bounce = {
                'universe.c137': mock_service_instance_1,
                'universe.c138': mock_service_instance_2,
            }
            self.inbox.process_to_bounce()
            self.mock_bounce_q.put.assert_called_with(mock_service_instance_1)
            assert self.mock_bounce_q.put.call_count == 1

    def tearDown(self):
        self.inbox.to_bounce = {}


class TestDeployDaemon(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            'paasta_tools.deployd.master.PaastaQueue', autospec=True,
        ), mock.patch(
            'paasta_tools.deployd.master.Inbox', autospec=True,
        ) as self.mock_inbox, mock.patch(
            'paasta_tools.deployd.master.get_marathon_client_from_config', autospec=True,
        ), mock.patch(
            'paasta_tools.deployd.master.load_system_paasta_config', autospec=True,
        ) as mock_config_getter:
            mock_config = mock.Mock(
                get_deployd_log_level=mock.Mock(return_value='INFO'),
                get_deployd_number_workers=mock.Mock(return_value=5),
                get_deployd_big_bounce_rate=mock.Mock(return_value=10),
                get_cluster=mock.Mock(return_value='westeros-prod'),
                get_log_writer=mock.Mock(return_value={'driver': None}),
            )
            mock_config_getter.return_value = mock_config
            self.deployd = DeployDaemon()

    def test_run(self):
        with mock.patch(
            'paasta_tools.deployd.master.ZookeeperPool', autospec=True,
        ), mock.patch(
            'paasta_tools.deployd.master.PaastaLeaderElection', autospec=True,
        ) as mock_election_class:
            mock_election = mock.Mock()
            mock_election_class.return_value = mock_election
            self.deployd.run()
            assert mock_election_class.called
            mock_election.run.assert_called_with(self.deployd.startup)

    def test_bounce(self):
        mock_si = mock.Mock()
        self.deployd.bounce(mock_si)
        self.deployd.inbox_q.put.assert_called_with(mock_si)

    def test_startup(self):
        assert not hasattr(self.deployd, 'is_leader')
        assert not self.deployd.started
        with mock.patch(
            'paasta_tools.deployd.master.QueueMetrics', autospec=True,
        ) as mock_q_metrics, mock.patch(
            'paasta_tools.deployd.master.get_metrics_interface', autospec=True,
        ) as mock_get_metrics_interface, mock.patch(
            'paasta_tools.deployd.master.DeployDaemon.start_watchers', autospec=True,
        ) as mock_start_watchers, mock.patch(
            'paasta_tools.deployd.master.DeployDaemon.prioritise_bouncing_services', autospec=True,
        ) as mock_prioritise_bouncing_services, mock.patch(
            'paasta_tools.deployd.master.DeployDaemon.add_all_services', autospec=True,
        ) as mock_add_all_services, mock.patch(
            'paasta_tools.deployd.master.DeployDaemon.start_workers', autospec=True,
        ) as mock_start_workers, mock.patch(
            'paasta_tools.deployd.master.DeployDaemon.main_loop', autospec=True,
        ) as mock_main_loop:
            self.deployd.startup()
            assert self.deployd.started
            assert self.deployd.is_leader
            mock_q_metrics.assert_called_with(
                self.deployd.inbox,
                self.deployd.bounce_q,
                'westeros-prod',
                mock_get_metrics_interface.return_value,
            )
            assert mock_q_metrics.return_value.start.called
            assert mock_start_watchers.called
            assert mock_add_all_services.called
            assert mock_prioritise_bouncing_services.called
            assert mock_start_workers.called
            assert mock_main_loop.called

    def test_main_loop(self):
        with mock.patch(
            'time.sleep', autospec=True,
        ) as mock_sleep, mock.patch(
            'paasta_tools.deployd.master.DeployDaemon.all_watchers_running', autospec=True,
        ) as mock_all_watchers_running, mock.patch(
            'paasta_tools.deployd.master.DeployDaemon.all_workers_dead', autospec=True,
        ) as mock_all_workers_dead, mock.patch(
            'paasta_tools.deployd.master.DeployDaemon.check_and_start_workers', autospec=True,
        ) as mock_check_and_start_workers:
            mock_all_workers_dead.return_value = False
            mock_all_watchers_running.return_value = True
            self.deployd.control.get.return_value = "ABORT"
            self.deployd.main_loop()
            assert not mock_sleep.called
            assert not mock_check_and_start_workers.called

            mock_all_workers_dead.return_value = True
            mock_all_watchers_running.return_value = True
            self.deployd.control.get.return_value = None
            with raises(SystemExit):
                self.deployd.main_loop()
            assert not mock_sleep.called
            assert not mock_check_and_start_workers.called

            mock_all_workers_dead.return_value = False
            mock_all_watchers_running.return_value = False
            self.deployd.control.get.return_value = None
            with raises(SystemExit):
                self.deployd.main_loop()
            assert not mock_sleep.called
            assert not mock_check_and_start_workers.called

            mock_all_workers_dead.return_value = False
            mock_all_watchers_running.return_value = True
            mock_sleep.side_effect = [None, LoopBreak]
            self.deployd.control.get.side_effect = Empty
            with raises(LoopBreak):
                self.deployd.main_loop()
            assert mock_sleep.call_count == 2
            assert mock_check_and_start_workers.call_count == 2

    def test_all_watchers_running(self):
        mock_watchers = [
            mock.Mock(is_alive=mock.Mock(return_value=True)),
            mock.Mock(is_alive=mock.Mock(return_value=True)),
        ]
        self.deployd.watcher_threads = mock_watchers
        assert self.deployd.all_watchers_running()

        mock_watchers = [
            mock.Mock(is_alive=mock.Mock(return_value=True)),
            mock.Mock(is_alive=mock.Mock(return_value=False)),
        ]
        self.deployd.watcher_threads = mock_watchers
        assert not self.deployd.all_watchers_running()

        mock_watchers = [
            mock.Mock(is_alive=mock.Mock(return_value=False)),
            mock.Mock(is_alive=mock.Mock(return_value=False)),
        ]
        self.deployd.watcher_threads = mock_watchers
        assert not self.deployd.all_watchers_running()

    def test_all_workers_dead(self):
        mock_workers = [
            mock.Mock(is_alive=mock.Mock(return_value=True)),
            mock.Mock(is_alive=mock.Mock(return_value=True)),
        ]
        self.deployd.workers = mock_workers
        assert not self.deployd.all_workers_dead()

        mock_workers = [
            mock.Mock(is_alive=mock.Mock(return_value=True)),
            mock.Mock(is_alive=mock.Mock(return_value=False)),
        ]
        self.deployd.workers = mock_workers
        assert not self.deployd.all_workers_dead()

        mock_workers = [
            mock.Mock(is_alive=mock.Mock(return_value=False)),
            mock.Mock(is_alive=mock.Mock(return_value=False)),
        ]
        self.deployd.workers = mock_workers
        assert self.deployd.all_workers_dead()

    def test_check_and_start_workers(self):
        with mock.patch(
            'paasta_tools.deployd.master.PaastaDeployWorker', autospec=True,
        ) as mock_paasta_worker:
            mock_worker_instance = mock.Mock()
            mock_paasta_worker.return_value = mock_worker_instance
            self.deployd.metrics = None
            mock_alive_worker = mock.Mock(is_alive=mock.Mock(return_value=True))
            mock_dead_worker = mock.Mock(is_alive=mock.Mock(return_value=False))
            self.deployd.workers = [mock_alive_worker] * 5
            self.deployd.check_and_start_workers()
            assert not mock_paasta_worker.called

            self.deployd.workers = [mock_alive_worker] * 3 + [mock_dead_worker] * 2
            self.deployd.check_and_start_workers()
            assert mock_paasta_worker.call_count == 2
            assert mock_worker_instance.start.call_count == 2

    def test_stop(self):
        self.deployd.stop()
        self.deployd.control.put.assert_called_with("ABORT")

    def test_start_workers(self):
        with mock.patch(
            'paasta_tools.deployd.master.PaastaDeployWorker', autospec=True,
        ) as mock_paasta_worker:
            self.deployd.metrics = mock.Mock()
            self.deployd.start_workers()
            assert mock_paasta_worker.call_count == 5

    def test_prioritise_bouncing_services(self):
        with mock.patch(
            'paasta_tools.deployd.master.get_service_instances_that_need_bouncing', autospec=True,
        ) as mock_get_service_instances_that_need_bouncing, mock.patch(
            'time.time', autospec=True, return_value=1,
        ):
            mock_changed_instances = (x for x in {'universe.c137', 'universe.c138'})
            mock_get_service_instances_that_need_bouncing.return_value = mock_changed_instances

            self.deployd.prioritise_bouncing_services()
            mock_get_service_instances_that_need_bouncing.assert_called_with(
                self.deployd.marathon_client,
                '/nail/etc/services',
            )
            calls = [
                mock.call(ServiceInstance(
                    service='universe',
                    instance='c138',
                    watcher='DeployDaemon',
                    bounce_by=1,
                    bounce_timers=None,
                    failures=0,
                )),
                mock.call(ServiceInstance(
                    service='universe',
                    instance='c137',
                    watcher='DeployDaemon',
                    bounce_by=1,
                    bounce_timers=None,
                    failures=0,
                )),
            ]
            self.deployd.inbox_q.put.assert_has_calls(calls, any_order=True)

    def test_start_watchers(self):
        class FakeWatchers(object):  # pragma: no cover
            class PaastaWatcher(object):
                def __init__(self, *args, **kwargs):
                    pass

                def start(self):
                    pass

                @property
                def is_ready(self):
                    return True

            class FakeWatcher(PaastaWatcher):
                pass
        with mock.patch(
            'paasta_tools.deployd.master.watchers', autospec=False, new=FakeWatchers,
        ), mock.patch(
            'time.sleep', autospec=True,
        ) as mock_sleep:
            mock_zk = mock.Mock()
            self.deployd.zk = mock_zk
            mock_start = mock.Mock()
            FakeWatchers.PaastaWatcher.start = mock_start
            self.deployd.start_watchers()
            assert mock_start.call_count == 1

            FakeWatchers.PaastaWatcher.is_ready = False
            mock_sleep.side_effect = LoopBreak
            with raises(LoopBreak):
                self.deployd.start_watchers()


def test_main():
    with mock.patch(
        'paasta_tools.deployd.master.DeployDaemon', autospec=True,
    ) as mock_deployd_class, mock.patch(
        'time.sleep', autospec=True, side_effect=LoopBreak,
    ):
        mock_deployd = mock.Mock()
        mock_deployd_class.return_value = mock_deployd
        with raises(LoopBreak):
            main()
        assert mock_deployd_class.called
        assert mock_deployd.start.called


class LoopBreak(Exception):
    pass
