from __future__ import absolute_import
from __future__ import unicode_literals

import unittest

import mock
from pytest import raises

from paasta_tools.deployd.common import BounceTimers
from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.deployd.workers import PaastaDeployWorker
from paasta_tools.marathon_tools import DEFAULT_SOA_DIR


class TestPaastaDeployWorker(unittest.TestCase):
    def setUp(self):
        self.mock_inbox_q = mock.Mock()
        self.mock_bounce_q = mock.Mock()
        self.mock_metrics = mock.Mock()
        with mock.patch(
            'paasta_tools.deployd.workers.PaastaDeployWorker.setup', autospec=True
        ):
            self.worker = PaastaDeployWorker(1,
                                             self.mock_inbox_q,
                                             self.mock_bounce_q,
                                             "westeros-prod",
                                             self.mock_metrics)

    def test_setup(self):
        with mock.patch(
            'paasta_tools.deployd.workers.marathon_tools.load_marathon_config', autospec=True
        ), mock.patch(
            'paasta_tools.deployd.workers.marathon_tools.get_marathon_client', autospec=True
        ):
            self.worker.setup()

    def test_setup_timers(self):
        mock_si = mock.Mock(bounce_timers=None,
                            service='universe',
                            instance='c137')
        ret = self.worker.setup_timers(mock_si)
        calls = [mock.call('bounce_length_timer',
                           service='universe',
                           paasta_cluster='westeros-prod',
                           instance='c137'),
                 mock.call().start(),
                 mock.call('processed_by_worker',
                           service='universe',
                           paasta_cluster='westeros-prod',
                           instance='c137'),
                 mock.call('setup_marathon_timer',
                           service='universe',
                           paasta_cluster='westeros-prod',
                           instance='c137')]
        self.mock_metrics.create_timer.assert_has_calls(calls)
        assert ret == BounceTimers(processed_by_worker=self.mock_metrics.create_timer.return_value,
                                   setup_marathon=self.mock_metrics.create_timer.return_value,
                                   bounce_length=self.mock_metrics.create_timer.return_value)

    def test_run(self):
        with mock.patch(
            'time.sleep', autospec=True
        ) as mock_sleep, mock.patch(
            'paasta_tools.deployd.workers.marathon_tools.get_all_marathon_apps', autospec=True
        ) as mock_get_all_marathon_apps, mock.patch(
            'paasta_tools.deployd.workers.PaastaDeployWorker.setup_timers', autospec=True
        ) as mock_setup_timers, mock.patch(
            'paasta_tools.deployd.workers.deploy_marathon_service', autospec=True
        ) as mock_deploy_marathon_service, mock.patch(
            'time.time', autospec=True, return_value=1
        ):
            mock_sleep.side_effect = LoopBreak
            self.worker.marathon_client = mock.Mock()
            self.worker.marathon_config = mock.Mock()
            mock_deploy_marathon_service.return_value = (0, None)
            mock_si = mock.Mock(service='universe',
                                instance='c137')
            self.mock_bounce_q.get.return_value = mock_si
            with raises(LoopBreak):
                self.worker.run()
            mock_setup_timers.assert_called_with(self.worker, mock_si)
            assert mock_setup_timers.return_value.setup_marathon.start.called
            mock_deploy_marathon_service.assert_called_with(service='universe',
                                                            instance='c137',
                                                            client=self.worker.marathon_client,
                                                            soa_dir=DEFAULT_SOA_DIR,
                                                            marathon_config=self.worker.marathon_config,
                                                            marathon_apps=mock_get_all_marathon_apps.return_value)
            assert mock_setup_timers.return_value.setup_marathon.stop.called
            assert not mock_setup_timers.return_value.processed_by_worker.start.called
            assert not self.mock_inbox_q.put.called
            assert mock_setup_timers.return_value.bounce_length.stop.called

            mock_deploy_marathon_service.return_value = (0, 60)
            mock_setup_timers.return_value.bounce_length.stop.reset_mock()
            with raises(LoopBreak):
                self.worker.run()
            mock_setup_timers.assert_called_with(self.worker, mock_si)
            assert mock_setup_timers.return_value.setup_marathon.start.called
            mock_deploy_marathon_service.assert_called_with(service='universe',
                                                            instance='c137',
                                                            client=self.worker.marathon_client,
                                                            soa_dir=DEFAULT_SOA_DIR,
                                                            marathon_config=self.worker.marathon_config,
                                                            marathon_apps=mock_get_all_marathon_apps.return_value)
            assert mock_setup_timers.return_value.setup_marathon.stop.called
            assert mock_setup_timers.return_value.processed_by_worker.start.called
            self.mock_inbox_q.put.assert_called_with(ServiceInstance(service='universe',
                                                                     instance='c137',
                                                                     bounce_by=61,
                                                                     watcher='Worker1',
                                                                     bounce_timers=mock_setup_timers.return_value))
            assert not mock_setup_timers.return_value.bounce_length.stop.called


class LoopBreak(Exception):
    pass
