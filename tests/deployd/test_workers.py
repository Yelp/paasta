import unittest

import mock
from pytest import raises

from paasta_tools.deployd.common import BaseServiceInstance
from paasta_tools.deployd.common import BounceTimers
from paasta_tools.deployd.workers import BounceResults
from paasta_tools.deployd.workers import PaastaDeployWorker
from paasta_tools.marathon_tools import DEFAULT_SOA_DIR


class TestPaastaDeployWorker(unittest.TestCase):
    def setUp(self):
        self.mock_instances_to_bounce = mock.Mock(
            get=mock.Mock(
                return_value=mock.Mock(__enter__=mock.Mock(), __exit__=mock.Mock())
            )
        )
        self.mock_metrics = mock.Mock()
        mock_config = mock.Mock(
            get_cluster=mock.Mock(return_value="westeros-prod"),
            get_deployd_worker_failure_backoff_factor=mock.Mock(return_value=30),
        )
        with mock.patch(
            "paasta_tools.deployd.workers.PaastaDeployWorker.setup", autospec=True
        ):
            self.worker = PaastaDeployWorker(
                1, self.mock_instances_to_bounce, mock_config, self.mock_metrics
            )

    def test_setup(self):
        with mock.patch(
            "paasta_tools.deployd.workers.load_system_paasta_config", autospec=True
        ), mock.patch(
            "paasta_tools.deployd.workers.marathon_tools.get_marathon_clients",
            autospec=True,
        ), mock.patch(
            "paasta_tools.deployd.workers.marathon_tools.get_marathon_servers",
            autospec=True,
        ):
            self.worker.setup()

    def test_setup_timers(self):
        mock_si = mock.Mock(bounce_timers=None, service="universe", instance="c137")
        ret = self.worker.setup_timers(mock_si)
        calls = [
            mock.call(
                "bounce_length_timer",
                service="universe",
                paasta_cluster="westeros-prod",
                instance="c137",
            ),
            mock.call().start(),
            mock.call(
                "processed_by_worker",
                service="universe",
                paasta_cluster="westeros-prod",
                instance="c137",
            ),
            mock.call(
                "setup_marathon_timer",
                service="universe",
                paasta_cluster="westeros-prod",
                instance="c137",
            ),
        ]
        self.mock_metrics.create_timer.assert_has_calls(calls)
        assert ret == BounceTimers(
            processed_by_worker=self.mock_metrics.create_timer.return_value,
            setup_marathon=self.mock_metrics.create_timer.return_value,
            bounce_length=self.mock_metrics.create_timer.return_value,
        )

    def test_run(self):
        with mock.patch("time.time", autospec=True, return_value=1), mock.patch(
            "time.sleep", autospec=True
        ) as mock_sleep, mock.patch(
            "paasta_tools.deployd.workers.PaastaDeployWorker.process_service_instance",
            autospec=True,
        ) as mock_process_service_instance:
            mock_timers = mock.Mock()
            mock_bounce_results = BounceResults(
                bounce_again_in_seconds=None, return_code=0, bounce_timers=mock_timers
            )
            mock_process_service_instance.return_value = mock_bounce_results
            mock_sleep.side_effect = LoopBreak
            mock_si = mock.Mock(
                service="universe", instance="c137", failures=0, processed_count=0
            )
            self.mock_instances_to_bounce.get.return_value.__enter__.return_value = (
                mock_si
            )
            with raises(LoopBreak):
                self.worker.run()
            mock_process_service_instance.assert_called_with(self.worker, mock_si)
            assert not self.mock_instances_to_bounce.put.called

            mock_bounce_results = BounceResults(
                bounce_again_in_seconds=60, return_code=1, bounce_timers=mock_timers
            )
            mock_process_service_instance.return_value = mock_bounce_results
            mock_queued_si = BaseServiceInstance(
                service="universe",
                instance="c137",
                cluster="westeros-prod",
                bounce_by=61,
                wait_until=61,
                watcher="Worker1",
                bounce_timers=mock_timers,
                failures=1,
                processed_count=1,
            )
            with raises(LoopBreak):
                self.worker.run()
            mock_process_service_instance.assert_called_with(self.worker, mock_si)
            self.mock_instances_to_bounce.put.assert_called_with(mock_queued_si)

            mock_si = mock.Mock(
                service="universe", instance="c137", failures=0, processed_count=0
            )
            self.mock_instances_to_bounce.get.return_value.__enter__.return_value = (
                mock_si
            )
            mock_process_service_instance.side_effect = Exception
            mock_queued_si = BaseServiceInstance(
                service="universe",
                instance="c137",
                cluster="westeros-prod",
                bounce_by=61,
                wait_until=61,
                watcher="Worker1",
                bounce_timers=mock_si.bounce_timers,
                failures=1,
                processed_count=1,
            )
            with raises(LoopBreak):
                self.worker.run()
            mock_process_service_instance.assert_called_with(self.worker, mock_si)
            self.mock_instances_to_bounce.put.assert_called_with(mock_queued_si)

    def test_process_service_instance(self):
        mock_client = mock.Mock()
        mock_app = mock.Mock()

        with mock.patch(
            "paasta_tools.deployd.workers.marathon_tools.get_all_marathon_apps",
            autospec=True,
            return_value=[mock_app],
        ), mock.patch(
            "paasta_tools.deployd.workers.PaastaDeployWorker.setup_timers",
            autospec=True,
        ) as mock_setup_timers, mock.patch(
            "paasta_tools.deployd.workers.deploy_marathon_service", autospec=True
        ) as mock_deploy_marathon_service, mock.patch(
            "time.time", autospec=True, return_value=1
        ):
            self.worker.marathon_clients = mock.Mock(
                get_all_clients=mock.Mock(return_value=[mock_client])
            )
            self.worker.marathon_config = mock.Mock()
            mock_deploy_marathon_service.return_value = (0, None)
            mock_si = mock.Mock(
                service="universe",
                instance="c137",
                failures=0,
                processed_count=0,
                bounce_by=0,
            )
            ret = self.worker.process_service_instance(mock_si)
            expected = BounceResults(None, 0, mock_setup_timers.return_value)
            assert ret == expected
            mock_setup_timers.assert_called_with(self.worker, mock_si)
            assert mock_setup_timers.return_value.setup_marathon.start.called
            mock_deploy_marathon_service.assert_called_with(
                service="universe",
                instance="c137",
                clients=self.worker.marathon_clients,
                soa_dir=DEFAULT_SOA_DIR,
                marathon_apps_with_clients=None,
            )
            assert mock_setup_timers.return_value.setup_marathon.stop.called
            assert not mock_setup_timers.return_value.processed_by_worker.start.called
            assert not mock_setup_timers.return_value.bounce_length.stop.called

            mock_si = mock.Mock(
                service="universe",
                instance="c137",
                failures=0,
                processed_count=1,
                bounce_by=0,
            )
            mock_setup_timers.return_value.bounce_length.stop.reset_mock()
            ret = self.worker.process_service_instance(mock_si)
            assert mock_setup_timers.return_value.bounce_length.stop.called

            mock_si = mock.Mock(
                service="universe",
                instance="c137",
                failures=0,
                processed_count=1,
                bounce_by=0,
            )
            mock_deploy_marathon_service.return_value = (0, 60)
            mock_setup_timers.return_value.bounce_length.stop.reset_mock()
            ret = self.worker.process_service_instance(mock_si)
            expected = BounceResults(60, 0, mock_setup_timers.return_value)
            assert ret == expected
            mock_setup_timers.assert_called_with(self.worker, mock_si)
            assert mock_setup_timers.return_value.setup_marathon.start.called
            mock_deploy_marathon_service.assert_called_with(
                service="universe",
                instance="c137",
                clients=self.worker.marathon_clients,
                soa_dir=DEFAULT_SOA_DIR,
                marathon_apps_with_clients=None,
            )
            assert mock_setup_timers.return_value.setup_marathon.stop.called
            assert mock_setup_timers.return_value.processed_by_worker.start.called
            assert not mock_setup_timers.return_value.bounce_length.stop.called


class LoopBreak(Exception):
    pass
