import unittest

import mock

from paasta_tools.deployd import metrics


class TestQueueAndWorkerMetrics(unittest.TestCase):
    def setUp(self):
        mock_metrics_provider = mock.Mock()
        self.mock_queue = mock.Mock()
        self.mock_workers = []
        mock_create_gauge = mock.Mock(side_effect=lambda *args, **kwargs: mock.Mock())
        mock_metrics_provider.create_gauge = mock_create_gauge
        self.metrics = metrics.QueueAndWorkerMetrics(
            self.mock_queue, self.mock_workers, "mock-cluster", mock_metrics_provider
        )

    def test_all_metrics(self):
        with mock.patch("time.time", autospec=True, return_value=10):
            self.mock_queue.available_service_instances.queue = [
                (0, mock.Mock()),
                (1, mock.Mock()),
                (2, mock.Mock()),
                (3, mock.Mock()),
                (4, mock.Mock()),
                # 0-60
                (11, mock.Mock()),
                (12, mock.Mock()),
                # 60-300
                (71, mock.Mock()),
                (72, mock.Mock()),
                (73, mock.Mock()),
                # 300-3600
                (311, mock.Mock()),
                # 3600+
                (3611, mock.Mock()),
            ]
            self.mock_queue.unavailable_service_instances.queue = [
                (15, 75, mock.Mock())
            ]
            self.metrics.run_once()

            # Don't bother testing instances_to_bounce_later_gauge and instances_to_bounce_now_gauge -- they just call
            # qsize on things we've mocked out.

            self.metrics.instances_with_past_deadline_gauge.set.assert_called_once_with(
                5
            )

            self.metrics.instances_with_deadline_in_next_n_seconds_gauges[
                "available", 60
            ].set.assert_called_once_with(2)
            self.metrics.instances_with_deadline_in_next_n_seconds_gauges[
                "available", 300
            ].set.assert_called_once_with(5)
            self.metrics.instances_with_deadline_in_next_n_seconds_gauges[
                "available", 3600
            ].set.assert_called_once_with(6)

            self.metrics.instances_with_deadline_in_next_n_seconds_gauges[
                "unavailable", 60
            ].set.assert_called_once_with(0)
            self.metrics.instances_with_deadline_in_next_n_seconds_gauges[
                "unavailable", 300
            ].set.assert_called_once_with(1)
            self.metrics.instances_with_deadline_in_next_n_seconds_gauges[
                "unavailable", 3600
            ].set.assert_called_once_with(1)

            self.metrics.max_time_past_deadline_gauge.set.assert_called_once_with(10)
            self.metrics.sum_time_past_deadline_gauge.set.assert_called_once_with(
                10 + 9 + 8 + 7 + 6
            )
