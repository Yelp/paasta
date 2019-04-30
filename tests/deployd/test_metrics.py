import unittest

import mock
from py.test import raises

from paasta_tools.deployd import metrics


class TestQueueMetrics(unittest.TestCase):
    def setUp(self):
        mock_metrics_provider = mock.Mock()
        self.mock_gauge = mock.Mock()
        self.mock_inbox = mock.Mock(instances_to_bounce_later=mock.Mock(), to_bounce={})
        self.mock_instances_to_bounce_now = mock.Mock()
        mock_create_gauge = mock.Mock(return_value=self.mock_gauge)
        mock_metrics_provider.create_gauge = mock_create_gauge
        self.metrics = metrics.QueueMetrics(
            self.mock_inbox, "mock-cluster", mock_metrics_provider,
        )

    def test_run(self):
        with mock.patch('time.sleep', autospec=True, side_effect=LoopBreak):
            with raises(LoopBreak):
                self.metrics.run()
            assert self.mock_gauge.set.call_count == 3


class LoopBreak(Exception):
    pass
