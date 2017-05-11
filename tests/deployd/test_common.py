from __future__ import absolute_import
from __future__ import unicode_literals

import unittest

import mock

from paasta_tools.deployd.common import PaastaQueue
from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.common import rate_limit_instances
from paasta_tools.deployd.common import ServiceInstance


class TestPaastaThread(unittest.TestCase):
    def setUp(self):
        self.thread = PaastaThread()

    def test_log(self):
        self.thread.log.info("HAAAALP ME")


class TestPaastaQueue(unittest.TestCase):
    def setUp(self):
        self.queue = PaastaQueue("AtThePostOffice")

    def test_log(self):
        self.queue.log.info("HAAAALP ME")

    def test_put(self):
        with mock.patch(
            'paasta_tools.deployd.common.Queue', autospec=True
        ) as mock_q:
            self.queue.put("human")
            mock_q.put.assert_called_with(self.queue, "human")


def test_rate_limit_instances():
    with mock.patch(
        'time.time', autospec=True
    ) as mock_time:
        mock_time.return_value = 1
        mock_si_1 = ('universe', 'c137')
        mock_si_2 = ('universe', 'c138')
        ret = rate_limit_instances([mock_si_1, mock_si_2], 2, "Custos")
        expected = [ServiceInstance(service='universe',
                                    instance='c137',
                                    watcher='Custos',
                                    bounce_by=1,
                                    bounce_timers=None),
                    ServiceInstance(service='universe',
                                    instance='c138',
                                    watcher='Custos',
                                    bounce_by=31,
                                    bounce_timers=None)]
        assert ret == expected
