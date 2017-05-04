from __future__ import absolute_import
from __future__ import unicode_literals

import unittest

import mock

from paasta_tools.deployd.common import PaastaQueue
from paasta_tools.deployd.common import PaastaThread


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
