from __future__ import absolute_import
from __future__ import unicode_literals

import time

from paasta_tools.deployd.common import PaastaThread

try:
    import yelp_meteorite
except ImportError:
    # Sorry to any non-yelpers but you won't
    # get metrics emitted as our metrics library
    # is currently not open source
    import mock
    yelp_meteorite = mock.Mock()


def create_timer(name, **kwargs):
    return yelp_meteorite.create_timer('paasta.deployd.{}'.format(name), kwargs)


def create_gauge(name, **kwargs):
    return yelp_meteorite.create_gauge('paasta.deployd.{}'.format(name), kwargs)


class QueueMetrics(PaastaThread):
    def __init__(self, inbox_q, bounce_q):
        super(QueueMetrics, self).__init__()
        self.daemon = True
        self.inbox_q = inbox_q
        self.bounce_q = bounce_q
        self.inbox_q_gauge = create_gauge("inbox_queue")
        self.bounce_q_gauge = create_gauge("bounce_queue")

    def run(self):
        while True:
            self.inbox_q_gauge.set(self.inbox_q.qsize())
            self.bounce_q_gauge.set(self.bounce_q.qsize())
            time.sleep(20)
