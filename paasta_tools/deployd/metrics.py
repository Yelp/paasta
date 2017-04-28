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
    yelp_meteorite = None

_metricis_interfaces = {}


def register_metrics_interface(name):
    def outer(func):
        _metricis_interfaces[name] = func
        return func
    return outer


def get_metrics_interface(name):
    return _metricis_interfaces[name]()


@register_metrics_interface(None)
class NoMetrics(object):
    def create_timer(self, *args, **kwargs):
        return Timer()

    def create_gauge(self, *args, **kwargs):
        return Gauge()


class Timer(object):
    def start(self):
        pass

    def stop(self):
        pass


class Gauge(object):
    def set(self, value):
        pass


@register_metrics_interface('meteorite')
class MeteoriteMetrics(object):
    def __init__(self):
        if not yelp_meteorite:
            raise ImportError("yelp_meteorite not imported, please try another deployd_metrics_provider")

    def create_timer(self, name, **kwargs):
        return yelp_meteorite.create_timer('paasta.deployd.{}'.format(name), kwargs)

    def create_gauge(self, name, **kwargs):
        return yelp_meteorite.create_gauge('paasta.deployd.{}'.format(name), kwargs)


class QueueMetrics(PaastaThread):
    def __init__(self, inbox_q, bounce_q, metrics_provider):
        super(QueueMetrics, self).__init__()
        self.daemon = True
        self.inbox_q = inbox_q
        self.bounce_q = bounce_q
        self.metrics = metrics_provider
        self.inbox_q_gauge = self.metrics.create_gauge("inbox_queue")
        self.bounce_q_gauge = self.metrics.create_gauge("bounce_queue")

    def run(self):
        while True:
            self.inbox_q_gauge.set(self.inbox_q.qsize())
            self.bounce_q_gauge.set(self.bounce_q.qsize())
            time.sleep(20)
