import unittest

import mock
from py.test import raises

from paasta_tools.metrics import metrics_lib


class TestNoMetrics(unittest.TestCase):
    def setUp(self):
        self.metrics = metrics_lib.NoMetrics("paasta.deployd")

    def test_timer(self):
        timer = self.metrics.create_timer("name", dimension="thing")
        timer.start()
        timer.stop()

    def test_gauge(self):
        gauge = self.metrics.create_gauge("name", dimension="thing")
        gauge.set(1212)

    def test_counter(self):
        counter = self.metrics.create_counter("name", dimension="thing")
        counter.count()


class TestMeteoriteMetrics(unittest.TestCase):
    def setUp(self):
        self.mock_meteorite = mock.Mock()
        metrics_lib.yelp_meteorite = self.mock_meteorite
        self.metrics = metrics_lib.MeteoriteMetrics("paasta.deployd")

    def test_init(self):
        metrics_lib.yelp_meteorite = None
        with raises(ImportError):
            metrics_lib.MeteoriteMetrics("paasta.deployd")

    def test_init_no_error(self):
        metrics_lib.MeteoriteMetrics("paasta.deployd")

    def test_create_timer(self):
        self.metrics.create_timer("name", dimension="thing")
        self.mock_meteorite.create_timer.assert_called_with(
            "paasta.deployd.name", {"dimension": "thing"}
        )

    def test_create_gauge(self):
        self.metrics.create_gauge("name", dimension="thing")
        self.mock_meteorite.create_gauge.assert_called_with(
            "paasta.deployd.name", {"dimension": "thing"}
        )

    def test_create_counter(self):
        self.metrics.create_counter("name", dimension="thing")
        self.mock_meteorite.create_counter.assert_called_with(
            "paasta.deployd.name", {"dimension": "thing"}
        )

    def tearDown(self):
        metrics_lib.yelp_meteorite = None
