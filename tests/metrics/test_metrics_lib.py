import unittest
from unittest import mock

from py.test import raises

from paasta_tools.metrics import metrics_lib
from paasta_tools.metrics.metrics_lib import _parse_metric_labels_env


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

    def test_event(self):
        self.metrics.emit_event("name", dimension="thing")


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
            "paasta.deployd.name", dimension="thing"
        )

    def test_create_gauge(self):
        self.metrics.create_gauge("name", dimension="thing")
        self.mock_meteorite.create_gauge.assert_called_with(
            "paasta.deployd.name", dimension="thing"
        )

    def test_create_counter(self):
        self.metrics.create_counter("name", dimension="thing")
        self.mock_meteorite.create_counter.assert_called_with(
            "paasta.deployd.name", dimension="thing"
        )

    def test_emit_event(self):
        self.metrics.emit_event("name", dimension="thing")
        self.mock_meteorite.emit_event.assert_called_with(
            "paasta.deployd.name", dimension="thing"
        )

    def tearDown(self):
        metrics_lib.yelp_meteorite = None


class TestParseMetricLabelsEnv(unittest.TestCase):
    def test_empty_envvar(self):
        with mock.patch.dict("os.environ", {"PAASTA_METRICS_LABELS": ""}):
            self.assertEqual(_parse_metric_labels_env(), {})

    def test_envvar_absent(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(_parse_metric_labels_env(), {})

    def test_single_label(self):
        with mock.patch.dict(
            "os.environ", {"PAASTA_METRICS_LABELS": "job=sync_paasta_secrets_flink"}
        ):
            self.assertEqual(
                _parse_metric_labels_env(), {"job": "sync_paasta_secrets_flink"}
            )

    def test_multiple_labels(self):
        with mock.patch.dict(
            "os.environ", {"PAASTA_METRICS_LABELS": "job=sync_flink,workload=flink"}
        ):
            self.assertEqual(
                _parse_metric_labels_env(), {"job": "sync_flink", "workload": "flink"}
            )

    def test_malformed_token_skipped(self):
        with mock.patch.dict(
            "os.environ", {"PAASTA_METRICS_LABELS": "bad_token_no_equals"}
        ):
            self.assertEqual(_parse_metric_labels_env(), {})

    def test_mixed_valid_and_malformed(self):
        with mock.patch.dict(
            "os.environ", {"PAASTA_METRICS_LABELS": "good=val,bad_token,other=x"}
        ):
            self.assertEqual(_parse_metric_labels_env(), {"good": "val", "other": "x"})
