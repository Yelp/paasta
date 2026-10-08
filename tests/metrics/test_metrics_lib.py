import sys
import unittest
from unittest import mock

import pytest
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


@pytest.fixture
def mock_system_timer():
    with mock.patch(
        "paasta_tools.metrics.metrics_lib.system_timer", autospec=True
    ) as mock_system_timer:
        yield mock_system_timer


def _sys_exit(code):
    def fn():
        sys.exit(code)

    return fn


def _raise():
    raise ValueError("boom")


def _raise_keyboard_interrupt():
    raise KeyboardInterrupt()


@pytest.mark.parametrize(
    "fn,expected_exception,expected_result",
    [
        (lambda: None, None, 0),
        (_sys_exit(None), SystemExit, 0),
        (_sys_exit(0), SystemExit, 0),
        (_sys_exit(2), SystemExit, 2),
        (_sys_exit(-1), SystemExit, 255),
        (_sys_exit(256), SystemExit, 0),
        (_sys_exit("error message"), SystemExit, 1),
        (_raise, ValueError, metrics_lib.CRASHED_RESULT),
        (_raise_keyboard_interrupt, KeyboardInterrupt, metrics_lib.CRASHED_RESULT),
    ],
)
def test_timed_entry_point_result(
    mock_system_timer, fn, expected_exception, expected_result
):
    wrapped = metrics_lib.timed_entry_point(fn)
    if expected_exception:
        with raises(expected_exception):
            wrapped()
    else:
        wrapped()

    mock_system_timer.assert_called_once_with(dimensions={"path": __file__})
    mock_timer = mock_system_timer.return_value
    mock_timer.start.assert_called_once_with()
    mock_timer.stop.assert_called_once_with(tmp_dimensions={"result": expected_result})


def test_timed_entry_point_preserves_exit_message_and_return_value(mock_system_timer):
    with raises(SystemExit) as excinfo:
        metrics_lib.timed_entry_point(_sys_exit("error message"))()
    assert excinfo.value.code == "error message"

    assert metrics_lib.timed_entry_point(lambda: 3)() == 3


def test_add_timer_dimensions(mock_system_timer):
    @metrics_lib.timed_entry_point
    def main():
        metrics_lib.add_timer_dimensions(cluster="a_cluster", eks=True)
        metrics_lib.add_timer_dimensions(result="ignored")

    main()

    mock_system_timer.return_value.stop.assert_called_once_with(
        tmp_dimensions={"cluster": "a_cluster", "eks": True, "result": 0}
    )
    # dimensions don't leak outside the decorated call
    assert metrics_lib._timer_extra_dimensions.get() is None


def test_add_timer_dimensions_outside_decorator():
    metrics_lib.add_timer_dimensions(cluster="a_cluster")
    assert metrics_lib._timer_extra_dimensions.get() is None


def test_timed_entry_point_metrics_failure_does_not_break_script(mock_system_timer):
    mock_system_timer.side_effect = Exception("no metrics for you")
    assert metrics_lib.timed_entry_point(lambda: 3)() == 3
