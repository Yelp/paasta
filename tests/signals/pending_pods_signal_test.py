import arrow
import mock
import pytest

from clusterman.signals.pending_pods_signal import PendingPodsSignal


@pytest.fixture
def pending_pods_signal():
    return PendingPodsSignal('foo', 'bar', 'kube', 'app1', 'bar.kube_config', mock.Mock())


def test_most_recent_values(pending_pods_signal):
    metrics = {
        'cpus_allocated': [(900, 250), (1000, 150)],
        'mem_allocated': [(900, 1250), (1000, 1000)],
        'disk_allocated': [(900, 600), (1000, 500)],
        'gpus_allocated': [(900, 0), (1000, None)],
        'cpus_pending': [(1000, 543)],
    }
    with mock.patch(
        'clusterman.signals.pending_pods_signal.get_metrics_for_signal', return_value=metrics,
    ):
        assert pending_pods_signal.evaluate(arrow.get(1234)) == dict(cpus=693, mem=1000, disk=500, gpus=None)


@pytest.mark.parametrize('timestamp', [1234, 2234])
def test_boost_factor(timestamp, pending_pods_signal):
    metrics = {
        'cpus_allocated': [(900, 250), (1000, 150)],
        'boost_factor|cluster=foo,pool=bar.kube': [(950, 3)] + ([(1500, 1)] if timestamp > 1500 else [])
    }
    with mock.patch(
        'clusterman.signals.pending_pods_signal.get_metrics_for_signal', return_value=metrics,
    ):
        expected_cpus = 750 if timestamp < 1500 else 150
        assert pending_pods_signal.evaluate(arrow.get(timestamp)) == dict(
            cpus=expected_cpus,
            mem=None,
            disk=None,
            gpus=None,
        )


def test_empty_metric_cache(pending_pods_signal):
    metrics = {'cpus_allocated': [], 'mem_allocated': [], 'disk_allocated': [], 'gpus_allocated': []}
    with mock.patch(
        'clusterman.signals.pending_pods_signal.get_metrics_for_signal', return_value=metrics,
    ):
        assert pending_pods_signal.evaluate(arrow.get(1234)) == dict(cpus=None, mem=None, disk=None, gpus=None)
