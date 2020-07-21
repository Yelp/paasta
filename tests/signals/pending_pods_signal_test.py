from decimal import Decimal

import arrow
import mock
import pytest
from kubernetes.client import V1Container
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1Pod
from kubernetes.client import V1PodCondition
from kubernetes.client import V1PodSpec
from kubernetes.client import V1PodStatus
from kubernetes.client import V1ResourceRequirements

from clusterman.kubernetes.util import PodUnschedulableReason
from clusterman.signals.pending_pods_signal import _get_max_resources
from clusterman.signals.pending_pods_signal import _get_resource_request
from clusterman.signals.pending_pods_signal import PendingPodsSignal
from clusterman.util import SignalResourceRequest


@pytest.fixture
def pending_pods_signal():
    return PendingPodsSignal(
        'foo',
        'bar',
        'kube',
        'app1',
        'bar.kube_config',
        mock.Mock(),
        mock.Mock(get_unschedulable_pods=mock.Mock(return_value=[])),
    )


@pytest.fixture
def allocated_metrics():
    return {
        'cpus_allocated': [(Decimal('900'), Decimal('250')), (Decimal('1000'), Decimal('150'))],
        'mem_allocated': [(Decimal('900'), Decimal('1250')), (Decimal('1000'), Decimal('1000'))],
        'disk_allocated': [(Decimal('900'), Decimal('600')), (Decimal('1000'), Decimal('500'))],
        'gpus_allocated': [(Decimal('900'), Decimal('0')), (Decimal('1000'), None)],
    }


@pytest.fixture
def pending_pods():
    return [
        (
            V1Pod(
                metadata=V1ObjectMeta(name='pod1'),
                status=V1PodStatus(
                    phase='Pending',
                    conditions=[
                        V1PodCondition(status='False', type='PodScheduled', reason='Unschedulable')
                    ],
                ),
                spec=V1PodSpec(containers=[
                    V1Container(
                        name='container1',
                        resources=V1ResourceRequirements(requests={'cpu': '1.5', 'memory': '150MB'})
                    ),
                    V1Container(
                        name='container1',
                        resources=V1ResourceRequirements(requests={'cpu': '1.5', 'memory': '350MB'})
                    )
                ]),
            ),
            PodUnschedulableReason.InsufficientResources,
        ),
        (
            V1Pod(
                metadata=V1ObjectMeta(name='pod2'),
                status=V1PodStatus(
                    phase='Pending',
                    conditions=[
                        V1PodCondition(status='False', type='PodScheduled', reason='Unschedulable')
                    ],
                ),
                spec=V1PodSpec(containers=[
                    V1Container(
                        name='container1',
                        resources=V1ResourceRequirements(requests={'cpu': '1.5'})
                    ),
                    V1Container(
                        name='container1',
                        resources=V1ResourceRequirements(requests={'cpu': '1.5', 'mem': '300MB'})
                    )
                ]),
            ),
            PodUnschedulableReason.Unknown,
        )
    ]


def test_get_resource_request_empty():
    assert _get_resource_request(None) == SignalResourceRequest()


def test_get_resource_request_no_pending_pods(allocated_metrics):
    assert _get_resource_request(allocated_metrics) == SignalResourceRequest(
        cpus=150,
        mem=1000,
        disk=500,
    )


def test_get_resource_request_only_pending_pods(pending_pods):
    assert _get_resource_request(None, pending_pods) == SignalResourceRequest(cpus=6, mem=1000, disk=0, gpus=0)


def test_get_resource_request_pending_pods_and_metrics(allocated_metrics, pending_pods):
    assert _get_resource_request(allocated_metrics, pending_pods) == SignalResourceRequest(
        cpus=156,
        mem=2000,
        disk=500,
        gpus=0,
    )


def test_get_max_resources():
    assert _get_max_resources(
        SignalResourceRequest(
            cpus=100,
            mem=50,
        ),
        SignalResourceRequest(
            cpus=20,
            mem=100,
            gpus=1,
        ),
    ) == SignalResourceRequest(
        cpus=100,
        mem=100,
        disk=None,
        gpus=1,
    )


def test_most_recent_values(allocated_metrics, pending_pods_signal):
    with mock.patch(
        'clusterman.signals.pending_pods_signal.get_metrics_for_signal',
        return_value=allocated_metrics,
    ):
        assert pending_pods_signal.evaluate(arrow.get(1234)) == SignalResourceRequest(
            cpus=150,
            mem=1000,
            disk=500,
            gpus=None,
        )


@pytest.mark.parametrize('timestamp', [1234, 2234])
def test_boost_factor(timestamp, pending_pods_signal):
    metrics = {
        'cpus_allocated': [(Decimal('900'), Decimal('250')), (Decimal('1000'), Decimal('150'))],
        'boost_factor|cluster=foo,pool=bar.kube': [(Decimal('950'), Decimal('3'))] + (
            [(Decimal('1500'), Decimal('1'))] if timestamp > 1500 else []
        )
    }
    with mock.patch(
        'clusterman.signals.pending_pods_signal.get_metrics_for_signal', return_value=metrics,
    ):
        expected_cpus = 750 if timestamp < 1500 else 150
        assert pending_pods_signal.evaluate(arrow.get(timestamp)) == SignalResourceRequest(
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
        assert pending_pods_signal.evaluate(arrow.get(1234)) == SignalResourceRequest(
            cpus=None,
            mem=None,
            disk=None,
            gpus=None,
        )
