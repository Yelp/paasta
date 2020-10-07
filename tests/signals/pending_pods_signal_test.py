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
from clusterman.signals.pending_pods_signal import _get_resource_request
from clusterman.signals.pending_pods_signal import PendingPodsSignal
from clusterman.util import ClustermanResources
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
def allocated_resources():
    return ClustermanResources(cpus=150, mem=1000, disk=500, gpus=0)


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


def test_get_resource_request_no_pending_pods(allocated_resources):
    assert _get_resource_request(allocated_resources) == SignalResourceRequest(
        cpus=150,
        mem=1000,
        disk=500,
        gpus=0,
    )


def test_get_resource_request_only_pending_pods(pending_pods):
    assert _get_resource_request(ClustermanResources(), pending_pods) == SignalResourceRequest(
        cpus=6,
        mem=1000,
        disk=0,
        gpus=0,
    )


def test_get_resource_request_pending_pods_and_metrics(allocated_resources, pending_pods):
    assert _get_resource_request(allocated_resources, pending_pods) == SignalResourceRequest(
        cpus=156,
        mem=2000,
        disk=500,
        gpus=0,
    )
