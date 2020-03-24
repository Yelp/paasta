import mock
from kubernetes.client import V1DeleteOptions
from pysensu_yelp import Status

from paasta_tools.kubernetes.bin.kubernetes_remove_evicted_pods import (
    evicted_pods_per_service,
)
from paasta_tools.kubernetes.bin.kubernetes_remove_evicted_pods import EvictedPod
from paasta_tools.kubernetes.bin.kubernetes_remove_evicted_pods import get_evicted_pods
from paasta_tools.kubernetes.bin.kubernetes_remove_evicted_pods import get_pod_service
from paasta_tools.kubernetes.bin.kubernetes_remove_evicted_pods import (
    notify_service_owners,
)
from paasta_tools.kubernetes.bin.kubernetes_remove_evicted_pods import remove_pods


def test_get_evicted_pods():
    pod1 = mock.MagicMock(
        status=mock.MagicMock(reason="Evicted", phase="Failed"),
        metadata=mock.MagicMock(),
    )
    pod1.metadata.name = "pod-id-1"

    pod2 = mock.MagicMock(
        status=mock.MagicMock(reason=None, phase="Running"), metadata=mock.MagicMock()
    )
    pod2.metadata.name = "pod-id-2"

    pod3 = mock.MagicMock(
        status=mock.MagicMock(reason=None, phase="Running"), metadata=mock.MagicMock()
    )
    pod3.metadata.name = "pod-id-3"

    evicted_pods = get_evicted_pods([pod1, pod2, pod3])
    assert len(evicted_pods) == 1
    assert evicted_pods[0].metadata.name == "pod-id-1"


def test_get_pod_service():
    pod1 = mock.MagicMock(
        metadata=mock.MagicMock(labels={"paasta.yelp.com/service": "my-service"})
    )

    pod_service = get_pod_service(pod1)
    assert pod_service == "my-service"


def test_notify_service_owners():
    service_map = {
        "service1": [
            EvictedPod("pod1", "namespace1", "Ran out of disk"),
            EvictedPod("pod2", "namespace1", "Ran out of mem"),
        ]
    }
    check_output = "The following pods have been evicted and will be removed from the cluster:\n- pod1: Ran out of disk\n- pod2: Ran out of mem\n"

    with mock.patch(
        "paasta_tools.kubernetes.bin.kubernetes_remove_evicted_pods.send_event",
        autospec=True,
    ) as mock_send_event:
        notify_service_owners(service_map, "/soa_dir", False)
        mock_send_event.assert_called_with(
            "service1",
            "pod-eviction.service1",
            mock.ANY,
            Status.CRITICAL,
            check_output,
            "/soa_dir",
        )


def test_notify_service_ownersi_dry_run():
    service_map = {
        "service1": [
            EvictedPod("pod1", "namespace1", "Ran out of disk"),
            EvictedPod("pod2", "namespace1", "Ran out of mem"),
        ]
    }
    with mock.patch(
        "paasta_tools.kubernetes.bin.kubernetes_remove_evicted_pods.send_event",
        autospec=True,
    ) as mock_send_event, mock.patch(
        "paasta_tools.kubernetes.bin.kubernetes_remove_evicted_pods.log", autospec=True
    ) as mock_logging:
        notify_service_owners(service_map, "/soa_dir", True)
        assert mock_send_event.call_count == 0
        mock_logging.info.assert_called_once_with(
            "Would have notified owners for service service1"
        )


def test_remove_pods():
    service_map = {
        "service1": [
            EvictedPod("pod1", "namespace1", "Ran out of disk"),
            EvictedPod("pod2", "namespace1", "Ran out of mem"),
            EvictedPod("pod3", "namespace1", "Ran out of disk"),
        ]
    }
    mock_client = mock.MagicMock()
    remove_pods(mock_client, service_map, False)
    assert mock_client.core.delete_namespaced_pod.call_count == 2
    assert mock_client.core.delete_namespaced_pod.mock_calls == [
        mock.call(
            "pod1",
            "namespace1",
            body=V1DeleteOptions(),
            grace_period_seconds=0,
            propagation_policy="Background",
        ),
        mock.call(
            "pod2",
            "namespace1",
            body=V1DeleteOptions(),
            grace_period_seconds=0,
            propagation_policy="Background",
        ),
    ]


def test_remove_pods_dry_run():
    service_map = {
        "service1": [
            EvictedPod("pod1", "namespace1", "Ran out of disk"),
            EvictedPod("pod2", "namespace1", "Ran out of mem"),
            EvictedPod("pod3", "namespace1", "Ran out of disk"),
        ]
    }
    mock_client = mock.MagicMock()
    with mock.patch(
        "paasta_tools.kubernetes.bin.kubernetes_remove_evicted_pods.log", autospec=True
    ) as mock_logging:
        remove_pods(mock_client, service_map, True)
        assert mock_client.core.delete_namespaced_pod.call_count == 0
        assert mock_logging.info.mock_calls == [
            mock.call("Would have removed pod pod1"),
            mock.call("Would have removed pod pod2"),
        ]


def test_evicted_pods_per_service():
    pod1 = mock.MagicMock(
        status=mock.MagicMock(
            reason="Evicted", phase="Failed", message="Ran out of disk"
        ),
        metadata=mock.MagicMock(
            labels={"paasta.yelp.com/service": "my-service"}, namespace="namespace1"
        ),
    )
    pod1.metadata.name = "pod-id-1"

    pod2 = mock.MagicMock(
        status=mock.MagicMock(reason=None, phase="Running", message=None),
        metadata=mock.MagicMock(
            labels={"paasta.yelp.com/service": "my-service"}, namespace="namespace1"
        ),
    )
    pod2.metadata.name = "pod-id-2"

    pod3 = mock.MagicMock(
        status=mock.MagicMock(reason=None, phase="Running", message=None),
        metadata=mock.MagicMock(
            labels={"paasta.yelp.com/service": "my-service"}, namespace="namespace1"
        ),
    )
    pod3.metadata.name = "pod-id-3"

    mock_client = mock.MagicMock()
    with mock.patch(
        "paasta_tools.kubernetes.bin.kubernetes_remove_evicted_pods.get_all_pods",
        autospec=True,
    ) as mock_get_all_pods:
        mock_get_all_pods.return_value = [pod1, pod2, pod3]
        evicted_pods = evicted_pods_per_service(mock_client)
        assert evicted_pods == {
            "my-service": [EvictedPod("pod-id-1", "namespace1", "Ran out of disk")]
        }
