from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import arrow
import colorlog
from clusterman_metrics import ClustermanMetricsBotoClient
from kubernetes.client.models.v1_pod import V1Pod as KubernetesPod

from clusterman.interfaces.signal import Signal
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector
from clusterman.kubernetes.util import PodUnschedulableReason
from clusterman.kubernetes.util import total_pod_resources
from clusterman.util import ClustermanResources
from clusterman.util import SignalResourceRequest

logger = colorlog.getLogger(__name__)


def _get_resource_request(
    allocated_resources: ClustermanResources,
    pending_pods: Optional[List[Tuple[KubernetesPod, PodUnschedulableReason]]] = None,
) -> SignalResourceRequest:
    """ Given a list of metrics, construct a resource request based on the most recent
    data for allocated and pending pods """

    resource_request = SignalResourceRequest()
    pending_pods = pending_pods or []
    if pending_pods:
        for pod, reason in pending_pods:
            if reason == PodUnschedulableReason.InsufficientResources:
                # This is a temporary measure to try to improve scaling behaviour when Clusterman thinks
                # there are enough resources but no single box can hold a new pod.  The goal is to replace
                # this with a more intelligent solution in the future.
                resource_request += total_pod_resources(pod) * 2

    return resource_request + allocated_resources


class PendingPodsSignal(Signal):
    def __init__(
        self,
        cluster: str,
        pool: str,
        scheduler: str,
        app: str,
        config_namespace: str,
        metrics_client: ClustermanMetricsBotoClient,
        cluster_connector: KubernetesClusterConnector,
    ) -> None:
        super().__init__(self.__class__.__name__, cluster, pool, scheduler, app, config_namespace)
        self.cluster_connector = cluster_connector

    def evaluate(
            self,
            timestamp: arrow.Arrow,
            retry_on_broken_pipe: bool = True,
     ) -> Union[SignalResourceRequest, List[KubernetesPod]]:
        allocated_resources = self.cluster_connector.get_cluster_allocated_resources()
        pending_pods = self.cluster_connector.get_unschedulable_pods()

        # Get the most recent metrics _now_ and when the boost was set (if any) and merge them
        if self.parameters.get('per_pod_resource_requests'):
            return pending_pods
        else:
            return _get_resource_request(allocated_resources, pending_pods)
