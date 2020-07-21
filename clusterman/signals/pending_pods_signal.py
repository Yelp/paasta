from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import arrow
import colorlog
from clusterman_metrics import APP_METRICS
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import generate_key_with_dimensions
from clusterman_metrics import SYSTEM_METRICS
from kubernetes.client.models.v1_pod import V1Pod as KubernetesPod

from clusterman.interfaces.signal import get_metrics_for_signal
from clusterman.interfaces.signal import MetricsConfigDict
from clusterman.interfaces.signal import MetricsValuesDict
from clusterman.interfaces.signal import Signal
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector
from clusterman.kubernetes.util import PodUnschedulableReason
from clusterman.kubernetes.util import total_pod_resources
from clusterman.util import SignalResourceRequest

logger = colorlog.getLogger(__name__)


def _get_required_metrics(cluster: str, pool: str, scheduler: str, minute_range: int = 240) -> List[MetricsConfigDict]:
    required_metrics = []
    for resource in SignalResourceRequest._fields:
        required_metrics.append(MetricsConfigDict(
            name=f'{resource}_allocated',
            type=SYSTEM_METRICS,
            minute_range=minute_range,
            regex=False,
        ))
    required_metrics.append(MetricsConfigDict(
        name=f'boost_factor|cluster={cluster},pool={pool}.{scheduler}',
        type=APP_METRICS,
        minute_range=minute_range,
        regex=False,
    ))
    return required_metrics


def _get_max_allocated_resource(resource: str, metrics: MetricsValuesDict) -> Optional[float]:
    val = max(metrics.get(f'{resource}_allocated', []), default=(None, None))[1]
    return float(val) if val else None


def _get_max_resources(*args: SignalResourceRequest) -> SignalResourceRequest:
    """ given two (or more) resource request dicts, merge them together by taking
    the maximum value from each for each resource.  If no dict specifies anything for that
    resource, set it to None
    """
    return SignalResourceRequest(**{
        r: max([getattr(rdict, r) for rdict in args if getattr(rdict, r) is not None], default=None)
        for r in SignalResourceRequest._fields
    })


def _get_resource_request(
    metrics: Optional[MetricsValuesDict],
    pending_pods: Optional[List[Tuple[KubernetesPod, PodUnschedulableReason]]] = None,
) -> SignalResourceRequest:
    """ Given a list of metrics, construct a resource request based on the most recent
    data for allocated and pending pods """
    resource_request = SignalResourceRequest()
    if not metrics and not pending_pods:
        return resource_request

    pending_pods = pending_pods or []
    if pending_pods:
        for pod, reason in pending_pods:
            if reason == PodUnschedulableReason.InsufficientResources:
                # This is a temporary measure to try to improve scaling behaviour when Clusterman thinks
                # there are enough resources but no single box can hold a new pod.  The goal is to replace
                # this with a more intelligent solution in the future.
                resource_request += total_pod_resources(pod) * 2

    if metrics:
        resource_request += SignalResourceRequest(**{
            r: _get_max_allocated_resource(r, metrics)
            for r in SignalResourceRequest._fields
        })
    return resource_request


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
        self.required_metrics = _get_required_metrics(self.cluster, self.pool, self.scheduler)
        self.metrics_client = metrics_client
        self.cluster_connector = cluster_connector

    def evaluate(
            self,
            timestamp: arrow.Arrow,
            retry_on_broken_pipe: bool = True,
     ) -> Union[SignalResourceRequest, List[SignalResourceRequest]]:
        metrics = get_metrics_for_signal(
            self.cluster,
            self.pool,
            self.scheduler,
            self.app,
            self.metrics_client,
            self.required_metrics,
            timestamp,
        )

        pending_pods = self.cluster_connector.get_unschedulable_pods()

        # Get the most recent metrics _now_ and when the boost was set (if any) and merge them
        current_resource_request = _get_resource_request(metrics, pending_pods)
        boosted_metrics = self._get_boosted_metrics(metrics)
        boosted_resource_request = _get_resource_request(boosted_metrics)

        return _get_max_resources(current_resource_request, boosted_resource_request)

    def _get_boosted_metrics(self, metrics: Dict) -> Optional[MetricsValuesDict]:
        """ Given a list of metrics, check to see if a boost_factor has been set,
        and then apply that boost_factor to the most recent metrics *at the time it was set*
        """
        boost_key = generate_key_with_dimensions(
            'boost_factor',
            {'cluster': self.cluster, 'pool': f'{self.pool}.{self.scheduler}'}
        )
        try:
            boost_time, boost_factor = metrics[boost_key][-1]

            # Get the metrics at the time of the boost
            return MetricsValuesDict(list, {
                key: [
                    (t, v * boost_factor)
                    for t, v in values if t < boost_time
                ]
                for key, values in metrics.items()
            })
        except (IndexError, KeyError):
            # No boost factor found in the datastore
            return None
