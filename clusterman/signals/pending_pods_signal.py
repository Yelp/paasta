from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import arrow
import colorlog
from clusterman_metrics import APP_METRICS
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import generate_key_with_dimensions
from clusterman_metrics import SYSTEM_METRICS

from clusterman.interfaces.signal import get_metrics_for_signal
from clusterman.interfaces.signal import MetricsConfigDict
from clusterman.interfaces.signal import Signal
from clusterman.interfaces.signal import SignalResponseDict

logger = colorlog.getLogger(__name__)
RESOURCES = ['cpus', 'mem', 'disk', 'gpus']


def _get_required_metrics(cluster: str, pool: str, scheduler: str, minute_range: int = 240) -> List[MetricsConfigDict]:
    required_metrics = []
    for resource in RESOURCES:
        required_metrics.append(MetricsConfigDict(
            name=f'{resource}_allocated',
            type=SYSTEM_METRICS,
            minute_range=minute_range,
            regex=False,
        ))
        required_metrics.append(MetricsConfigDict(
            name=f'{resource}_pending',
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


def _get_max_resources(*args: SignalResponseDict) -> SignalResponseDict:
    """ given two (or more) resource request dicts, merge them together by taking
    the maximum value from each for each resource.  If no dict specifies anything for that
    resource, set it to None
    """

    return {
        r: max([rdict.get(r) for rdict in args if rdict.get(r) is not None], default=None)
        for r in RESOURCES
    }


def _get_resource_request(metrics: Optional[Dict]) -> SignalResponseDict:
    """ Given a list of metrics, construct a resource request based on the most recent
    data for allocated and pending pods """
    if not metrics:
        return {r: None for r in RESOURCES}

    resource_request = {}
    for resource in RESOURCES:
        allocated = metrics.get(resource + '_allocated', [])
        pending = metrics.get(resource + '_pending', [])
        latest_allocated = max(allocated, default=(None, None))
        latest_pending = max(pending, default=(None, None))

        if latest_allocated[1] is not None and latest_pending[1] is not None:
            resource_request[resource] = latest_allocated[1] + latest_pending[1]
        else:
            # This could be None but that's OK
            resource_request[resource] = latest_allocated[1]
    return resource_request


class PendingPodsSignal(Signal):
    def __init__(
        self,
        cluster: str,
        pool: str,
        scheduler: str,
        app: str,
        config_namespace: str,
        metrics_client: ClustermanMetricsBotoClient
    ) -> None:
        super().__init__(self.__class__.__name__, cluster, pool, scheduler, app, config_namespace)
        self.required_metrics = _get_required_metrics(self.cluster, self.pool, self.scheduler)
        self.metrics_client = metrics_client

    def evaluate(
            self,
            timestamp: arrow.Arrow,
            retry_on_broken_pipe: bool = True,
     ) -> Union[SignalResponseDict, List[SignalResponseDict]]:
        metrics = get_metrics_for_signal(
            self.cluster,
            self.pool,
            self.scheduler,
            self.app,
            self.metrics_client,
            self.required_metrics,
            timestamp,
        )

        # Get the most recent metrics _now_ and when the boost was set (if any) and merge them
        current_resource_request = _get_resource_request(metrics)
        boosted_metrics = self._get_boosted_metrics(metrics)
        boosted_resource_request = _get_resource_request(boosted_metrics)

        return _get_max_resources(current_resource_request, boosted_resource_request)

    def _get_boosted_metrics(self, metrics: Dict) -> Optional[Dict]:
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
            return {
                key: [
                    (t, v * boost_factor)
                    for t, v in values if t < boost_time
                ]
                for key, values in metrics.items()
            }
        except (IndexError, KeyError):
            # No boost factor found in the datastore
            return None
