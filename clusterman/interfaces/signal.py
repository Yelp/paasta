# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from abc import ABCMeta
from abc import abstractmethod
from collections import defaultdict
from typing import Dict
from typing import List
from typing import Union

import arrow
import staticconf
from clusterman_metrics import APP_METRICS
from clusterman_metrics import ClustermanMetricsBotoClient
from clusterman_metrics import MetricsValuesDict
from clusterman_metrics import SYSTEM_METRICS
from kubernetes.client.models.v1_pod import V1Pod as KubernetesPod
from mypy_extensions import TypedDict

from clusterman.exceptions import MetricsError
from clusterman.exceptions import SignalValidationError
from clusterman.util import get_cluster_dimensions
from clusterman.util import SignalResourceRequest


class MetricsConfigDict(TypedDict):
    name: str
    type: str
    minute_range: int
    regex: bool


class Signal(metaclass=ABCMeta):
    def __init__(
        self,
        name: str,
        cluster: str,
        pool: str,
        scheduler: str,
        app: str,
        config_namespace: str,
    ) -> None:
        """ Create an encapsulation of the Unix sockets via which we communicate with signals

        :param cluster: the name of the cluster this signal is for
        :param pool: the name of the pool this signal is for
        :param app: the name of the application this signal is for
        :param config_namespace: the staticconf namespace we can find the signal config in
        :param metrics_client: the metrics client to use to populate signal metrics
        :param signal_namespace: the namespace in the signals repo to find the signal class
            (if this is None, we default to the app name)
        """
        reader = staticconf.NamespaceReaders(config_namespace)

        self.name = name
        self.cluster: str = cluster
        self.pool: str = pool
        self.scheduler: str = scheduler
        self.app: str = app

        self.period_minutes: int = reader.read_int('autoscale_signal.period_minutes')
        if self.period_minutes <= 0:
            raise SignalValidationError(f'Length of signal period must be positive, got {self.period_minutes}')

        self.parameters: Dict = {
            key: value
            for param_dict in reader.read_list('autoscale_signal.parameters', default=[])
            for (key, value) in param_dict.items()
        }
        # Even if cluster and pool were set in parameters, we override them here
        # as we want to preserve a single source of truth
        self.parameters.update(dict(
            cluster=self.cluster,
            pool=self.pool,
        ))

    @abstractmethod
    def evaluate(
            self,
            timestamp: arrow.Arrow,
            retry_on_broken_pipe: bool = True,
     ) -> Union[SignalResourceRequest, List[KubernetesPod]]:
        """ Compute a signal and return either a single response (representing an aggregate resource request), or a
        list of responses (representing per-pod resource requests)

        :param timestamp: a Unix timestamp to pass to the signal as the "current time"
        :param retry_on_broken_pipe: if the signal socket pipe is broken, restart the signal process and try again
        :returns: a dict of resource_name -> requested resources from the signal
        :raises SignalConnectionError: if the signal connection fails for some reason
        """
        pass


def get_metrics_for_signal(
    cluster: str,
    pool: str,
    scheduler: str,
    app: str,
    metrics_client: ClustermanMetricsBotoClient,
    required_metrics: List[MetricsConfigDict],
    end_time: arrow.Arrow,
) -> MetricsValuesDict:
    """ Get the metrics required for a signal """

    metrics: MetricsValuesDict = defaultdict(list)
    for metric_dict in required_metrics:
        if metric_dict['type'] not in (SYSTEM_METRICS, APP_METRICS):
            raise MetricsError(f"Metrics of type {metric_dict['type']} cannot be queried by signals.")

        # Need to add the cluster/pool to get the right system metrics
        # TODO (CLUSTERMAN-126) this should probably be cluster/pool/app eventually
        # TODO (CLUSTERMAN-446) if a mesos pool and a k8s pool share the same app_name,
        #      APP_METRICS will be used for both
        if metric_dict['type'] == SYSTEM_METRICS:
            dims_list = [get_cluster_dimensions(cluster, pool, scheduler)]
            if scheduler == 'mesos':  # handle old (non-scheduler-aware) metrics
                dims_list.insert(0, get_cluster_dimensions(cluster, pool, None))
        else:
            dims_list = [{}]

        # We only support regex expressions for APP_METRICS
        if 'regex' not in metric_dict:
            metric_dict['regex'] = False

        start_time = end_time.shift(minutes=-metric_dict['minute_range'])
        for dims in dims_list:
            query_results = metrics_client.get_metric_values(
                metric_dict['name'],
                metric_dict['type'],
                start_time.timestamp,
                end_time.timestamp,
                is_regex=metric_dict['regex'],
                extra_dimensions=dims,
                app_identifier=app,
            )
            for metric_name, timeseries in query_results.items():
                metrics[metric_name].extend(timeseries)
                # safeguard; the metrics _should_ already be sorted since we inserted the old
                # (non-scheduler-aware) metrics before the new metrics above, so this should be fast
                metrics[metric_name].sort()
    return metrics
