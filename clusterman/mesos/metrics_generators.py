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
from typing import Dict
from typing import Generator
from typing import Mapping
from typing import NamedTuple
from typing import Union

from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.util import get_cluster_dimensions


SYSTEM_METRICS = {
    'cpus_allocated': lambda manager: manager.cluster_connector.get_resource_allocation('cpus'),
    'mem_allocated': lambda manager: manager.cluster_connector.get_resource_allocation('mem'),
    'disk_allocated': lambda manager: manager.cluster_connector.get_resource_allocation('disk'),
    'gpus_allocated': lambda manager: manager.cluster_connector.get_resource_allocation('gpus'),
}
SIMPLE_METADATA = {
    'cpus_total': lambda manager: manager.cluster_connector.get_resource_total('cpus'),
    'mem_total': lambda manager: manager.cluster_connector.get_resource_total('mem'),
    'disk_total': lambda manager: manager.cluster_connector.get_resource_total('disk'),
    'gpus_total': lambda manager: manager.cluster_connector.get_resource_total('gpus'),
    'target_capacity': lambda manager: manager.target_capacity,
    'fulfilled_capacity': lambda manager: {str(market): value for market,
                                           value in manager.get_market_capacities().items()},
    'non_orphan_fulfilled_capacity': lambda manager: manager.non_orphan_fulfilled_capacity,
}
KUBERNETES_METRICS = {
    'unschedulable_pods': lambda manager: len(manager.cluster_connector.get_unschedulable_pods()),
    'cpus_pending': lambda manager: manager.cluster_connector.get_resource_pending('cpus'),
    'mem_pending': lambda manager: manager.cluster_connector.get_resource_pending('mem'),
    'disk_pending': lambda manager: manager.cluster_connector.get_resource_pending('disk'),
    'gpus_pending': lambda manager: manager.cluster_connector.get_resource_pending('gpus'),
}


class ClusterMetric(NamedTuple):
    metric_name: str
    value: Union[float, Mapping[str, float]]
    dimensions: Dict[str, str]  # clusterman_metrics wants a Dict here


def generate_system_metrics(manager: PoolManager) -> Generator[ClusterMetric, None, None]:
    dimensions = get_cluster_dimensions(manager.cluster, manager.pool, manager.scheduler)
    for metric_name, value_method in SYSTEM_METRICS.items():
        yield ClusterMetric(metric_name, value_method(manager), dimensions=dimensions)


def generate_simple_metadata(manager: PoolManager) -> Generator[ClusterMetric, None, None]:
    dimensions = get_cluster_dimensions(manager.cluster, manager.pool, manager.scheduler)
    for metric_name, value_method in SIMPLE_METADATA.items():
        yield ClusterMetric(metric_name, value_method(manager), dimensions=dimensions)


def generate_kubernetes_metrics(manager: PoolManager) -> Generator[ClusterMetric, None, None]:
    dimensions = get_cluster_dimensions(manager.cluster, manager.pool, manager.scheduler)
    for metric_name, value_method in KUBERNETES_METRICS.items():
        yield ClusterMetric(metric_name, value_method(manager), dimensions=dimensions)
