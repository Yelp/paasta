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
from typing import cast
from typing import Dict
from typing import Generator
from typing import Mapping
from typing import MutableMapping
from typing import NamedTuple
from typing import Union

from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.mesos.mesos_cluster_connector import FrameworkState
from clusterman.mesos.mesos_cluster_connector import MesosClusterConnector
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


def _prune_resources_dict(resources_dict: Mapping) -> MutableMapping:
    return {resource: resources_dict[resource] for resource in ('cpus', 'mem', 'disk', 'gpus')}


def _get_framework_metadata_for_frameworks(
    cluster_connector: MesosClusterConnector,
    framework_state: FrameworkState,
) -> Generator[ClusterMetric, None, None]:
    cluster = cluster_connector.cluster
    completed = (framework_state == FrameworkState.COMPLETED)

    for framework in cluster_connector.get_framework_list(framework_state):
        value = _prune_resources_dict(framework['used_resources'])
        value['registered_time'] = int(framework['registered_time'])
        value['unregistered_time'] = int(framework['unregistered_time'])
        value['running_task_count'] = len([
            task for task in framework['tasks'] if task['state'] == 'TASK_RUNNING'
        ])

        dimensions: MutableMapping[str, str]
        dimensions = {field: str(framework[field]) for field in ('name', 'id', 'active')}  # type: ignore
        dimensions['cluster'] = cluster
        dimensions['completed'] = str(completed)

        yield ClusterMetric(metric_name='framework', value=value, dimensions=dimensions)


def generate_framework_metadata(manager: PoolManager) -> Generator[ClusterMetric, None, None]:
    yield from _get_framework_metadata_for_frameworks(
        cast(MesosClusterConnector, manager.cluster_connector),
        FrameworkState.RUNNING,
    )
    yield from _get_framework_metadata_for_frameworks(
        cast(MesosClusterConnector, manager.cluster_connector),
        FrameworkState.COMPLETED,
    )
