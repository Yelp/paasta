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
import enum
from abc import ABCMeta
from abc import abstractmethod
from typing import NamedTuple
from typing import Optional

import staticconf

from clusterman.config import POOL_NAMESPACE
from clusterman.util import ClustermanResources


class AgentState(enum.Enum):
    IDLE = 'idle'
    ORPHANED = 'orphaned'
    RUNNING = 'running'
    UNKNOWN = 'unknown'


class AgentMetadata(NamedTuple):
    agent_id: str = ''
    allocated_resources: ClustermanResources = ClustermanResources()
    batch_task_count: int = 0
    is_safe_to_kill: bool = True
    state: AgentState = AgentState.UNKNOWN
    task_count: int = 0
    total_resources: ClustermanResources = ClustermanResources()


class ClusterConnector(metaclass=ABCMeta):
    SCHEDULER: str

    def __init__(self, cluster: str, pool: str) -> None:
        self.cluster = cluster
        self.pool = pool
        self.pool_config = staticconf.NamespaceReaders(POOL_NAMESPACE.format(pool=self.pool, scheduler=self.SCHEDULER))

    @abstractmethod
    def reload_state(self) -> None:  # pragma: no cover
        """ Refresh any state that needs to be stored at the start of an autoscaling run """
        pass

    def get_agent_metadata(self, ip_address: Optional[str]) -> AgentMetadata:
        """ Get metadata about a cluster agent given an IP address

        :param ip_address: the IP address of the agent in question; it's possible for this IP value to
            be None, which will return an object with UNKNOWN state.
        :returns: whatever information the cluster connector can determine about the state of the agent
        """
        if not ip_address:
            return AgentMetadata()
        return self._get_agent_metadata(ip_address)

    @abstractmethod
    def get_resource_allocation(self, resource_name: str) -> float:  # pragma: no cover
        """Get the total amount of the given resource currently allocated for this pool.

        :param resource_name: a resource recognized by Clusterman (e.g. 'cpus', 'mem', 'disk')
        :returns: the allocated resources in the cluster for the specified resource
        """
        pass

    @abstractmethod
    def get_resource_total(self, resource_name: str) -> float:  # pragma: no cover
        """Get the total amount of the given resource for this pool.

        :param resource_name: a resource recognized by Clusterman (e.g. 'cpus', 'mem', 'disk')
        :returns: the total resources in the cluster for the specified resource
        """
        pass

    def get_percent_resource_allocation(self, resource_name: str) -> float:
        """Get the overall proportion of the given resource that is in use.

        :param resource_name: a resource recognized by Clusterman (e.g. 'cpus', 'mem', 'disk')
        :returns: the percentage allocated for the specified resource
        """
        total = self.get_resource_total(resource_name)
        used = self.get_resource_allocation(resource_name)
        return used / total if total else 0

    @abstractmethod
    def _get_agent_metadata(self, ip_address: str) -> AgentMetadata:  # pragma: no cover
        pass

    @staticmethod
    def load(cluster: str, pool: str, scheduler: str) -> 'ClusterConnector':
        """ Load the cluster connector for the given cluster and pool """
        if scheduler == 'mesos':
            from clusterman.mesos.mesos_cluster_connector import MesosClusterConnector
            return MesosClusterConnector(cluster, pool)
        elif scheduler == 'kubernetes':
            from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector
            return KubernetesClusterConnector(cluster, pool)
        else:
            raise ValueError(f'Unknown scheduler type: {scheduler}')
