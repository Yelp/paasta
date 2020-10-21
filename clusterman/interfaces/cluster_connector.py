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
from typing import Optional
from typing import Set

import staticconf
from kubernetes.client.models.v1_node import V1Node as KubernetesNode

from clusterman.config import POOL_NAMESPACE
from clusterman.interfaces.types import AgentMetadata
from clusterman.util import ClustermanResources


class ClusterConnector(metaclass=ABCMeta):
    SCHEDULER: str

    def __init__(self, cluster: str, pool: Optional[str]) -> None:
        self.cluster = cluster
        self.pool = pool
        self.pool_config = staticconf.NamespaceReaders(POOL_NAMESPACE.format(pool=self.pool, scheduler=self.SCHEDULER))

    @abstractmethod
    def reload_state(self) -> None:  # pragma: no cover
        """ Refresh any state that needs to be stored at the start of an autoscaling run """
        pass

    # def get_removed_nodes_since_last_reload(self) -> Set[KubernetesNode]:
    #     # this is only available in the KubernetesClusterConnector and shouldn't be called otherwise
    #     raise NotImplementedError

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

    def get_cluster_allocated_resources(self) -> ClustermanResources:
        """Get all allocated resources for the cluster"""
        allocated_resources = {
            resource: self.get_resource_allocation(resource)
            for resource in ClustermanResources._fields
        }
        return ClustermanResources(**allocated_resources)

    def get_cluster_total_resources(self) -> ClustermanResources:
        """Get the total available resources for the cluster"""
        total_resources = {
            resource: self.get_resource_total(resource)
            for resource in ClustermanResources._fields
        }
        return ClustermanResources(**total_resources)

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
