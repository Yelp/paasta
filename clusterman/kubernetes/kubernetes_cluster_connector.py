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
from collections import defaultdict
from distutils.util import strtobool
from typing import List
from typing import Mapping

import colorlog
import kubernetes
import staticconf
from kubernetes.client.models.v1_node import V1Node as KubernetesNode
from kubernetes.client.models.v1_pod import V1Pod as KubernetesPod

from clusterman.interfaces.cluster_connector import AgentMetadata
from clusterman.interfaces.cluster_connector import AgentState
from clusterman.interfaces.cluster_connector import ClusterConnector
from clusterman.kubernetes.util import allocated_node_resources
from clusterman.kubernetes.util import get_node_ip
from clusterman.kubernetes.util import total_node_resources

logger = colorlog.getLogger(__name__)


class KubernetesClusterConnector(ClusterConnector):
    SCHEDULER = 'kubernetes'
    _core_api: kubernetes.client.CoreV1Api
    _pods: List[KubernetesPod]
    _nodes_by_ip: Mapping[str, KubernetesNode]
    _pods_by_ip: Mapping[str, List[KubernetesPod]]

    def __init__(self, cluster: str, pool: str) -> None:
        super().__init__(cluster, pool)
        self.kubeconfig_path = f'clusters.{cluster}.kubeconfig_path'
        self._safe_to_evict_annotation = staticconf.read_string(
            f'clusters.{cluster}.pod_safe_to_evict_annotation',
            default='cluster-autoscaler.kubernetes.io/safe-to-evict',
        )

    def reload_state(self) -> None:
        logger.info('Reloading nodes')
        kubernetes.config.load_kube_config(staticconf.read_string(f'{self.kubeconfig_path}'))
        self._core_api = kubernetes.client.CoreV1Api()
        self._pods = self._get_all_pods()
        self._nodes_by_ip = self._get_nodes_by_ip()
        self._pods_by_ip = self._get_pods_by_ip()

    def get_resource_pending(self, resource_name: str) -> float:
        return getattr(allocated_node_resources(self.get_unschedulable_pods()), resource_name)

    def get_resource_allocation(self, resource_name: str) -> float:
        return sum(
            getattr(allocated_node_resources(self._pods_by_ip[node_ip]), resource_name)
            for node_ip, node in self._nodes_by_ip.items()
        )

    def get_resource_total(self, resource_name: str) -> float:
        return sum(
            getattr(total_node_resources(node), resource_name)
            for node in self._nodes_by_ip.values()
        )

    def get_unschedulable_pods(self) -> List[KubernetesPod]:
        unschedulable_pods = []
        for pod in self._get_pending_pods():
            is_unschedulable = False
            for condition in pod.status.conditions:
                if condition.reason == 'Unschedulable':
                    is_unschedulable = True
            if is_unschedulable:
                unschedulable_pods.append(pod)
        return unschedulable_pods

    def _get_pending_pods(self) -> List[KubernetesPod]:
        pool_label_key = self.pool_config.read_string('pool_label_key', default='clusterman.com/pool')
        node_selector = {pool_label_key: self.pool}
        return [pod for pod in self._pods if pod.spec.node_selector == node_selector and pod.status.phase == 'Pending']

    def _get_agent_metadata(self, node_ip: str) -> AgentMetadata:
        node = self._nodes_by_ip.get(node_ip)
        if not node:
            return AgentMetadata(state=AgentState.ORPHANED)
        return AgentMetadata(
            agent_id=node.metadata.name,
            allocated_resources=allocated_node_resources(self._pods_by_ip[node_ip]),
            batch_task_count=self._count_batch_tasks(node_ip),
            is_safe_to_kill=self._is_node_safe_to_kill(node_ip),
            state=(AgentState.RUNNING if self._pods_by_ip[node_ip] else AgentState.IDLE),
            task_count=len(self._pods_by_ip[node_ip]),
            total_resources=total_node_resources(node),
        )

    def _is_node_safe_to_kill(self, node_ip: str) -> bool:
        safe_to_evict_key = self.pool_config.read_string('safe_to_evict_key', default='clusterman.com/safe_to_evict')
        for pod in self._pods_by_ip[node_ip]:
            pod_safe_to_evict = strtobool(pod.metadata.annotations.get(safe_to_evict_key, 'true'))
            if not pod_safe_to_evict:
                return False
        return True

    def _get_nodes_by_ip(self) -> Mapping[str, KubernetesNode]:
        pool_label_selector = self.pool_config.read_string('pool_label_key', default='clusterman.com/pool') \
            + '=' + self.pool
        pool_nodes = self._core_api.list_node(label_selector=pool_label_selector).items
        return {
            get_node_ip(node): node
            for node in pool_nodes
        }

    def _get_pods_by_ip(self) -> Mapping[str, List[KubernetesPod]]:
        all_pods = self._pods
        pods_by_ip: Mapping[str, List[KubernetesPod]] = defaultdict(list)
        for pod in all_pods:
            if pod.status.phase == 'Running' and pod.status.host_ip in self._nodes_by_ip:
                pods_by_ip[pod.status.host_ip].append(pod)
        return pods_by_ip

    def _get_all_pods(self) -> List[KubernetesPod]:
        all_pods = self._core_api.list_pod_for_all_namespaces().items
        return all_pods

    def _count_batch_tasks(self, node_ip: str) -> int:
        count = 0
        for pod in self._pods_by_ip[node_ip]:
            if pod.metadata.annotations is None:
                continue
            for annotation, value in pod.metadata.annotations.items():
                if annotation == self._safe_to_evict_annotation:
                    count += (not strtobool(value))  # if it's safe to evict, it's NOT a batch task
                    break
        return count
