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
import copy
from collections import defaultdict
from distutils.util import strtobool
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple

import colorlog
import kubernetes
import staticconf
from kubernetes.client.models.v1_node import V1Node as KubernetesNode
from kubernetes.client.models.v1_node_selector_requirement import V1NodeSelectorRequirement
from kubernetes.client.models.v1_node_selector_term import V1NodeSelectorTerm
from kubernetes.client.models.v1_pod import V1Pod as KubernetesPod

from clusterman.interfaces.cluster_connector import ClusterConnector
from clusterman.interfaces.types import AgentMetadata
from clusterman.interfaces.types import AgentState
from clusterman.kubernetes.util import allocated_node_resources
from clusterman.kubernetes.util import CachedCoreV1Api
from clusterman.kubernetes.util import get_node_ip
from clusterman.kubernetes.util import PodUnschedulableReason
from clusterman.kubernetes.util import selector_term_matches_requirement
from clusterman.kubernetes.util import total_node_resources
from clusterman.kubernetes.util import total_pod_resources

logger = colorlog.getLogger(__name__)
KUBERNETES_SCHEDULED_PHASES = {'Pending', 'Running'}


class KubernetesClusterConnector(ClusterConnector):
    SCHEDULER = 'kubernetes'
    _core_api: kubernetes.client.CoreV1Api
    _pods: List[KubernetesPod]
    _prev_nodes_by_ip: Mapping[str, KubernetesNode]
    _nodes_by_ip: Mapping[str, KubernetesNode]
    _pending_pods: List[KubernetesPod]
    _pods_by_ip: Mapping[str, List[KubernetesPod]]

    def __init__(self, cluster: str, pool: Optional[str]) -> None:
        super().__init__(cluster, pool)
        self.kubeconfig_path = staticconf.read_string(f'clusters.{cluster}.kubeconfig_path')
        self._safe_to_evict_annotation = staticconf.read_string(
            f'clusters.{cluster}.pod_safe_to_evict_annotation',
            default='cluster-autoscaler.kubernetes.io/safe-to-evict',
        )
        self._nodes_by_ip = {}

    def reload_state(self) -> None:
        logger.info('Reloading nodes')

        self._core_api = CachedCoreV1Api(self.kubeconfig_path)

        # store the previous _nodes_by_ip for use in get_removed_nodes_before_last_reload()
        self._prev_nodes_by_ip = copy.deepcopy(self._nodes_by_ip)
        self._nodes_by_ip = self._get_nodes_by_ip()
        self._pods_by_ip, self._pending_pods = self._get_pods_by_ip_or_pending()

    def get_num_removed_nodes_before_last_reload(self) -> int:
        previous_nodes = self._prev_nodes_by_ip
        current_nodes = self._nodes_by_ip

        return max(0, len(previous_nodes) - len(current_nodes))

    def get_resource_pending(self, resource_name: str) -> float:
        return getattr(allocated_node_resources([p for p, __ in self.get_unschedulable_pods()]), resource_name)

    def get_resource_allocation(self, resource_name: str) -> float:
        return sum(
            getattr(allocated_node_resources(pod), resource_name)
            for pod in self._pods_by_ip.values()
        )

    def get_resource_total(self, resource_name: str) -> float:
        return sum(
            getattr(total_node_resources(node), resource_name)
            for node in self._nodes_by_ip.values()
        )

    def get_unschedulable_pods(self) -> List[Tuple[KubernetesPod, PodUnschedulableReason]]:
        unschedulable_pods = []
        for pod in self._pending_pods:
            is_unschedulable = False
            if not pod.status or not pod.status.conditions:
                logger.info('No conditions in pod status, skipping')
                continue

            for condition in pod.status.conditions:
                if condition.reason == 'Unschedulable':
                    is_unschedulable = True
            if is_unschedulable:
                unschedulable_pods.append((pod, self._get_pod_unschedulable_reason(pod)))
        return unschedulable_pods

    def _pod_belongs_to_pool(self, pod: KubernetesPod) -> bool:
        # Check if the pod is on a node in the pool -- this should cover most cases
        if pod.status.phase in KUBERNETES_SCHEDULED_PHASES and pod.status.host_ip in self._nodes_by_ip:
            return True

        # Otherwise, check if the node selector matches the pool; we'll only get to either of the
        # following checks if the pod _should_ be running on the cluster, but isn't currently.  (This won't catch things
        # that have a nodeSelector or nodeAffinity for anything other than "pool name", for example, system-level
        # DaemonSets like kiam)
        if pod.spec.node_selector:
            for key, value in pod.spec.node_selector.items():
                if key == self.pool_label_key:
                    return value == self.pool

        # Lastly, check if an affinity rule matches
        selector_requirement = V1NodeSelectorRequirement(
            key=self.pool_label_key, operator='In', values=[self.pool]
        )

        if pod.spec.affinity and pod.spec.affinity.node_affinity:
            node_affinity = pod.spec.affinity.node_affinity
            terms: List[V1NodeSelectorTerm] = []
            if node_affinity.required_during_scheduling_ignored_during_execution:
                terms.extend(node_affinity.required_during_scheduling_ignored_during_execution.node_selector_terms)
            if node_affinity.preferred_during_scheduling_ignored_during_execution:
                terms.extend([
                    term.preference
                    for term in node_affinity.preferred_during_scheduling_ignored_during_execution
                ])
            if selector_term_matches_requirement(terms, selector_requirement):
                return True
        return False

    def _get_pod_unschedulable_reason(self, pod: KubernetesPod) -> PodUnschedulableReason:
        pod_resource_request = total_pod_resources(pod)
        for node_ip, pods_on_node in self._pods_by_ip.items():
            node = self._nodes_by_ip.get(node_ip)
            if node and pod_resource_request < total_node_resources(node) - allocated_node_resources(pods_on_node):
                return PodUnschedulableReason.Unknown

        return PodUnschedulableReason.InsufficientResources

    def _get_agent_metadata(self, node_ip: str) -> AgentMetadata:
        node = self._nodes_by_ip.get(node_ip)
        if not node:
            return AgentMetadata(state=AgentState.ORPHANED)
        return AgentMetadata(
            agent_id=node.metadata.name,
            allocated_resources=allocated_node_resources(self._pods_by_ip[node_ip]),
            is_safe_to_kill=self._is_node_safe_to_kill(node_ip),
            batch_task_count=self._count_batch_tasks(node_ip),
            state=(AgentState.RUNNING if self._pods_by_ip[node_ip] else AgentState.IDLE),
            task_count=len(self._pods_by_ip[node_ip]),
            total_resources=total_node_resources(node),
        )

    def _is_node_safe_to_kill(self, node_ip: str) -> bool:
        for pod in self._pods_by_ip[node_ip]:
            annotations = pod.metadata.annotations or dict()
            pod_safe_to_evict = strtobool(annotations.get(self.safe_to_evict_key, 'true'))
            if not pod_safe_to_evict:
                return False
        return True

    def _get_nodes_by_ip(self) -> Mapping[str, KubernetesNode]:
        pool_label_selector = self.pool_config.read_string('pool_label_key', default='clusterman.com/pool')
        pool_nodes = self._core_api.list_node().items

        return {
            get_node_ip(node): node
            for node in pool_nodes
            if not self.pool or node.metadata.labels.get(pool_label_selector, None) == self.pool
        }

    def _get_pods_by_ip_or_pending(self) -> Tuple[Mapping[str, List[KubernetesPod]], List[KubernetesPod]]:
        pods_by_ip: Mapping[str, List[KubernetesPod]] = defaultdict(list)
        pending_pods: List[KubernetesPod] = []

        all_pods = self._core_api.list_pod_for_all_namespaces().items
        for pod in all_pods:
            if self._pod_belongs_to_pool(pod):
                if pod.status.phase == 'Running':
                    pods_by_ip[pod.status.host_ip].append(pod)
                else:
                    pending_pods.append(pod)
        return pods_by_ip, pending_pods

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

    @property
    def pool_label_key(self):
        return self.pool_config.read_string('pool_label_key', default='clusterman.com/pool')

    @property
    def safe_to_evict_key(self):
        return self.pool_config.read_string('safe_to_evict_key', default='clusterman.com/safe_to_evict')
