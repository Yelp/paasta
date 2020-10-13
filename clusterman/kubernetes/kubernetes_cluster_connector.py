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
import socket
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
from clusterman.kubernetes.util import get_node_ip
from clusterman.kubernetes.util import PodUnschedulableReason
from clusterman.kubernetes.util import total_node_resources
from clusterman.kubernetes.util import total_pod_resources

logger = colorlog.getLogger(__name__)


class KubernetesClusterConnector(ClusterConnector):
    SCHEDULER = 'kubernetes'
    _core_api: kubernetes.client.CoreV1Api
    _pods: List[KubernetesPod]
    _nodes_by_ip: Mapping[str, KubernetesNode]
    _pods_by_ip: Mapping[str, List[KubernetesPod]]

    def __init__(self, cluster: str, pool: Optional[str]) -> None:
        super().__init__(cluster, pool)
        self.kubeconfig_path = f'clusters.{cluster}.kubeconfig_path'
        self._safe_to_evict_annotation = staticconf.read_string(
            f'clusters.{cluster}.pod_safe_to_evict_annotation',
            default='cluster-autoscaler.kubernetes.io/safe-to-evict',
        )

    def reload_state(self) -> None:
        logger.info('Reloading nodes')
        try:
            kubernetes.config.load_kube_config(staticconf.read_string(f'{self.kubeconfig_path}'))
        except TypeError:
            error_msg = 'Could not load KUBECONFIG; is this running on Kubernetes master?'
            if 'yelpcorp' in socket.getfqdn():
                error_msg += '\nHint: try using the clusterman-k8s-<clustername> wrapper script!'
            logger.error(error_msg)
            raise

        self._core_api = kubernetes.client.CoreV1Api()
        self._pods = self._get_all_pods()
        self._nodes_by_ip = self._get_nodes_by_ip()
        self._pods_by_ip = self._get_pods_by_ip()

    def get_resource_pending(self, resource_name: str) -> float:
        return getattr(allocated_node_resources([p for p, __ in self.get_unschedulable_pods()]), resource_name)

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

    def get_unschedulable_pods(self) -> List[Tuple[KubernetesPod, PodUnschedulableReason]]:
        unschedulable_pods = []
        for pod in self._get_pending_pods():
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

    def _selector_term_matches_requirement(
        self,
        selector_term: V1NodeSelectorTerm,
        selector_requirement: V1NodeSelectorRequirement
    ) -> bool:
        if selector_term.match_expressions:
            for match_expression in selector_term.match_expressions:
                if match_expression == selector_requirement:
                    return True
        return False

    def _pod_matches_node_selector_or_affinity(self, pod: KubernetesPod) -> bool:
        if pod.spec.node_selector:
            for key, value in pod.spec.node_selector.items():
                if key == self.pool_label_key:
                    return value == self.pool

        selector_requirement = V1NodeSelectorRequirement(
            key=self.pool_label_key, operator='In', values=[self.pool]
        )

        if pod.spec.affinity and pod.spec.affinity.node_affinity:
            node_affinity = pod.spec.affinity.node_affinity
            if node_affinity.required_during_scheduling_ignored_during_execution:
                node_selector_terms = node_affinity.required_during_scheduling_ignored_during_execution.node_selector_terms  # noqa: E501
                for selector_term in node_selector_terms:
                    if self._selector_term_matches_requirement(selector_term, selector_requirement):
                        return True
            if node_affinity.preferred_during_scheduling_ignored_during_execution:
                for preferred_scheduling_term in node_affinity.preferred_during_scheduling_ignored_during_execution:
                    if self._selector_term_matches_requirement(
                        preferred_scheduling_term.preference,
                        selector_requirement
                    ):
                        return True
        return False

    def _get_pending_pods(self) -> List[KubernetesPod]:
        return [
            pod for pod in self._pods
            if pod.status.phase == 'Pending'
            and self._pod_matches_node_selector_or_affinity(pod)
        ]

    def _get_pod_unschedulable_reason(self, pod: KubernetesPod) -> PodUnschedulableReason:
        pod_resource_request = total_pod_resources(pod)
        for node in self._nodes_by_ip.values():
            if pod_resource_request < total_node_resources(node):
                return PodUnschedulableReason.Unknown

        return PodUnschedulableReason.InsufficientResources

    def set_node_unschedulable(self, node_ip: str):
        try:
            agent_metadata = self._get_agent_metadata(node_ip)
            self._core_api.patch_node(
                name=agent_metadata.agent_id,
                body={'spec': {'unschedulable': True}}
            )
        except Exception as e:
            logger.warning(f'error when unscheduling pod: {e}')

    def evict_pods_on_node(self, node_ip: str):
        pods = self._pods_by_ip[node_ip]
        for pod in pods:
            try:
                self._core_api.create_namespaced_pod_eviction(
                    name=pod.metadata.name,
                    namespace=pod.metadata.namespace,
                    body=kubernetes.client.V1beta1Eviction()
                )
            except Exception as e:
                logger.warning(f'error when evict pod: {e}')

    def delete_node(self, node_ip: str):
        agent_metadata = self._get_agent_metadata(node_ip)
        self._core_api.delete_node(
            name=agent_metadata.agent_id,
            grace_period_seconds=0,
            body=kubernetes.client.V1DeleteOptions()
        )

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
        if self.pool is not None:
            pool_label_selector = self.pool_config.read_string('pool_label_key', default='clusterman.com/pool') \
                                  + '=' + self.pool
            pool_nodes = self._core_api.list_node(label_selector=pool_label_selector).items
        else:
            pool_nodes = self._core_api.list_node().items
        return {
            get_node_ip(node): node
            for node in pool_nodes
        }

    def _get_pods_by_ip(self) -> Mapping[str, List[KubernetesPod]]:
        KUBERNETES_SCHEDULED_PHASES = {'Pending', 'Running'}
        all_pods = self._core_api.list_pod_for_all_namespaces().items
        pods_by_ip: Mapping[str, List[KubernetesPod]] = defaultdict(list)
        for pod in all_pods:
            if pod.status.phase in KUBERNETES_SCHEDULED_PHASES \
                    and pod.status.host_ip in self._nodes_by_ip:
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

    @property
    def pool_label_key(self):
        return self.pool_config.read_string('pool_label_key', default='clusterman.com/pool')

    @property
    def safe_to_evict_key(self):
        return self.pool_config.read_string('safe_to_evict_key', default='clusterman.com/safe_to_evict')
