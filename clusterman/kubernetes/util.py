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
import os
import socket
from enum import auto
from enum import Enum
from typing import List

import colorlog
import kubernetes
from cachetools import TTLCache
from cachetools.keys import hashkey
from humanfriendly import parse_size
from kubernetes.client.models.v1_node import V1Node as KubernetesNode
from kubernetes.client.models.v1_node_selector_requirement import V1NodeSelectorRequirement
from kubernetes.client.models.v1_node_selector_term import V1NodeSelectorTerm
from kubernetes.client.models.v1_pod import V1Pod as KubernetesPod

from clusterman.util import ClustermanResources


# If a container does not specify a resource request, Kubernetes makes up
# numbers for the purposes of scheduling.  I think it makes the most sense
# to use the same made-up numbers here.
#
# https://github.com/kubernetes/kubernetes/blob/1c11ff7a26c498dc623f060aa30c7c970f3d3eee/pkg/scheduler/util/non_zero.go#L34
DEFAULT_KUBERNETES_CPU_REQUEST = '100m'
DEFAULT_KUBERNETES_MEMORY_REQUEST = '200MB'
DEFAULT_KUBERNETES_DISK_REQUEST = '0'  # Kubernetes doesn't schedule based on disk allocation right now
KUBERNETES_API_CACHE_SIZE = 16
KUBERNETES_API_CACHE_TTL = 60
KUBERNETES_API_CACHE = TTLCache(maxsize=KUBERNETES_API_CACHE_SIZE, ttl=KUBERNETES_API_CACHE_TTL)
logger = colorlog.getLogger(__name__)


class CachedCoreV1Api:
    CACHED_FUNCTION_CALLS = {'list_node', 'list_pod_for_all_namespaces'}

    def __init__(self, kubeconfig_path: str):
        try:
            kubernetes.config.load_kube_config(kubeconfig_path)
        except TypeError:
            error_msg = 'Could not load KUBECONFIG; is this running on Kubernetes master?'
            if 'yelpcorp' in socket.getfqdn():
                error_msg += '\nHint: try using the clusterman-k8s-<clustername> wrapper script!'
            logger.error(error_msg)
            raise

        self._client = kubernetes.client.CoreV1Api()

    def __getattr__(self, attr):
        global KUBERNETES_API_CACHE
        func = getattr(self._client, attr)

        if os.environ.get('KUBE_CACHE_ENABLED', '') and attr in self.CACHED_FUNCTION_CALLS:
            def decorator(f):
                def wrapper(*args, **kwargs):
                    k = hashkey(attr, *args, **kwargs)
                    try:
                        return KUBERNETES_API_CACHE[k]
                    except KeyError:
                        logger.debug(f'Cache miss for the {attr} Kubernetes function call')
                        pass  # the function call hasn't been cached recently
                    v = f(*args, **kwargs)
                    KUBERNETES_API_CACHE[k] = v
                    return v
                return wrapper
            func = decorator(func)

        return func


class ResourceParser:
    @staticmethod
    def cpus(resources):
        resources = resources or {}
        cpu_str = resources.get('cpu', DEFAULT_KUBERNETES_CPU_REQUEST)
        if cpu_str[-1] == 'm':
            return float(cpu_str[:-1]) / 1000
        else:
            return float(cpu_str)

    @staticmethod
    def mem(resources):
        resources = resources or {}
        return parse_size(resources.get('memory', DEFAULT_KUBERNETES_MEMORY_REQUEST)) / 1000000

    @staticmethod
    def disk(resources):
        resources = resources or {}
        return parse_size(resources.get('ephemeral-storage', DEFAULT_KUBERNETES_DISK_REQUEST)) / 1000000

    @staticmethod
    def gpus(resources):
        resources = resources or {}
        try:
            return int(resources.get('nvidia.com/gpu', 0))
        except ValueError:
            # on the off chance kubernetes tries to set this to a non-integer
            return 0


class PodUnschedulableReason(Enum):
    InsufficientResources = auto()
    Unknown = auto()


def allocated_node_resources(pods: List[KubernetesPod]) -> ClustermanResources:
    cpus = mem = disk = gpus = 0
    for pod in pods:
        cpus += sum(ResourceParser.cpus(c.resources.requests) for c in pod.spec.containers)
        mem += sum(ResourceParser.mem(c.resources.requests) for c in pod.spec.containers)
        disk += sum(ResourceParser.disk(c.resources.requests) for c in pod.spec.containers)
        gpus += sum(ResourceParser.gpus(c.resources.requests) for c in pod.spec.containers)

    return ClustermanResources(
        cpus=cpus,
        mem=mem,
        disk=disk,
        gpus=gpus,
    )


def get_node_ip(node: KubernetesNode) -> str:
    for address in node.status.addresses:
        if address.type == 'InternalIP':
            return address.address
    raise ValueError('Kubernetes node {node.metadata.name} has no "InternalIP" address')


def total_node_resources(node: KubernetesNode) -> ClustermanResources:
    return ClustermanResources(
        cpus=ResourceParser.cpus(node.status.allocatable),
        mem=ResourceParser.mem(node.status.allocatable),
        disk=ResourceParser.disk(node.status.allocatable),
        gpus=ResourceParser.gpus(node.status.allocatable),
    )


def total_pod_resources(pod: KubernetesPod) -> ClustermanResources:
    return ClustermanResources(
        cpus=sum(ResourceParser.cpus(c.resources.requests) for c in pod.spec.containers),
        mem=sum(ResourceParser.mem(c.resources.requests) for c in pod.spec.containers),
        disk=sum(ResourceParser.disk(c.resources.requests) for c in pod.spec.containers),
        gpus=sum(ResourceParser.gpus(c.resources.requests) for c in pod.spec.containers),
    )


def selector_term_matches_requirement(
    selector_terms: List[V1NodeSelectorTerm],
    selector_requirement: V1NodeSelectorRequirement,
) -> bool:
    for selector_term in selector_terms:
        if selector_term.match_expressions:
            for match_expression in selector_term.match_expressions:
                if match_expression == selector_requirement:
                    return True
    return False
