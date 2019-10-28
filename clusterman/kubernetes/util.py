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
from typing import List

from humanfriendly import parse_size
from kubernetes.client.models.v1_node import V1Node as KubernetesNode
from kubernetes.client.models.v1_pod import V1Pod as KubernetesPod

from clusterman.util import ClustermanResources


# If a container does not specify a resource request, Kubernetes makes up
# numbers for the purposes of scheduling.  I think it makes the most sense
# to use the same made-up numbers here.
#
# https://github.com/kubernetes/kubernetes/blob/master/pkg/scheduler/algorithm/priorities/util/non_zero.go
DEFAULT_KUBERNETES_CPU_REQUEST = '100m'
DEFAULT_KUBERNETES_MEMORY_REQUEST = '200MB'
DEFAULT_KUBERNETES_DISK_REQUEST = '0'  # Kubernetes doesn't schedule based on disk allocation right now


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
        return 0  # TODO


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
        cpus=ResourceParser.cpus(node.status.capacity),
        mem=ResourceParser.mem(node.status.capacity),
        disk=ResourceParser.disk(node.status.capacity),
        gpus=ResourceParser.gpus(node.status.capacity),
    )
