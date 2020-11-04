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
from copy import deepcopy

import mock
import pytest
from kubernetes.client import V1Container
from kubernetes.client import V1NodeStatus
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1Pod
from kubernetes.client import V1PodCondition
from kubernetes.client import V1PodSpec
from kubernetes.client import V1PodStatus
from kubernetes.client import V1ResourceRequirements
from kubernetes.client.models.v1_affinity import V1Affinity
from kubernetes.client.models.v1_node import V1Node as KubernetesNode
from kubernetes.client.models.v1_node_address import V1NodeAddress
from kubernetes.client.models.v1_node_affinity import V1NodeAffinity
from kubernetes.client.models.v1_node_selector import V1NodeSelector
from kubernetes.client.models.v1_node_selector_requirement import V1NodeSelectorRequirement
from kubernetes.client.models.v1_node_selector_term import V1NodeSelectorTerm
from kubernetes.client.models.v1_preferred_scheduling_term import V1PreferredSchedulingTerm
from staticconf.testing import PatchConfiguration

from clusterman.interfaces.types import AgentState
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector


@pytest.fixture
def running_pod_1():
    return V1Pod(
        metadata=V1ObjectMeta(name='running_pod_1'),
        status=V1PodStatus(phase='Running', host_ip='10.10.10.2'),
        spec=V1PodSpec(containers=[
               V1Container(
                    name='container1',
                    resources=V1ResourceRequirements(requests={'cpu': '1.5'})
                )
            ],
            node_selector={'clusterman.com/pool': 'bar'}
        )
    )


@pytest.fixture
def running_pod_2():
    return V1Pod(
        metadata=V1ObjectMeta(name='running_pod_2'),
        status=V1PodStatus(phase='Running', host_ip='10.10.10.3'),
        spec=V1PodSpec(containers=[
               V1Container(
                    name='container1',
                    resources=V1ResourceRequirements(requests={'cpu': '1.5'})
                )
            ],
            node_selector={'clusterman.com/pool': 'bar'}
        )
    )


@pytest.fixture
def running_pod_on_nonexistent_node():
    return V1Pod(
        metadata=V1ObjectMeta(name='running_pod_on_nonexistent_node'),
        status=V1PodStatus(phase='Running', host_ip='10.10.10.4'),
        spec=V1PodSpec(containers=[
               V1Container(
                    name='container1',
                    resources=V1ResourceRequirements(requests={'cpu': '1.5'})
                )
            ],
            node_selector={'clusterman.com/pool': 'bar'}
        )
    )


@pytest.fixture
def unevictable_pod():
    return V1Pod(
        metadata=V1ObjectMeta(name='unevictable_pod', annotations={'clusterman.com/safe_to_evict': 'false'}),
        status=V1PodStatus(phase='Running', host_ip='10.10.10.2'),
        spec=V1PodSpec(containers=[
               V1Container(
                    name='container1',
                    resources=V1ResourceRequirements(requests={'cpu': '1.5'})
                )
            ]
        )
    )


@pytest.fixture
def unschedulable_pod():
    return V1Pod(
        metadata=V1ObjectMeta(name='unschedulable_pod', annotations=dict()),
        status=V1PodStatus(
            phase='Pending',
            conditions=[
                V1PodCondition(status='False', type='PodScheduled', reason='Unschedulable')
            ]
        ),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name='container2',
                    resources=V1ResourceRequirements(requests={'cpu': '1.5'})
                )
            ],
            node_selector={'clusterman.com/pool': 'bar'}
        )
    )


@pytest.fixture
def pending_pod():
    return V1Pod(
        metadata=V1ObjectMeta(name='pending_pod', annotations=dict()),
        status=V1PodStatus(
            phase='Pending',
            conditions=None,
        ),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name='container2',
                    resources=V1ResourceRequirements(requests={'cpu': '1.5'})
                )
            ],
            node_selector={'clusterman.com/pool': 'bar'}
        )
    )


@pytest.fixture
def pod_with_required_affinity():
    return V1Pod(
        status=V1PodStatus(
            phase='Pending',
            conditions=[
                V1PodCondition(status='False', type='PodScheduled', reason='Unschedulable')
            ]
        ),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name='container',
                    resources=V1ResourceRequirements(requests={'cpu': '1.5'})
                )
            ],
            affinity=V1Affinity(
                node_affinity=V1NodeAffinity(
                    required_during_scheduling_ignored_during_execution=V1NodeSelector(
                        node_selector_terms=[
                            V1NodeSelectorTerm(
                                match_expressions=[
                                    V1NodeSelectorRequirement(
                                        key='clusterman.com/pool',
                                        operator='In',
                                        values=['bar']
                                    )
                                ]
                            )
                        ]
                    )
                )
            )
        )
    )


@pytest.fixture
def pod_with_preferred_affinity():
    return V1Pod(
        status=V1PodStatus(
            phase='Pending',
            conditions=[
                V1PodCondition(status='False', type='PodScheduled', reason='Unschedulable')
            ]
        ),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name='container',
                    resources=V1ResourceRequirements(requests={'cpu': '1.5'})
                )
            ],
            affinity=V1Affinity(
                node_affinity=V1NodeAffinity(
                    required_during_scheduling_ignored_during_execution=V1NodeSelector(
                        node_selector_terms=[
                            V1NodeSelectorTerm(
                                match_expressions=[
                                    V1NodeSelectorRequirement(
                                        key='clusterman.com/scheduler',
                                        operator='Exists'
                                    )
                                ]
                            )
                        ]
                    ),
                    preferred_during_scheduling_ignored_during_execution=[
                        V1PreferredSchedulingTerm(
                            weight=10,
                            preference=V1NodeSelectorTerm(
                                match_expressions=[
                                    V1NodeSelectorRequirement(
                                        key='clusterman.com/pool',
                                        operator='In',
                                        values=['bar']
                                    )
                                ]
                            )
                        )
                    ]
                )
            )
        )
    )


@pytest.fixture
def mock_cluster_connector(
    running_pod_1,
    running_pod_2,
    running_pod_on_nonexistent_node,
    unevictable_pod,
    unschedulable_pod,
    pending_pod,
):
    with mock.patch(
        'clusterman.kubernetes.kubernetes_cluster_connector.kubernetes',
    ), mock.patch(
        'clusterman.kubernetes.kubernetes_cluster_connector.CachedCoreV1Api',
    ) as mock_core_api, PatchConfiguration(
        {'clusters': {'kubernetes-test': {'kubeconfig_path': '/var/lib/clusterman.conf'}}},
    ):
        mock_core_api.return_value.list_node.return_value.items = [
            KubernetesNode(
                metadata=V1ObjectMeta(name='node1', labels={'clusterman.com/pool': 'bar'}),
                status=V1NodeStatus(
                    allocatable={'cpu': '4', 'gpu': 2},
                    capacity={'cpu': '4', 'gpu': '2'},
                    addresses=[V1NodeAddress(type='InternalIP', address='10.10.10.1')],
                )
            ),
            KubernetesNode(
                metadata=V1ObjectMeta(name='node2', labels={'clusterman.com/pool': 'bar'}),
                status=V1NodeStatus(
                    allocatable={'cpu': '6.5'},
                    capacity={'cpu': '8'},
                    addresses=[V1NodeAddress(type='InternalIP', address='10.10.10.2')],
                )
            ),
            KubernetesNode(
                metadata=V1ObjectMeta(name='node2', labels={'clusterman.com/pool': 'bar'}),
                status=V1NodeStatus(
                    allocatable={'cpu': '1'},
                    capacity={'cpu': '8'},
                    addresses=[V1NodeAddress(type='InternalIP', address='10.10.10.3')],
                )
            ),
        ]
        mock_core_api.return_value.list_pod_for_all_namespaces.return_value.items = [
            running_pod_1,
            running_pod_2,
            running_pod_on_nonexistent_node,
            unevictable_pod,
            unschedulable_pod,
            pending_pod,
        ]
        mock_cluster_connector = KubernetesClusterConnector('kubernetes-test', 'bar')
        mock_cluster_connector.reload_state()
        yield mock_cluster_connector


@pytest.mark.parametrize('ip_address,expected_state', [
    (None, AgentState.UNKNOWN),
    ('1.2.3.4', AgentState.ORPHANED),
    ('10.10.10.1', AgentState.IDLE),
    ('10.10.10.2', AgentState.RUNNING),
])
def test_get_agent_metadata(mock_cluster_connector, ip_address, expected_state):
    agent_metadata = mock_cluster_connector.get_agent_metadata(ip_address)
    assert agent_metadata.is_safe_to_kill == (ip_address != '10.10.10.2')
    assert agent_metadata.state == expected_state


def test_allocation(mock_cluster_connector):
    assert mock_cluster_connector.get_resource_allocation('cpus') == 6.0


def test_total_cpus(mock_cluster_connector):
    assert mock_cluster_connector.get_resource_total('cpus') == 11.5


def test_get_unschedulable_pods(mock_cluster_connector):
    assert len(mock_cluster_connector.get_unschedulable_pods()) == 1


def test_pending_cpus(mock_cluster_connector):
    assert mock_cluster_connector.get_resource_pending('cpus') == 1.5


def test_pod_belongs_to_pool(
    mock_cluster_connector,
    running_pod_1,
    running_pod_2,
    running_pod_on_nonexistent_node,
    unevictable_pod,
    unschedulable_pod,
    pending_pod,
    pod_with_required_affinity,
    pod_with_preferred_affinity,
):
    pod_with_node_selector_elsewhere = deepcopy(pending_pod)
    pod_with_node_selector_elsewhere.spec.node_selector = {'clusterman.com/pool': 'not-bar'}
    pod_with_required_affinity_elsewhere = deepcopy(pod_with_required_affinity)
    pod_with_required_affinity_elsewhere.spec.affinity.node_affinity. \
        required_during_scheduling_ignored_during_execution.node_selector_terms[0].match_expressions[0].values = \
        ['not-bar']
    assert mock_cluster_connector._pod_belongs_to_pool(running_pod_1)
    assert mock_cluster_connector._pod_belongs_to_pool(running_pod_2)
    assert mock_cluster_connector._pod_belongs_to_pool(running_pod_on_nonexistent_node)
    assert mock_cluster_connector._pod_belongs_to_pool(unevictable_pod)
    assert mock_cluster_connector._pod_belongs_to_pool(unschedulable_pod)
    assert mock_cluster_connector._pod_belongs_to_pool(pending_pod)
    assert mock_cluster_connector._pod_belongs_to_pool(pod_with_required_affinity)
    assert mock_cluster_connector._pod_belongs_to_pool(pod_with_preferred_affinity)
    assert not mock_cluster_connector._pod_belongs_to_pool(pod_with_node_selector_elsewhere)
    assert not mock_cluster_connector._pod_belongs_to_pool(pod_with_required_affinity_elsewhere)
