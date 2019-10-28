import mock
import pytest
from kubernetes.client import V1Container
from kubernetes.client import V1NodeStatus
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1Pod
from kubernetes.client import V1PodSpec
from kubernetes.client import V1PodStatus
from kubernetes.client import V1ResourceRequirements
from kubernetes.client.models.v1_node import V1Node as KubernetesNode

from clusterman.interfaces.cluster_connector import AgentState
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector


@pytest.fixture
def mock_cluster_connector():
    with mock.patch('clusterman.kubernetes.kubernetes_cluster_connector.kubernetes'), \
            mock.patch('clusterman.kubernetes.kubernetes_cluster_connector.staticconf'):
        mock_cluster_connector = KubernetesClusterConnector('kubernetes-test', 'bar')
        mock_cluster_connector._nodes_by_ip = {
            '10.10.10.1': KubernetesNode(
                metadata=V1ObjectMeta(name='node1'),
                status=V1NodeStatus(
                    allocatable={'cpu': '4', 'gpu': 2},
                    capacity={'cpu': '4', 'gpu': '2'}
                )
            ),
            '10.10.10.2': KubernetesNode(
                metadata=V1ObjectMeta(name='node2'),
                status=V1NodeStatus(
                    allocatable={'cpu': '6.5'},
                    capacity={'cpu': '8'}
                )
            )
        }
        mock_cluster_connector._pods_by_ip = {
            '10.10.10.1': [],
            '10.10.10.2': [
                V1Pod(
                    metadata=V1ObjectMeta(name='pod1'),
                    status=V1PodStatus(phase='Running'),
                    spec=V1PodSpec(containers=[
                           V1Container(
                                name='container1',
                                resources=V1ResourceRequirements(requests={'cpu': '1.5'})
                            )
                        ]
                    )
                ),
            ]
        }
        return mock_cluster_connector


@pytest.mark.parametrize('ip_address,expected_state', [
    (None, AgentState.UNKNOWN),
    ('1.2.3.4', AgentState.ORPHANED),
    ('10.10.10.1', AgentState.IDLE),
    ('10.10.10.2', AgentState.RUNNING),
])
def test_get_agent_metadata(mock_cluster_connector, ip_address, expected_state):
    agent_metadata = mock_cluster_connector.get_agent_metadata(ip_address)
    assert agent_metadata.state == expected_state


def test_allocation(mock_cluster_connector):
    assert mock_cluster_connector.get_resource_allocation('cpus') == 1.5


def test_total_cpus(mock_cluster_connector):
    assert mock_cluster_connector.get_resource_total('cpus') == 12
