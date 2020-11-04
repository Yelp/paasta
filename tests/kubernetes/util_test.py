import os

import mock
import pytest
from kubernetes.client.models.v1_node_selector_requirement import V1NodeSelectorRequirement
from kubernetes.client.models.v1_node_selector_term import V1NodeSelectorTerm

from clusterman.kubernetes.util import CachedCoreV1Api
from clusterman.kubernetes.util import ResourceParser
from clusterman.kubernetes.util import selector_term_matches_requirement


@pytest.fixture
def mock_cached_core_v1_api():
    with mock.patch('clusterman.kubernetes.util.kubernetes'):
        yield CachedCoreV1Api('/foo/bar/admin.conf')


def test_cached_corev1_api_no_kubeconfig(caplog):
    with pytest.raises(TypeError):
        CachedCoreV1Api('/foo/bar/admin.conf')
        assert 'Could not load KUBECONFIG' in caplog.text


def test_cached_corev1_api_caches_non_cached_function(mock_cached_core_v1_api):
    mock_cached_core_v1_api.list_namespace()
    assert mock_cached_core_v1_api._client.list_namespace.call_count == 1


def test_cached_corev1_api_caches_cached_function_no_env_var(mock_cached_core_v1_api):
    mock_cached_core_v1_api.list_node()
    mock_cached_core_v1_api.list_node()
    assert mock_cached_core_v1_api._client.list_node.call_count == 2


def test_cached_corev1_api_caches_cached_function(mock_cached_core_v1_api):
    with mock.patch.dict(os.environ, {'KUBE_CACHE_ENABLED': 'true'}):
        mock_cached_core_v1_api.list_node()
        mock_cached_core_v1_api.list_node()
    assert mock_cached_core_v1_api._client.list_node.call_count == 1


def test_resource_parser_cpu():
    assert ResourceParser.cpus({'cpu': '2'}) == 2.0
    assert ResourceParser.cpus({'cpu': '500m'}) == 0.5


def test_resource_parser_mem():
    assert ResourceParser.mem({'memory': '1Gi'}) == 1000.0


def test_resource_parser_disk():
    assert ResourceParser.disk({'ephemeral-storage': '1Gi'}) == 1000.0


def test_resource_parser_gpus():
    assert ResourceParser.gpus({'nvidia.com/gpu': '3'}) == 3


def test_resource_parser_gpus_non_integer():
    with pytest.raises(ValueError):
        ResourceParser.gpus({'nvidia.com/gpu': '3.5'})


def test_selector_term_matches_requirement():
    selector_term = [V1NodeSelectorTerm(
        match_expressions=[
            V1NodeSelectorRequirement(
                key='clusterman.com/scheduler',
                operator='Exists'
            ),
            V1NodeSelectorRequirement(
                key='clusterman.com/pool',
                operator='In',
                values=['bar']
            )
        ]
    )]
    selector_requirement = V1NodeSelectorRequirement(
        key='clusterman.com/pool',
        operator='In',
        values=['bar']
    )
    assert selector_term_matches_requirement(selector_term, selector_requirement)
