import mock
import pytest
from kubernetes.client import V1ObjectMeta

from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.vitesscluster_tools import load_vitess_cluster_instance_config
from paasta_tools.vitesscluster_tools import load_vitess_cluster_instance_configs
from paasta_tools.vitesscluster_tools import VitessClusterConfig


CONFIG_DICT = {
    "cells": ["fake_cell"],
    "data": {},
    "dependencies": {},
    "deploy": {"pipeline": [{"step": "fake_deploy_group"}]},
    "deploy_group": "fake_deploy_group",
    "description": "Test Description",
    "external_link": "fake_link",
    "git_url": "git@github.yelpcorp.com:services/vitess-k8s",
    "healthcheck_cmd": "fake_cmd",
    "healthcheck_grace_period_seconds": 60,
    "healthcheck_mode": "cmd",
    "monitoring": {},
    "node_selectors": {"fake_pool": ["fake_pool_value"]},
    "paasta_pool": "fake_pool_value",
    "port": None,
    "smartstack": {},
    "vtadmin_resources": {
        "replicas": 1,
        "requests": {"cpu": "100m", "memory": "256Mi"},
    },
    "vtctld_resources": {"replicas": 1, "requests": {"cpu": "100m", "memory": "256Mi"}},
    "zk_address": "fake_zk_address",
}

MOCK_SYSTEM_PAASTA_CONFIG = SystemPaastaConfig(
    config={
        "superregion_to_region_mapping": {"fake_superregion": "fake_region"},
        "vitess_tablet_pool_type_mapping": {
            "primary": "externalmaster",
            "migration": "externalreplica",
        },
    },
    directory="/fake/config/directory",
)

VITESS_CONFIG = {
    "namespace": "paasta-vitessclusters",
    "cells": [{"gateway": {"replicas": 0}, "name": "fake_cell"}],
    "globalLockserver": {
        "external": {
            "address": "fake_zk_address",
            "implementation": "zk2",
            "rootPath": "/vitess-paasta/global",
        }
    },
    "images": {
        "vtadmin": "docker-paasta.yelpcorp.com:443/vtadmin:v16.0.3",
        "vtctld": "docker-paasta.yelpcorp.com:443/vitess_base:v16.0.3",
        "vtgate": "docker-paasta.yelpcorp.com:443/vitess_base:v16.0.3",
        "vttablet": "docker-paasta.yelpcorp.com:443/vitess_base:v16.0.3",
    },
    "updateStrategy": {"type": "Immediate"},
    "vitessDashboard": {
        "affinity": {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "fake_pool",
                                    "operator": "In",
                                    "values": ["fake_pool_value"],
                                }
                            ]
                        }
                    ]
                }
            }
        },
        "annotations": {
            "paasta.yelp.com/routable_ip": "false",
            "smartstack_registrations": '["fake_service.fake_instance"]',
        },
        "cells": ["fake_cell"],
        "extraEnv": [
            {
                "name": "TOPOLOGY_FLAGS",
                "value": "--topo_implementation zk2 "
                "--topo_global_server_address "
                "fake_zk_address "
                "--topo_global_root "
                "/vitess-paasta/global",
            },
            {"name": "WEB_PORT", "value": "15000"},
            {"name": "GRPC_PORT", "value": "15999"},
        ],
        "extraFlags": {
            "disable_active_reparents": "true",
            "security_policy": "read-only",
        },
        "extraLabels": {},
        "replicas": 1,
        "resources": {
            "limits": {"cpu": "100m", "memory": "256Mi"},
            "requests": {"cpu": "100m", "memory": "256Mi"},
        },
    },
    "vtadmin": {
        "affinity": {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "fake_pool",
                                    "operator": "In",
                                    "values": ["fake_pool_value"],
                                }
                            ]
                        }
                    ]
                }
            }
        },
        "annotations": {
            "paasta.yelp.com/routable_ip": "false",
            "smartstack_registrations": '["fake_service.fake_instance"]',
        },
        "apiAddresses": ["http://localhost:15000"],
        "apiResources": {
            "limits": {"cpu": "100m", "memory": "256Mi"},
            "requests": {"cpu": "100m", "memory": "256Mi"},
        },
        "cells": ["fake_cell"],
        "extraEnv": [],
        "extraFlags": {"grpc-allow-reflection": "true"},
        "extraLabels": {},
        "readOnly": False,
        "replicas": 1,
        "webResources": {
            "limits": {"cpu": "100m", "memory": "256Mi"},
            "requests": {"cpu": "100m", "memory": "256Mi"},
        },
    },
}


@pytest.fixture
def mock_vitess_cluster_config():
    with mock.patch.object(
        VitessClusterConfig, "get_region", return_value="mock_region-devc"
    ), mock.patch.object(
        VitessClusterConfig, "get_container_env", return_value=[]
    ), mock.patch.object(
        VitessClusterConfig, "get_docker_url", return_value="fake_docker_url"
    ), mock.patch.object(
        VitessClusterConfig, "get_cluster", return_value="fake_superregion"
    ), mock.patch.object(
        VitessClusterConfig,
        "get_kubernetes_metadata",
        return_value=V1ObjectMeta(labels={}),
    ):
        yield


@mock.patch(
    "paasta_tools.vitesscluster_tools.load_vitess_cluster_instance_config",
    autospec=True,
)
@mock.patch(
    "paasta_tools.vitesscluster_tools.load_system_paasta_config",
    autospec=True,
)
def test_load_vitess_cluster_instance_configs(
    mock_load_system_paasta_config,
    mock_load_vitess_cluster_instance_config,
    mock_vitess_cluster_config,
):
    mock_load_vitess_cluster_instance_config.return_value = VitessClusterConfig(
        service="fake_service",
        instance="fake_instance",
        cluster="fake_cluster",
        config_dict=CONFIG_DICT,
        branch_dict={},
        soa_dir="fake_soa_dir",
    )
    mock_load_system_paasta_config.return_value = MOCK_SYSTEM_PAASTA_CONFIG
    vitess_cluster_instance_configs = load_vitess_cluster_instance_configs(
        service="fake_service",
        soa_dir="fake_soa_dir",
        cluster="fake_cluster",
        instance="fake_instance",
    )
    assert vitess_cluster_instance_configs == VITESS_CONFIG


@mock.patch("paasta_tools.vitesscluster_tools.load_v2_deployments_json", autospec=True)
@mock.patch(
    "paasta_tools.vitesscluster_tools.load_service_instance_config",
    autospec=True,
)
@mock.patch(
    "paasta_tools.vitesscluster_tools.VitessClusterConfig",
    autospec=True,
)
def test_load_vitess_cluster_instance_config(
    mock_vitess_cluster_config,
    mock_load_cluster_instance_config,
    mock_load_v2_deployments_json,
):
    mock_config = {
        "port": None,
        "monitoring": {},
        "deploy": {},
        "data": {},
        "smartstack": {},
        "dependencies": {},
    }
    vitess_cluster_config = load_vitess_cluster_instance_config(
        service="fake_vitesscluster_service",
        instance="fake_instance",
        cluster="fake_cluster",
        load_deployments=True,
        soa_dir="/foo/bar",
    )
    mock_load_v2_deployments_json.assert_called_with(
        service="fake_vitesscluster_service", soa_dir="/foo/bar"
    )
    mock_vitess_cluster_config.assert_called_with(
        service="fake_vitesscluster_service",
        instance="fake_instance",
        cluster="fake_cluster",
        config_dict=mock_config,
        branch_dict=mock_load_v2_deployments_json.return_value.get_branch_dict(),
        soa_dir="/foo/bar",
    )

    assert vitess_cluster_config == mock_vitess_cluster_config.return_value
