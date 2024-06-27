import mock
import pytest
from kubernetes.client import V1ObjectMeta

from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.vitesscluster_tools import load_vitess_instance_config
from paasta_tools.vitesscluster_tools import load_vitess_service_instance_configs
from paasta_tools.vitesscluster_tools import VitessDeploymentConfig


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
    "keyspaces": [
        {
            "cluster": "fake_cluster",
            "keyspace": "fake_keyspaces",
            "vttablet_resources": {
                "replicas": 1,
                "requests": {"cpu": "100m", "memory": "256Mi"},
            },
        }
    ],
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
    "vtgate_resources": {"replicas": 1, "requests": {"cpu": "100m", "memory": "256Mi"}},
    "zk_address": "fake_zk_address",
}

MOCK_SYSTEM_PAASTA_CONFIG = SystemPaastaConfig(
    config={
        "superregion_to_region_mapping": {"fake_superregion": "fake_region"},
        "mysql_port_mappings": {
            "fake_cluster": {
                "primary": 1234,
                "migration": 1234,
                "read": 1234,
                "reporting": 1234,
            },
        },
        "vitess_tablet_pool_type_mapping": {
            "primary": "externalmaster",
            "migration": "externalreplica",
        },
    },
    directory="/fake/config/directory",
)

VITESS_CONFIG = {
    "namespace": "paasta-vitessclusters",
    "cells": [
        {
            "gateway": {
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
                "extraEnv": [
                    {
                        "name": "VAULT_ADDR",
                        "value": "https://vault-dre.mock_region.yelpcorp.com:8200",
                    },
                    {
                        "name": "VAULT_CACERT",
                        "value": "/etc/vault/all_cas/acm-privateca-mock_region.crt",
                    },
                    {
                        "name": "VAULT_ROLEID",
                        "valueFrom": {
                            "secretKeyRef": {
                                "key": "vault-vtgate-approle-roleid",
                                "name": "paasta-vitessclusters-secret-vitess-k8s-vault-vtgate-approle-roleid",
                            }
                        },
                    },
                    {
                        "name": "VAULT_SECRETID",
                        "valueFrom": {
                            "secretKeyRef": {
                                "key": "vault-vtgate-approle-secretid",
                                "name": "paasta-vitessclusters-secret-vitess-k8s-vault-vtgate-approle-secretid",
                            }
                        },
                    },
                ],
                "extraFlags": {
                    "mysql_auth_server_impl": "vault",
                    "mysql_auth_vault_addr": "https://vault-dre.mock_region.yelpcorp.com:8200",
                    "mysql_auth_vault_path": "secrets/vitess/vt-gate/vttablet_credentials.json",
                    "mysql_auth_vault_tls_ca": "/etc/vault/all_cas/acm-privateca-mock_region.crt",
                    "mysql_auth_vault_ttl": "60s",
                },
                "extraLabels": {"tablet_type": "fake_keyspaces_migration"},
                "replicas": 1,
                "resources": {
                    "limits": {"cpu": "100m", "memory": "256Mi"},
                    "requests": {"cpu": "100m", "memory": "256Mi"},
                },
            },
            "name": "fake_cell",
        }
    ],
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
    "keyspaces": [
        {
            "durabilityPolicy": "none",
            "name": "fake_keyspaces",
            "partitionings": [
                {
                    "equal": {
                        "parts": 1,
                        "shardTemplate": {
                            "databaseInitScriptSecret": {
                                "key": "/etc/init_db.sql",
                                "volumeName": "keyspace-fake-init-script",
                            },
                            "tabletPools": [
                                {
                                    "affinity": {
                                        "nodeAffinity": {
                                            "requiredDuringSchedulingIgnoredDuringExecution": {
                                                "nodeSelectorTerms": [
                                                    {
                                                        "matchExpressions": [
                                                            {
                                                                "key": "fake_pool",
                                                                "operator": "In",
                                                                "values": [
                                                                    "fake_pool_value"
                                                                ],
                                                            }
                                                        ]
                                                    }
                                                ]
                                            }
                                        }
                                    },
                                    "cell": "fake_cell",
                                    "dataVolumeClaimTemplate": {
                                        "accessModes": ["ReadWriteOnce"],
                                        "resources": {"requests": {"storage": "10Gi"}},
                                        "storageClassName": "ebs-csi-gp3",
                                    },
                                    "externalDatastore": {
                                        "credentialsSecret": {
                                            "key": "/etc/credentials.yaml",
                                            "volumeName": "vttablet-fake-credentials",
                                        },
                                        "database": "fake_keyspaces",
                                        "host": "169.254.255.254",
                                        "port": 1234,
                                        "user": "vt_app",
                                    },
                                    "extraEnv": [
                                        {
                                            "name": "VAULT_ADDR",
                                            "value": "https://vault-dre.mock_region.yelpcorp.com:8200",
                                        },
                                        {
                                            "name": "VAULT_CACERT",
                                            "value": "/etc/vault/all_cas/acm-privateca-mock_region.crt",
                                        },
                                        {
                                            "name": "TOPOLOGY_FLAGS",
                                            "value": "--topo_implementation "
                                            "zk2 "
                                            "--topo_global_server_address "
                                            "$fake_zk_address "
                                            "--topo_global_root "
                                            "/vitess-paasta/global",
                                        },
                                        {
                                            "name": "CELL_TOPOLOGY_SERVERS",
                                            "value": "fake_zk_address",
                                        },
                                        {"name": "DB", "value": "fake_keyspaces"},
                                        {"name": "KEYSPACE", "value": "fake_keyspaces"},
                                        {"name": "WEB_PORT", "value": "15000"},
                                        {"name": "GRPC_PORT", "value": "15999"},
                                        {"name": "SHARD", "value": "0"},
                                        {"name": "EXTERNAL_DB", "value": "1"},
                                        {"name": "ROLE", "value": "replica"},
                                        {
                                            "name": "VAULT_ROLEID",
                                            "valueFrom": {
                                                "secretKeyRef": {
                                                    "key": "vault-vttablet-approle-roleid",
                                                    "name": "paasta-vitessclusters-secret-vitess-k8s-vault-vttablet-approle-roleid",
                                                }
                                            },
                                        },
                                        {
                                            "name": "VAULT_SECRETID",
                                            "valueFrom": {
                                                "secretKeyRef": {
                                                    "key": "vault-vttablet-approle-secretid",
                                                    "name": "paasta-vitessclusters-secret-vitess-k8s-vault-vttablet-approle-secretid",
                                                }
                                            },
                                        },
                                    ],
                                    "extraLabels": {
                                        "tablet_type": "fake_keyspaces_migration"
                                    },
                                    "extraVolumeMounts": [
                                        {
                                            "mountPath": "/etc/vault/all_cas",
                                            "name": "vault-secrets",
                                            "readOnly": True,
                                        },
                                        {
                                            "mountPath": "/etc/srv",
                                            "name": "srv-configs",
                                            "readOnly": True,
                                        },
                                        {
                                            "mountPath": "etc/credentials.yaml",
                                            "name": "vttablet-fake-credentials",
                                            "readOnly": True,
                                        },
                                        {
                                            "mountPath": "/etc/init_db.sql",
                                            "name": "keyspace-fake-init-script",
                                            "readOnly": True,
                                        },
                                    ],
                                    "extraVolumes": [
                                        {
                                            "hostPath": {
                                                "path": "/nail/etc/vault/all_cas"
                                            },
                                            "name": "vault-secrets",
                                        },
                                        {
                                            "hostPath": {"path": "/nail/srv"},
                                            "name": "srv-configs",
                                        },
                                        {
                                            "hostPath": {"path": "/dev/null"},
                                            "name": "vttablet-fake-credentials",
                                        },
                                        {
                                            "hostPath": {"path": "/dev/null"},
                                            "name": "keyspace-fake-init-script",
                                        },
                                    ],
                                    "name": "fake_keyspaces_primary",
                                    "replicas": 1,
                                    "type": "externalmaster",
                                    "vttablet": {
                                        "extraFlags": {
                                            "db-credentials-server": "vault",
                                            "db-credentials-vault-addr": "https://vault-dre.mock_region.yelpcorp.com:8200",
                                            "db-credentials-vault-path": "secrets/vitess/vt-tablet/vttablet_credentials.json",
                                            "db-credentials-vault-tls-ca": "/etc/vault/all_cas/acm-privateca-mock_region.crt",
                                            "db-credentials-vault-ttl": "60s",
                                            "db_charset": "utf8mb4",
                                            "dba_pool_size": "4",
                                            "disable_active_reparents": "true",
                                            "enable-lag-throttler": "true",
                                            "enforce-tableacl-config": "true",
                                            "grpc_max_message_size": "134217728",
                                            "init_tablet_type": "replica",
                                            "keep_logs": "72h",
                                            "log_err_stacks": "true",
                                            "queryserver-config-schema-reload-time": "1800",
                                            "queryserver-config-strict-table-acl": "true",
                                            "table-acl-config": "/etc/srv/configs/vitess_keyspace_acls/acls_for_fake_keyspaces.json",
                                            "table-acl-config-reload-interval": "60s",
                                            "throttle_check_as_check_self": "true",
                                            "throttle_metrics_query": "select "
                                            "max_replication_delay "
                                            "from "
                                            "max_mysql_replication_delay.read_replication_delay;",
                                            "throttle_metrics_threshold": "3",
                                            "vreplication_heartbeat_update_interval": "60",
                                            "vreplication_tablet_type": "REPLICA",
                                        },
                                        "resources": {
                                            "limits": {
                                                "cpu": "100m",
                                                "memory": "256Mi",
                                            },
                                            "requests": {
                                                "cpu": "100m",
                                                "memory": "256Mi",
                                            },
                                        },
                                    },
                                },
                                {
                                    "affinity": {
                                        "nodeAffinity": {
                                            "requiredDuringSchedulingIgnoredDuringExecution": {
                                                "nodeSelectorTerms": [
                                                    {
                                                        "matchExpressions": [
                                                            {
                                                                "key": "fake_pool",
                                                                "operator": "In",
                                                                "values": [
                                                                    "fake_pool_value"
                                                                ],
                                                            }
                                                        ]
                                                    }
                                                ]
                                            }
                                        }
                                    },
                                    "cell": "fake_cell",
                                    "dataVolumeClaimTemplate": {
                                        "accessModes": ["ReadWriteOnce"],
                                        "resources": {"requests": {"storage": "10Gi"}},
                                        "storageClassName": "ebs-csi-gp3",
                                    },
                                    "externalDatastore": {
                                        "credentialsSecret": {
                                            "key": "/etc/credentials.yaml",
                                            "volumeName": "vttablet-fake-credentials",
                                        },
                                        "database": "fake_keyspaces",
                                        "host": "169.254.255.254",
                                        "port": 1234,
                                        "user": "vt_app",
                                    },
                                    "extraEnv": [
                                        {
                                            "name": "VAULT_ADDR",
                                            "value": "https://vault-dre.mock_region.yelpcorp.com:8200",
                                        },
                                        {
                                            "name": "VAULT_CACERT",
                                            "value": "/etc/vault/all_cas/acm-privateca-mock_region.crt",
                                        },
                                        {
                                            "name": "TOPOLOGY_FLAGS",
                                            "value": "--topo_implementation "
                                            "zk2 "
                                            "--topo_global_server_address "
                                            "$fake_zk_address "
                                            "--topo_global_root "
                                            "/vitess-paasta/global",
                                        },
                                        {
                                            "name": "CELL_TOPOLOGY_SERVERS",
                                            "value": "fake_zk_address",
                                        },
                                        {"name": "DB", "value": "fake_keyspaces"},
                                        {"name": "KEYSPACE", "value": "fake_keyspaces"},
                                        {"name": "WEB_PORT", "value": "15000"},
                                        {"name": "GRPC_PORT", "value": "15999"},
                                        {"name": "SHARD", "value": "0"},
                                        {"name": "EXTERNAL_DB", "value": "1"},
                                        {"name": "ROLE", "value": "replica"},
                                        {
                                            "name": "VAULT_ROLEID",
                                            "valueFrom": {
                                                "secretKeyRef": {
                                                    "key": "vault-vttablet-approle-roleid",
                                                    "name": "paasta-vitessclusters-secret-vitess-k8s-vault-vttablet-approle-roleid",
                                                }
                                            },
                                        },
                                        {
                                            "name": "VAULT_SECRETID",
                                            "valueFrom": {
                                                "secretKeyRef": {
                                                    "key": "vault-vttablet-approle-secretid",
                                                    "name": "paasta-vitessclusters-secret-vitess-k8s-vault-vttablet-approle-secretid",
                                                }
                                            },
                                        },
                                    ],
                                    "extraLabels": {
                                        "tablet_type": "fake_keyspaces_migration"
                                    },
                                    "extraVolumeMounts": [
                                        {
                                            "mountPath": "/etc/vault/all_cas",
                                            "name": "vault-secrets",
                                            "readOnly": True,
                                        },
                                        {
                                            "mountPath": "/etc/srv",
                                            "name": "srv-configs",
                                            "readOnly": True,
                                        },
                                        {
                                            "mountPath": "etc/credentials.yaml",
                                            "name": "vttablet-fake-credentials",
                                            "readOnly": True,
                                        },
                                        {
                                            "mountPath": "/etc/init_db.sql",
                                            "name": "keyspace-fake-init-script",
                                            "readOnly": True,
                                        },
                                    ],
                                    "extraVolumes": [
                                        {
                                            "hostPath": {
                                                "path": "/nail/etc/vault/all_cas"
                                            },
                                            "name": "vault-secrets",
                                        },
                                        {
                                            "hostPath": {"path": "/nail/srv"},
                                            "name": "srv-configs",
                                        },
                                        {
                                            "hostPath": {"path": "/dev/null"},
                                            "name": "vttablet-fake-credentials",
                                        },
                                        {
                                            "hostPath": {"path": "/dev/null"},
                                            "name": "keyspace-fake-init-script",
                                        },
                                    ],
                                    "name": "fake_keyspaces_migration",
                                    "replicas": 1,
                                    "type": "externalreplica",
                                    "vttablet": {
                                        "extraFlags": {
                                            "db-credentials-server": "vault",
                                            "db-credentials-vault-addr": "https://vault-dre.mock_region.yelpcorp.com:8200",
                                            "db-credentials-vault-path": "secrets/vitess/vt-tablet/vttablet_credentials.json",
                                            "db-credentials-vault-tls-ca": "/etc/vault/all_cas/acm-privateca-mock_region.crt",
                                            "db-credentials-vault-ttl": "60s",
                                            "db_charset": "utf8mb4",
                                            "dba_pool_size": "4",
                                            "disable_active_reparents": "true",
                                            "enable-lag-throttler": "true",
                                            "enforce-tableacl-config": "true",
                                            "grpc_max_message_size": "134217728",
                                            "init_tablet_type": "replica",
                                            "keep_logs": "72h",
                                            "log_err_stacks": "true",
                                            "queryserver-config-schema-reload-time": "1800",
                                            "queryserver-config-strict-table-acl": "true",
                                            "table-acl-config": "/etc/srv/configs/vitess_keyspace_acls/acls_for_fake_keyspaces.json",
                                            "table-acl-config-reload-interval": "60s",
                                            "throttle_check_as_check_self": "true",
                                            "throttle_metrics_query": "select "
                                            "max_replication_delay "
                                            "from "
                                            "max_mysql_replication_delay.migration_replication_delay;",
                                            "throttle_metrics_threshold": "3",
                                            "vreplication_heartbeat_update_interval": "60",
                                            "vreplication_tablet_type": "REPLICA",
                                        },
                                        "resources": {
                                            "limits": {
                                                "cpu": "100m",
                                                "memory": "256Mi",
                                            },
                                            "requests": {
                                                "cpu": "100m",
                                                "memory": "256Mi",
                                            },
                                        },
                                    },
                                },
                            ],
                        },
                    }
                }
            ],
            "turndownPolicy": "Immediate",
        }
    ],
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
        "extraLabels": {"tablet_type": "fake_keyspaces_migration"},
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
        "apiAddresses": ["http://localhost:15000"],
        "apiResources": {
            "limits": {"cpu": "100m", "memory": "256Mi"},
            "requests": {"cpu": "100m", "memory": "256Mi"},
        },
        "cells": ["fake_cell"],
        "extraEnv": [],
        "extraFlags": {"grpc-allow-reflection": "true"},
        "extraLabels": {"tablet_type": "fake_keyspaces_migration"},
        "readOnly": False,
        "replicas": 1,
        "webResources": {
            "limits": {"cpu": "100m", "memory": "256Mi"},
            "requests": {"cpu": "100m", "memory": "256Mi"},
        },
    },
}


@pytest.fixture
def mock_vitess_deployment_config():
    with mock.patch.object(
        VitessDeploymentConfig, "get_region", return_value="mock_region"
    ), mock.patch.object(
        VitessDeploymentConfig, "get_container_env", return_value=[]
    ), mock.patch.object(
        VitessDeploymentConfig, "get_docker_url", return_value="fake_docker_url"
    ), mock.patch.object(
        VitessDeploymentConfig, "get_cluster", return_value="fake_superregion"
    ), mock.patch.object(
        VitessDeploymentConfig,
        "get_kubernetes_metadata",
        return_value=V1ObjectMeta(labels={}),
    ):
        yield


@mock.patch(
    "paasta_tools.vitesscluster_tools.load_vitess_instance_config",
    autospec=True,
)
@mock.patch(
    "paasta_tools.vitesscluster_tools.load_system_paasta_config",
    autospec=True,
)
def test_load_vitess_service_instance_configs(
    mock_load_system_paasta_config,
    mock_load_vitess_instance_config,
    mock_vitess_deployment_config,
):
    mock_load_vitess_instance_config.return_value = VitessDeploymentConfig(
        service="fake_service",
        instance="fake_instance",
        cluster="fake_cluster",
        config_dict=CONFIG_DICT,
        branch_dict={},
        soa_dir="fake_soa_dir",
    )
    mock_load_system_paasta_config.return_value = MOCK_SYSTEM_PAASTA_CONFIG
    vitess_service_instance_configs = load_vitess_service_instance_configs(
        service="fake_service",
        soa_dir="fake_soa_dir",
        cluster="fake_cluster",
        instance="fake_instance",
    )
    assert vitess_service_instance_configs == VITESS_CONFIG


@mock.patch(
    "paasta_tools.vitesscluster_tools.load_vitess_instance_config",
    autospec=True,
)
@mock.patch(
    "paasta_tools.vitesscluster_tools.load_system_paasta_config",
    autospec=True,
)
def test_load_vitess_service_instance_configs_missing_mysql_cluster_mapping_entry(
    mock_load_system_paasta_config,
    mock_load_vitess_instance_config,
    mock_vitess_deployment_config,
):
    mock_load_vitess_instance_config.return_value = VitessDeploymentConfig(
        service="fake_service",
        instance="fake_instance",
        cluster="fake_cluster",
        config_dict=CONFIG_DICT,
        branch_dict={},
        soa_dir="fake_soa_dir",
    )
    mock_load_system_paasta_config.return_value = SystemPaastaConfig(
        config={},
        directory="/fake/config/directory",
    )
    with pytest.raises(KeyError):
        load_vitess_service_instance_configs(
            service="fake_service",
            soa_dir="fake_soa_dir",
            cluster="fake_cluster",
            instance="fake_instance",
        )


@mock.patch("paasta_tools.vitesscluster_tools.load_v2_deployments_json", autospec=True)
@mock.patch(
    "paasta_tools.vitesscluster_tools.load_service_instance_config",
    autospec=True,
)
@mock.patch(
    "paasta_tools.vitesscluster_tools.VitessDeploymentConfig",
    autospec=True,
)
def test_load_vitess_instance_config(
    mock_vitess_deployment_config,
    mock_load_service_instance_config,
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
    vitess_deployment_config = load_vitess_instance_config(
        service="fake_vitesscluster_service",
        instance="fake_instance",
        cluster="fake_cluster",
        load_deployments=True,
        soa_dir="/foo/bar",
    )
    mock_load_v2_deployments_json.assert_called_with(
        service="fake_vitesscluster_service", soa_dir="/foo/bar"
    )
    mock_vitess_deployment_config.assert_called_with(
        service="fake_vitesscluster_service",
        instance="fake_instance",
        cluster="fake_cluster",
        config_dict=mock_config,
        branch_dict=mock_load_v2_deployments_json.return_value.get_branch_dict(),
        soa_dir="/foo/bar",
    )

    assert vitess_deployment_config == mock_vitess_deployment_config.return_value
