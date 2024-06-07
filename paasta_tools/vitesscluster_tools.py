import copy
import logging
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional

import service_configuration_lib

from paasta_tools.kubernetes_tools import sanitised_cr_name
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import LongRunningServiceConfigDict
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_service_instance_config
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import load_v2_deployments_json


log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


KUBERNETES_NAMESPACE = "paasta-vitessclusters"


# Global variables
TOPO_IMPLEMENTATION = "zk2"
TOPO_GLOBAL_ROOT = "/vitess-paasta/global"
SOURCE_DB_HOST = "169.254.255.254"
TABLET_TYPES = ["primary", "migration"]
WEB_PORT = "15000"
GRPC_PORT = "15999"


# Environment variables
VTCTLD_EXTRA_ENV = [
    {
        "name": "WEB_PORT",
        "value": WEB_PORT,
    },
    {
        "name": "GRPC_PORT",
        "value": GRPC_PORT,
    },
    {
        "name": "TOPOLOGY_FLAGS",
        "value": "",
    },
]

VTTABLET_EXTRA_ENV = [
    {
        "name": "SHARD",
        "value": "0",
    },
    {
        "name": "EXTERNAL_DB",
        "value": "1",
    },
    {
        "name": "ROLE",
        "value": "rdonly",
    },
    {
        "name": "WEB_PORT",
        "value": WEB_PORT,
    },
    {
        "name": "GRPC_PORT",
        "value": GRPC_PORT,
    },
    {
        "name": "VAULT_ROLEID",
        "valueFrom": {
            "secretKeyRef": {
                "name": "paasta-vitessclusters-secret-vitess-k8s-vault-vttablet-approle-roleid",
                "key": "vault-vttablet-approle-roleid",
            }
        },
    },
    {
        "name": "VAULT_SECRETID",
        "valueFrom": {
            "secretKeyRef": {
                "name": "paasta-vitessclusters-secret-vitess-k8s-vault-vttablet-approle-secretid",
                "key": "vault-vttablet-approle-secretid",
            }
        },
    },
]

# Vault auth related variables
VTGATE_EXTRA_ENV = [
    {
        "name": "VAULT_ROLEID",
        "valueFrom": {
            "secretKeyRef": {
                "name": "paasta-vitessclusters-secret-vitess-k8s-vault-vtgate-approle-roleid",
                "key": "vault-vtgate-approle-roleid",
            }
        },
    },
    {
        "name": "VAULT_SECRETID",
        "valueFrom": {
            "secretKeyRef": {
                "name": "paasta-vitessclusters-secret-vitess-k8s-vault-vtgate-approle-secretid",
                "key": "vault-vtgate-approle-secretid",
            }
        },
    },
]


# Extra Flags
VTADMIN_EXTRA_FLAGS = {"grpc-allow-reflection": "true"}

VTCTLD_EXTRA_FLAGS = {
    "disable_active_reparents": "true",
    "security_policy": "read-only",
}

VTTABLET_EXTRA_FLAGS = {
    "log_err_stacks": "true",
    "grpc_max_message_size": "134217728",
    "init_tablet_type": "replica",
    "queryserver-config-schema-reload-time": "1800",
    "dba_pool_size": "4",
    "vreplication_heartbeat_update_interval": "60",
    "vreplication_tablet_type": "REPLICA",
    "keep_logs": "72h",
    "enable-lag-throttler": "true",
    "throttle_check_as_check_self": "true",
    "db_charset": "utf8mb4",
    "disable_active_reparents": "true",
}


def get_updated_environment_variables(original_list: List[Dict], update_dict: Dict):
    for env in original_list:
        if env["name"] in update_dict:
            env["value"] = update_dict[env["name"]]
            del update_dict[env["name"]]
    original_list.extend([{"name": k, "value": v} for k, v in update_dict.items()])
    return original_list


def get_affinity_spec(paasta_pool: str) -> Dict[str, Dict[str, List[str]]]:
    spec = {
        "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [
                    {
                        "matchExpressions": [
                            {
                                "key": "yelp.com/pool",
                                "operator": "In",
                                "values": [paasta_pool],
                            }
                        ]
                    }
                ]
            }
        }
    }
    return spec


def get_extra_labels(paasta_pool: str, paasta_cluster: str) -> Dict[str, str]:
    """
    get extra labels to adhere to paasta contract
    """
    extra_labels = {
        "yelp.com/owner": "dre_mysql",
        "paasta.yelp.com/cluster": paasta_cluster,
        "paasta.yelp.com/pool": paasta_pool,
    }
    return extra_labels


def get_extra_env(paasta_cluster: str) -> List[Dict[str, str]]:
    """
    get extra env to adhere to paasta contract
    """
    extra_env = [
        {
            "name": "PAASTA_POD_IP",
            "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}},
        },
        {"name": "PAASTA_CLUSTER", "value": paasta_cluster},
    ]
    return extra_env


def get_cell_config(
    cell: str,
    paasta_pool: str,
    paasta_cluster: str,
    region: str,
    vtgate_resources: Dict[str, str],
) -> Dict[str, str]:
    """
    get vtgate config
    """
    replicas = vtgate_resources.get("replicas", 1)
    requests = vtgate_resources.get("requests", {"cpu": "100m", "memory": "256Mi"})

    vtgate_extra_env = copy.deepcopy(VTGATE_EXTRA_ENV)
    vtgate_extra_env.extend(get_extra_env(paasta_cluster))
    environment_overrides = {
        "VAULT_ADDR": f"https://vault-dre.{region}.yelpcorp.com:8200",
        "VAULT_CACERT": f"/etc/vault/all_cas/acm-privateca-{region}.crt",
    }
    updated_vtgate_extra_env = get_updated_environment_variables(
        vtgate_extra_env, environment_overrides
    )

    config = {
        "name": cell,
        "gateway": {
            "extraFlags": {
                "mysql_auth_server_impl": "vault",
                "mysql_auth_vault_addr": f"https://vault-dre.{region}.yelpcorp.com:8200",
                "mysql_auth_vault_path": "secrets/vitess/vt-gate/vttablet_credentials.json",
                "mysql_auth_vault_tls_ca": f"/etc/vault/all_cas/acm-privateca-{region}.crt",
                "mysql_auth_vault_ttl": "60s",
            },
            "affinity": get_affinity_spec(paasta_pool),
            "extraLabels": get_extra_labels(paasta_pool, paasta_cluster),
            "extraEnv": updated_vtgate_extra_env,
            "replicas": replicas,
            "resources": {
                "requests": requests,
                "limits": requests,
            },
        },
    }
    return config


def get_vitess_dashboard_config(
    cells: List[str],
    paasta_pool: str,
    paasta_cluster: str,
    zk_address: str,
    vtctld_resources: Dict[str, str],
) -> Dict[str, str]:
    """
    get vtctld config
    """
    replicas = vtctld_resources.get("replicas", 1)
    requests = vtctld_resources.get("requests", {"cpu": "100m", "memory": "256Mi"})
    vtctld_extra_env = copy.deepcopy(VTCTLD_EXTRA_ENV)
    vtctld_extra_env.extend(get_extra_env(paasta_cluster))
    environment_overrides = {
        "TOPOLOGY_FLAGS": f"--topo_implementation {TOPO_IMPLEMENTATION} --topo_global_server_address {zk_address} --topo_global_root {TOPO_GLOBAL_ROOT}",
    }
    updated_vtctld_extra_env = get_updated_environment_variables(
        vtctld_extra_env, environment_overrides
    )
    config = {
        "cells": cells,
        "affinity": get_affinity_spec(paasta_pool),
        "extraLabels": get_extra_labels(paasta_pool, paasta_cluster),
        "extraEnv": updated_vtctld_extra_env,
        "extraFlags": VTCTLD_EXTRA_FLAGS,
        "replicas": replicas,
        "resources": {
            "requests": requests,
            "limits": requests,
        },
    }

    return config


def get_vt_admin_config(
    cells: List[str],
    paasta_pool: str,
    paasta_cluster: str,
    vtadmin_resources: Dict[str, str],
) -> Dict[str, str]:
    """
    get vtadmin config
    """
    replicas = vtadmin_resources.get("replicas", 1)
    requests = vtadmin_resources.get("requests", {"cpu": "100m", "memory": "256Mi"})
    config = {
        "cells": cells,
        "apiAddresses": ["http://localhost:15000"],
        "affinity": get_affinity_spec(paasta_pool),
        "extraLabels": get_extra_labels(paasta_pool, paasta_cluster),
        "extraFlags": VTADMIN_EXTRA_FLAGS,
        "extraEnv": get_extra_env(paasta_cluster),
        "replicas": replicas,
        "readOnly": False,
        "apiResources": {
            "requests": requests,
            "limits": requests,
        },
        "webResources": {
            "requests": requests,
            "limits": requests,
        },
    }
    return config


def get_tablet_pool_config(
    cell: str,
    db_name: str,
    keyspace: str,
    port: str,
    paasta_pool: str,
    paasta_cluster: str,
    zk_address: str,
    throttle_query_table: str,
    throttle_metrics_threshold: str,
    tablet_type: str,
    region: str,
    vttablet_resources: Dict[str, str],
) -> Dict[str, str]:
    """
    get vttablet config
    """
    vttablet_extra_flags = VTTABLET_EXTRA_FLAGS.copy()
    flag_overrides = {
        "throttle_metrics_query": f"select max_replication_delay from max_mysql_replication_delay.{throttle_query_table};",
        "throttle_metrics_threshold": throttle_metrics_threshold,
        "enforce-tableacl-config": "true",
        "table-acl-config": f"/etc/vitess_keyspace_acls/acls_for_{db_name}.json",
        "table-acl-config-reload-interval": "60s",
        "queryserver-config-strict-table-acl": "true",
        "db-credentials-server": "vault",
        "db-credentials-vault-addr": f"https://vault-dre.{region}.yelpcorp.com:8200",
        "db-credentials-vault-path": "secrets/vitess/vt-tablet/vttablet_credentials.json",
        "db-credentials-vault-tls-ca": f"/etc/vault/all_cas/acm-privateca-{region}.crt",
        "db-credentials-vault-ttl": "60s",
    }
    vttablet_extra_flags.update(flag_overrides)

    vttablet_extra_env = copy.deepcopy(VTTABLET_EXTRA_ENV)
    vttablet_extra_env.extend(get_extra_env(paasta_cluster))
    environment_overrides = {
        "VAULT_ADDR": f"https://vault-dre.{region}.yelpcorp.com:8200",
        "VAULT_CACERT": f"/etc/vault/all_cas/acm-privateca-{region}.crt",
        "TOPOLOGY_FLAGS": f"--topo_implementation {TOPO_IMPLEMENTATION} --topo_global_server_address ${zk_address} --topo_global_root {TOPO_GLOBAL_ROOT}",
        "CELL_TOPOLOGY_SERVERS": zk_address,
        "DB": db_name,
        "KEYSPACE": keyspace,
    }
    updated_vttablet_extra_env = get_updated_environment_variables(
        vttablet_extra_env, environment_overrides
    )

    if tablet_type == "primary":
        type = "externalmaster"
    else:
        type = "externalreplica"

    replicas = vttablet_resources.get("replicas", 1)
    requests = vttablet_resources.get("requests", {"cpu": "100m", "memory": "256Mi"})

    config = {
        "cell": cell,
        "name": f"{db_name}_{tablet_type}",
        "type": type,
        "affinity": get_affinity_spec(paasta_pool),
        "extraLabels": get_extra_labels(paasta_pool, paasta_cluster),
        "extraEnv": updated_vttablet_extra_env,
        "extraVolumeMounts": [
            {
                "mountPath": "/etc/vault/all_cas",
                "name": "vault-secrets",
                "readOnly": True,
            },
            {
                "mountPath": "/etc/vitess_keyspace_acls",
                "name": "acls",
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
            {"name": "vault-secrets", "hostPath": {"path": "/nail/etc/vault/all_cas"}},
            {
                "name": "acls",
                "hostPath": {"path": "/nail/srv/configs/vitess_keyspace_acls"},
            },
            {"name": "vttablet-fake-credentials", "hostPath": {"path": "/dev/null"}},
            {"name": "keyspace-fake-init-script", "hostPath": {"path": "/dev/null"}},
        ],
        "replicas": replicas,
        "vttablet": {
            "extraFlags": vttablet_extra_flags,
            "resources": {
                "requests": requests,
                "limits": requests,
            },
        },
        "externalDatastore": {
            "database": db_name,
            "host": SOURCE_DB_HOST,
            "port": port,
            "user": "vt_app",
            "credentialsSecret": {
                "key": "/etc/credentials.yaml",
                "volumeName": "vttablet-fake-credentials",
            },
        },
        "dataVolumeClaimTemplate": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "10Gi"}},
            "storageClassName": "ebs-csi-gp3",
        },
    }

    # Add extra pod label to filter
    config["extraLabels"]["tablet_type"] = f"{db_name}_{tablet_type}"

    return config


def get_keyspaces_config(
    cells: List[str],
    keyspaces: List[Dict[str, str]],
    paasta_pool: str,
    paasta_cluster: str,
    zk_address: str,
    region: str,
) -> List[Dict[str, str]]:
    """
    get vitess keyspace config
    """
    config = []

    for keyspace_config in keyspaces:
        keyspace = keyspace_config["keyspace"]
        db_name = keyspace_config["keyspace"]
        cluster = keyspace_config["cluster"]
        vttablet_resources = keyspace_config.get("vttablet_resources", {})

        tablet_pools = []

        mysql_port_mappings = load_system_paasta_config().get_mysql_port_mappings()

        # get vttablets
        for tablet_type in TABLET_TYPES:
            # We don't have migration or reporting tablets in all clusters
            if cluster not in mysql_port_mappings:
                log.error(
                    f"MySQL Cluster {cluster} not found in system paasta config mysql_port_mappings"
                )
            if tablet_type not in mysql_port_mappings[cluster]:
                continue
            port = mysql_port_mappings[cluster][tablet_type]

            # We use migration_replication delay for migration tablets and read_replication_delay for everything else
            # Also throttling threshold for migration tablets is 2 hours, refresh and sanitized primaries at 30 seconds and everything else at 3 seconds
            if tablet_type == "migration":
                throttle_query_table = "migration_replication_delay"
                throttle_metrics_threshold = "7200"
            else:
                throttle_query_table = "read_replication_delay"
                if cluster.startswith("refresh") or cluster.startswith("sanitized"):
                    throttle_metrics_threshold = "30"
                else:
                    throttle_metrics_threshold = "3"

            tablet_pools.extend(
                [
                    get_tablet_pool_config(
                        cell,
                        db_name,
                        keyspace,
                        port,
                        paasta_pool,
                        paasta_cluster,
                        zk_address,
                        throttle_query_table,
                        throttle_metrics_threshold,
                        tablet_type,
                        region,
                        vttablet_resources,
                    )
                    for cell in cells
                ]
            )

        config.append(
            {
                "name": keyspace,
                "durabilityPolicy": "none",
                "turndownPolicy": "Immediate",
                "partitionings": [
                    {
                        "equal": {
                            "parts": 1,
                            "shardTemplate": {
                                "databaseInitScriptSecret": {
                                    "volumeName": "keyspace-fake-init-script",
                                    "key": "/etc/init_db.sql",
                                },
                                "tabletPools": tablet_pools,
                            },
                        }
                    }
                ],
            }
        )

    return config


class VitessDeploymentConfigDict(LongRunningServiceConfigDict, total=False):
    replicas: int


class VitessDeploymentConfig(LongRunningServiceConfig):
    config_dict: VitessDeploymentConfigDict

    config_filename_prefix = "vitesscluster"

    def __init__(
        self,
        service: str,
        cluster: str,
        instance: str,
        config_dict: VitessDeploymentConfigDict,
        branch_dict: Optional[BranchDictV2],
        soa_dir: str = DEFAULT_SOA_DIR,
    ) -> None:

        super().__init__(
            cluster=cluster,  # superregion
            instance=instance,  # host-1
            service=service,  # vitess
            soa_dir=soa_dir,
            config_dict=config_dict,
            branch_dict=branch_dict,
        )

    def get_instances(self, with_limit: bool = True) -> int:
        return self.config_dict.get("replicas", 1)

    def validate(
        self,
        params: List[str] = [
            "cpus",
            "security",
            "dependencies_reference",
            "deploy_group",
        ],
    ) -> List[str]:
        # Use InstanceConfig to validate shared config keys like cpus and mem
        # TODO: add mem back to this list once we fix PAASTA-15582 and
        # move to using the same units as flink/marathon etc.
        error_msgs = super().validate(params=params)

        if error_msgs:
            name = self.get_instance()
            return [f"{name}: {msg}" for msg in error_msgs]
        else:
            return []


def generate_vitess_instance_config(
    instance_config: Dict,
) -> Dict:
    # Generate vitess instance config from yelpsoa config

    cpus = instance_config.get("cpus")
    mem = instance_config.get("mem")
    deploy_group = instance_config.get("deploy_group")
    zk_address = instance_config.get("zk_address")
    paasta_pool = instance_config.get("paasta_pool")
    paasta_cluster = instance_config.get("paasta_cluster")
    cells = instance_config.get("cells")
    keyspaces = instance_config.get("keyspaces")
    region = instance_config.get("region")
    vtgate_resources = instance_config.get("vtgate_resources")
    vtadmin_resources = instance_config.get("vtadmin_resources")
    vtctld_resources = instance_config.get("vtctld_resources")

    vitess_images = load_system_paasta_config().get_vitess_images()

    vitess_instance_config = {
        "namespace": "paasta-vitessclusters",
        "cpus": cpus,
        "mem": mem,
        "min_instances": 1,
        "max_instances": 1,
        "deploy_group": deploy_group,
        "autoscaling": {"setpoint": 0.7},
        "env": {
            "OPERATOR_NAME": "vitess-operator",
            "POD_NAME": "vitess-k8s",
            "PS_OPERATOR_POD_NAME": "vitess-k8s",
            "PS_OPERATOR_POD_NAMESPACE": "paasta-vitessclusters",
            "WATCH_NAMESPACE": "paasta-vitessclusters",
        },
        "healthcheck_grace_period_seconds": 60,
        "healthcheck_mode": "cmd",
        "healthcheck_cmd": "true",
        "images": {
            "vtctld": vitess_images["vtctld_image"],
            "vtadmin": vitess_images["vtadmin_image"],
            "vtgate": vitess_images["vtgate_image"],
            "vttablet": vitess_images["vttablet_image"],
        },
        "globalLockserver": {
            "external": {
                "implementation": TOPO_IMPLEMENTATION,
                "address": zk_address,
                "rootPath": TOPO_GLOBAL_ROOT,
            }
        },
        "cells": [
            get_cell_config(cell, paasta_pool, paasta_cluster, region, vtgate_resources)
            for cell in cells
        ],
        "vitessDashboard": get_vitess_dashboard_config(
            cells, paasta_pool, paasta_cluster, zk_address, vtctld_resources
        ),
        "vtadmin": get_vt_admin_config(
            cells, paasta_pool, paasta_cluster, vtadmin_resources
        ),
        "keyspaces": get_keyspaces_config(
            cells, keyspaces, paasta_pool, paasta_cluster, zk_address, region
        ),
        "updateStrategy": {"type": "Immediate"},
    }
    return vitess_instance_config


def load_vitess_service_instance_configs(
    service: str,
    instance: str,
    instance_type: str,
    cluster: str,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> VitessDeploymentConfigDict:
    general_config = service_configuration_lib.read_service_configuration(
        service, soa_dir=soa_dir
    )
    instance_config = load_service_instance_config(
        service, instance, instance_type, cluster, soa_dir=soa_dir
    )
    vitess_instance_config = generate_vitess_instance_config(instance_config)

    general_config = deep_merge_dictionaries(
        overrides=vitess_instance_config, defaults=general_config
    )
    return general_config


def load_vitess_instance_config(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> VitessDeploymentConfig:
    general_config = load_vitess_service_instance_configs(
        service, instance, "vitesscluster", cluster, soa_dir=soa_dir
    )

    branch_dict: Optional[BranchDictV2] = None
    if load_deployments:
        deployments_json = load_v2_deployments_json(service, soa_dir=soa_dir)
        temp_instance_config = VitessDeploymentConfig(
            service=service,
            cluster=cluster,
            instance=instance,
            config_dict=general_config,
            branch_dict=None,
            soa_dir=soa_dir,
        )
        branch = temp_instance_config.get_branch()
        deploy_group = temp_instance_config.get_deploy_group()
        branch_dict = deployments_json.get_branch_dict(service, branch, deploy_group)

    return VitessDeploymentConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=general_config,
        branch_dict=branch_dict,
        soa_dir=soa_dir,
    )


# TODO: read this from CRD in service configs
def cr_id(service: str, instance: str) -> Mapping[str, str]:
    return dict(
        group="yelp.com",
        version="v1alpha1",
        namespace=KUBERNETES_NAMESPACE,
        plural="vitessclusters",
        name=sanitised_cr_name(service, instance),
    )
