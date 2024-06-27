import logging
import sys
from typing import Any
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import TypedDict
from typing import Union

import service_configuration_lib
from kubernetes.client import ApiClient

from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfigDict
from paasta_tools.kubernetes_tools import limit_size_with_hash
from paasta_tools.kubernetes_tools import sanitised_cr_name
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_git_sha_from_dockerurl
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
WEB_PORT = "15000"
GRPC_PORT = "15999"


# Environment variables
VTCTLD_EXTRA_ENV = {
    "WEB_PORT": WEB_PORT,
    "GRPC_PORT": GRPC_PORT,
}

VTTABLET_EXTRA_ENV = {
    "WEB_PORT": WEB_PORT,
    "GRPC_PORT": GRPC_PORT,
    "SHARD": "0",
    "EXTERNAL_DB": "1",
    "ROLE": "replica",
    "VAULT_ROLEID": {
        "secretKeyRef": {
            "name": "paasta-vitessclusters-secret-vitess-k8s-vault-vttablet-approle-roleid",
            "key": "vault-vttablet-approle-roleid",
        }
    },
    "VAULT_SECRETID": {
        "secretKeyRef": {
            "name": "paasta-vitessclusters-secret-vitess-k8s-vault-vttablet-approle-secretid",
            "key": "vault-vttablet-approle-secretid",
        }
    },
}

VTGATE_EXTRA_ENV = {
    "VAULT_ROLEID": {
        "secretKeyRef": {
            "name": "paasta-vitessclusters-secret-vitess-k8s-vault-vtgate-approle-roleid",
            "key": "vault-vtgate-approle-roleid",
        }
    },
    "VAULT_SECRETID": {
        "secretKeyRef": {
            "name": "paasta-vitessclusters-secret-vitess-k8s-vault-vtgate-approle-secretid",
            "key": "vault-vtgate-approle-secretid",
        }
    },
}


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


class KVEnvVar(TypedDict, total=False):
    name: str
    value: str


class KVEnvVarValueFrom(TypedDict, total=False):
    name: str
    valueFrom: Dict[str, Any]


class RequestsDict(TypedDict, total=False):
    cpu: str
    memory: str
    disk: str


class ResourceConfigDict(TypedDict, total=False):
    replicas: int
    requests: Dict[str, RequestsDict]
    limits: Dict[str, RequestsDict]


class GatewayConfigDict(TypedDict, total=False):
    affinity: Dict[str, Any]
    extraEnv: List[Union[KVEnvVar, KVEnvVarValueFrom]]
    extraFlags: Dict[str, str]
    extraLabels: Dict[str, str]
    replicas: int
    resources: Dict[str, Any]


class CellConfigDict(TypedDict, total=False):
    name: str
    gateway: GatewayConfigDict


class VitessDashboardConfigDict(TypedDict, total=False):
    cells: List[str]
    affinity: Dict[str, Any]
    extraEnv: List[Union[KVEnvVar, KVEnvVarValueFrom]]
    extraFlags: Dict[str, str]
    extraLabels: Dict[str, str]
    replicas: int
    resources: Dict[str, Any]


class VtAdminConfigDict(TypedDict, total=False):
    cells: List[str]
    apiAddresses: List[str]
    affinity: Dict[str, Any]
    extraEnv: List[Union[KVEnvVar, KVEnvVarValueFrom]]
    extraFlags: Dict[str, str]
    extraLabels: Dict[str, str]
    replicas: int
    readOnly: bool
    apiResources: Dict[str, Any]
    webResources: Dict[str, Any]


class VtTabletDict(TypedDict, total=False):
    extraFlags: Dict[str, str]
    resources: Dict[str, Any]


class TabletPoolDict(TypedDict, total=False):
    cell: str
    name: str
    type: str
    affinity: Dict[str, Any]
    extraLabels: Dict[str, str]
    extraEnv: List[Union[KVEnvVar, KVEnvVarValueFrom]]
    extraVolumeMounts: List[Dict[str, Any]]
    extraVolumes: List[Dict[str, Any]]
    replicas: int
    vttablet: VtTabletDict
    externalDatastore: Dict[str, Any]
    dataVolumeClaimTemplate: Dict[str, Any]


class ShardTemplateDict(TypedDict, total=False):
    databaseInitScriptSecret: Dict[str, str]
    tabletPools: List[TabletPoolDict]


class PartitioningValueDict(TypedDict, total=False):
    parts: int
    shardTemplate: ShardTemplateDict


class KeyspaceConfigDict(TypedDict, total=False):
    durabilityPolicy: str
    turndownPolicy: str
    partitionings: List[Dict[str, PartitioningValueDict]]
    name: str


def get_formatted_environment_variables(
    env_vars: Dict[str, Any]
) -> List[Union[KVEnvVar, KVEnvVarValueFrom]]:
    """
    Helper function to take in key value pairs of environment variables and return a list of dicts
    """
    updated_environment_variables: List[Union[KVEnvVar, KVEnvVarValueFrom]] = []
    for env_key, env_value in env_vars.items():
        if isinstance(env_value, str):
            updated_environment_variables.append(
                KVEnvVar(name=env_key, value=env_value)
            )
        elif isinstance(env_value, dict):
            updated_environment_variables.append(
                KVEnvVarValueFrom(name=env_key, valueFrom=env_value)
            )
        else:
            log.error(f"Invalid environment variable {env_key}={env_value}")
    return updated_environment_variables


def get_cell_config(
    cell: str,
    region: str,
    vtgate_resources: ResourceConfigDict,
    env: List[Union[KVEnvVar, KVEnvVarValueFrom]],
    labels: Dict[str, str],
    node_affinity: dict,
) -> CellConfigDict:
    """
    get vtgate config
    """
    replicas = vtgate_resources.get("replicas")
    requests = vtgate_resources.get(
        "requests", RequestsDict(cpu="100m", memory="256Mi")
    )
    environment_overrides: Dict[str, Any] = {
        "VAULT_ADDR": f"https://vault-dre.{region}.yelpcorp.com:8200",
        "VAULT_CACERT": f"/etc/vault/all_cas/acm-privateca-{region}.crt",
    }
    environment_overrides.update(VTGATE_EXTRA_ENV)
    updated_vtgate_extra_env = (
        get_formatted_environment_variables(environment_overrides) + env
    )

    config = CellConfigDict(
        name=cell,
        gateway=GatewayConfigDict(
            affinity={"nodeAffinity": node_affinity},
            extraEnv=updated_vtgate_extra_env,
            extraFlags={
                "mysql_auth_server_impl": "vault",
                "mysql_auth_vault_addr": f"https://vault-dre.{region}.yelpcorp.com:8200",
                "mysql_auth_vault_path": "secrets/vitess/vt-gate/vttablet_credentials.json",
                "mysql_auth_vault_tls_ca": f"/etc/vault/all_cas/acm-privateca-{region}.crt",
                "mysql_auth_vault_ttl": "60s",
            },
            extraLabels=labels,
            replicas=replicas,
            resources={
                "requests": requests,
                "limits": requests,
            },
        ),
    )
    return config


def get_vitess_dashboard_config(
    cells: List[str],
    zk_address: str,
    vtctld_resources: ResourceConfigDict,
    env: List[Union[KVEnvVar, KVEnvVarValueFrom]],
    labels: Dict[str, str],
    node_affinity: dict,
) -> VitessDashboardConfigDict:
    """
    get vtctld config
    """
    replicas = vtctld_resources.get("replicas")
    requests = vtctld_resources.get(
        "requests", RequestsDict(cpu="100m", memory="256Mi")
    )
    environment_overrides: Dict[str, Any] = {
        "TOPOLOGY_FLAGS": f"--topo_implementation {TOPO_IMPLEMENTATION} --topo_global_server_address {zk_address} --topo_global_root {TOPO_GLOBAL_ROOT}",
    }
    environment_overrides.update(VTCTLD_EXTRA_ENV)
    updated_vtctld_extra_env = (
        get_formatted_environment_variables(environment_overrides) + env
    )

    config = VitessDashboardConfigDict(
        cells=cells,
        affinity={"nodeAffinity": node_affinity},
        extraEnv=updated_vtctld_extra_env,
        extraFlags=VTCTLD_EXTRA_FLAGS,
        extraLabels=labels,
        replicas=replicas,
        resources={
            "requests": requests,
            "limits": requests,
        },
    )
    return config


def get_vt_admin_config(
    cells: List[str],
    vtadmin_resources: ResourceConfigDict,
    env: List[Union[KVEnvVar, KVEnvVarValueFrom]],
    labels: Dict[str, str],
    node_affinity: dict,
) -> VtAdminConfigDict:
    """
    get vtadmin config
    """
    replicas = vtadmin_resources.get("replicas")
    requests = vtadmin_resources.get(
        "requests", RequestsDict(cpu="100m", memory="256Mi")
    )
    config = VtAdminConfigDict(
        cells=cells,
        apiAddresses=["http://localhost:15000"],
        affinity={"nodeAffinity": node_affinity},
        extraLabels=labels,
        extraFlags=VTADMIN_EXTRA_FLAGS,
        extraEnv=env,
        replicas=replicas,
        readOnly=False,
        apiResources={
            "requests": requests,
            "limits": requests,
        },
        webResources={
            "requests": requests,
            "limits": requests,
        },
    )
    return config


def get_tablet_pool_config(
    cell: str,
    db_name: str,
    keyspace: str,
    port: str,
    zk_address: str,
    throttle_query_table: str,
    throttle_metrics_threshold: str,
    tablet_type: str,
    region: str,
    vttablet_resources: ResourceConfigDict,
    env: List[Union[KVEnvVar, KVEnvVarValueFrom]],
    labels: Dict[str, str],
    node_affinity: dict,
) -> TabletPoolDict:
    """
    get vttablet config
    """
    vttablet_extra_flags = VTTABLET_EXTRA_FLAGS.copy()
    flag_overrides = {
        "throttle_metrics_query": f"select max_replication_delay from max_mysql_replication_delay.{throttle_query_table};",
        "throttle_metrics_threshold": throttle_metrics_threshold,
        "enforce-tableacl-config": "true",
        "table-acl-config": f"/nail/srv/configs/vitess_keyspace_acls/acls_for_{db_name}.json",
        "table-acl-config-reload-interval": "60s",
        "queryserver-config-strict-table-acl": "true",
        "db-credentials-server": "vault",
        "db-credentials-vault-addr": f"https://vault-dre.{region}.yelpcorp.com:8200",
        "db-credentials-vault-path": "secrets/vitess/vt-tablet/vttablet_credentials.json",
        "db-credentials-vault-tls-ca": f"/etc/vault/all_cas/acm-privateca-{region}.crt",
        "db-credentials-vault-ttl": "60s",
    }
    vttablet_extra_flags.update(flag_overrides)

    environment_overrides: Dict[str, Any] = {
        "VAULT_ADDR": f"https://vault-dre.{region}.yelpcorp.com:8200",
        "VAULT_CACERT": f"/etc/vault/all_cas/acm-privateca-{region}.crt",
        "TOPOLOGY_FLAGS": f"--topo_implementation {TOPO_IMPLEMENTATION} --topo_global_server_address ${zk_address} --topo_global_root {TOPO_GLOBAL_ROOT}",
        "CELL_TOPOLOGY_SERVERS": zk_address,
        "DB": db_name,
        "KEYSPACE": keyspace,
    }
    environment_overrides.update(VTTABLET_EXTRA_ENV)
    updated_vttablet_extra_env = (
        get_formatted_environment_variables(environment_overrides) + env
    )

    # Add extra pod label to filter
    tablet_type_label = limit_size_with_hash(name=f"{db_name}_{tablet_type}", limit=63)
    labels.update({"tablet_type": tablet_type_label})

    try:
        type = load_system_paasta_config().get_vitess_tablet_pool_type_mapping()[
            tablet_type
        ]
    except KeyError:
        log.error(
            f"Tablet type {tablet_type} not found in system paasta config vitess_tablet_pool_type_mapping"
        )
        type = "externalmaster"

    replicas = vttablet_resources.get("replicas")
    requests = vttablet_resources.get(
        "requests", RequestsDict(cpu="100m", memory="256Mi")
    )

    config = TabletPoolDict(
        cell=cell,
        name=f"{db_name}_{tablet_type}",
        type=type,
        affinity={"nodeAffinity": node_affinity},
        extraLabels=labels,
        extraEnv=updated_vttablet_extra_env,
        extraVolumeMounts=[
            {
                "mountPath": "/etc/vault/all_cas",
                "name": "vault-secrets",
                "readOnly": True,
            },
            {
                "mountPath": "/nail/srv",
                "name": "srv-configs",
                "readOnly": True,
            },
            {
                "mountPath": "/nail/etc/srv-configs",
                "name": "etc-srv-configs",
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
        extraVolumes=[
            {"name": "vault-secrets", "hostPath": {"path": "/nail/etc/vault/all_cas"}},
            {
                "name": "srv-configs",
                "hostPath": {"path": "/nail/srv"},
            },
            {
                "name": "etc-srv-configs",
                "hostPath": {"path": "/nail/etc/srv-configs"},
            },
            {"name": "vttablet-fake-credentials", "hostPath": {"path": "/dev/null"}},
            {"name": "keyspace-fake-init-script", "hostPath": {"path": "/dev/null"}},
        ],
        replicas=replicas,
        vttablet={
            "extraFlags": vttablet_extra_flags,
            "resources": {
                "requests": requests,
                "limits": requests,
            },
        },
        externalDatastore={
            "database": db_name,
            "host": SOURCE_DB_HOST,
            "port": port,
            "user": "vt_app",
            "credentialsSecret": {
                "key": "/etc/credentials.yaml",
                "volumeName": "vttablet-fake-credentials",
            },
        },
        dataVolumeClaimTemplate={
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "10Gi"}},
            "storageClassName": "ebs-csi-gp3",
        },
    )
    return config


def get_keyspaces_config(
    cells: List[str],
    keyspaces: List[Dict[str, Any]],
    zk_address: str,
    region: str,
    env: List[Union[KVEnvVar, KVEnvVarValueFrom]],
    labels: Dict[str, str],
    node_affinity: dict,
) -> List[KeyspaceConfigDict]:
    """
    get vitess keyspace config
    """
    config = []

    for keyspace_config in keyspaces:
        keyspace = keyspace_config["keyspace"]
        db_name = keyspace_config["keyspace"]
        cluster = keyspace_config["cluster"]
        vttablet_resources = keyspace_config.get("vttablet_resources")

        tablet_pools = []

        mysql_port_mappings = load_system_paasta_config().get_mysql_port_mappings()

        # get vttablets
        tablet_types = load_system_paasta_config().get_vitess_tablet_types()
        for tablet_type in tablet_types:
            # We don't have migration or reporting tablets in all clusters
            if cluster not in mysql_port_mappings:
                log.error(
                    f"MySQL Cluster {cluster} not found in system paasta config mysql_port_mappings"
                )
            if tablet_type not in mysql_port_mappings[cluster]:
                continue
            port = mysql_port_mappings[cluster][tablet_type]

            # We use migration_replication delay for migration tablets and read_replication_delay for everything else
            # Also throttling threshold for refresh and sanitized primaries is set at 30 seconds and everything else at 3 seconds
            try:
                throttling_configs = (
                    load_system_paasta_config().get_vitess_throttling_config()
                )
                throttle_query_table = throttling_configs[tablet_type][
                    "throttle_query_table"
                ]
                throttle_metrics_threshold = throttling_configs[tablet_type][
                    "throttle_metrics_threshold"
                ]
            except KeyError:
                log.error(
                    f"Throttling configs for tablet type {tablet_type} not found in system paasta config vitess_throttling_configs"
                )

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
                        zk_address,
                        throttle_query_table,
                        throttle_metrics_threshold,
                        tablet_type,
                        region,
                        vttablet_resources,
                        env,
                        labels,
                        node_affinity,
                    )
                    for cell in cells
                ]
            )
        keyspace_config_value = KeyspaceConfigDict(
            name=keyspace,
            durabilityPolicy="none",
            turndownPolicy="Immediate",
            partitionings=[
                {
                    "equal": PartitioningValueDict(
                        parts=1,
                        shardTemplate=ShardTemplateDict(
                            databaseInitScriptSecret={
                                "volumeName": "keyspace-fake-init-script",
                                "key": "/etc/init_db.sql",
                            },
                            tabletPools=tablet_pools,
                        ),
                    )
                }
            ],
        )
        config.append(keyspace_config_value)
    return config


class VitessDeploymentConfigDict(KubernetesDeploymentConfigDict, total=False):
    images: Dict[str, str]
    cells: List[CellConfigDict]
    vitessDashboard: VitessDashboardConfigDict
    vtadmin: VtAdminConfigDict
    keyspaces: List[KeyspaceConfigDict]
    updateStrategy: Dict[str, str]
    globalLockserver: Dict[str, Dict[str, str]]


class VitessInstanceConfigDict(KubernetesDeploymentConfigDict, total=False):
    cells: List[str]
    zk_address: str
    vtctld_resources: ResourceConfigDict
    vtgate_resources: ResourceConfigDict
    vttablet_resources: ResourceConfigDict
    vtadmin_resources: ResourceConfigDict
    images: Dict[str, str]
    keyspaces: List[Dict[str, Any]]


class VitessDeploymentConfig(KubernetesDeploymentConfig):
    config_dict: VitessInstanceConfigDict

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

    def get_namespace(self) -> str:
        return KUBERNETES_NAMESPACE

    def get_images(self) -> Dict[str, str]:
        vitess_images = self.config_dict.get(
            "images", load_system_paasta_config().get_vitess_images()
        )
        return {
            "vtctld": vitess_images["vtctld_image"],
            "vtadmin": vitess_images["vtadmin_image"],
            "vtgate": vitess_images["vtgate_image"],
            "vttablet": vitess_images["vttablet_image"],
        }

    def get_env_variables(self) -> List[Union[KVEnvVar, KVEnvVarValueFrom]]:
        # get all K8s container env vars and format their keys to camel case

        # Workaround from https://github.com/kubernetes-client/python/issues/390
        api_client = ApiClient()
        env = [
            api_client.sanitize_for_serialization(env)
            for env in self.get_container_env()
        ]
        return env

    def get_labels(self) -> Dict[str, str]:
        # get default labels from parent class to adhere to paasta contract
        docker_url = self.get_docker_url(
            system_paasta_config=load_system_paasta_config()
        )
        git_sha = get_git_sha_from_dockerurl(docker_url)
        labels = self.get_kubernetes_metadata(git_sha=git_sha).labels
        if "yelp.com/owner" in labels.keys():
            labels["yelp.com/owner"] = "dre_mysql"
        return labels

    def get_vitess_node_affinity(self) -> dict:
        # Workaround from https://github.com/kubernetes-client/python/issues/390
        api_client = ApiClient()
        node_affinity = api_client.sanitize_for_serialization(self.get_node_affinity())
        return node_affinity

    def get_region(self) -> str:
        superregion = self.get_cluster()
        superregion_to_region_map = (
            load_system_paasta_config().get_superregion_to_region_mapping()
        )
        region = None
        for superregion_prefix in superregion_to_region_map:
            if superregion.startswith(superregion_prefix):
                region = superregion.replace(
                    superregion_prefix, superregion_to_region_map[superregion_prefix]
                )
        if region is None:
            log.error(
                f"Region not found for superregion {superregion}. Check superregion_to_region_mapping in system paasta config"
            )
            # Exiting early here since region is needed to fetch secrets from vault
            sys.exit(1)
        return region

    def get_global_lock_server(self) -> Dict[str, Dict[str, str]]:
        zk_address = self.config_dict.get("zk_address")
        return {
            "external": {
                "implementation": TOPO_IMPLEMENTATION,
                "address": zk_address,
                "rootPath": TOPO_GLOBAL_ROOT,
            }
        }

    def get_cells(self) -> List[CellConfigDict]:
        cells = self.config_dict.get("cells")
        region = self.get_region()
        vtgate_resources = self.config_dict.get("vtgate_resources")

        formatted_env = self.get_env_variables()
        labels = self.get_labels()
        node_affinity = self.get_vitess_node_affinity()

        return [
            get_cell_config(
                cell, region, vtgate_resources, formatted_env, labels, node_affinity
            )
            for cell in cells
        ]

    def get_vitess_dashboard(self) -> VitessDashboardConfigDict:
        cells = self.config_dict.get("cells")
        zk_address = self.config_dict.get("zk_address")
        vtctld_resources = self.config_dict.get("vtctld_resources")

        formatted_env = self.get_env_variables()
        labels = self.get_labels()
        node_affinity = self.get_vitess_node_affinity()

        return get_vitess_dashboard_config(
            cells, zk_address, vtctld_resources, formatted_env, labels, node_affinity
        )

    def get_vtadmin(self) -> VtAdminConfigDict:
        cells = self.config_dict.get("cells")
        vtadmin_resources = self.config_dict.get("vtadmin_resources")

        formatted_env = self.get_env_variables()
        labels = self.get_labels()
        node_affinity = self.get_vitess_node_affinity()

        return get_vt_admin_config(
            cells, vtadmin_resources, formatted_env, labels, node_affinity
        )

    def get_keyspaces(self) -> List[KeyspaceConfigDict]:
        cells = self.config_dict.get("cells")
        zk_address = self.config_dict.get("zk_address")
        region = self.get_region()
        keyspaces = self.config_dict.get("keyspaces")

        formatted_env = self.get_env_variables()
        labels = self.get_labels()
        node_affinity = self.get_vitess_node_affinity()

        return get_keyspaces_config(
            cells, keyspaces, zk_address, region, formatted_env, labels, node_affinity
        )

    def get_update_strategy(self) -> Dict[str, str]:
        return {"type": "Immediate"}

    def get_vitess_config(self) -> VitessDeploymentConfigDict:
        vitess_config = VitessDeploymentConfigDict(
            namespace=self.get_namespace(),
            images=self.get_images(),
            globalLockserver=self.get_global_lock_server(),
            cells=self.get_cells(),
            vitessDashboard=self.get_vitess_dashboard(),
            vtadmin=self.get_vtadmin(),
            keyspaces=self.get_keyspaces(),
            updateStrategy=self.get_update_strategy(),
        )
        return vitess_config

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


def load_vitess_instance_config(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> VitessDeploymentConfig:
    general_config = service_configuration_lib.read_service_configuration(
        service, soa_dir=soa_dir
    )
    instance_config = load_service_instance_config(
        service, instance, "vitesscluster", cluster, soa_dir=soa_dir
    )
    general_config = deep_merge_dictionaries(
        overrides=instance_config, defaults=general_config
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

    vitess_deployment_config = VitessDeploymentConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=general_config,
        branch_dict=branch_dict,
        soa_dir=soa_dir,
    )

    return vitess_deployment_config


def load_vitess_service_instance_configs(
    service: str,
    instance: str,
    cluster: str,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> VitessDeploymentConfigDict:
    vitess_service_instance_configs = load_vitess_instance_config(
        service, instance, cluster, soa_dir=soa_dir
    ).get_vitess_config()
    return vitess_service_instance_configs


# TODO: read this from CRD in service configs
def cr_id(service: str, instance: str) -> Mapping[str, str]:
    return dict(
        group="planetscale.com",
        version="v2",
        namespace=KUBERNETES_NAMESPACE,
        plural="vitessclusters",
        name=sanitised_cr_name(service, instance),
    )
