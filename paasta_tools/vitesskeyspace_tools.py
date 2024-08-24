import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import TypedDict
from typing import Union

import service_configuration_lib

from paasta_tools.kubernetes_tools import KubernetesDeploymentConfigDict
from paasta_tools.kubernetes_tools import limit_size_with_hash
from paasta_tools.kubernetes_tools import sanitised_cr_name
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_service_instance_config
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.vitesscluster_tools import get_formatted_environment_variables
from paasta_tools.vitesscluster_tools import KVEnvVar
from paasta_tools.vitesscluster_tools import KVEnvVarValueFrom
from paasta_tools.vitesscluster_tools import RequestsDict
from paasta_tools.vitesscluster_tools import ResourceConfigDict
from paasta_tools.vitesscluster_tools import VitessDeploymentConfig
from paasta_tools.vitesscluster_tools import VitessDeploymentConfigDict


log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


KUBERNETES_NAMESPACE = "paasta-vitessclusters"


# Global variables
TOPO_IMPLEMENTATION = "zk2"
TOPO_GLOBAL_ROOT = "/vitess-paasta/global"
WEB_PORT = "15000"
GRPC_PORT = "15999"


# Environment variables
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

# Extra Flags
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
    annotations: Mapping[str, Any]


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


def get_tablet_pool_config(
    cell: str,
    db_name: str,
    keyspace: str,
    host: str,
    zk_address: str,
    throttle_query_table: str,
    throttle_metrics_threshold: str,
    tablet_type: str,
    region: str,
    vttablet_resources: ResourceConfigDict,
    env: List[Union[KVEnvVar, KVEnvVarValueFrom]],
    labels: Dict[str, str],
    node_affinity: dict,
    annotations: Mapping[str, Any],
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
            "host": host,
            "port": 3306,
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
        annotations=annotations,
    )
    return config


class VitessKeyspaceConfigDict(VitessDeploymentConfigDict, total=False):
    name: str
    databaseName: str
    durabilityPolicy: str
    turndownPolicy: str
    partitionings: List[Dict[str, PartitioningValueDict]]
    updateStrategy: Dict[str, str]
    globalLockserver: Dict[str, str]
    zoneMap: Dict[str, Any]


def get_keyspace_config(
    cells: List[str],
    keyspace: str,
    cluster: str,
    vttablet_resources: ResourceConfigDict,
    images: Dict[str, str],
    update_strategy: Dict[str, str],
    global_lockserver: Dict[str, str],
    zk_address: str,
    region: str,
    env: List[Union[KVEnvVar, KVEnvVarValueFrom]],
    labels: Dict[str, str],
    node_affinity: dict,
    annotations: Mapping[str, Any],
) -> VitessKeyspaceConfigDict:
    """
    get vitess keyspace config
    """
    db_name = keyspace

    tablet_pools = []

    # get vttablets
    tablet_types = load_system_paasta_config().get_vitess_tablet_types()
    for tablet_type in tablet_types:
        ecosystem = region.split("-")[-1]
        host = f"mysql-{cluster}-{tablet_type}.dre-{ecosystem}"

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
                    host,
                    zk_address,
                    throttle_query_table,
                    throttle_metrics_threshold,
                    tablet_type,
                    region,
                    vttablet_resources,
                    env,
                    labels,
                    node_affinity,
                    annotations,
                )
                for cell in cells
            ]
        )
    vitess_keyspace_config = VitessKeyspaceConfigDict(
        name=keyspace,
        durabilityPolicy="none",
        turndownPolicy="Immediate",
        images=images,
        databaseName=db_name,
        updateStrategy=update_strategy,
        globalLockserver=global_lockserver,
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
        zoneMap={},
    )
    return vitess_keyspace_config


class VitessKeyspaceInstanceConfigDict(KubernetesDeploymentConfigDict, total=False):
    cells: List[str]
    zk_address: str
    images: Dict[str, str]
    keyspace: str
    cluster: str
    vttablet_resources: ResourceConfigDict


class VitessKeyspaceConfig(VitessDeploymentConfig):
    config_dict: VitessKeyspaceInstanceConfigDict

    config_filename_prefix = "vitesskeyspace"

    def __init__(
        self,
        service: str,
        cluster: str,
        instance: str,
        config_dict: VitessKeyspaceConfigDict,
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

    def get_global_lock_server(self) -> Dict[str, str]:
        zk_address = self.config_dict.get("zk_address")
        return {
            "implementation": TOPO_IMPLEMENTATION,
            "address": zk_address,
            "rootPath": TOPO_GLOBAL_ROOT,
        }

    def get_vitess_config(self) -> VitessKeyspaceConfigDict:
        cells = self.config_dict.get("cells")
        zk_address = self.config_dict.get("zk_address")
        region = self.get_region()
        keyspace = self.config_dict.get("keyspace")
        cluster = self.config_dict.get("cluster")
        vttablet_resources = self.config_dict.get("vttablet_resources")
        global_lockserver = self.get_global_lock_server()
        images = self.get_images()
        update_strategy = self.get_update_strategy()

        formatted_env = self.get_env_variables()
        labels = self.get_labels()
        node_affinity = self.get_vitess_node_affinity()
        annotations = self.get_annotations()

        return get_keyspace_config(
            cells,
            keyspace,
            cluster,
            vttablet_resources,
            images,
            update_strategy,
            global_lockserver,
            zk_address,
            region,
            formatted_env,
            labels,
            node_affinity,
            annotations,
        )

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


def load_vitess_keyspace_instance_config(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> VitessKeyspaceConfig:
    general_config = service_configuration_lib.read_service_configuration(
        service, soa_dir=soa_dir
    )
    instance_config = load_service_instance_config(
        service, instance, "vitesskeyspace", cluster, soa_dir=soa_dir
    )
    general_config = deep_merge_dictionaries(
        overrides=instance_config, defaults=general_config
    )

    branch_dict: Optional[BranchDictV2] = None
    if load_deployments:
        deployments_json = load_v2_deployments_json(service, soa_dir=soa_dir)
        temp_instance_config = VitessKeyspaceConfig(
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

    vitess_keyspace_config = VitessKeyspaceConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=general_config,
        branch_dict=branch_dict,
        soa_dir=soa_dir,
    )

    return vitess_keyspace_config


def load_vitess_keyspace_instance_configs(
    service: str,
    instance: str,
    cluster: str,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> VitessKeyspaceConfigDict:
    vitess_keyspace_instance_configs = load_vitess_keyspace_instance_config(
        service, instance, cluster, soa_dir=soa_dir
    ).get_vitess_config()
    return vitess_keyspace_instance_configs


# TODO: read this from CRD in service configs
def cr_id(service: str, instance: str) -> Mapping[str, str]:
    return dict(
        group="planetscale.com",
        version="v2",
        namespace=KUBERNETES_NAMESPACE,
        plural="vitesskeyspaces",
        name=sanitised_cr_name(service, instance),
    )
