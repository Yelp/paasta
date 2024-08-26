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


class GatewayConfigDict(TypedDict, total=False):
    affinity: Dict[str, Any]
    extraEnv: List[Union[KVEnvVar, KVEnvVarValueFrom]]
    extraFlags: Dict[str, str]
    extraLabels: Dict[str, str]
    replicas: int
    resources: Dict[str, Any]
    annotations: Mapping[str, Any]


class VitessCellConfigDict(VitessDeploymentConfigDict, total=False):
    name: str
    allCells: List[str]
    globalLockserver: Dict[str, str]
    gateway: GatewayConfigDict


def get_cell_config(
    cell: str,
    images: Dict[str, str],
    allCells: List[str],
    global_lock_server: Dict[str, str],
    region: str,
    vtgate_resources: ResourceConfigDict,
    env: List[Union[KVEnvVar, KVEnvVarValueFrom]],
    labels: Dict[str, str],
    node_affinity: dict,
    annotations: Mapping[str, Any],
) -> VitessCellConfigDict:
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

    config = VitessCellConfigDict(
        name=cell,
        images=images,
        allCells=allCells,
        globalLockserver=global_lock_server,
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
            annotations=annotations,
        ),
    )
    return config


class VitessCellInstanceConfigDict(KubernetesDeploymentConfigDict, total=False):
    cell: str
    cells: List[str]
    zk_address: str
    vtgate_resources: ResourceConfigDict
    images: Dict[str, str]


class VitessCellConfig(VitessDeploymentConfig):
    config_dict: VitessCellInstanceConfigDict

    config_filename_prefix = "vitesscell"

    def __init__(
        self,
        service: str,
        cluster: str,
        instance: str,
        config_dict: VitessCellConfigDict,
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

    def get_vitess_cell_config(self) -> VitessCellConfigDict:
        cell = self.config_dict.get("cell")
        all_cells = self.config_dict.get("cells")
        images = self.get_images()
        global_lock_server = self.get_global_lock_server()
        region = self.get_region()
        vtgate_resources = self.config_dict.get("vtgate_resources")

        formatted_env = self.get_env_variables()
        labels = self.get_labels()
        node_affinity = self.get_vitess_node_affinity()
        annotations = self.get_annotations()

        return get_cell_config(
            cell,
            images,
            all_cells,
            global_lock_server,
            region,
            vtgate_resources,
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


def load_vitess_cell_instance_config(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> VitessCellConfig:
    general_config = service_configuration_lib.read_service_configuration(
        service, soa_dir=soa_dir
    )
    instance_config = load_service_instance_config(
        service, instance, "vitesscell", cluster, soa_dir=soa_dir
    )
    general_config = deep_merge_dictionaries(
        overrides=instance_config, defaults=general_config
    )

    branch_dict: Optional[BranchDictV2] = None
    if load_deployments:
        deployments_json = load_v2_deployments_json(service, soa_dir=soa_dir)
        temp_instance_config = VitessCellConfig(
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

    vitess_cell_config = VitessCellConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=general_config,
        branch_dict=branch_dict,
        soa_dir=soa_dir,
    )

    return vitess_cell_config


def load_vitess_cell_instance_configs(
    service: str,
    instance: str,
    cluster: str,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> VitessCellConfigDict:
    vitess_cell_instance_configs = load_vitess_cell_instance_config(
        service, instance, cluster, soa_dir=soa_dir
    ).get_vitess_cell_config()
    return vitess_cell_instance_configs


# TODO: read this from CRD in service configs
def cr_id(service: str, instance: str) -> Mapping[str, str]:
    return dict(
        group="planetscale.com",
        version="v2",
        namespace=KUBERNETES_NAMESPACE,
        plural="vitesscells",
        name=sanitised_cr_name(service, instance),
    )
