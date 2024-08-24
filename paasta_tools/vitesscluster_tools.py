import json
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
from paasta_tools.kubernetes_tools import sanitised_cr_name
from paasta_tools.long_running_service_tools import load_service_namespace_config
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
WEB_PORT = "15000"
GRPC_PORT = "15999"


# Environment variables
VTCTLD_EXTRA_ENV = {
    "WEB_PORT": WEB_PORT,
    "GRPC_PORT": GRPC_PORT,
}


# Extra Flags
VTADMIN_EXTRA_FLAGS = {"grpc-allow-reflection": "true"}

VTCTLD_EXTRA_FLAGS = {
    "disable_active_reparents": "true",
    "security_policy": "read-only",
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
    replicas: int


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
    annotations: Mapping[str, Any]


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
    annotations: Mapping[str, Any]


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
) -> CellConfigDict:
    """
    get vtgate config
    """
    config = CellConfigDict(
        name=cell,
        gateway=GatewayConfigDict(
            replicas=0,
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
    annotations: Mapping[str, Any],
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
        annotations=annotations,
    )
    return config


def get_vt_admin_config(
    cells: List[str],
    vtadmin_resources: ResourceConfigDict,
    env: List[Union[KVEnvVar, KVEnvVarValueFrom]],
    labels: Dict[str, str],
    node_affinity: dict,
    annotations: Mapping[str, Any],
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
        annotations=annotations,
    )
    return config


class VitessDeploymentConfigDict(KubernetesDeploymentConfigDict, total=False):
    images: Dict[str, str]
    zk_address: str


class VitessClusterConfigDict(VitessDeploymentConfigDict, total=False):
    cells: List[CellConfigDict]
    vitessDashboard: VitessDashboardConfigDict
    vtadmin: VtAdminConfigDict
    updateStrategy: Dict[str, str]
    globalLockserver: Dict[str, Dict[str, str]]


class VitessClusterInstanceConfigDict(KubernetesDeploymentConfigDict, total=False):
    cells: List[str]
    zk_address: str
    vtctld_resources: ResourceConfigDict
    vtgate_resources: ResourceConfigDict
    vttablet_resources: ResourceConfigDict
    vtadmin_resources: ResourceConfigDict
    images: Dict[str, str]


class VitessDeploymentConfig(KubernetesDeploymentConfig):
    def get_namespace(self) -> str:
        return KUBERNETES_NAMESPACE

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

    def get_annotations(self) -> Mapping[str, Any]:
        # get required annotations to be added to the formatted resource before creating or updating custom resource
        service_namespace_config = load_service_namespace_config(
            service=self.service, namespace=self.get_nerve_namespace()
        )
        system_paasta_config = load_system_paasta_config()
        has_routable_ip = self.has_routable_ip(
            service_namespace_config, system_paasta_config
        )
        annotations: Mapping[str, Any] = {
            "smartstack_registrations": json.dumps(self.get_registrations()),
            "paasta.yelp.com/routable_ip": has_routable_ip,
        }

        return annotations

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

    def get_update_strategy(self) -> Dict[str, str]:
        return {"type": "Immediate"}


class VitessClusterConfig(VitessDeploymentConfig):
    config_dict: VitessClusterInstanceConfigDict

    config_filename_prefix = "vitesscluster"

    def __init__(
        self,
        service: str,
        cluster: str,
        instance: str,
        config_dict: VitessClusterConfigDict,
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

    def get_global_lock_server(self) -> Dict[str, Dict[str, str]]:
        zk_address = self.config_dict.get("zk_address")
        return {
            "external": {
                "implementation": TOPO_IMPLEMENTATION,
                "address": zk_address,
                "rootPath": TOPO_GLOBAL_ROOT,
            }
        }

    def get_vitess_dashboard(self) -> VitessDashboardConfigDict:
        cells = self.config_dict.get("cells")
        zk_address = self.config_dict.get("zk_address")
        vtctld_resources = self.config_dict.get("vtctld_resources")

        formatted_env = self.get_env_variables()
        labels = self.get_labels()
        node_affinity = self.get_vitess_node_affinity()
        annotations = self.get_annotations()

        return get_vitess_dashboard_config(
            cells,
            zk_address,
            vtctld_resources,
            formatted_env,
            labels,
            node_affinity,
            annotations,
        )

    def get_vtadmin(self) -> VtAdminConfigDict:
        cells = self.config_dict.get("cells")
        vtadmin_resources = self.config_dict.get("vtadmin_resources")

        formatted_env = self.get_env_variables()
        labels = self.get_labels()
        node_affinity = self.get_vitess_node_affinity()
        annotations = self.get_annotations()

        return get_vt_admin_config(
            cells, vtadmin_resources, formatted_env, labels, node_affinity, annotations
        )

    def get_cells(self) -> List[CellConfigDict]:
        cells = self.config_dict.get("cells")
        return [get_cell_config(cell) for cell in cells]

    def get_vitess_config(self) -> VitessClusterConfigDict:
        vitess_config = VitessClusterConfigDict(
            namespace=self.get_namespace(),
            images=self.get_images(),
            globalLockserver=self.get_global_lock_server(),
            cells=self.get_cells(),
            vitessDashboard=self.get_vitess_dashboard(),
            vtadmin=self.get_vtadmin(),
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


def load_vitess_cluster_instance_config(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> VitessClusterConfig:
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
        temp_instance_config = VitessClusterConfig(
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

    vitess_cluster_config = VitessClusterConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=general_config,
        branch_dict=branch_dict,
        soa_dir=soa_dir,
    )

    return vitess_cluster_config


def load_vitess_cluster_instance_configs(
    service: str,
    instance: str,
    cluster: str,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> VitessClusterConfigDict:
    vitess_cluster_instance_configs = load_vitess_cluster_instance_config(
        service, instance, cluster, soa_dir=soa_dir
    ).get_vitess_config()
    return vitess_cluster_instance_configs


# TODO: read this from CRD in service configs
def cr_id(service: str, instance: str) -> Mapping[str, str]:
    return dict(
        group="planetscale.com",
        version="v2",
        namespace=KUBERNETES_NAMESPACE,
        plural="vitessclusters",
        name=sanitised_cr_name(service, instance),
    )
