from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union

from mypy_extensions import TypedDict

DeployBlacklist = List[Tuple[str, str]]
DeployWhitelist = Optional[Tuple[str, List[str]]]
# The actual config files will have lists, since tuples are not expressible in base YAML, so we define different types
# here to represent that. The getter functions will convert to the safe versions above.
UnsafeDeployBlacklist = Optional[Sequence[Sequence[str]]]
UnsafeDeployWhitelist = Optional[Sequence[Union[str, Sequence[str]]]]

Constraint = Sequence[str]

# e.g. ['GROUP_BY', 'habitat', 2]. Marathon doesn't like that so we'll convert to Constraint later.
UnstringifiedConstraint = Sequence[Union[str, int, float]]

SecurityConfigDict = Dict  # Todo: define me.


class VolumeWithMode(TypedDict):
    mode: str


class DockerVolume(VolumeWithMode):
    hostPath: str
    containerPath: str


class AwsEbsVolume(VolumeWithMode):
    volume_id: str
    fs_type: str
    partition: int
    container_path: str


class PersistentVolume(VolumeWithMode):
    size: int
    container_path: str
    storage_class_name: str


class SecretVolumeItem(TypedDict, total=False):
    key: str
    path: str
    mode: Union[str, int]


class SecretVolume(TypedDict, total=False):
    secret_name: str
    container_path: str
    default_mode: Union[str, int]
    items: List[SecretVolumeItem]


class InstanceConfigDict(TypedDict, total=False):
    deploy_group: str
    mem: float
    cpus: float
    disk: float
    cmd: str
    args: List[str]
    cfs_period_us: float
    cpu_burst_add: float
    cap_add: List
    env: Dict[str, str]
    monitoring: Dict[str, str]
    deploy_blacklist: UnsafeDeployBlacklist
    deploy_whitelist: UnsafeDeployWhitelist
    pool: str
    persistent_volumes: List[PersistentVolume]
    role: str
    extra_volumes: List[DockerVolume]
    aws_ebs_volumes: List[AwsEbsVolume]
    secret_volumes: List[SecretVolume]
    security: SecurityConfigDict
    dependencies_reference: str
    dependencies: Dict[str, Dict]
    constraints: List[UnstringifiedConstraint]
    extra_constraints: List[UnstringifiedConstraint]
    net: str
    extra_docker_args: Dict[str, str]
    gpus: int
    branch: str


class DockerParameter(TypedDict):
    key: str
    value: str


class NoConfigurationForServiceError(Exception):
    pass


class ClusterAutoscalingResource(TypedDict):
    type: str
    id: str
    region: str
    pool: str
    min_capacity: int
    max_capacity: int


IdToClusterAutoscalingResourcesDict = Dict[str, ClusterAutoscalingResource]


class ResourcePoolSettings(TypedDict):
    target_utilization: float
    drain_timeout: int


PoolToResourcePoolSettingsDict = Dict[str, ResourcePoolSettings]


class MarathonConfigDict(TypedDict, total=False):
    user: str
    password: str
    url: List[str]


class LocalRunConfig(TypedDict, total=False):
    default_cluster: str


class RemoteRunConfig(TypedDict, total=False):
    default_role: str


class SparkRunConfig(TypedDict, total=False):
    default_cluster: str
    default_pool: str


class PaastaNativeConfig(TypedDict, total=False):
    principal: str
    secret: str


ExpectedSlaveAttributes = List[Dict[str, Any]]


class KubeKindDict(TypedDict, total=False):
    singular: str
    plural: str


class KubeCustomResourceDict(TypedDict, total=False):
    version: str
    file_prefix: str
    kube_kind: KubeKindDict
    group: str


class KubeStateMetricsCollectorConfigDict(TypedDict, total=False):
    unaggregated_metrics: List[str]
    summed_metric_to_group_keys: Dict[str, List[str]]
    label_metric_to_label_key: Dict[str, List[str]]
    label_renames: Dict[str, str]


class LogWriterConfig(TypedDict):
    driver: str
    options: Dict


class LogReaderConfig(TypedDict):
    driver: str
    options: Dict


class SystemPaastaConfigDict(TypedDict, total=False):
    api_endpoints: Dict[str, str]
    auth_certificate_ttl: str
    auto_config_instance_types_enabled: Dict[str, bool]
    auto_hostname_unique_size: int
    boost_regions: List[str]
    cluster_autoscaler_max_decrease: float
    cluster_autoscaler_max_increase: float
    cluster_autoscaling_draining_enabled: bool
    cluster_autoscaling_resources: IdToClusterAutoscalingResourcesDict
    cluster_boost_enabled: bool
    cluster_fqdn_format: str
    clusters: Sequence[str]
    cluster: str
    dashboard_links: Dict[str, Dict[str, str]]
    default_push_groups: List
    deploy_blacklist: UnsafeDeployBlacklist
    deployd_big_bounce_deadline: float
    deployd_log_level: str
    deployd_maintenance_polling_frequency: int
    deployd_max_service_instance_failures: int
    deployd_metrics_provider: str
    deployd_number_workers: int
    deployd_startup_bounce_deadline: float
    deployd_startup_oracle_enabled: bool
    deployd_use_zk_queue: bool
    deployd_worker_failure_backoff_factor: int
    deploy_whitelist: UnsafeDeployWhitelist
    disabled_watchers: List
    dockercfg_location: str
    docker_registry: str
    enable_client_cert_auth: bool
    enable_nerve_readiness_check: bool
    enable_envoy_readiness_check: bool
    enforce_disk_quota: bool
    envoy_admin_domain_name: str
    envoy_admin_endpoint_format: str
    envoy_nerve_readiness_check_script: List[str]
    envoy_readiness_check_script: List[str]
    expected_slave_attributes: ExpectedSlaveAttributes
    filter_bogus_mesos_cputime_enabled: bool
    fsm_template: str
    git_config: Dict
    hacheck_sidecar_image_url: str
    hacheck_sidecar_volumes: List[DockerVolume]
    hpa_always_uses_external_for_signalfx: bool
    kubernetes_custom_resources: List[KubeCustomResourceDict]
    kubernetes_use_hacheck_sidecar: bool
    ldap_host: str
    ldap_reader_password: str
    ldap_reader_username: str
    ldap_search_base: str
    ldap_search_ou: str
    local_run_config: LocalRunConfig
    log_reader: LogReaderConfig
    log_writer: LogWriterConfig
    maintenance_resource_reservation_enabled: bool
    marathon_servers: List[MarathonConfigDict]
    mesos_config: Dict
    metrics_provider: str
    monitoring_config: Dict
    nerve_readiness_check_script: List[str]
    paasta_native: PaastaNativeConfig
    pdb_max_unavailable: Union[str, int]
    pki_backend: str
    pod_defaults: Dict[str, Any]
    previous_marathon_servers: List[MarathonConfigDict]
    register_k8s_pods: bool
    register_marathon_services: bool
    register_native_services: bool
    remote_run_config: RemoteRunConfig
    resource_pool_settings: PoolToResourcePoolSettingsDict
    secret_provider: str
    security_check_command: str
    sensu_host: str
    sensu_port: int
    service_discovery_providers: Dict[str, Any]
    slack: Dict[str, str]
    spark_run_config: SparkRunConfig
    supported_storage_classes: Sequence[str]
    synapse_haproxy_url_format: str
    synapse_host: str
    synapse_port: int
    taskproc: Dict
    tron: Dict
    vault_cluster_map: Dict
    vault_environment: str
    volumes: List[DockerVolume]
    zookeeper: str
