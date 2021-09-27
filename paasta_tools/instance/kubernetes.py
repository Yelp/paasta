import asyncio
from collections import defaultdict
from enum import Enum
from typing import Any
from typing import DefaultDict
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple

import a_sync
import pytz
from kubernetes.client import V1Container
from kubernetes.client import V1ControllerRevision
from kubernetes.client import V1Pod
from kubernetes.client import V1Probe
from kubernetes.client import V1ReplicaSet
from kubernetes.client.rest import ApiException
from mypy_extensions import TypedDict

from paasta_tools import cassandracluster_tools
from paasta_tools import envoy_tools
from paasta_tools import flink_tools
from paasta_tools import kafkacluster_tools
from paasta_tools import kubernetes_tools
from paasta_tools import marathon_tools
from paasta_tools import monkrelaycluster_tools
from paasta_tools import nrtsearchservice_tools
from paasta_tools import smartstack_tools
from paasta_tools.cli.utils import LONG_RUNNING_INSTANCE_TYPE_HANDLERS
from paasta_tools.instance.hpa_metrics_parser import HPAMetricsDict
from paasta_tools.instance.hpa_metrics_parser import HPAMetricsParser
from paasta_tools.kubernetes_tools import get_pod_event_messages
from paasta_tools.kubernetes_tools import get_tail_lines_for_kubernetes_container
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
from paasta_tools.smartstack_tools import KubeSmartstackEnvoyReplicationChecker
from paasta_tools.smartstack_tools import match_backends_and_pods
from paasta_tools.utils import calculate_tail_lines


INSTANCE_TYPES_CR = {"flink", "cassandracluster", "kafkacluster"}
INSTANCE_TYPES_K8S = {"kubernetes", "cassandracluster"}
INSTANCE_TYPES = INSTANCE_TYPES_K8S.union(INSTANCE_TYPES_CR)

INSTANCE_TYPES_WITH_SET_STATE = {"flink"}
INSTANCE_TYPE_CR_ID = dict(
    flink=flink_tools.cr_id,
    cassandracluster=cassandracluster_tools.cr_id,
    kafkacluster=kafkacluster_tools.cr_id,
    nrtsearchservice=nrtsearchservice_tools.cr_id,
    monkrelaycluster=monkrelaycluster_tools.cr_id,
)


class ServiceMesh(Enum):
    SMARTSTACK = "smartstack"
    ENVOY = "envoy"


class KubernetesAutoscalingStatusDict(TypedDict):
    min_instances: int
    max_instances: int
    metrics: List
    desired_replicas: int
    last_scale_time: str


class KubernetesVersionDict(TypedDict, total=False):
    name: str
    type: str
    replicas: int
    ready_replicas: int
    create_timestamp: int
    git_sha: str
    config_sha: str
    pods: Sequence[Dict[str, Any]]


def cr_id(service: str, instance: str, instance_type: str) -> Mapping[str, str]:
    cr_id_fn = INSTANCE_TYPE_CR_ID.get(instance_type)
    if not cr_id_fn:
        raise RuntimeError(f"Unknown instance type {instance_type}")
    return cr_id_fn(service, instance)


def can_handle(instance_type: str) -> bool:
    return instance_type in INSTANCE_TYPES


def can_set_state(instance_type: str) -> bool:
    return instance_type in INSTANCE_TYPES_WITH_SET_STATE


def set_cr_desired_state(
    kube_client: kubernetes_tools.KubeClient,
    service: str,
    instance: str,
    instance_type: str,
    desired_state: str,
):
    try:
        kubernetes_tools.set_cr_desired_state(
            kube_client=kube_client,
            cr_id=cr_id(service, instance, instance_type),
            desired_state=desired_state,
        )
    except ApiException as e:
        error_message = (
            f"Error while setting state {desired_state} of "
            f"{service}.{instance}: {e}"
        )
        raise RuntimeError(error_message)


def autoscaling_status(
    kube_client: kubernetes_tools.KubeClient,
    job_config: LongRunningServiceConfig,
    namespace: str,
) -> KubernetesAutoscalingStatusDict:
    try:
        hpa = kube_client.autoscaling.read_namespaced_horizontal_pod_autoscaler(
            name=job_config.get_sanitised_deployment_name(), namespace=namespace
        )
    except ApiException as e:
        if e.status == 404:
            return KubernetesAutoscalingStatusDict(
                min_instances=-1,
                max_instances=-1,
                metrics=[],
                desired_replicas=-1,
                last_scale_time="unknown (could not find HPA object)",
            )
        else:
            raise

    # Parse metrics sources, based on
    # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V2beta2ExternalMetricSource.md#v2beta2externalmetricsource
    parser = HPAMetricsParser(hpa)

    # https://github.com/python/mypy/issues/7217
    metrics_by_name: DefaultDict[str, HPAMetricsDict] = defaultdict(
        lambda: HPAMetricsDict()
    )

    if hpa.spec.metrics is not None:
        for metric_spec in hpa.spec.metrics:
            parsed = parser.parse_target(metric_spec)
            metrics_by_name[parsed["name"]].update(parsed)

    if hpa.status.current_metrics is not None:
        for metric_spec in hpa.status.current_metrics:
            parsed = parser.parse_current(metric_spec)
            if parsed is not None:
                metrics_by_name[parsed["name"]].update(parsed)

    metric_stats = list(metrics_by_name.values())

    last_scale_time = (
        hpa.status.last_scale_time.replace(tzinfo=pytz.UTC).isoformat()
        if getattr(hpa.status, "last_scale_time")
        else "N/A"
    )

    return KubernetesAutoscalingStatusDict(
        min_instances=hpa.spec.min_replicas,
        max_instances=hpa.spec.max_replicas,
        metrics=metric_stats,
        desired_replicas=hpa.status.desired_replicas,
        last_scale_time=last_scale_time,
    )


async def pod_info(
    pod: V1Pod, client: kubernetes_tools.KubeClient, num_tail_lines: int,
):
    container_statuses = pod.status.container_statuses or []
    try:
        pod_event_messages = await get_pod_event_messages(client, pod)
    except asyncio.TimeoutError:
        pod_event_messages = [{"error": "Could not fetch events for pod"}]
    containers = [
        dict(
            name=container.name,
            tail_lines=await get_tail_lines_for_kubernetes_container(
                client, pod, container, num_tail_lines,
            ),
        )
        for container in container_statuses
    ]
    return {
        "name": pod.metadata.name,
        "host": kubernetes_tools.get_pod_hostname(client, pod),
        "deployed_timestamp": pod.metadata.creation_timestamp.timestamp(),
        "phase": pod.status.phase,
        "ready": kubernetes_tools.is_pod_ready(pod),
        "containers": containers,
        "reason": pod.status.reason,
        "message": pod.status.message,
        "events": pod_event_messages,
        "git_sha": pod.metadata.labels.get("paasta.yelp.com/git_sha"),
        "config_sha": pod.metadata.labels.get("paasta.yelp.com/config_sha"),
    }


@a_sync.to_blocking
async def job_status(
    kstatus: MutableMapping[str, Any],
    client: kubernetes_tools.KubeClient,
    job_config: LongRunningServiceConfig,
    pod_list: Sequence[V1Pod],
    replicaset_list: Sequence[V1ReplicaSet],
    verbose: int,
    namespace: str,
) -> None:
    app_id = job_config.get_sanitised_deployment_name()
    kstatus["app_id"] = app_id
    kstatus["pods"] = []
    kstatus["replicasets"] = []

    if verbose > 0:
        num_tail_lines = calculate_tail_lines(verbose)
        kstatus["pods"] = await asyncio.gather(
            *[pod_info(pod, client, num_tail_lines) for pod in pod_list]
        )

    for replicaset in replicaset_list:
        kstatus["replicasets"].append(
            {
                "name": replicaset.metadata.name,
                "replicas": replicaset.spec.replicas,
                "ready_replicas": ready_replicas_from_replicaset(replicaset),
                "create_timestamp": replicaset.metadata.creation_timestamp.timestamp(),
                "git_sha": replicaset.metadata.labels.get("paasta.yelp.com/git_sha"),
                "config_sha": replicaset.metadata.labels.get(
                    "paasta.yelp.com/config_sha"
                ),
            }
        )

    kstatus["expected_instance_count"] = job_config.get_instances()

    app = kubernetes_tools.get_kubernetes_app_by_name(
        name=app_id, kube_client=client, namespace=namespace
    )
    desired_instances = (
        job_config.get_instances() if job_config.get_desired_state() != "stop" else 0
    )
    deploy_status, message = kubernetes_tools.get_kubernetes_app_deploy_status(
        app=app, desired_instances=desired_instances,
    )
    kstatus["deploy_status"] = kubernetes_tools.KubernetesDeployStatus.tostring(
        deploy_status
    )
    kstatus["deploy_status_message"] = message
    kstatus["running_instance_count"] = (
        app.status.ready_replicas if app.status.ready_replicas else 0
    )
    kstatus["create_timestamp"] = app.metadata.creation_timestamp.timestamp()
    kstatus["namespace"] = app.metadata.namespace


def mesh_status(
    service: str,
    service_mesh: ServiceMesh,
    instance: str,
    job_config: LongRunningServiceConfig,
    service_namespace_config: ServiceNamespaceConfig,
    pods: Sequence[V1Pod],
    settings: Any,
    should_return_individual_backends: bool = False,
) -> Mapping[str, Any]:

    registration = job_config.get_registrations()[0]
    instance_pool = job_config.get_pool()

    replication_checker = KubeSmartstackEnvoyReplicationChecker(
        nodes=kubernetes_tools.get_all_nodes(settings.kubernetes_client),
        system_paasta_config=settings.system_paasta_config,
    )
    node_hostname_by_location = replication_checker.get_allowed_locations_and_hosts(
        job_config
    )

    expected_smartstack_count = marathon_tools.get_expected_instance_count_for_namespace(
        service=service,
        namespace=job_config.get_nerve_namespace(),
        cluster=settings.cluster,
        instance_type_class=KubernetesDeploymentConfig,
    )
    expected_count_per_location = int(
        expected_smartstack_count / len(node_hostname_by_location)
    )
    mesh_status: MutableMapping[str, Any] = {
        "registration": registration,
        "expected_backends_per_location": expected_count_per_location,
        "locations": [],
    }

    for location, hosts in node_hostname_by_location.items():
        host = replication_checker.get_first_host_in_pool(hosts, instance_pool)
        if service_mesh == ServiceMesh.SMARTSTACK:
            mesh_status["locations"].append(
                _build_smartstack_location_dict(
                    synapse_host=host,
                    synapse_port=settings.system_paasta_config.get_synapse_port(),
                    synapse_haproxy_url_format=settings.system_paasta_config.get_synapse_haproxy_url_format(),
                    registration=registration,
                    pods=pods,
                    location=location,
                    should_return_individual_backends=should_return_individual_backends,
                )
            )
        elif service_mesh == ServiceMesh.ENVOY:
            mesh_status["locations"].append(
                _build_envoy_location_dict(
                    envoy_host=host,
                    envoy_admin_port=settings.system_paasta_config.get_envoy_admin_port(),
                    envoy_admin_endpoint_format=settings.system_paasta_config.get_envoy_admin_endpoint_format(),
                    registration=registration,
                    pods=pods,
                    location=location,
                    should_return_individual_backends=should_return_individual_backends,
                )
            )
    return mesh_status


def _build_envoy_location_dict(
    envoy_host: str,
    envoy_admin_port: int,
    envoy_admin_endpoint_format: str,
    registration: str,
    pods: Iterable[V1Pod],
    location: str,
    should_return_individual_backends: bool,
) -> MutableMapping[str, Any]:
    backends = envoy_tools.get_backends(
        registration,
        envoy_host=envoy_host,
        envoy_admin_port=envoy_admin_port,
        envoy_admin_endpoint_format=envoy_admin_endpoint_format,
    )
    sorted_envoy_backends = sorted(
        [
            backend[0]
            for _, service_backends in backends.items()
            for backend in service_backends
        ],
        key=lambda backend: backend["eds_health_status"],
    )
    casper_proxied_backends = {
        (backend["address"], backend["port_value"])
        for _, service_backends in backends.items()
        for backend, is_casper_proxied_backend in service_backends
        if is_casper_proxied_backend
    }

    matched_envoy_backends_and_pods = envoy_tools.match_backends_and_pods(
        sorted_envoy_backends, pods,
    )

    return envoy_tools.build_envoy_location_dict(
        location,
        matched_envoy_backends_and_pods,
        should_return_individual_backends,
        casper_proxied_backends,
    )


def _build_smartstack_location_dict(
    synapse_host: str,
    synapse_port: int,
    synapse_haproxy_url_format: str,
    registration: str,
    pods: Iterable[V1Pod],
    location: str,
    should_return_individual_backends: bool,
) -> MutableMapping[str, Any]:
    sorted_backends = sorted(
        smartstack_tools.get_backends(
            registration,
            synapse_host=synapse_host,
            synapse_port=synapse_port,
            synapse_haproxy_url_format=synapse_haproxy_url_format,
        ),
        key=lambda backend: backend["status"],
        reverse=True,  # put 'UP' backends above 'MAINT' backends
    )

    matched_backends_and_pods = match_backends_and_pods(sorted_backends, pods)
    location_dict = smartstack_tools.build_smartstack_location_dict(
        location, matched_backends_and_pods, should_return_individual_backends
    )
    return location_dict


def cr_status(
    service: str, instance: str, verbose: int, instance_type: str, kube_client: Any,
) -> Mapping[str, Any]:
    status: MutableMapping[str, Any] = {}
    cr = (
        kubernetes_tools.get_cr(
            kube_client=kube_client, cr_id=cr_id(service, instance, instance_type)
        )
        or {}
    )
    crstatus = cr.get("status")
    metadata = cr.get("metadata")
    if crstatus is not None:
        status["status"] = crstatus
    if metadata is not None:
        status["metadata"] = metadata
    return status


def filter_actually_running_replicasets(
    replicaset_list: Sequence[V1ReplicaSet],
) -> List[V1ReplicaSet]:
    return [
        rs
        for rs in replicaset_list
        if not (rs.spec.replicas == 0 and ready_replicas_from_replicaset(rs) == 0)
    ]


def bounce_status(
    service: str, instance: str, settings: Any,
):
    status: Dict[str, Any] = {}
    job_config = kubernetes_tools.load_kubernetes_service_config(
        service=service,
        instance=instance,
        cluster=settings.cluster,
        soa_dir=settings.soa_dir,
        load_deployments=True,
    )
    expected_instance_count = job_config.get_instances()
    status["expected_instance_count"] = expected_instance_count
    desired_state = job_config.get_desired_state()
    status["desired_state"] = desired_state

    kube_client = settings.kubernetes_client
    if kube_client is None:
        raise RuntimeError("Could not load Kubernetes client!")

    app = kubernetes_tools.get_kubernetes_app_by_name(
        name=job_config.get_sanitised_deployment_name(),
        kube_client=kube_client,
        namespace=job_config.get_kubernetes_namespace(),
    )
    status["running_instance_count"] = (
        app.status.ready_replicas if app.status.ready_replicas else 0
    )

    deploy_status, message = kubernetes_tools.get_kubernetes_app_deploy_status(
        app=app,
        desired_instances=(expected_instance_count if desired_state != "stop" else 0),
    )
    status["deploy_status"] = kubernetes_tools.KubernetesDeployStatus.tostring(
        deploy_status
    )

    if job_config.get_persistent_volumes():
        version_objects = kubernetes_tools.controller_revisions_for_service_instance(
            service=job_config.service,
            instance=job_config.instance,
            kube_client=kube_client,
            namespace=job_config.get_kubernetes_namespace(),
        )
    else:
        replicasets = kubernetes_tools.replicasets_for_service_instance(
            service=job_config.service,
            instance=job_config.instance,
            kube_client=kube_client,
            namespace=job_config.get_kubernetes_namespace(),
        )
        version_objects = filter_actually_running_replicasets(replicasets)

    active_shas = kubernetes_tools.get_active_shas_for_service([app, *version_objects],)
    status["active_shas"] = list(active_shas)
    status["app_count"] = len(active_shas)
    return status


def kubernetes_status_v2(
    service: str,
    instance: str,
    verbose: int,
    include_smartstack: bool,
    include_envoy: bool,
    instance_type: str,
    settings: Any,
):
    status: Dict[str, Any] = {}
    config_loader = LONG_RUNNING_INSTANCE_TYPE_HANDLERS[instance_type].loader
    job_config = config_loader(
        service=service,
        instance=instance,
        cluster=settings.cluster,
        soa_dir=settings.soa_dir,
        load_deployments=True,
    )
    kube_client = settings.kubernetes_client
    if kube_client is None:
        return status

    if (
        verbose > 1
        and job_config.is_autoscaling_enabled()
        and job_config.get_autoscaling_params().get("decision_policy", "") != "bespoke"  # type: ignore
    ):
        try:
            status["autoscaling_status"] = autoscaling_status(
                kube_client, job_config, job_config.get_kubernetes_namespace()
            )
        except Exception as e:
            status[
                "error_message"
            ] = f"Unknown error occurred while fetching autoscaling status. Please contact #compute-infra for help: {e}"

    desired_state = job_config.get_desired_state()
    status["app_name"] = job_config.get_sanitised_deployment_name()
    status["desired_state"] = desired_state
    status["desired_instances"] = (
        job_config.get_instances() if desired_state != "stop" else 0
    )
    status["bounce_method"] = job_config.get_bounce_method()

    pod_list = kubernetes_tools.pods_for_service_instance(
        service=job_config.service,
        instance=job_config.instance,
        kube_client=kube_client,
        namespace=job_config.get_kubernetes_namespace(),
    )

    service_namespace_config = kubernetes_tools.load_service_namespace_config(
        service=service,
        namespace=job_config.get_nerve_namespace(),
        soa_dir=settings.soa_dir,
    )
    backends = None
    if "proxy_port" in service_namespace_config:
        envoy_status = mesh_status(
            service=service,
            service_mesh=ServiceMesh.ENVOY,
            instance=job_config.get_nerve_namespace(),
            job_config=job_config,
            service_namespace_config=service_namespace_config,
            pods=pod_list,
            should_return_individual_backends=True,
            settings=settings,
        )
        if envoy_status.get("locations"):
            backends = {
                be["address"] for be in envoy_status["locations"][0].get("backends", [])
            }
        else:
            backends = set()
        if include_envoy:
            # Note we always include backends here now
            status["envoy"] = envoy_status

    update_kubernetes_status(
        status, kube_client, job_config, pod_list, backends, verbose,
    )

    return status


@a_sync.to_blocking
async def update_kubernetes_status(
    status: MutableMapping[str, Any],
    client: kubernetes_tools.KubeClient,
    job_config: LongRunningServiceConfig,
    pod_list: List[V1Pod],
    backends: Optional[Set[str]],
    verbose: int,
):
    """ Updates a status object with relevant information, useful for async calls """
    num_tail_lines = calculate_tail_lines(verbose)

    if job_config.get_persistent_volumes():
        controller_revision_list = kubernetes_tools.controller_revisions_for_service_instance(
            service=job_config.service,
            instance=job_config.instance,
            kube_client=client,
            namespace=job_config.get_kubernetes_namespace(),
        )
        status["versions"] = await get_versions_for_controller_revisions(
            controller_revision_list, client, pod_list, backends, num_tail_lines
        )
    else:
        replicaset_list = kubernetes_tools.replicasets_for_service_instance(
            service=job_config.service,
            instance=job_config.instance,
            kube_client=client,
            namespace=job_config.get_kubernetes_namespace(),
        )
        status["versions"] = await get_versions_for_replicasets(
            replicaset_list, client, pod_list, backends, num_tail_lines
        )


async def get_versions_for_replicasets(
    replicaset_list: Sequence[V1ReplicaSet],
    client: kubernetes_tools.KubeClient,
    pod_list: Sequence[V1Pod],
    backends: Optional[Set[str]],
    num_tail_lines: int,
) -> List[KubernetesVersionDict]:
    # For the purpose of active_shas/app_count, don't count replicasets that
    # are at 0/0.
    actually_running_replicasets = filter_actually_running_replicasets(replicaset_list)
    pods_by_replicaset = get_pods_by_replicaset(pod_list)
    versions = await asyncio.gather(
        *[
            get_replicaset_status(
                replicaset,
                client,
                pods_by_replicaset.get(replicaset.metadata.name),
                backends,
                num_tail_lines,
            )
            for replicaset in actually_running_replicasets
        ]
    )
    return versions


def get_pods_by_replicaset(pods: Sequence[V1Pod]) -> Dict[str, List[V1Pod]]:
    pods_by_replicaset: DefaultDict[str, List[V1Pod]] = defaultdict(list)
    for pod in pods:
        for owner_reference in pod.metadata.owner_references:
            if owner_reference.kind == "ReplicaSet":
                pods_by_replicaset[owner_reference.name].append(pod)

    return pods_by_replicaset


async def get_replicaset_status(
    replicaset: V1ReplicaSet,
    client: kubernetes_tools.KubeClient,
    pods: Sequence[V1Pod],
    backends: Optional[Set[str]],
    num_tail_lines: int,
) -> KubernetesVersionDict:
    return {
        "name": replicaset.metadata.name,
        "type": "ReplicaSet",
        "replicas": replicaset.spec.replicas,
        "ready_replicas": ready_replicas_from_replicaset(replicaset),
        "create_timestamp": replicaset.metadata.creation_timestamp.timestamp(),
        "git_sha": replicaset.metadata.labels.get("paasta.yelp.com/git_sha"),
        "config_sha": replicaset.metadata.labels.get("paasta.yelp.com/config_sha"),
        "pods": await asyncio.gather(
            *[get_pod_status(pod, backends, client, num_tail_lines) for pod in pods]
        ),
    }


async def get_pod_status(
    pod: V1Pod, backends: Optional[Set[str]], client: Any, num_tail_lines: int
) -> Dict[str, Any]:
    reason = pod.status.reason
    message = pod.status.message
    scheduled = kubernetes_tools.is_pod_scheduled(pod)
    ready = kubernetes_tools.is_pod_ready(pod)
    delete_timestamp = (
        pod.metadata.deletion_timestamp.timestamp()
        if pod.metadata.deletion_timestamp
        else None
    )

    try:
        # Filter events to only last 15m
        pod_event_messages = await get_pod_event_messages(
            client, pod, max_age_in_seconds=900
        )
    except asyncio.TimeoutError:
        pod_event_messages = [{"error": "Could not retrieve events. Please try again."}]

    # if evicted, there is no condition
    if not scheduled and reason != "Evicted":
        sched_condition = kubernetes_tools.get_pod_condition(pod, "PodScheduled")
        reason = sched_condition.reason
        message = sched_condition.message

    mesh_ready = None
    if backends is not None:
        # TODO: Remove this once k8s readiness reflects mesh readiness, PAASTA-17266
        mesh_ready = pod.status.pod_ip in backends

    return {
        "name": pod.metadata.name,
        "ip": pod.status.pod_ip,
        "host": pod.status.host_ip,
        "phase": pod.status.phase,
        "reason": reason,
        "message": message,
        "scheduled": scheduled,
        "ready": ready,
        "mesh_ready": mesh_ready,
        "containers": await get_pod_containers(pod, client, num_tail_lines),
        "create_timestamp": pod.metadata.creation_timestamp.timestamp(),
        "delete_timestamp": delete_timestamp,
        "events": pod_event_messages,
    }


def get_container_healthcheck(pod_ip: str, probe: V1Probe) -> Dict[str, Any]:
    if getattr(probe, "http_get", None):
        return {
            "http_url": f"http://{pod_ip}:{probe.http_get.port}{probe.http_get.path}"
        }
    if getattr(probe, "tcp_socket", None):
        return {"tcp_port": f"{probe.tcp_socket.port}"}
    if getattr(probe, "_exec", None):
        return {"cmd": f"{' '.join(probe._exec.command)}"}
    return {}


async def get_pod_containers(
    pod: V1Pod, client: Any, num_tail_lines: int
) -> List[Dict[str, Any]]:
    containers = []
    statuses = pod.status.container_statuses or []
    container_specs = pod.spec.containers
    for cs in statuses:
        specs: List[V1Container] = [c for c in container_specs if c.name == cs.name]
        healthcheck_grace_period = 0
        healthcheck = None
        if specs:
            # There should be only one matching spec
            spec = specs[0]
            if spec.liveness_probe:
                healthcheck_grace_period = (
                    spec.liveness_probe.initial_delay_seconds or 0
                )
                healthcheck = get_container_healthcheck(
                    pod.status.pod_ip, spec.liveness_probe
                )

        state_dict = cs.state.to_dict()
        state = None
        reason = None
        message = None
        start_timestamp = None
        for state_name, this_state in state_dict.items():
            # Each container has only populated state at a time
            if this_state:
                state = state_name
                if "reason" in this_state:
                    reason = this_state["reason"]
                if "message" in this_state:
                    message = this_state["message"]
                if "started_at" in this_state:
                    start_timestamp = this_state["started_at"].timestamp()

        last_state_dict = cs.last_state.to_dict()
        last_state = None
        last_reason = None
        last_message = None
        last_duration = None
        last_timestamp = None
        for state_name, this_state in last_state_dict.items():
            if this_state:
                last_state = state_name
                if "reason" in this_state:
                    last_reason = this_state["reason"]
                if "message" in this_state:
                    last_message = this_state["message"]
                if this_state.get("started_at"):
                    if this_state.get("finished_at"):
                        last_duration = (
                            this_state["finished_at"] - this_state["started_at"]
                        ).total_seconds()

                    last_timestamp = this_state["started_at"].timestamp()

        async def get_tail_lines():
            try:
                return await get_tail_lines_for_kubernetes_container(
                    client, pod, cs, num_tail_lines, previous=False,
                )
            except asyncio.TimeoutError:
                return {"error_message": f"Could not fetch logs for {cs.name}"}

        # get previous log lines as well if this container restarted recently
        async def get_previous_tail_lines():
            nonlocal previous_tail_lines
            if state == "running" and kubernetes_tools.recent_container_restart(
                cs.restart_count, last_state, last_timestamp
            ):
                try:
                    return await get_tail_lines_for_kubernetes_container(
                        client, pod, cs, num_tail_lines, previous=True,
                    )
                except asyncio.TimeoutError:
                    return {
                        "error_message": f"Could not fetch previous logs for {cs.name}"
                    }
            return None

        tail_lines, previous_tail_lines = await asyncio.gather(
            asyncio.ensure_future(get_tail_lines()),
            asyncio.ensure_future(get_previous_tail_lines()),
        )

        containers.append(
            {
                "name": cs.name,
                "restart_count": cs.restart_count,
                "state": state,
                "reason": reason,
                "message": message,
                "last_state": last_state,
                "last_reason": last_reason,
                "last_message": last_message,
                "last_duration": last_duration,
                "last_timestamp": last_timestamp,
                "previous_tail_lines": previous_tail_lines,
                "timestamp": start_timestamp,
                "healthcheck_grace_period": healthcheck_grace_period,
                "healthcheck_cmd": healthcheck,
                "tail_lines": tail_lines,
            }
        )
    return containers


async def get_versions_for_controller_revisions(
    controller_revisions: Sequence[V1ControllerRevision],
    client: kubernetes_tools.KubeClient,
    pods: Sequence[V1Pod],
    backends: Optional[Set[str]],
    num_tail_lines: int,
) -> List[KubernetesVersionDict]:
    versions: List[KubernetesVersionDict] = []

    cr_by_shas: Dict[Tuple[str, str], V1ControllerRevision] = {}
    for cr in controller_revisions:
        git_sha = cr.metadata.labels["paasta.yelp.com/git_sha"]
        config_sha = cr.metadata.labels["paasta.yelp.com/config_sha"]
        cr_by_shas[(git_sha, config_sha)] = cr

    pods_by_shas: DefaultDict[Tuple[str, str], List[V1Pod]] = defaultdict(list)
    for pod in pods:
        git_sha = pod.metadata.labels["paasta.yelp.com/git_sha"]
        config_sha = pod.metadata.labels["paasta.yelp.com/config_sha"]
        pods_by_shas[(git_sha, config_sha)].append(pod)

    versions = await asyncio.gather(
        *[
            get_version_for_controller_revision(
                cr,
                pods_by_shas[(git_sha, config_sha)],
                backends,
                num_tail_lines,
                client,
            )
            for (git_sha, config_sha), cr in cr_by_shas.items()
        ]
    )

    return versions


async def get_version_for_controller_revision(
    cr: V1ControllerRevision,
    pods: Sequence[V1Pod],
    backends: Optional[Set[str]],
    num_tail_lines: int,
    client: Any,
) -> KubernetesVersionDict:
    ready_pods = [pod for pod in pods if kubernetes_tools.is_pod_ready(pod)]
    return {
        "name": cr.metadata.name,
        "type": "ControllerRevision",
        "replicas": len(pods),
        "ready_replicas": len(ready_pods),
        "create_timestamp": cr.metadata.creation_timestamp.timestamp(),
        "git_sha": cr.metadata.labels.get("paasta.yelp.com/git_sha"),
        "config_sha": cr.metadata.labels.get("paasta.yelp.com/config_sha"),
        "pods": await asyncio.gather(
            *[get_pod_status(pod, backends, client, num_tail_lines) for pod in pods]
        ),
    }


def kubernetes_status(
    service: str,
    instance: str,
    verbose: int,
    include_smartstack: bool,
    include_envoy: bool,
    instance_type: str,
    settings: Any,
) -> Mapping[str, Any]:
    kstatus: Dict[str, Any] = {}
    config_loader = LONG_RUNNING_INSTANCE_TYPE_HANDLERS[instance_type].loader
    job_config = config_loader(
        service=service,
        instance=instance,
        cluster=settings.cluster,
        soa_dir=settings.soa_dir,
        load_deployments=True,
    )
    kube_client = settings.kubernetes_client
    if kube_client is None:
        return kstatus

    app = kubernetes_tools.get_kubernetes_app_by_name(
        name=job_config.get_sanitised_deployment_name(),
        kube_client=kube_client,
        namespace=job_config.get_kubernetes_namespace(),
    )
    # bouncing status can be inferred from app_count, ref get_bouncing_status
    pod_list = kubernetes_tools.pods_for_service_instance(
        service=job_config.service,
        instance=job_config.instance,
        kube_client=kube_client,
        namespace=job_config.get_kubernetes_namespace(),
    )
    replicaset_list = kubernetes_tools.replicasets_for_service_instance(
        service=job_config.service,
        instance=job_config.instance,
        kube_client=kube_client,
        namespace=job_config.get_kubernetes_namespace(),
    )
    # For the purpose of active_shas/app_count, don't count replicasets that are at 0/0.
    actually_running_replicasets = filter_actually_running_replicasets(replicaset_list)
    active_shas = kubernetes_tools.get_active_shas_for_service(
        [app, *pod_list, *actually_running_replicasets]
    )
    kstatus["app_count"] = len(active_shas)
    kstatus["desired_state"] = job_config.get_desired_state()
    kstatus["bounce_method"] = job_config.get_bounce_method()
    kstatus["active_shas"] = list(active_shas)

    job_status(
        kstatus=kstatus,
        client=kube_client,
        namespace=job_config.get_kubernetes_namespace(),
        job_config=job_config,
        verbose=verbose,
        pod_list=pod_list,
        replicaset_list=replicaset_list,
    )

    if (
        job_config.is_autoscaling_enabled() is True
        and job_config.get_autoscaling_params().get("decision_policy", "") != "bespoke"  # type: ignore
    ):
        try:
            kstatus["autoscaling_status"] = autoscaling_status(
                kube_client, job_config, job_config.get_kubernetes_namespace()
            )
        except Exception as e:
            kstatus[
                "error_message"
            ] = f"Unknown error occurred while fetching autoscaling status. Please contact #compute-infra for help: {e}"

    evicted_count = 0
    for pod in pod_list:
        if pod.status.reason == "Evicted":
            evicted_count += 1
    kstatus["evicted_count"] = evicted_count

    if include_smartstack or include_envoy:
        service_namespace_config = kubernetes_tools.load_service_namespace_config(
            service=service,
            namespace=job_config.get_nerve_namespace(),
            soa_dir=settings.soa_dir,
        )
        if "proxy_port" in service_namespace_config:
            if include_smartstack:
                kstatus["smartstack"] = mesh_status(
                    service=service,
                    service_mesh=ServiceMesh.SMARTSTACK,
                    instance=job_config.get_nerve_namespace(),
                    job_config=job_config,
                    service_namespace_config=service_namespace_config,
                    pods=pod_list,
                    should_return_individual_backends=verbose > 0,
                    settings=settings,
                )
            if include_envoy:
                kstatus["envoy"] = mesh_status(
                    service=service,
                    service_mesh=ServiceMesh.ENVOY,
                    instance=job_config.get_nerve_namespace(),
                    job_config=job_config,
                    service_namespace_config=service_namespace_config,
                    pods=pod_list,
                    should_return_individual_backends=verbose > 0,
                    settings=settings,
                )
    return kstatus


def instance_status(
    service: str,
    instance: str,
    verbose: int,
    include_smartstack: bool,
    include_envoy: bool,
    use_new: bool,
    instance_type: str,
    settings: Any,
) -> Mapping[str, Any]:
    status = {}

    if not can_handle(instance_type):
        raise RuntimeError(
            f"Unknown instance type: {instance_type!r}, "
            f"can handle: {INSTANCE_TYPES}"
        )

    if instance_type in INSTANCE_TYPES_CR:
        status[instance_type] = cr_status(
            service=service,
            instance=instance,
            instance_type=instance_type,
            verbose=verbose,
            kube_client=settings.kubernetes_client,
        )

    if instance_type in INSTANCE_TYPES_K8S:
        if use_new:
            status["kubernetes_v2"] = kubernetes_status_v2(
                service=service,
                instance=instance,
                instance_type=instance_type,
                verbose=verbose,
                include_smartstack=include_smartstack,
                include_envoy=include_envoy,
                settings=settings,
            )
        else:
            status["kubernetes"] = kubernetes_status(
                service=service,
                instance=instance,
                instance_type=instance_type,
                verbose=verbose,
                include_smartstack=include_smartstack,
                include_envoy=include_envoy,
                settings=settings,
            )

    return status


def ready_replicas_from_replicaset(replicaset: V1ReplicaSet) -> int:
    try:
        ready_replicas = replicaset.status.ready_replicas
        if ready_replicas is None:
            ready_replicas = 0
    except AttributeError:
        ready_replicas = 0

    return ready_replicas


def kubernetes_mesh_status(
    service: str,
    instance: str,
    instance_type: str,
    settings: Any,
    include_smartstack: bool = True,
    include_envoy: bool = True,
) -> Mapping[str, Any]:

    if not include_smartstack and not include_envoy:
        raise RuntimeError("No mesh types specified when requesting mesh status")
    if instance_type not in LONG_RUNNING_INSTANCE_TYPE_HANDLERS:
        raise RuntimeError(
            f"Getting mesh status for {instance_type} instances is not supported"
        )

    config_loader = LONG_RUNNING_INSTANCE_TYPE_HANDLERS[instance_type].loader
    job_config = config_loader(
        service=service,
        instance=instance,
        cluster=settings.cluster,
        soa_dir=settings.soa_dir,
        load_deployments=True,
    )
    service_namespace_config = kubernetes_tools.load_service_namespace_config(
        service=service,
        namespace=job_config.get_nerve_namespace(),
        soa_dir=settings.soa_dir,
    )
    if "proxy_port" not in service_namespace_config:
        raise RuntimeError(
            f"Instance '{service}.{instance}' is not configured for the mesh"
        )

    kube_client = settings.kubernetes_client
    pod_list = kubernetes_tools.pods_for_service_instance(
        service=job_config.service,
        instance=job_config.instance,
        kube_client=kube_client,
        namespace=job_config.get_kubernetes_namespace(),
    )

    kmesh: Dict[str, Any] = {}
    mesh_status_kwargs = dict(
        service=service,
        instance=job_config.get_nerve_namespace(),
        job_config=job_config,
        service_namespace_config=service_namespace_config,
        pods=pod_list,
        should_return_individual_backends=True,
        settings=settings,
    )
    if include_smartstack:
        kmesh["smartstack"] = mesh_status(
            service_mesh=ServiceMesh.SMARTSTACK, **mesh_status_kwargs,
        )
    if include_envoy:
        kmesh["envoy"] = mesh_status(
            service_mesh=ServiceMesh.ENVOY, **mesh_status_kwargs,
        )

    return kmesh
