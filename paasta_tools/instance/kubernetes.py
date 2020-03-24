from typing import Any
from typing import Dict
from typing import Mapping
from typing import MutableMapping
from typing import Sequence

import a_sync
import pytz
from kubernetes.client import V1Pod
from kubernetes.client import V1ReplicaSet
from kubernetes.client.rest import ApiException

from paasta_tools import cassandracluster_tools
from paasta_tools import flink_tools
from paasta_tools import kafkacluster_tools
from paasta_tools import kubernetes_tools
from paasta_tools import marathon_tools
from paasta_tools import smartstack_tools
from paasta_tools.cli.utils import LONG_RUNNING_INSTANCE_TYPE_HANDLERS
from paasta_tools.instance.hpa_metrics_parser import HPAMetricsParser
from paasta_tools.kubernetes_tools import get_tail_lines_for_kubernetes_container
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
from paasta_tools.smartstack_tools import KubeSmartstackReplicationChecker
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
)


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
):
    status = {}
    try:
        hpa = kube_client.autoscaling.read_namespaced_horizontal_pod_autoscaler(
            name=job_config.get_sanitised_deployment_name(), namespace=namespace
        )
        status["min_instances"] = hpa.spec.min_replicas
        status["max_instances"] = hpa.spec.max_replicas
        # Parse metrics sources, based on
        # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V2beta1ExternalMetricSource.md#v2beta1externalmetricsource
        metric_stats = []
        parser = HPAMetricsParser(hpa)
        if hpa.spec.metrics is not None:
            for metric_spec in hpa.spec.metrics:
                metric_stats.append(parser.parse_target(metric_spec))
        if hpa.status.current_metrics is not None:
            for metric_spec in hpa.status.current_metrics:
                metric_stats.append(parser.parse_current(metric_spec))
        status["metrics"] = metric_stats
        status["desired_replicas"] = hpa.status.desired_replicas
        status["last_scale_time"] = (
            hpa.status.last_scale_time.replace(tzinfo=pytz.UTC).isoformat()
            if getattr(hpa.status, "last_scale_time")
            else "N/A"
        )
    except ApiException as e:
        if e.status == 404:
            # This means that HPA doesn't exist. Return nothing.
            return {}
        error_message = f"Error while reading autoscaling information: {e}"
        raise RuntimeError(error_message)
    return status


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

        for pod in pod_list:
            container_statuses = pod.status.container_statuses or []
            containers = [
                dict(
                    name=container.name,
                    tail_lines=await get_tail_lines_for_kubernetes_container(
                        client, pod, container, num_tail_lines,
                    ),
                )
                for container in container_statuses
            ]
            kstatus["pods"].append(
                {
                    "name": pod.metadata.name,
                    "host": kubernetes_tools.get_pod_hostname(client, pod),
                    "deployed_timestamp": pod.metadata.creation_timestamp.timestamp(),
                    "phase": pod.status.phase,
                    "ready": kubernetes_tools.is_pod_ready(pod),
                    "containers": containers,
                    "reason": pod.status.reason,
                    "message": pod.status.message,
                }
            )
        for replicaset in replicaset_list:
            try:
                ready_replicas = replicaset.status.ready_replicas
                if ready_replicas is None:
                    ready_replicas = 0
            except AttributeError:
                ready_replicas = 0

            kstatus["replicasets"].append(
                {
                    "name": replicaset.metadata.name,
                    "replicas": replicaset.spec.replicas,
                    "ready_replicas": ready_replicas,
                    "create_timestamp": replicaset.metadata.creation_timestamp.timestamp(),
                }
            )

    kstatus["expected_instance_count"] = job_config.get_instances()

    app = kubernetes_tools.get_kubernetes_app_by_name(
        name=app_id, kube_client=client, namespace=namespace
    )
    deploy_status = kubernetes_tools.get_kubernetes_app_deploy_status(
        app=app, desired_instances=job_config.get_instances()
    )
    kstatus["deploy_status"] = kubernetes_tools.KubernetesDeployStatus.tostring(
        deploy_status
    )
    kstatus["running_instance_count"] = (
        app.status.ready_replicas if app.status.ready_replicas else 0
    )
    kstatus["create_timestamp"] = app.metadata.creation_timestamp.timestamp()
    kstatus["namespace"] = app.metadata.namespace


def smartstack_status(
    service: str,
    instance: str,
    job_config: LongRunningServiceConfig,
    service_namespace_config: ServiceNamespaceConfig,
    pods: Sequence[V1Pod],
    settings: Any,
    should_return_individual_backends: bool = False,
) -> Mapping[str, Any]:

    registration = job_config.get_registrations()[0]
    instance_pool = job_config.get_pool()

    smartstack_replication_checker = KubeSmartstackReplicationChecker(
        nodes=kubernetes_tools.get_all_nodes(settings.kubernetes_client),
        system_paasta_config=settings.system_paasta_config,
    )
    node_hostname_by_location = smartstack_replication_checker.get_allowed_locations_and_hosts(
        job_config
    )

    expected_smartstack_count = marathon_tools.get_expected_instance_count_for_namespace(
        service=service,
        namespace=instance,
        cluster=settings.cluster,
        instance_type_class=KubernetesDeploymentConfig,
    )
    expected_count_per_location = int(
        expected_smartstack_count / len(node_hostname_by_location)
    )
    smartstack_status: MutableMapping[str, Any] = {
        "registration": registration,
        "expected_backends_per_location": expected_count_per_location,
        "locations": [],
    }

    for location, hosts in node_hostname_by_location.items():
        synapse_host = smartstack_replication_checker.get_first_host_in_pool(
            hosts, instance_pool
        )
        sorted_backends = sorted(
            smartstack_tools.get_backends(
                registration,
                synapse_host=synapse_host,
                synapse_port=settings.system_paasta_config.get_synapse_port(),
                synapse_haproxy_url_format=settings.system_paasta_config.get_synapse_haproxy_url_format(),
            ),
            key=lambda backend: backend["status"],
            reverse=True,  # put 'UP' backends above 'MAINT' backends
        )

        matched_backends_and_pods = match_backends_and_pods(sorted_backends, pods)
        location_dict = smartstack_tools.build_smartstack_location_dict(
            location, matched_backends_and_pods, should_return_individual_backends
        )
        smartstack_status["locations"].append(location_dict)

    return smartstack_status


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


def kubernetes_status(
    service: str,
    instance: str,
    verbose: int,
    include_smartstack: bool,
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
    active_shas = kubernetes_tools.get_active_shas_for_service(
        [app, *pod_list, *replicaset_list]
    )
    kstatus["app_count"] = max(
        len(active_shas["config_sha"]), len(active_shas["git_sha"])
    )
    kstatus["desired_state"] = job_config.get_desired_state()
    kstatus["bounce_method"] = job_config.get_bounce_method()
    job_status(
        kstatus=kstatus,
        client=kube_client,
        namespace=job_config.get_kubernetes_namespace(),
        job_config=job_config,
        verbose=verbose,
        pod_list=pod_list,
        replicaset_list=replicaset_list,
    )

    kstatus["autoscaling_status"] = autoscaling_status(
        kube_client, job_config, job_config.get_kubernetes_namespace()
    )

    evicted_count = 0
    for pod in pod_list:
        if pod.status.reason == "Evicted":
            evicted_count += 1
    kstatus["evicted_count"] = evicted_count

    if include_smartstack:
        service_namespace_config = kubernetes_tools.load_service_namespace_config(
            service=job_config.get_service_name_smartstack(),
            namespace=job_config.get_nerve_namespace(),
            soa_dir=settings.soa_dir,
        )
        if "proxy_port" in service_namespace_config:
            kstatus["smartstack"] = smartstack_status(
                service=job_config.get_service_name_smartstack(),
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
        status["kubernetes"] = kubernetes_status(
            service=service,
            instance=instance,
            instance_type=instance_type,
            verbose=verbose,
            include_smartstack=include_smartstack,
            settings=settings,
        )

    return status
