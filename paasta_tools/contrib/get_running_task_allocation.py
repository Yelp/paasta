#!/opt/venvs/paasta-tools/bin/python
import argparse
import json
import time
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import NamedTuple
from typing import Optional
from typing import Tuple

from kubernetes.client import V1Pod
from kubernetes.client import V1ResourceRequirements

from paasta_tools import kubernetes_tools
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.utils import load_system_paasta_config


MAIN_CONTAINER_TYPE = "main"


class TaskAllocationInfo(NamedTuple):
    paasta_service: str
    paasta_instance: str
    container_type: str
    paasta_pool: str
    resources: Mapping[str, float]
    start_time: float
    docker_id: str
    pod_name: str
    pod_ip: str
    host_ip: str
    git_sha: str
    config_sha: str
    mesos_container_id: Optional[
        str
    ]  # XXX(luisp): can we delete this now or do we need to cleanup splunk usages first?
    namespace: Optional[str]


def get_all_running_kubernetes_pods(
    kube_client: KubeClient, namespace: str
) -> Iterable[V1Pod]:
    running = []
    for pod in kubernetes_tools.get_all_pods(kube_client, namespace):
        if kubernetes_tools.get_pod_status(pod) == kubernetes_tools.PodStatus.RUNNING:
            running.append(pod)
    return running


def get_kubernetes_resource_request_limit(
    resources: V1ResourceRequirements,
) -> Dict[str, float]:
    if not resources:
        requests: Dict[str, str] = {}
        limits: Dict[str, str] = {}
    else:
        requests = resources.requests or {}
        limits = resources.limits or {}

    parsed_requests = kubernetes_tools.parse_container_resources(requests)
    parsed_limits = kubernetes_tools.parse_container_resources(limits)
    return {
        "cpus": parsed_requests.cpus,
        "mem": parsed_requests.mem,
        "disk": parsed_requests.disk,
        "cpus_limit": parsed_limits.cpus,
    }


def get_pod_pool(
    kube_client: KubeClient,
    pod: V1Pod,
) -> str:
    node = kubernetes_tools.get_pod_node(kube_client, pod, cache_nodes=True)
    pool = "default"
    if node:
        if node.metadata.labels:
            pool = node.metadata.labels.get("paasta.yelp.com/pool", "default")
    return pool


def get_kubernetes_metadata(
    pod: V1Pod,
) -> Tuple[
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
]:
    labels = pod.metadata.labels or {}
    pod_name = pod.metadata.name
    pod_ip = pod.status.pod_ip
    host_ip = pod.status.host_ip
    service = labels.get("paasta.yelp.com/service")
    instance = labels.get("paasta.yelp.com/instance")
    git_sha = labels.get("paasta.yelp.com/git_sha")
    config_sha = labels.get("paasta.yelp.com/config_sha")
    return service, instance, pod_name, pod_ip, host_ip, git_sha, config_sha


def get_container_type(container_name: str, instance_name: str) -> str:
    """
    To differentiate between main service containers and sidecars
    """
    if instance_name and container_name == kubernetes_tools.sanitise_kubernetes_name(
        instance_name
    ):
        return MAIN_CONTAINER_TYPE
    else:
        return container_name


def get_kubernetes_task_allocation_info(
    namespace: str, client: KubeClient
) -> Iterable[TaskAllocationInfo]:
    pods = get_all_running_kubernetes_pods(client, namespace)
    info_list = []
    for pod in pods:
        (
            service,
            instance,
            pod_name,
            pod_ip,
            host_ip,
            git_sha,
            config_sha,
        ) = get_kubernetes_metadata(pod)
        pool = get_pod_pool(client, pod)
        name_to_info: MutableMapping[str, Any] = {}
        for container in pod.spec.containers:
            name_to_info[container.name] = {
                "resources": get_kubernetes_resource_request_limit(container.resources),
                "container_type": get_container_type(container.name, instance),
                "pod_name": pod_name,
                "pod_ip": pod_ip,
                "host_ip": host_ip,
                "git_sha": git_sha,
                "config_sha": config_sha,
            }
        container_statuses = pod.status.container_statuses or []
        for container in container_statuses:
            if not container.state.running:
                continue
            # docker://abcdef
            docker_id = (
                container.container_id.split("/")[-1]
                if container.container_id
                else None
            )
            update = {
                "docker_id": docker_id,
                "start_time": container.state.running.started_at.timestamp(),
            }
            name_to_info[container.name].update(update)
        for info in name_to_info.values():
            info_list.append(
                TaskAllocationInfo(
                    paasta_service=service,
                    paasta_instance=instance,
                    container_type=info.get("container_type"),
                    paasta_pool=pool,
                    resources=info.get("resources"),
                    start_time=info.get("start_time"),
                    docker_id=info.get("docker_id"),
                    pod_name=info.get("pod_name"),
                    pod_ip=info.get("pod_ip"),
                    host_ip=info.get("host_ip"),
                    git_sha=info.get("git_sha"),
                    config_sha=info.get("config_sha"),
                    mesos_container_id=None,
                    namespace=namespace,
                )
            )

    return info_list


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "--additional-namespaces-exclude",
        help="full names of namespaces to not fetch allocation info for those that don't match --namespace-prefix-exlude",
        dest="additional_namespaces_exclude",
        nargs="+",
        default=[],
    )
    args = parser.parse_args()

    args.additional_namespaces_exclude = set(args.additional_namespaces_exclude)

    return args


def get_unexcluded_namespaces(
    namespaces: List[str], excluded_namespaces: List[str]
) -> List[str]:
    return [n for n in namespaces if n not in excluded_namespaces]


def main(args: argparse.Namespace) -> None:
    cluster = load_system_paasta_config().get_cluster()
    kube_client = KubeClient()
    all_namespaces = kubernetes_tools.get_all_namespaces(kube_client)
    for matching_namespace in get_unexcluded_namespaces(
        all_namespaces,
        args.additional_namespaces_exclude,
    ):
        display_task_allocation_info(cluster, matching_namespace, kube_client)


def display_task_allocation_info(
    cluster: str,
    namespace: str,
    kube_client: KubeClient,
) -> None:
    info_list = get_kubernetes_task_allocation_info(namespace, kube_client)
    timestamp = time.time()
    for info in info_list:
        info_dict = info._asdict()
        info_dict["cluster"] = cluster
        info_dict["timestamp"] = timestamp
        print(json.dumps(info_dict))


if __name__ == "__main__":
    args = parse_args()
    main(args)
