#!/usr/bin/env python
import argparse
import time
from typing import Any
from typing import Iterable
from typing import Mapping
from typing import MutableMapping
from typing import NamedTuple
from typing import Optional
from typing import Tuple

import a_sync
import simplejson as json
from kubernetes.client import V1Pod
from kubernetes.client import V1ResourceRequirements

from paasta_tools import kubernetes_tools
from paasta_tools import mesos_tools
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.mesos.exceptions import SlaveDoesNotExist
from paasta_tools.mesos.task import Task
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
    mesos_container_id: str  # Because Mesos task info does not have docker id


def get_container_info_from_mesos_task(
    task: Task,
) -> Tuple[Optional[str], Optional[float]]:
    for status in task["statuses"]:
        if status["state"] != "TASK_RUNNING":
            continue
        container_id = (
            status.get("container_status", {}).get("container_id", {}).get("value")
        )
        time_start = status.get("timestamp")
        return container_id, time_start
    return None, None


def get_paasta_service_instance_from_mesos_task(
    task: Task,
) -> Tuple[Optional[str], Optional[str]]:
    try:
        docker_params = task["container"].get("docker", {}).get("parameters", [])
    except KeyError:
        return None, None
    service, instance = None, None
    for param in docker_params:
        if param["key"] == "label":
            label = param["value"]
            if label.startswith("paasta_service="):
                service = label.split("=")[1]
            if label.startswith("paasta_instance="):
                instance = label.split("=")[1]
    return service, instance


async def get_pool_from_mesos_task(task: Task) -> Optional[str]:
    try:
        attributes = (await task.slave())["attributes"]
        return attributes.get("pool", "default")
    except SlaveDoesNotExist:
        return None


@a_sync.to_blocking
async def get_mesos_task_allocation_info() -> Iterable[TaskAllocationInfo]:
    tasks = await mesos_tools.get_cached_list_of_running_tasks_from_frameworks()
    info_list = []
    for task in tasks:
        mesos_container_id, start_time = get_container_info_from_mesos_task(task)
        paasta_service, paasta_instance = get_paasta_service_instance_from_mesos_task(
            task
        )
        paasta_pool = await get_pool_from_mesos_task(task)
        info_list.append(
            TaskAllocationInfo(
                paasta_service=paasta_service,
                paasta_instance=paasta_instance,
                container_type=MAIN_CONTAINER_TYPE,
                paasta_pool=paasta_pool,
                resources=task["resources"],
                start_time=start_time,
                docker_id=None,
                mesos_container_id=mesos_container_id,
            )
        )
    return info_list


def get_all_running_kubernetes_pods(
    kube_client: KubeClient, namespace: str
) -> Iterable[V1Pod]:
    running = []
    for pod in kubernetes_tools.get_all_pods(kube_client, namespace):
        if kubernetes_tools.get_pod_status(pod) == kubernetes_tools.PodStatus.RUNNING:
            running.append(pod)
    return running


def get_kubernetes_resource_request(
    resources: V1ResourceRequirements,
) -> Mapping[str, float]:
    if not resources:
        requests: Mapping[str, str] = {}
    else:
        requests = resources.requests or {}

    parsed = kubernetes_tools.parse_container_resources(requests)
    return {
        "cpus": parsed.cpus,
        "mem": parsed.mem,
        "disk": parsed.disk,
    }


def get_kubernetes_metadata(
    pod: V1Pod,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    labels = pod.metadata.labels or {}
    node_selectors = pod.spec.node_selector or {}
    service = labels.get("paasta.yelp.com/service")
    instance = labels.get("paasta.yelp.com/instance")
    pool = node_selectors.get("yelp.com/pool", "default")
    return service, instance, pool


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


def get_kubernetes_task_allocation_info(namespace: str) -> Iterable[TaskAllocationInfo]:
    client = KubeClient()
    pods = get_all_running_kubernetes_pods(client, namespace)
    info_list = []
    for pod in pods:
        service, instance, pool = get_kubernetes_metadata(pod)
        name_to_info: MutableMapping[str, Any] = {}
        for container in pod.spec.containers:
            name_to_info[container.name] = {
                "resources": get_kubernetes_resource_request(container.resources),
                "container_type": get_container_type(container.name, instance),
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
                    mesos_container_id=None,
                )
            )

    return info_list


def get_task_allocation_info(
    scheduler: str, namespace: str
) -> Iterable[TaskAllocationInfo]:
    if scheduler == "mesos":
        return get_mesos_task_allocation_info()
    elif scheduler == "kubernetes":
        return get_kubernetes_task_allocation_info(namespace)
    else:
        return []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "--scheduler",
        help="Scheduler to get task info from",
        dest="scheduler",
        default="mesos",
        choices=["mesos", "kubernetes"],
    )
    parser.add_argument(
        "--namespace-prefix",
        help="prefix of the namespace to fetch the logs for"
        "Used only when scheduler is kubernetes",
        dest="namespace_prefix",
        default="paasta",
    )
    return parser.parse_args()


def main(args: argparse.Namespace) -> None:
    cluster = load_system_paasta_config().get_cluster()
    if args.scheduler == "mesos":
        display_task_allocation_info(cluster, args.scheduler, args.namespace_prefix)
    else:
        client = KubeClient()
        all_namespaces = kubernetes_tools.get_all_namespaces(client)
        matching_namespaces = [
            n for n in all_namespaces if n.startswith(args.namespace_prefix)
        ]
        for matching_namespace in matching_namespaces:
            display_task_allocation_info(cluster, args.scheduler, matching_namespace)


def display_task_allocation_info(cluster, scheduler, namespace):
    info_list = get_task_allocation_info(scheduler, namespace)
    timestamp = time.time()
    for info in info_list:
        info_dict = info._asdict()
        info_dict["cluster"] = cluster
        info_dict["timestamp"] = timestamp
        print(json.dumps(info_dict))


if __name__ == "__main__":
    args = parse_args()
    main(args)
