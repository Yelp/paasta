#!/usr/bin/env python
import time

import a_sync
import simplejson as json

from paasta_tools import mesos_tools
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print


def get_container_info_from_task(task):
    for status in task["statuses"]:
        if status["state"] != "TASK_RUNNING":
            continue
        container_id = (
            status.get("container_status", {}).get("container_id", {}).get("value")
        )
        time_start = status.get("timestamp")
        return container_id, time_start
    return None, None


def get_paasta_service_instance_from_task(task):
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


async def get_pool_from_task(task):
    attributes = (await task.slave())["attributes"]
    return attributes.get("pool", "default")


@a_sync.to_blocking
async def get_task_allocation_info():
    tasks = await mesos_tools.get_cached_list_of_running_tasks_from_frameworks()
    info_list = []
    for task in tasks:
        info = {"resources": task["resources"]}
        info["mesos_container_id"], info["start_time"] = get_container_info_from_task(
            task
        )
        info["paasta_service"], info[
            "paasta_instance"
        ] = get_paasta_service_instance_from_task(task)
        info["paasta_pool"] = await get_pool_from_task(task)
        info_list.append(info)
    return info_list


def main():
    cluster = load_system_paasta_config().get_cluster()
    info_list = get_task_allocation_info()
    timestamp = time.time()
    for info in info_list:
        info["cluster"] = cluster
        info["timestamp"] = timestamp
        paasta_print(json.dumps(info))


if __name__ == "__main__":
    main()
