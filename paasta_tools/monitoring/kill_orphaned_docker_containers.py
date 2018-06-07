#!/usr/bin/env python
import argparse
import sys

import a_sync

from paasta_tools import mesos_tools
from paasta_tools.utils import get_docker_client
from paasta_tools.utils import get_running_mesos_docker_containers
from paasta_tools.utils import paasta_print


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            'Cross references running containers with task ids from the mesos slave'
            ' and optionally kills them.'
        ),
    )
    parser.add_argument(
        '-f', '--force', action="store_true",
        help="Actually kill the containers. (defaults to dry-run)",
    )
    args = parser.parse_args()
    return args


@a_sync.to_blocking
async def main():
    args = parse_args()
    docker_client = get_docker_client()

    running_mesos_task_ids = [task["id"] for task in mesos_tools.filter_running_tasks(
        await mesos_tools.get_running_tasks_from_frameworks(''),
    )]
    running_mesos_docker_containers = get_running_mesos_docker_containers()

    orphaned_containers = []
    for container in running_mesos_docker_containers:
        mesos_task_id = mesos_tools.get_mesos_id_from_container(
            container=container, client=docker_client,
        )
        if mesos_task_id not in running_mesos_task_ids:
            orphaned_containers.append((container["Names"][0].strip("/"), mesos_task_id))

    if orphaned_containers:
        paasta_print("CRIT: Docker containers are orphaned: {}{}".format(
            ", ".join(
                f"{container_name} ({mesos_task_id})"
                for container_name, mesos_task_id in orphaned_containers
            ), " and will be killed" if args.force else "",
        ))
        if args.force:
            for container_name, mesos_task_id in orphaned_containers:
                docker_client.kill(container_name)
        sys.exit(1)
    else:
        paasta_print("OK: All mesos task IDs accounted for")
        sys.exit(0)


if __name__ == "__main__":
    main()
