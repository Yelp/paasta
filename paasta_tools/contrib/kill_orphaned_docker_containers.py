#!/usr/bin/env python
import argparse

from docker import Client

from paasta_tools import mesos_tools
from paasta_tools.utils import get_docker_host


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            'Cross references running containers with task ids from the mesos slave',
            ' and optionally kills them.'
        )
    )
    parser.add_argument('-f', '--force', help="Actually kill the containers. (defaults to dry-run)")
    args = parser.parse_args()
    return args


def get_running_task_ids_from_mesos_slave():
    state = mesos_tools.get_local_slave_state()
    frameworks = state.get('frameworks')
    executors = [ex for fw in frameworks for ex in fw.get('executors', [])
                 if u'TASK_RUNNING' in [t[u'state'] for t in ex.get('tasks', [])]]
    return set([e["id"] for e in executors])


def get_running_mesos_docker_containers(client):
    running_containers = client.containers()
    return [container for container in running_containers if "mesos-" in container["Names"][0]]


def get_docker_client():
    base_docker_url = get_docker_host()
    return Client(base_url=base_docker_url)


def main():
    args = parse_args()
    docker_client = get_docker_client()
    running_mesos_task_ids = get_running_task_ids_from_mesos_slave()
    running_mesos_docker_containers = get_running_mesos_docker_containers(docker_client)
    print running_mesos_task_ids

    for container in running_mesos_docker_containers:
        mesos_task_id = mesos_tools.get_mesos_id_from_container(container=container, client=docker_client)
        print mesos_task_id
        if mesos_task_id not in running_mesos_task_ids:
            if args.force:
                print "Killing %s. (%s)" % (container["Names"][0], mesos_task_id)
                docker_client.kill(container)
            else:
                print "Would kill %s. (%s)" % (container["Names"][0], mesos_task_id)
        else:
            print "Not killing %s. (%s)" % (container["Names"][0], mesos_task_id)


if __name__ == "__main__":
    main()
