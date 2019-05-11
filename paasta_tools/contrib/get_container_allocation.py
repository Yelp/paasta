#!/usr/bin/env python
import a_sync

from paasta_tools import mesos_tools
from paasta_tools.utils import load_system_paasta_config


def get_mesos_container_id_from_task(task):
    for status in task['statuses']:
        container_id = status.get('container_status', {}).get('container_id', {}).get('value')
        if container_id:
            return container_id


def get_paasta_service_instance_from_task(task):
    docker_params = task['container'].get('docker', {}).get('parameters', [])
    service, instance = None, None
    for param in docker_params:
        if param['key'] == 'label':
            label = param['value']
            if label.startswith('paasta_service='):
                service = label.split('=')[1]
            if label.startswith('paasta_instance='):
                instance = label.split('=')[1]
    return service, instance


@a_sync.to_blocking
async def get_task_allocation_info():
    tasks = await mesos_tools.get_cached_list_of_running_tasks_from_frameworks()
    info_list = []
    for task in tasks:
        info = {'resources': task['resources']}
        info['mesos_container_id'] = get_mesos_container_id_from_task(task)
        service, instance = get_paasta_service_instance_from_task(task)
        info['service'] = service
        info['instance'] = instance
        info_list.append(info)


def main():
    cluster = load_system_paasta_config().get_cluster()
    info_list = get_task_allocation_info()
    for info in info_list:
        info['cluster'] = cluster
        print(info)


if __name__ == '__main__':
    main()
