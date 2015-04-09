#!/usr/bin/env python

from paasta_tools.marathon_tools import get_marathon_client
from paasta_tools.mesos_tools import fetch_mesos_stats


def get_mesos_status():
    """Gathers information about the mesos cluster.
    :return: string containing the status
    """
    output = []
    metrics = fetch_mesos_stats()
    output.append("Mesos:")
    output.append(
        "    cpus: %d total => %d used, %d available" %
        (
            metrics['master/cpus_total'],
            metrics['master/cpus_used'],
            metrics['master/cpus_total']-metrics['master/cpus_used']
        )
    )
    output.append(
        "    memory: %0.2f GB total => %0.2f GB used, %0.2f GB available" %
        (
            metrics['master/mem_total']/1024,
            metrics['master/mem_used']/1024,
            (metrics['master/mem_total']-metrics['master/mem_used'])/1024
        )
    )
    output.append(
        "    tasks: %d running, %d staging, %d starting" %
        (
            metrics['master/tasks_running'],
            metrics['master/tasks_staging'],
            metrics['master/tasks_starting'],
        )
    )
    output.append(
        "    slaves: %d active, %d inactive" %
        (
            metrics['master/slaves_active'],
            metrics['master/slaves_inactive'],
        )
    )
    return "\n".join(output)


def get_marathon_status():
    """Gathers information about marathon.
    :return: string containing the status."""
    output = []
    client = get_marathon_client()
    output.append("Marathon:")
    output.append("    %d apps" % len(client.list_apps()))
    output.append("    %d tasks" % len(client.list_tasks()))
    output.append("    %d deployments" % len(client.list_deployments()))
    return "\n".join(output)


def main():
    print get_mesos_status()
    print get_marathon_status()


if __name__ == '__main__':
    main()
