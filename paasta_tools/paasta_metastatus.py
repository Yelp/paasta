#!/usr/bin/env python

from paasta_tools import marathon_tools
from paasta_tools.mesos_tools import fetch_mesos_stats
from paasta_tools.mesos_tools import fetch_mesos_state_from_leader
from paasta_tools.mesos_tools import get_mesos_quorum
from paasta_tools.mesos_tools import get_zookeeper_config
from paasta_tools.mesos_tools import get_number_of_mesos_masters
from paasta_tools.utils import PaastaColors


def get_mesos_masters_status(state):
    """Returns a tuple containing the information about mesos
    masters.
    :param state: mesos state dictionary"""
    quorum = get_mesos_quorum(state)
    num_of_masters = get_number_of_mesos_masters(get_zookeeper_config(state))

    return num_of_masters, quorum


def get_mesos_cpu_status(metrics):
    """Takes in the mesos metrics and analyzes them, returning the status
    :param metrics: mesos metrics dictionary
    :returns: Tuple of the output array and is_ok bool
    """
    is_ok = True
    output = []
    total = metrics['master/cpus_total']
    used = metrics['master/cpus_used']
    available = metrics['master/cpus_total'] - metrics['master/cpus_used']
    percent_available = round(available / float(total) * 100.0, 2)
    output.append("    cpus: %d total => %d used, %d available" % (total, used, available))
    if percent_available < 10:
        is_ok = False
        output.append(PaastaColors.red(
            "    CRITICAL: Less than 10%% CPUs available. (Currently at %.2f%%)" % percent_available
        ))
    return output, is_ok


def get_mesos_status():
    """Gathers information about the mesos cluster.
    :return: tuple of a string containing the status and a bool representing if it is ok or not
    """
    output = []
    metrics = fetch_mesos_stats()
    state = fetch_mesos_state_from_leader()
    output.append("Mesos:")
    cpu_output, is_cpu_ok = get_mesos_cpu_status(metrics)
    output.extend(cpu_output)
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
        "    masters: %d masters (%d need for quorum)" %
        get_mesos_masters_status(state)
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
    marathon_config = marathon_tools.load_marathon_config()
    client = marathon_tools.get_marathon_client(
        marathon_config.get_url(),
        marathon_config.get_username(),
        marathon_config.get_password()
    )
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
