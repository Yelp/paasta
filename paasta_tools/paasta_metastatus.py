#!/usr/bin/env python

from paasta_tools import marathon_tools
from paasta_tools.mesos_tools import fetch_mesos_stats
from paasta_tools.mesos_tools import fetch_mesos_state_from_leader
from paasta_tools.mesos_tools import get_mesos_quorum
from paasta_tools.mesos_tools import get_zookeeper_config
from paasta_tools.mesos_tools import get_number_of_mesos_masters
from paasta_tools.utils import PaastaColors
from paasta_tools.mesos_tools import MesosCliException
import sys


class MesosHealthException(Exception):
    def __init__(self, message):
        super(MesosHealthException, self).__init__(message)

class MesosQuorumException(MesosHealthException):
    def __init__(self, message):
        super(MesosQuorumException, self).__init__(message)

class MesosCPUException(MesosHealthException):
    def __init__(self, message):
        super(MesosCPUException, self).__init__(message)


def get_configured_quorum_size(state):
    """ Gets the quorum size from mesos state """
    return get_mesos_quorum(state)

def get_num_masters(state):
    """ Gets the number of masters from mesos state """
    return get_number_of_mesos_masters(get_zookeeper_config(state))

def masters_for_quorum(masters):
    return (masters/2) + 1

def get_mesos_cpu_status(metrics):
    """Takes in the mesos metrics and analyzes them, returning the status
    :param metrics: mesos metrics dictionary
    :returns: Tuple of the output array and is_ok bool
    """
    total = metrics['master/cpus_total']
    used = metrics['master/cpus_used']
    available = metrics['master/cpus_total'] - metrics['master/cpus_used']
    return total, used, available


def quorum_ok(masters, quorum):
    return masters >= quorum

def cpu_ok(percent_available):
    return percent_available > 10

def get_mesos_status():
    """Gathers information about the mesos cluster.
    :return: tuple of a string containing the status and a bool representing if it is ok or not
    """
    output = []
    output.append("Mesos:")
    state = fetch_mesos_state_from_leader()

    #check the quorum
    masters, quorum = get_num_masters(state), get_configured_quorum_size(state)
    output.append(
            "    Quorum: masters: %d configured quorum: %d " % (masters, quorum)
    )
    if not quorum_ok(masters, quorum):
        output.append(PaastaColors.red(
            "    CRITICAL: Number of masters (%d) less than configured quorum(%d)."
            % (masters, quorum)
        ))
        raise_exception(MesosQuorumException, output)

    metrics = fetch_mesos_stats()

    #check cpu usage
    total, used, available = get_mesos_cpu_status(metrics)
    percent_available = round(available / float(total) * 100.0, 2)
    output.append(
            "    cpus: total: %d used: %d available: %d percent_available: %d"
            % (total, used, available, percent_available)
    )
    if not cpu_ok(percent_available):
        output.append(PaastaColors.red(
            "    CRITICAL: Less than 10%% CPUs available. (Currently at %.2f%%)"
            % percent_available
        ))
        raise_exception(MesosCPUException, output)

    #check memory usage
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

def raise_exception(exception_class, messages):
    raise exception_class(("\n").join(messages))


def get_marathon_status():
    """Gathers information about marathon.
    :return: string containing the status."""
    output = []
    marathon_config = marathon_tools.load_marathon_config()
    client = marathon_tools.get_marathon_client(
        marathon_config['url'],
        marathon_config['user'],
        marathon_config['pass']
    )
    output.append("Marathon:")
    output.append("    %d apps" % len(client.list_apps()))
    output.append("    %d tasks" % len(client.list_tasks()))
    output.append("    %d deployments" % len(client.list_deployments()))
    return "\n".join(output)


def main():
    try:
        get_mesos_status()
    except (MesosCliException, MesosHealthException) as a:
        print a.message
        sys.exit(2)
    print get_marathon_status()


if __name__ == '__main__':
    main()
