#!/usr/bin/env python

from collections import Counter
from paasta_tools import marathon_tools
from paasta_tools.mesos_tools import fetch_mesos_stats
from paasta_tools.mesos_tools import fetch_mesos_state_from_leader
from paasta_tools.mesos_tools import get_mesos_quorum
from paasta_tools.mesos_tools import get_zookeeper_config
from paasta_tools.mesos_tools import get_number_of_mesos_masters
from paasta_tools.utils import PaastaColors
from paasta_tools.mesos_tools import MissingMasterException
import sys


def get_num_masters(state):
    """ Gets the number of masters from mesos state """
    return get_number_of_mesos_masters(get_zookeeper_config(state))


def get_mesos_cpu_status(metrics):
    """Takes in the mesos metrics and analyzes them, returning the status
    :param metrics: mesos metrics dictionary
    :returns: Tuple of the output array and is_ok bool
    """

    total = metrics['master/cpus_total']
    used = metrics['master/cpus_used']
    available = total - used
    return total, used, available


def quorum_ok(masters, quorum):
    return masters >= quorum


def check_threshold(percent_available, threshold):
    return percent_available > threshold


def percent_available(total, available):
    return round(available / float(total) * 100.0, 2)


def assert_cpu_health(metrics, threshold=10):
    total, used, available = get_mesos_cpu_status(metrics)
    perc_available = percent_available(total, available)
    if check_threshold(perc_available, threshold):
        return ("cpus: total: %d used: %d available: %d percent_available: %d"
                % (total, used, available, perc_available),
                True)
    else:
        return (PaastaColors.red(
                "CRITICAL: Less than %d%% CPUs available. (Currently at %.2f%%)"
                % (threshold, perc_available)),
                False)


def assert_memory_health(metrics, threshold=10):
    total = metrics['master/mem_total']/float(1024)
    used = metrics['master/mem_used']/float(1024)
    available = total - used
    perc_available = percent_available(total, available)

    if check_threshold(perc_available, threshold):
        return ("memory: total: %0.2f GB used: %0.2f GB available: %0.2f GB"
                % (total, used, available),
                True)
    else:
        return (PaastaColors.red(
                "CRITICAL: Less than %d%% memory available. (Currently at %.2f%%)"
                % (threshold, perc_available)),
                False)


def assert_tasks_running(metrics):
    running = metrics['master/tasks_running']
    staging = metrics['master/tasks_staging']
    starting = metrics['master/tasks_starting']
    return ("tasks: running: %d staging: %d starting: %d"
            % (running, staging, starting),
            True)


def assert_no_duplicate_frameworks(state):
    """Takes in the current state of the Mesos master and pulls out the running frameworks.
    Counts up the number of running instances of each framework and reports an error
    if there are duplicate frameworks running.
    param state: the state info from the Mesos master
    returns: log messages and a not-ok status if duplicate running frameworks were found, else an ok status
    """
    frameworks = state['frameworks']
    framework_counts = Counter()
    output = []
    ok = True

    for fw in frameworks:
        framework_counts[fw.get('name')] += 1

    for fw in framework_counts:
        if framework_counts[fw] > 1:
            ok = False
            output.append(PaastaColors.red(
                          "CRITICAL: Framework %s has %d instances running--expected no more than 1."
                          % (fw, framework_counts[fw])))
    return (("\n").join(output), ok)


def assert_slave_health(metrics):
    active, inactive = metrics['master/slaves_active'], metrics['master/slaves_inactive']
    return ("slaves: active: %d inactive: %d"
            % (active, inactive),
            True)


def assert_quorum_size(state):
    masters, quorum = get_num_masters(state), get_mesos_quorum(state)
    if quorum_ok(masters, quorum):
        return ("quorum: masters: %d configured quorum: %d "
                % (masters, quorum),
                True)
    else:
        return (PaastaColors.red(
                "CRITICAL: Number of masters (%d) less than configured quorum(%d)."
                % (masters, quorum)),
                False)


def get_mesos_status():
    """Gathers information about the mesos cluster.
       :return: tuple of a string containing the status and a bool representing if it is ok or not
    """

    state = fetch_mesos_state_from_leader()
    cluster_outputs, cluster_ok = run_healthchecks_with_param(state, [assert_quorum_size,
                                                                      assert_no_duplicate_frameworks])

    metrics = fetch_mesos_stats()
    metrics_outputs, metrics_ok = run_healthchecks_with_param(metrics, [
        assert_cpu_health,
        assert_memory_health,
        assert_slave_health,
        assert_tasks_running])

    cluster_outputs.extend(metrics_outputs)
    cluster_ok.extend(metrics_ok)
    return cluster_outputs, cluster_ok


def run_healthchecks_with_param(param, healthcheck_functions):
    outputs, oks = [], []
    for healthcheck in healthcheck_functions:
        output, ok = healthcheck(param)
        outputs.append(output)
        oks.append(ok)
    return outputs, oks


def assert_marathon_apps(client):
    num_apps = len(client.list_apps())
    if num_apps < 1:
        return (PaastaColors.red(
            "CRITICAL: No marathon apps running"),
            False)
    else:
        return ("marathon apps: %d"
                % num_apps,
                True)


def assert_marathon_tasks(client):
    num_tasks = len(client.list_tasks())
    return ("marathon tasks: %d"
            % num_tasks,
            True)


def assert_marathon_deployments(client):
    num_deployments = len(client.list_deployments())
    return ("marathon deployments: %d"
            % num_deployments,
            True)


def get_marathon_status():
    """ Gathers information about marathon.
    :return: string containing the status.  """
    marathon_config = marathon_tools.load_marathon_config()
    client = marathon_tools.get_marathon_client(
        marathon_config.get_url(),
        marathon_config.get_username(),
        marathon_config.get_password()
    )
    outputs, oks = run_healthchecks_with_param(client, [
        assert_marathon_apps,
        assert_marathon_tasks,
        assert_marathon_deployments])
    return outputs, oks


def main():
    try:
        mesos_outputs, mesos_oks = get_mesos_status()
        marathon_outputs, marathon_oks = get_marathon_status()
    except MissingMasterException as e:
        # if we can't connect to master at all,
        # then bomb out early
        print(PaastaColors.red("CRITICAL:  %s" % e.message))
        sys.exit(2)

    print("Mesos Status:")
    print(("\n").join(map(lambda x: "  %s" % x, mesos_outputs)))
    print("Marathon Status:")
    print(("\n").join(map(lambda x: "  %s" % x, marathon_outputs)))

    if False in mesos_oks or False in marathon_oks:
        sys.exit(2)


if __name__ == '__main__':
    main()
