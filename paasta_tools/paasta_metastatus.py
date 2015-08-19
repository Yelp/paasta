#!/usr/bin/env python
import sys

from collections import Counter, OrderedDict
from httplib2 import ServerNotFoundError

from paasta_tools import marathon_tools
from paasta_tools.chronos_tools import ChronosNotConfigured
from paasta_tools.chronos_tools import get_chronos_client
from paasta_tools.chronos_tools import load_chronos_config
from paasta_tools.marathon_tools import MarathonNotConfigured
from paasta_tools.mesos_tools import fetch_mesos_stats
from paasta_tools.mesos_tools import fetch_mesos_state_from_leader
from paasta_tools.mesos_tools import get_mesos_quorum
from paasta_tools.mesos_tools import get_zookeeper_config
from paasta_tools.mesos_tools import get_number_of_mesos_masters
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import print_with_indent
from paasta_tools.mesos_tools import MasterNotAvailableException


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
    total = metrics['master/mem_total'] / float(1024)
    used = metrics['master/mem_used'] / float(1024)
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
    """A function which asserts that there are no duplicate frameworks running, where
    frameworks are identified by their name.

    Note the extra spaces in the output strings: this is to account for the extra indentation
    we add, so we can have:
        frameworks:
          framework: marathon count: 1

    :param state: the state info from the Mesos master
    :return a tuple containing (output, ok): output is a log of the state of frameworks, ok a boolean
    indicating if there are any duplicate frameworks.
    """
    frameworks = state['frameworks']
    framework_counts = OrderedDict(sorted(Counter([fw['name'] for fw in frameworks]).items()))
    output = ["frameworks:"]
    ok = True

    for framework, count in framework_counts.iteritems():
        if count > 1:
            ok = False
            output.append(PaastaColors.red(
                          "    CRITICAL: Framework %s has %d instances running--expected no more than 1."
                          % (framework, count)))
        else:
            output.append("    framework: %s count: %d" % (framework, count))
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


def get_marathon_status(client):
    """ Gathers information about marathon.
    :return: string containing the status.  """
    outputs, oks = run_healthchecks_with_param(client, [
        assert_marathon_apps,
        assert_marathon_tasks,
        assert_marathon_deployments])
    return outputs, oks


def assert_chronos_scheduled_jobs(client):
    """
    :returns: a tuple of a string and a bool containing representing if it is ok or not
    """
    try:
        num_jobs = len(client.list())
    except ServerNotFoundError:
        num_jobs = 0
    return ("chronos jobs: %d" % num_jobs, True)


def get_chronos_status(chronos_client):
    """ Gather information about chronos.
    :return: string containing the status
    """
    outputs, oks = run_healthchecks_with_param(chronos_client, [
        assert_chronos_scheduled_jobs,
    ])
    return outputs, oks


def get_marathon_client(marathon_config):
    """ Given a MarathonConfig object, return
    a client.
    :param marathon_config: a MarathonConfig object
    :returns client: a marathon client
    """
    return marathon_tools.get_marathon_client(
        marathon_config.get_url(),
        marathon_config.get_username(),
        marathon_config.get_password()
    )


def main():
    marathon_config = None
    chronos_config = None

    # Check to see if Marathon should be running here by checking for config
    try:
        marathon_config = marathon_tools.load_marathon_config()
    except MarathonNotConfigured:
        marathon_outputs, marathon_oks = (['marathon is not configured to run here'], [True])

    # Check to see if Chronos should be running here by checking for config
    try:
        chronos_config = load_chronos_config()
    except ChronosNotConfigured:
        chronos_outputs, chronos_oks = (['chronos is not configured to run here'], [True])

    try:
        mesos_outputs, mesos_oks = get_mesos_status()
    except MasterNotAvailableException as e:
        # if we can't connect to master at all,
        # then bomb out early
        print(PaastaColors.red("CRITICAL:  %s" % e.message))
        sys.exit(2)

    print("Mesos Status:")
    for line in mesos_outputs:
        print_with_indent(line, 2)

    if marathon_config:
        marathon_client = get_marathon_client(marathon_config)
        marathon_outputs, marathon_oks = get_marathon_status(marathon_client)
        print("Marathon Status:")
        for line in marathon_outputs:
            print_with_indent(line, 2)

    if chronos_config:
        chronos_client = get_chronos_client(chronos_config)
        chronos_outputs, chronos_oks = get_chronos_status(chronos_client)
        print("Chronos Status:")
        for line in chronos_outputs:
            print_with_indent(line, 2)

    if False in mesos_oks or False in marathon_oks or False in chronos_oks:
        sys.exit(2)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
