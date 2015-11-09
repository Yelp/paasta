#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import sys

from collections import Counter, OrderedDict
from httplib2 import ServerNotFoundError

from paasta_tools import marathon_tools
from paasta_tools import chronos_tools
from paasta_tools.chronos_tools import ChronosNotConfigured
from paasta_tools.chronos_tools import get_chronos_client
from paasta_tools.chronos_tools import load_chronos_config
from paasta_tools.marathon_tools import MarathonNotConfigured
from paasta_tools.mesos_tools import get_mesos_stats
from paasta_tools.mesos_tools import get_mesos_state_from_leader
from paasta_tools.mesos_tools import get_mesos_quorum
from paasta_tools.mesos_tools import get_zookeeper_config
from paasta_tools.mesos_tools import get_number_of_mesos_masters
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import print_with_indent
from paasta_tools.mesos_tools import MasterNotAvailableException


def parse_args():
    parser = argparse.ArgumentParser(
        description='',
    )
    parser.add_argument('-v', '--verbose', action='store_true', dest="verbose", default=False,
                        help="Print out more output regarding the state of the cluster")
    return parser.parse_args()


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


def check_threshold(percent_used, threshold):
    return (100 - percent_used) > threshold


def percent_used(total, used):
    return round(used / float(total) * 100.0, 2)


def assert_cpu_health(metrics, threshold=10):
    total, used, available = get_mesos_cpu_status(metrics)
    perc_used = percent_used(total, used)
    if check_threshold(perc_used, threshold):
        return ("CPUs: %.2f / %d in use (%s)"
                % (used, total, PaastaColors.green("%.2f%%" % perc_used)),
                True)
    else:
        return (PaastaColors.red(
                "CRITICAL: Less than %d%% CPUs available. (Currently using %.2f%%)"
                % (threshold, perc_used)),
                False)


def assert_memory_health(metrics, threshold=10):
    total = metrics['master/mem_total'] / float(1024)
    used = metrics['master/mem_used'] / float(1024)
    perc_used = percent_used(total, used)

    if check_threshold(perc_used, threshold):
        return ("Memory: %0.2f / %0.2fGB in use (%s)"
                % (used, total, PaastaColors.green("%.2f%%" % perc_used)),
                True)
    else:
        return (PaastaColors.red(
                "CRITICAL: Less than %d%% memory available. (Currently using %.2f%%)"
                % (threshold, perc_used)),
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
    :returns: a tuple containing (output, ok): output is a log of the state of frameworks, ok a boolean
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

    state = get_mesos_state_from_leader()
    cluster_results = run_healthchecks_with_param(state, [assert_quorum_size, assert_no_duplicate_frameworks])

    metrics = get_mesos_stats()
    metrics_results = run_healthchecks_with_param(metrics, [
        assert_cpu_health,
        assert_memory_health,
        assert_slave_health,
        assert_tasks_running])

    return cluster_results + metrics_results


def run_healthchecks_with_param(param, healthcheck_functions):
    return [healthcheck(param) for healthcheck in healthcheck_functions]


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
    return run_healthchecks_with_param(client, [
        assert_marathon_apps,
        assert_marathon_tasks,
        assert_marathon_deployments])


def assert_chronos_scheduled_jobs(client):
    """
    :returns: a tuple of a string and a bool containing representing if it is ok or not
    """
    try:
        num_jobs = len(chronos_tools.filter_enabled_jobs(client.list()))
    except ServerNotFoundError:
        num_jobs = 0
    return ("Enabled chronos jobs: %d" % num_jobs, True)


def get_chronos_status(chronos_client):
    """Gather information about chronos.
    :return: string containing the status
    """
    return run_healthchecks_with_param(chronos_client, [
        assert_chronos_scheduled_jobs,
    ])


def get_marathon_client(marathon_config):
    """Given a MarathonConfig object, return
    a client.
    :param marathon_config: a MarathonConfig object
    :returns client: a marathon client
    """
    return marathon_tools.get_marathon_client(
        marathon_config.get_url(),
        marathon_config.get_username(),
        marathon_config.get_password()
    )


def critical_events_in_outputs(healthcheck_outputs):
    """Given a list of healthcheck pairs (output, healthy), return
    those which are unhealthy.
    """
    return [healthcheck for healthcheck in healthcheck_outputs if healthcheck[-1] is False]


def generate_summary_for_check(name, ok):
    """Given a check name and a boolean indicating if the service is OK, return
    a formatted message.
    """
    status = PaastaColors.green("OK") if ok is True else PaastaColors.red("CRITICAL")
    summary = "%s Status: %s" % (name, status)
    return summary


def status_for_results(results):
    """Given a list of (output, ok) pairs, return the ok status
    for each pair
    """
    return [result[-1] for result in results]


def print_results_for_healthchecks(summary, ok, results, verbose):
    print summary
    if verbose:
        for line in [res[0] for res in results]:
            print_with_indent(line, 2)
    elif not ok:
        critical_results = critical_events_in_outputs(results)
        for line in [res[0] for res in critical_results]:
            print_with_indent(line, 2)


def main():
    marathon_config = None
    chronos_config = None
    args = parse_args()

    # Check to see if Marathon should be running here by checking for config
    try:
        marathon_config = marathon_tools.load_marathon_config()
    except MarathonNotConfigured:
        marathon_results = [('marathon is not configured to run here', True)]

    # Check to see if Chronos should be running here by checking for config
    try:
        chronos_config = load_chronos_config()
    except ChronosNotConfigured:
        chronos_results = [('chronos is not configured to run here', True)]

    try:
        mesos_results = get_mesos_status()
    except MasterNotAvailableException as e:
        # if we can't connect to master at all,
        # then bomb out early
        print(PaastaColors.red("CRITICAL:  %s" % e.message))
        sys.exit(2)

    if marathon_config:
        marathon_client = get_marathon_client(marathon_config)
        marathon_results = get_marathon_status(marathon_client)

    if chronos_config:
        chronos_client = get_chronos_client(chronos_config)
        chronos_results = get_chronos_status(chronos_client)

    mesos_ok = all(status_for_results(mesos_results))
    marathon_ok = all(status_for_results(marathon_results))
    chronos_ok = all(status_for_results(chronos_results))

    mesos_summary = generate_summary_for_check("Mesos", mesos_ok)
    marathon_summary = generate_summary_for_check("Marathon", marathon_ok)
    chronos_summary = generate_summary_for_check("Chronos", chronos_ok)

    print_results_for_healthchecks(mesos_summary, mesos_ok, mesos_results, args.verbose)
    print_results_for_healthchecks(marathon_summary, marathon_ok, marathon_results, args.verbose)
    print_results_for_healthchecks(chronos_summary, chronos_ok, chronos_results, args.verbose)

    if not all([mesos_ok, marathon_ok, chronos_ok]):
        sys.exit(2)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
