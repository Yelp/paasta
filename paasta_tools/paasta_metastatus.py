#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
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
import copy
import sys
from collections import Counter
from collections import defaultdict
from collections import OrderedDict

from httplib2 import ServerNotFoundError
from humanize import naturalsize
from marathon.exceptions import MarathonError

from paasta_tools import chronos_tools
from paasta_tools import marathon_tools
from paasta_tools.chronos_tools import ChronosNotConfigured
from paasta_tools.chronos_tools import get_chronos_client
from paasta_tools.chronos_tools import load_chronos_config
from paasta_tools.marathon_tools import MarathonNotConfigured
from paasta_tools.mesos_tools import get_mesos_quorum
from paasta_tools.mesos_tools import get_mesos_state_from_leader
from paasta_tools.mesos_tools import get_mesos_stats
from paasta_tools.mesos_tools import get_number_of_mesos_masters
from paasta_tools.mesos_tools import get_zookeeper_config
from paasta_tools.mesos_tools import MasterNotAvailableException
from paasta_tools.utils import format_table
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import print_with_indent


def parse_args():
    parser = argparse.ArgumentParser(
        description='',
    )
    parser.add_argument('-v', '--verbose', action='count', dest="verbose", default=0,
                        help="Print out more output regarding the state of the cluster")
    parser.add_argument('-H', '--humanize', action='store_true', dest="humanize", default=False,
                        help="Print human-readable sizes")
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


def get_mesos_disk_status(metrics):
    """Takes in the mesos metrics and analyzes them, returning the status
    :param metrics: mesos metrics dictionary
    :returns: Tuple of the output array and is_ok bool
    """

    total = metrics['master/disk_total']
    used = metrics['master/disk_used']
    available = total - used
    return total, used, available


def filter_mesos_state_metrics(dictionary):
    valid_keys = ['cpus', 'mem', 'disk']
    return {key: value for (key, value) in dictionary.items() if key in valid_keys}


def get_extra_mesos_slave_data(mesos_state):
    slaves = dict((slave['id'], {
        'total_resources': Counter(filter_mesos_state_metrics(slave['resources'])),
        'hostname': slave['hostname'],
        'free_resources': Counter(filter_mesos_state_metrics(slave['resources'])),
    }) for slave in mesos_state['slaves'])

    for framework in mesos_state.get('frameworks', []):
        for task in framework.get('tasks', []):
            mesos_metrics = filter_mesos_state_metrics(task['resources'])
            slaves[task['slave_id']]['free_resources'].subtract(mesos_metrics)

    return sorted(slaves.values())


def get_extra_mesos_attribute_data(mesos_state):
    slaves = mesos_state.get('slaves', [])
    attributes = {attribute
                  for slave in slaves
                  for attribute in slave.get('attributes', {}).keys()}
    tasks = [task
             for framework in mesos_state.get('frameworks', [])
             for task in framework.get('tasks', [])]

    for attribute in attributes:
        yield (attribute, get_mesos_utilization_for_attribute(slaves, tasks, attribute))


def get_mesos_utilization_for_attribute(slaves, tasks, attribute):
    resource_total_dict = defaultdict(Counter)
    slave_attribute_mapping = {}
    for slave in slaves:
        slave_attribute_name = slave['attributes'].get(attribute, 'UNDEFINED')
        slave_attribute_mapping[slave['id']] = slave_attribute_name
        filtered_resources = filter_mesos_state_metrics(slave['resources'])
        resource_total_dict[slave_attribute_name].update(filtered_resources)
    resource_free_dict = copy.deepcopy(resource_total_dict)
    for task in tasks:
        task_resources = task['resources']
        attribute_value = slave_attribute_mapping[task['slave_id']]
        resource_free_dict[attribute_value].subtract(filter_mesos_state_metrics(task_resources))
    return {"free": resource_free_dict, "total": resource_total_dict}


def quorum_ok(masters, quorum):
    return masters >= quorum


def check_threshold(percent_used, threshold):
    return (100 - percent_used) > threshold


def percent_used(total, used):
    return round(used / float(total) * 100.0, 2)


def assert_cpu_health(metrics, threshold=10):
    total, used, available = get_mesos_cpu_status(metrics)
    try:
        perc_used = percent_used(total, used)
    except ZeroDivisionError:
        return (PaastaColors.red("Error reading total available cpu from mesos!"), False)

    if check_threshold(perc_used, threshold):
        return ("CPUs: %.2f / %d in use (%s)"
                % (used, total, PaastaColors.green("%.2f%%" % perc_used)),
                True)
    else:
        return (PaastaColors.red(
                "CRITICAL: Less than %d%% CPUs available. (Currently using %.2f%% of %d)"
                % (threshold, perc_used, total)),
                False)


def assert_memory_health(metrics, threshold=10):
    total = metrics['master/mem_total'] / float(1024)
    used = metrics['master/mem_used'] / float(1024)
    try:
        perc_used = percent_used(total, used)
    except ZeroDivisionError:
        return (PaastaColors.red("Error reading total available memory from mesos!"), False)

    if check_threshold(perc_used, threshold):
        return ("Memory: %0.2f / %0.2fGB in use (%s)"
                % (used, total, PaastaColors.green("%.2f%%" % perc_used)),
                True)
    else:
        return (PaastaColors.red(
                "CRITICAL: Less than %d%% memory available. (Currently using %.2f%% of %.2fGB)"
                % (threshold, perc_used, total)),
                False)


def assert_disk_health(metrics, threshold=10):
    total = metrics['master/disk_total'] / float(1024)
    used = metrics['master/disk_used'] / float(1024)
    try:
        perc_used = percent_used(total, used)
    except ZeroDivisionError:
        return (PaastaColors.red("Error reading total available disk from mesos!"), False)

    if check_threshold(perc_used, threshold):
        return ("Disk: %0.2f / %0.2fGB in use (%s)"
                % (used, total, PaastaColors.green("%.2f%%" % perc_used)),
                True)
    else:
        return (PaastaColors.red(
            "CRITICAL: Less than %d%% disk available. (Currently using %.2f%%)"
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


def assert_extra_slave_data(mesos_state, humanize_output=False):
    if not slaves_registered(mesos_state):
        return ('  No mesos slaves registered on this cluster!', False)
    extra_slave_data = get_extra_mesos_slave_data(mesos_state)
    rows = [('Hostname', 'CPU (free/total)', 'RAM (free/total)', 'Disk (free/total)')]

    for slave in extra_slave_data:
        if humanize_output:
            formatted_line = (
                slave['hostname'],
                '%.2f/%.2f' % (slave['free_resources']['cpus'], slave['total_resources']['cpus']),
                '%s/%s' % (naturalsize(slave['free_resources']['mem'] * 1024 * 1024, gnu=True),
                           naturalsize(slave['total_resources']['mem'] * 1024 * 1024, gnu=True)),
                '%s/%s' % (naturalsize(slave['free_resources']['disk'] * 1024 * 1024, gnu=True),
                           naturalsize(slave['total_resources']['disk'] * 1024 * 1024, gnu=True)),
            )
        else:
            formatted_line = (
                slave['hostname'],
                '%.2f/%.2f' % (slave['free_resources']['cpus'], slave['total_resources']['cpus']),
                '%.2f/%.2f' % (slave['free_resources']['mem'], slave['total_resources']['mem']),
                '%.2f/%.2f' % (slave['free_resources']['disk'], slave['total_resources']['disk']),
            )
        rows.append(formatted_line)
    result = ('\n'.join(('    %s' % row for row in format_table(rows)))[2:], True)
    return result


def assert_extra_attribute_data(mesos_state, humanize_output=False):
    if not slaves_registered(mesos_state):
        return ('  No mesos slaves registered on this cluster!', False)
    extra_attribute_data = list(get_extra_mesos_attribute_data(mesos_state))
    rows = []
    for attribute, resource_dict in extra_attribute_data:
        resource_free_dict = resource_dict['free']
        resource_total_dict = resource_dict['total']
        if len(resource_free_dict.keys()) >= 2:  # filter out attributes that apply to every slave in the cluster
            rows.append((attribute.capitalize(), 'CPU (free/total)', 'RAM (free/total)', 'Disk (free/total)'))
            for attribute_location in sorted(resource_free_dict.keys()):
                resources_remaining = resource_free_dict[attribute_location]
                resources_total = resource_total_dict[attribute_location]

                if humanize_output:
                    formatted_line = (
                        attribute_location,
                        '%.2f/%.2f' % (resources_remaining['cpus'], resources_total['cpus']),
                        '%s/%s' % (naturalsize(resources_remaining['mem'] * 1024 * 1024, gnu=True),
                                   naturalsize(resources_total['mem'] * 1024 * 1024, gnu=True)),
                        '%s/%s' % (naturalsize(resources_remaining['disk'] * 1024 * 1024, gnu=True),
                                   naturalsize(resources_total['disk'] * 1024 * 1024, gnu=True))
                    )
                else:
                    formatted_line = (
                        attribute_location,
                        '%.2f/%.2f' % (resources_remaining['cpus'], resources_total['cpus']),
                        '%.2f/%.2f' % (resources_remaining['mem'], resources_total['mem']),
                        '%.2f/%.2f' % (resources_remaining['disk'], resources_total['disk'])
                    )
                rows.append(formatted_line)
    if len(rows) == 0:
        result = ("  No slave attributes that apply to more than one slave were detected.", True)
    else:
        result = ('\n'.join(('    %s' % row for row in format_table(rows)))[2:], True)
    return result


def slaves_registered(mesos_state):
    return 'slaves' in mesos_state and mesos_state['slaves']


def get_mesos_status(mesos_state, verbosity, humanize_output=False):
    """Gathers information about the mesos cluster.
       :return: tuple of a string containing the status and a bool representing if it is ok or not
    """

    cluster_results = run_healthchecks_with_param(mesos_state, [assert_quorum_size, assert_no_duplicate_frameworks])

    metrics = get_mesos_stats()
    metrics_results = run_healthchecks_with_param(metrics, [
        assert_cpu_health,
        assert_memory_health,
        assert_disk_health,
        assert_tasks_running,
        assert_slave_health,
    ])

    if verbosity == 2:
        metrics_results.extend(run_healthchecks_with_param(
            mesos_state, [assert_extra_attribute_data], {"humanize_output": humanize_output}))
    elif verbosity >= 3:
        metrics_results.extend(run_healthchecks_with_param(
            mesos_state, [assert_extra_slave_data], {"humanize_output": humanize_output}))

    return cluster_results + metrics_results


def run_healthchecks_with_param(param, healthcheck_functions, format_options={}):
    return [healthcheck(param, **format_options) for healthcheck in healthcheck_functions]


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
    num_jobs = len(chronos_tools.filter_enabled_jobs(client.list()))
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
    if verbose >= 1:
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

    try:
        mesos_state = get_mesos_state_from_leader()
    except MasterNotAvailableException as e:
        # if we can't connect to master at all,
        # then bomb out early
        print(PaastaColors.red("CRITICAL:  %s" % e.message))
        sys.exit(2)
    mesos_results = get_mesos_status(mesos_state, verbosity=args.verbose,
                                     humanize_output=args.humanize)

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

    if marathon_config:
        marathon_client = get_marathon_client(marathon_config)
        try:
            marathon_results = get_marathon_status(marathon_client)
        except MarathonError as e:
            print(PaastaColors.red("CRITICAL: Unable to contact Marathon! Error: %s" % e))
            sys.exit(2)

    if chronos_config:
        chronos_client = get_chronos_client(chronos_config)
        try:
            chronos_results = get_chronos_status(chronos_client)
        except ServerNotFoundError as e:
            print(PaastaColors.red("CRITICAL: Unable to contact Chronos! Error: %s" % e))
            sys.exit(2)

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
