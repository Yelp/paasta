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
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import itertools
from collections import Counter
from collections import namedtuple
from collections import OrderedDict

from humanize import naturalsize

from paasta_tools import chronos_tools
from paasta_tools import marathon_tools
from paasta_tools.mesos_tools import get_all_tasks_from_state
from paasta_tools.mesos_tools import get_mesos_quorum
from paasta_tools.mesos_tools import get_number_of_mesos_masters
from paasta_tools.mesos_tools import get_zookeeper_host_path
from paasta_tools.mesos_tools import is_task_terminal
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import print_with_indent


HealthCheckResult = namedtuple('HealthCheckResult', ['message', 'healthy'])
ResourceInfo = namedtuple('ResourceInfo', ['cpus', 'mem', 'disk'])
ResourceUtilization = namedtuple('ResourceUtilization', ['metric', 'total', 'free'])

EXPECTED_HEALTHY_FRAMEWORKS = 2
HIGH_QUEUE_GAUGE = 'org.apache.mesos.chronos.scheduler.jobs.TaskManager.highQueueSize'
QUEUE_GAUGE = 'org.apache.mesos.chronos.scheduler.jobs.TaskManager.queueSize'


def get_num_masters():
    """ Gets the number of masters from mesos state """
    zookeeper_host_path = get_zookeeper_host_path()
    return get_number_of_mesos_masters(zookeeper_host_path.host, zookeeper_host_path.path)


def get_mesos_cpu_status(metrics, mesos_state):
    """Takes in the mesos metrics and analyzes them, returning the status.

    :param metrics: mesos metrics dictionary
    :returns: Tuple of the output array and is_ok bool
    """

    total = metrics['master/cpus_total']
    used = metrics['master/cpus_used']

    for slave in mesos_state['slaves']:
        for role in slave['reserved_resources']:
            used += slave['reserved_resources'][role]['cpus']

    available = total - used
    return total, used, available


def get_mesos_disk_status(metrics):
    """Takes in the mesos metrics and analyzes them, returning the status.

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


def healthcheck_result_for_resource_utilization(resource_utilization, threshold):
    """ Given a resource data dict, assert that cpu
    data is ok.

    :param resource_utilization: the resource_utilization tuple to check
    :returns: a HealthCheckResult
    """
    try:
        utilization = percent_used(resource_utilization.total, resource_utilization.total - resource_utilization.free)
    except ZeroDivisionError:
        utilization = 0
    message = "%s: %.2f/%.2f(%.2f%%) used. Threshold (%.2f%%)" % (
        resource_utilization.metric,
        float(resource_utilization.total - resource_utilization.free),
        resource_utilization.total,
        utilization,
        threshold,
    )
    healthy = utilization <= threshold
    return HealthCheckResult(
        message=message,
        healthy=healthy
    )


def quorum_ok(masters, quorum):
    return masters >= quorum


def check_threshold(percent_used, threshold):
    return (100 - percent_used) > threshold


def percent_used(total, used):
    return round(used / float(total) * 100.0, 2)


def assert_cpu_health(metrics, mesos_state, threshold=10):
    total, used, available = get_mesos_cpu_status(metrics, mesos_state)
    try:
        perc_used = percent_used(total, used)
    except ZeroDivisionError:
        return HealthCheckResult(message="Error reading total available cpu from mesos!",
                                 healthy=False)

    if check_threshold(perc_used, threshold):
        return HealthCheckResult(message="CPUs: %.2f / %d in use (%s)"
                                 % (used, total, PaastaColors.green("%.2f%%" % perc_used)),
                                 healthy=True)
    else:
        return HealthCheckResult(message="CRITICAL: Less than %d%% CPUs available. (Currently using %.2f%% of %d)"
                                 % (threshold, perc_used, total),
                                 healthy=False)


def assert_memory_health(metrics, mesos_state, threshold=10):
    total = metrics['master/mem_total'] / float(1024)
    used = metrics['master/mem_used']

    for slave in mesos_state['slaves']:
        for role in slave['reserved_resources']:
            used += slave['reserved_resources'][role]['mem']

    used /= float(1024)

    try:
        perc_used = percent_used(total, used)
    except ZeroDivisionError:
        return HealthCheckResult(message="Error reading total available memory from mesos!",
                                 healthy=False)

    if check_threshold(perc_used, threshold):
        return HealthCheckResult(
            message="Memory: %0.2f / %0.2fGB in use (%s)"
            % (used, total, PaastaColors.green("%.2f%%" % perc_used)),
            healthy=True
        )
    else:
        return HealthCheckResult(
            message="CRITICAL: Less than %d%% memory available. (Currently using %.2f%% of %.2fGB)"
                    % (threshold, perc_used, total),
                    healthy=False
        )


def assert_disk_health(metrics, mesos_state, threshold=10):
    total = metrics['master/disk_total'] / float(1024)
    used = metrics['master/disk_used']

    for slave in mesos_state['slaves']:
        for role in slave['reserved_resources']:
            used += slave['reserved_resources'][role]['disk']

    used /= float(1024)

    try:
        perc_used = percent_used(total, used)
    except ZeroDivisionError:
        return HealthCheckResult(message="Error reading total available disk from mesos!",
                                 healthy=False)

    if check_threshold(perc_used, threshold):
        return HealthCheckResult(
            message="Disk: %0.2f / %0.2fGB in use (%s)"
            % (used, total, PaastaColors.green("%.2f%%" % perc_used)),
            healthy=True
        )
    else:
        return HealthCheckResult(
            message="CRITICAL: Less than %d%% disk available. (Currently using %.2f%%)" % (threshold, perc_used),
            healthy=False
        )


def assert_tasks_running(metrics):
    running = metrics['master/tasks_running']
    staging = metrics['master/tasks_staging']
    starting = metrics['master/tasks_starting']
    return HealthCheckResult(
        message="Tasks: running: %d staging: %d starting: %d" % (running, staging, starting),
        healthy=True
    )


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
    output = ["Frameworks:"]
    ok = True

    for framework, count in framework_counts.items():
        if count > 1:
            ok = False
            output.append("    CRITICAL: Framework %s has %d instances running--expected no more than 1."
                          % (framework, count))
        else:
            output.append("    Framework: %s count: %d" % (framework, count))
    return HealthCheckResult(
        message=("\n").join(output),
        healthy=ok
    )


def assert_slave_health(metrics):
    active, inactive = metrics['master/slaves_active'], metrics['master/slaves_inactive']
    return HealthCheckResult(
        message="Slaves: active: %d inactive: %d" % (active, inactive),
        healthy=True
    )


def assert_connected_frameworks(mesos_metrics):
    connected_frameworks = mesos_metrics['master/frameworks_connected']
    healthy = connected_frameworks == EXPECTED_HEALTHY_FRAMEWORKS
    return HealthCheckResult(
        message="Connected Frameworks: expected: %d actual: %d" % (EXPECTED_HEALTHY_FRAMEWORKS, connected_frameworks),
        healthy=healthy
    )


def assert_disconnected_frameworks(mesos_metrics):
    disconnected_frameworks = mesos_metrics['master/frameworks_disconnected']
    healthy = disconnected_frameworks == 0
    return HealthCheckResult(
        message="Disconnected Frameworks: expected: 0 actual: %d" % disconnected_frameworks,
        healthy=healthy
    )


def assert_active_frameworks(mesos_metrics):
    active_frameworks = mesos_metrics['master/frameworks_active']
    healthy = active_frameworks == EXPECTED_HEALTHY_FRAMEWORKS
    return HealthCheckResult(
        message="Active Frameworks: expected: %d actual: %d" % (EXPECTED_HEALTHY_FRAMEWORKS, active_frameworks),
        healthy=healthy
    )


def assert_inactive_frameworks(mesos_metrics):
    inactive_frameworks = mesos_metrics['master/frameworks_inactive']
    healthy = inactive_frameworks == 0
    return HealthCheckResult(
        message="Inactive Frameworks: expected: 0 actual: %d" % inactive_frameworks,
        healthy=healthy
    )


def assert_quorum_size():
    masters, quorum = get_num_masters(), get_mesos_quorum()
    if quorum_ok(masters, quorum):
        return HealthCheckResult(
            message="Quorum: masters: %d configured quorum: %d " % (masters, quorum),
            healthy=True
        )
    else:
        return HealthCheckResult(
            message="CRITICAL: Number of masters (%d) less than configured quorum(%d)." % (masters, quorum),
            healthy=False
        )


def key_func_for_attribute(attribute):
    """ Return a closure that given a slave, will return the value of a specific
    attribute.

    :param attribute: the attribute to inspect in the slave
    :returns: a closure, which takes a slave and returns the value of an attribute
    """
    def key_func(slave):
        return slave['attributes'].get(attribute, 'unknown')
    return key_func


def group_slaves_by_key_func(key_func, slaves):
    """ Given a function for grouping slaves, return a
    dict where keys are the unique values returned by
    the key_func and the values are all those slaves which
    have that specific value.

    :param key_func: a function which consumes a slave and returns a value
    :param slaves: a list of slaves
    :returns: a dict of key: [slaves]
    """
    sorted_slaves = sorted(slaves, key=key_func)
    return {k: list(v) for k, v in itertools.groupby(sorted_slaves, key=key_func)}


def calculate_resource_utilization_for_slaves(slaves, tasks):
    """ Given a list of slaves and a list of tasks, calculate the total available
    resource available in that list of slaves, and the resources consumed by tasks
    running on those slaves.

    :param slaves: a list of slaves to calculate resource usage for
    :param tasks: the list of tasks running in the mesos cluster
    :returns: a dict, containing keys for "free" and "total" resources. Each of these keys
    is a ResourceInfo tuple, exposing a number for cpu, disk and mem.
    """
    resource_total_dict = Counter()
    for slave in slaves:
        filtered_resources = filter_mesos_state_metrics(slave['resources'])
        resource_total_dict.update(Counter(filtered_resources))
    resource_free_dict = copy.deepcopy(resource_total_dict)
    for task in tasks:
        task_resources = task['resources']
        resource_free_dict.subtract(Counter(filter_mesos_state_metrics(task_resources)))
    for slave in slaves:
        for role in slave['reserved_resources']:
            filtered_resources = filter_mesos_state_metrics(slave['reserved_resources'][role])
            resource_free_dict.subtract(Counter(filtered_resources))
    return {
        "free": ResourceInfo(
            cpus=resource_free_dict['cpus'],
            disk=resource_free_dict['disk'],
            mem=resource_free_dict['mem']
        ),
        "total": ResourceInfo(
            cpus=resource_total_dict['cpus'],
            disk=resource_total_dict['disk'],
            mem=resource_total_dict['mem'],
        )
    }


def filter_tasks_for_slaves(slaves, tasks):
    """ Given a list of slaves and a list of tasks, return a filtered
    list of tasks, where those returned belong to slaves in the list of
    slaves

    :param slaves: the list of slaves which the tasks provided should be
    running on.
    :param tasks: the tasks to filter :returns: a list of tasks,
    identical to that provided by the tasks param, but with only those where
    the task is running on one of the provided slaves included.
    """
    slave_ids = [slave['id'] for slave in slaves]
    return [task for task in tasks if task['slave_id'] in slave_ids]


def get_resource_utilization_by_grouping(grouping_func, mesos_state):
    """ Given a function used to group slaves and mesos state, calculate
    resource utilization for each value of a given attribute.

    :grouping_func: a function that given a slave, will return the value of an
    attribtue to group by.
    :param mesos_state: the mesos state
    :returns: a dict of {attribute_value: resource_usage}, where resource usage
    is the dict returned by ``calculate_resource_utilization_for_slaves`` for
    slaves grouped by attribute value.
    """
    slaves = mesos_state.get('slaves', [])
    if not has_registered_slaves(mesos_state):
        raise ValueError("There are no slaves registered in the mesos state.")

    tasks = get_all_tasks_from_state(mesos_state, include_orphans=True)
    non_terminal_tasks = [task for task in tasks if not is_task_terminal(task)]
    slave_groupings = group_slaves_by_key_func(grouping_func, slaves)

    return {
        attribute_value: calculate_resource_utilization_for_slaves(
            slaves=slaves,
            tasks=filter_tasks_for_slaves(slaves, non_terminal_tasks)
        )
        for attribute_value, slaves in slave_groupings.items()
    }


def resource_utillizations_from_resource_info(total, free):
    """
    Given two ResourceInfo tuples, one for total and one for free,
    create a ResourceUtilization tuple for each metric in the ResourceInfo.
    :param total:
    :param free:
    :returns: ResourceInfo for a metric
    """
    return [
        ResourceUtilization(metric=field, total=total[index], free=free[index])
        for index, field in enumerate(ResourceInfo._fields)
    ]


def has_registered_slaves(mesos_state):
    """ Return a boolean indicating if there are any slaves registered
    to the master according to the mesos state.
    :param mesos_state: the mesos state from the master
    :returns: a boolean, indicating if there are > 0 slaves
    """
    return len(mesos_state.get('slaves', [])) > 0


def get_mesos_resource_utilization_health(mesos_metrics, mesos_state):
    """Perform healthchecks against mesos metrics.
    :param mesos_metrics: a dict exposing the mesos metrics described in
    https://mesos.apache.org/documentation/latest/monitoring/
    :returns: a list of HealthCheckResult tuples
    """
    return [
        assert_cpu_health(mesos_metrics, mesos_state),
        assert_memory_health(mesos_metrics, mesos_state),
        assert_disk_health(mesos_metrics, mesos_state),
        assert_tasks_running(mesos_metrics),
        assert_slave_health(mesos_metrics),
    ]


def get_framework_metrics_status(metrics):
    return [
        assert_connected_frameworks(metrics),
        assert_disconnected_frameworks(metrics),
        assert_inactive_frameworks(metrics)
    ]


def get_mesos_state_status(mesos_state):
    """Perform healthchecks against mesos state.
    :param mesos_state: a dict exposing the mesos state described in
    https://mesos.apache.org/documentation/latest/endpoints/master/state.json/
    :returns: a list of HealthCheckResult tuples
    """
    return [assert_quorum_size(), assert_no_duplicate_frameworks(mesos_state)]


def run_healthchecks_with_param(param, healthcheck_functions, format_options={}):
    return [healthcheck(param, **format_options) for healthcheck in healthcheck_functions]


def assert_marathon_apps(client):
    num_apps = len(client.list_apps())
    if num_apps < 1:
        return HealthCheckResult(message="CRITICAL: No marathon apps running",
                                 healthy=False)
    else:
        return HealthCheckResult(message="marathon apps: %d" % num_apps, healthy=True)


def assert_marathon_tasks(client):
    num_tasks = len(client.list_tasks())
    return HealthCheckResult(message="marathon tasks: %d" % num_tasks, healthy=True)


def assert_marathon_deployments(client):
    num_deployments = len(client.list_deployments())
    return HealthCheckResult(message="marathon deployments: %d" % num_deployments, healthy=True)


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
    return HealthCheckResult(message="Enabled chronos jobs: %d" % num_jobs, healthy=True)


def assert_chronos_queued_jobs(client):
    high_priority_queue_size = client.metrics()['gauges'][HIGH_QUEUE_GAUGE]['value']
    normal_priority_queue_size = client.metrics()['gauges'][QUEUE_GAUGE]['value']
    all_jobs_queued = high_priority_queue_size + normal_priority_queue_size
    num_jobs = len(chronos_tools.filter_enabled_jobs(client.list()))

    try:
        perc_used = percent_used(num_jobs, all_jobs_queued)
    except ZeroDivisionError:
        perc_used = 0
    return HealthCheckResult(
        message="Jobs Queued: %s (%s%%)" % (all_jobs_queued, perc_used),
        healthy=True
    )


def get_chronos_status(chronos_client):
    """Gather information about chronos.
    :return: string containing the status
    """
    return run_healthchecks_with_param(chronos_client, [
        assert_chronos_scheduled_jobs,
        assert_chronos_queued_jobs,
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
    """Given a list of HealthCheckResults return those which are unhealthy.
    """
    return [healthcheck for healthcheck in healthcheck_outputs if healthcheck.healthy is False]


def generate_summary_for_check(name, ok):
    """Given a check name and a boolean indicating if the service is OK, return
    a formatted message.
    """
    status = PaastaColors.green("OK") if ok is True else PaastaColors.red("CRITICAL")
    summary = "%s Status: %s" % (name, status)
    return summary


def status_for_results(healthcheck_results):
    """Given a list of HealthCheckResult tuples, return the ok status
    for each one.
    :param healthcheck_results: a list of HealthCheckResult tuples
    :returns: a list of booleans.
    """
    return [result.healthy for result in healthcheck_results]


def print_results_for_healthchecks(summary, ok, results, verbose, indent=2):
    paasta_print(summary)
    if verbose >= 1:
        for health_check_result in results:
            if health_check_result.healthy:
                print_with_indent(health_check_result.message, indent)
            else:
                print_with_indent(PaastaColors.red(health_check_result.message), indent)
    elif not ok:
        unhealthy_results = critical_events_in_outputs(results)
        for health_check_result in unhealthy_results:
            print_with_indent(PaastaColors.red(health_check_result.message), indent)


def healthcheck_result_resource_utilization_pair_for_resource_utilization(utilization, threshold):
    """Given a ResourceUtilization, produce a tuple of (HealthCheckResult, ResourceUtilization),
    where that HealthCheckResult describes the 'health' of a given utilization.
    :param utilization: a ResourceUtilization tuple
    :param threshold: a threshold which decides the health of the given ResourceUtilization
    :returns: a tuple of (HealthCheckResult, ResourceUtilization)
    """
    return (healthcheck_result_for_resource_utilization(utilization, threshold), utilization)


def format_table_column_for_healthcheck_resource_utilization_pair(healthcheck_utilization_pair, humanize):
    """Given a tuple of (HealthCheckResult, ResourceUtilization), return a
    string representation of the ResourceUtilization such that it is formatted
    according to the value of HealthCheckResult.healthy. Further, humanize the
    string according to the humanize boolean parameter and the metric - be sure
    to *not* try and humanize if the ResourceUtilization metric is cpus
    (because we don't want to try and show that as some other unit).

    :param healthcheck_utilization_pair: a tuple of (HealthCheckResult, ResourceUtilization)
    :param humanize: a boolean indicating if the string should be humanized
    :returns: a string representing the ResourceUtilization.
    """
    color_func = PaastaColors.green if healthcheck_utilization_pair[0].healthy else PaastaColors.red
    utilization = healthcheck_utilization_pair[1].total - healthcheck_utilization_pair[1].free
    if int(healthcheck_utilization_pair[1].total) == 0:
        utilization_perc = 100
    else:
        utilization_perc = utilization / float(healthcheck_utilization_pair[1].total) * 100
    if humanize and healthcheck_utilization_pair[1].metric != 'cpus':
        return color_func('%s/%s (%.2f%%)' % (
            naturalsize(utilization * 1024 * 1024, gnu=True),
            naturalsize(healthcheck_utilization_pair[1].total * 1024 * 1024, gnu=True),
            utilization_perc,
        ))
    else:
        return color_func('%s/%s (%.2f%%)' % (
            utilization,
            healthcheck_utilization_pair[1].total,
            utilization_perc,
        ))


def format_row_for_resource_utilization_healthchecks(healthcheck_utilization_pairs, humanize):
    """Given a list of (HealthCheckResult, ResourceUtilization) tuples, return a list with each of those
    tuples represented by a formatted string.

    :param healthcheck_utilization_pairs: a list of (HealthCheckResult, ResourceUtilization) tuples.
    :param humanize: a boolean indicating if the strings should be humanized.
    :returns: a list containing a string representation of each (HealthCheckResult, ResourceUtilization) tuple.
    """
    return [format_table_column_for_healthcheck_resource_utilization_pair(pair, humanize)
            for pair in healthcheck_utilization_pairs]


def get_table_rows_for_resource_info_dict(attribute_value, healthcheck_utilization_pairs, humanize):
    """ A wrapper method to join together

    :param attribute: The attribute value and formatted columns to be shown in
    a single row.  :param attribute_value: The value of the attribute
    associated with the row. This becomes index 0 in the array returned.
    :param healthcheck_utilization_pairs: a list of 2-tuples, where each tuple has the elements
    (HealthCheckResult, ResourceUtilization)
    :param humanize: a boolean indicating whether the outut strings should be 'humanized'
    :returns: a list of strings, representing a row in a table to be formatted.
    """
    row = [attribute_value]
    row.extend(format_row_for_resource_utilization_healthchecks(healthcheck_utilization_pairs, humanize))
    return row
