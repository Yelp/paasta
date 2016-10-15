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
import logging
from contextlib import contextmanager
from datetime import datetime
from math import ceil
from math import floor

import requests
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

from paasta_tools.autoscaling.utils import _autoscaling_components
from paasta_tools.autoscaling.utils import register_autoscaling_component
from paasta_tools.bounce_lib import LockHeldException
from paasta_tools.bounce_lib import LockTimeout
from paasta_tools.bounce_lib import ZK_LOCK_CONNECT_TIMEOUT_S
from paasta_tools.marathon_tools import compose_autoscaling_zookeeper_root
from paasta_tools.marathon_tools import format_job_id
from paasta_tools.marathon_tools import get_marathon_client
from paasta_tools.marathon_tools import is_task_healthy
from paasta_tools.marathon_tools import load_marathon_config
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.marathon_tools import MESOS_TASK_SPACER
from paasta_tools.marathon_tools import set_instances_for_marathon_service
from paasta_tools.mesos_tools import get_running_tasks_from_active_frameworks
from paasta_tools.utils import _log
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import get_user_agent
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import mean
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import use_requests_cache
from paasta_tools.utils import ZookeeperPool

SERVICE_METRICS_PROVIDER_KEY = 'metrics_provider'
DECISION_POLICY_KEY = 'decision_policy'

AUTOSCALING_DELAY = 300

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


def get_service_metrics_provider(name):
    """
    Returns a service metrics provider matching the given name.
    """
    return _autoscaling_components[SERVICE_METRICS_PROVIDER_KEY][name]


def get_decision_policy(name):
    """
    Returns a decision policy matching the given name.
    Decision policies determine the direction a service needs to be scaled in.
    Each decision policy returns one of the following values:
    -1: autoscale down
    0:  don't autoscale
    1:  autoscale up
    """
    return _autoscaling_components[DECISION_POLICY_KEY][name]


class MetricsProviderNoDataError(ValueError):
    pass


@register_autoscaling_component('threshold', DECISION_POLICY_KEY)
def threshold_decision_policy(current_instances, error, **kwargs):
    """
    Decides to autoscale up or down by 10% if the service exceeds the upper or lower thresholds
    (see get_error_from_value() for how the thresholds are created)
    """
    autoscaling_amount = max(1, int(current_instances * 0.1))
    if error > 0:
        return autoscaling_amount
    elif abs(error) > 0:
        return -autoscaling_amount
    else:
        return 0


@register_autoscaling_component('pid', DECISION_POLICY_KEY)
def pid_decision_policy(zookeeper_path, current_instances, min_instances, max_instances, error, **kwargs):
    """
    Uses a PID to determine when to autoscale a service.
    See https://en.wikipedia.org/wiki/PID_controller for more information on PIDs.
    Kp, Ki and Kd are the canonical PID constants, where the output of the PID is:
    Kp * error + Ki * integral(error * dt) + Kd * (d(error) / dt)
    """
    min_delta = min_instances - current_instances
    max_delta = max_instances - current_instances

    def clamp_value(number):
        return min(max(number, min_delta), max_delta)

    Kp = 4
    Ki = 4 / AUTOSCALING_DELAY
    Kd = 1 * AUTOSCALING_DELAY

    zk_iterm_path = '%s/pid_iterm' % zookeeper_path
    zk_last_error_path = '%s/pid_last_error' % zookeeper_path
    zk_last_time_path = '%s/pid_last_time' % zookeeper_path

    with ZookeeperPool() as zk:
        try:
            iterm, _ = zk.get(zk_iterm_path)
            last_error, _ = zk.get(zk_last_error_path)
            last_time, _ = zk.get(zk_last_time_path)
            iterm = float(iterm)
            last_error = float(last_error)
            last_time = float(last_time)
        except NoNodeError:
            iterm = 0.0
            last_error = 0.0
            last_time = 0.0

    with ZookeeperPool() as zk:
        zk.ensure_path(zk_iterm_path)
        zk.ensure_path(zk_last_error_path)
        zk.set(zk_iterm_path, str(iterm))
        zk.set(zk_last_error_path, str(error))

    current_time = int(datetime.now().strftime('%s'))
    time_delta = current_time - last_time

    iterm = clamp_value(iterm + (Ki * error) * time_delta)

    with ZookeeperPool() as zk:
        zk.ensure_path(zk_iterm_path)
        zk.ensure_path(zk_last_error_path)
        zk.ensure_path(zk_last_time_path)
        zk.set(zk_iterm_path, str(iterm))
        zk.set(zk_last_error_path, str(error))
        zk.set(zk_last_time_path, str(current_time))

    return int(round(clamp_value(Kp * error + iterm + Kd * (error - last_error) / time_delta)))


def get_json_body_from_service(host, port, endpoint):
    return requests.get(
        'http://%s:%s/%s' % (host, port, endpoint),
        headers={'User-Agent': get_user_agent()},
    ).json()


def get_http_utilization_for_all_tasks(marathon_service_config, marathon_tasks, endpoint, json_mapper):
    """
    Gets the mean utilization of a service across all of its tasks by fetching
    json from an http endpoint and applying a function that maps it to a
    utilization

    :param marathon_service_config: the MarathonServiceConfig to get data from
    :param marathon_tasks: Marathon tasks to get data from
    :param endpoint: The http endpoint to get the uwsgi stats from
    :param json_mapper: A function that takes a dictionary for a task and returns that task's utilization

    :returns: the service's mean utilization, from 0 to 1
    """

    endpoint = endpoint.lstrip('/')
    utilization = []
    for task in marathon_tasks:
        try:
            utilization.append(json_mapper(get_json_body_from_service(task.host, task.ports[0], endpoint)))
        except requests.exceptions.Timeout:
            # If we time out querying an endpoint, assume the task is fully loaded
            # This won't trigger in the event of DNS error or when a request is refused
            # a requests.exception.ConnectionError is raised in those cases
            utilization.append(1.0)
            log.debug('Recieved a timeout when querying %s on %s:%s. Assuming the service is at full utilization.' % (
                marathon_service_config.get_service(), task.host, task.ports[0]))
        except Exception as e:
            log.debug('Caught excpetion when querying %s on %s:%s : %s' % (
                marathon_service_config.get_service(), task.host, task.ports[0], str(e)))
    if not utilization:
        raise MetricsProviderNoDataError('Couldn\'t get any data from http endpoint %s for %s.%s' % (
            endpoint, marathon_service_config.service, marathon_service_config.instance))
    return mean(utilization)


@register_autoscaling_component('uwsgi', SERVICE_METRICS_PROVIDER_KEY)
def uwsgi_metrics_provider(marathon_service_config, marathon_tasks, endpoint='uwsgi_stats', *args, **kwargs):
    """
    Gets the mean utilization of a service across all of its tasks, where
    the utilization of a task is the percentage of non-idle workers as read
    from the UWSGI stats endpoint

    :param marathon_service_config: the MarathonServiceConfig to get data from
    :param marathon_tasks: Marathon tasks to get data from
    :param endpoint: The http endpoint to get the uwsgi stats from

    :returns: the service's mean utilization, from 0 to 1
    """

    def uwsgi_mapper(json):
        workers = json['workers']
        utilization = [1.0 if worker['status'] != 'idle' else 0.0 for worker in workers]
        return mean(utilization)

    return get_http_utilization_for_all_tasks(marathon_service_config, marathon_tasks, endpoint, uwsgi_mapper)


@register_autoscaling_component('http', SERVICE_METRICS_PROVIDER_KEY)
def http_metrics_provider(marathon_service_config, marathon_tasks, endpoint='status', *args, **kwargs):
    """
    Gets the mean utilization of a service across all of its tasks, where
    the utilization of a task is read from a HTTP endpoint on the host. The
    HTTP endpoint must return JSON with a 'utilization' key with a value from 0
    to 1.

    :param marathon_service_config: the MarathonServiceConfig to get data from
    :param marathon_tasks: Marathon tasks to get data from
    :param endpoint: The http endpoint to get the task utilization from

    :returns: the service's mean utilization, from 0 to 1
    """

    def utilization_mapper(json):
        return float(json['utilization'])

    return get_http_utilization_for_all_tasks(marathon_service_config, marathon_tasks, endpoint, utilization_mapper)


@register_autoscaling_component('mesos_cpu', SERVICE_METRICS_PROVIDER_KEY)
def mesos_cpu_metrics_provider(marathon_service_config, marathon_tasks, mesos_tasks, **kwargs):
    """
    Gets the mean cpu utilization of a service across all of its tasks.

    :param marathon_service_config: the MarathonServiceConfig to get data from
    :param marathon_tasks: Marathon tasks to get data from
    :param mesos_tasks: Mesos tasks to get data from

    :returns: the service's mean utilization, from 0 to 1
    """

    autoscaling_root = compose_autoscaling_zookeeper_root(
        service=marathon_service_config.service,
        instance=marathon_service_config.instance,
    )
    zk_last_time_path = '%s/cpu_last_time' % autoscaling_root
    zk_last_cpu_data = '%s/cpu_data' % autoscaling_root

    with ZookeeperPool() as zk:
        try:
            last_time, _ = zk.get(zk_last_time_path)
            last_cpu_data, _ = zk.get(zk_last_cpu_data)
            last_time = float(last_time)
            last_cpu_data = (datum for datum in last_cpu_data.split(',') if datum)
        except NoNodeError:
            last_time = 0.0
            last_cpu_data = []

    mesos_tasks = {task['id']: task.stats for task in mesos_tasks}
    current_time = int(datetime.now().strftime('%s'))
    time_delta = current_time - last_time

    mesos_cpu_data = {task_id: float(stats.get('cpus_system_time_secs', 0.0) + stats.get(
        'cpus_user_time_secs', 0.0)) / (stats.get('cpus_limit', 0) - .1) for task_id, stats in mesos_tasks.items()}

    if not mesos_cpu_data:
        raise MetricsProviderNoDataError("Couldn't get any cpu data from Mesos")

    cpu_data_csv = ','.join('%s:%s' % (cpu_seconds, task_id) for task_id, cpu_seconds in mesos_cpu_data.items())

    with ZookeeperPool() as zk:
        zk.ensure_path(zk_last_cpu_data)
        zk.ensure_path(zk_last_time_path)
        zk.set(zk_last_cpu_data, str(cpu_data_csv))
        zk.set(zk_last_time_path, str(current_time))

    utilization = {}
    for datum in last_cpu_data:
        last_cpu_seconds, task_id = datum.split(':')
        if task_id in mesos_cpu_data:
            utilization[task_id] = (mesos_cpu_data[task_id] - float(last_cpu_seconds)) / time_delta

    if not utilization:
        raise MetricsProviderNoDataError("""The mesos_cpu metrics provider doesn't have Zookeeper data for this service.
                                         This is expected for its first run.""")

    task_utilization = utilization.values()
    mean_utilization = mean(task_utilization)

    return mean_utilization


def get_error_from_utilization(utilization, setpoint, current_instances):
    """
    Consider scaling up if utilization is above the setpoint
    Consider scaling down if the utilization is below the setpoint AND scaling down wouldn't bring it above the setpoint
    Otherwise don't scale
    """
    max_threshold = setpoint
    min_threshold = max_threshold * (current_instances - 1) / current_instances
    if utilization < min_threshold:
        return utilization - min_threshold
    elif utilization > max_threshold:
        return utilization - max_threshold
    else:
        return 0.0


def autoscale_marathon_instance(marathon_service_config, marathon_tasks, mesos_tasks):
    current_instances = marathon_service_config.get_instances()
    if len(marathon_tasks) != current_instances:
        write_to_log(config=marathon_service_config,
                     line='Delaying scaling as marathon is either waiting for resources or is delayed')
        return
    autoscaling_params = marathon_service_config.get_autoscaling_params()
    autoscaling_metrics_provider = get_service_metrics_provider(autoscaling_params.pop(SERVICE_METRICS_PROVIDER_KEY))
    autoscaling_decision_policy = get_decision_policy(autoscaling_params.pop(DECISION_POLICY_KEY))

    utilization = autoscaling_metrics_provider(marathon_service_config, marathon_tasks,
                                               mesos_tasks, **autoscaling_params)
    error = get_error_from_utilization(
        utilization=utilization,
        setpoint=autoscaling_params.pop('setpoint'),
        current_instances=current_instances,
    )

    zookeeper_path = compose_autoscaling_zookeeper_root(
        service=marathon_service_config.service,
        instance=marathon_service_config.instance,
    )
    autoscaling_amount = autoscaling_decision_policy(
        error=error,
        min_instances=marathon_service_config.get_min_instances(),
        max_instances=marathon_service_config.get_max_instances(),
        current_instances=current_instances,
        zookeeper_path=zookeeper_path,
        **autoscaling_params
    )

    new_instance_count = marathon_service_config.limit_instance_count(current_instances + autoscaling_amount)
    if new_instance_count != current_instances:
        write_to_log(
            config=marathon_service_config,
            line='Scaling from %d to %d instances (%s)' % (
                current_instances, new_instance_count, humanize_error(error)),
        )
        set_instances_for_marathon_service(
            service=marathon_service_config.service,
            instance=marathon_service_config.instance,
            instance_count=new_instance_count,
        )
    else:
        write_to_log(
            config=marathon_service_config,
            line='Staying at %d instances (%s)' % (current_instances, humanize_error(error)),
            level='debug',
        )


def humanize_error(error):
    if error < 0:
        return '%d%% underutilized' % floor(-error * 100)
    elif error > 0:
        return '%d%% overutilized' % ceil(error * 100)
    else:
        return 'utilization within thresholds'


def get_configs_of_services_to_scale(cluster, soa_dir=DEFAULT_SOA_DIR):
    services = get_services_for_cluster(
        cluster=cluster,
        instance_type='marathon',
        soa_dir=soa_dir,
    )
    configs = []
    for service, instance in services:
        try:
            service_config = load_marathon_service_config(
                service=service,
                instance=instance,
                cluster=cluster,
                soa_dir=soa_dir,
            )
        except NoDeploymentsAvailable:
            log.debug("%s is not deployed yet, refusing to do autoscaling calculations for it" %
                      compose_job_id(service, instance))
            continue

        if service_config.get_max_instances() and service_config.get_desired_state() == 'start' \
                and service_config.get_autoscaling_params()['decision_policy'] != 'bespoke':
            configs.append(service_config)

    return configs


@use_requests_cache('service_autoscaler')
def autoscale_services(soa_dir=DEFAULT_SOA_DIR):
    try:
        with create_autoscaling_lock():
            cluster = load_system_paasta_config().get_cluster()
            configs = get_configs_of_services_to_scale(cluster=cluster, soa_dir=soa_dir)
            if configs:
                marathon_config = load_marathon_config()
                marathon_client = get_marathon_client(
                    url=marathon_config.get_url(),
                    user=marathon_config.get_username(),
                    passwd=marathon_config.get_password())
                all_marathon_tasks = marathon_client.list_tasks()
                all_mesos_tasks = get_running_tasks_from_active_frameworks('')  # empty string matches all app ids
                with ZookeeperPool():
                    for config in configs:
                        try:
                            job_id = format_job_id(config.service, config.instance)
                            # Get a dict of healthy tasks, we assume tasks with no healthcheck defined
                            # are healthy. We assume tasks with no healthcheck results but a defined
                            # healthcheck to be unhealthy.
                            log.info("Inspecting %s for autoscaling" % job_id)
                            marathon_tasks = {task.id: task for task in all_marathon_tasks
                                              if job_id == get_short_job_id(task.id) and
                                              (is_task_healthy(task) or not
                                               marathon_client.get_app(task.app_id).health_checks)}
                            if not marathon_tasks:
                                raise MetricsProviderNoDataError("Couldn't find any healthy marathon tasks")
                            mesos_tasks = [task for task in all_mesos_tasks if task['id'] in marathon_tasks]
                            autoscale_marathon_instance(config, list(marathon_tasks.values()), mesos_tasks)
                        except Exception as e:
                            write_to_log(config=config, line='Caught Exception %s' % e)
    except LockHeldException:
        log.warning("Skipping autoscaling run for services because the lock is held")
        pass


def write_to_log(config, line, level='event'):
    _log(
        service=config.service,
        line=line,
        component='deploy',
        level=level,
        cluster=config.cluster,
        instance=config.instance,
    )


def get_short_job_id(task_id):
    return MESOS_TASK_SPACER.join(task_id.split(MESOS_TASK_SPACER, 2)[:2])


@contextmanager
def create_autoscaling_lock():
    """Acquire a lock in zookeeper for autoscaling. This is
    to avoid autoscaling a service multiple times, and to avoid
    having multiple paasta services all attempting to autoscale and
    fetching mesos data."""
    zk = KazooClient(hosts=load_system_paasta_config().get_zk_hosts(), timeout=ZK_LOCK_CONNECT_TIMEOUT_S)
    zk.start()
    lock = zk.Lock('/autoscaling/autoscaling.lock')
    try:
        lock.acquire(timeout=1)  # timeout=0 throws some other strange exception
        yield
    except LockTimeout:
        raise LockHeldException("Failed to acquire lock for autoscaling!")
    else:
        lock.release()
    finally:
        zk.stop()
