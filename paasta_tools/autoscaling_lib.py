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
from contextlib import contextmanager
from datetime import datetime
from math import ceil

import requests
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

from paasta_tools.bounce_lib import LockHeldException
from paasta_tools.bounce_lib import LockTimeout
from paasta_tools.bounce_lib import ZK_LOCK_CONNECT_TIMEOUT_S
from paasta_tools.marathon_tools import compose_autoscaling_zookeeper_root
from paasta_tools.marathon_tools import format_job_id
from paasta_tools.marathon_tools import get_marathon_client
from paasta_tools.marathon_tools import load_marathon_config
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.marathon_tools import MESOS_TASK_SPACER
from paasta_tools.marathon_tools import set_instances_for_marathon_service
from paasta_tools.mesos_tools import get_running_tasks_from_active_frameworks
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import ZookeeperPool

_autoscaling_metrics_providers = {}
_autoscaling_decision_policies = {}


METRICS_PROVIDER_KEY = 'metrics_provider'
DECISION_POLICY_KEY = 'decision_policy'


def register_autoscaling_component(name, method_type):
    if method_type == METRICS_PROVIDER_KEY:
        component_dict = _autoscaling_metrics_providers
    elif method_type == DECISION_POLICY_KEY:
        component_dict = _autoscaling_decision_policies
    else:
        raise KeyError('Autoscaling component type %s does not exist.' % method_type)

    def outer(autoscaling_method):
        component_dict[name] = autoscaling_method
        return autoscaling_method
    return outer


def get_autoscaling_metrics_provider(name):
    """
    Returns an metrics provider matching the given name.
    """
    return _autoscaling_metrics_providers[name]


def get_autoscaling_decision_policy(name):
    """
    Returns a decision policy matching the given name.
    Decision policies determine the direction a service needs to be scaled in.
    Each decision policy returns one of the following values:
    -1: autoscale down
    0:  don't autoscale
    1:  autoscale up
    """
    return _autoscaling_decision_policies[name]


class IngesterNoDataError(ValueError):
    pass


@register_autoscaling_component('threshold', DECISION_POLICY_KEY)
def threshold_decision_policy(marathon_service_config, metrics_provider_method, marathon_tasks, mesos_tasks,
                              delay=600, setpoint=0.8, threshold=0.1, **kwargs):
    """
    Decides to autoscale a service up or down if the service utilization exceeds the setpoint
    by a certain threshold.
    """
    zk_last_time_path = '%s/threshold_last_time' % compose_autoscaling_zookeeper_root(
        service=marathon_service_config.service,
        instance=marathon_service_config.instance,
    )
    with ZookeeperPool() as zk:
        try:
            last_time, _ = zk.get(zk_last_time_path)
            last_time = float(last_time)
        except NoNodeError:
            last_time = 0.0

    current_time = int(datetime.now().strftime('%s'))
    if current_time - last_time < delay:
        return 0

    error = metrics_provider_method(marathon_service_config, marathon_tasks, mesos_tasks, **kwargs) - setpoint

    with ZookeeperPool() as zk:
        zk.ensure_path(zk_last_time_path)
        zk.set(zk_last_time_path, str(current_time))

    if error > threshold:
        return 1
    elif abs(error) > threshold:
        return -1
    else:
        return 0


def clamp_value(number):
    """Limits the PID output to scaling up/down by 1.
    Also used to limit the integral term which avoids windup lag."""
    return min(max(number, -1), 1)


@register_autoscaling_component('pid', DECISION_POLICY_KEY)
def pid_decision_policy(marathon_service_config, metrics_provider_method, marathon_tasks, mesos_tasks,
                        delay=600, setpoint=0.8, **kwargs):
    """
    Uses a PID to determine when to autoscale a service.
    See https://en.wikipedia.org/wiki/PID_controller for more information on PIDs.
    Kp, Ki and Kd are the canonical PID constants, where the output of the PID is:
    Kp * error + Ki * integral(error * dt) + Kd * (d(error) / dt)
    """
    Kp = 0.2
    Ki = 0.2 / delay
    Kd = 0.05 * delay

    autoscaling_root = compose_autoscaling_zookeeper_root(
        service=marathon_service_config.service,
        instance=marathon_service_config.instance,
    )
    zk_iterm_path = '%s/pid_iterm' % autoscaling_root
    zk_last_error_path = '%s/pid_last_error' % autoscaling_root
    zk_last_time_path = '%s/pid_last_time' % autoscaling_root

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

    if int(datetime.now().strftime('%s')) - last_time < delay:
        return 0

    utilization = metrics_provider_method(marathon_service_config, marathon_tasks, mesos_tasks, **kwargs)
    error = utilization - setpoint

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


@register_autoscaling_component('bespoke', DECISION_POLICY_KEY)
def bespoke_decision_policy(*args, **kwargs):
    """
    Autoscaling method for service authors that have written their own autoscaling method.
    Allows Marathon to read instance counts from Zookeeper but doesn't attempt to scale the service automatically.
    """
    return 0


@register_autoscaling_component('http', METRICS_PROVIDER_KEY)
def http_metrics_provider(marathon_service_config, marathon_tasks, mesos_tasks, endpoint='status', *args, **kwargs):
    """
    Gets the average utilization of a service across all of its tasks, where the utilization of
    a task is read from a HTTP endpoint on the host.

    The HTTP endpoint must return JSON with a 'utilization' key with a value from 0 to 1.

    :param marathon_service_config: the MarathonServiceConfig to get data from
    :param marathon_tasks: Marathon tasks to get data from
    :param mesos_tasks: Mesos tasks to get data from

    :returns: the service's average utilization, from 0 to 1
    """

    job_id = format_job_id(marathon_service_config.service, marathon_service_config.instance)
    endpoint = endpoint.lstrip('/')

    def get_short_job_id(task_id):
        return MESOS_TASK_SPACER.join(task_id.split(MESOS_TASK_SPACER, 2)[:2])

    tasks = [task for task in marathon_tasks if job_id == get_short_job_id(task.id) and task.health_check_results]
    utilization = []
    for task in tasks:
        try:
            utilization.append(float(requests.get('http://%s:%s/%s' % (
                task.host, task.ports[0], endpoint)).json()['utilization']))
        except Exception:
            pass
    if not utilization:
        raise IngesterNoDataError('Couldn\'t get any data from http endpoint %s for %s.%s' % (
            endpoint, marathon_service_config.service, marathon_service_config.instance))
    return sum(utilization) / len(utilization)


@register_autoscaling_component('mesos_cpu_ram', METRICS_PROVIDER_KEY)
def mesos_cpu_ram_metrics_provider(marathon_service_config, marathon_tasks, mesos_tasks, **kwargs):
    """
    Gets the average utilization of a service across all of its tasks, where the utilization of
    a task is the maximum value between its cpu and ram utilization.

    :param marathon_service_config: the MarathonServiceConfig to get data from
    :param marathon_tasks: Marathon tasks to get data from
    :param mesos_tasks: Mesos tasks to get data from

    :returns: the service's average utilization, from 0 to 1
    """

    job_id = format_job_id(marathon_service_config.service, marathon_service_config.instance)

    def get_short_job_id(task_id):
        return MESOS_TASK_SPACER.join(task_id.split(MESOS_TASK_SPACER, 2)[:2])

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

    healthy_ids = {task.id for task in marathon_tasks
                   if job_id == get_short_job_id(task.id) and task.health_check_results}
    if not healthy_ids:
        raise IngesterNoDataError("Couldn't find any healthy marathon tasks")

    mesos_tasks = {task['id']: task.stats for task in mesos_tasks if task['id'] in healthy_ids}
    current_time = int(datetime.now().strftime('%s'))
    time_delta = current_time - last_time

    mesos_cpu_data = {task_id: float(stats.get('cpus_system_time_secs', 0.0) + stats.get(
        'cpus_user_time_secs', 0.0)) / (stats.get('cpus_limit', 0) - .1) for task_id, stats in mesos_tasks.items()}

    utilization = {}
    for datum in last_cpu_data:
        last_cpu_seconds, task_id = datum.split(':')
        if task_id in mesos_cpu_data:
            utilization[task_id] = (mesos_cpu_data[task_id] - float(last_cpu_seconds)) / time_delta

    for task_id, stats in mesos_tasks.items():
        if stats.get('mem_limit_bytes', 0) != 0:
            utilization[task_id] = max(
                utilization.get(task_id, 0),
                float(stats.get('mem_rss_bytes', 0)) / stats.get('mem_limit_bytes', 0),
            )

    if not utilization:
        raise IngesterNoDataError("Couldn't get any cpu or ram data from Mesos")

    task_utilization = utilization.values()
    average_utilization = sum(task_utilization) / len(task_utilization)

    cpu_data_csv = ','.join('%s:%s' % (cpu_seconds, task_id) for task_id, cpu_seconds in mesos_cpu_data.items())

    with ZookeeperPool() as zk:
        zk.ensure_path(zk_last_cpu_data)
        zk.ensure_path(zk_last_time_path)
        zk.set(zk_last_cpu_data, str(cpu_data_csv))
        zk.set(zk_last_time_path, str(current_time))

    return average_utilization


def get_new_instance_count(current_instances, autoscaling_direction):
    return int(ceil((1 + float(autoscaling_direction) / 10) * current_instances))


def autoscale_marathon_instance(marathon_service_config, marathon_tasks, mesos_tasks):
    autoscaling_params = marathon_service_config.get_autoscaling_params()
    autoscaling_metrics_provider = get_autoscaling_metrics_provider(autoscaling_params[METRICS_PROVIDER_KEY])
    autoscaling_decision_policy = get_autoscaling_decision_policy(autoscaling_params[DECISION_POLICY_KEY])
    autoscaling_direction = autoscaling_decision_policy(marathon_service_config, autoscaling_metrics_provider,
                                                        marathon_tasks, mesos_tasks, **autoscaling_params)
    if autoscaling_direction:
        current_instances = marathon_service_config.get_instances()
        autoscaling_amount = get_new_instance_count(current_instances, autoscaling_direction)
        instances = marathon_service_config.limit_instance_count(autoscaling_amount)
        if instances != current_instances:
            write_to_log(config=marathon_service_config, line='Scaling from %d to %d' % (current_instances, instances))
            set_instances_for_marathon_service(
                service=marathon_service_config.service,
                instance=marathon_service_config.instance,
                instance_count=instances,
            )


def autoscale_services(soa_dir=DEFAULT_SOA_DIR):
    try:
        with create_autoscaling_lock():
            cluster = load_system_paasta_config().get_cluster()
            services = get_services_for_cluster(
                cluster=cluster,
                instance_type='marathon',
                soa_dir=soa_dir,
            )
            configs = []
            for service, instance in services:
                service_config = load_marathon_service_config(
                    service=service,
                    instance=instance,
                    cluster=cluster,
                    soa_dir=soa_dir,
                )
                if service_config.get_max_instances() and service_config.get_desired_state() == 'start':
                    configs.append(service_config)

            if configs:
                marathon_config = load_marathon_config()
                marathon_tasks = get_marathon_client(
                    url=marathon_config.get_url(),
                    user=marathon_config.get_username(),
                    passwd=marathon_config.get_password(),
                ).list_tasks()
                mesos_tasks = get_running_tasks_from_active_frameworks('')
                for config in configs:
                    try:
                        autoscale_marathon_instance(config, marathon_tasks, mesos_tasks)
                    except Exception as e:
                        write_to_log(config=config, line='Caught Exception %s' % e, level='event')
    except LockHeldException:
        pass


def write_to_log(config, line, level='debug'):
    _log(
        service=config.service,
        line=line,
        component='deploy',
        level=level,
        cluster=config.cluster,
        instance=config.instance,
    )


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
