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
from datetime import datetime
from time import sleep

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from service_configuration_lib import DEFAULT_SOA_DIR

from paasta_tools.mesos_tools import get_running_tasks_from_active_frameworks
from paasta_tools.utils import load_system_paasta_config

_autoscaling_methods = {}


def register_autoscaling_method(name):
    def outer(autoscaling_method):
        _autoscaling_methods[name] = autoscaling_method
        return autoscaling_method
    return outer


def get_autoscaling_method(function_name):
    return _autoscaling_methods[function_name]


def compose_autoscaling_zookeeper_root(service, instance):
    return '/autoscaling/%s/%s' % (service, instance)


def set_instances_for_marathon_service(service, instance, instance_count, soa_dir=DEFAULT_SOA_DIR):
    zookeeper_path = '%s/instances' % compose_autoscaling_zookeeper_root(service, instance)
    with ZookeeperPool() as zookeeper_client:
        zookeeper_client.ensure_path(zookeeper_path)
        zookeeper_client.set(zookeeper_path, str(instance_count))


def get_instances_from_zookeeper(service, instance):
    with ZookeeperPool() as zookeeper_client:
        (instances, _) = zookeeper_client.get('%s/instances' % compose_autoscaling_zookeeper_root(service, instance))
        return int(instances)


@register_autoscaling_method('bespoke')
def bespoke_autoscaling_method(*args, **kwargs):
    """
    Autoscaling method for service authors that have written their own autoscaling method.
    Allows Marathon to read instance counts from Zookeeper but doesn't attempt to scale the service automatically.
    """
    return 0


@register_autoscaling_method('default')
def default_autoscaling_method(marathon_service_config, delay=600, setpoint=0.8, **kwargs):
    """
    PID control of instance count using ram and cpu.

    :param marathon_service_config: the MarathonServiceConfig to scale
    :param delay: the number of seconds to wait between PID updates
    :param setpoint: the target utilization percentage

    :returns: the number of instances to scale up/down by
    """
    Kp = 0.2
    Ki = 0.2 / delay
    Kd = 0.05 * delay

    def clamp_value(number):
        """Limits the PID output to scaling up/down by 1.
        Also used to limit the integral term which avoids windup lag."""
        return min(max(number, -1), 1)

    def get_current_time():
        return int(datetime.now().strftime('%s'))

    def get_mesos_cpu_data(mesos_tasks):
        return {task['id']: (float(task.stats.get('cpus_system_time_secs', 0.0) + task.stats.get(
            'cpus_user_time_secs', 0.0)) / (task.cpu_limit - .1)) for task in mesos_tasks}

    autoscaling_root = compose_autoscaling_zookeeper_root(
        service=marathon_service_config.service,
        instance=marathon_service_config.instance,
    )
    zk_iterm_path = '%s/iterm' % autoscaling_root
    zk_last_error_path = '%s/last_error' % autoscaling_root
    zk_last_time_path = '%s/last_time' % autoscaling_root

    with ZookeeperPool() as zk:
        try:
            iterm, _ = zk.get(zk_iterm_path)
            last_error, _ = zk.get(zk_last_error_path)
            last_time, _ = zk.get(zk_last_error_path)
            iterm = float(iterm)
            last_error = float(last_error)
            last_time = float(last_time)
        except NoNodeError:
            iterm = 0.0
            last_error = 0.0
            last_time = 0.0

    initial_time = get_current_time()
    if initial_time - last_time < delay:
        return 0
    job_id = marathon_service_config.format_marathon_app_dict()['id']

    mesos_tasks = get_running_tasks_from_active_frameworks(job_id)
    initial_task_cpu_data = get_mesos_cpu_data(mesos_tasks)

    sleep(5)

    mesos_tasks = get_running_tasks_from_active_frameworks(job_id)
    current_time = get_current_time()
    time_delta = current_time - initial_time
    task_cpu_data = get_mesos_cpu_data(mesos_tasks)

    utilization = {task_id: (cpu_seconds - initial_task_cpu_data[task_id]) / time_delta
                   for task_id, cpu_seconds in task_cpu_data.items() if task_id in initial_task_cpu_data}

    for task in mesos_tasks:
        utilization[task['id']] = max(utilization[task['id']], float(task.rss) / task.mem_limit)

    task_errors = [amount - setpoint for amount in utilization.values()]
    error = sum(task_errors) / len(task_errors)
    iterm = clamp_value(iterm + (Ki * error) * time_delta)

    with ZookeeperPool() as zk:
        zk.ensure_path(zk_iterm_path)
        zk.ensure_path(zk_last_error_path)
        zk.ensure_path(zk_last_time_path)
        zk.set(zk_iterm_path, str(iterm))
        zk.set(zk_last_error_path, str(error))
        zk.set(zk_last_time_path, str(current_time))

    return int(round(clamp_value(Kp * error + iterm + Kd * (error - last_error) / time_delta)))


def autoscale_marathon_instance(marathon_service_config):
    if marathon_service_config.get_max_instances() is None:
        return
    autoscaling_params = marathon_service_config.get_autoscaling_params()
    autoscaling_method = get_autoscaling_method(autoscaling_params['method'])
    with ZookeeperPool():
        autoscale_amount = autoscaling_method(marathon_service_config, **autoscaling_params)
        if autoscale_amount:
            current_instances = marathon_service_config.get_instances()
            instances = min(
                marathon_service_config.get_max_instances(),
                max(marathon_service_config.get_min_instances(),
                    current_instances + autoscale_amount),
            )
            if instances != current_instances:
                set_instances_for_marathon_service(
                    service=marathon_service_config.service,
                    instance=marathon_service_config.instance,
                    instance_count=instances,
                )


class ZookeeperPool(object):
    """
    A context manager that shares the same KazooClient with its children. The first nested contest manager
    creates and deletes the client and shares it with any of its children. This allows to place a context
    manager over a large number of zookeeper calls without opening and closing a connection each time.
    GIL makes this 'safe'.
    """
    counter = 0
    zk = None

    @classmethod
    def __enter__(cls):
        if cls.zk is None:
            cls.zk = KazooClient(hosts=load_system_paasta_config().get_zk_hosts(), read_only=True)
            cls.zk.start()
        cls.counter = cls.counter + 1
        return cls.zk

    @classmethod
    def __exit__(cls, *args, **kwargs):
        cls.counter = cls.counter - 1
        if cls.counter == 0:
            cls.zk.stop()
            cls.zk.close()
            cls.zk = None
