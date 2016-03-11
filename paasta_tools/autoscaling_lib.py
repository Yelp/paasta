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
import datetime

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

    zookeeper_root = compose_autoscaling_zookeeper_root(marathon_service_config.service,
                                                        marathon_service_config.instance)
    tstamp_zk_node = '%s/last_tstamp' % zookeeper_root
    error_zk_node = '%s/last_error' % zookeeper_root
    integral_term_zk_node = '%s/iterm' % zookeeper_root
    cpu_seconds_zk_node = '%s/cpu_seconds' % zookeeper_root
    start_time_zk_node = '%s/start_time' % zookeeper_root

    def clamp_value(number):
        """Limits the PID output to scaling up/down by 1.
        Also used to limit the integral term which avoids windup lag."""
        return min(max(number, -1), 1)

    def get_resource_data_from_task(task):
        cpu_shares = task.cpu_limit - .1
        used_seconds = task.stats.get('cpus_system_time_secs', 0.0) + task.stats.get('cpus_user_time_secs', 0.0)
        start_time = task['statuses'][0]['timestamp']
        return float(used_seconds) / cpu_shares, float(task.rss) / task.mem_limit, float(start_time)

    def get_zookeeper_data():
        with ZookeeperPool() as zk:
            last_time, _ = zk.get(tstamp_zk_node)
            last_error, _ = zk.get(error_zk_node)
            iterm, _ = zk.get(integral_term_zk_node)
            last_average_cpu_seconds, _ = zk.get(cpu_seconds_zk_node)
            last_average_start_time, _ = zk.get(start_time_zk_node)
            return (float(x) for x in (last_time, last_error, iterm, last_average_cpu_seconds, last_average_start_time))

    def send_zookeeper_data(current_time, error, iterm, average_cpu_seconds, average_start_time):
        with ZookeeperPool() as zk:
            zk.ensure_path(tstamp_zk_node)
            zk.ensure_path(error_zk_node)
            zk.ensure_path(integral_term_zk_node)
            zk.ensure_path(cpu_seconds_zk_node)
            zk.ensure_path(start_time_zk_node)
            zk.set(tstamp_zk_node, str(current_time))
            zk.set(error_zk_node, str(error))
            zk.set(integral_term_zk_node, str(iterm))
            zk.set(cpu_seconds_zk_node, str(average_cpu_seconds))
            zk.set(start_time_zk_node, str(average_start_time))

    def get_current_time():
        return int(datetime.datetime.now().strftime('%s'))

    current_time = get_current_time()

    try:
        last_time, last_error, iterm, last_average_cpu_seconds, last_average_start_time = get_zookeeper_data()
    except NoNodeError:
        # we have no historical data for this service yet
        current_time = 0
        error = 0.0
        iterm = 0.0
        pid_output = 0
        average_cpu_seconds = 0.0
        average_start_time = 0.0
    else:
        time_delta = current_time - last_time
        if time_delta < delay:
            return 0

        job_id = marathon_service_config.format_marathon_app_dict()['id']
        mesos_tasks = get_running_tasks_from_active_frameworks(job_id)
        if not mesos_tasks:
            return 0

        resource_data = [get_resource_data_from_task(task) for task in mesos_tasks]
        average_cpu_seconds, average_ram, average_start_time = (
            sum(item) / len(resource_data) for item in zip(*resource_data))

        current_time = get_current_time()

        time_delta = current_time - last_time
        cpu_seconds_delta = (average_cpu_seconds - last_average_cpu_seconds) / time_delta
        start_time_delta = (average_start_time - last_average_start_time) / time_delta

        average_cpu = (cpu_seconds_delta + start_time_delta) / time_delta

        # if one resource is underprovisioned and one is overprovisioned, we still want to scale up
        max_utilization = max(average_cpu, average_ram)
        error = max_utilization - setpoint
        iterm = clamp_value(iterm + (Ki * error) * time_delta)
        pid_output = round(clamp_value(Kp * error + iterm + Kd * (error - last_error) / time_delta))

    send_zookeeper_data(
        current_time=current_time,
        error=error,
        iterm=iterm,
        average_cpu_seconds=average_cpu_seconds,
        average_start_time=average_start_time,
    )

    return int(pid_output)


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
