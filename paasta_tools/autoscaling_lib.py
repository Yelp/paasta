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
from math import ceil

from kazoo.exceptions import NoNodeError

from paasta_tools.marathon_tools import compose_autoscaling_zookeeper_root
from paasta_tools.marathon_tools import format_job_id
from paasta_tools.marathon_tools import get_marathon_client
from paasta_tools.marathon_tools import load_marathon_config
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.marathon_tools import MESOS_TASK_SPACER
from paasta_tools.marathon_tools import set_instances_for_marathon_service
from paasta_tools.mesos_tools import get_running_tasks_from_active_frameworks
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import ZookeeperPool

_autoscaling_methods = {}


def register_autoscaling_method(name):
    def outer(autoscaling_method):
        _autoscaling_methods[name] = autoscaling_method
        return autoscaling_method
    return outer


def get_autoscaling_method(function_name):
    return _autoscaling_methods[function_name]


@register_autoscaling_method('bespoke')
def bespoke_autoscaling_method(*args, **kwargs):
    """
    Autoscaling method for service authors that have written their own autoscaling method.
    Allows Marathon to read instance counts from Zookeeper but doesn't attempt to scale the service automatically.
    """
    return 0


@register_autoscaling_method('default')
def default_autoscaling_method(marathon_service_config, marathon_client, mesos_tasks,
                               delay=600, setpoint=0.8, **kwargs):
    """
    PID control of instance count using ram and cpu.

    :param marathon_service_config: the MarathonServiceConfig to scale
    :param marathon_client: a Marathon client to fetch task data
    :param delay: the number of seconds to wait between PID updates
    :param setpoint: the target utilization percentage

    :returns: the number of instances to scale up/down by
    """
    Kp = 0.2
    Ki = 0.2 / delay
    Kd = 0.05 * delay

    job_id = format_job_id(marathon_service_config.service, marathon_service_config.instance)

    def clamp_value(number):
        """Limits the PID output to scaling up/down by 1.
        Also used to limit the integral term which avoids windup lag."""
        return min(max(number, -1), 1)

    def get_current_time():
        return int(datetime.now().strftime('%s'))

    def get_short_job_id(task_id):
        return MESOS_TASK_SPACER.join(task_id.split(MESOS_TASK_SPACER, 2)[:2])

    autoscaling_root = compose_autoscaling_zookeeper_root(
        service=marathon_service_config.service,
        instance=marathon_service_config.instance,
    )
    zk_iterm_path = '%s/iterm' % autoscaling_root
    zk_last_error_path = '%s/last_error' % autoscaling_root
    zk_last_time_path = '%s/last_time' % autoscaling_root
    zk_last_cpu_data = '%s/cpu_data' % autoscaling_root

    with ZookeeperPool() as zk:
        try:
            iterm, _ = zk.get(zk_iterm_path)
            last_error, _ = zk.get(zk_last_error_path)
            last_time, _ = zk.get(zk_last_time_path)
            last_cpu_data, _ = zk.get(zk_last_cpu_data)
            iterm = float(iterm)
            last_error = float(last_error)
            last_time = float(last_time)
            last_cpu_data = (datum for datum in last_cpu_data.split(',') if datum)
        except NoNodeError:
            iterm = 0.0
            last_error = 0.0
            last_time = 0.0
            last_cpu_data = []

    if get_current_time() - last_time < delay:
        return 0

    healthy_ids = {task.id for task in marathon_client.list_tasks()
                   if job_id == get_short_job_id(task.id) and task.health_check_results}
    if not healthy_ids:
        return 0

    mesos_tasks = {task['id']: task.stats for task in mesos_tasks if task['id'] in healthy_ids}
    current_time = get_current_time()
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
        return 0

    task_errors = [amount - setpoint for amount in utilization.values()]
    error = sum(task_errors) / len(task_errors)
    iterm = clamp_value(iterm + (Ki * error) * time_delta)

    cpu_data_csv = ','.join('%s:%s' % (cpu_seconds, task_id) for task_id, cpu_seconds in mesos_cpu_data.items())

    with ZookeeperPool() as zk:
        zk.ensure_path(zk_iterm_path)
        zk.ensure_path(zk_last_error_path)
        zk.ensure_path(zk_last_time_path)
        zk.ensure_path(zk_last_cpu_data)
        zk.set(zk_iterm_path, str(iterm))
        zk.set(zk_last_error_path, str(error))
        zk.set(zk_last_time_path, str(current_time))
        zk.set(zk_last_cpu_data, str(cpu_data_csv))

    return int(round(clamp_value(Kp * error + iterm + Kd * (error - last_error) / time_delta)))


def get_new_instance_count(current_instances, autoscaling_direction):
    return int(ceil((1 + float(autoscaling_direction) / 10) * current_instances))


def autoscale_marathon_instance(marathon_service_config, marathon_client, mesos_tasks):
    autoscaling_params = marathon_service_config.get_autoscaling_params()
    autoscaling_method = get_autoscaling_method(autoscaling_params['method'])
    autoscaling_direction = autoscaling_method(marathon_service_config, marathon_client, mesos_tasks,
                                               **autoscaling_params)
    if autoscaling_direction:
        current_instances = marathon_service_config.get_instances()
        autoscaling_amount = get_new_instance_count(current_instances, autoscaling_direction)
        instances = marathon_service_config.limit_instance_count(autoscaling_amount)
        if instances != current_instances:
            set_instances_for_marathon_service(
                service=marathon_service_config.service,
                instance=marathon_service_config.instance,
                instance_count=instances,
            )


def autoscale_services(soa_dir=DEFAULT_SOA_DIR):
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

    marathon_config = load_marathon_config()
    marathon_client = get_marathon_client(
        url=marathon_config.get_url(),
        user=marathon_config.get_username(),
        passwd=marathon_config.get_password(),
    )
    mesos_tasks = get_running_tasks_from_active_frameworks('')
    with ZookeeperPool():
        for config in configs:
            autoscale_marathon_instance(config, marathon_client, mesos_tasks)
