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
import time
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from math import ceil
from math import floor

import boto3
import requests
from botocore.exceptions import ClientError
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

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
from paasta_tools.mesos_tools import get_mesos_state_from_leader
from paasta_tools.mesos_tools import get_mesos_task_count_by_slave
from paasta_tools.mesos_tools import get_running_tasks_from_active_frameworks
from paasta_tools.mesos_tools import slave_pid_to_ip
from paasta_tools.paasta_maintenance import drain
from paasta_tools.paasta_metastatus import get_resource_utilization_by_grouping
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import Timeout
from paasta_tools.utils import TimeoutError
from paasta_tools.utils import ZookeeperPool

_autoscaling_components = defaultdict(dict)

SERVICE_METRICS_PROVIDER_KEY = 'metrics_provider'
CLUSTER_METRICS_PROVIDER_KEY = 'cluster_metrics_provider'
CLUSTER_TARGET_UTILIZATION = 0.8
CLUSTER_DRAIN_TIMEOUT = 900  # seconds
DECISION_POLICY_KEY = 'decision_policy'
SCALER_KEY = 'scaler'

AUTOSCALING_DELAY = 300
MISSING_SLAVE_PANIC_THRESHOLD = .3
MAX_CLUSTER_DELTA = .1

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


def register_autoscaling_component(name, method_type):
    def outer(autoscaling_method):
        _autoscaling_components[method_type][name] = autoscaling_method
        return autoscaling_method
    return outer


def get_service_metrics_provider(name):
    """
    Returns a service metrics provider matching the given name.
    """
    return _autoscaling_components[SERVICE_METRICS_PROVIDER_KEY][name]


def get_cluster_metrics_provider(name):
    """
    Returns a cluster metrics provider matching the given name.
    """
    return _autoscaling_components[CLUSTER_METRICS_PROVIDER_KEY][name]


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


def get_scaler(name):
    """
    Returns a scaler matching the given name.
    """
    return _autoscaling_components[SCALER_KEY][name]


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


@register_autoscaling_component('http', SERVICE_METRICS_PROVIDER_KEY)
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

    endpoint = endpoint.lstrip('/')
    utilization = []
    for task in marathon_tasks:
        try:
            utilization.append(float(requests.get('http://%s:%s/%s' % (
                task.host, task.ports[0], endpoint)).json()['utilization']))
        except Exception:
            pass
    if not utilization:
        raise MetricsProviderNoDataError('Couldn\'t get any data from http endpoint %s for %s.%s' % (
            endpoint, marathon_service_config.service, marathon_service_config.instance))
    return sum(utilization) / len(utilization)


@register_autoscaling_component('mesos_cpu', SERVICE_METRICS_PROVIDER_KEY)
def mesos_cpu_metrics_provider(marathon_service_config, marathon_tasks, mesos_tasks, **kwargs):
    """
    Gets the average utilization of a service across all of its tasks, where the utilization of
    a task is the maximum value between its cpu and ram utilization.

    :param marathon_service_config: the MarathonServiceConfig to get data from
    :param marathon_tasks: Marathon tasks to get data from
    :param mesos_tasks: Mesos tasks to get data from

    :returns: the service's average utilization, from 0 to 1
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
        raise MetricsProviderNoDataError("Couldn't get any cpu or ram data from Mesos")

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
    average_utilization = sum(task_utilization) / len(task_utilization)

    return average_utilization


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
                if service_config.get_max_instances() and service_config.get_desired_state() == 'start' \
                        and service_config.get_autoscaling_params()['decision_policy'] != 'bespoke':
                    configs.append(service_config)

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


def get_spot_fleet_instances(spotfleet_request_id):
    ec2_client = boto3.client('ec2')
    spot_fleet_instances = ec2_client.describe_spot_fleet_instances(
        SpotFleetRequestId=spotfleet_request_id)['ActiveInstances']
    return spot_fleet_instances


def get_sfr_instance_ips(spotfleet_request_id):
    ec2_client = boto3.client('ec2')
    spot_fleet_instances = get_spot_fleet_instances(spotfleet_request_id)
    instance_ips = {
        instance['PrivateIpAddress']
        for reservation in ec2_client.describe_instances(
            InstanceIds=[instance['InstanceId'] for instance in spot_fleet_instances])['Reservations']
        for instance in reservation['Instances']
    }
    return instance_ips


def get_sfr_state(spotfleet_request_id):
    ec2_client = boto3.client('ec2')
    try:
        sfrs = ec2_client.describe_spot_fleet_requests(SpotFleetRequestIds=[spotfleet_request_id])
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidSpotFleetRequestId.NotFound':
            log.warn('Cannot find SFR {0}'.format(spotfleet_request_id))
            return None
        else:
            raise
    ret = sfrs['SpotFleetRequestConfigs'][0]['SpotFleetRequestState']
    return ret


@register_autoscaling_component('aws_spot_fleet_request', CLUSTER_METRICS_PROVIDER_KEY)
def spotfleet_metrics_provider(spotfleet_request_id, mesos_state, resource):
    if not get_sfr_state(spotfleet_request_id) == 'active':
        log.error("Ignoring SFR {0} that is not yet active or is cancelled etc.".format(spotfleet_request_id))
        return 0, 0
    desired_instances = len(get_spot_fleet_instances(spotfleet_request_id))
    instance_ips = get_sfr_instance_ips(spotfleet_request_id)
    slaves = {
        slave['id']: slave for slave in mesos_state.get('slaves', [])
        if slave_pid_to_ip(slave['pid']) in instance_ips and
        slave['attributes'].get('pool', 'default') == resource['pool']
    }
    current_instances = len(slaves)
    log.info("Found %.2%% slaves registered in mesos for this SFR (%d/%d)" % (
             (current_instances / desired_instances) * 100, current_instances, desired_instances))
    if float(current_instances) / desired_instances < (1.00 - MISSING_SLAVE_PANIC_THRESHOLD):
        error_message = ("We currently have %d instances active in mesos out of a desired %d.\n"
                         "Refusing to scale because we either need to wait for the requests to be "
                         "filled, or the new instances are not healthy for some reason.\n"
                         "(cowardly refusing to go past %.2f%% missing instances)") % (
            current_instances, desired_instances, MISSING_SLAVE_PANIC_THRESHOLD)
        raise ClusterAutoscalingError(error_message)

    pool_utilization_dict = get_resource_utilization_by_grouping(
        lambda slave: slave['attributes']['pool'],
        mesos_state
    )[resource['pool']]

    log.debug(pool_utilization_dict)
    free_pool_resources = pool_utilization_dict['free']
    total_pool_resources = pool_utilization_dict['total']
    utilization = 1.0 - min([
        float(float(pair[0]) / float(pair[1]))
        for pair in zip(free_pool_resources, total_pool_resources)
    ])
    error = utilization - CLUSTER_TARGET_UTILIZATION
    delta, target = get_spot_fleet_delta(resource, error)
    return delta, target


def get_spot_fleet_delta(resource, error):
    ec2_client = boto3.client('ec2')
    spot_fleet_request = ec2_client.describe_spot_fleet_requests(
        SpotFleetRequestIds=[resource['id']])['SpotFleetRequestConfigs'][0]
    if spot_fleet_request['SpotFleetRequestState'] != 'active':
        raise ClusterAutoscalingError('Can not scale non-active spot fleet requests. This one is "%s"' %
                                      spot_fleet_request['SpotFleetRequestState'])
    current_capacity = int(spot_fleet_request['SpotFleetRequestConfig']['TargetCapacity'])
    ideal_capacity = int(ceil((1 + error) * current_capacity))
    log.debug("Ideal calculated capacity is %d instances" % ideal_capacity)
    new_capacity = int(min(
        max(
            resource['min_instances'],
            floor(current_capacity * (1.00 - MAX_CLUSTER_DELTA)),
            ideal_capacity,
            1,  # A SFR cannot scale below 1 instance
        ),
        ceil(current_capacity * (1.00 + MAX_CLUSTER_DELTA)),
        resource['max_instances'],
    ))
    log.debug("The new capacity to scale to is %d instances" % ideal_capacity)

    if ideal_capacity > resource['max_instances']:
        log.warning("Our ideal capacity (%d) is higher than max_instances (%d). Consider rasing max_instances!" % (
            ideal_capacity, resource['max_instances']))
    if ideal_capacity < resource['min_instances']:
        log.warning("Our ideal capacity (%d) is lower than min_instances (%d). Consider lowering min_instances!" % (
            ideal_capacity, resource['min_instances']))
    if (ideal_capacity < floor(current_capacity * (1.00 - MAX_CLUSTER_DELTA)) or
            ideal_capacity > ceil(current_capacity * (1.00 + MAX_CLUSTER_DELTA))):
        log.warning(
            "Our ideal capacity (%d) is greater than %.2f%% of current %d. Just doing a %.2f%% change for now to %d." %
            (ideal_capacity, MAX_CLUSTER_DELTA * 100, current_capacity, MAX_CLUSTER_DELTA * 100, new_capacity))

    if new_capacity != current_capacity:
        print "Scaling SFR %s from %d to %d!" % (resource['id'], current_capacity, new_capacity)
        return new_capacity - current_capacity, new_capacity
    else:
        return 0


def sort_slaves_to_kill(mesos_state, pool='default'):
    """Pick the best slaves to kill. This returns a list of slaves
    after sorting in preference of which slaves we kill first.
    It sorts first by number of chronos tasks, then by total number of tasks

    :param mesos_state: mesos_state dict
    :param pool: pool of slaves to consider
    :returns: list of slaves"""
    slaves = get_mesos_task_count_by_slave(mesos_state, pool=pool)
    if slaves:
        slaves_by_task_count = [slave.slave for slave in sorted(slaves.values(),
                                                                key=lambda x: (x.chronos_count, x.count))]
        return slaves_by_task_count
    else:
        return []


def wait_and_terminate(instances_to_kill, dry_run):
    """Currently kills a list of instances, will wait for draining to complete soon

    :param instances_to_kill: dict of instances to kill
    :param dry_run: Don't drain or make changes to spot fleet if True"""
    ec2_client = boto3.client('ec2')
    try:
        # This loop should always finish because the maintenance window should trigger is_ready_to_kill
        # being true. Just in case though we set a timeout and terminate anyway
        with Timeout(seconds=CLUSTER_DRAIN_TIMEOUT + 300):
            while instances_to_kill:
                for slave_pid, slave_data in instances_to_kill.items():
                    instance_id = slave_data['instance_id']
                    if not instance_id:
                        log.warning("Didn't find instance ID for slave: {0}. Skipping terminating".format(slave_pid))
                        continue
                    is_ready_to_kill = True
                    # is_ready_to_kill = paasta_maintenance.something(slave['pid'])
                    if is_ready_to_kill:
                        log.info("TERMINATING: {0}".format(instance_id))
                        try:
                            ec2_client.terminate_instances(InstanceIds=[instance_id], DryRun=dry_run)
                        except ClientError as e:
                            if e.response['Error'].get('Code') == 'DryRunOperation':
                                pass
                            else:
                                raise
                        instances_to_kill.pop(slave_pid)
                    else:
                        log.debug("Instance {0}: NOT ready to kill".format(instance_id))
                log.debug("Waiting 5 seconds and then checking again")
                time.sleep(5)
    except TimeoutError:
        log.error("Timed out after {0} waiting to drain {1}, now terminating anyway".format(CLUSTER_DRAIN_TIMEOUT,
                                                                                            instances_to_kill))
        try:
            ec2_client.terminate_instances(InstanceIds=instances_to_kill, DryRun=dry_run)
        except ClientError as e:
            if e.response['Error'].get('Code') == 'DryRunOperation':
                pass
            else:
                raise


@register_autoscaling_component('aws_spot_fleet_request', SCALER_KEY)
def scale_aws_spot_fleet_request(resource, delta, target_capacity, sorted_slaves, dry_run):
    """Scales a spot fleet request by delta to reach target capacity
    If scaling up we just set target capacity and let AWS take care of the rest
    If scaling down we pick the slaves we'd prefer to kill, put them in maintenance
    mode and drain them (via paasta_maintenance and setup_marathon_jobs). We then kill
    them once they are running 0 tasks or once a timeout is reached

    :param resource: resource to scale
    :param delta: integer change in number of servers
    :param target_capacity: target number of instances
    :param sorted_slaves: list of slaves by order to kill
    :param dry_run: Don't drain or make changes to spot fleet if True"""
    sfr_id = resource['id']
    ec2_client = boto3.client('ec2')
    if delta == 0:
        return
    elif delta > 0:
        log.info("Increasing spot fleet capacity to: {0}".format(target_capacity))
        if not dry_run:
            ec2_client.modify_spot_fleet_request(SpotFleetRequestId=sfr_id, TargetCapacity=target_capacity,
                                                 ExcessCapacityTerminationPolicy='noTermination')
            return
    elif delta < 0:
        number_to_kill = delta * -1
        sfr_ips = get_sfr_instance_ips(sfr_id)
        log.debug("IPs in SFR: {0}".format(sfr_ips))
        sfr_sorted_slaves = [slave for slave in sorted_slaves if slave_pid_to_ip(slave['pid']) in sfr_ips]
        log.info("SFR slave kill preference: {0}".format([slave['pid'] for slave in sfr_sorted_slaves]))
        if number_to_kill > len(sfr_sorted_slaves):
            log.error("Didn't find enough candidates to kill. This shouldn't happen so let's not kill anything!")
            return
        slaves_to_kill = sfr_sorted_slaves[0:number_to_kill]
        log.info("Set to kill: {0}".format([slave['pid'] for slave in slaves_to_kill]))
        instances_to_kill = {}
        for slave in slaves_to_kill:
            ip = slave_pid_to_ip(slave['pid'])
            instances_to_kill[slave['pid']] = {'ip': ip,
                                               'instance_id': get_instance_id_from_ip(ip)}
        # The start time of the maintenance window is the point at which
        # we giveup waiting for the instance to drain and mark it for termination anyway
        start = int(time.time() + CLUSTER_DRAIN_TIMEOUT)
        # Set the duration to an hour, if we haven't cleaned up and termintated by then
        # mesos should put the slave back into the pool
        duration = 600
        log.info("Draining {0}".format(instances_to_kill))
        log.info("Decreasing spot fleet capacity to: {0}".format(target_capacity))
        if not dry_run:
            # sort to make testing easier
            drain([instance['ip'] for instance in sorted(instances_to_kill.values())], start, duration)
            ec2_client.modify_spot_fleet_request(SpotFleetRequestId=sfr_id, TargetCapacity=target_capacity,
                                                 ExcessCapacityTerminationPolicy='noTermination')
        log.info("Waiting for instances to drain before we terminate")
        wait_and_terminate(instances_to_kill, dry_run)


class ClusterAutoscalingError(Exception):
    pass


def get_instance_id_from_ip(ip):
    """Get AWS instance ID from private IP

    :param ip: private IP of AWS instance
    :returns: Instance ID"""
    ec2_client = boto3.client('ec2')
    instances = ec2_client.describe_instances(Filters=[{'Name': 'private-ip-address', 'Values': [ip]}])
    try:
        instance_id = instances['Reservations'][0]['Instances'][0]['InstanceId']
    except IndexError:
        log.error('Cannot find instance from IP: {0}'.format(ip))
        return None
    return instance_id


def autoscale_local_cluster(dry_run=False):
    if dry_run:
        log.info("Running in dry_run mode, no changes should be made")
    system_config = load_system_paasta_config()
    autoscaling_resources = system_config.get_cluster_autoscaling_resources()
    mesos_state = get_mesos_state_from_leader()
    for identifier, resource in autoscaling_resources.items():
        log.info("Autoscaling {0}".format(identifier))
        resource_metrics_provider = get_cluster_metrics_provider(resource['type'])
        try:
            delta, target = resource_metrics_provider(resource['id'], mesos_state, resource)
            log.info("Target capacity: {0}, Capacity delta: {1}".format(target, delta))
            resource_scaler = get_scaler(resource['type'])
            if delta < 0:
                sorted_slaves = sort_slaves_to_kill(mesos_state, pool=resource['pool'])
                log.debug("Slaves by kill preference: {0}".format(sorted_slaves))
            else:
                sorted_slaves = []
            resource_scaler(resource, delta, target, sorted_slaves, dry_run)
        except ClusterAutoscalingError as e:
            log.error('%s: %s' % (identifier, e))
