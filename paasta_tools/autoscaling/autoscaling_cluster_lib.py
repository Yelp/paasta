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
from math import ceil
from math import floor

import boto3
from botocore.exceptions import ClientError
from requests.exceptions import HTTPError

from paasta_tools.autoscaling.utils import _autoscaling_components
from paasta_tools.autoscaling.utils import register_autoscaling_component
from paasta_tools.mesos_maintenance import drain
from paasta_tools.mesos_maintenance import undrain
from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.mesos_tools import get_mesos_task_count_by_slave
from paasta_tools.mesos_tools import slave_pid_to_ip
from paasta_tools.paasta_maintenance import is_safe_to_kill
from paasta_tools.paasta_metastatus import get_resource_utilization_by_grouping
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import Timeout
from paasta_tools.utils import TimeoutError


CLUSTER_METRICS_PROVIDER_KEY = 'cluster_metrics_provider'
DEFAULT_TARGET_UTILIZATION = 0.8  # decimal fraction
DEFAULT_DRAIN_TIMEOUT = 600  # seconds
SCALER_KEY = 'scaler'

AWS_SPOT_MODIFY_TIMEOUT = 30
MISSING_SLAVE_PANIC_THRESHOLD = .3
MAX_CLUSTER_DELTA = .2

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


def get_cluster_metrics_provider(name):
    """
    Returns a cluster metrics provider matching the given name.
    """
    return _autoscaling_components[CLUSTER_METRICS_PROVIDER_KEY][name]


def get_scaler(name):
    """
    Returns a scaler matching the given name.
    """
    return _autoscaling_components[SCALER_KEY][name]


def get_spot_fleet_instances(spotfleet_request_id, region=None):
    ec2_client = boto3.client('ec2', region_name=region)
    spot_fleet_instances = ec2_client.describe_spot_fleet_instances(
        SpotFleetRequestId=spotfleet_request_id)['ActiveInstances']
    return spot_fleet_instances


def get_sfr_instance_ips(sfr, region=None):
    spot_fleet_instances = sfr['ActiveInstances']
    instance_descriptions = describe_instances([instance['InstanceId'] for instance in spot_fleet_instances],
                                               region=region)
    instance_ips = []
    for instance in instance_descriptions:
        try:
            instance_ips.append(instance['PrivateIpAddress'])
        except KeyError:
            log.warning("Instance {0} does not have an IP. This normally means it has been"
                        " terminated".format(instance['InstanceId']))
    return instance_ips


def get_sfr(spotfleet_request_id, region=None):
    ec2_client = boto3.client('ec2', region_name=region)
    try:
        sfrs = ec2_client.describe_spot_fleet_requests(SpotFleetRequestIds=[spotfleet_request_id])
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidSpotFleetRequestId.NotFound':
            log.warn('Cannot find SFR {0}'.format(spotfleet_request_id))
            return None
        else:
            raise
    ret = sfrs['SpotFleetRequestConfigs'][0]
    return ret


def describe_instances(instance_ids, region=None, instance_filters=None):
    """This wraps ec2.describe_instances and catches instance not
    found errors. It returns a list of instance description
    dictionaries. It assumes one instance per reservation (which
    seems to be the case for SFRs) Optionally, a filter can be
    passed through to the ec2 call

    :param instance_ids: a list of instance ids, [] means all
    :param instance_filters: a list of ec2 filters
    :returns: a list of instance description dictionaries"""
    if not instance_filters:
        instance_filters = []
    ec2_client = boto3.client('ec2', region_name=region)
    try:
        instances = ec2_client.describe_instances(InstanceIds=instance_ids, Filters=instance_filters)
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidInstanceID.NotFound':
            log.warn('Cannot find one or more instance from IDs {0}'.format(instance_ids))
            return None
        else:
            raise
    ret = [reservation['Instances'][0] for reservation in instances['Reservations']]
    return ret


@register_autoscaling_component('aws_spot_fleet_request', CLUSTER_METRICS_PROVIDER_KEY)
def spotfleet_metrics_provider(spotfleet_request_id, resource, pool_settings):
    mesos_state = get_mesos_master().state
    sfr = get_sfr(spotfleet_request_id, region=resource['region'])
    if not sfr or not sfr['SpotFleetRequestState'] == 'active':
        log.error("Ignoring SFR {0} that does not exist or is not active.".format(spotfleet_request_id))
        return 0, 0
    sfr['ActiveInstances'] = get_spot_fleet_instances(spotfleet_request_id, region=resource['region'])
    resource['sfr'] = sfr
    desired_instances = len(sfr['ActiveInstances'])
    instance_ips = get_sfr_instance_ips(sfr, region=resource['region'])
    slaves = {
        slave['id']: slave for slave in mesos_state.get('slaves', [])
        if slave_pid_to_ip(slave['pid']) in instance_ips and
        slave['attributes'].get('pool', 'default') == resource['pool']
    }
    current_instances = len(slaves)
    log.info("Found %.2f%% slaves registered in mesos for this SFR (%d/%d)" % (
             float(float(current_instances) / float(desired_instances)) * 100, current_instances, desired_instances))
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
    target_utilization = pool_settings.get('target_utilization', DEFAULT_TARGET_UTILIZATION)
    error = utilization - target_utilization
    current, target = get_spot_fleet_delta(resource, error)
    return current, target


def get_spot_fleet_delta(resource, error):
    ec2_client = boto3.client('ec2', region_name=resource['region'])
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
            resource['min_capacity'],
            floor(current_capacity * (1.00 - MAX_CLUSTER_DELTA)),
            ideal_capacity,
            1,  # A SFR cannot scale below 1 instance
        ),
        ceil(current_capacity * (1.00 + MAX_CLUSTER_DELTA)),
        resource['max_capacity'],
    ))
    log.debug("The new capacity to scale to is %d instances" % ideal_capacity)

    if ideal_capacity > resource['max_capacity']:
        log.warning("Our ideal capacity (%d) is higher than max_capacity (%d). Consider rasing max_capacity!" % (
            ideal_capacity, resource['max_capacity']))
    if ideal_capacity < resource['min_capacity']:
        log.warning("Our ideal capacity (%d) is lower than min_capacity (%d). Consider lowering min_capacity!" % (
            ideal_capacity, resource['min_capacity']))
    if (ideal_capacity < floor(current_capacity * (1.00 - MAX_CLUSTER_DELTA)) or
            ideal_capacity > ceil(current_capacity * (1.00 + MAX_CLUSTER_DELTA))):
        log.warning(
            "Our ideal capacity (%d) is greater than %.2f%% of current %d. Just doing a %.2f%% change for now to %d." %
            (ideal_capacity, MAX_CLUSTER_DELTA * 100, current_capacity, MAX_CLUSTER_DELTA * 100, new_capacity))
    return current_capacity, new_capacity


def wait_and_terminate(slave, drain_timeout, dry_run, region=None):
    """Waits for slave to be drained and then terminate

    :param slave: dict of slave to kill
    :param dry_run: Don't drain or make changes to spot fleet if True"""
    ec2_client = boto3.client('ec2', region_name=region)
    try:
        # This loop should always finish because the maintenance window should trigger is_ready_to_kill
        # being true. Just in case though we set a timeout and terminate anyway
        with Timeout(seconds=drain_timeout + 300):
            while True:
                instance_id = slave['instance_id']
                if not instance_id:
                    log.warning("Didn't find instance ID for slave: {0}. Skipping terminating".format(slave['pid']))
                    continue
                # Check if no tasks are running or we have reached the maintenance window
                if is_safe_to_kill(slave['hostname']) or dry_run:
                    log.info("TERMINATING: {0} (Hostname = {1}, IP = {2})".format(
                        instance_id,
                        slave['hostname'],
                        slave['ip'],
                    ))
                    try:
                        ec2_client.terminate_instances(InstanceIds=[instance_id], DryRun=dry_run)
                    except ClientError as e:
                        if e.response['Error'].get('Code') == 'DryRunOperation':
                            pass
                        else:
                            raise
                    break
                else:
                    log.info("Instance {0}: NOT ready to kill".format(instance_id))
                log.debug("Waiting 5 seconds and then checking again")
                time.sleep(5)
    except TimeoutError:
        log.error("Timed out after {0} waiting to drain {1}, now terminating anyway".format(drain_timeout,
                                                                                            slave['pid']))
        try:
            ec2_client.terminate_instances(InstanceIds=instance_id, DryRun=dry_run)
        except ClientError as e:
            if e.response['Error'].get('Code') == 'DryRunOperation':
                pass
            else:
                raise


def sort_slaves_to_kill(slaves):
    """Pick the best slaves to kill. This returns a list of slaves
    after sorting in preference of which slaves we kill first.
    It sorts first by number of chronos tasks, then by total number of tasks

    :param slaves: list of slaves dict
    :returns: list of slaves dicts"""
    return sorted(slaves, key=lambda x: (x['task_counts'].chronos_count, x['task_counts'].count))


def get_instance_type_weights(sfr):
    launch_specifications = sfr['SpotFleetRequestConfig']['LaunchSpecifications']
    return {ls['InstanceType']: ls['WeightedCapacity'] for ls in launch_specifications}


def filter_sfr_slaves(slaves_list, resource):
    sfr = resource['sfr']
    sfr_ips = get_sfr_instance_ips(sfr, region=resource['region'])
    log.debug("IPs in SFR: {0}".format(sfr_ips))
    sfr_slaves = [slave for slave in slaves_list if slave_pid_to_ip(slave['task_counts'].slave['pid']) in sfr_ips]
    sfr_slave_ips = [slave_pid_to_ip(slave['task_counts'].slave['pid']) for slave in sfr_slaves]
    sfr_instance_descriptions = describe_instances([], region=resource['region'],
                                                   instance_filters=[{'Name': 'private-ip-address',
                                                                      'Values': sfr_slave_ips}])
    sfr_slave_instances = []
    for slave in sfr_slaves:
        ip = slave_pid_to_ip(slave['task_counts'].slave['pid'])
        instances = get_instances_from_ip(ip, sfr_instance_descriptions)
        if not instances:
            log.warning("Couldn't find instance for ip {0}".format(ip))
            continue
        if len(instances) > 1:
            log.error("Found more than one instance with the same private IP {0}. "
                      "This should never happen")
            continue
        sfr_slave_instances.append({'ip': ip,
                                    'task_counts': slave['task_counts'],
                                    'hostname': slave['task_counts'].slave['hostname'],
                                    'id': slave['task_counts'].slave['id'],
                                    'pid': slave['task_counts'].slave['pid'],
                                    'instance_id': instances[0]['InstanceId']})
    ret = []
    instance_type_weights = get_instance_type_weights(sfr)
    for slave in sfr_slave_instances:
        instance_description = [instance_description for instance_description in sfr_instance_descriptions
                                if instance_description['InstanceId'] == slave['instance_id']][0]
        slave['instance_type'] = instance_description['InstanceType']
        slave['instance_weight'] = instance_type_weights[slave['instance_type']]
        ret.append(slave)
    return ret


def set_spot_fleet_request_capacity(sfr_id, capacity, dry_run, region=None):
    """ AWS won't modify a request that is already modifying. This
    function ensures we wait a few seconds in case we've just modified
    a SFR"""
    ec2_client = boto3.client('ec2', region_name=region)
    with Timeout(seconds=AWS_SPOT_MODIFY_TIMEOUT):
        try:
            state = None
            while True:
                state = get_sfr(sfr_id, region=region)['SpotFleetRequestState']
                if state == 'active':
                    break
                log.debug("SFR {0} in state {1}, waiting for state: active".format(sfr_id, state))
                log.debug("Sleep 5 seconds")
                time.sleep(5)
        except TimeoutError:
            log.error("Spot fleet {0} not in active state so we can't modify it.".format(sfr_id))
            raise FailSetSpotCapacity
    if dry_run:
        return True
    try:
        ret = ec2_client.modify_spot_fleet_request(SpotFleetRequestId=sfr_id, TargetCapacity=capacity,
                                                   ExcessCapacityTerminationPolicy='noTermination')
    except ClientError as e:
        log.error("Error modifying spot fleet request: {0}".format(e))
        raise FailSetSpotCapacity
    return ret


@register_autoscaling_component('aws_spot_fleet_request', SCALER_KEY)
def scale_aws_spot_fleet_request(resource, current_capacity, target_capacity, pool_settings, dry_run):
    """Scales a spot fleet request by delta to reach target capacity
    If scaling up we just set target capacity and let AWS take care of the rest
    If scaling down we pick the slaves we'd prefer to kill, put them in maintenance
    mode and drain them (via paasta_maintenance and setup_marathon_jobs). We then kill
    them once they are running 0 tasks or once a timeout is reached

    :param resource: resource to scale
    :param current_capacity: integer current SFR capacity
    :param target_capacity: target SFR capacity
    :param pool_settings: pool settings dict with timeout settings
    :param dry_run: Don't drain or make changes to spot fleet if True"""
    target_capacity = int(target_capacity)
    current_capacity = int(current_capacity)
    delta = target_capacity - current_capacity
    sfr_id = resource['id']
    if delta == 0:
        log.info("Already at target capacity: {0}".format(target_capacity))
        return
    elif delta > 0:
        log.info("Increasing spot fleet capacity to: {0}".format(target_capacity))
        set_spot_fleet_request_capacity(sfr_id, target_capacity, dry_run, region=resource['region'])
        return
    elif delta < 0:
        mesos_state = get_mesos_master().state_summary()
        slaves_list = get_mesos_task_count_by_slave(mesos_state, pool=resource['pool'])
        filtered_slaves = filter_sfr_slaves(slaves_list, resource)
        killable_capacity = sum([slave['instance_weight'] for slave in filtered_slaves])
        amount_to_decrease = delta * -1
        if amount_to_decrease > killable_capacity:
            log.error("Didn't find enough candidates to kill. This shouldn't happen so let's not kill anything!")
            return
        downscale_spot_fleet_request(resource=resource,
                                     filtered_slaves=filtered_slaves,
                                     current_capacity=current_capacity,
                                     target_capacity=target_capacity,
                                     pool_settings=pool_settings,
                                     dry_run=dry_run)


def gracefully_terminate_slave(resource, slave_to_kill, pool_settings, current_capacity, new_capacity, dry_run):
    sfr_id = resource['id']
    drain_timeout = pool_settings.get('drain_timeout', DEFAULT_DRAIN_TIMEOUT)
    # The start time of the maintenance window is the point at which
    # we giveup waiting for the instance to drain and mark it for termination anyway
    start = int(time.time() + drain_timeout) * 1000000000  # nanoseconds
    # Set the duration to an hour, this is fairly arbitrary as mesos doesn't actually
    # do anything at the end of the maintenance window.
    duration = 600 * 1000000000  # nanoseconds
    log.info("Draining {0}".format(slave_to_kill['pid']))
    if not dry_run:
        try:
            drain_host_string = "{0}|{1}".format(slave_to_kill['hostname'], slave_to_kill['ip'])
            drain([drain_host_string], start, duration)
        except HTTPError as e:
            log.error("Failed to start drain "
                      "on {0}: {1}\n Trying next host".format(slave_to_kill['hostname'], e))
            raise
    log.info("Decreasing spot fleet capacity from {0} to: {1}".format(current_capacity, new_capacity))
    # Instance weights can be floats but the target has to be an integer
    # because this is all AWS allows on the API call to set target capacity
    new_capacity = int(floor(new_capacity))
    try:
        set_spot_fleet_request_capacity(sfr_id, new_capacity, dry_run, region=resource['region'])
    except FailSetSpotCapacity:
        log.error("Couldn't update spot fleet, stopping autoscaler")
        log.info("Undraining {0}".format(slave_to_kill['pid']))
        if not dry_run:
            undrain([drain_host_string])
        raise
    log.info("Waiting for instance to drain before we terminate")
    try:
        wait_and_terminate(slave_to_kill, drain_timeout, dry_run, region=resource['region'])
    except ClientError as e:
        log.error("Failure when terminating: {0}: {1}".format(slave_to_kill['pid'], e))
        log.error("Setting spot fleet capacity back to {0}".format(current_capacity))
        set_spot_fleet_request_capacity(sfr_id, current_capacity, dry_run, region=resource['region'])
    finally:
        log.info("Undraining {0}".format(slave_to_kill['pid']))
        if not dry_run:
            undrain([drain_host_string])


def downscale_spot_fleet_request(resource, filtered_slaves, current_capacity, target_capacity, pool_settings, dry_run):
    while True:
        filtered_sorted_slaves = sort_slaves_to_kill(filtered_slaves)
        if len(filtered_sorted_slaves) == 0:
            break
        log.info("SFR slave kill preference: {0}".format([slave['hostname'] for slave in filtered_sorted_slaves]))
        filtered_sorted_slaves.reverse()
        slave_to_kill = filtered_sorted_slaves.pop()
        instance_capacity = slave_to_kill['instance_weight']
        new_capacity = current_capacity - instance_capacity
        if new_capacity < target_capacity:
            log.info("Terminating instance {0} with weight {1} would take us below our target of {2}, so this is as"
                     " close to our target as we can get".format(slave_to_kill['instance_id'],
                                                                 slave_to_kill['instance_weight'],
                                                                 target_capacity))
            break
        try:
            gracefully_terminate_slave(resource=resource,
                                       slave_to_kill=slave_to_kill,
                                       pool_settings=pool_settings,
                                       current_capacity=current_capacity,
                                       new_capacity=new_capacity,
                                       dry_run=dry_run)
        except HTTPError:
            # Something wrong draining host so try next host
            continue
        except FailSetSpotCapacity:
            break
        current_capacity = new_capacity
        mesos_state = get_mesos_master().state_summary()
        filtered_slaves = get_mesos_task_count_by_slave(mesos_state, slaves_list=filtered_sorted_slaves)


class ClusterAutoscalingError(Exception):
    pass


class FailSetSpotCapacity(Exception):
    pass


def get_instances_from_ip(ip, instance_descriptions):
    """Filter AWS instance_descriptions based on PrivateIpAddress

    :param ip: private IP of AWS instance.
    :param instance_descriptions: list of AWS instance description dicts.
    :returns: list of instance description dicts"""
    instances = [instance for instance in instance_descriptions if instance['PrivateIpAddress'] == ip]
    return instances


def autoscale_local_cluster(dry_run=False):
    if dry_run:
        log.info("Running in dry_run mode, no changes should be made")
    system_config = load_system_paasta_config()
    autoscaling_resources = system_config.get_cluster_autoscaling_resources()
    all_pool_settings = system_config.get_resource_pool_settings()
    for identifier, resource in autoscaling_resources.items():
        pool_settings = all_pool_settings.get(resource['pool'], {})
        log.info("Autoscaling {0} in pool, {1}".format(identifier, resource['pool']))
        resource_metrics_provider = get_cluster_metrics_provider(resource['type'])
        try:
            current, target = resource_metrics_provider(resource['id'], resource, pool_settings)
            log.info("Target capacity: {0}, Capacity current: {1}".format(target, current))
            resource_scaler = get_scaler(resource['type'])
            resource_scaler(resource, current, target, pool_settings, dry_run)
        except ClusterAutoscalingError as e:
            log.error('%s: %s' % (identifier, e))
