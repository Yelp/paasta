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

import logging
import os
import time
from collections import namedtuple
from math import ceil
from math import floor

import boto3
from botocore.exceptions import ClientError
from requests.exceptions import HTTPError

from paasta_tools.autoscaling import ec2_fitness
from paasta_tools.mesos_maintenance import drain
from paasta_tools.mesos_maintenance import undrain
from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.mesos_tools import get_mesos_task_count_by_slave
from paasta_tools.mesos_tools import slave_pid_to_ip
from paasta_tools.metrics.metastatus_lib import get_resource_utilization_by_grouping
from paasta_tools.paasta_maintenance import is_safe_to_kill
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import Timeout
from paasta_tools.utils import TimeoutError


AutoscalingInfo = namedtuple("AutoscalingInfo",
                             ["resource_id",
                              "pool",
                              "state",
                              "current",
                              "target",
                              "min_capacity",
                              "max_capacity",
                              "instances"])
CLUSTER_METRICS_PROVIDER_KEY = 'cluster_metrics_provider'
DEFAULT_TARGET_UTILIZATION = 0.8  # decimal fraction
DEFAULT_DRAIN_TIMEOUT = 600  # seconds

AWS_SPOT_MODIFY_TIMEOUT = 30
MISSING_SLAVE_PANIC_THRESHOLD = .3
MAX_CLUSTER_DELTA = .2

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class ResourceLogMixin(object):

    @property
    def log(self):
        resource_id = self.resource.get("id", "unknown")
        name = '.'.join([__name__, self.__class__.__name__, resource_id])
        return logging.getLogger(name)


class ClusterAutoscaler(ResourceLogMixin):

    def __init__(self, resource, pool_settings, config_folder, dry_run, log_level=None):
        self.resource = resource
        self.pool_settings = pool_settings
        self.config_folder = config_folder
        self.dry_run = dry_run
        if log_level is not None:
            self.log.setLevel(log_level)

    def set_capacity(self, capacity):
        pass

    def get_instance_type_weights(self):
        pass

    def describe_instances(self, instance_ids, region=None, instance_filters=None):
        """This wraps ec2.describe_instances and catches instance not
        found errors. It returns a list of instance description
        dictionaries.  Optionally, a filter can be passed through to
        the ec2 call

        :param instance_ids: a list of instance ids, [] means all
        :param instance_filters: a list of ec2 filters
        :param region to connect to ec2
        :returns: a list of instance description dictionaries"""
        if not instance_filters:
            instance_filters = []
        ec2_client = boto3.client('ec2', region_name=region)
        try:
            instance_descriptions = ec2_client.describe_instances(InstanceIds=instance_ids, Filters=instance_filters)
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidInstanceID.NotFound':
                self.log.warn('Cannot find one or more instance from IDs {}'.format(instance_ids))
                return None
            else:
                raise
        instance_reservations = [reservation['Instances'] for reservation in instance_descriptions['Reservations']]
        instances = [instance for reservation in instance_reservations for instance in reservation]
        return instances

    def describe_instance_status(self, instance_ids, region=None, instance_filters=None):
        """This wraps ec2.describe_instance_status and catches instance not
        found errors. It returns a list of instance description
        dictionaries.  Optionally, a filter can be passed through to
        the ec2 call

        :param instance_ids: a list of instance ids, [] means all
        :param instance_filters: a list of ec2 filters
        :param region to connect to ec2
        :returns: a list of instance description dictionaries"""
        if not instance_filters:
            instance_filters = []
        ec2_client = boto3.client('ec2', region_name=region)
        try:
            instance_descriptions = ec2_client.describe_instance_status(
                InstanceIds=instance_ids,
                Filters=instance_filters
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidInstanceID.NotFound':
                self.log.warn('Cannot find one or more instance from IDs {}'.format(instance_ids))
                return None
            else:
                raise
        return instance_descriptions

    def get_instance_ips(self, instances, region=None):
        instance_descriptions = self.describe_instances([instance['InstanceId'] for instance in instances],
                                                        region=region)
        instance_ips = []
        for instance in instance_descriptions:
            try:
                instance_ips.append(instance['PrivateIpAddress'])
            except KeyError:
                self.log.warning("Instance {} does not have an IP. This normally means it has been"
                                 " terminated".format(instance['InstanceId']))
        return instance_ips

    def cleanup_cancelled_config(self, resource_id, config_folder, dry_run=False):
        file_name = "{}.json".format(resource_id)
        configs_to_delete = [os.path.join(walk[0], file_name)
                             for walk in os.walk(config_folder) if file_name in walk[2]]
        if not configs_to_delete:
            self.log.info("Resource config for {} not found".format(resource_id))
            return
        if not dry_run:
            os.remove(configs_to_delete[0])
        self.log.info("Deleted Resource config {}".format(configs_to_delete[0]))

    def get_aws_slaves(self, mesos_state):
        instance_ips = self.get_instance_ips(self.instances, region=self.resource['region'])
        slaves = {
            slave['id']: slave for slave in mesos_state.get('slaves', [])
            if slave_pid_to_ip(slave['pid']) in instance_ips and
            slave['attributes'].get('pool', 'default') == self.resource['pool']
        }
        return slaves

    def get_pool_slaves(self, mesos_state):
        slaves = {
            slave['id']: slave for slave in mesos_state.get('slaves', [])
            if slave['attributes'].get('pool', 'default') == self.resource['pool']
        }
        return slaves

    def get_mesos_utilization_error(self,
                                    slaves,
                                    mesos_state,
                                    expected_instances=None):
        current_instances = len(slaves)
        if current_instances == 0:
            error_message = ("No instances are active, not scaling until the instances are attached to mesos")
            raise ClusterAutoscalingError(error_message)
        if expected_instances:
            self.log.info("Found %.2f%% slaves registered in mesos for this resource (%d/%d)" % (
                float(float(current_instances) / float(expected_instances)) * 100,
                current_instances,
                expected_instances))
            if float(current_instances) / expected_instances < (1.00 - MISSING_SLAVE_PANIC_THRESHOLD):
                error_message = ("We currently have %d instances active in mesos out of a desired %d.\n"
                                 "Refusing to scale because we either need to wait for the requests to be "
                                 "filled, or the new instances are not healthy for some reason.\n"
                                 "(cowardly refusing to go past %.2f%% missing instances)") % (
                    current_instances, expected_instances, MISSING_SLAVE_PANIC_THRESHOLD)
                raise ClusterAutoscalingError(error_message)

        region_pool_utilization_dict = get_resource_utilization_by_grouping(
            lambda slave: (slave['attributes']['pool'], slave['attributes']['datacenter'],),
            mesos_state
        )[(self.resource['pool'], self.resource['region'],)]

        self.log.debug(region_pool_utilization_dict)
        free_pool_resources = region_pool_utilization_dict['free']
        total_pool_resources = region_pool_utilization_dict['total']
        utilization = 1.0 - min([
            float(float(pair[0]) / float(pair[1]))
            for pair in zip(free_pool_resources, total_pool_resources)
        ])
        target_utilization = self.pool_settings.get('target_utilization', DEFAULT_TARGET_UTILIZATION)
        return utilization - target_utilization

    def wait_and_terminate(self, slave, drain_timeout, dry_run, region=None):
        """Waits for slave to be drained and then terminate

        :param slave: dict of slave to kill
        :param drain_timeout: how long to wait before terminating
            even if not drained
        :param region to connect to ec2
        :param dry_run: Don't drain or make changes to spot fleet if True"""
        ec2_client = boto3.client('ec2', region_name=region)
        try:
            # This loop should always finish because the maintenance window should trigger is_ready_to_kill
            # being true. Just in case though we set a timeout and terminate anyway
            with Timeout(seconds=drain_timeout + 300):
                while True:
                    instance_id = slave.instance_id
                    if not instance_id:
                        self.log.warning(
                            "Didn't find instance ID for slave: {}. Skipping terminating".format(slave.pid)
                        )
                        continue
                    # Check if no tasks are running or we have reached the maintenance window
                    if is_safe_to_kill(slave.hostname) or dry_run:
                        self.log.info("TERMINATING: {} (Hostname = {}, IP = {})".format(
                            instance_id,
                            slave.hostname,
                            slave.ip,
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
                        self.log.info("Instance {}: NOT ready to kill".format(instance_id))
                    self.log.debug("Waiting 5 seconds and then checking again")
                    time.sleep(5)
        except TimeoutError:
            self.log.error("Timed out after {} waiting to drain {}, now terminating anyway".format(drain_timeout,
                                                                                                   slave.pid))
            try:
                ec2_client.terminate_instances(InstanceIds=instance_id, DryRun=dry_run)
            except ClientError as e:
                if e.response['Error'].get('Code') == 'DryRunOperation':
                    pass
                else:
                    raise

    def scale_resource(self, current_capacity, target_capacity):
        """Scales an AWS resource based on current and target capacity
        If scaling up we just set target capacity and let AWS take care of the rest
        If scaling down we pick the slaves we'd prefer to kill, put them in maintenance
        mode and drain them (via paasta_maintenance and setup_marathon_jobs). We then kill
        them once they are running 0 tasks or once a timeout is reached

        :param current_capacity: integer current resource capacity
        :param target_capacity: target resource capacity
        """
        target_capacity = int(target_capacity)
        delta = target_capacity - current_capacity
        if delta == 0:
            self.log.info("Already at target capacity: {}".format(target_capacity))
            return
        elif delta > 0:
            self.log.info("Increasing resource capacity to: {}".format(target_capacity))
            self.set_capacity(target_capacity)
            return
        elif delta < 0:
            mesos_state = get_mesos_master().state_summary()
            slaves_list = get_mesos_task_count_by_slave(mesos_state, pool=self.resource['pool'])
            filtered_slaves = self.filter_aws_slaves(slaves_list)
            killable_capacity = sum([slave.instance_weight for slave in filtered_slaves])
            amount_to_decrease = delta * -1
            if amount_to_decrease > killable_capacity:
                self.log.error(
                    "Didn't find enough candidates to kill. This shouldn't happen so let's not kill anything!"
                )
                return
            self.downscale_aws_resource(
                filtered_slaves=filtered_slaves,
                current_capacity=current_capacity,
                target_capacity=target_capacity
            )

    def gracefully_terminate_slave(self, slave_to_kill, current_capacity, new_capacity):
        drain_timeout = self.pool_settings.get('drain_timeout', DEFAULT_DRAIN_TIMEOUT)
        # The start time of the maintenance window is the point at which
        # we giveup waiting for the instance to drain and mark it for termination anyway
        start = int(time.time() + drain_timeout) * 1000000000  # nanoseconds
        # Set the duration to an hour, this is fairly arbitrary as mesos doesn't actually
        # do anything at the end of the maintenance window.
        duration = 600 * 1000000000  # nanoseconds
        self.log.info("Draining {}".format(slave_to_kill.pid))
        if not self.dry_run:
            try:
                drain_host_string = "{}|{}".format(slave_to_kill.hostname, slave_to_kill.ip)
                drain([drain_host_string], start, duration)
            except HTTPError as e:
                self.log.error("Failed to start drain "
                               "on {}: {}\n Trying next host".format(slave_to_kill.hostname, e))
                raise
        self.log.info("Decreasing resource from {} to: {}".format(current_capacity, new_capacity))
        # Instance weights can be floats but the target has to be an integer
        # because this is all AWS allows on the API call to set target capacity
        new_capacity = int(floor(new_capacity))
        try:
            self.set_capacity(new_capacity)
        except FailSetResourceCapacity:
            self.log.error("Couldn't update resource capacity, stopping autoscaler")
            self.log.info("Undraining {}".format(slave_to_kill.pid))
            if not self.dry_run:
                undrain([drain_host_string])
            raise
        self.log.info("Waiting for instance to drain before we terminate")
        try:
            self.wait_and_terminate(slave_to_kill, drain_timeout, self.dry_run, region=self.resource['region'])
        except ClientError as e:
            self.log.error("Failure when terminating: {}: {}".format(slave_to_kill.pid, e))
            self.log.error("Setting resource capacity back to {}".format(current_capacity))
            self.set_capacity(current_capacity)
            self.log.info("Undraining {}".format(slave_to_kill.pid))
            if not self.dry_run:
                undrain([drain_host_string])

    def filter_aws_slaves(self, slaves_list):
        ips = self.get_instance_ips(self.instances, region=self.resource['region'])
        self.log.debug("IPs in AWS resources: {}".format(ips))
        slaves = [
            slave for slave in slaves_list
            if slave_pid_to_ip(slave['task_counts'].slave['pid']) in ips
        ]
        slave_ips = [slave_pid_to_ip(slave['task_counts'].slave['pid']) for slave in slaves]
        instance_type_weights = self.get_instance_type_weights()
        instance_statuses = self.instance_status_for_instance_ids(
            instance_ids=[instance['InstanceId'] for instance in self.instances]
        )
        instance_descriptions = self.instance_descriptions_for_ips(slave_ips)

        paasta_aws_slaves = []
        for slave in slaves:
            slave_ip = slave_pid_to_ip(slave['task_counts'].slave['pid'])
            matching_descriptions = self.filter_instance_description_for_ip(slave_ip, instance_descriptions)
            if matching_descriptions:
                assert len(matching_descriptions) == 1, (
                    "There should be only one instance with the same IP."
                    "Found instances %s with the same ip %d"
                    % (",".join(
                        [x['InstanceId'] for x in matching_descriptions]),
                        slave_ip
                       )
                )
                description = matching_descriptions[0]
                matching_status = self.filter_instance_status_for_instance_id(
                    instance_id=description['InstanceId'],
                    instance_statuses=instance_statuses
                )
                assert len(matching_status) == 1, "There should be only one InstanceStatus per instance"
            else:
                description = None

            paasta_aws_slaves.append(PaastaAwsSlave(
                slave=slave,
                instance_status=matching_status[0],
                instance_description=description,
                instance_type_weights=instance_type_weights
            ))

        return paasta_aws_slaves

    def instance_status_for_instance_ids(self, instance_ids):
        """
        Return a list of instance statuses. Batch the API calls into
        groups of 99, since AWS limit it.
        """
        partitions = [
            instance_ids[partition: partition + 99]
            for partition in
            range(0, len(instance_ids), 99)
        ]
        accumulated = {'InstanceStatuses': []}
        for subgroup in partitions:
            res = self.describe_instance_status(
                instance_ids=subgroup,
                region=self.resource['region']
            )
            accumulated['InstanceStatuses'] += res['InstanceStatuses']

        return accumulated

    def instance_descriptions_for_ips(self, ips):
        return self.describe_instances(
            instance_ids=[],
            region=self.resource['region'],
            instance_filters=[
                {
                    'Name': 'private-ip-address',
                    'Values': ips
                }
            ]
        )

    def filter_instance_description_for_ip(self, ip, instance_descriptions):
        return [
            i for i in instance_descriptions
            if ip == i['PrivateIpAddress']
        ]

    def filter_instance_status_for_instance_id(self, instance_id, instance_statuses):
        return [
            status for status in instance_statuses['InstanceStatuses']
            if status['InstanceId'] == instance_id
        ]

    def downscale_aws_resource(self, filtered_slaves, current_capacity, target_capacity):
        killed_slaves = 0
        while True:
            filtered_sorted_slaves = ec2_fitness.sort_by_ec2_fitness(filtered_slaves)[::-1]
            if len(filtered_sorted_slaves) == 0:
                self.log.info("ALL slaves killed so moving on to next resource!")
                break
            self.log.info("Resource slave kill preference: {}".format([slave.hostname
                                                                       for slave in filtered_sorted_slaves]))
            slave_to_kill = filtered_sorted_slaves.pop(0)
            instance_capacity = slave_to_kill.instance_weight
            new_capacity = current_capacity - instance_capacity
            if new_capacity < target_capacity:
                self.log.info("Terminating instance {} with weight {} would take us below our target of {},"
                              " so this is as close to our target as we can get".format(
                                  slave_to_kill.instance_id,
                                  slave_to_kill.instance_weight,
                                  target_capacity
                              ))
                if self.resource['type'] == 'aws_spot_fleet_request' and killed_slaves == 0:
                    self.log.info("This is a SFR so we must kill at least one slave to prevent the autoscaler "
                                  "getting stuck whilst scaling down gradually")
                else:
                    break
            try:
                self.gracefully_terminate_slave(
                    slave_to_kill=slave_to_kill,
                    current_capacity=current_capacity,
                    new_capacity=new_capacity
                )
                killed_slaves += 1
            except HTTPError:
                # Something wrong draining host so try next host
                continue
            except FailSetResourceCapacity:
                break

            current_capacity = new_capacity
            mesos_state = get_mesos_master().state_summary()
            if filtered_sorted_slaves:
                task_counts = get_mesos_task_count_by_slave(
                    mesos_state,
                    slaves_list=[
                        {'task_counts': slave.task_counts}
                        for slave in filtered_sorted_slaves
                    ]
                )
                for i, slave in enumerate(filtered_sorted_slaves):
                    slave.task_counts = task_counts[i]['task_counts']
            filtered_slaves = filtered_sorted_slaves


class SpotAutoscaler(ClusterAutoscaler):

    def __init__(self, *args, **kwargs):
        super(SpotAutoscaler, self).__init__(*args, **kwargs)
        self.sfr = self.get_sfr(self.resource['id'], region=self.resource['region'])
        if self.sfr:
            self.instances = self.get_spot_fleet_instances(self.resource['id'], region=self.resource['region'])

    @property
    def exists(self):
        return False if not self.sfr or self.sfr['SpotFleetRequestState'] == 'cancelled' else True

    def get_sfr(self, spotfleet_request_id, region=None):
        ec2_client = boto3.client('ec2', region_name=region)
        try:
            sfrs = ec2_client.describe_spot_fleet_requests(SpotFleetRequestIds=[spotfleet_request_id])
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidSpotFleetRequestId.NotFound':
                self.log.warn('Cannot find SFR {}'.format(spotfleet_request_id))
                return None
            else:
                raise
        ret = sfrs['SpotFleetRequestConfigs'][0]
        return ret

    def get_spot_fleet_instances(self, spotfleet_request_id, region=None):
        ec2_client = boto3.client('ec2', region_name=region)
        spot_fleet_instances = ec2_client.describe_spot_fleet_instances(
            SpotFleetRequestId=spotfleet_request_id)['ActiveInstances']
        return spot_fleet_instances

    def metrics_provider(self):
        if not self.sfr or self.sfr['SpotFleetRequestState'] == 'cancelled':
            self.log.error("SFR not found, removing config file.".format(self.resource['id']))
            self.cleanup_cancelled_config(self.resource['id'], self.config_folder, dry_run=self.dry_run)
            return 0, 0
        elif self.sfr['SpotFleetRequestState'] in ['cancelled_running', 'active']:
            expected_instances = len(self.instances)
            if expected_instances == 0:
                self.log.warning("No instances found in SFR, this shouldn't be possible so we "
                                 "do nothing")
                return 0, 0
            mesos_state = get_mesos_master().state
            slaves = self.get_aws_slaves(mesos_state)
            error = self.get_mesos_utilization_error(
                slaves=slaves,
                mesos_state=mesos_state,
                expected_instances=expected_instances)
        elif self.sfr['SpotFleetRequestState'] in ['submitted', 'modifying', 'cancelled_terminating']:
            self.log.warning("Not scaling an SFR in state: {} so {}, skipping...".format(
                self.sfr['SpotFleetRequestState'],
                self.resource['id'])
            )
            return 0, 0
        else:
            self.log.error("Unexpected SFR state: {} for {}".format(self.sfr['SpotFleetRequestState'],
                                                                    self.resource['id']))
            raise ClusterAutoscalingError
        if self.is_aws_launching_instances() and self.sfr['SpotFleetRequestState'] == 'active':
            self.log.warning("AWS hasn't reached the TargetCapacity that is currently set. We won't make any "
                             "changes this time as we should wait for AWS to launch more instances first.")
            return 0, 0
        current, target = self.get_spot_fleet_delta(error)
        if self.sfr['SpotFleetRequestState'] == 'cancelled_running':
            self.resource['min_capacity'] = 0
            slaves = self.get_pool_slaves(mesos_state)
            pool_error = self.get_mesos_utilization_error(
                slaves=slaves,
                mesos_state=mesos_state)
            if pool_error > 0:
                self.log.info(
                    "Not scaling cancelled SFR %s because we are under provisioned" % (self.resource['id'])
                )
                return 0, 0
            current, target = self.get_spot_fleet_delta(-1)
            if target == 1:
                target = 0
        return current, target

    def is_aws_launching_instances(self):
        fulfilled_capacity = self.sfr['SpotFleetRequestConfig']['FulfilledCapacity']
        target_capacity = self.sfr['SpotFleetRequestConfig']['TargetCapacity']
        return target_capacity > fulfilled_capacity

    @property
    def current_capacity(self):
        return float(self.sfr['SpotFleetRequestConfig']['FulfilledCapacity'])

    def get_spot_fleet_delta(self, error):
        current_capacity = self.current_capacity
        ideal_capacity = int(ceil((1 + error) * current_capacity))
        self.log.debug("Ideal calculated capacity is %d instances" % ideal_capacity)
        new_capacity = int(min(
            max(
                self.resource['min_capacity'],
                floor(current_capacity * (1.00 - MAX_CLUSTER_DELTA)),
                ideal_capacity,
                1,  # A SFR cannot scale below 1 instance
            ),
            ceil(current_capacity * (1.00 + MAX_CLUSTER_DELTA)),
            self.resource['max_capacity'],
        ))
        new_capacity = max(new_capacity, self.resource['min_capacity'])
        self.log.debug("The ideal capacity to scale to is %d instances" % ideal_capacity)
        self.log.debug("The capacity we will scale to is %d instances" % new_capacity)
        if ideal_capacity > self.resource['max_capacity']:
            self.log.warning(
                "Our ideal capacity (%d) is higher than max_capacity (%d). Consider raising max_capacity!" % (
                    ideal_capacity, self.resource['max_capacity']
                )
            )
        if ideal_capacity < self.resource['min_capacity']:
            self.log.warning(
                "Our ideal capacity (%d) is lower than min_capacity (%d). Consider lowering min_capacity!" % (
                    ideal_capacity, self.resource['min_capacity']
                )
            )
        return current_capacity, new_capacity

    def set_capacity(self, capacity):
        """ AWS won't modify a request that is already modifying. This
        function ensures we wait a few seconds in case we've just modified
        a SFR"""
        ec2_client = boto3.client('ec2', region_name=self.resource['region'])
        with Timeout(seconds=AWS_SPOT_MODIFY_TIMEOUT):
            try:
                state = None
                while True:
                    state = self.get_sfr(self.resource['id'], region=self.resource['region'])['SpotFleetRequestState']
                    if state == 'active':
                        break
                    if state == 'cancelled_running':
                        self.log.info("Not updating target capacity because this is a cancelled SFR, "
                                      "we are just draining and killing the instances")
                        return
                    self.log.debug("SFR {} in state {}, waiting for state: active".format(self.resource['id'], state))
                    self.log.debug("Sleep 5 seconds")
                    time.sleep(5)
            except TimeoutError:
                self.log.error("Spot fleet {} not in active state so we can't modify it.".format(self.resource['id']))
                raise FailSetResourceCapacity
        if self.dry_run:
            return True
        try:
            ret = ec2_client.modify_spot_fleet_request(SpotFleetRequestId=self.resource['id'], TargetCapacity=capacity,
                                                       ExcessCapacityTerminationPolicy='noTermination')
        except ClientError as e:
            self.log.error("Error modifying spot fleet request: {}".format(e))
            raise FailSetResourceCapacity
        return ret

    def is_resource_cancelled(self):
        if not self.sfr:
            return True
        state = self.sfr['SpotFleetRequestState']
        if state in ['cancelled', 'cancelled_running']:
            return True
        return False

    def get_instance_type_weights(self):
        launch_specifications = self.sfr['SpotFleetRequestConfig']['LaunchSpecifications']
        return {ls['InstanceType']: ls['WeightedCapacity'] for ls in launch_specifications}


class AsgAutoscaler(ClusterAutoscaler):

    def __init__(self, *args, **kwargs):
        super(AsgAutoscaler, self).__init__(*args, **kwargs)
        self.asg = self.get_asg(self.resource['id'], region=self.resource['region'])
        if self.asg:
            self.instances = self.asg['Instances']

    @property
    def exists(self):
        return True if self.asg else False

    def get_asg(self, asg_name, region=None):
        asg_client = boto3.client('autoscaling', region_name=region)
        asgs = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
        try:
            return asgs['AutoScalingGroups'][0]
        except IndexError:
            self.log.warning("No ASG found in this account with name: {}".format(asg_name))
            return None

    def metrics_provider(self):
        if not self.asg:
            self.log.warning("ASG {} not found, removing config file".format(self.resource['id']))
            self.cleanup_cancelled_config(self.resource['id'], self.config_folder, dry_run=self.dry_run)
            return 0, 0
        if self.is_aws_launching_instances():
            self.log.warning("ASG still launching new instances so we won't make any"
                             "changes this time.")
            return 0, 0
        expected_instances = len(self.instances)
        if expected_instances == 0:
            self.log.warning("This ASG has no instances, delta should be 1 to "
                             "launch first instance unless max/min capacity override")
            return self.get_asg_delta(1)
        mesos_state = get_mesos_master().state
        slaves = self.get_aws_slaves(mesos_state)
        error = self.get_mesos_utilization_error(
            slaves=slaves,
            mesos_state=mesos_state,
            expected_instances=expected_instances)
        return self.get_asg_delta(error)

    def is_aws_launching_instances(self):
        fulfilled_capacity = len(self.asg['Instances'])
        target_capacity = self.asg['DesiredCapacity']
        return target_capacity > fulfilled_capacity

    @property
    def current_capacity(self):
        return len(self.asg['Instances'])

    def get_asg_delta(self, error):
        current_capacity = self.current_capacity
        if current_capacity == 0:
            new_capacity = int(min(
                max(1, self.resource['min_capacity']),
                self.resource['max_capacity']))
            ideal_capacity = new_capacity
        else:
            ideal_capacity = int(ceil((1 + error) * current_capacity))
            new_capacity = int(min(
                max(
                    self.resource['min_capacity'],
                    floor(current_capacity * (1.00 - MAX_CLUSTER_DELTA)),
                    ideal_capacity,
                    0,
                ),
                ceil(current_capacity * (1.00 + MAX_CLUSTER_DELTA)),
                # if max and min set to 0 we still drain gradually
                max(self.resource['max_capacity'], floor(current_capacity * (1.00 - MAX_CLUSTER_DELTA)))
            ))
        new_capacity = max(new_capacity, self.resource['min_capacity'])
        self.log.debug("The ideal capacity to scale to is %d instances" % ideal_capacity)
        self.log.debug("The capacity we will scale to is %d instances" % new_capacity)
        if ideal_capacity > self.resource['max_capacity']:
            self.log.warning(
                "Our ideal capacity (%d) is higher than max_capacity (%d). Consider raising max_capacity!" % (
                    ideal_capacity, self.resource['max_capacity']
                ))
        if ideal_capacity < self.resource['min_capacity']:
            self.log.warning(
                "Our ideal capacity (%d) is lower than min_capacity (%d). Consider lowering min_capacity!" % (
                    ideal_capacity, self.resource['min_capacity']
                )
            )
        return current_capacity, new_capacity

    def set_capacity(self, capacity):
        if self.dry_run:
            return
        asg_client = boto3.client('autoscaling', region_name=self.resource['region'])
        try:
            ret = asg_client.update_auto_scaling_group(AutoScalingGroupName=self.resource['id'],
                                                       DesiredCapacity=capacity)
        except ClientError as e:
            self.log.error("Error modifying ASG: {}".format(e))
            raise FailSetResourceCapacity
        return ret

    def is_resource_cancelled(self):
        return not self.asg

    def get_instance_type_weights(self):
        # None means that all instances will be
        # assumed to have a weight of 1
        return None


class ClusterAutoscalingError(Exception):
    pass


class FailSetResourceCapacity(Exception):
    pass


class PaastaAwsSlave(object):
    """
    Defines a slave object for use by the autoscaler, containing the mesos slave
    object from mesos state and some properties from AWS
    """

    def __init__(self, slave, instance_description, instance_status=None, instance_type_weights=None):
        if instance_status is None:
            self.instance_status = {}
        else:
            self.instance_status = instance_status
        self.wrapped_slave = slave
        self.instance_description = instance_description
        self.instance_type_weights = instance_type_weights
        self.task_counts = slave['task_counts']
        self.slave = self.task_counts.slave
        self.ip = slave_pid_to_ip(self.slave['pid'])

    @property
    def instance(self):
        return self.instance_description

    @property
    def instance_id(self):
        return self.instance['InstanceId']

    @property
    def hostname(self):
        return self.slave['hostname']

    @property
    def pid(self):
        return self.slave['pid']

    @property
    def instance_type(self):
        return self.instance_description['InstanceType']

    @property
    def instance_weight(self):
        if self.instance_type_weights:
            return self.instance_type_weights[self.instance_type]
        else:
            return 1


def autoscale_local_cluster(config_folder, dry_run=False, log_level=None):
    log.debug("Sleep 20s to throttle AWS API calls")
    time.sleep(20)
    if dry_run:
        log.info("Running in dry_run mode, no changes should be made")
    system_config = load_system_paasta_config()
    autoscaling_resources = system_config.get_cluster_autoscaling_resources()
    all_pool_settings = system_config.get_resource_pool_settings()
    autoscaling_scalers = []
    for identifier, resource in autoscaling_resources.items():
        pool_settings = all_pool_settings.get(resource['pool'], {})
        try:
            autoscaling_scalers.append(get_scaler(resource['type'])(resource=resource,
                                                                    pool_settings=pool_settings,
                                                                    config_folder=config_folder,
                                                                    dry_run=dry_run,
                                                                    log_level=log_level))
        except KeyError:
            log.warning("Couldn't find a metric provider for resource of type: {}".format(resource['type']))
            continue
        log.debug("Sleep 3s to throttle AWS API calls")
        time.sleep(3)
    sorted_autoscaling_scalers = sorted(autoscaling_scalers, key=lambda x: x.is_resource_cancelled(), reverse=True)
    for scaler in sorted_autoscaling_scalers:
        autoscale_cluster_resource(scaler)
        log.debug("Sleep 3s to throttle AWS API calls")
        time.sleep(3)


def get_scaler(scaler_type):
    scalers = {'aws_spot_fleet_request': SpotAutoscaler,
               'aws_autoscaling_group': AsgAutoscaler}
    return scalers[scaler_type]


def autoscale_cluster_resource(scaler):
    log.info("Autoscaling {} in pool, {}".format(scaler.resource['id'], scaler.resource['pool']))
    try:
        current, target = scaler.metrics_provider()
        log.info("Target capacity: {}, Capacity current: {}".format(target, current))
        scaler.scale_resource(current, target)
    except ClusterAutoscalingError as e:
        log.error('%s: %s' % (scaler.resource['id'], e))


def get_instances_from_ip(ip, instance_descriptions):
    """Filter AWS instance_descriptions based on PrivateIpAddress

    :param ip: private IP of AWS instance.
    :param instance_descriptions: list of AWS instance description dicts.
    :returns: list of instance description dicts"""
    instances = [instance for instance in instance_descriptions if instance['PrivateIpAddress'] == ip]
    return instances


def get_autoscaling_info_for_all_resources():
    system_config = load_system_paasta_config()
    autoscaling_resources = system_config.get_cluster_autoscaling_resources()
    pool_settings = system_config.get_resource_pool_settings()
    vals = [
        autoscaling_info_for_resource(resource, pool_settings)
        for resource in autoscaling_resources.values()
    ]
    return [x for x in vals if x is not None]


def autoscaling_info_for_resource(resource, pool_settings):
    pool_settings.get(resource['pool'], {})
    scaler_ref = get_scaler(resource['type'])
    scaler = scaler_ref(
        resource=resource,
        pool_settings=pool_settings,
        config_folder=None,
        dry_run=True
    )
    if not scaler.exists:
        log.info("no scaler for resource {}. ignoring".format(resource['id']))
        return None
    try:
        current_capacity, target_capacity = scaler.metrics_provider()
    except ClusterAutoscalingError:
        current_capacity, target_capacity = scaler.current_capacity, "Exception"
    return AutoscalingInfo(
        resource_id=scaler.resource['id'],
        pool=scaler.resource['pool'],
        state="active" if not scaler.is_resource_cancelled() else "cancelled",
        current=str(current_capacity),
        target=str(target_capacity),
        min_capacity=str(scaler.resource['min_capacity']),
        max_capacity=str(scaler.resource['max_capacity']),
        instances=str(len(scaler.instances))
    )
