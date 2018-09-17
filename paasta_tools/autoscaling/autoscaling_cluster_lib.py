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
import asyncio
import logging
import math
import os
import time
from collections import defaultdict
from collections import namedtuple
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from math import ceil
from math import floor
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import a_sync
import boto3
from botocore.exceptions import ClientError
from mypy_extensions import TypedDict
from requests.exceptions import HTTPError

from paasta_tools.autoscaling import cluster_boost
from paasta_tools.autoscaling import ec2_fitness
from paasta_tools.mesos.master import MesosState
from paasta_tools.mesos_maintenance import drain
from paasta_tools.mesos_maintenance import undrain
from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.mesos_tools import get_mesos_task_count_by_slave
from paasta_tools.mesos_tools import slave_pid_to_ip
from paasta_tools.mesos_tools import SlaveTaskCount
from paasta_tools.metrics.metastatus_lib import get_resource_utilization_by_grouping
from paasta_tools.metrics.metastatus_lib import ResourceInfo
from paasta_tools.metrics.metrics_lib import get_metrics_interface
from paasta_tools.paasta_maintenance import is_safe_to_kill
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import Timeout
from paasta_tools.utils import TimeoutError


AutoscalingInfo = namedtuple(
    "AutoscalingInfo",
    [
        "resource_id",
        "pool",
        "state",
        "current",
        "target",
        "min_capacity",
        "max_capacity",
        "instances",
    ],
)

ClusterAutoscalingResource = TypedDict(
    'ClusterAutoscalingResource',
    {
        'type': str,
        'id': str,
        'region': str,
        'pool': str,
        'min_capacity': int,
        'max_capacity': int,
    },
)

ResourcePoolSetting = TypedDict(
    'ResourcePoolSetting',
    {
        'target_utilization': float,
        'drain_timeout': int,
    },
)

CLUSTER_METRICS_PROVIDER_KEY = 'cluster_metrics_provider'
DEFAULT_TARGET_UTILIZATION = 0.8  # decimal fraction
DEFAULT_DRAIN_TIMEOUT = 600  # seconds

AWS_SPOT_MODIFY_TIMEOUT = 30
MISSING_SLAVE_PANIC_THRESHOLD = .3

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class Timer:
    def __init__(self, timeout: int) -> None:
        self.timeout = timedelta(seconds=timeout)
        self.start()

    def start(self) -> None:
        self.last_start = datetime.now()

    def ready(self) -> bool:
        return datetime.now() > self.last_start + self.timeout

    def left(self) -> timedelta:
        return self.last_start + self.timeout - datetime.now()


class ClusterAutoscaler:

    def __init__(
        self,
        resource: ClusterAutoscalingResource,
        pool_settings: ResourcePoolSetting,
        config_folder: str,
        dry_run: bool,
        utilization_error: float,
        max_increase: float,
        max_decrease: float,
        log_level: str=None,
        draining_enabled: bool=True,
        enable_metrics: bool=False,
        enable_maintenance_reservation: bool=True,
    ) -> None:
        self.resource = resource
        self.pool_settings = pool_settings
        self.config_folder = config_folder
        self.dry_run = dry_run
        self.utilization_error = utilization_error
        self.ideal_capacity: Optional[int] = None
        self.draining_enabled = draining_enabled
        self.max_increase = max_increase
        self.max_decrease = max_decrease
        if log_level is not None:
            self.log.setLevel(log_level)
        self.instances: List[Dict] = []
        self.sfr: Optional[Dict[str, Any]] = None
        self.enable_metrics = enable_metrics
        self.enable_maintenance_reservation = enable_maintenance_reservation

        self.log.info('Initialized with utilization error %s' % self.utilization_error)
        config = load_system_paasta_config()
        self.slave_newness_threshold = config.get_monitoring_config().get('check_registered_slave_threshold')
        self.setup_metrics(config)

    @property
    def log(self) -> logging.Logger:
        resource_id = self.resource.get("id", "unknown")
        name = '.'.join([__name__, self.__class__.__name__, resource_id])
        return logging.getLogger(name)

    def setup_metrics(self, config: SystemPaastaConfig) -> None:
        if not self.enable_metrics:
            return None
        dims = {
            'paasta_cluster': config.get_cluster(),
            'region': self.resource.get('region', 'unknown'),
            'pool': self.resource.get('pool', 'unknown'),
            'resource_id': self.resource.get('id', 'unknown'),
            'resource_type': self.__class__.__name__,
        }
        self.metrics = get_metrics_interface('paasta.cluster_autoscaler')
        self.target_gauge = self.metrics.create_gauge('target_capacity', **dims)
        self.current_gauge = self.metrics.create_gauge('current_capacity', **dims)
        self.ideal_gauge = self.metrics.create_gauge('ideal_capacity', **dims)
        self.max_gauge = self.metrics.create_gauge('max_capacity', **dims)
        self.min_gauge = self.metrics.create_gauge('min_capacity', **dims)
        self.mesos_error_gauge = self.metrics.create_gauge('mesos_error', **dims)
        self.aws_instances_gauge = self.metrics.create_gauge('aws_instances', **dims)
        self.mesos_slaves_gauge = self.metrics.create_gauge('mesos_slaves', **dims)

    def emit_metrics(
        self,
        current_capacity: float,
        target_capacity: float,
        mesos_slave_count: int,
    ) -> None:
        if not self.enable_metrics:
            return None
        self.current_gauge.set(current_capacity)
        self.target_gauge.set(target_capacity)
        self.ideal_gauge.set(self.ideal_capacity)
        self.min_gauge.set(self.resource['min_capacity'])
        self.max_gauge.set(self.resource['max_capacity'])
        self.mesos_error_gauge.set(self.utilization_error)
        self.aws_instances_gauge.set(len(self.instances))
        self.mesos_slaves_gauge.set(mesos_slave_count)

    def set_capacity(self, capacity: float) -> Optional[Any]:
        pass

    def get_instance_type_weights(self) -> Optional[Dict[str, float]]:
        pass

    def is_resource_cancelled(self) -> bool:
        raise NotImplementedError()

    def metrics_provider(self, mesos_state: MesosState) -> Tuple[float, int]:
        raise NotImplementedError()

    def describe_instances(
        self,
        instance_ids: List[str],
        region: Optional[str]=None,
        instance_filters: Optional[List[Dict]]=None,
    ) -> Optional[List[Dict]]:
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
                self.log.warn(f'Cannot find one or more instance from IDs {instance_ids}')
                return None
            else:
                raise
        instance_reservations = [reservation['Instances'] for reservation in instance_descriptions['Reservations']]
        instances = [instance for reservation in instance_reservations for instance in reservation]
        return instances

    def is_new_autoscaling_resource(self) -> bool:
        raise NotImplementedError()

    def describe_instance_status(
        self,
        instance_ids: List[str],
        region: Optional[str]=None,
        instance_filters: Optional[List[Dict]]=None,
    ) -> Optional[Dict]:
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
                Filters=instance_filters,
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidInstanceID.NotFound':
                self.log.warn(f'Cannot find one or more instance from IDs {instance_ids}')
                return None
            else:
                raise
        return instance_descriptions

    def get_instance_ips(
        self,
        instances: List[Dict],
        region: Optional[str]=None,
    ) -> List[str]:
        instance_descriptions = self.describe_instances(
            [instance['InstanceId'] for instance in instances],
            region=region,
        )
        instance_ips = []
        for instance in instance_descriptions:
            try:
                instance_ips.append(instance['PrivateIpAddress'])
            except KeyError:
                self.log.warning("Instance {} does not have an IP. This normally means it has been"
                                 " terminated".format(instance['InstanceId']))
        return instance_ips

    def cleanup_cancelled_config(
        self,
        resource_id: str,
        config_folder: str,
        dry_run: bool=False,
    ) -> None:
        file_name = f"{resource_id}.json"
        configs_to_delete = [os.path.join(walk[0], file_name)
                             for walk in os.walk(config_folder) if file_name in walk[2]]
        if not configs_to_delete:
            self.log.info(f"Resource config for {resource_id} not found")
            return
        if not dry_run:
            os.remove(configs_to_delete[0])
        self.log.info("Deleted Resource config {}".format(configs_to_delete[0]))

    def get_aws_slaves(self, mesos_state: MesosState) -> Dict[str, Dict]:
        instance_ips = self.get_instance_ips(self.instances, region=self.resource['region'])
        slaves = {
            slave['id']: slave for slave in mesos_state.get('slaves', [])
            if slave_pid_to_ip(slave['pid']) in instance_ips and
            slave['attributes'].get('pool', 'default') == self.resource['pool']
        }
        return slaves

    def check_expected_slaves(
        self,
        slaves: Dict[str, Dict],
        expected_instances: Optional[int],
    ) -> None:
        current_instances = len(slaves)
        if current_instances == 0:
            error_message = ("No instances are active, not scaling until the instances are attached to mesos")
            raise ClusterAutoscalingError(error_message)
        if not expected_instances:
            return None

        self.log.info("Found %.2f%% slaves registered in mesos for this resource (%d/%d)" % (
            float(float(current_instances) / float(expected_instances)) * 100,
            current_instances,
            expected_instances,
        ))
        if float(current_instances) / expected_instances < (1.00 - MISSING_SLAVE_PANIC_THRESHOLD):
            warning_message = (
                "We currently have %d instances active in mesos out of a desired %d.\n"
            ) % (
                current_instances, expected_instances,
            )

            if self.sfr and self.sfr['SpotFleetRequestState'] == 'cancelled_running':
                warning_message += "But this is an sfr in cancelled_running state, so continuing anyways"
            elif self.utilization_error < 0:
                warning_message += (
                    "Continuing because we are scaling down and we don't need any "
                    "of the unregistered instances."
                )
            else:
                warning_message += (
                    "This may be a sign that new instances are not healthy for some reason. "
                    "Continuing to scale."
                )

            self.log.warn(warning_message)

    async def can_kill(
        self,
        hostname: str,
        should_drain: bool,
        dry_run: bool,
        timer: Timer,
    ) -> bool:
        if dry_run:
            return True
        if timer.ready():
            self.log.warning("Timer expired before slave ready to kill, proceeding to terminate anyways")
            timer.start()
            raise TimeoutError
        if not should_drain:
            self.log.info("Not draining, waiting %s longer before killing" % timer.left())
            return False
        if await a_sync.run(is_safe_to_kill, hostname):
            self.log.info("Slave {} is ready to kill, with {} left on timer".format(hostname, timer.left()))
            timer.start()
            return True
        return False

    async def wait_and_terminate(
        self,
        slave: 'PaastaAwsSlave',
        drain_timeout: int,
        dry_run: bool,
        timer: Timer,
        region: Optional[str]=None,
        should_drain: bool=True,
    ) -> None:
        """Waits for slave to be drained and then terminate

        :param slave: dict of slave to kill
        :param drain_timeout: how long to wait before terminating
            even if not drained
        :param region to connect to ec2
        :param dry_run: Don't drain or make changes to spot fleet if True
        :param should_drain: whether we should drain hosts before waiting to stop them
        :param timer: a Timer object to keep terminates happening once every n seconds across co-routines
        """
        self.log.info("Starting TERMINATING: {} (Hostname = {}, IP = {})".format(
            slave.instance_id,
            slave.hostname,
            slave.ip,
        ))
        try:
            # This loop should always finish because the maintenance window should trigger is_ready_to_kill
            # being true. Just in case though we set a timeout (from timer) and terminate anyway
            while True:
                instance_id = slave.instance_id
                if not instance_id:
                    self.log.warning(
                        f"Didn't find instance ID for slave: {slave.pid}. Skipping terminating",
                    )
                    break
                # Check if no tasks are running or we have reached the maintenance window
                if await self.can_kill(slave.hostname, should_drain, dry_run, timer):
                    self.log.info("TERMINATING: {} (Hostname = {}, IP = {})".format(
                        instance_id,
                        slave.hostname,
                        slave.ip,
                    ))
                    self.terminate_instances([instance_id])
                    break
                else:
                    self.log.info(f"Instance {instance_id}: NOT ready to kill")
                self.log.debug("Waiting 5 seconds and then checking again")
                await asyncio.sleep(5)
        except TimeoutError:
            self.log.error("Timed out after {} waiting to drain {}, now terminating anyway".format(
                timer.timeout,
                slave.pid,
            ))
            self.terminate_instances([instance_id])

    def terminate_instances(self, instance_ids: List[str]) -> None:
        ec2_client = boto3.client('ec2', region_name=self.resource['region'])
        try:
            ec2_client.terminate_instances(
                InstanceIds=instance_ids,
                DryRun=self.dry_run,
            )
        except ClientError as e:
            if e.response['Error'].get('Code') == 'DryRunOperation':
                pass
            else:
                raise

    async def scale_resource(
        self,
        current_capacity: float,
        target_capacity: int,
    ) -> None:
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
            self.log.info(f"Already at target capacity: {target_capacity}")
            return
        elif delta > 0:
            self.log.info(f"Increasing resource capacity to: {target_capacity}")
            self.set_capacity(target_capacity)
            return
        elif delta < 0:
            mesos_state = await get_mesos_master().state_summary()
            slaves_list = await get_mesos_task_count_by_slave(mesos_state, pool=self.resource['pool'])
            filtered_slaves = self.filter_aws_slaves(slaves_list)
            killable_capacity = round(sum([slave.instance_weight for slave in filtered_slaves]), 2)
            amount_to_decrease = round(delta * -1, 2)
            if amount_to_decrease > killable_capacity:
                instance_ids_in_mesos = {
                    slave.instance_id for slave in filtered_slaves
                }
                instance_ids_not_in_mesos = [
                    instance['InstanceId'] for instance in self.instances
                    if instance['InstanceId'] not in instance_ids_in_mesos
                ]
                self.log.warning(
                    "Didn't find enough candidates to kill. This may mean that "
                    "some instances have not yet joined the cluster. Since we "
                    "are scaling down, we will kill the instances in this "
                    "pool that have not joined.\n"
                    "Desired decrease in capacity: %s, killable capacity: %s\n"
                    "Setting capacity to %s and killing instances %s before "
                    "proceeding scale down" % (
                        amount_to_decrease,
                        killable_capacity,
                        killable_capacity,
                        instance_ids_not_in_mesos,
                    ),
                )
                self.set_capacity(killable_capacity)
                self.terminate_instances(instance_ids_not_in_mesos)

            await self.downscale_aws_resource(
                filtered_slaves=filtered_slaves,
                current_capacity=current_capacity,
                target_capacity=target_capacity,
            )

    def should_drain(self, slave_to_kill: 'PaastaAwsSlave') -> bool:
        if not self.draining_enabled:
            return False
        if self.dry_run:
            return False
        if slave_to_kill.instance_status['SystemStatus']['Status'] != 'ok':
            return False
        if slave_to_kill.instance_status['InstanceStatus']['Status'] != 'ok':
            return False
        return True

    async def gracefully_terminate_slave(
        self,
        slave_to_kill: 'PaastaAwsSlave',
        capacity_diff: int,
        timer: Timer,
    ) -> None:
        """
        Since this is async, it can be suspended at an `await` call.  Because of this, we need to re-calculate
        the capacity each time we call `set_capacity` (as another coroutine could have set the capacity while
        this one was suspended).  `set_capacity` stores the currently set capacity in the object, and then
        this function re-calculates that from the capacity_diff each time we call `set_capacity`
        """
        drain_timeout = self.pool_settings.get('drain_timeout', DEFAULT_DRAIN_TIMEOUT)
        # The start time of the maintenance window is the point at which
        # we giveup waiting for the instance to drain and mark it for termination anyway
        start = int(time.time() + drain_timeout) * 1000000000  # nanoseconds
        # Set the duration to an hour, this is fairly arbitrary as mesos doesn't actually
        # do anything at the end of the maintenance window.
        duration = 600 * 1000000000  # nanoseconds
        self.log.info(f"Draining {slave_to_kill.pid}")
        should_drain = self.should_drain(slave_to_kill)
        if should_drain:
            try:
                drain_host_string = f"{slave_to_kill.hostname}|{slave_to_kill.ip}"
                await a_sync.to_async(drain)(
                    hostnames=[drain_host_string],
                    start=start,
                    duration=duration,
                    reserve_resources=self.enable_maintenance_reservation,
                )
            except HTTPError as e:
                self.log.error("Failed to start drain "
                               "on {}: {}\n Trying next host".format(slave_to_kill.hostname, e))
                raise
        self.log.info("Decreasing resource from {} to: {}".format(self.capacity, self.capacity + capacity_diff))
        # Instance weights can be floats but the target has to be an integer
        # because this is all AWS allows on the API call to set target capacity
        try:
            self.set_capacity(self.capacity + capacity_diff)
        except FailSetResourceCapacity:
            self.log.error("Couldn't update resource capacity, stopping autoscaler")
            self.log.info(f"Undraining {slave_to_kill.pid}")
            if should_drain:
                await a_sync.to_async(undrain)(
                    hostnames=[drain_host_string],
                    unreserve_resources=self.enable_maintenance_reservation,
                )
            raise
        self.log.info("Waiting for instance to drain before we terminate")
        try:
            await self.wait_and_terminate(
                slave=slave_to_kill,
                drain_timeout=drain_timeout,
                dry_run=self.dry_run,
                timer=timer,
                region=self.resource['region'],
                should_drain=should_drain,
            )
            if should_drain:
                await a_sync.to_async(undrain)(
                    hostnames=[drain_host_string],
                    unreserve_resources=self.enable_maintenance_reservation,
                )
        except ClientError as e:
            self.log.error(f"Failure when terminating: {slave_to_kill.pid}: {e}")
            self.log.error("Setting resource capacity back to {}".format(self.capacity - capacity_diff))
            self.set_capacity(self.capacity - capacity_diff)
            self.log.info(f"Undraining {slave_to_kill.pid}")
            if should_drain:
                await a_sync.to_async(undrain)(
                    hostnames=[drain_host_string],
                    unreserve_resources=self.enable_maintenance_reservation,
                )

    def filter_aws_slaves(self, slaves_list: Iterable[Dict[str, SlaveTaskCount]]) -> List['PaastaAwsSlave']:
        ips = self.get_instance_ips(self.instances, region=self.resource['region'])
        self.log.debug(f"IPs in AWS resources: {ips}")
        slaves = [
            slave for slave in slaves_list
            if slave_pid_to_ip(slave['task_counts'].slave['pid']) in ips
        ]
        slave_ips = [slave_pid_to_ip(slave['task_counts'].slave['pid']) for slave in slaves]
        instance_type_weights = self.get_instance_type_weights()
        instance_statuses = self.instance_status_for_instance_ids(
            instance_ids=[instance['InstanceId'] for instance in self.instances],
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
                    % (
                        ",".join(
                            [x['InstanceId'] for x in matching_descriptions],
                        ),
                        slave_ip,
                    )
                )
                description = matching_descriptions[0]
                matching_status = self.filter_instance_status_for_instance_id(
                    instance_id=description['InstanceId'],
                    instance_statuses=instance_statuses,
                )
                assert len(matching_status) == 1, "There should be only one InstanceStatus per instance"
            else:
                description = None

            paasta_aws_slaves.append(PaastaAwsSlave(
                slave=slave,
                instance_status=matching_status[0],
                instance_description=description,
                instance_type_weights=instance_type_weights,
            ))

        return paasta_aws_slaves

    def instance_status_for_instance_ids(
        self,
        instance_ids: List[str],
    ) -> Dict[str, List[Dict[str, Union[str, Dict, List]]]]:
        """
        Return a list of instance statuses. Batch the API calls into
        groups of 99, since AWS limit it.
        """
        partitions = [
            instance_ids[partition: partition + 99]
            for partition in
            range(0, len(instance_ids), 99)
        ]
        accumulated: Dict[str, List[Dict[str, Union[str, Dict, List]]]] = {
            'InstanceStatuses': [],
        }
        for subgroup in partitions:
            res = self.describe_instance_status(
                instance_ids=subgroup,
                region=self.resource['region'],
            )
            accumulated['InstanceStatuses'] += res['InstanceStatuses']

        return accumulated

    def instance_descriptions_for_ips(self, ips: List[str]) -> List[Dict[str, Any]]:
        all_instances: List[Dict] = []
        for start, stop in zip(range(0, len(ips), 199), range(199, len(ips) + 199, 199)):
            all_instances += self.describe_instances(
                instance_ids=[],
                region=self.resource['region'],
                instance_filters=[
                    {
                        'Name': 'private-ip-address',
                        'Values': ips[start:stop],
                    },
                ],
            )
        return all_instances

    def filter_instance_description_for_ip(
        self,
        ip: str,
        instance_descriptions: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        return [
            i for i in instance_descriptions
            if ip == i['PrivateIpAddress']
        ]

    def filter_instance_status_for_instance_id(
        self,
        instance_id: str,
        instance_statuses: Dict[str, List[Dict[str, Union[str, Dict, List]]]],
    ) -> List[Dict[str, Union[str, Dict, List]]]:
        return [
            status for status in instance_statuses['InstanceStatuses']
            if status['InstanceId'] == instance_id
        ]

    async def downscale_aws_resource(
        self,
        filtered_slaves: List['PaastaAwsSlave'],
        current_capacity: float,
        target_capacity: int,
    ) -> None:
        self.log.info("downscale_aws_resource for %s" % filtered_slaves)
        killed_slaves = 0
        terminate_tasks = {}
        self.capacity = current_capacity
        timer = Timer(300)
        while True:
            filtered_sorted_slaves = ec2_fitness.sort_by_ec2_fitness(filtered_slaves)[::-1]
            if len(filtered_sorted_slaves) == 0:
                self.log.info("ALL slaves killed so moving on to next resource!")
                break
            self.log.info("Resource slave kill preference: {}".format([
                slave.hostname
                for slave in filtered_sorted_slaves
            ]))
            slave_to_kill = filtered_sorted_slaves.pop(0)
            instance_capacity = slave_to_kill.instance_weight
            new_capacity = current_capacity - instance_capacity
            if new_capacity < target_capacity:
                self.log.info("Terminating instance {} with weight {} would take us below our target of {},"
                              " so this is as close to our target as we can get".format(
                                  slave_to_kill.instance_id,
                                  slave_to_kill.instance_weight,
                                  target_capacity,
                              ))
                if self.resource['type'] == 'aws_spot_fleet_request' and killed_slaves == 0:
                    self.log.info(
                        "This is a SFR so we must kill at least one slave to prevent the autoscaler "
                        "getting stuck whilst scaling down gradually",
                    )
                    if new_capacity < 1 and self.sfr['SpotFleetRequestState'] == 'active':
                        self.log.info(
                            "Can't set target capacity to less than 1 for SFRs. No further "
                            "action for this SFR",

                        )
                        break
                else:
                    break

            capacity_diff = new_capacity - current_capacity
            self.log.info("Starting async kill for %s" % slave_to_kill.hostname)
            # My understanding is that ensure_future will actually start running the coroutine
            #  (gracefully_terminate_slave), until it hits something that sleeps, then the loop
            #  can continue and we start killing the next slave
            terminate_tasks[slave_to_kill.hostname] = asyncio.ensure_future(
                self.gracefully_terminate_slave(
                    slave_to_kill=slave_to_kill,
                    capacity_diff=capacity_diff,
                    timer=timer,
                ),
            )
            killed_slaves += 1

            current_capacity = new_capacity
            filtered_slaves = filtered_sorted_slaves

        # Now we wait for each task to actually finish...
        for hostname, task in terminate_tasks.items():
            try:
                await task
            except (HTTPError, FailSetResourceCapacity):
                continue


class SpotAutoscaler(ClusterAutoscaler):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.sfr = self.get_sfr(self.resource['id'], region=self.resource['region'])
        if self.sfr:
            self.instances = self.get_spot_fleet_instances(self.resource['id'], region=self.resource['region'])
            if self.sfr['SpotFleetRequestState'] == 'cancelled_running' and self.utilization_error < 0:
                self.log.warn(
                    'We are in cancelled_running with utilization_error < 0, '
                    'resetting utilization error to -1, min_capacity to 0 in '
                    'order to fully scale down.',
                )
                self.utilization_error = -1
                self.resource['min_capacity'] = 0

            self.ideal_capacity = self.get_ideal_capacity()

    @property
    def exists(self) -> bool:
        return False if not self.sfr or self.sfr['SpotFleetRequestState'] == 'cancelled' else True

    def get_sfr(
        self,
        spotfleet_request_id: str,
        region: Optional[str]=None,
    ) -> Dict[str, Any]:
        ec2_client = boto3.client('ec2', region_name=region)
        try:
            sfrs = ec2_client.describe_spot_fleet_requests(SpotFleetRequestIds=[spotfleet_request_id])
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidSpotFleetRequestId.NotFound':
                self.log.warn(f'Cannot find SFR {spotfleet_request_id}')
                return None
            else:
                raise
        ret = sfrs['SpotFleetRequestConfigs'][0]
        return ret

    def is_new_autoscaling_resource(self) -> bool:
        """
        Determines if the spot fleet request was created recently as defined by
        CHECK_REGISTERED_SLAVE_THRESHOLD.
        """
        if not self.sfr:
            self.log.warn('Cannot find SFR {}'.format(self.resource['id']))
            return True

        now = datetime.now(timezone.utc)
        return (now - self.sfr['CreateTime']).total_seconds() < self.slave_newness_threshold

    def get_spot_fleet_instances(
        self,
        spotfleet_request_id: str,
        region: Optional[str]=None,
    ) -> List[Dict[str, str]]:
        ec2_client = boto3.client('ec2', region_name=region)
        spot_fleet_instances = ec2_client.describe_spot_fleet_instances(
            SpotFleetRequestId=spotfleet_request_id,
        )['ActiveInstances']
        return spot_fleet_instances

    def metrics_provider(self, mesos_state: MesosState) -> Tuple[float, int]:
        if not self.sfr or self.sfr['SpotFleetRequestState'] == 'cancelled':
            self.log.error("SFR not found or cancelled, removing config file.")
            self.cleanup_cancelled_config(self.resource['id'], self.config_folder, dry_run=self.dry_run)
            return 0, 0
        elif self.sfr['SpotFleetRequestState'] == 'cancelled_running' and len(self.instances) == 0:
            self.log.error(
                "This is a cancelled_running SFR with no instances. This is not "
                "a valid SFR state, and it should eventually be cancelled by AWS. "
                "Removing its config.",
            )
            self.cleanup_cancelled_config(self.resource['id'], self.config_folder, dry_run=self.dry_run)
            return 0, 0
        elif self.sfr['SpotFleetRequestState'] in ['submitted', 'modifying', 'cancelled_terminating']:
            self.log.warning(
                "Not scaling an SFR in state: {} so {}, skipping...".format(
                    self.sfr['SpotFleetRequestState'],
                    self.resource['id'],
                ),
            )
            return 0, 0
        elif self.sfr['SpotFleetRequestState'] == 'cancelled_running' and self.utilization_error > 0:
            self.log.warning("Cannot scale cancelled_running SFR upwards, skipping.")
            return 0, 0
        elif self.sfr['SpotFleetRequestState'] not in ['cancelled_running', 'active']:
            self.log.error("Unexpected SFR state: {} for {}".format(
                self.sfr['SpotFleetRequestState'],
                self.resource['id'],
            ))
            raise ClusterAutoscalingError

        expected_instances = len(self.instances)
        if expected_instances == 0:
            self.log.warning(
                "No instances found in SFR, this shouldn't be possible so we "
                "do nothing",
            )
            return 0, 0
        slaves = self.get_aws_slaves(mesos_state)
        self.check_expected_slaves(slaves, expected_instances)
        current, target = self.get_spot_fleet_delta()

        self.emit_metrics(current, target, mesos_slave_count=len(slaves))
        return current, target

    def is_aws_launching_instances(self) -> bool:
        fulfilled_capacity = self.sfr['SpotFleetRequestConfig']['FulfilledCapacity']
        target_capacity = self.sfr['SpotFleetRequestConfig']['TargetCapacity']
        return target_capacity > fulfilled_capacity

    @property
    def current_capacity(self) -> float:
        return float(self.sfr['SpotFleetRequestConfig']['FulfilledCapacity'])

    def get_ideal_capacity(self) -> int:
        return int(ceil((1 + self.utilization_error) * self.current_capacity))

    def get_spot_fleet_delta(self) -> Tuple[float, int]:
        current_capacity = self.current_capacity
        new_capacity = int(min(
            max(
                self.resource['min_capacity'],
                floor(current_capacity * (1.00 - self.max_decrease)),
                self.ideal_capacity,

                # Can only scale a cancelled_running SFR to 0 instances
                0 if self.sfr['SpotFleetRequestState'] == 'cancelled_running' else 1,
            ),
            ceil(current_capacity * (1.00 + self.max_increase)),
            self.resource['max_capacity'],
        ))
        new_capacity = max(new_capacity, self.resource['min_capacity'])
        self.log.debug("The ideal capacity to scale to is %d instances" % self.ideal_capacity)
        self.log.debug("The capacity we will scale to is %d instances" % new_capacity)
        if self.ideal_capacity > self.resource['max_capacity']:
            self.log.warning(
                "Our ideal capacity (%d) is higher than max_capacity (%d). Consider raising max_capacity!" % (
                    self.ideal_capacity, self.resource['max_capacity'],
                ),
            )
        if self.ideal_capacity < self.resource['min_capacity']:
            self.log.warning(
                "Our ideal capacity (%d) is lower than min_capacity (%d). Consider lowering min_capacity!" % (
                    self.ideal_capacity, self.resource['min_capacity'],
                ),
            )
        return current_capacity, new_capacity

    def set_capacity(self, capacity: float) -> Optional[Any]:
        """ AWS won't modify a request that is already modifying. This
        function ensures we wait a few seconds in case we've just modified
        a SFR"""
        rounded_capacity = int(floor(capacity))
        ec2_client = boto3.client('ec2', region_name=self.resource['region'])
        with Timeout(seconds=AWS_SPOT_MODIFY_TIMEOUT):
            try:
                state = None
                while True:
                    state = self.get_sfr(self.resource['id'], region=self.resource['region'])['SpotFleetRequestState']
                    if state == 'active':
                        break
                    if state == 'cancelled_running':
                        self.log.info(
                            "Not updating target capacity because this is a cancelled SFR, "
                            "we are just draining and killing the instances",
                        )
                        return None
                    self.log.debug("SFR {} in state {}, waiting for state: active".format(self.resource['id'], state))
                    self.log.debug("Sleep 5 seconds")
                    time.sleep(5)
            except TimeoutError:
                self.log.error("Spot fleet {} not in active state so we can't modify it.".format(self.resource['id']))
                raise FailSetResourceCapacity
        if self.dry_run:
            return True
        try:
            ret = ec2_client.modify_spot_fleet_request(
                SpotFleetRequestId=self.resource['id'], TargetCapacity=rounded_capacity,
                ExcessCapacityTerminationPolicy='noTermination',
            )
        except ClientError as e:
            self.log.error(f"Error modifying spot fleet request: {e}")
            raise FailSetResourceCapacity
        self.capacity = capacity
        return ret

    def is_resource_cancelled(self) -> bool:
        if not self.sfr:
            return True
        state = self.sfr['SpotFleetRequestState']
        if state in ['cancelled', 'cancelled_running']:
            return True
        return False

    def get_instance_type_weights(self) -> Dict[str, float]:
        launch_specifications = self.sfr['SpotFleetRequestConfig']['LaunchSpecifications']
        return {ls['InstanceType']: ls['WeightedCapacity'] for ls in launch_specifications}


class AsgAutoscaler(ClusterAutoscaler):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.asg = self.get_asg(self.resource['id'], region=self.resource['region'])
        if self.asg:
            self.instances = self.asg['Instances']

            if len(self.instances) == 0:
                self.log.warning(
                    "This ASG has no instances, delta should be 1 to "
                    "launch first instance unless max/min capacity override",
                )
                self.utilization_error = 1

            self.ideal_capacity = self.get_ideal_capacity()

    @property
    def exists(self) -> bool:
        return True if self.asg else False

    def is_new_autoscaling_resource(self) -> bool:
        """
        Determines if an autoscaling group was created recently as defined by
        CHECK_REGISTERED_SLAVE_THRESHOLD.
        """
        if not self.asg:
            self.log.warning("ASG {} not found, removing config file".format(self.resource['id']))
            return True

        now = datetime.now(timezone.utc)
        return (now - self.asg['CreatedTime']).total_seconds() < self.slave_newness_threshold

    def get_asg(self, asg_name: str, region: Optional[str]=None) -> Optional[Dict[str, Any]]:
        asg_client = boto3.client('autoscaling', region_name=region)
        asgs = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
        try:
            return asgs['AutoScalingGroups'][0]
        except IndexError:
            self.log.warning(f"No ASG found in this account with name: {asg_name}")
            return None

    def metrics_provider(self, mesos_state: MesosState) -> Tuple[int, int]:
        if not self.asg:
            self.log.warning("ASG {} not found, removing config file".format(self.resource['id']))
            self.cleanup_cancelled_config(self.resource['id'], self.config_folder, dry_run=self.dry_run)
            return 0, 0
        if self.is_aws_launching_instances():
            self.log.warning(
                "ASG still launching new instances so we won't make any"
                "changes this time.",
            )
            return 0, 0
        expected_instances = len(self.instances)
        slaves = self.get_aws_slaves(mesos_state)
        self.check_expected_slaves(slaves, expected_instances)
        current, target = self.get_asg_delta()

        self.emit_metrics(current, target, mesos_slave_count=len(slaves))
        return current, target

    def is_aws_launching_instances(self) -> bool:
        fulfilled_capacity = len(self.asg['Instances'])
        target_capacity = self.asg['DesiredCapacity']
        return target_capacity > fulfilled_capacity

    @property
    def current_capacity(self) -> int:
        return len(self.asg['Instances'])

    def get_ideal_capacity(self) -> int:
        if self.current_capacity == 0:
            return int(min(
                max(1, self.resource['min_capacity']),
                self.resource['max_capacity'],
            ))
        else:
            return int(ceil((1 + self.utilization_error) * self.current_capacity))

    def get_asg_delta(self) -> Tuple[int, int]:
        current_capacity = self.current_capacity
        if current_capacity == 0:
            new_capacity = self.ideal_capacity
        else:
            new_capacity = int(min(
                max(
                    self.resource['min_capacity'],
                    floor(current_capacity * (1.00 - self.max_decrease)),
                    self.ideal_capacity,
                    0,
                ),
                ceil(current_capacity * (1.00 + self.max_increase)),
                # if max and min set to 0 we still drain gradually
                max(self.resource['max_capacity'], floor(current_capacity * (1.00 - self.max_decrease))),
            ))
        new_capacity = max(new_capacity, self.resource['min_capacity'])
        self.log.debug("The ideal capacity to scale to is %d instances" % self.ideal_capacity)
        self.log.debug("The capacity we will scale to is %d instances" % new_capacity)
        if self.ideal_capacity > self.resource['max_capacity']:
            self.log.warning(
                "Our ideal capacity (%d) is higher than max_capacity (%d). Consider raising max_capacity!" % (
                    self.ideal_capacity, self.resource['max_capacity'],
                ),
            )
        if self.ideal_capacity < self.resource['min_capacity']:
            self.log.warning(
                "Our ideal capacity (%d) is lower than min_capacity (%d). Consider lowering min_capacity!" % (
                    self.ideal_capacity, self.resource['min_capacity'],
                ),
            )
        return current_capacity, new_capacity

    def set_capacity(self, capacity: float) -> Optional[Any]:
        if self.dry_run:
            return True
        asg_client = boto3.client('autoscaling', region_name=self.resource['region'])
        try:
            ret = asg_client.update_auto_scaling_group(
                AutoScalingGroupName=self.resource['id'],
                DesiredCapacity=capacity,
            )
        except ClientError as e:
            self.log.error(f"Error modifying ASG: {e}")
            raise FailSetResourceCapacity
        self.capacity = capacity
        return ret

    def is_resource_cancelled(self) -> bool:
        return not self.asg

    def get_instance_type_weights(self) -> None:
        # None means that all instances will be
        # assumed to have a weight of 1
        return None


class ClusterAutoscalingError(Exception):
    pass


class FailSetResourceCapacity(Exception):
    pass


class PaastaAwsSlave:
    """
    Defines a slave object for use by the autoscaler, containing the mesos slave
    object from mesos state and some properties from AWS
    """

    def __init__(
        self,
        slave: Dict[str, SlaveTaskCount],
        instance_description: Dict[str, Any],
        instance_status: Optional[Dict[str, Any]]=None,
        instance_type_weights: Optional[Dict]=None,
    ) -> None:
        if instance_status is None:
            self.instance_status: Dict[str, Any] = {}
        else:
            self.instance_status = instance_status
        self.wrapped_slave = slave
        self.instance_description = instance_description
        self.instance_type_weights = instance_type_weights
        self.task_counts = slave['task_counts']
        self.slave = self.task_counts.slave
        self.ip = slave_pid_to_ip(self.slave['pid'])

    @property
    def instance(self) -> Dict[str, Any]:
        return self.instance_description

    @property
    def instance_id(self) -> str:
        return self.instance['InstanceId']

    @property
    def hostname(self) -> str:
        return self.slave['hostname']

    @property
    def pid(self) -> str:
        return self.slave['pid']

    @property
    def instance_type(self) -> str:
        return self.instance_description['InstanceType']

    @property
    def instance_weight(self) -> float:
        if self.instance_type_weights:
            return self.instance_type_weights[self.instance_type]
        else:
            return 1


def get_all_utilization_errors(
    autoscaling_resources: Dict[str, Dict[str, str]],
    all_pool_settings: Dict[str, Dict],
    mesos_state: MesosState,
    system_config: SystemPaastaConfig,
) -> Dict[Tuple[str, str], float]:
    errors: Dict[Tuple[str, str], float] = {}
    for identifier, resource in autoscaling_resources.items():
        pool = resource['pool']
        region = resource['region']
        if (region, pool) in errors.keys():
            continue

        target_utilization = all_pool_settings.get(
            pool, {},
        ).get(
            'target_utilization', DEFAULT_TARGET_UTILIZATION,
        )
        errors[(region, pool)] = get_mesos_utilization_error(
            mesos_state=mesos_state,
            system_config=system_config,
            region=region,
            pool=pool,
            target_utilization=target_utilization,
        )

    return errors


async def autoscale_local_cluster(
    config_folder: str,
    dry_run: bool=False,
    log_level: str=None,
) -> None:
    log.debug("Sleep 20s to throttle AWS API calls")
    time.sleep(20)
    if dry_run:
        log.info("Running in dry_run mode, no changes should be made")
    system_config = load_system_paasta_config()
    autoscaling_resources = system_config.get_cluster_autoscaling_resources()
    autoscaling_draining_enabled = system_config.get_cluster_autoscaling_draining_enabled()
    all_pool_settings = system_config.get_resource_pool_settings()
    mesos_state = await get_mesos_master().state()
    utilization_errors = get_all_utilization_errors(
        autoscaling_resources=autoscaling_resources,
        all_pool_settings=all_pool_settings,
        mesos_state=mesos_state,
        system_config=system_config,
    )
    log.info("Utilization errors: %s" % utilization_errors)
    autoscaling_scalers: Dict[Tuple[str, str], List[ClusterAutoscaler]] = defaultdict(list)
    for identifier, resource in autoscaling_resources.items():
        pool_settings = all_pool_settings.get(resource['pool'], {})
        try:
            scaler = get_scaler(resource['type'])(
                resource=resource,
                pool_settings=pool_settings,
                config_folder=config_folder,
                dry_run=dry_run,
                max_increase=system_config.get_cluster_autoscaler_max_increase(),
                max_decrease=system_config.get_cluster_autoscaler_max_decrease(),
                log_level=log_level,
                utilization_error=utilization_errors[(resource['region'], resource['pool'])],
                draining_enabled=autoscaling_draining_enabled,
                enable_metrics=True,
                enable_maintenance_reservation=system_config.get_maintenance_resource_reservation_enabled(),
            )
            autoscaling_scalers[(resource['region'], resource['pool'])].append(scaler)
        except KeyError:
            log.warning("Couldn't find a metric provider for resource of type: {}".format(resource['type']))
            continue
        log.debug("Sleep 3s to throttle AWS API calls")
        time.sleep(3)
    filtered_autoscaling_scalers = filter_scalers(autoscaling_scalers, utilization_errors)
    sorted_autoscaling_scalers = sort_scalers(filtered_autoscaling_scalers)
    await run_parallel_scalers(sorted_autoscaling_scalers, mesos_state)


def sort_scalers(filtered_autoscaling_scalers: List[ClusterAutoscaler]) -> List[ClusterAutoscaler]:
    return sorted(
        filtered_autoscaling_scalers, key=lambda x: x.is_resource_cancelled(), reverse=True,
    )


def filter_scalers(
    autoscaling_scalers: Dict[Tuple[str, str], List[ClusterAutoscaler]],
    utilization_errors: Dict[Tuple[str, str], float],
) -> List[ClusterAutoscaler]:
    any_cancelled_in_pool: Dict[Tuple[str, str], bool] = defaultdict(lambda: False)
    for (region, pool), scalers in autoscaling_scalers.items():
        for scaler in scalers:
            if scaler.is_resource_cancelled():
                any_cancelled_in_pool[(region, pool)] = True

    filtered_autoscaling_scalers: List[ClusterAutoscaler] = []

    for (region, pool), scalers in autoscaling_scalers.items():
        if any_cancelled_in_pool[(region, pool)] and utilization_errors[(region, pool)] < 0:
            filtered_autoscaling_scalers += [s for s in scalers if s.is_resource_cancelled()]
            skipped: List[ClusterAutoscaler] = [s for s in scalers if not s.is_resource_cancelled()]
            log.info("There are cancelled resources in pool %s, skipping active resources" % pool)
            log.info("Skipped: %s" % skipped)
        else:
            filtered_autoscaling_scalers += scalers

    return filtered_autoscaling_scalers


async def run_parallel_scalers(
    sorted_autoscaling_scalers: List[ClusterAutoscaler],
    mesos_state: MesosState,
) -> None:
    scaling_tasks = []
    for scaler in sorted_autoscaling_scalers:
        scaling_tasks.append(asyncio.ensure_future(autoscale_cluster_resource(scaler, mesos_state)))
        log.debug("Sleep 3s to throttle AWS API calls")
        await asyncio.sleep(3)
    for task in scaling_tasks:
        if task.cancelled() or task.done():
            continue
        await task


def get_scaler(scaler_type: str) -> type:
    scalers = {
        'aws_spot_fleet_request': SpotAutoscaler,
        'aws_autoscaling_group': AsgAutoscaler,
    }
    return scalers[scaler_type]


async def autoscale_cluster_resource(scaler: ClusterAutoscaler, mesos_state: MesosState) -> None:
    log.info("Autoscaling {} in pool, {}".format(scaler.resource['id'], scaler.resource['pool']))
    try:
        current, target = scaler.metrics_provider(mesos_state)
        log.info(f"Target capacity: {target}, Capacity current: {current}")
        await scaler.scale_resource(current, target)
    except ClusterAutoscalingError as e:
        log.error('{}: {}'.format(scaler.resource['id'], e))


def get_instances_from_ip(ip: str, instance_descriptions: List[Dict]) -> List[Dict]:
    """Filter AWS instance_descriptions based on PrivateIpAddress

    :param ip: private IP of AWS instance.
    :param instance_descriptions: list of AWS instance description dicts.
    :returns: list of instance description dicts"""
    instances = [instance for instance in instance_descriptions if instance['PrivateIpAddress'] == ip]
    return instances


def get_autoscaling_info_for_all_resources(mesos_state: MesosState) -> List[AutoscalingInfo]:
    system_config = load_system_paasta_config()
    autoscaling_resources = system_config.get_cluster_autoscaling_resources()
    pool_settings = system_config.get_resource_pool_settings()
    all_pool_settings = system_config.get_resource_pool_settings()
    utilization_errors = get_all_utilization_errors(
        autoscaling_resources=autoscaling_resources,
        all_pool_settings=all_pool_settings,
        mesos_state=mesos_state,
        system_config=system_config,
    )
    vals = [
        autoscaling_info_for_resource(
            resource=resource,
            pool_settings=pool_settings,
            mesos_state=mesos_state,
            utilization_errors=utilization_errors,
            max_increase=system_config.get_cluster_autoscaler_max_increase(),
            max_decrease=system_config.get_cluster_autoscaler_max_decrease(),
        )
        for resource in autoscaling_resources.values()
    ]
    return [x for x in vals if x is not None]


def autoscaling_info_for_resource(
    resource: Dict[str, str],
    pool_settings: Dict[str, Dict],
    mesos_state: MesosState,
    utilization_errors: Dict[Tuple[str, str], float],
    max_increase: float,
    max_decrease: float,
) -> Optional[AutoscalingInfo]:
    scaler_ref = get_scaler(resource['type'])
    scaler = scaler_ref(
        resource=resource,
        pool_settings=pool_settings,
        config_folder=None,
        max_increase=max_increase,
        max_decrease=max_decrease,
        dry_run=True,
        utilization_error=utilization_errors[(resource['region'], resource['pool'])],
    )
    if not scaler.exists:
        log.info("no scaler for resource {}. ignoring".format(resource['id']))
        return None
    try:
        current_capacity, target_capacity = scaler.metrics_provider(mesos_state)
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
        instances=str(len(scaler.instances)),
    )


def get_mesos_utilization_error(
    mesos_state: MesosState,
    system_config: SystemPaastaConfig,
    region: str,
    pool: str,
    target_utilization: float,
) -> float:
    """Return the relative capacity needed to reach the cluster target usage.
    Example: If the current capacity is 10 unit (could be CPU, memory, disk, gpu...)
    If the target usage is 0.8 and the current usage is 9 units. We will return 1.125:
    An 12.5% increase in capacity is required => 9/11.25 = 80% usage
    When the boost feature is enabled, the current_load will be artificially increased
    and stored into boosted_load. If the boost is disabled, boosted_load = current_load
    """
    try:
        region_pool_utilization_dict = get_resource_utilization_by_grouping(
            lambda slave: (slave['attributes']['pool'], slave['attributes']['datacenter'],),
            mesos_state,
        )[(pool, region,)]
    except KeyError:
        log.info(
            "Failed to find utilization for region %s, pool %s, returning 0 error" %
            (region, pool),
        )
        return 0

    log.debug(repr(region_pool_utilization_dict))
    usage_percs = []
    for resource in ResourceInfo._fields:
        free = getattr(region_pool_utilization_dict['free'], resource)
        total = getattr(region_pool_utilization_dict['total'], resource)

        if math.isclose(total, 0):
            continue

        current_load = total - free

        # We apply the boost only on the cpu resource.
        if resource == 'cpus'and system_config.get_cluster_boost_enabled():
            boosted_load = cluster_boost.get_boosted_load(region=region, pool=pool, current_load=current_load)
        else:
            boosted_load = current_load

        usage_percs.append(boosted_load / float(total))

    if len(usage_percs) == 0:  # If all resource totals are close to 0 for some reason
        return 0

    # We only look at the percentage of utilization. Whichever is the highest (closer or past the setpoint)
    # will be used to determine how much we need to scale
    utilization = max(usage_percs)
    return utilization - target_utilization
