# Copyright 2019 Yelp Inc.
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
import argparse
import json
import socket
import time
from typing import Callable
from typing import Dict
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Type

import arrow
import colorlog
import staticconf

from clusterman.args import add_cluster_arg
from clusterman.args import subparser
from clusterman.aws.aws_resource_group import AWSResourceGroup
from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.client import sqs
from clusterman.aws.spot_fleet_resource_group import SpotFleetResourceGroup
from clusterman.aws.util import RESOURCE_GROUPS
from clusterman.aws.util import RESOURCE_GROUPS_REV
from clusterman.config import load_cluster_pool_config
from clusterman.config import POOL_NAMESPACE
from clusterman.config import setup_config
from clusterman.draining.kubernetes import kube_delete_node
from clusterman.draining.kubernetes import kube_drain
from clusterman.draining.mesos import down
from clusterman.draining.mesos import drain
from clusterman.draining.mesos import operator_api
from clusterman.draining.mesos import up
from clusterman.interfaces.resource_group import InstanceMetadata
from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector
from clusterman.util import get_pool_name_list


logger = colorlog.getLogger(__name__)
DRAIN_CACHE_SECONDS = 1800


class Host(NamedTuple):
    instance_id: str
    hostname: str
    group_id: str
    ip: str
    sender: str
    receipt_handle: str
    scheduler: str = 'mesos'


class DrainingClient():
    def __init__(self, cluster_name: str) -> None:
        self.client = sqs
        self.cluster = cluster_name
        self.drain_queue_url = staticconf.read_string(f'clusters.{cluster_name}.drain_queue_url')
        self.termination_queue_url = staticconf.read_string(f'clusters.{cluster_name}.termination_queue_url')
        self.draining_host_ttl_cache: Dict[str, arrow.Arrow] = {}
        self.warning_queue_url = staticconf.read_string(
            f'clusters.{cluster_name}.warning_queue_url',
            default=None,
        )

    def submit_instance_for_draining(
        self,
        instance: InstanceMetadata,
        sender: Type[AWSResourceGroup],
        scheduler: str
    ) -> None:
        return self.client.send_message(
            QueueUrl=self.drain_queue_url,
            MessageAttributes={
                'Sender': {
                    'DataType': 'String',
                    'StringValue': RESOURCE_GROUPS_REV[sender],
                },
            },
            MessageBody=json.dumps(
                {
                    'instance_id': instance.instance_id,
                    'ip': instance.ip_address,
                    'hostname': instance.hostname,
                    'group_id': instance.group_id,
                    'scheduler': scheduler,
                }
            ),
        )

    def submit_host_for_draining(self, host: Host) -> None:
        return self.client.send_message(
            QueueUrl=self.drain_queue_url,
            MessageAttributes={
                'Sender': {
                    'DataType': 'String',
                    'StringValue': host.sender,
                },
            },
            MessageBody=json.dumps(
                {
                    'instance_id': host.instance_id,
                    'ip': host.ip,
                    'hostname': host.hostname,
                    'group_id': host.group_id,
                    'scheduler': host.scheduler,
                }
            ),
        )

    def submit_host_for_termination(self, host: Host, delay: Optional[int] = None) -> None:
        delay_seconds = delay if delay is not None else staticconf.read_int(
            f'drain_termination_timeout_seconds.{host.sender}', default=90
        )
        logger.info(f'Delaying terminating {host.instance_id} for {delay_seconds} seconds')
        return self.client.send_message(
            QueueUrl=self.termination_queue_url,
            DelaySeconds=delay_seconds,
            MessageAttributes={
                'Sender': {
                    'DataType': 'String',
                    'StringValue': host.sender,
                },
            },
            MessageBody=json.dumps(
                {
                    'instance_id': host.instance_id,
                    'ip': host.ip,
                    'hostname': host.hostname,
                    'group_id': host.group_id,
                    'scheduler': host.scheduler,
                }
            ),
        )

    def get_host_to_drain(self) -> Optional[Host]:
        messages = self.client.receive_message(
            QueueUrl=self.drain_queue_url,
            MessageAttributeNames=['Sender'],
            MaxNumberOfMessages=1
        ).get('Messages', [])
        if messages:
            host_data = json.loads(messages[0]['Body'])
            return Host(
                sender=messages[0]['MessageAttributes']['Sender']['StringValue'],
                receipt_handle=messages[0]['ReceiptHandle'],
                **host_data,
            )
        return None

    def get_warned_host(self) -> Optional[Host]:
        if self.warning_queue_url is None:
            return None
        messages = self.client.receive_message(
            QueueUrl=self.warning_queue_url,
            MessageAttributeNames=['Sender'],
            MaxNumberOfMessages=1
        ).get('Messages', [])
        if messages:
            event_data = json.loads(messages[0]['Body'])
            host = host_from_instance_id(
                sender=RESOURCE_GROUPS_REV[SpotFleetResourceGroup],
                receipt_handle=messages[0]['ReceiptHandle'],
                instance_id=event_data['detail']['instance-id'],
            )
            # if we couldn't derive the host data from the instance id
            # then we just delete the message so we don't get stuck
            # worse case AWS will just terminate the box for us...
            if not host:
                logger.warning(
                    "Couldn't derive host data from instance id {} skipping".format(event_data['detail']['instance-id'])
                )
                self.client.delete_message(
                    QueueUrl=self.warning_queue_url,
                    ReceiptHandle=messages[0]['ReceiptHandle']
                )
            else:
                return host
        return None

    def get_host_to_terminate(self) -> Optional[Host]:
        messages = self.client.receive_message(
            QueueUrl=self.termination_queue_url,
            MessageAttributeNames=['Sender'],
            MaxNumberOfMessages=1,
        ).get('Messages', [])
        if messages:
            host_data = json.loads(messages[0]['Body'])
            return Host(
                sender=messages[0]['MessageAttributes']['Sender']['StringValue'],
                receipt_handle=messages[0]['ReceiptHandle'],
                **host_data,
            )
        return None

    def delete_drain_messages(self, hosts: Sequence[Host]) -> None:
        for host in hosts:
            self.client.delete_message(
                QueueUrl=self.drain_queue_url,
                ReceiptHandle=host.receipt_handle,
            )

    def delete_terminate_messages(self, hosts: Sequence[Host]) -> None:
        for host in hosts:
            self.client.delete_message(
                QueueUrl=self.termination_queue_url,
                ReceiptHandle=host.receipt_handle,
            )

    def delete_warning_messages(self, hosts: Sequence[Host]) -> None:
        if self.warning_queue_url is None:
            return
        for host in hosts:
            self.client.delete_message(
                QueueUrl=self.warning_queue_url,
                ReceiptHandle=host.receipt_handle,
            )

    def process_termination_queue(
        self,
        mesos_operator_client: Optional[Callable[..., Callable[[str], Callable[..., None]]]],
        kube_operator_client: Optional[KubernetesClusterConnector],
    ) -> None:
        host_to_terminate = self.get_host_to_terminate()
        if host_to_terminate:
            # as for draining if it has a hostname we should down + up around the termination
            if host_to_terminate.scheduler == 'mesos':
                logger.info(f'Mesos hosts to down+terminate+up: {host_to_terminate}')
                hostname_ip = f'{host_to_terminate.hostname}|{host_to_terminate.ip}'
                try:
                    down(mesos_operator_client, [hostname_ip])
                except Exception as e:
                    logger.error(f'Failed to down {hostname_ip} continuing to terminate anyway: {e}')
                terminate_host(host_to_terminate)
                try:
                    up(mesos_operator_client, [hostname_ip])
                except Exception as e:
                    logger.error(f'Failed to up {hostname_ip} continuing to terminate anyway: {e}')
            elif host_to_terminate.scheduler == 'kubernetes':
                logger.info(f'Kubernetes hosts to delete k8s node and terminate: {host_to_terminate}')
                try:
                    logger.info(f'Deleting kubernetes node')
                    kube_delete_node(kube_operator_client, host_to_terminate)
                except Exception as e:
                    logger.warning(f'Failed to delete {host_to_terminate.ip} node continuing to terminate anyway: {e}')
                terminate_host(host_to_terminate)
            else:
                logger.info(f'Host to terminate immediately: {host_to_terminate}')
                terminate_host(host_to_terminate)
            self.delete_terminate_messages([host_to_terminate])

    def process_drain_queue(
        self,
        mesos_operator_client: Optional[Callable[..., Callable[[str], Callable[..., None]]]],
        kube_operator_client: Optional[KubernetesClusterConnector],
    ) -> None:
        host_to_process = self.get_host_to_drain()
        if host_to_process and host_to_process.instance_id not in self.draining_host_ttl_cache:
            self.draining_host_ttl_cache[host_to_process.instance_id] = arrow.now().shift(seconds=DRAIN_CACHE_SECONDS)
            if host_to_process.scheduler == 'mesos':
                logger.info(f'Mesos host to drain and submit for termination: {host_to_process}')
                try:
                    drain(
                        mesos_operator_client,
                        [f'{host_to_process.hostname}|{host_to_process.ip}'],
                        arrow.now().timestamp * 1000000000,
                        staticconf.read_int('mesos_maintenance_timeout_seconds', default=600) * 1000000000
                    )
                except Exception as e:
                    logger.error(f'Failed to drain {host_to_process.hostname} continuing to terminate anyway: {e}')
                finally:
                    self.submit_host_for_termination(host_to_process)
            elif host_to_process.scheduler == 'kubernetes':
                logger.info(f'Kubernetes host to drain and submit for termination: {host_to_process}')
                kube_drain(kube_operator_client, host_to_process)
                self.submit_host_for_termination(host_to_process, delay=0)
            else:
                logger.info(f'Host to submit for termination immediately: {host_to_process}')
                self.submit_host_for_termination(host_to_process, delay=0)
            self.delete_drain_messages([host_to_process])
        elif host_to_process:
            logger.warning(f'Host: {host_to_process.hostname} already being processed, skipping...')
            self.delete_drain_messages([host_to_process])

    def clean_processing_hosts_cache(self) -> None:
        hosts_to_remove = []
        for instance_id, expiration_time in self.draining_host_ttl_cache.items():
            if arrow.now() > expiration_time:
                hosts_to_remove.append(instance_id)
        for host in hosts_to_remove:
            del self.draining_host_ttl_cache[host]

    def process_warning_queue(self) -> None:
        host_to_process = self.get_warned_host()
        if host_to_process:
            logger.info(f'Processing spot warning for {host_to_process.hostname}')
            spot_fleet_resource_groups = []
            for pool in get_pool_name_list(self.cluster, 'mesos'):  # draining only supported for Mesos clusters
                pool_config = staticconf.NamespaceReaders(POOL_NAMESPACE.format(pool=pool, scheduler='mesos'))
                for resource_group_conf in pool_config.read_list('resource_groups'):
                    spot_fleet_resource_groups.extend(list(SpotFleetResourceGroup.load(
                        cluster=self.cluster,
                        pool=pool,
                        config=list(resource_group_conf.values())[0],
                    ).keys()))

            # we should definitely ignore termination warnings that aren't from this
            # cluster or maybe not even paasta instances...
            if host_to_process.group_id in spot_fleet_resource_groups:
                logger.info(f'Sending spot warned host to drain: {host_to_process.hostname}')
                self.submit_host_for_draining(host_to_process)
            else:
                logger.info(f'Ignoring spot warned host because not in our SFRs: {host_to_process.hostname}')
            self.delete_warning_messages([host_to_process])


def host_from_instance_id(
    sender: str,
    receipt_handle: str,
    instance_id: str,
) -> Optional[Host]:
    instance_data = ec2_describe_instances(instance_ids=[instance_id])
    if not instance_data:
        logger.warning(f'No instance data found for {instance_id}')
        return None
    try:
        sfr_ids = [tag['Value'] for tag in instance_data[0]['Tags'] if tag['Key'] == 'aws:ec2spot:fleet-request-id']
        scheduler = 'mesos'
        for tag in instance_data[0]['Tags']:
            if tag['Key'] == 'KubernetesCluster':
                scheduler = 'kubernetes'
                break
    except KeyError as e:
        logger.warning(f'SFR tag key not found: {e}')
        sfr_ids = []
    if not sfr_ids:
        logger.warning(f'No SFR ID found for {instance_id}')
        return None
    try:
        ip = instance_data[0]['PrivateIpAddress']
    except KeyError:
        logger.warning(f'No primary IP found for {instance_id}')
        return None
    try:
        hostnames = socket.gethostbyaddr(ip)
    except socket.error:
        logger.warning(f"Couldn't derive hostname from IP via DNS for {ip}")
        return None
    return Host(
        sender=sender,
        receipt_handle=receipt_handle,
        instance_id=instance_id,
        hostname=hostnames[0],
        group_id=sfr_ids[0],
        ip=ip,
        scheduler=scheduler,
    )


def process_queues(cluster_name: str) -> None:
    draining_client = DrainingClient(cluster_name)
    cluster_manager_name = staticconf.read_string(f'clusters.{cluster_name}.cluster_manager')
    mesos_operator_client = kube_operator_client = None
    try:
        kube_operator_client = KubernetesClusterConnector(cluster_name, None)
    except Exception:
        logger.error(f'Cluster specified is mesos specific. Skipping kubernetes operator')
    if cluster_manager_name == 'mesos':
        try:
            mesos_master_url = staticconf.read_string(f'clusters.{cluster_name}.mesos_master_fqdn')
            mesos_secret_path = staticconf.read_string(f'mesos.mesos_agent_secret_path', default=None)
            mesos_operator_client = operator_api(mesos_master_url, mesos_secret_path)
        except Exception:
            logger.error('Cluster specified is kubernetes specific. Skipping mesos operator')

    logger.info('Polling SQS for messages every 5s')
    while True:
        draining_client.clean_processing_hosts_cache()
        draining_client.process_warning_queue()
        draining_client.process_drain_queue(
            mesos_operator_client=mesos_operator_client,
            kube_operator_client=kube_operator_client,
        )
        draining_client.process_termination_queue(
            mesos_operator_client=mesos_operator_client,
            kube_operator_client=kube_operator_client,
        )
        time.sleep(5)


def terminate_host(host: Host) -> None:
    logger.info(f'Terminating: {host.instance_id}')
    resource_group_class = RESOURCE_GROUPS[host.sender]
    resource_group = resource_group_class(host.group_id)
    resource_group.terminate_instances_by_id([host.instance_id])


def main(args: argparse.Namespace) -> None:
    setup_config(args)
    for pool in get_pool_name_list(args.cluster, 'mesos'):
        load_cluster_pool_config(args.cluster, pool, 'mesos', None)
    for pool in get_pool_name_list(args.cluster, 'kubernetes'):
        load_cluster_pool_config(args.cluster, pool, 'kubernetes', None)
    process_queues(args.cluster)


@subparser('drain', 'Drains and terminates instances submitted to SQS by clusterman', main)
def add_queue_parser(
    subparser: argparse.ArgumentParser,
    required_named_args: argparse.Namespace,
    optional_named_args: argparse.Namespace
) -> None:
    add_cluster_arg(required_named_args, required=True)
