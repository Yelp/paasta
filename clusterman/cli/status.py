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
import sys

from humanfriendly import format_size
from humanfriendly import format_timespan

from clusterman.args import add_cluster_arg
from clusterman.args import add_cluster_config_directory_arg
from clusterman.args import add_pool_arg
from clusterman.args import add_scheduler_arg
from clusterman.args import subparser
from clusterman.autoscaler.pool_manager import ClusterNodeMetadata
from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.interfaces.cluster_connector import AgentState
from clusterman.util import any_of
from clusterman.util import color_conditions


def _write_resource_group_line(group) -> None:
    # TODO (CLUSTERMAN-100) These are just the status responses for spot fleets; this probably won't
    # extend to other types of resource groups, so we should figure out what to do about that.
    status_str = color_conditions(
        group.status,
        green=any_of('active',),
        blue=any_of('modifying', 'submitted'),
        red=any_of('cancelled', 'failed', 'cancelled_running', 'cancelled_terminating'),
    )
    print(f'\t{group.id}: {status_str} ({group.fulfilled_capacity} / {group.target_capacity})')


def _write_agent_details(node_metadata: ClusterNodeMetadata) -> None:
    agent_aws_state = color_conditions(
        node_metadata.instance.state,
        green=any_of('running',),
        blue=any_of('pending',),
        red=any_of('shutting-down', 'terminated', 'stopping', 'stopped'),
    )
    print(
        f'\t - {node_metadata.instance.instance_id} {node_metadata.instance.market} '
        f'({node_metadata.instance.ip_address}): {agent_aws_state}, up for '
        f'{format_timespan(node_metadata.instance.uptime.total_seconds(), max_units=1)}'
    )

    agent_mesos_state = color_conditions(
        node_metadata.agent.state,
        green=any_of(AgentState.RUNNING,),
        blue=any_of(AgentState.IDLE,),
        red=any_of(AgentState.ORPHANED, AgentState.UNKNOWN),
    )
    sys.stdout.write(f'\t   {agent_mesos_state} ')

    if node_metadata.agent.state == AgentState.RUNNING:
        colored_resources = [
            (
                color_conditions(
                    int(allocated / total * 100),
                    postfix='%',
                    green=lambda x: x <= 90,
                    yellow=lambda x: x <= 95,
                    red=lambda x: x > 95,
                )
                if total
                else 'None'
            )
            for (allocated, total) in zip(node_metadata.agent.allocated_resources, node_metadata.agent.total_resources)
        ]

        sys.stdout.write(
            f'{node_metadata.agent.task_count} tasks; '
            f'CPUs: {colored_resources[0]}, '
            f'Mem: {colored_resources[1]}, '
            f'Disk: {colored_resources[2]}, '
            f'GPUs: {colored_resources[3]}'
        )
    sys.stdout.write('\n')


def _write_summary(manager: PoolManager) -> None:
    print('Cluster statistics:')
    total_cpus = manager.cluster_connector.get_resource_total('cpus')
    total_mem = format_size(manager.cluster_connector.get_resource_total('mem') * 1000000)
    total_disk = format_size(manager.cluster_connector.get_resource_total('disk') * 1000000)
    total_gpus = manager.cluster_connector.get_resource_total('gpus')
    allocated_cpus = manager.cluster_connector.get_resource_allocation('cpus')
    allocated_mem = format_size(manager.cluster_connector.get_resource_allocation('mem') * 1000000)
    allocated_disk = format_size(manager.cluster_connector.get_resource_allocation('disk') * 1000000)
    allocated_gpus = manager.cluster_connector.get_resource_allocation('gpus')
    print(f'\tCPU allocation: {allocated_cpus:.1f} CPUs allocated to tasks, {total_cpus:.1f} total')
    print(f'\tMemory allocation: {allocated_mem} memory allocated to tasks, {total_mem} total')
    print(f'\tDisk allocation: {allocated_disk} disk space allocated to tasks, {total_disk} total')
    print(f'\tGPUs allocation: {allocated_gpus} GPUs allocated to tasks, {total_gpus} total')


def print_status(manager: PoolManager, args) -> None:
    sys.stdout.write('\n')
    print(f'Current status for the {manager.pool} pool in the {manager.cluster} cluster:\n')
    print(
        f'Resource groups (target capacity: {manager.target_capacity}, fulfilled: {manager.fulfilled_capacity}, '
        f'non-orphan: {manager.non_orphan_fulfilled_capacity}):'
    )

    node_metadatas = manager.get_node_metadatas() if args.verbose else {}

    for group in manager.resource_groups.values():
        _write_resource_group_line(group)
        for metadata in node_metadatas:
            if (metadata.instance.group_id != group.id or
                    (args.only_orphans and metadata.agent.state != AgentState.ORPHANED) or
                    (args.only_idle and metadata.agent.state != AgentState.IDLE)):
                continue
            _write_agent_details(metadata)

        sys.stdout.write('\n')

    _write_summary(manager)
    sys.stdout.write('\n')


def main(args: argparse.Namespace) -> None:  # pragma: no cover
    manager = PoolManager(args.cluster, args.pool, args.scheduler)
    print_status(manager, args)


@subparser('status', 'check the status of a cluster', main)
def add_mesos_status_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    add_cluster_arg(required_named_args, required=True)
    add_pool_arg(required_named_args)
    add_scheduler_arg(required_named_args)

    optional_named_args.add_argument(
        '--only-idle',
        action='store_true',
        help='Only show information about idle agents'
    )
    optional_named_args.add_argument(
        '--only-orphans',
        action='store_true',
        help='Only show information about orphaned instances (instances that are not in the Mesos cluster)'
    )
    optional_named_args.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Show more detailed status information',
    )
    add_cluster_config_directory_arg(optional_named_args)
