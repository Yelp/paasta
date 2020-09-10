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
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence

import arrow
import simplejson as json
from colorama import Fore
from colorama import Style
from humanfriendly import format_size
from humanfriendly import format_timespan
from mypy_extensions import TypedDict

from clusterman.args import add_cluster_arg
from clusterman.args import add_cluster_config_directory_arg
from clusterman.args import add_json_arg
from clusterman.args import add_pool_arg
from clusterman.args import add_scheduler_arg
from clusterman.args import subparser
from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.aws.markets import InstanceMarket
from clusterman.cli.util import timeout_wrapper
from clusterman.interfaces.resource_group import ResourceGroup
from clusterman.interfaces.types import AgentState
from clusterman.interfaces.types import ClusterNodeMetadata
from clusterman.util import any_of
from clusterman.util import autoscaling_is_paused
from clusterman.util import ClustermanResources
from clusterman.util import color_conditions


class ResourceDictJsonObject(TypedDict):
    allocated: float
    total: float


class AgentJsonObject(TypedDict):
    agent_state: AgentState
    aws_state: str
    instance_id: str
    ip_address: Optional[str]
    market: InstanceMarket
    task_count: int
    uptime: float
    resources: Mapping[str, ResourceDictJsonObject]


class ResourceGroupJsonObject(TypedDict):
    fulfilled_capacity: float
    id: str
    status: str
    target_capacity: float
    agents: List[AgentJsonObject]


class StatusJsonObject(TypedDict):
    disabled: bool
    fulfilled_capacity: float
    target_capacity: float
    non_orphan_fulfilled_capacity: float
    resource_groups: List[ResourceGroupJsonObject]


def _get_agent_json(metadata: ClusterNodeMetadata) -> AgentJsonObject:
    return {
        'agent_state': metadata.agent.state,
        'aws_state': metadata.instance.state,
        'instance_id': metadata.instance.instance_id,
        'ip_address': metadata.instance.ip_address,
        'market': metadata.instance.market,
        'task_count': metadata.agent.task_count,
        'uptime': metadata.instance.uptime.total_seconds(),
        'resources': {
            resource: {
                'allocated': getattr(metadata.agent.allocated_resources, resource),
                'total': getattr(metadata.agent.total_resources, resource),
            }
            for resource in ClustermanResources._fields
        }
    }


def _get_resource_groups_json(
    groups: Iterable[ResourceGroup],
    node_metadatas: Sequence[ClusterNodeMetadata],
) -> List[ResourceGroupJsonObject]:
    return [
        {
            'id': group.id,
            'fulfilled_capacity': group.fulfilled_capacity,
            'target_capacity': group.target_capacity,
            'status': group.status,
            'agents': [
                _get_agent_json(metadata)
                for metadata in node_metadatas
                if metadata.instance.group_id == group.id
            ]
        }
        for group in groups
    ]


def _status_json(manager: PoolManager, get_node_metadatas: bool) -> StatusJsonObject:
    node_metadatas = manager.get_node_metadatas() if get_node_metadatas else []
    return {
        'disabled': autoscaling_is_paused(manager.cluster, manager.pool, manager.scheduler, arrow.now()),
        'target_capacity': manager.target_capacity,
        'fulfilled_capacity': manager.fulfilled_capacity,
        'non_orphan_fulfilled_capacity': manager.non_orphan_fulfilled_capacity,
        'resource_groups': _get_resource_groups_json(manager.resource_groups.values(), node_metadatas),
    }


def _write_resource_group_line(group) -> None:
    # TODO (CLUSTERMAN-100) These are just the status responses for spot fleets; this probably won't
    # extend to other types of resource groups, so we should figure out what to do about that.
    status_str = color_conditions(
        group['status'],
        green=any_of('active',),
        blue=any_of('modifying', 'submitted'),
        red=any_of('cancelled', 'failed', 'cancelled_running', 'cancelled_terminating'),
    )
    print(f'\t{group["id"]}: {status_str} ({group["fulfilled_capacity"]} / {group["target_capacity"]})')


def _write_agent_details(agent: AgentJsonObject) -> None:
    agent_aws_state = color_conditions(
        agent['aws_state'],
        green=any_of('running',),
        blue=any_of('pending',),
        red=any_of('shutting-down', 'terminated', 'stopping', 'stopped'),
    )
    print(
        f'\t - {agent["instance_id"]} {agent["market"]} '
        f'({agent["ip_address"]}): {agent_aws_state}, up for '
        f'{format_timespan(agent["uptime"], max_units=1)}'
    )

    agent_state = color_conditions(
        agent['agent_state'],
        green=any_of(AgentState.RUNNING,),
        blue=any_of(AgentState.IDLE,),
        red=any_of(AgentState.ORPHANED, AgentState.UNKNOWN),
    )
    sys.stdout.write(f'\t   {agent_state} ')

    if agent['agent_state'] == AgentState.RUNNING:
        output_str = f'{agent["task_count"]} tasks; '
        resource_strings = []
        for resource in ClustermanResources._fields:
            allocated, total = agent['resources'][resource]['allocated'], agent['resources'][resource]['total']
            if total:
                resource_strings.append(
                    resource + ': ' +
                    color_conditions(
                        int(allocated / total * 100),
                        postfix='%',
                        green=lambda x: x <= 90,
                        yellow=lambda x: x <= 95,
                        red=lambda x: x > 95,
                    )
                )
            else:
                resource_strings.append(resource + ': None')

        sys.stdout.write(output_str + ', '.join(resource_strings))
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


def print_status_json(manager: PoolManager):
    print(json.dumps(_status_json(manager, get_node_metadatas=True), default=str))


def print_status(manager: PoolManager, args: argparse.Namespace) -> None:
    status_obj = _status_json(manager, get_node_metadatas=args.verbose)
    sys.stdout.write('\n')
    print(f'Current status for the {manager.pool} pool in the {manager.cluster} cluster:\n')
    if status_obj['disabled']:
        print(Fore.RED + 'Autoscaling is currently PAUSED!!!\n' + Style.RESET_ALL)

    print(
        f'Resource groups (target capacity: {status_obj["target_capacity"]}, '
        f'fulfilled: {status_obj["fulfilled_capacity"]}, '
        f'non-orphan: {status_obj["non_orphan_fulfilled_capacity"]}):'
    )

    for group in status_obj['resource_groups']:
        _write_resource_group_line(group)
        for metadata in group['agents']:
            if ((args.only_orphans and metadata['agent_state'] != AgentState.ORPHANED) or
                    (args.only_idle and metadata['agent_state'] != AgentState.IDLE)):
                continue
            _write_agent_details(metadata)

        sys.stdout.write('\n')

    _write_summary(manager)
    sys.stdout.write('\n')


@timeout_wrapper
def main(args: argparse.Namespace) -> None:  # pragma: no cover
    manager = PoolManager(args.cluster, args.pool, args.scheduler)
    if args.json:
        print_status_json(manager)
    else:
        print_status(manager, args)


@subparser('status', 'check the status of a cluster', main)
def add_status_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    add_cluster_arg(required_named_args, required=True)
    add_pool_arg(required_named_args)
    add_scheduler_arg(required_named_args)

    optional_named_args.add_argument(
        '--only-idle',
        action='store_true',
        help='Only show information about idle agents',
    )
    optional_named_args.add_argument(
        '--only-orphans',
        action='store_true',
        help='Only show information about orphaned instances (instances that are not in the Mesos cluster)',
    )
    optional_named_args.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Show more detailed status information (implies -v, ignores --only-idle and --only-orphans)',
    )
    add_json_arg(optional_named_args)
    add_cluster_config_directory_arg(optional_named_args)
