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
from getpass import getuser
from socket import gethostname

import arrow
import staticconf

from clusterman.args import add_cluster_arg
from clusterman.args import add_cluster_config_directory_arg
from clusterman.args import add_pool_arg
from clusterman.args import add_scheduler_arg
from clusterman.args import subparser
from clusterman.autoscaler.pool_manager import PoolManager
from clusterman.cli.util import timeout_wrapper
from clusterman.config import POOL_NAMESPACE
from clusterman.util import ask_for_confirmation
from clusterman.util import get_autoscaler_scribe_stream
from clusterman.util import log_to_scribe

LOG_TEMPLATE = f'{arrow.now()} {gethostname()} {__name__}'


def get_target_capacity_value(target_capacity: str, pool: str, scheduler: str) -> int:
    target_capacity = target_capacity.lower()
    pool_namespace = POOL_NAMESPACE.format(pool=pool, scheduler=scheduler)
    if target_capacity == 'min':
        return staticconf.read_int('scaling_limits.min_capacity', namespace=pool_namespace)
    elif target_capacity == 'max':
        return staticconf.read_int('scaling_limits.max_capacity', namespace=pool_namespace)
    else:
        return int(target_capacity)


def change_target_capacity(manager: PoolManager, target_capacity: str, dry_run: bool) -> str:
    old_target = manager.target_capacity
    requested_target = get_target_capacity_value(target_capacity, manager.pool, manager.scheduler)
    if not dry_run and not ask_for_confirmation(
        f'Modifying target capacity for {manager.cluster}, {manager.pool}.{manager.scheduler} '
        f'from {old_target} to {requested_target}.  Proceed? '
    ):
        print('Aborting operation.')
        return ''

    new_target = manager.modify_target_capacity(requested_target, dry_run)
    return (
        f'Target capacity for {manager.pool}.{manager.scheduler} on {manager.cluster} manually changed '
        f'from {old_target} to {new_target} by {getuser()}'
    )


def mark_stale(manager: PoolManager, dry_run: bool) -> str:
    if not dry_run and not ask_for_confirmation(
        f'Marking all resource groups in {manager.cluster}, {manager.pool}.{manager.scheduler} stale.  Proceed? '
    ):
        print('Aborting operation.')
        return ''

    manager.mark_stale(dry_run)
    return (
        f'All resource groups in {manager.pool}.{manager.scheduler} on {manager.cluster} manually '
        f'marked as stale by {getuser()}'
    )


@timeout_wrapper
def main(args: argparse.Namespace) -> None:
    if args.target_capacity and args.mark_stale:
        raise ValueError('Cannot specify --target-capacity and --mark-stale simultaneously')

    manager = PoolManager(args.cluster, args.pool, args.scheduler)
    log_messages = []
    if args.target_capacity:
        log_message = change_target_capacity(manager, args.target_capacity, args.dry_run)
        log_messages.append(log_message)

    elif args.mark_stale:
        log_message = mark_stale(manager, args.dry_run)
        log_messages.append(log_message)

    for log_message in log_messages:
        if not log_message:
            continue

        print(log_message)
        if not args.dry_run:
            scribe_stream = get_autoscaler_scribe_stream(args.cluster, args.pool, args.scheduler)
            log_to_scribe(scribe_stream, f'{LOG_TEMPLATE} {log_message}')


@subparser('manage', 'check the status of a cluster', main)
def add_mesos_manager_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    add_cluster_arg(required_named_args, required=True)
    add_pool_arg(required_named_args)
    add_scheduler_arg(required_named_args)
    optional_named_args.add_argument(
        '--target-capacity',
        metavar='X',
        help='New target capacity for the cluster (valid options: min, max, positive integer)',
    )
    optional_named_args.add_argument(
        '--mark-stale',
        action='store_true',
        help=(
            'Mark the resource groups of a cluster as "stale" (ASGs only); these resource groups '
            'will no longer contribute to the pool\'s target capacity.'
        ),
    )
    optional_named_args.add_argument(
        '--dry-run',
        action='store_true',
        help='Just print what would happen, don\'t actually add or remove instances'
    )
    add_cluster_config_directory_arg(optional_named_args)
