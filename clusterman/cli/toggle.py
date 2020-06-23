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
import time

import staticconf

from clusterman.args import add_cluster_arg
from clusterman.args import add_cluster_config_directory_arg
from clusterman.args import add_pool_arg
from clusterman.args import add_scheduler_arg
from clusterman.args import subparser
from clusterman.aws.client import dynamodb
from clusterman.util import AUTOSCALER_PAUSED
from clusterman.util import autoscaling_is_paused
from clusterman.util import CLUSTERMAN_STATE_TABLE
from clusterman.util import parse_time_string


def disable(args: argparse.Namespace) -> None:
    state = {
        'state': {'S': AUTOSCALER_PAUSED},
        'entity': {'S': f'{args.cluster}.{args.pool}.{args.scheduler}'},
        'timestamp': {'N':  str(int(time.time()))},
    }
    if args.until:
        state['expiration_timestamp'] = {'N': str(parse_time_string(args.until).timestamp)}

    dynamodb.put_item(
        TableName=staticconf.read('aws.state_table', default=CLUSTERMAN_STATE_TABLE),
        Item=state,
    )

    time.sleep(1)  # Give DynamoDB some time to settle
    now = parse_time_string('now').to('local')
    if not autoscaling_is_paused(args.cluster, args.pool, args.scheduler, now):
        print('Something went wrong!  The autoscaler is NOT paused')
    else:
        s = f'The autoscaler for {args.cluster}.{args.pool}.{args.scheduler} was paused at {now}'
        if args.until:
            until_str = str(parse_time_string(args.until).to('local'))
            s += f' until {until_str}'
        print(s)


def enable(args: argparse.Namespace) -> None:
    dynamodb.delete_item(
        TableName=staticconf.read('aws.state_table', default=CLUSTERMAN_STATE_TABLE),
        Key={
            'state': {'S': AUTOSCALER_PAUSED},
            'entity': {'S': f'{args.cluster}.{args.pool}.{args.scheduler}'},
        }
    )
    time.sleep(1)  # Give DynamoDB some time to settle
    now = parse_time_string('now').to('local')
    if autoscaling_is_paused(args.cluster, args.pool, args.scheduler, now):
        print('Something went wrong!  The autoscaler is paused')
    else:
        print(f'The autoscaler for {args.cluster}.{args.pool}.{args.scheduler} was enabled at {now}')


@subparser('disable', 'temporarily turn the autoscaler for a cluster off', disable)
def add_cluster_disable_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    add_cluster_arg(required_named_args, required=True)
    add_pool_arg(required_named_args)
    add_scheduler_arg(required_named_args)
    optional_named_args.add_argument(
        '--until',
        metavar='timestamp',
        help='time at which to re-enable autoscaling (try "tomorrow", "+5m"; use quotes)',
    )
    add_cluster_config_directory_arg(optional_named_args)


@subparser('enable', 'turn the autoscaler for a cluster back on', enable)
def add_cluster_enable_parser(subparser, required_named_args, optional_named_args):  # pragma: no cover
    add_cluster_arg(required_named_args, required=True)
    add_pool_arg(required_named_args)
    add_scheduler_arg(required_named_args)
    add_cluster_config_directory_arg(optional_named_args)
