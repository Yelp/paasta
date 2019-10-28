import argparse

import staticconf

from clusterman.args import add_cluster_arg
from clusterman.args import add_cluster_config_directory_arg
from clusterman.args import add_pool_arg
from clusterman.args import add_scheduler_arg
from clusterman.args import subparser
from clusterman.autoscaler.autoscaler import AUTOSCALER_PAUSED
from clusterman.autoscaler.autoscaler import CLUSTERMAN_STATE_TABLE
from clusterman.aws.client import dynamodb
from clusterman.util import parse_time_string


def disable(args: argparse.Namespace) -> None:
    state = {
        'state': {'S': AUTOSCALER_PAUSED},
        'entity': {'S': f'{args.cluster}.{args.pool}.{args.scheduler}'},
    }
    if args.until:
        state['expiration_timestamp'] = {'N': str(parse_time_string(args.until).timestamp)}

    dynamodb.put_item(
        TableName=staticconf.read('aws.state_table', default=CLUSTERMAN_STATE_TABLE),
        Item=state,
    )


def enable(args: argparse.Namespace) -> None:
    dynamodb.delete_item(
        TableName=staticconf.read('aws.state_table', default=CLUSTERMAN_STATE_TABLE),
        Key={
            'state': {'S': AUTOSCALER_PAUSED},
            'entity': {'S': f'{args.cluster}.{args.pool}.{args.scheduler}'},
        }
    )


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
