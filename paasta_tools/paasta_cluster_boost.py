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
import argparse
import logging
import sys

from paasta_tools.autoscaling import cluster_boost
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print

log = logging.getLogger(__name__)


def parse_args():
    """Parses the command line arguments passed to this script"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-r', '--region',
        type=str,
        required=True,
        help="name of the AWS region where the pool is. eg: us-east-1",
    )
    parser.add_argument(
        '-p', '--pool',
        type=str,
        default='default',
        help="Name of the pool you want to increase the capacity. Default is 'default' pool.",
    )
    parser.add_argument(
        '-b', '--boost',
        type=float,
        default=cluster_boost.DEFAULT_BOOST_FACTOR,
        help="Boost factor to apply. Default is 1.5. A big failover should be 2, 3 is the max.",
    )
    parser.add_argument(
        '-d', '--duration',
        type=int,
        default=cluster_boost.DEFAULT_BOOST_DURATION,
        help="Duration of the capacity boost in minutes. Default is 40min.",
    )
    parser.add_argument(
        '-f', '--force',
        action='store_true',
        dest='override',
        help="Replace an existing boost. Default is false",
    )
    parser.add_argument(
        'action',
        choices=[
            'set',
            'status',
            'clear',
        ],
        help="You can set, get or clear a boost.",
    )
    parser.add_argument(
        '-v', '--verbose',
        action='count',
        dest="verbose",
        default=0,
        help="Print out more output.",
    )
    return parser.parse_args()


def check_pool_exist(pool: str, region: str) -> bool:
    """ Check that we have slaves in the provided pool and region
    """
    system_paasta_config = load_system_paasta_config()
    expected_slave_attributes = system_paasta_config.get_expected_slave_attributes()
    if expected_slave_attributes is None:
        return False

    region_pool_pairs = []
    for slave in expected_slave_attributes:
        slave_pool = slave['pool']
        slave_region = slave['datacenter']
        region_pool_pair = (slave_region, slave_pool)
        if region_pool_pair not in region_pool_pairs:
            region_pool_pairs.append(region_pool_pair)

    return (region, pool) in region_pool_pairs


def paasta_cluster_boost():
    """ Set, Get or clear a boost on a paasta cluster for a given pool in a given region
    :returns: None
    """
    args = parse_args()

    if args.verbose >= 2:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose == 1:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    action = args.action
    region = args.region
    pool = args.pool

    if action == 'set':
        # Let's disallow people to set boost on a non existing pool
        if not check_pool_exist(pool=pool, region=region):
            paasta_print('Could not find the pool {} in the region {}'.format(pool, region))
            return False
        if not cluster_boost.set_boost_factor(
            region=region,
            pool=pool,
            factor=args.boost,
            duration_minutes=args.duration_minutes,
            override=args.override,
        ):
            paasta_print('Failed! Check the logs.')
            return False

    elif action == 'get':
        paasta_print('Current boost value: {}'.format(cluster_boost.get_boosted_load(
            region=region,
            pool=pool,
            current_load=1.0,
        )))

    elif action == 'clear':
        if not cluster_boost.clear_boost(pool=pool, region=region):
            paasta_print('Failed! Check the logs.')
            return False

    else:
        raise NotImplementedError("Action: '%s' is not implemented." % action)
        return False


if __name__ == '__main__':
    if paasta_cluster_boost():
        sys.exit(0)
    sys.exit(1)
