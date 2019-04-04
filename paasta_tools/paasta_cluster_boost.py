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

from paasta_tools.autoscaling import load_boost
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print

log = logging.getLogger(__name__)


def parse_args():
    """Parses the command line arguments passed to this script"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p', '--pool',
        type=str,
        default='default',
        help="Name of the pool you want to increase the capacity. Default is 'default' pool.",
    )
    parser.add_argument(
        '-b', '--boost',
        type=float,
        default=load_boost.DEFAULT_BOOST_FACTOR,
        help="Boost factor to apply. Default is 1.5. A big failover should be 2, 3 is the max.",
    )
    parser.add_argument(
        '-d', '--duration',
        type=int,
        default=load_boost.DEFAULT_BOOST_DURATION,
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
        help="You can view the status, set or clear a boost.",
    )
    parser.add_argument(
        '-v', '--verbose',
        action='count',
        dest="verbose",
        default=0,
        help="Print out more output.",
    )
    return parser.parse_args()


def get_regions(pool: str) -> list:
    """ Return the regions where we have slaves running for a given pool
    """
    system_paasta_config = load_system_paasta_config()
    expected_slave_attributes = system_paasta_config.get_expected_slave_attributes()
    if expected_slave_attributes is None:
        return []

    regions = []  # type: list
    for slave in expected_slave_attributes:
        slave_region = slave['datacenter']
        if slave['pool'] == pool:
            if slave_region not in regions:
                regions.append(slave_region)
    return regions


def paasta_cluster_boost(
    action: str,
    pool: str,
    boost: float,
    duration: int,
    override: bool,
) -> bool:
    """ Set, Get or clear a boost on a paasta cluster for a given pool in a given region
    :returns: None
    """
    system_config = load_system_paasta_config()

    if not system_config.get_cluster_boost_enabled():
        paasta_print('ERROR: cluster_boost feature is not enabled.')
        return False

    regions = get_regions(pool)

    if len(regions) == 0:
        paasta_print(f'ERROR: no slaves found in pool {pool}')
        return False

    for region in regions:
        zk_boost_path = load_boost.get_zk_cluster_boost_path(
            region=region,
            pool=pool,
        )
        if action == 'set':
            if not load_boost.set_boost_factor(
                zk_boost_path=zk_boost_path,
                region=region,
                pool=pool,
                factor=boost,
                duration_minutes=duration,
                override=override,
            ):
                paasta_print(f'ERROR: Failed to set the boost for pool {pool}, region {region}.')
                return False

        elif action == 'status':
            pass

        elif action == 'clear':
            if not load_boost.clear_boost(
                zk_boost_path,
                region=region,
                pool=pool,
            ):
                paasta_print('ERROR: Failed to clear the boost for pool {}, region {}.')
                return False

        else:
            raise NotImplementedError("Action: '%s' is not implemented." % action)
            return False

        paasta_print('Current boost value for path: {}: {}'.format(
            zk_boost_path, load_boost.get_boost_factor(
                zk_boost_path=zk_boost_path,
            ),
        ))
    return True


def main() -> bool:
    args = parse_args()

    if args.verbose >= 2:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose == 1:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    if paasta_cluster_boost(
        action=args.action,
        pool=args.pool,
        boost=args.boost,
        duration=args.duration,
        override=args.override,
    ):
        sys.exit(0)
    sys.exit(1)


if __name__ == '__main__':
    main()
