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
from collections import defaultdict

import arrow
import mock
import pytest

from clusterman.aws.markets import InstanceMarket
from clusterman.math.piecewise import PiecewiseConstantFunction
from clusterman.simulator.simulated_spot_fleet_resource_group import SimulatedSpotFleetResourceGroup


MARKETS = [
    InstanceMarket('c3.4xlarge', 'us-west-1a'),
    InstanceMarket('c3.4xlarge', 'us-west-1b'),
    InstanceMarket('i2.8xlarge', 'us-west-2a'),
    InstanceMarket('m4.4xlarge', 'us-west-2b'),
]


@pytest.fixture
def spot_fleet_request_config():
    return {
        'AllocationStrategy': 'diversified',
        'LaunchSpecifications': [
            {
                'InstanceType': 'c3.4xlarge',
                'SpotPrice': 1.01,
                'WeightedCapacity': 1,
                'SubnetId': 'us-west-1a',
            },
            {
                'InstanceType': 'c3.4xlarge',
                'SpotPrice': 1.01,
                'WeightedCapacity': 2,
                'SubnetId': 'us-west-1b',
            },
            {
                'InstanceType': 'i2.8xlarge',
                'SpotPrice': 0.27,
                'WeightedCapacity': 3,
                'SubnetId': 'us-west-2a',
            },
            {
                'InstanceType': 'm4.4xlarge',
                'SpotPrice': 0.42,
                'WeightedCapacity': 0.5,
                'SubnetId': 'us-west-2b',
            },
        ],
    }


@pytest.fixture
def spot_prices():
    instance_price = defaultdict(lambda: PiecewiseConstantFunction())
    instance_price[MARKETS[0]].add_breakpoint(arrow.get(0), 0.5)
    instance_price[MARKETS[1]].add_breakpoint(arrow.get(0), 2.5)
    instance_price[MARKETS[2]].add_breakpoint(arrow.get(0), 0.1)
    instance_price[MARKETS[3]].add_breakpoint(arrow.get(0), 0.5)
    return instance_price


def get_fake_instance_market(spec):
    return InstanceMarket(spec['InstanceType'], spec['SubnetId'])


@pytest.fixture
def spot_fleet(spot_fleet_request_config, simulator, spot_prices):
    with mock.patch('clusterman.simulator.simulated_spot_fleet_resource_group.get_instance_market',
                    side_effect=get_fake_instance_market):
        s = SimulatedSpotFleetResourceGroup(spot_fleet_request_config, simulator)
    s.simulator.instance_prices = spot_prices
    return s


@pytest.fixture
def test_instances_by_market():
    return {MARKETS[0]: 1, MARKETS[1]: 1, MARKETS[2]: 3, MARKETS[3]: 4}


@pytest.mark.parametrize('residuals,result', [
    # no overflow -- all weights evenly divide residuals
    ([(MARKETS[0], 4), (MARKETS[3], 3)], {MARKETS[0]: 4.0, MARKETS[3]: 6.0}),
    # weight of MARKETS[0] does not divide its residual
    ([(MARKETS[0], 2.5), (MARKETS[3], 3), (MARKETS[1], 4.5)], {MARKETS[0]: 3.0, MARKETS[1]: 2.0, MARKETS[3]: 6.0}),
    # MARKETS[0] residual is covered by overflow
    ([(MARKETS[2], 7), (MARKETS[1], 5), (MARKETS[0], 1)], {MARKETS[1]: 2.0, MARKETS[2]: 3.0}),
    # MARKETS[0] residual goes negative because of overflow
    ([(MARKETS[1], 9), (MARKETS[2], 7), (MARKETS[0], 1), (MARKETS[3], 3)],
        {MARKETS[1]: 5.0, MARKETS[2]: 3.0, MARKETS[3]: 3.0}),
    # MARKET[0] residual is negative, MARKET[1] residual goes negative because of overflow
    ([(MARKETS[0], -6), (MARKETS[1], 1), (MARKETS[2], 3), (MARKETS[3], 6)],
        {MARKETS[2]: 1.0, MARKETS[3]: 2.0}),
])
def test_get_new_market_counts(residuals, result, spot_fleet):
    spot_fleet._find_available_markets = mock.Mock()
    spot_fleet._compute_market_residuals = mock.Mock(return_value=residuals)
    assert spot_fleet._get_new_market_counts(12345678) == result


def test_compute_market_residuals_new_fleet(spot_fleet, test_instances_by_market):
    target_capacity = 10
    residuals = spot_fleet._compute_market_residuals(target_capacity, test_instances_by_market.keys())
    assert residuals == list(zip(
        sorted(list(test_instances_by_market.keys()),
               key=lambda x: spot_fleet.simulator.instance_prices[x].call(spot_fleet.simulator.current_time)),
        [target_capacity / len(test_instances_by_market)] * len(test_instances_by_market)
    ))


def test_compute_market_residuals_existing_fleet(spot_fleet, test_instances_by_market):
    target_capacity = 20
    spot_fleet.modify_size(test_instances_by_market)
    residuals = spot_fleet._compute_market_residuals(target_capacity, test_instances_by_market.keys())
    assert residuals == [(MARKETS[2], -4), (MARKETS[3], 3), (MARKETS[1], 3), (MARKETS[0], 4)]


def test_market_capacities(spot_fleet_request_config, spot_fleet, test_instances_by_market):
    spot_fleet.modify_size(test_instances_by_market)
    for i, (market, instance_count) in enumerate(test_instances_by_market.items()):
        assert spot_fleet.market_capacities[market] == \
            instance_count * spot_fleet_request_config['LaunchSpecifications'][i]['WeightedCapacity']


def test_find_available_markets(spot_fleet):
    available_markets = spot_fleet._find_available_markets()
    assert len(available_markets) == 2
    assert MARKETS[0] in available_markets
    assert MARKETS[2] in available_markets


def test_terminate_instance(spot_fleet, test_instances_by_market):
    # The instances after the split point (including itself) will be terminated
    split_point = 2
    added_instances, __ = spot_fleet.modify_size(test_instances_by_market)
    for instance in spot_fleet.instances.values():
        instance.join_time = instance.start_time
    terminate_instances_ids = [instance.id for instance in added_instances[split_point:]]
    spot_fleet.terminate_instances_by_id(terminate_instances_ids)
    remain_instances = spot_fleet.instances
    assert len(remain_instances) == split_point
    for instance in added_instances[:split_point]:
        assert instance.id in remain_instances


@pytest.mark.parametrize('dry_run', [True, False])
def test_modify_target_capacity(dry_run, spot_fleet):
    spot_fleet.modify_target_capacity(10, dry_run=dry_run)
    capacity = spot_fleet.target_capacity
    spot_fleet.modify_target_capacity(capacity * 2, dry_run=dry_run)
    set1 = set(spot_fleet.instances)
    spot_fleet.modify_target_capacity(
        spot_fleet.target_capacity - capacity,
        dry_run=dry_run
    )
    set3 = set(spot_fleet.instances)
    assert set3 == set1


def test_downsize_capacity_by_small_weight(spot_fleet):
    market_composition = {MARKETS[1]: 1, MARKETS[2]: 3}
    spot_fleet.simulator.current_time.shift(seconds=+100)
    spot_fleet.modify_size(market_composition)
    spot_fleet.simulator.current_time.shift(seconds=+50)
    market_composition.update({MARKETS[0]: 1})
    spot_fleet.modify_size(market_composition)
    for instance in spot_fleet.instances.values():
        instance.join_time = instance.start_time
    spot_fleet.__target_capacity = 12
    # This should remove the last instance to meet capacity requirements
    spot_fleet.modify_target_capacity(11)
    assert spot_fleet.target_capacity == 11
    assert spot_fleet.market_size(MARKETS[0]) == 1


@pytest.mark.parametrize('target_capacity', [5, 10, 30, 50, 100])
def test_restore_capacity(spot_fleet, target_capacity):
    spot_fleet.modify_target_capacity(target_capacity)
    # terminate all instances
    spot_fleet.terminate_instances_by_id(list(spot_fleet.instances))
    assert spot_fleet.fulfilled_capacity >= target_capacity
