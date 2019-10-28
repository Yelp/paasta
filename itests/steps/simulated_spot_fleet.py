from collections import defaultdict

import arrow
import behave
import mock
from hamcrest import assert_that
from hamcrest import close_to
from hamcrest import equal_to
from hamcrest import greater_than_or_equal_to

from clusterman.aws.markets import InstanceMarket
from clusterman.math.piecewise import PiecewiseConstantFunction
from clusterman.simulator.simulated_spot_fleet_resource_group import SimulatedSpotFleetResourceGroup


_MARKETS = [
    InstanceMarket('c3.4xlarge', 'us-west-1a'),
    InstanceMarket('c3.4xlarge', 'us-west-1b'),
    InstanceMarket('i2.8xlarge', 'us-west-2a'),
    InstanceMarket('m4.4xlarge', 'us-west-2b'),
    InstanceMarket('r4.2xlarge', 'us-west-1c'),
    InstanceMarket('d2.2xlarge', 'us-west-1c'),
    InstanceMarket('r4.4xlarge', 'us-west-2c'),
    InstanceMarket('d2.4xlarge', 'us-west-2c'),
]


def _make_mock_simulator():
    instance_prices = defaultdict(lambda: PiecewiseConstantFunction())
    instance_prices[_MARKETS[0]].add_breakpoint(arrow.get(0), 0.5)
    instance_prices[_MARKETS[1]].add_breakpoint(arrow.get(0), 0.7)
    instance_prices[_MARKETS[2]].add_breakpoint(arrow.get(0), 0.6)
    instance_prices[_MARKETS[3]].add_breakpoint(arrow.get(0), 0.55)
    instance_prices[_MARKETS[4]].add_breakpoint(arrow.get(0), 0.65)
    instance_prices[_MARKETS[5]].add_breakpoint(arrow.get(0), 0.75)
    instance_prices[_MARKETS[6]].add_breakpoint(arrow.get(0), 0.8)
    instance_prices[_MARKETS[7]].add_breakpoint(arrow.get(0), 0.9)
    return mock.Mock(
        instance_prices=instance_prices,
        current_time=arrow.get(0),
    )


@behave.given('a simulated spot fleet resource group')
def sfrg(context):
    spot_fleet_request_config = {
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
                'SpotPrice': 0.41,
                'WeightedCapacity': 2,
                'SubnetId': 'us-west-1b',
            },
            {
                'InstanceType': 'i2.8xlarge',
                'SpotPrice': 0.57,
                'WeightedCapacity': 3,
                'SubnetId': 'us-west-2a',
            },
            {
                'InstanceType': 'm4.4xlarge',
                'SpotPrice': 2.02,
                'WeightedCapacity': 0.5,
                'SubnetId': 'us-west-2b',
            },

            {
                'InstanceType': 'r4.2xlarge',
                'SpotPrice': 1.2,
                'WeightedCapacity': 1,
                'SubnetId': 'us-west-1c',
            },
            {
                'InstanceType': 'd2.2xlarge',
                'SpotPrice': 0.6,
                'WeightedCapacity': 1.5,
                'SubnetId': 'us-west-1c',
            },
            {
                'InstanceType': 'r4.4xlarge',
                'SpotPrice': 0.57,
                'WeightedCapacity': 2,
                'SubnetId': 'us-west-2c',
            },
            {
                'InstanceType': 'd2.4xlarge',
                'SpotPrice': 1.5,
                'WeightedCapacity': 0.8,
                'SubnetId': 'us-west-2c',
            },
        ],
    }
    with mock.patch(
        'clusterman.simulator.simulated_spot_fleet_resource_group.get_instance_market',
        side_effect=lambda spec: InstanceMarket(spec['InstanceType'], spec['SubnetId']),
    ):
        context.spot_fleet = SimulatedSpotFleetResourceGroup(spot_fleet_request_config, _make_mock_simulator())


@behave.when('we request (?P<quantity>\d+) target capacity')
def request_target_capacity(context, quantity):
    context.desired_target_capacity = int(quantity)
    context.spot_fleet.modify_target_capacity(context.desired_target_capacity)


@behave.when('capacity in one market drops')
def no_capacity_in_market(context):
    context.outbid_market = 4
    context.spot_fleet.simulator.instance_prices[_MARKETS[context.outbid_market]].add_breakpoint(arrow.get(300), 3.0)
    context.spot_fleet.simulator.current_time = arrow.get(480)
    terminate_ids = list(context.spot_fleet.instance_ids_by_market[_MARKETS[context.outbid_market]])
    context.spot_fleet.terminate_instances_by_id(terminate_ids)


@behave.when('capacity in one market is high')
def market_high_capacity(context):
    context.high_capacity_market = 0
    context.spot_fleet.modify_size({_MARKETS[context.high_capacity_market]: 100})


@behave.then('the simulated spot fleet should be diversified')
def check_diversification(context):
    for market in _MARKETS:
        assert_that(
            context.spot_fleet.market_size(market) * (context.spot_fleet._instance_types[market].weight),
            close_to(context.desired_target_capacity / len(_MARKETS), 5.0),
        )


@behave.then('the fulfilled capacity should be above the target capacity')
def check_fulfilled_capacity(context):
    assert_that(
        context.spot_fleet.fulfilled_capacity,
        greater_than_or_equal_to(context.spot_fleet.target_capacity),
    )


@behave.then('the spot fleet should have no instances from the empty market')
def empty_market_is_empty(context):
    assert_that(
        context.spot_fleet.market_size(_MARKETS[context.outbid_market]),
        equal_to(0),
    )


@behave.then('the spot fleet should not add instances from the high market')
def high_market_is_high(context):
    assert_that(
        context.spot_fleet.market_size(_MARKETS[context.high_capacity_market]),
        equal_to(100),
    )
