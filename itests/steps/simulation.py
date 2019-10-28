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
from datetime import timedelta

import arrow
import behave
import staticconf.testing
from hamcrest import assert_that
from hamcrest import close_to
from hamcrest import contains
from hamcrest import equal_to

from clusterman.aws.markets import InstanceMarket
from clusterman.run import setup_logging
from clusterman.simulator.event import InstancePriceChangeEvent
from clusterman.simulator.event import ModifyClusterSizeEvent
from clusterman.simulator.simulator import SimulationMetadata
from clusterman.simulator.simulator import Simulator

_MARKETS = {
    'a': InstanceMarket('c3.8xlarge', 'us-west-2a'),
    'b': InstanceMarket('c3.8xlarge', 'us-west-2b'),
    'c': InstanceMarket('c3.8xlarge', 'us-west-2c'),
}


@behave.given('market (?P<market_id>[a-cA-C]) has (?P<count>\d+) instances? at time (?P<time>\d+)')
def setup_instance(context, market_id, count, time):
    if not hasattr(context, 'market_counts'):
        context.market_counts = [(0, {})]
    last_time = context.market_counts[-1][0]

    if int(time) != last_time:
        context.market_counts.append((time, dict(context.market_counts[-1][1])))
    context.market_counts[-1][1].update({_MARKETS[market_id.lower()]: int(count)})


@behave.given('market (?P<market_id>[a-cA-C]) costs \$(?P<cost>\d+(?:\.\d+)?)/hour at time (?P<time>\d+)')
def setup_cost(context, market_id, cost, time):
    if not hasattr(context, 'markets'):
        context.markets = {}
    context.markets.setdefault(market_id.lower(), []).append((int(time), float(cost)))


@behave.when('the instance takes (?P<time>\d+) seconds to join')
def setup_join_delay(context, time):
    context.join_delay_seconds = int(time)


@behave.when('the join-delay override flag is set')
def setup_join_delay_override(context):
    context.use_join_delay = False


@behave.when('the simulator runs for (?P<hours>\d+) hours(?P<per_second_billing> and billing is per-second)?')
def run_simulator(context, hours, per_second_billing):
    billing_frequency = timedelta(seconds=1) if per_second_billing else timedelta(hours=1)
    refund_outbid = not per_second_billing
    setup_logging()
    context.simulator = Simulator(
        SimulationMetadata('test', 'Testing', 'mesos', 'test-tag'),
        start_time=arrow.get(0),
        end_time=arrow.get(int(hours) * 3600),
        autoscaler_config_file=None,
        metrics_client=None,
        billing_frequency=billing_frequency,
        refund_outbid=refund_outbid,
    )
    with staticconf.testing.PatchConfiguration({
        'join_delay_mean_seconds': getattr(context, 'join_delay_seconds', 0),
        'join_delay_stdev_seconds': 0,
    }):
        for join_time, market_counts in context.market_counts:
            context.simulator.add_event(ModifyClusterSizeEvent(
                arrow.get(join_time),
                market_counts,
                use_join_delay=getattr(context, 'use_join_delay', True),
            ))
        for market_id, prices in getattr(context, 'markets', {}).items():
            for time, cost in sorted(prices):
                market = _MARKETS[market_id.lower()]
                context.simulator.add_event(InstancePriceChangeEvent(arrow.get(time), {market: cost}))
        context.simulator.run()


@behave.then('the simulated cluster costs \$(?P<cost>\d+(?:\.\d+)?) total')
def check_cost(context, cost):
    assert_that(context.simulator.total_cost, close_to(float(cost), 0.01))


@behave.then('the instance (?P<time_param>start|join) time should be (?P<time>\d+)')
def check_instance_times(context, time_param, time):
    instance = list(context.simulator.aws_clusters[0].instances.values())[0]
    assert_that(getattr(instance, f'{time_param}_time').timestamp, equal_to(int(time)))


@behave.then('no instances should join the Mesos cluster')
def check_cluster_cpus_empty(context):
    for y in context.simulator.mesos_cpus.breakpoints.values():
        assert_that(y, equal_to(0))


@behave.then('instances should join the Mesos cluster')
def check_cluster_cpus(context):
    assert_that(
        list(context.simulator.mesos_cpus.breakpoints.items()),
        contains(
            (arrow.get(300), 32),
            (arrow.get(1800), 0),
        ),
    )
