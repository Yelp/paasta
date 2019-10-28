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
import operator
import os
import random
import subprocess
from collections import defaultdict
from datetime import timedelta
from heapq import heappop
from heapq import heappush
from typing import List
from typing import Mapping
from typing import Optional
from typing import Set

import colorlog
import staticconf
import yaml
from arrow import Arrow
from clusterman_metrics import METADATA
from sortedcontainers import SortedDict  # noqa

from clusterman.autoscaler.autoscaler import Autoscaler
from clusterman.autoscaler.signals import setup_signals_environment
from clusterman.aws.client import ec2
from clusterman.aws.markets import get_instance_market
from clusterman.aws.markets import InstanceMarket
from clusterman.math.piecewise import hour_transform
from clusterman.math.piecewise import piecewise_breakpoint_generator
from clusterman.math.piecewise import piecewise_max
from clusterman.math.piecewise import PiecewiseConstantFunction
from clusterman.simulator.event import Event
from clusterman.simulator.simulated_aws_cluster import SimulatedAWSCluster
from clusterman.simulator.simulated_pool_manager import SimulatedPoolManager
from clusterman.simulator.util import patch_join_delay
from clusterman.simulator.util import SimulationMetadata
from clusterman.util import get_cluster_dimensions


logger = colorlog.getLogger(__name__)
SimFn = PiecewiseConstantFunction[Arrow]


class Simulator:
    def __init__(self, metadata, start_time, end_time, autoscaler_config_file=None, metrics_client=None,
                 billing_frequency=timedelta(seconds=1), refund_outbid=True) -> None:
        """ Maintains all of the state for a clusterman simulation

        :param metadata: a SimulationMetadata object
        :param start_time: an arrow object indicating the start of the simulation
        :param end_time: an arrow object indicating the end of the simulation
        :param autoscaler_config_file: a filename specifying a list of existing SFRs or SFR configs
        :param billing_frequency: a timedelta object indicating how often to charge for an instance
        :param refund_outbid: if True, do not incur any cost for an instance lost to an outbid event
        """
        self.autoscaler: Optional[Autoscaler] = None
        self.metadata = metadata
        self.metrics_client = metrics_client
        self.start_time = start_time
        self.current_time = start_time
        self.end_time = end_time

        self.instance_prices: Mapping[InstanceMarket, PiecewiseConstantFunction] = defaultdict(
            lambda: PiecewiseConstantFunction()
        )
        self.cost_per_hour: SimFn = PiecewiseConstantFunction()
        self.aws_cpus: SimFn = PiecewiseConstantFunction()
        self.mesos_cpus: SimFn = PiecewiseConstantFunction()
        self.mesos_cpus_allocated: SimFn = PiecewiseConstantFunction()
        self.markets: Set = set()

        self.billing_frequency = billing_frequency
        self.refund_outbid = refund_outbid

        if autoscaler_config_file:
            self._make_autoscaler(autoscaler_config_file)
            self.aws_clusters = self.autoscaler.pool_manager.resource_groups.values()  # type: ignore
            period = self.autoscaler.signal.period_minutes  # type: ignore
            print(f'Autoscaler configured; will run every {period} minutes')
        else:
            self.aws_clusters = [SimulatedAWSCluster(self)]
            print('No autoscaler configured; using metrics for cluster size')

        # The event queue holds all of the simulation events, ordered by time
        self.event_queue: List[Event] = []

        # Don't use add_event here or the end_time event will get discarded
        heappush(self.event_queue, Event(self.start_time, msg='Simulation begins'))
        heappush(self.event_queue, Event(self.end_time, msg='Simulation ends'))

    def add_event(self, evt):
        """ Add a new event to the queue; events outside the simulation time bounds will be ignored

        :param evt: an Event object or subclass
        """
        if evt.time >= self.end_time:
            logger.info(f'Adding event after simulation end time ({evt.time}); event ignored')
            return
        elif evt.time < self.current_time:
            logger.info(f'Adding event before self.current_time ({evt.time}); event ignored')
            return

        heappush(self.event_queue, evt)

    def run(self):
        """ Run the simulation until the end, processing each event in the queue one-at-a-time in priority order """
        print(f'Starting simulation from {self.start_time} to {self.end_time}')

        with self.metadata:
            while self.event_queue:
                evt = heappop(self.event_queue)
                self.current_time = evt.time
                logger.event(evt)
                evt.handle(self)

        # charge any instances that haven't been terminated yet
        for cluster in self.aws_clusters:
            for instance in cluster.instances.values():
                instance.end_time = self.current_time
                self._compute_instance_cost(instance)

        print('Simulation complete ({time}s)'.format(
            time=(self.metadata.sim_end - self.metadata.sim_start).total_seconds()
        ))

    def add_instance(self, instance):
        cpus = instance.resources.cpus
        self.aws_cpus.add_delta(self.current_time, cpus)

        join_delay_mean = staticconf.read_int('join_delay_mean_seconds')
        join_delay_stdev = staticconf.read_int('join_delay_stdev_seconds')
        instance.join_time = instance.start_time.shift(seconds=random.gauss(join_delay_mean, join_delay_stdev))

        self.mesos_cpus.add_delta(instance.join_time, cpus)

    def remove_instance(self, instance):
        cpus = instance.resources.cpus
        instance.end_time = self.current_time
        self.aws_cpus.add_delta(self.current_time, -cpus)

        # If the instance was terminated before it could join the Mesos cluster, we need to re-adjust the mesos_cpus
        # function at the join time; otherwise, we need to adjust it at the termination time
        self.mesos_cpus.add_delta(max(instance.join_time, instance.end_time), -cpus)
        if instance.join_time > instance.end_time:
            instance.join_time = None
        self._compute_instance_cost(instance)

    @property
    def total_cost(self):
        return self.get_data('cost').values()[0]

    def get_data(
        self,
        key: str,
        start_time: Optional[Arrow] = None,
        end_time: Optional[Arrow] = None,
        step: Optional[timedelta] = None,
    ) -> 'SortedDict[Arrow, float]':
        """ Compute the capacity for the cluster in the specified time range, grouped into chunks

        :param key: the type of data to retreive; must correspond to a key in REPORT_TYPES
        :param start_time: the lower bound of the range (if None, use simulation start time)
        :param end_time: the upper bound of the range (if None, use simulation end time)
        :param step: the width of time for each chunk
        :returns: a list of CPU capacities for the cluster from start_time to end_time
        """
        start_time = start_time or self.start_time
        end_time = end_time or self.end_time
        if key == 'cpus':
            return self.mesos_cpus.values(start_time, end_time, step)
        elif key == 'cpus_allocated':
            return self.mesos_cpus_allocated.values(start_time, end_time, step)
        elif key == 'unused_cpus':
            # If an agent hasn't joined the cluster yet, we'll treat it as "unused" in the simulation
            unused_cpus = self.aws_cpus - self.mesos_cpus_allocated
            return unused_cpus.values(start_time, end_time, step)
        elif key == 'cost':
            return self.cost_per_hour.integrals(start_time, end_time, step, transform=hour_transform)
        elif key == 'unused_cpus_cost':
            # Here we treat CPUs that haven't joined the Mesos cluster as un-allocated.  It's arguable
            # if that's the right way to do this or not.
            percent_unallocated = (self.aws_cpus - self.mesos_cpus_allocated) / self.aws_cpus
            percent_cost = percent_unallocated * self.cost_per_hour
            return percent_cost.integrals(start_time, end_time, step, transform=hour_transform)
        elif key == 'cost_per_cpu':
            cost_per_cpu = self.cost_per_hour / self.aws_cpus
            return cost_per_cpu.values(start_time, end_time, step)
        elif key == 'oversubscribed':
            max_fn = piecewise_max(self.mesos_cpus_allocated - self.aws_cpus, PiecewiseConstantFunction())
            return max_fn.values(start_time, end_time, step)
        else:
            raise ValueError(f'Data key {key} is not recognized')

    def _compute_instance_cost(self, instance):
        """ Adjust the cost-per-hour function to account for the specified instance

        :param instance: an Instance object to compute costs for; the instance must have a start_time and end_time
        """

        # Charge for the price of the instance when it is launched
        prices = self.instance_prices[instance.market]
        curr_timestamp = instance.start_time
        delta, last_billed_price = 0, prices.call(curr_timestamp)
        self.cost_per_hour.add_delta(curr_timestamp, last_billed_price)

        # Loop through all the breakpoints in the instance_prices function (in general this should be more efficient
        # than looping through the billing times, as long as billing happens more frequently than price change
        # events; this is expected to be the case for billing frequencies of ~1s)
        for bp_timestamp in piecewise_breakpoint_generator(prices.breakpoints, instance.start_time, instance.end_time):

            # if the breakpoint exceeds the next billing point, we need to charge for that billing point
            # based on whatever the most recent breakpoint value before the billing point was (this is tracked
            # in the delta variable).  Then, we need to advance the current time to the billing point immediately
            # preceding (and not equal to) the breakpoint
            if bp_timestamp >= curr_timestamp + self.billing_frequency:
                self.cost_per_hour.add_delta(curr_timestamp + self.billing_frequency, delta)

                # we assume that if the price change and the billing point occur simultaneously,
                # that the instance is charged with the new price; so, we step the curr_timestep back
                # so that this block will get triggered on the next time through the loop
                jumps, remainder = divmod(bp_timestamp - curr_timestamp, self.billing_frequency)
                if not remainder:
                    jumps -= 1
                curr_timestamp += jumps * self.billing_frequency
                last_billed_price += delta

            # piecewise_breakpoint_generator includes instance.end_time in the list of results, so that we can do
            # one last price check (the above if block) before the instance gets terminated.  However, that means
            # here we only want to update delta if the timestamp is a real breakpoint
            if bp_timestamp in prices.breakpoints:
                delta = prices.breakpoints[bp_timestamp] - last_billed_price

        # TODO (CLUSTERMAN-54) add some itests to make sure this is working correctly
        # Determine whether or not to bill for the last billing period of the instance.  We charge for the last billing
        # period if any of the following conditions are met:
        #   a) the instance is not a spot instance
        #   b) self.refund_outbid is false, e.g. we have "new-style" AWS pricing
        #   c) the instance bid price (when it was terminated) is greater than the current spot price
        if not instance.spot or not self.refund_outbid or instance.bid_price > prices.call(instance.end_time):
            curr_timestamp += self.billing_frequency
        self.cost_per_hour.add_delta(curr_timestamp, -last_billed_price)

    def _make_autoscaler(self, autoscaler_config_file: str) -> None:
        fetch_count, signal_count = setup_signals_environment(self.metadata.pool, self.metadata.scheduler)
        signal_dir = os.path.join(os.path.expanduser('~'), '.cache', 'clusterman')

        endpoint_url = staticconf.read_string('aws.endpoint_url', '').format(svc='s3')
        env = os.environ.copy()
        if endpoint_url:
            env['AWS_ENDPOINT_URL_ARGS'] = f'--endpoint-url {endpoint_url}'

        for i in range(fetch_count):
            subprocess.run(['fetch_clusterman_signal', str(i), signal_dir], check=True, env=env)
        for i in range(signal_count):
            subprocess.Popen(['run_clusterman_signal', str(i), signal_dir], env=env)

        with open(autoscaler_config_file) as f:
            autoscaler_config = yaml.safe_load(f)
        configs = autoscaler_config.get('configs', [])
        if 'sfrs' in autoscaler_config:
            aws_configs = ec2.describe_spot_fleet_requests(SpotFleetRequestIds=autoscaler_config['sfrs'])
            configs.extend([config['SpotFleetRequestConfig'] for config in aws_configs['SpotFleetRequestConfigs']])
        pool_manager = SimulatedPoolManager(self.metadata.cluster, self.metadata.pool, configs, self)
        metric_values = self.metrics_client.get_metric_values(
            'target_capacity',
            METADATA,
            self.start_time.timestamp,
            # metrics collector runs 1x/min, but we'll try to get five data points in case some data is missing
            self.start_time.shift(minutes=5).timestamp,
            use_cache=False,
            extra_dimensions=get_cluster_dimensions(self.metadata.cluster, self.metadata.pool, self.metadata.scheduler),
        )
        # take the earliest data point available - this is a Decimal, which doesn't play nicely, so convert to an int
        with patch_join_delay():
            actual_target_capacity = int(metric_values['target_capacity'][0][1])
            pool_manager.modify_target_capacity(actual_target_capacity, force=True, prune=False)

        for config in configs:
            for spec in config['LaunchSpecifications']:
                self.markets |= {get_instance_market(spec)}
        self.autoscaler = Autoscaler(
            self.metadata.cluster,
            self.metadata.pool,
            self.metadata.scheduler,
            [self.metadata.pool],
            pool_manager=pool_manager,
            metrics_client=self.metrics_client,
            monitoring_enabled=False,  # no sensu alerts during simulations
        )

    def __add__(self, other):
        opcode = '+'
        return _make_comparison_sim(self, other, operator.add, opcode)

    def __sub__(self, other):
        opcode = '-'
        return _make_comparison_sim(self, other, operator.sub, opcode)

    def __mul__(self, other):
        opcode = '*'
        return _make_comparison_sim(self, other, operator.mul, opcode)

    def __truediv__(self, other):
        opcode = '/'
        return _make_comparison_sim(self, other, operator.truediv, opcode)

    def __getstate__(self):
        serialized_keys = ['metadata', 'start_time', 'current_time', 'end_time'] + \
            ['instance_prices', 'cost_per_hour', 'cpus', 'cpus_allocated']
        states = {}
        for key in serialized_keys:
            states[key] = self.__dict__[key]
        return states


def _make_comparison_sim(sim1, sim2, op, opcode):
    metadata = SimulationMetadata(
        f'[{sim1.metadata.name}] {opcode} [{sim2.metadata.name}]',

        f'{sim1.metadata.cluster}' if
        sim1.metadata.cluster == sim2.metadata.cluster else
        f'{sim1.metadata.cluster}, {sim2.metadata.cluster}',

        f'{sim1.metadata.pool}' if
        sim1.metadata.pool == sim2.metadata.pool else
        f'{sim1.metadata.pool}, {sim2.metadata.pool}',
    )
    if sim1.start_time != sim2.start_time or sim1.end_time != sim2.end_time:
        logger.warn('Compared simulators do not have the same time boundaries; '
                    'results outside the common window will be incorrect.')
        logger.warn(f'{sim1.metadata.name}: [{sim1.start_time}, {sim1.end_time}]')
        logger.warn(f'{sim2.metadata.name}: [{sim2.start_time}, {sim2.end_time}]')

    comp_sim = Simulator(metadata, sim1.start_time, sim1.end_time)
    comp_sim.cost_per_hour = op(sim1.cost_per_hour, sim2.cost_per_hour)
    comp_sim.cpus = op(sim1.cpus, sim2.cpus)
    return comp_sim
