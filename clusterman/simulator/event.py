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
import itertools
from contextlib import ExitStack

from clusterman.simulator.util import patch_join_delay


class Event(object):
    """ Base event class; does nothing """
    id = itertools.count()

    def __init__(self, time, msg=None):
        """ Every subclass should call super().__init__(time, msg) to ensure needed setup is done

        :param time: an arrow object indicating the time the event should fire
        :param msg: a message to display; if this is None, just print the class name
        """
        self.id = next(Event.id)
        self.time = time
        self.msg = msg or type(self).__name__

    def __lt__(self, other):
        """ Sort order is based on time, then priority """
        return (self.time, EVENT_PRIORITIES[self.__class__]) < (other.time, EVENT_PRIORITIES[other.__class__])

    def __str__(self):
        return f'=== Event {self.id} -- {self.time}\t[{self.msg}]'

    def handle(self, simulator):
        """ Subclasses can override this for more complex behaviour

        :param simulator: a clusterman Simulator instance which the event can access to change state
        """
        pass


class AutoscalingEvent(Event):
    def __init__(self, time, msg=None):
        """ Trigger this event whenever the autoscaler should be called """
        super().__init__(time, msg=msg)

    def handle(self, simulator):
        simulator.autoscaler.run(timestamp=simulator.current_time)


class ModifyClusterSizeEvent(Event):
    def __init__(self, time, instance_types, use_join_delay=True, msg=None):
        """ Directly modify the size of an AWS cluster

        :param instance_types: a dict of InstanceMarket -> integer indicating the new (desired) size for the market
        :param use_join_delay: if True, instances will use the join delay parameters to postpone when they join the
            Mesos cluster; if False, they will join the Mesos cluster immediately
        """
        super().__init__(time, msg=msg)
        self.instance_types = dict(instance_types)
        self.use_join_delay = use_join_delay

    def handle(self, simulator):
        aws_cluster = simulator.aws_clusters[0]
        added_instances, removed_instances = aws_cluster.modify_size(self.instance_types)
        with (ExitStack() if self.use_join_delay else patch_join_delay()):
            for instance in added_instances:
                simulator.add_instance(instance)

        for instance in removed_instances:
            simulator.remove_instance(instance)

    def __str__(self):
        return super().__str__() + ' new size: ' + str(sum(self.instance_types.values()))


class InstancePriceChangeEvent(Event):
    def __init__(self, time, prices, msg=None):
        """ Trigger this event whenever instance prices change

        :param prices: a dict of InstanceMarket -> float indicating the new instance prices
        """
        super().__init__(time, msg=msg)
        self.prices = dict(prices)

    def handle(self, simulator):
        for market, price in self.prices.items():
            simulator.instance_prices[market].add_breakpoint(self.time, price)


# Event priorities are used for secondary sorting of events; if event A and B are scheduled at the same
# time and priority(A) < priority(B), A will be processed before B.
EVENT_PRIORITIES = {
    Event: 0,
    AutoscalingEvent: 1,
    ModifyClusterSizeEvent: 1,
    InstancePriceChangeEvent: 2,
}
