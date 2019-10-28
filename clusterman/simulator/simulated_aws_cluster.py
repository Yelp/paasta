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
from collections import defaultdict
from ipaddress import ip_address

from clusterman.aws.markets import get_market_resources


class Instance:
    id = itertools.count()
    ip = ip_address('10.0.0.1')

    def __init__(self, market, start_time, bid_price=None, join_time=None):
        self.id = next(Instance.id)
        self.ip_address = Instance.ip
        self.market = market
        self.start_time = start_time
        self.join_time = join_time
        self.end_time = None
        self.resources = get_market_resources(self.market)
        self.bid_price = bid_price

        Instance.ip += 1

    @property
    def spot(self):
        return self.bid_price is not None


class SimulatedAWSCluster:
    def __init__(self, simulator):
        self.simulator = simulator
        self.instances = {}
        self.instance_ids_by_market = defaultdict(list)
        self.ebs_storage = 0

    def __len__(self):
        return len(self.instances)

    def modify_size(self, instances_by_market):
        """ Modify the capacity of the cluster to match a specified state

        :param instances_by_market: a dict from InstanceMarket -> num, representing the desired number of
            instances in each specified market; unspecified markets are set to 0
        :returns: a tuple (added_instances, removed_instances)
        """
        added_instances, removed_instances = [], []
        instances_by_market.update({
            market_to_empty: 0
            for market_to_empty in set(self.instance_ids_by_market) - set(instances_by_market)
        })
        for market, num in instances_by_market.items():
            delta = int(num - self.market_size(market))

            if delta > 0:
                instances = [Instance(market, self.simulator.current_time) for i in range(delta)]
                self.instance_ids_by_market[market].extend([instance.id for instance in instances])
                added_instances.extend(instances)

            if delta < 0:
                to_del = abs(delta)
                for id in self.instance_ids_by_market[market][:to_del]:
                    self.instances[id].end_time = self.simulator.current_time
                    removed_instances.append(self.instances[id])
                    del self.instances[id]
                del self.instance_ids_by_market[market][:to_del]
                if not self.instance_ids_by_market[market]:
                    del self.instance_ids_by_market[market]

        self.instances.update({instance.id: instance for instance in added_instances})
        return added_instances, removed_instances

    def terminate_instances_by_id(self, ids, batch_size=-1):
        """ Terminate instance in the ids list

        :param ids: a list of IDs to be terminated
        :param batch_size: unused, needed for inheritance compatibility
        """
        for terminate_id in ids:
            instance = self.instances[terminate_id]
            market = instance.market
            del self.instances[terminate_id]
            self.instance_ids_by_market[market].remove(terminate_id)

    def market_size(self, market):
        return len(self.instance_ids_by_market[market])

    @property
    def cpus(self):
        return sum(instance.resources.cpus for instance in self.instances.values())

    @property
    def mem(self):
        return sum(instance.resources.mem for instance in self.instances.values())

    @property
    def disk(self):
        # Not all instance types have storage and require a mounted EBS volume
        return self.ebs_storage + sum(
            instance.resources.disk
            for instance in self.instances.values()
            if instance.resources.disk is not None
        )
