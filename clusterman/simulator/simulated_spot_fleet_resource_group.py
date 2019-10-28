from collections import namedtuple
from functools import lru_cache
from uuid import uuid4

from clusterman.aws.aws_resource_group import AWSResourceGroup
from clusterman.aws.markets import get_instance_market
from clusterman.simulator.simulated_aws_cluster import SimulatedAWSCluster

SpotMarketConfig = namedtuple('SpotMarketConfig', ['bid_price', 'weight'])


class SimulatedSpotFleetResourceGroup(SimulatedAWSCluster, AWSResourceGroup):
    """ An implementation of a SimulatedAWSCluster designed to model the AWS EC2 Spot Fleet object, which is also a
    AWSResourceGroup in a simulated Mesos cluster.

    The simulated spot fleet resource group object encapsulates a group of spot instances and attempts to maintain a
    specified capacity of those instances, as long as the bid price for the instances does not exceed a user-specified
    threshold.  If a fleet gets outbid in a particular market, the spot fleet will try to replenish the needed capacity
    in one or more different markets.  Users may specify an instance weight for each requested market, which will be
    used in capacity calculations.

    AWS provides two modes for allocating new instances, called the "allocation strategy": lowestPrice, and diversified.
    This model implementation only supports the diversified strategy; moreover, the details on how spot fleets maintain
    capacity with the diversified strategy are sparse, so this implementation provides a naive diversification strategy
    based on the limited documentation provided by AWS.

    Specifically, the diversification strategy implemented here does the following:
    1. Find all available markets (where an available market is defined as one in which the current price is no greater
       than the bid price in that market)
    2. Compute the residual capacity needed to bring each available market up to the same capacity
    3. Starting with the market having the smallest residual capacity, assign enough instances to the available markets
       to "cover" their residual capacity.
       a. Since instance weights may not evenly divide the residual capacity, there may be some overflow in a market.
          Any overflow is subtracted evenly from each of the remaining markets to ensure that we don't allocate too many
          new instances in other markets.
       b. If two markets have the same residual capacity, we fill the market with the cheaper spot price first.
    """

    def __init__(self, config, simulator):
        """
        :param config: a configuration dictionary that follows the SFR launch configuration schema.  Not all values
            needed for the SFR config are required here.  Specifically, we require the following elements:
            {
                'LaunchSpecifications': [
                    {
                        'InstanceType': AWS EC2 instance type name,
                        'SubnetId': Subnet the instance should be launched in (should map to a region in common/aws.py),
                        'SpotPrice': How much, in terms of price per unit capacity, to bid in this market,
                        'WeightedCapacity': How much to weight instances in this market by when calculating capacity
                    },
                    ...
                ]
            }
        """
        SimulatedAWSCluster.__init__(self, simulator)
        AWSResourceGroup.__init__(self, f'ssfr-{uuid4()}')
        self._instance_types = {}
        for spec in config['LaunchSpecifications']:
            bid_price = float(spec['SpotPrice']) * spec['WeightedCapacity']
            market = get_instance_market(spec)
            self._instance_types[market] = SpotMarketConfig(bid_price, spec['WeightedCapacity'])

        self.__target_capacity = 0
        self.allocation_strategy = config['AllocationStrategy']
        if self.allocation_strategy != 'diversified':
            raise NotImplementedError(f'{self.allocation_strategy} not supported')

    def market_weight(self, market):
        return self._instance_types[market].weight

    def modify_target_capacity(self, target_capacity, *, dry_run=False):
        """ Modify the requested capacity for a particular spot fleet

        :param target_capacity: desired capacity after this operation
        """
        if dry_run:
            return

        curr_capacity = self.fulfilled_capacity
        self.__target_capacity = target_capacity
        if curr_capacity < target_capacity:
            self._increase_capacity_to_target(target_capacity)

    def terminate_instances_by_id(self, ids, batch_size=-1):
        """ Terminate specified instances

        :param ids: desired ids of instances to be terminated
        :returns: a list of the terminated instance ids
        """
        for id in ids:
            self.simulator.remove_instance(self.instances[id])
        super().terminate_instances_by_id(ids, batch_size)
        # restore capacity if current capacity is less than target capacity
        if self.fulfilled_capacity < self.target_capacity:
            self._increase_capacity_to_target(self.target_capacity)
        return ids

    def _increase_capacity_to_target(self, target_capacity):
        """ When current capacity is less than target_capacity, this function would increase capacity to meet
        target_capacity

        :returns: the current capacity after filling up
        """
        new_market_counts = self._get_new_market_counts(target_capacity)
        added_instances, __ = self.modify_size(new_market_counts)
        for instance in added_instances:
            instance.bid_price = self._instance_types[instance.market].bid_price
            self.simulator.add_instance(instance)
        return self.fulfilled_capacity

    def _get_new_market_counts(self, target_capacity):
        """ Given a target capacity and current spot market prices, find instances to add to achieve the target capacity

        :param target_capacity: the desired total capacity of the fleet
        :returns: a dictionary suitable for passing to SimulatedAWSCluster.modify_size
        :raises ValueError: if target_capacity is less than the current self.target_capacity
        """
        if target_capacity < self.target_capacity:
            raise ValueError(f'Target capacity {target_capacity} < current capacity {self.target_capacity}')

        available_markets = self._find_available_markets()
        residuals = self._compute_market_residuals(target_capacity, available_markets)

        residual_correction = 0  # If we overflow in one market, correct the residuals in the remaining markets
        new_market_counts = {
            market: len(ids)
            for market, ids in self.instance_ids_by_market.items()
        }

        for i, (market, residual) in enumerate(residuals):
            remaining_markets = len(residuals) - (i + 1)

            # If the residual correction is larger than the residual, this means we shouldn't add any more instances
            # in this market because its residual has been "eaten up" by overflow in previous markets.  When this
            # happens, we just update the correction with any overflow that might have been introduced in *this* market.
            # Sorting the residuals in ascending order ensures correction has been updated by negative residuals
            # first.
            if residual < residual_correction:
                if remaining_markets > 0:
                    residual_correction += (residual_correction - residual) / remaining_markets
                continue

            residual -= residual_correction
            weight = self._instance_types[market].weight
            instance_num, remainder = divmod(residual, weight)

            # If the instance weight doesn't evenly divide the residual, add an extra instance (which will
            # cause some overflow in that market)
            if remainder > 0:
                instance_num += 1
                overflow = (instance_num * weight) - residual

                # Evenly divide the overflow among the remaining markets
                if remaining_markets > 0:
                    residual_correction += overflow / remaining_markets

            if instance_num != 0:
                new_market_counts[market] = instance_num + self.market_size(market)
        return new_market_counts

    def _compute_market_residuals(self, target_capacity, markets):
        """ Given a target capacity and list of available markets, compute the residuals needed to bring all markets up
        to an (approximately) equal capacity such that the total capacity meets or exceeds the target capacity

        :param target_capacity: the desired total capacity of the fleet
        :param markets: a list of available markets
        :returns: a list of (market, residual) tuples, sorted first by lowest capacity and next by lowest spot price
        """
        target_capacity_per_market = target_capacity / len(markets) if len(markets) != 0 else 0

        # Some helper closures for computing residuals and sorting;
        @lru_cache()  # memoize the results
        def residual(market):
            return target_capacity_per_market - self.market_capacities.get(market, 0)

        def residual_sort_key(value_tuple):
            market, residual = value_tuple
            return (residual, self.simulator.instance_prices[market].call(self.simulator.current_time))

        return sorted(
            [(market, residual(market)) for market in markets],
            key=residual_sort_key,
        )

    def _find_available_markets(self):
        """
        :returns: a list of available spot markets, e.g. markets in the spot fleet request whose bid price is above the
            current market price
        """
        # TODO (CLUSTERMAN-51) need to factor in on-demand prices here
        return [
            market
            for market, config in self._instance_types.items()
            if config.bid_price >= self.simulator.instance_prices[market].call(self.simulator.current_time)
        ]

    @property
    def instance_ids(self):
        return list(self.instances.keys())

    @property
    def market_capacities(self):
        return {
            market: len(instance_ids) * self.market_weight(market)
            for market, instance_ids in self.instance_ids_by_market.items()
            if market.az
        }

    @property
    def _target_capacity(self):
        return self.__target_capacity

    @property
    def fulfilled_capacity(self):
        """ The current actual capacity of the spot fleet

        Note that the actual capacity may be greater than the target capacity if instance weights do not evenly divide
        the given target capacity
        """
        return sum(self.market_capacities.values())

    @property
    def status(self):
        return 'active'

    @property
    def is_stale(self):
        return False

    @staticmethod
    def load(cluster, pool, config):
        raise NotImplementedError("Shouldn't be called")
