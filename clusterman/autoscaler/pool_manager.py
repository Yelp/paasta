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
import math
import traceback
from collections import defaultdict
from typing import cast
from typing import Collection
from typing import Dict
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Type

import colorlog
import staticconf

from clusterman.aws.aws_resource_group import AWSResourceGroup
from clusterman.aws.markets import InstanceMarket
from clusterman.aws.util import RESOURCE_GROUPS
from clusterman.config import POOL_NAMESPACE
from clusterman.draining.queue import DrainingClient
from clusterman.exceptions import AllResourceGroupsAreStaleError
from clusterman.exceptions import PoolManagerError
from clusterman.exceptions import ResourceGroupError
from clusterman.interfaces.cluster_connector import AgentMetadata
from clusterman.interfaces.cluster_connector import AgentState
from clusterman.interfaces.cluster_connector import ClusterConnector
from clusterman.interfaces.resource_group import InstanceMetadata
from clusterman.interfaces.resource_group import ResourceGroup
from clusterman.monitoring_lib import get_monitoring_client
from clusterman.util import read_int_or_inf

AWS_RUNNING_STATES = ('running',)
MIN_CAPACITY_PER_GROUP = 1
SFX_RESOURCE_GROUP_MODIFICATION_FAILED_NAME = 'clusterman.resource_group_modification_failed'
logger = colorlog.getLogger(__name__)


class ClusterNodeMetadata(NamedTuple):
    agent: AgentMetadata
    instance: InstanceMetadata


class PoolManager:
    def __init__(
        self,
        cluster: str,
        pool: str,
        scheduler: str,
        fetch_state: bool = True,
    ) -> None:
        self.cluster = cluster
        self.pool = pool
        self.scheduler = scheduler
        self.cluster_connector = ClusterConnector.load(self.cluster, self.pool, self.scheduler)
        self.pool_config = staticconf.NamespaceReaders(POOL_NAMESPACE.format(pool=self.pool, scheduler=self.scheduler))

        self.draining_enabled = self.pool_config.read_bool('draining_enabled', default=False)
        self.draining_client: Optional[DrainingClient] = DrainingClient(cluster) if self.draining_enabled else None
        self.min_capacity = self.pool_config.read_int('scaling_limits.min_capacity')
        self.max_capacity = self.pool_config.read_int('scaling_limits.max_capacity')
        self.max_tasks_to_kill = read_int_or_inf(self.pool_config, 'scaling_limits.max_tasks_to_kill')
        self.max_weight_to_add = self.pool_config.read_int('scaling_limits.max_weight_to_add')
        self.max_weight_to_remove = self.pool_config.read_int('scaling_limits.max_weight_to_remove')

        if fetch_state:
            self.reload_state()

    def reload_state(self) -> None:
        """ Fetch any state that may have changed behind our back, but which we do not want to change during an
        ``Autoscaler.run()``.
        """
        logger.info('Reloading cluster connector state')
        self.cluster_connector.reload_state()

        logger.info('Reloading resource groups')
        self._reload_resource_groups()

        logger.info('Recalculating non-orphan fulfilled capacity')
        self.non_orphan_fulfilled_capacity = self._calculate_non_orphan_fulfilled_capacity()

    def mark_stale(self, dry_run: bool) -> None:
        if dry_run:
            logger.warning('Running in "dry-run" mode; cluster state will not be modified')

        for group_id, group in self.resource_groups.items():
            logger.info(f'Marking {group_id} as stale!')
            try:
                group.mark_stale(dry_run)
            except NotImplementedError as e:
                logger.warning(f'Skipping {group_id} because of error:')
                logger.warning(str(e))

    def modify_target_capacity(
        self,
        new_target_capacity: float,
        dry_run: bool = False,
        force: bool = False,
        prune: bool = True,
    ) -> float:
        """ Change the desired :attr:`target_capacity` of the resource groups belonging to this pool.

        Capacity changes are roughly evenly distributed across the resource groups to ensure that
        nodes are diversified in the cluster

        :param new_target_capacity: the desired target capacity for the cluster and pool
        :param dry_run: boolean indicating whether the cluster should actually be modified
        :param force: boolean indicating whether to override the scaling limits
        :returns: the (set) new target capacity

        .. note:: It may take some time (up to a few minutes) for changes in the target capacity to be reflected in
           :attr:`fulfilled_capacity`.  Once the capacity has equilibrated, the fulfilled capacity and the target
           capacity may not exactly match, but the fulfilled capacity will never be under the target (for example, if
           there is no combination of nodes that evenly sum to the desired target capacity, the final fulfilled
           capacity will be slightly above the target capacity)
        """
        if dry_run:
            logger.warning('Running in "dry-run" mode; cluster state will not be modified')
        if not self.resource_groups:
            raise PoolManagerError('No resource groups available')

        orig_target_capacity = self.target_capacity
        new_target_capacity = self._constrain_target_capacity(new_target_capacity, force)

        res_group_targets = self._compute_new_resource_group_targets(new_target_capacity)
        for group_id, target in res_group_targets.items():
            try:
                self.resource_groups[group_id].modify_target_capacity(
                    target,
                    dry_run=dry_run,
                )
            except ResourceGroupError:
                logger.critical(traceback.format_exc())
                rge_counter = get_monitoring_client().create_counter(
                    SFX_RESOURCE_GROUP_MODIFICATION_FAILED_NAME,
                    {'cluster': self.cluster, 'pool': self.pool},
                )
                rge_counter.count()
                continue

        if prune:
            self.prune_excess_fulfilled_capacity(new_target_capacity, res_group_targets, dry_run)
        logger.info(f'Target capacity for {self.pool} changed from {orig_target_capacity} to {new_target_capacity}')
        return new_target_capacity

    def prune_excess_fulfilled_capacity(
        self,
        new_target_capacity: float,
        group_targets: Optional[Mapping[str, float]] = None,
        dry_run: bool = False,
    ) -> None:
        """ Decrease the capacity in the cluster

        The number of tasks killed is limited by ``self.max_tasks_to_kill``, and the nodes are terminated in an
        order which (hopefully) reduces the impact on jobs running on the cluster.

        :param group_targets: a list of new resource group target_capacities; if None, use the existing
            target_capacities (this parameter is necessary in order for dry runs to work correctly)
        :param dry_run: if True, do not modify the state of the cluster, just log actions
        """

        marked_nodes_by_group = self._choose_nodes_to_prune(new_target_capacity, group_targets)

        if not dry_run:
            if self.draining_enabled:
                assert self.draining_client  # make mypy happy
                for group_id, node_metadatas in marked_nodes_by_group.items():
                    for node_metadata in node_metadatas:
                        self.draining_client.submit_instance_for_draining(
                            node_metadata.instance,
                            sender=cast(Type[AWSResourceGroup], self.resource_groups[group_id].__class__),
                            scheduler=self.scheduler,
                        )
            else:
                for group_id, node_metadatas in marked_nodes_by_group.items():
                    self.resource_groups[group_id].terminate_instances_by_id([
                        node_metadata.instance.instance_id
                        for node_metadata in node_metadatas
                    ])

    def get_node_metadatas(self, state_filter: Optional[Collection[str]] = None) -> Sequence[ClusterNodeMetadata]:
        """ Get a list of metadata about the nodes currently in the pool

        :param state_filter: only return nodes matching a particular state ('running', 'cancelled', etc)
        :returns: a list of InstanceMetadata objects
        """
        return [
            ClusterNodeMetadata(
                self.cluster_connector.get_agent_metadata(instance_metadata.ip_address),
                instance_metadata,
            )
            for group in self.resource_groups.values()
            for instance_metadata in group.get_instance_metadatas(state_filter)
        ]

    def _reload_resource_groups(self) -> None:
        resource_groups: MutableMapping[str, ResourceGroup] = {}
        for resource_group_conf in self.pool_config.read_list('resource_groups'):
            if not isinstance(resource_group_conf, dict) or len(resource_group_conf) != 1:
                logger.error(f'Malformed config: {resource_group_conf}')
                continue
            resource_group_type = list(resource_group_conf.keys())[0]
            resource_group_cls = RESOURCE_GROUPS.get(resource_group_type)
            if resource_group_cls is None:
                logger.error(f'Unknown resource group {resource_group_type}')
                continue

            resource_groups.update(resource_group_cls.load(
                cluster=self.cluster,
                pool=self.pool,
                config=list(resource_group_conf.values())[0],
            ))
        self.resource_groups = resource_groups
        logger.info(f'Loaded resource groups: {list(resource_groups)}')

    def _constrain_target_capacity(
        self,
        requested_target_capacity: float,
        force: bool = False,
    ) -> float:
        """ Signals can return arbitrary values, so make sure we don't add or remove too much capacity """

        requested_delta = requested_target_capacity - self.target_capacity

        # first, determine whether or not the delta is actually positive or negative.
        # for example, if the current target capacity is above the maximum, the resulting delta
        # will be negative, even if the requested delta is positive because we take the min
        # of the two. This is good because using this delta means moving towards the
        # limit, in the case of the example, towards the maximum, since the target capacity
        # is currently above the maximum.
        if requested_delta > 0:
            delta = min(self.max_capacity - self.target_capacity, requested_delta)
        elif requested_delta < 0:
            delta = max(self.min_capacity - self.target_capacity, requested_delta)
        else:
            delta = 0

        # second, constrain the delta by the max weight to change, depending on if it
        # it is positive or negative.
        if delta > 0:
            delta = min(self.max_weight_to_add, delta)
        elif delta < 0:
            delta = max(-self.max_weight_to_remove, delta)

        constrained_target_capacity = self.target_capacity + delta
        if requested_delta != delta:
            if force:
                forced_target_capacity = self.target_capacity + requested_delta
                logger.warning(
                    f'Forcing target capacity to {forced_target_capacity} even though '
                    f'scaling limits would restrict to {constrained_target_capacity}.'
                )
                return forced_target_capacity
            else:
                logger.warning(
                    f'Requested target capacity {requested_target_capacity}; '
                    f'restricting to {constrained_target_capacity} due to scaling limits.'
                )
        return constrained_target_capacity

    def _choose_nodes_to_prune(
        self,
        new_target_capacity: float,
        group_targets: Optional[Mapping[str, float]],
    ) -> Mapping[str, List[ClusterNodeMetadata]]:
        """ Choose nodes to kill in order to decrease the capacity on the cluster.

        The number of tasks killed is limited by self.max_tasks_to_kill, and the nodes are terminated in an order
        which (hopefully) reduces the impact on jobs running on the cluster.

        :param new_target_capacity: The total new target capacity for the pool. Most of the time, this is equal to
            self.target_capacity, but in some situations (such as when all resource groups are stale),
            modify_target_capacity cannot make self.target_capacity equal new_target_capacity. We'd rather this method
            aim for the actual target value.
        :param group_targets: a list of new resource group target_capacities; if None, use the existing
            target_capacities (this parameter is necessary in order for dry runs to work correctly)
        :returns: a dict of resource group ids -> list of nodes to terminate
        """

        # If dry_run is True in modify_target_capacity, the resource group target_capacity values will not have changed,
        # so this function would not choose to terminate any nodes (see case #2 in the while loop below).  So
        # instead we take a list of new target capacities to use in this computation.
        #
        # We leave the option for group_targets to be None in the event that we want to call
        # prune_excess_fulfilled_capacity outside the context of a modify_target_capacity call
        if not group_targets:
            group_targets = {group_id: rg.target_capacity for group_id, rg in self.resource_groups.items()}

        curr_capacity = self.fulfilled_capacity
        # we use new_target_capacity instead of self.target_capacity here in case they are different (see docstring)
        if curr_capacity <= new_target_capacity:
            return {}

        prioritized_killable_nodes = self._get_prioritized_killable_nodes()
        logger.info('Killable instance IDs in kill order:\n{instance_ids}'.format(
            instance_ids=[node_metadata.instance.instance_id for node_metadata in prioritized_killable_nodes],
        ))

        if not prioritized_killable_nodes:
            return {}
        rem_group_capacities = {group_id: rg.fulfilled_capacity for group_id, rg in self.resource_groups.items()}

        # How much capacity is actually up and available in Mesos.
        remaining_non_orphan_capacity = self.non_orphan_fulfilled_capacity

        # Iterate through all of the idle agents and mark one at a time for removal until we reach our target capacity
        # or have reached our limit of tasks to kill.
        marked_nodes: Mapping[str, List[ClusterNodeMetadata]] = defaultdict(list)
        removed_weight, killed_task_count = 0.0, 0
        for node_metadata in prioritized_killable_nodes:
            # Try to mark the node for removal; this could fail in a few different ways:
            #  0) We've gone over our limit for max weight to remove
            #  1) The resource group the node belongs to can't be reduced further.
            #  2) Killing the node's tasks would take over the maximum number of tasks we are willing to kill.
            #  3) Killing the node would bring us under our target_capacity of non-orphaned nodes.
            # In each of the cases, the node has been removed from consideration and we jump to the next iteration.

            instance_id = node_metadata.instance.instance_id
            group_id = node_metadata.instance.group_id
            instance_weight = node_metadata.instance.weight

            new_group_capacity = rem_group_capacities[group_id] - instance_weight
            if instance_weight + removed_weight > self.max_weight_to_remove:  # case 0
                logger.info(
                    f'Killing instance {instance_id} with weight {instance_weight} would take us '
                    f'over our max_weight_to_remove of {self.max_weight_to_remove}. Skipping this instance.'
                )
                continue

            if new_group_capacity < group_targets[group_id]:  # case 1
                logger.info(
                    f'Resource group {group_id} is at target capacity; skipping {instance_id}'
                )
                continue

            if killed_task_count + node_metadata.agent.task_count > self.max_tasks_to_kill:  # case 2
                logger.info(
                    f'Killing instance {instance_id} with {node_metadata.agent.task_count} tasks would take us '
                    f'over our max_tasks_to_kill of {self.max_tasks_to_kill}. Skipping this instance.'
                )
                continue

            if node_metadata.agent.state != AgentState.ORPHANED:
                if (remaining_non_orphan_capacity - instance_weight < new_target_capacity):  # case 3
                    logger.info(
                        f'Killing instance {instance_id} with weight {instance_weight} would take us under '
                        f'our target_capacity for non-orphan boxes. Skipping this instance.'
                    )
                    continue

            logger.info(f'marking {instance_id} for termination')
            marked_nodes[group_id].append(node_metadata)
            rem_group_capacities[group_id] -= instance_weight
            curr_capacity -= instance_weight
            killed_task_count += node_metadata.agent.task_count
            removed_weight += instance_weight
            if node_metadata.agent.state != AgentState.ORPHANED:
                remaining_non_orphan_capacity -= instance_weight

            if curr_capacity <= new_target_capacity:
                logger.info("Seems like we've picked enough nodes to kill; finishing")
                break

        return marked_nodes

    def _compute_new_resource_group_targets(self, new_target_capacity: float) -> Mapping[str, float]:
        """ Compute a balanced distribution of target capacities for the resource groups in the cluster

        :param new_target_capacity: the desired new target capacity that needs to be distributed
        :returns: A list of target_capacity values, sorted in order of resource groups
        """

        stale_groups = [group for group in self.resource_groups.values() if group.is_stale]
        non_stale_groups = [group for group in self.resource_groups.values() if not group.is_stale]

        # If we're scaling down the logic is identical but reversed, so we multiply everything by -1
        coeff = -1 if new_target_capacity < self.target_capacity else 1
        targets: Dict[str, float] = {g.id: g.target_capacity for g in non_stale_groups}

        # For stale groups, we set target_capacity to 0. This is a noop on SpotFleetResourceGroup.
        for stale_group in stale_groups:
            targets[stale_group.id] = 0

        def is_constrained(group):
            if coeff > 0:
                return targets[group.id] + coeff > group.max_capacity
            else:
                return targets[group.id] + coeff < group.min_capacity

        while sum(targets.values()) * coeff < math.ceil(new_target_capacity) * coeff:
            try:
                group = sorted(
                    [g for g in non_stale_groups if not is_constrained(g)],
                    key=lambda g: (coeff * targets[g.id], g.id),
                )[0]
            except IndexError:
                logger.warning(' '.join([
                    'All resource groups are stale or constrained.',
                    f'The closest we could get to {new_target_capacity} is {sum(targets.values())}',
                ]))
                break

            targets[group.id] += coeff

        return targets

    def get_market_capacities(
        self,
        market_filter: Optional[Collection[InstanceMarket]] = None
    ) -> Mapping[InstanceMarket, float]:
        """ Return the total (fulfilled) capacities in the cluster across all resource groups

        :param market_filter: a set of :py:class:`.InstanceMarket` to filter by
        :returns: the total capacity in each of the specified markets
        """
        total_market_capacities: MutableMapping[InstanceMarket, float] = defaultdict(float)
        for group in self.resource_groups.values():
            for market, capacity in group.market_capacities.items():
                if not market_filter or market in market_filter:
                    total_market_capacities[market] += capacity
        return total_market_capacities

    def _get_prioritized_killable_nodes(self) -> List[ClusterNodeMetadata]:
        """Get a list of killable nodes in the cluster in the order in which they should be considered for
        termination.
        """
        killable_nodes = [
            metadata for metadata in self.get_node_metadatas(AWS_RUNNING_STATES)
            if self._is_node_killable(metadata)
        ]
        return self._prioritize_killable_nodes(killable_nodes)

    def _is_node_killable(self, node_metadata: ClusterNodeMetadata) -> bool:
        if node_metadata.agent.state == AgentState.UNKNOWN:
            return False
        elif not node_metadata.agent.is_safe_to_kill:
            return False
        elif self.max_tasks_to_kill > node_metadata.agent.task_count:
            return True
        else:
            return node_metadata.agent.task_count == 0

    def _prioritize_killable_nodes(self, killable_nodes: List[ClusterNodeMetadata]) -> List[ClusterNodeMetadata]:
        """Returns killable_nodes sorted with most-killable things first."""
        def sort_key(node_metadata: ClusterNodeMetadata) -> Tuple[int, int, int, int, int]:
            return (
                0 if node_metadata.agent.state == AgentState.ORPHANED else 1,
                0 if node_metadata.agent.state == AgentState.IDLE else 1,
                0 if node_metadata.instance.is_stale else 1,
                node_metadata.agent.batch_task_count,
                node_metadata.agent.task_count,
            )
        return sorted(
            killable_nodes,
            key=sort_key,
        )

    def _calculate_non_orphan_fulfilled_capacity(self) -> float:
        return sum(
            node_metadata.instance.weight
            for node_metadata in self.get_node_metadatas(AWS_RUNNING_STATES)
            if node_metadata.agent.state not in (AgentState.ORPHANED, AgentState.UNKNOWN)
        )

    @property
    def target_capacity(self) -> float:
        """ The target capacity is the *desired* weighted capacity for the given Mesos cluster pool.  There is no
        guarantee that the actual capacity will equal the target capacity.
        """
        non_stale_groups = [group for group in self.resource_groups.values() if not group.is_stale]
        if not non_stale_groups:
            raise AllResourceGroupsAreStaleError()
        return sum(group.target_capacity for group in non_stale_groups)

    @property
    def fulfilled_capacity(self) -> float:
        """ The fulfilled capacity is the *actual* weighted capacity for the given Mesos cluster pool at a particular
        point in time.  This may be equal to, above, or below the :attr:`target_capacity`, depending on the availability
        and state of AWS at the time.  In general, once the cluster has reached equilibrium, the fulfilled capacity will
        be greater than or equal to the target capacity.
        """
        return sum(group.fulfilled_capacity for group in self.resource_groups.values())
