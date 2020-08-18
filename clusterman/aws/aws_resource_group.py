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
from abc import ABCMeta
from abc import abstractproperty
from collections import defaultdict
from socket import gethostbyaddr
from typing import Any
from typing import Collection
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence

import arrow
import colorlog
import simplejson as json
from cached_property import timed_cached_property

from clusterman.aws import CACHE_TTL_SECONDS
from clusterman.aws.client import ec2
from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.markets import get_instance_market
from clusterman.aws.markets import InstanceMarket
from clusterman.interfaces.resource_group import InstanceMetadata
from clusterman.interfaces.resource_group import ResourceGroup


logger = colorlog.getLogger(__name__)


def protect_unowned_instances(func):
    """ A decorator that protects instances that are not owned by a particular AWSResourceGroup from being modified

    It is assumed that the decorated function takes a list of instance IDs as its first argument; this list
    is modified before the decorated function is called to strip out any unowned instances.  In this case a warning
    is logged.
    """

    def wrapper(self, instance_ids, *args, **kwargs):
        resource_group_instances = list(set(instance_ids) & set(self.instance_ids))
        invalid_instances = set(instance_ids) - set(self.instance_ids)
        if invalid_instances:
            logger.warning(f'Some instances are not part of this resource group ({self.id}):\n{invalid_instances}')
        return func(self, resource_group_instances, *args, **kwargs)
    return wrapper


class AWSResourceGroup(ResourceGroup, metaclass=ABCMeta):
    def __init__(self, group_id: str, **kwargs: Any) -> None:
        self.group_id = group_id

    def get_instance_metadatas(self, state_filter: Optional[Collection[str]] = None) -> Sequence[InstanceMetadata]:
        instance_metadatas = []
        for instance_dict in ec2_describe_instances(instance_ids=self.instance_ids):
            aws_state = instance_dict['State']['Name']
            if state_filter and aws_state not in state_filter:
                continue

            instance_market = get_instance_market(instance_dict)
            instance_ip = instance_dict.get('PrivateIpAddress')
            hostname = gethostbyaddr(instance_ip)[0] if instance_ip else None

            metadata = InstanceMetadata(
                group_id=self.id,
                hostname=hostname,
                instance_id=instance_dict['InstanceId'],
                ip_address=instance_ip,
                is_stale=(instance_dict['InstanceId'] in self.stale_instance_ids),
                market=instance_market,
                state=aws_state,
                uptime=(arrow.now() - arrow.get(instance_dict['LaunchTime'])),
                weight=self.market_weight(instance_market),
            )
            instance_metadatas.append(metadata)
        return instance_metadatas

    def market_weight(self, market: InstanceMarket) -> float:
        # some types of resource groups don't understand weights, so default to 1 for every market
        return 1

    @protect_unowned_instances
    def terminate_instances_by_id(self, instance_ids: List[str], batch_size: int = 500) -> Sequence[str]:
        """ Terminate instances in this resource group

        :param instance_ids: a list of instance IDs to terminate
        :param batch_size: number of instances to terminate at one time
        :returns: a list of terminated instance IDs
        """
        if not instance_ids:
            logger.warning(f'No instances to terminate in {self.group_id}')
            return []

        instance_weights = {}
        for instance in ec2_describe_instances(instance_ids):
            instance_market = get_instance_market(instance)
            if not instance_market.az:
                logger.warning(
                    f"Instance {instance['InstanceId']} missing AZ info, likely already terminated so skipping",
                )
                instance_ids.remove(instance['InstanceId'])
                continue
            instance_weights[instance['InstanceId']] = self.market_weight(get_instance_market(instance))

        # AWS API recommends not terminating more than 1000 instances at a time, and to
        # terminate larger numbers in batches
        terminated_instance_ids = []
        for batch in range(0, len(instance_ids), batch_size):
            response = ec2.terminate_instances(InstanceIds=instance_ids[batch:batch + batch_size])
            terminated_instance_ids.extend([instance['InstanceId'] for instance in response['TerminatingInstances']])

        # It's possible that not every instance is terminated.  The most likely cause for this
        # is that AWS terminated the instance in between getting its status and the terminate_instances
        # request.  This is probably fine but let's log a warning just in case.
        missing_instances = set(instance_ids) - set(terminated_instance_ids)
        if missing_instances:
            logger.warning('Some instances could not be terminated; they were probably killed previously')
            logger.warning(f'Missing instances: {list(missing_instances)}')
        terminated_capacity = sum(instance_weights[i] for i in instance_ids)

        logger.info(f'{self.id} terminated weight: {terminated_capacity}; instances: {terminated_instance_ids}')
        return terminated_instance_ids

    @property
    def id(self) -> str:
        """ A unique identifier for this AWSResourceGroup """
        return self.group_id

    @property
    def market_capacities(self) -> Mapping[InstanceMarket, float]:
        return {
            market: len(instances) * self.market_weight(market)
            for market, instances in self._instances_by_market.items()
            if market.az
        }

    @property
    def target_capacity(self) -> float:
        """ The target (or desired) weighted capacity for this AWSResourceGroup

        Note that the actual weighted capacity in the AWSResourceGroup may be smaller or larger than the
        target capacity, depending on the state of the AWSResourceGroup, available instance types, and
        previous operations; use self.fulfilled_capacity to get the actual capacity
        """
        if self.is_stale:
            # If we're in cancelled, cancelled_running, or cancelled_terminated, then no more instances will be
            # launched. This is effectively a target_capacity of 0, so let's just pretend like it is.
            return 0
        return self._target_capacity

    @timed_cached_property(ttl=CACHE_TTL_SECONDS)
    def _instances_by_market(self):
        """ Responses from this API call are cached to prevent hitting any AWS request limits """
        instance_dict: Mapping[InstanceMarket, List[Mapping]] = defaultdict(list)
        for instance in ec2_describe_instances(self.instance_ids):
            instance_dict[get_instance_market(instance)].append(instance)
        return instance_dict

    @abstractproperty
    def _target_capacity(self):  # pragma: no cover
        pass

    @classmethod
    def load(cls, cluster: str, pool: str, config: Any, **kwargs: Any) -> Mapping[str, 'AWSResourceGroup']:
        """ Load a list of corresponding resource groups

        :param cluster: a cluster name
        :param pool: a pool name
        :param config: a config specific to a resource group type
        :returns: a dictionary of resource groups, indexed by id
        """
        resource_group_tags = cls._get_resource_group_tags()
        matching_resource_groups = {}

        try:
            identifier_tag_label = config['tag']
        except KeyError:
            return {}

        for rg_id, tags in resource_group_tags.items():
            try:
                tag_json = tags.get(identifier_tag_label)
                # Not every ASG/SFR/etc will have the right tags, because they belong to someone else
                if tag_json:
                    identifier_tags = json.loads(tag_json)
                    if identifier_tags['pool'] == pool and identifier_tags['paasta_cluster'] == cluster:
                        rg = cls(rg_id, **kwargs)
                        matching_resource_groups[rg_id] = rg
            except Exception:
                logger.exception(f'Could not load resource group {rg_id}; skipping...')
                continue
        return matching_resource_groups

    @classmethod
    def _get_resource_group_tags(cls) -> Mapping[str, Mapping[str, str]]:  # pragma: no cover
        return {}
