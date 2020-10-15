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
from typing import Any
from typing import Iterable
from typing import Mapping
from typing import Sequence

import botocore
import colorlog
import simplejson as json
from cachetools.func import ttl_cache
from mypy_extensions import TypedDict

from clusterman.aws.aws_resource_group import AWSResourceGroup
from clusterman.aws.aws_resource_group import RESOURCE_GROUP_CACHE_SECONDS
from clusterman.aws.client import ec2
from clusterman.aws.client import s3
from clusterman.aws.markets import get_instance_market
from clusterman.aws.markets import InstanceMarket
from clusterman.exceptions import ResourceGroupError
from clusterman.interfaces.types import ClusterNodeMetadata

logger = colorlog.getLogger(__name__)
_CANCELLED_STATES = ('cancelled', 'cancelled_terminating')

_S3Config = TypedDict(
    '_S3Config',
    {
        'bucket': str,
        'prefix': str,
    }
)

SpotFleetResourceGroupConfig = TypedDict(
    'SpotFleetResourceGroupConfig',
    {
        's3': _S3Config,
        'tag': str,
    }
)


class SpotFleetResourceGroup(AWSResourceGroup):

    def __init__(self, group_id: str) -> None:
        super().__init__(group_id)

    def market_weight(self, market: InstanceMarket) -> float:
        return self._market_weights.get(market, 1)

    def modify_target_capacity(
        self,
        target_capacity: float,
        *,
        dry_run: bool = False,
    ) -> None:
        if self.is_stale:
            logger.info(f'Not modifying spot fleet request since it is in state {self.status}')
            return
        kwargs = {
            'SpotFleetRequestId': self.group_id,
            'TargetCapacity': int(target_capacity),
            'ExcessCapacityTerminationPolicy': 'NoTermination',
        }
        logger.info(f'Modifying spot fleet request with arguments: {kwargs}')
        if dry_run:
            return

        try:
            response = ec2.modify_spot_fleet_request(**kwargs)
        except botocore.exceptions.ClientError as e:
            raise ResourceGroupError('Could not change size of spot fleet') from e

        if not response['Return']:
            raise ResourceGroupError('Could not change size of spot fleet')

    @property
    def instance_ids(self) -> Sequence[str]:
        return self._instance_ids

    @property
    def fulfilled_capacity(self) -> float:
        if not self._configuration:
            return 0
        return self._configuration['SpotFleetRequestConfig']['FulfilledCapacity']

    @property
    def status(self) -> str:
        if not self._configuration:
            return 'cancelled'
        return self._configuration['SpotFleetRequestState']

    @property
    def is_stale(self) -> bool:
        return self.status.startswith('cancelled')

    def _reload_resource_group(self):
        try:
            self._configuration = self._get_sfr_configuration()
        except botocore.exceptions.ClientError as e:
            if e.response.get('Error', {}).get('Code', 'Unknown') == 'InvalidSpotFleetRequestId.NotFound':
                self._configuration = None
            else:
                raise e
        self._instance_ids = self._get_instance_ids()
        self._market_weights = self._generate_market_weights()

    def _generate_market_weights(self) -> Mapping[InstanceMarket, float]:
        if not self._configuration:
            return {}

        return {
            get_instance_market(spec): spec['WeightedCapacity']
            for spec in self._configuration['SpotFleetRequestConfig']['LaunchSpecifications']
        }

    def _get_sfr_configuration(self):
        """ Responses from this API call are cached to prevent hitting any AWS request limits """
        fleet_configuration = ec2.describe_spot_fleet_requests(SpotFleetRequestIds=[self.group_id])
        return fleet_configuration['SpotFleetRequestConfigs'][0]

    def _get_instance_ids(self) -> Sequence[str]:
        """ Responses from this API call are cached to prevent hitting any AWS request limits """
        return [
            instance['InstanceId']
            for page in ec2.get_paginator('describe_spot_fleet_instances').paginate(SpotFleetRequestId=self.group_id)
            for instance in page['ActiveInstances']
            if instance is not None
        ]

    @property
    def _target_capacity(self) -> float:
        if not self._configuration:
            return 0
        return self._configuration['SpotFleetRequestConfig']['TargetCapacity']

    @classmethod
    def load(
        cls,
        cluster: str,
        pool: str,
        config: SpotFleetResourceGroupConfig,
        **kwargs: Any,
    ) -> Mapping[str, AWSResourceGroup]:
        """ Loads a list of spot fleets in the given cluster and pool

        :param cluster: A cluster name
        :param pool: A pool name
        :param config: An spot fleet config
        :returns: A dictionary of spot fleet resource groups, indexed by the id
        """
        tagged_resource_groups = super().load(cluster, pool, config)
        if 's3' in config:
            s3_resource_groups = load_spot_fleets_from_s3(
                config['s3']['bucket'],
                config['s3']['prefix'],
                pool=pool,
            )
            logger.info(f'SFRs loaded from s3: {list(s3_resource_groups)}')
        else:
            s3_resource_groups = {}

        resource_groups = {
            sfr_id: sfrg for sfr_id, sfrg in {**tagged_resource_groups, **s3_resource_groups}.items()
            if sfrg.status not in _CANCELLED_STATES
        }
        logger.info(f'Merged ec2 & s3 SFRs: {list(resource_groups)}')
        return resource_groups

    @classmethod
    @ttl_cache(ttl=RESOURCE_GROUP_CACHE_SECONDS)
    def _get_resource_group_tags(cls) -> Mapping[str, Mapping[str, str]]:
        """ Gets a dictionary of SFR id -> a dictionary of tags. The tags are taken
        from the TagSpecifications for the first LaunchSpecification
        """
        spot_fleet_requests = ec2.describe_spot_fleet_requests()
        sfr_id_to_tags = {}
        for sfr_config in spot_fleet_requests['SpotFleetRequestConfigs']:
            launch_specs = sfr_config['SpotFleetRequestConfig']['LaunchSpecifications']
            try:
                # we take the tags from the 0th launch spec for now
                # they should always be identical in every launch spec
                tags = launch_specs[0]['TagSpecifications'][0]['Tags']
            except (IndexError, KeyError):
                # if this SFR is misssing the TagSpecifications
                tags = []
            tags_dict = {tag['Key']: tag['Value'] for tag in tags}
            sfr_id_to_tags[sfr_config['SpotFleetRequestId']] = tags_dict
        return sfr_id_to_tags

    def scale_up_options(self) -> Iterable[ClusterNodeMetadata]:
        """ Generate each of the options for scaling up this resource group. For a spot fleet, this would be one
        ClustermanResources for each instance type. For a non-spot ASG, this would be a single ClustermanResources that
        represents the instance type the ASG is configured to run.
        """
        raise NotImplementedError()

    def scale_down_options(self) -> Iterable[ClusterNodeMetadata]:
        """ Generate each of the options for scaling down this resource group, i.e. the list of instance types currently
        running in this resource group.
        """
        raise NotImplementedError()


def load_spot_fleets_from_s3(bucket: str, prefix: str, pool: str = None) -> Mapping[str, SpotFleetResourceGroup]:
    prefix = prefix.rstrip('/') + '/'
    object_list = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    spot_fleets = {}
    for obj_metadata in object_list['Contents']:
        obj = s3.get_object(Bucket=bucket, Key=obj_metadata['Key'])
        sfr_metadata = json.load(obj['Body'])
        for resource_key, resource in sfr_metadata['cluster_autoscaling_resources'].items():
            if not resource_key.startswith('aws_spot_fleet_request'):
                continue
            if pool and resource['pool'] != pool:
                continue

            spot_fleets[resource['id']] = SpotFleetResourceGroup(resource['id'])

    return spot_fleets
