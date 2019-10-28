from typing import Mapping
from typing import MutableMapping
from typing import Sequence

import botocore
import colorlog
import simplejson as json
from cached_property import timed_cached_property

from clusterman.aws import CACHE_TTL_SECONDS
from clusterman.aws.aws_resource_group import AWSResourceGroup
from clusterman.aws.client import ec2
from clusterman.aws.client import ec2_describe_fleet_instances
from clusterman.aws.markets import get_market
from clusterman.aws.markets import InstanceMarket
from clusterman.exceptions import ResourceGroupError


logger = colorlog.getLogger(__name__)
_CANCELLED_STATES = ('deleted', 'deleted-terminating', 'failed')


class EC2FleetResourceGroup(AWSResourceGroup):
    def __init__(self, group_id: str) -> None:
        super().__init__(group_id)

        # Can't change the WeightedCapacity of EC2Fleets, so cache them here for frequent access
        self._market_weights = self._generate_market_weights()

    def market_weight(self, market: InstanceMarket) -> float:
        return self._market_weights.get(market, 1)

    def modify_target_capacity(
        self,
        target_capacity: float,
        *,
        dry_run: bool = False,
    ) -> None:
        if self.is_stale:
            logger.info(f'Not modifying EC2 fleet since it is in state {self.status}')
            return

        kwargs = {
            'FleetId': self.group_id,
            'TargetCapacitySpecification': {
                'TotalTargetCapacity': int(target_capacity),
            },
            'ExcessCapacityTerminationPolicy': 'no-termination',
        }
        logger.info(f'Modifying spot fleet request with arguments: {kwargs}')
        if dry_run:
            return

        response = ec2.modify_fleet(**kwargs)
        if not response['Return']:
            logger.critical('Could not change size of spot fleet:\n{resp}'.format(resp=json.dumps(response)))
            raise ResourceGroupError('Could not change size of spot fleet: check logs for details')

    @timed_cached_property(ttl=CACHE_TTL_SECONDS)
    def instance_ids(self) -> Sequence[str]:
        """ Responses from this API call are cached to prevent hitting any AWS request limits """
        return [instance['InstanceId'] for instance in ec2_describe_fleet_instances(self.group_id)]

    @property
    def fulfilled_capacity(self) -> float:
        return self._configuration['FulfilledCapacity']

    @property
    def status(self) -> str:
        return self._configuration['FleetState']

    @property
    def is_stale(self) -> bool:
        try:
            return self.status.startswith('deleted')
        except botocore.exceptions.ClientError as e:
            if e.response.get('Error', {}).get('Code', 'Unknown') == 'TODO':
                return True
            raise e

    def _generate_market_weights(self) -> Mapping[InstanceMarket, float]:
        market_weights: MutableMapping[InstanceMarket, float] = {}
        for launch_template_config in self._configuration['LaunchTemplateConfigs']:
            instance_type, subnet_id = None, None
            for override in launch_template_config['Overrides']:
                instance_type = override.get('InstanceType')
                subnet_id = override.get('SubnetId')

            if not (instance_type and subnet_id):
                spec = launch_template_config['LaunchTemplateSpecification']
                launch_template_data = ec2.describe_launch_template_versions(
                    LaunchTemplateId=spec['LaunchTemplateId'],
                    Versions=[spec['Version']],
                )[0]['LaunchTemplateData']

                if not instance_type:
                    instance_type = launch_template_data['InstanceType']

                if not subnet_id:
                    subnet_id = launch_template_data['NetworkInterfaces']['SubnetId']

            market_weights[get_market(instance_type, subnet_id)] = override['WeightedCapacity']
        return market_weights

    @property
    def _target_capacity(self) -> float:
        return self._configuration['TargetCapacitySpecification']['TotalTargetCapacity']

    @timed_cached_property(ttl=CACHE_TTL_SECONDS)
    def _configuration(self):
        """ Responses from this API call are cached to prevent hitting any AWS request limits """
        fleet_configuration = ec2.describe_fleets(FleetIds=[self.group_id])
        return fleet_configuration['Fleets'][0]

    @classmethod
    def _get_resource_group_tags(cls) -> Mapping[str, Mapping[str, str]]:
        fleet_id_to_tags = {}
        for page in ec2.get_paginator('describe_fleets').paginate():
            for fleet in page['Fleets']:
                if fleet['FleetState'] in _CANCELLED_STATES:
                    continue
                if 'Tags' in fleet:
                    tags_dict = {tag['Key']: tag['Value'] for tag in fleet['Tags']}
                    fleet_id_to_tags[fleet['FleetId']] = tags_dict
        return fleet_id_to_tags
