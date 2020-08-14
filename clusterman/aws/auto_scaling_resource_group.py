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
import pprint
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Sequence
from typing import Tuple

import colorlog

from clusterman.aws.aws_resource_group import AWSResourceGroup
from clusterman.aws.client import autoscaling
from clusterman.aws.client import ec2
from clusterman.aws.markets import InstanceMarket
from clusterman.aws.response_types import AutoScalingGroupConfig
from clusterman.aws.response_types import InstanceOverrideConfig
from clusterman.aws.response_types import LaunchTemplateConfig
from clusterman.util import ClustermanResources

_BATCH_MODIFY_SIZE = 200
CLUSTERMAN_STALE_TAG = 'clusterman:is_stale'

logger = colorlog.getLogger(__name__)


class AutoScalingResourceGroup(AWSResourceGroup):
    """
    Wrapper for AWS Auto Scaling Groups (ASGs)

    .. note:: ASGs track their size in terms of number of instances, meaning that two
    ASGs with different instance types can have the same capacity but very
    different quantities of resources.

    .. note:: Clusterman controls which instances to terminate in the event of scale
    in. As a result, ASGs must be set to protect instances from scale in, and
    AutoScalingResourceGroup will assume that instances are indeed protected.
    """

    def __init__(self, group_id: str) -> None:
        super().__init__(group_id)

        # Resource Groups are reloaded on every autoscaling run, so we just query
        # AWS data once and store them so we don't run into AWS request limits
        self._group_config = self._get_auto_scaling_group_config()
        self._launch_template_config, self._launch_template_overrides = self._get_launch_template_and_overrides()
        self._stale_instance_ids = self._get_stale_instance_ids()

    def market_weight(self, market: InstanceMarket) -> float:
        """ Returns the weight of a given market

        ASGs can be defined with different instance weights. If we can find
        the weight for a given instance type, we return it. Otherwise we
        default to 1.

        :param market: The market for which we want the weight for
        :returns: The weight of a given market
        """
        if market.az in self._group_config['AvailabilityZones']:
            for instance in self._group_config.get('Instances', []):
                if market.instance == instance.get('InstanceType'):
                    return int(instance.get('WeightedCapacity', '1'))
            return 1
        else:
            return 0

    def mark_stale(self, dry_run: bool) -> None:
        for i in range(0, len(self.instance_ids), _BATCH_MODIFY_SIZE):
            inst_list = self.instance_ids[i:i + _BATCH_MODIFY_SIZE]
            logger.info(f'Setting staleness tags for {inst_list}')
            if dry_run:
                continue

            ec2.create_tags(
                Resources=inst_list,
                Tags=[{
                    'Key': CLUSTERMAN_STALE_TAG,
                    'Value': 'True',
                }],
            )

    def modify_target_capacity(
        self,
        target_capacity: float,
        *,
        dry_run: bool = False,
        honor_cooldown: bool = False,
    ) -> None:
        """ Modify the desired capacity for the ASG.

        :param target_capacity: The new desired number of instances in th ASG.
            Must be such that the desired capacity is between the minimum and
            maximum capacities of the ASGs. The desired capacity will be rounded
            to the minimum or maximum otherwise, whichever is closer.
        :param dry_run: Boolean indicating whether or not to take action or just
            log
        :param honor_cooldown: Boolean for whether or not to wait for a period
            of time (cooldown, set in ASG config) after the previous scaling
            activity has completed before initiating this one. Defaults to False,
            which is the AWS default for manual scaling activities.
        """
        # We pretend like stale instances aren't in the ASG, but actually they are so
        # we have to double-count them in the target capacity computation
        target_capacity += self._stale_capacity

        # Round target_cpacity to min or max if necessary
        if target_capacity > self.max_capacity:
            logger.warning(
                f'New target_capacity={target_capacity} exceeds ASG MaxSize={self.max_capacity}, '
                'setting to max instead'
            )
            target_capacity = self.max_capacity
        elif target_capacity < self.min_capacity:
            logger.warning(
                f'New target_capacity={target_capacity} falls below ASG MinSize={self.min_capacity}, '
                'setting to min instead'
            )
            target_capacity = self.min_capacity

        kwargs = dict(
            AutoScalingGroupName=self.group_id,
            DesiredCapacity=int(target_capacity),
            HonorCooldown=honor_cooldown,
        )
        logger.info(
            'Setting target capacity for ASG with arguments:\n'
            f'{pprint.pformat(kwargs)}'
        )
        if dry_run:
            return

        autoscaling.set_desired_capacity(**kwargs)

    def scale_up_options(self) -> Iterator[ClustermanResources]:
        raise NotImplementedError()

    def scale_down_options(self) -> Iterator[ClustermanResources]:
        """ Generate each of the options for scaling down this resource group, i.e. the list of instance types currently
        running in this resource group.
        """
        raise NotImplementedError()

    def _get_auto_scaling_group_config(self) -> AutoScalingGroupConfig:
        response = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[self.group_id],
        )
        return response['AutoScalingGroups'][0]

    def _get_launch_template_and_overrides(self) -> Tuple[LaunchTemplateConfig, List[InstanceOverrideConfig]]:
        try:
            template = self._group_config['LaunchTemplate']
            overrides: List[InstanceOverrideConfig] = []
        except KeyError:
            policy = self._group_config['MixedInstancesPolicy']
            template = policy['LaunchTemplate']['LaunchTemplateSpecification']
            overrides = policy['LaunchTemplate']['Overrides']

        launch_template_name = template['LaunchTemplateName']
        launch_template_version = template['Version']

        response = ec2.describe_launch_template_versions(
            LaunchTemplateName=launch_template_name,
            Versions=[launch_template_version],
        )
        return response['LaunchTemplateVersions'][0], overrides

    def _get_stale_instance_ids(self) -> List[str]:
        response = ec2.describe_tags(
            Filters=[
                {
                    'Name': 'key',
                    'Values': [CLUSTERMAN_STALE_TAG],
                },
                {
                    'Name': 'value',
                    'Values': ['True'],
                },
            ]
        )
        return [item['ResourceId'] for item in response.get('Tags', []) if item['ResourceId'] in self.instance_ids]

    @property
    def min_capacity(self) -> int:
        return self._group_config['MinSize']

    @property
    def max_capacity(self) -> int:
        return self._group_config['MaxSize']

    @property
    def instance_ids(self) -> Sequence[str]:
        return [
            inst['InstanceId']
            for inst in self._group_config.get('Instances', [])
            if inst is not None
        ]

    @property
    def stale_instance_ids(self) -> Sequence[str]:
        return self._stale_instance_ids

    @property
    def fulfilled_capacity(self) -> float:
        return sum(
            [
                int(instance.get('WeightedCapacity', '1'))
                for instance in self._group_config.get('Instances', [])
            ]
        )

    @property
    def status(self) -> str:
        """ The status of the ASG

        If all the instances are stale, then the ASG is 'stale'; otherwise, if only some instances
        are stale, it is 'rolling', and otherwise it is 'active'.
        """
        if len(self.stale_instance_ids) > 0:
            return 'rolling'
        else:
            return 'active'

    @property
    def is_stale(self) -> bool:
        """ Whether or not the ASG is stale

        An ASG is never stale; even if all the instances in it are stale, that means we still
        want Clusterman to track the existence of this specific ASG and replace the instances in it.
        Staleness by definition means the resource group should go away after we clean it up.
        """
        return False

    @property
    def _target_capacity(self) -> float:
        # We pretend like stale instances aren't in the ASG, but actually they are so
        # we have to remove them manually from the existing target capacity
        return self._group_config['DesiredCapacity'] - self._stale_capacity

    @classmethod
    def _get_resource_group_tags(cls) -> Mapping[str, Mapping[str, str]]:
        """ Retrieves the tags for each ASG """
        asg_id_to_tags = {}
        for page in autoscaling.get_paginator('describe_auto_scaling_groups').paginate():
            for asg in page['AutoScalingGroups']:
                tags_dict = {tag['Key']: tag['Value'] for tag in asg['Tags']}
                asg_id_to_tags[asg['AutoScalingGroupName']] = tags_dict
        return asg_id_to_tags

    @property
    def _stale_capacity(self) -> float:
        return sum(
            [int(instance.get('WeightedCapacity', '1'))
             for instance in self._group_config.get('Instances', [])
             if instance['InstanceId'] in self.stale_instance_ids]
        )
