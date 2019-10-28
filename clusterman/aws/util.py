from typing import Mapping
from typing import Type

from clusterman.aws.auto_scaling_resource_group import AutoScalingResourceGroup
from clusterman.aws.aws_resource_group import AWSResourceGroup
from clusterman.aws.ec2_fleet_resource_group import EC2FleetResourceGroup
from clusterman.aws.spot_fleet_resource_group import SpotFleetResourceGroup


RESOURCE_GROUPS: Mapping[
    str,
    Type[AWSResourceGroup]
] = {
    'asg': AutoScalingResourceGroup,
    'fleet': EC2FleetResourceGroup,
    'sfr': SpotFleetResourceGroup,
}
RESOURCE_GROUPS_REV: Mapping[
    Type[AWSResourceGroup],
    str
] = {v: k for k, v in RESOURCE_GROUPS.items()}
