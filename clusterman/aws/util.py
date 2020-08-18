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
from typing import Mapping
from typing import Type

from clusterman.aws.auto_scaling_resource_group import AutoScalingResourceGroup
from clusterman.aws.aws_resource_group import AWSResourceGroup
from clusterman.aws.spot_fleet_resource_group import SpotFleetResourceGroup


RESOURCE_GROUPS: Mapping[
    str,
    Type[AWSResourceGroup]
] = {
    'asg': AutoScalingResourceGroup,
    'sfr': SpotFleetResourceGroup,
}
RESOURCE_GROUPS_REV: Mapping[
    Type[AWSResourceGroup],
    str
] = {v: k for k, v in RESOURCE_GROUPS.items()}
