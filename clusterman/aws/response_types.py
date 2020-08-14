from typing import List

from mypy_extensions import TypedDict


class AutoScalingInstanceConfig(TypedDict):
    InstanceId: str
    InstanceType: str
    WeightedCapacity: str


class LaunchTemplateConfig(TypedDict):
    LaunchTemplateName: str
    Version: str


class MixedInstancesPolicyLaunchTemplateConfig(TypedDict):
    LaunchTemplateSpecification: LaunchTemplateConfig


class InstanceOverrideConfig(TypedDict):
    InstanceType: str
    WeightedCapacity: str


class MixedInstancesPolicyConfig(TypedDict):
    LaunchTemplate: MixedInstancesPolicyLaunchTemplateConfig
    Overrides: List[InstanceOverrideConfig]


class AutoScalingGroupConfig(TypedDict):
    AvailabilityZones: List[str]
    DesiredCapacity: int
    Instances: List[AutoScalingInstanceConfig]
    LaunchTemplate: LaunchTemplateConfig
    MaxSize: int
    MinSize: int
    MixedInstancesPolicy: MixedInstancesPolicyConfig
