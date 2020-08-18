from typing import List

from mypy_extensions import TypedDict


class AutoScalingInstanceConfig(TypedDict):
    InstanceId: str
    InstanceType: str
    WeightedCapacity: str


class LaunchTemplateDataConfig(TypedDict):
    InstanceType: str


class LaunchTemplateConfig(TypedDict):
    LaunchTemplateName: str
    LaunchTemplateData: LaunchTemplateDataConfig
    Version: str


class InstanceOverrideConfig(TypedDict):
    InstanceType: str
    WeightedCapacity: str


class MixedInstancesPolicyLaunchTemplateConfig(TypedDict):
    LaunchTemplateSpecification: LaunchTemplateConfig
    Overrides: List[InstanceOverrideConfig]


class MixedInstancesPolicyConfig(TypedDict):
    LaunchTemplate: MixedInstancesPolicyLaunchTemplateConfig


class AutoScalingGroupConfig(TypedDict):
    AvailabilityZones: List[str]
    DesiredCapacity: int
    Instances: List[AutoScalingInstanceConfig]
    LaunchTemplate: LaunchTemplateConfig
    MaxSize: int
    MinSize: int
    MixedInstancesPolicy: MixedInstancesPolicyConfig
