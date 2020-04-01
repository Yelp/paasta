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
from functools import lru_cache
from typing import List
from typing import Mapping
from typing import NamedTuple
from typing import Optional

from mypy_extensions import TypedDict

from clusterman.aws.client import ec2


class InstanceResources(NamedTuple):
    cpus: float
    mem: float
    disk: Optional[float]
    gpus: int


class _InstanceMarket(NamedTuple):
    instance: str
    az: Optional[str]


class MarketDict(TypedDict):
    InstanceType: str
    SubnetId: str
    Placement: Mapping


class InstanceMarket(_InstanceMarket):
    __slots__ = ()

    def __new__(cls, instance: str, az: Optional[str]):
        if (instance in EC2_INSTANCE_TYPES and az in EC2_AZS):
            return super().__new__(cls, instance, az)
        else:
            raise ValueError(f'Invalid AWS market specified: <{instance}, {az}> (choices from {EC2_AZS})')

    def __repr__(self) -> str:
        return f'<{self.instance}, {self.az}>'

    @classmethod
    def parse(cls, string: str):
        sans_brackets = string[1:-1]
        return cls(*sans_brackets.split(', '))


EC2_INSTANCE_TYPES: Mapping[str, InstanceResources] = {
    't2.nano': InstanceResources(1.0, 0.5, None, 0),
    't2.micro': InstanceResources(1.0, 1.0, None, 0),
    't2.small': InstanceResources(1.0, 2.0, None, 0),
    't2.medium': InstanceResources(2.0, 4.0, None, 0),
    't2.large': InstanceResources(2.0, 8.0, None, 0),
    't2.xlarge': InstanceResources(4.0, 16.0, None, 0),
    't2.2xlarge': InstanceResources(8.0, 32.0, None, 0),
    'm5.large': InstanceResources(2.0, 8.0, None, 0),
    'm5.xlarge': InstanceResources(4.0, 16.0, None, 0),
    'm5.2xlarge': InstanceResources(8.0, 32.0, None, 0),
    'm5.4xlarge': InstanceResources(16.0, 64.0, None, 0),
    'm5.8xlarge': InstanceResources(32.0, 128.0, None, 0),
    'm5.12xlarge': InstanceResources(48.0, 192.0, None, 0),
    'm5.16xlarge': InstanceResources(64.0, 256.0, None, 0),
    'm5.24xlarge': InstanceResources(96.0, 384.0, None, 0),
    'm5a.large': InstanceResources(2.0, 8.0, None, 0),
    'm5a.xlarge': InstanceResources(4.0, 16.0, None, 0),
    'm5a.2xlarge': InstanceResources(8.0, 32.0, None, 0),
    'm5a.4xlarge': InstanceResources(16.0, 64.0, None, 0),
    'm5a.8xlarge': InstanceResources(32.0, 128.0, None, 0),
    'm5a.12xlarge': InstanceResources(48.0, 192.0, None, 0),
    'm5a.16xlarge': InstanceResources(64.0, 256.0, None, 0),
    'm5a.24xlarge': InstanceResources(96.0, 384.0, None, 0),
    'm5ad.large': InstanceResources(2.0, 8.0, 75.0, 0),
    'm5ad.xlarge': InstanceResources(4.0, 16.0, 150.0, 0),
    'm5ad.2xlarge': InstanceResources(8.0, 32.0, 300.0, 0),
    'm5ad.4xlarge': InstanceResources(16.0, 64.0, 600.0, 0),
    'm5ad.8xlarge': InstanceResources(32.0, 128.0, 1200.0, 0),
    'm5ad.12xlarge': InstanceResources(48.0, 192.0, 1800.0, 0),
    'm5ad.16xlarge': InstanceResources(64.0, 256.0, 2400.0, 0),
    'm5ad.24xlarge': InstanceResources(96.0, 384.0, 3600.0, 0),
    'm5d.large': InstanceResources(2.0, 8.0, 75.0, 0),
    'm5d.xlarge': InstanceResources(4.0, 16.0, 150.0, 0),
    'm5d.2xlarge': InstanceResources(8.0, 32.0, 300.0, 0),
    'm5d.4xlarge': InstanceResources(16.0, 64.0, 600.0, 0),
    'm5d.8xlarge': InstanceResources(32.0, 128.0, 1200.0, 0),
    'm5d.12xlarge': InstanceResources(48.0, 192.0, 1800.0, 0),
    'm5d.16xlarge': InstanceResources(64.0, 256.0, 2400.0, 0),
    'm5d.24xlarge': InstanceResources(96.0, 384.0, 3600.0, 0),
    'm5dn.large': InstanceResources(2.0, 8.0, 75.0, 0),
    'm5dn.xlarge': InstanceResources(4.0, 16.0, 150.0, 0),
    'm5dn.2xlarge': InstanceResources(8.0, 32.0, 300.0, 0),
    'm5dn.4xlarge': InstanceResources(16.0, 64.0, 600.0, 0),
    'm5dn.8xlarge': InstanceResources(32.0, 128.0, 1200.0, 0),
    'm5dn.12xlarge': InstanceResources(48.0, 192.0, 1800.0, 0),
    'm5dn.16xlarge': InstanceResources(64.0, 256.0, 2400.0, 0),
    'm5dn.24xlarge': InstanceResources(96.0, 384.0, 3600.0, 0),
    'm5n.large': InstanceResources(2.0, 8.0, None, 0),
    'm5n.xlarge': InstanceResources(4.0, 16.0, None, 0),
    'm5n.2xlarge': InstanceResources(8.0, 32.0, None, 0),
    'm5n.4xlarge': InstanceResources(16.0, 64.0, None, 0),
    'm5n.8xlarge': InstanceResources(32.0, 128.0, None, 0),
    'm5n.12xlarge': InstanceResources(48.0, 192.0, None, 0),
    'm5n.16xlarge': InstanceResources(64.0, 256.0, None, 0),
    'm5n.24xlarge': InstanceResources(96.0, 384.0, None, 0),
    'm4.large': InstanceResources(2.0, 8.0, None, 0),
    'm4.xlarge': InstanceResources(4.0, 16.0, None, 0),
    'm4.2xlarge': InstanceResources(8.0, 32.0, None, 0),
    'm4.4xlarge': InstanceResources(16.0, 64.0, None, 0),
    'm4.10xlarge': InstanceResources(40.0, 160.0, None, 0),
    'm4.16xlarge': InstanceResources(64.0, 256.0, None, 0),
    'm3.medium': InstanceResources(1.0, 3.75, 4.0, 0),
    'm3.large': InstanceResources(2.0, 7.5, 32.0, 0),
    'm3.xlarge': InstanceResources(4.0, 15.0, 80.0, 0),
    'm3.2xlarge': InstanceResources(8.0, 30.0, 160.0, 0),
    'c5.large': InstanceResources(2.0, 4.0, None, 0),
    'c5.xlarge': InstanceResources(4.0, 8.0, None, 0),
    'c5.2xlarge': InstanceResources(8.0, 16.0, None, 0),
    'c5.4xlarge': InstanceResources(16.0, 32.0, None, 0),
    'c5.9xlarge': InstanceResources(36.0, 72.0, None, 0),
    'c5.12xlarge': InstanceResources(48.0, 96.0, None, 0),
    'c5.18xlarge': InstanceResources(72.0, 144.0, None, 0),
    'c5.24xlarge': InstanceResources(96.0, 192.0, None, 0),
    'c5d.large': InstanceResources(2.0, 4.0, 50.0, 0),
    'c5d.xlarge': InstanceResources(4.0, 8.0, 100.0, 0),
    'c5d.2xlarge': InstanceResources(8.0, 16.0, 200.0, 0),
    'c5d.4xlarge': InstanceResources(16.0, 32.0, 400.0, 0),
    'c5d.9xlarge': InstanceResources(36.0, 72.0, 900.0, 0),
    'c5d.12xlarge': InstanceResources(48.0, 96.0, 1800.0, 0),
    'c5d.18xlarge': InstanceResources(72.0, 144.0, 1800.0, 0),
    'c5d.24xlarge': InstanceResources(96.0, 192.0, 3600.0, 0),
    'c5n.large': InstanceResources(2.0, 4.0, None, 0),
    'c5n.xlarge': InstanceResources(4.0, 8.0, None, 0),
    'c5n.2xlarge': InstanceResources(8.0, 16.0, None, 0),
    'c5n.4xlarge': InstanceResources(16.0, 32.0, None, 0),
    'c5n.9xlarge': InstanceResources(36.0, 72.0, None, 0),
    'c5n.12xlarge': InstanceResources(48.0, 96.0, None, 0),
    'c5n.18xlarge': InstanceResources(72.0, 144.0, None, 0),
    'c5n.24xlarge': InstanceResources(96.0, 192.0, None, 0),
    'c4.large': InstanceResources(2.0, 3.75, None, 0),
    'c4.xlarge': InstanceResources(4.0, 7.5, None, 0),
    'c4.2xlarge': InstanceResources(8.0, 15.0, None, 0),
    'c4.4xlarge': InstanceResources(16.0, 30.0, None, 0),
    'c4.8xlarge': InstanceResources(36.0, 60.0, None, 0),
    'c3.large': InstanceResources(2.0, 3.75, 32.0, 0),
    'c3.xlarge': InstanceResources(4.0, 7.5, 80.0, 0),
    'c3.2xlarge': InstanceResources(8.0, 15.0, 160.0, 0),
    'c3.4xlarge': InstanceResources(16.0, 30.0, 320.0, 0),
    'c3.8xlarge': InstanceResources(32.0, 60.0, 640.0, 0),
    'x1.32xlarge': InstanceResources(128.0, 1952.0, 3840.0, 0),
    'x1.16xlarge': InstanceResources(64.0, 976.0, 1920.0, 0),
    'r5.large': InstanceResources(2.0, 16.0, None, 0),
    'r5.xlarge': InstanceResources(4.0, 32.0, None, 0),
    'r5.2xlarge': InstanceResources(8.0, 64.0, None, 0),
    'r5.4xlarge': InstanceResources(16.0, 128.0, None, 0),
    'r5.8xlarge': InstanceResources(32.0, 256.0, None, 0),
    'r5.12xlarge': InstanceResources(48.0, 384.0, None, 0),
    'r5.16xlarge': InstanceResources(64.0, 512.0, None, 0),
    'r5.24xlarge': InstanceResources(96.0, 768.0, None, 0),
    'r5a.large': InstanceResources(2.0, 16.0, None, 0),
    'r5a.xlarge': InstanceResources(4.0, 32.0, None, 0),
    'r5a.2xlarge': InstanceResources(8.0, 64.0, None, 0),
    'r5a.4xlarge': InstanceResources(16.0, 128.0, None, 0),
    'r5a.8xlarge': InstanceResources(32.0, 256.0, None, 0),
    'r5a.12xlarge': InstanceResources(48.0, 384.0, None, 0),
    'r5a.16xlarge': InstanceResources(64.0, 512.0, None, 0),
    'r5a.24xlarge': InstanceResources(96.0, 768.0, None, 0),
    'r5ad.large': InstanceResources(2.0, 16.0, 75.0, 0),
    'r5ad.xlarge': InstanceResources(4.0, 32.0, 150.0, 0),
    'r5ad.2xlarge': InstanceResources(8.0, 64.0, 300.0, 0),
    'r5ad.4xlarge': InstanceResources(16.0, 128.0, 600.0, 0),
    'r5ad.8xlarge': InstanceResources(32.0, 256.0, 1200.0, 0),
    'r5ad.12xlarge': InstanceResources(48.0, 384.0, 1800.0, 0),
    'r5ad.16xlarge': InstanceResources(64.0, 512.0, 2400.0, 0),
    'r5ad.24xlarge': InstanceResources(96.0, 768.0, 3600.0, 0),
    'r5d.large': InstanceResources(2.0, 16.0, 75.0, 0),
    'r5d.xlarge': InstanceResources(4.0, 32.0, 150.0, 0),
    'r5d.2xlarge': InstanceResources(8.0, 64.0, 300.0, 0),
    'r5d.4xlarge': InstanceResources(16.0, 128.0, 600.0, 0),
    'r5d.8xlarge': InstanceResources(32.0, 256.0, 1200.0, 0),
    'r5d.12xlarge': InstanceResources(48.0, 384.0, 1800.0, 0),
    'r5d.16xlarge': InstanceResources(64.0, 512.0, 2400.0, 0),
    'r5d.24xlarge': InstanceResources(96.0, 768.0, 3600.0, 0),
    'r5dn.large': InstanceResources(2.0, 16.0, 75.0, 0),
    'r5dn.xlarge': InstanceResources(4.0, 32.0, 150.0, 0),
    'r5dn.2xlarge': InstanceResources(8.0, 64.0, 300.0, 0),
    'r5dn.4xlarge': InstanceResources(16.0, 128.0, 600.0, 0),
    'r5dn.8xlarge': InstanceResources(32.0, 256.0, 1200.0, 0),
    'r5dn.12xlarge': InstanceResources(48.0, 384.0, 1800.0, 0),
    'r5dn.16xlarge': InstanceResources(64.0, 512.0, 2400.0, 0),
    'r5dn.24xlarge': InstanceResources(96.0, 768.0, 3600.0, 0),
    'r5n.large': InstanceResources(2.0, 16.0, None, 0),
    'r5n.xlarge': InstanceResources(4.0, 32.0, None, 0),
    'r5n.2xlarge': InstanceResources(8.0, 64.0, None, 0),
    'r5n.4xlarge': InstanceResources(16.0, 128.0, None, 0),
    'r5n.8xlarge': InstanceResources(32.0, 256.0, None, 0),
    'r5n.12xlarge': InstanceResources(48.0, 384.0, None, 0),
    'r5n.16xlarge': InstanceResources(64.0, 512.0, None, 0),
    'r5n.24xlarge': InstanceResources(96.0, 768.0, None, 0),
    'r4.large': InstanceResources(2.0, 15.25, None, 0),
    'r4.xlarge': InstanceResources(4.0, 30.5, None, 0),
    'r4.2xlarge': InstanceResources(8.0, 61.0, None, 0),
    'r4.4xlarge': InstanceResources(16.0, 122.0, None, 0),
    'r4.8xlarge': InstanceResources(32.0, 244.0, None, 0),
    'r4.16xlarge': InstanceResources(64.0, 488.0, None, 0),
    'r3.large': InstanceResources(2.0, 15.25, 32.0, 0),
    'r3.xlarge': InstanceResources(4.0, 30.5, 80.0, 0),
    'r3.2xlarge': InstanceResources(8.0, 61.0, 160.0, 0),
    'r3.4xlarge': InstanceResources(16.0, 122.0, 320.0, 0),
    'r3.8xlarge': InstanceResources(32.0, 244.0, 320.0, 0),
    'i2.xlarge': InstanceResources(4.0, 30.5, 800.0, 0),
    'i2.2xlarge': InstanceResources(8.0, 61.0, 1600.0, 0),
    'i2.4xlarge': InstanceResources(16.0, 122.0, 3200.0, 0),
    'i2.8xlarge': InstanceResources(32.0, 244.0, 6400.0, 0),
    'i3.large': InstanceResources(2.0, 15.25, 0.475, 0),
    'i3.xlarge': InstanceResources(4.0, 30.5, 0.95, 0),
    'i3.2xlarge': InstanceResources(8.0, 61.0, 1.9, 0),
    'i3.4xlarge': InstanceResources(16.0, 122.0, 3.8, 0),
    'i3.8xlarge': InstanceResources(32.0, 244.0, 7.6, 0),
    'i3.16xlarge': InstanceResources(64.0, 488.0, 15.2, 0),
    'i3en.large': InstanceResources(2.0, 16.0, 1250.0, 0),
    'i3en.xlarge': InstanceResources(4.0, 32.0, 2500.0, 0),
    'i3en.2xlarge': InstanceResources(8.0, 64.0, 5000.0, 0),
    'i3en.3xlarge': InstanceResources(12.0, 96.0, 7500.0, 0),
    'i3en.6xlarge': InstanceResources(24.0, 192.0, 15000.0, 0),
    'i3en.24xlarge': InstanceResources(96.0, 768.0, 60000.0, 0),
    'd2.xlarge': InstanceResources(4.0, 30.5, 6000.0, 0),
    'd2.2xlarge': InstanceResources(8.0, 61.0, 12000.0, 0),
    'd2.4xlarge': InstanceResources(16.0, 122.0, 24000.0, 0),
    'd2.8xlarge': InstanceResources(36.0, 244.0, 48000.0, 0),
    'z1d.large': InstanceResources(2.0, 16.0, 75.0, 0),
    'z1d.xlarge': InstanceResources(4.0, 32.0, 150.0, 0),
    'z1d.2xlarge': InstanceResources(8.0, 64.0, 300.0, 0),
    'z1d.3xlarge': InstanceResources(12.0, 96.0, 450.0, 0),
    'z1d.6xlarge': InstanceResources(24.0, 192.0, 900.0, 0),
    'z1d.12xlarge': InstanceResources(48.0, 384.0, 1800.0, 0),
    'g2.2xlarge': InstanceResources(8.0, 15.0, 60.0, 1),
    'g2.8xlarge': InstanceResources(32.0, 60.0, 240.0, 4),
    'g3.4xlarge': InstanceResources(16.0, 122.0, None, 1),
    'g3.8xlarge': InstanceResources(32.0, 244.0, None, 2),
    'g3.16xlarge': InstanceResources(64.0, 488.0, None, 4),
    'g3s.xlarge': InstanceResources(4.0, 30.5, None, 1),
    'p2.xlarge': InstanceResources(4.0, 61.0, None, 1),
    'p2.8xlarge': InstanceResources(32.0, 488.0, None, 8),
    'p2.16xlarge': InstanceResources(64.0, 768.0, None, 16),
    'p3.2xlarge': InstanceResources(8.0, 61.0, None, 1),
    'p3.8xlarge': InstanceResources(32.0, 244.0, None, 4),
    'p3.16xlarge': InstanceResources(64.0, 488.0, None, 8),
    'p3dn.24xlarge': InstanceResources(96.0, 768.0, 1800.0, 8),
}

EC2_AZS: List[Optional[str]] = [
    None,
    'us-east-1a',
    'us-east-1b',
    'us-east-1c',
    'us-west-1a',
    'us-west-1b',
    'us-west-1c',
    'us-west-2a',
    'us-west-2b',
    'us-west-2c',
]


def get_market_resources(market: InstanceMarket) -> InstanceResources:
    return EC2_INSTANCE_TYPES[market.instance]


def get_market(instance_type: str, subnet_id: Optional[str]) -> InstanceMarket:
    az: Optional[str]
    if subnet_id is not None:
        az = subnet_to_az(subnet_id)
    else:
        az = None
    return InstanceMarket(instance_type, az)


def get_instance_market(aws_instance_object: MarketDict) -> InstanceMarket:
    instance_type = aws_instance_object['InstanceType']
    subnet_id = aws_instance_object.get('SubnetId')
    if subnet_id:
        return get_market(instance_type, subnet_id)
    else:
        az = aws_instance_object.get('Placement', {}).get('AvailabilityZone')
        return InstanceMarket(instance_type, az)


@lru_cache(maxsize=32)
def subnet_to_az(subnet_id: str) -> str:
    return ec2.describe_subnets(SubnetIds=[subnet_id])['Subnets'][0]['AvailabilityZone']
