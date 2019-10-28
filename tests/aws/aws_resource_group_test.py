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
import mock
import pytest
import simplejson as json

from clusterman.aws.aws_resource_group import AWSResourceGroup
from clusterman.aws.client import ec2
from clusterman.aws.markets import InstanceMarket


class MockResourceGroup(AWSResourceGroup):
    def __init__(self, group_id, subnet):
        super().__init__(group_id)
        self.instances = ec2.run_instances(
            InstanceType='c3.4xlarge',
            MinCount=5,
            MaxCount=5,
            SubnetId=subnet['Subnet']['SubnetId'],
            ImageId='ami-785db401',  # this AMI is hard-coded into moto, represents ubuntu xenial
        )['Instances']

    def modify_target_capacity(self):
        pass

    @property
    def instance_ids(self):
        return [i['InstanceId'] for i in self.instances]

    @property
    def stale_instance_ids(self):
        return []

    @property
    def fulfilled_capacity(self):
        return 5

    @property
    def status(self):
        return 'running'

    @property
    def is_stale(self):
        return False

    @property
    def _target_capacity(self):
        return 5

    @classmethod
    def _get_resource_group_tags(cls):
        return {
            'sfr-123': {
                'some': 'tag',
                'paasta': 'true',
                'puppet:role::paasta': json.dumps({
                    'pool': 'default',
                    'paasta_cluster': 'westeros-prod',
                }),
            },
            'sfr-456': {
                'some': 'tag',
                'paasta': 'true',
                'puppet:role::paasta': json.dumps({
                    'pool': 'another',
                    'paasta_cluster': 'westeros-prod',
                }),
            },
            'sfr-789': {
                'some': 'tag',
                'paasta': 'true',
                'puppet:role::paasta': json.dumps({
                    'paasta_cluster': 'westeros-prod',
                }),
            },
            'sfr-abc': {
                'paasta': 'false',
                'puppet:role::riice': json.dumps({
                    'pool': 'default',
                    'paasta_cluster': 'westeros-prod',
                }),
            }
        }


@pytest.fixture
def mock_resource_groups(mock_subnet):
    return MockResourceGroup.load(
        cluster='westeros-prod',
        pool='default',
        config={'tag': 'puppet:role::paasta'},
        subnet=mock_subnet,
    )


@pytest.fixture
def mock_resource_group(mock_resource_groups):
    return mock_resource_groups['sfr-123']


def test_load_resource_groups_from_tags(mock_resource_groups):
    assert len(mock_resource_groups) == 1
    assert list(mock_resource_groups) == ['sfr-123']


def test_terminate_all_instances_by_id(mock_resource_group):
    instance_ids = mock_resource_group.instance_ids
    terminated_ids = mock_resource_group.terminate_instances_by_id(instance_ids)
    assert terminated_ids == instance_ids


def mock_describe_instances_with_missing_subnet(orig):
    def describe_instances_with_missing_subnet(InstanceIds):
        ret = orig(InstanceIds=InstanceIds)
        for i in ret['Reservations'][0]['Instances']:
            i.pop('SubnetId')
            i.pop('Placement')
        return ret
    return describe_instances_with_missing_subnet


@mock.patch('clusterman.aws.aws_resource_group.logger')
def test_terminate_instance_missing_subnet(mock_logger, mock_resource_group):
    ec2_describe = ec2.describe_instances
    with mock.patch(
        'clusterman.aws.aws_resource_group.ec2.describe_instances',
        wraps=mock_describe_instances_with_missing_subnet(ec2_describe)
    ):
        assert not mock_resource_group.terminate_instances_by_id(mock_resource_group.instance_ids)

    assert mock_logger.warning.call_count == 5
    for msg in mock_logger.warning.call_args_list:
        assert 'missing AZ info' in msg[0][0]


def test_terminate_all_instances_by_id_small_batch(mock_resource_group):
    instance_ids = mock_resource_group.instance_ids
    with mock.patch(
        'clusterman.aws.aws_resource_group.ec2.terminate_instances',
        wraps=ec2.terminate_instances,
    ) as mock_terminate:
        terminated_ids = mock_resource_group.terminate_instances_by_id(instance_ids, batch_size=1)
        assert mock_terminate.call_count == 5
        assert sorted(terminated_ids) == sorted(instance_ids)


@mock.patch('clusterman.aws.aws_resource_group.logger')
def test_terminate_some_instances_missing(mock_logger, mock_resource_group):
    with mock.patch('clusterman.aws.aws_resource_group.ec2.terminate_instances') as mock_terminate:
        mock_terminate.return_value = {
            'TerminatingInstances': [
                {'InstanceId': i} for i in mock_resource_group.instance_ids[:3]
            ]
        }
        instances = mock_resource_group.terminate_instances_by_id(
            mock_resource_group.instance_ids,
        )

        assert len(instances) == 3
        assert mock_logger.warning.call_count == 2


@mock.patch('clusterman.aws.aws_resource_group.logger')
def test_terminate_no_instances_by_id(mock_logger, mock_resource_group):
    terminated_ids = mock_resource_group.terminate_instances_by_id([])
    assert not terminated_ids
    assert mock_logger.warning.call_count == 1


def test_protect_unowned_instances(mock_resource_group):
    assert mock_resource_group.terminate_instances_by_id(['fake-1', 'fake-4']) == []


def test_market_capacities(mock_resource_group):
    assert mock_resource_group.market_capacities == {
        InstanceMarket('c3.4xlarge', 'us-west-2a'): 5
    }


@pytest.mark.parametrize('is_stale', [True, False])
def test_target_capacity(mock_resource_group, is_stale):
    with mock.patch(f'{__name__}.MockResourceGroup.is_stale', mock.PropertyMock(return_value=is_stale)):
        assert mock_resource_group.target_capacity == 0 if is_stale else 5


def test_get_node_metadatas(mock_resource_group):
    ips = [i['PrivateIpAddress'] for i in mock_resource_group.instances]
    with mock.patch('clusterman.aws.aws_resource_group.gethostbyaddr') as mock_get_host:
        mock_get_host.side_effect = lambda ip: {ips[i]: (f'host{i}',) for i in range(5)}[ip]
        instance_metadatas = mock_resource_group.get_instance_metadatas()
        cancelled_metadatas = mock_resource_group.get_instance_metadatas({'cancelled'})

    assert len(instance_metadatas) == 5
    assert len(cancelled_metadatas) == 0
    for i, instance_metadata in enumerate(instance_metadatas):
        assert instance_metadata.group_id == mock_resource_group.id
        assert instance_metadata.hostname == f'host{i}'
        assert instance_metadata.instance_id == mock_resource_group.instances[i]['InstanceId']
        assert not instance_metadata.is_stale
        assert instance_metadata.ip_address == ips[i]
        assert instance_metadata.market.instance == 'c3.4xlarge'
        assert instance_metadata.state == 'running'
        assert instance_metadata.weight == 1
