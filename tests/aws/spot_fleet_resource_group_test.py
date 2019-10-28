import json

import botocore
import mock
import pytest
from moto import mock_s3

from clusterman.aws.client import ec2
from clusterman.aws.client import s3
from clusterman.aws.markets import InstanceMarket
from clusterman.aws.spot_fleet_resource_group import load_spot_fleets_from_s3
from clusterman.aws.spot_fleet_resource_group import SpotFleetResourceGroup
from clusterman.exceptions import ResourceGroupError


@pytest.fixture
def mock_sfr_response(mock_subnet):
    sfr_response = ec2.request_spot_fleet(
        SpotFleetRequestConfig={
            'AllocationStrategy': 'diversified',
            'SpotPrice': '2.0',
            'TargetCapacity': 10,
            'LaunchSpecifications': [
                {
                    'ImageId': 'ami-785db401',  # this image is hard-coded into moto, represents ubuntu xenial
                    'SubnetId': mock_subnet['Subnet']['SubnetId'],
                    'WeightedCapacity': 2,
                    'InstanceType': 'c3.8xlarge',
                    'EbsOptimized': False,
                    # note that this is not useful until we solve
                    # https://github.com/spulec/moto/issues/1644
                    'TagSpecifications': [{
                        'ResourceType': 'instance',
                        'Tags': [{
                            'Key': 'foo',
                            'Value': 'bar',
                        }],
                    }],
                },
                {
                    'ImageId': 'ami-785db401',  # this image is hard-coded into moto, represents ubuntu xenial
                    'SubnetId': mock_subnet['Subnet']['SubnetId'],
                    'WeightedCapacity': 1,
                    'InstanceType': 'i2.4xlarge',
                    'EbsOptimized': False,
                    'TagSpecifications': [{
                        'ResourceType': 'instance',
                        'Tags': [{
                            'Key': 'foo',
                            'Value': 'bar',
                        }],
                    }],
                },
            ],
            'IamFleetRole': 'foo',
        },
    )
    return sfr_response


@pytest.fixture
def mock_spot_fleet_resource_group(mock_sfr_response):
    return SpotFleetResourceGroup(mock_sfr_response['SpotFleetRequestId'])


@mock_s3
def test_load_spot_fleets_from_s3():
    s3.create_bucket(Bucket='fake-clusterman-sfrs')
    s3.put_object(Bucket='fake-clusterman-sfrs', Key='fake-region/sfr-1.json', Body=json.dumps({
        'cluster_autoscaling_resources': {
            'aws_spot_fleet_request': {
                'id': 'sfr-1',
                'pool': 'my-pool'
            }
        }
    }).encode())
    s3.put_object(Bucket='fake-clusterman-sfrs', Key='fake-region/sfr-2.json', Body=json.dumps({
        'cluster_autoscaling_resources': {
            'aws_spot_fleet_request': {
                'id': 'sfr-2',
                'pool': 'my-pool'
            }
        }
    }).encode())
    s3.put_object(Bucket='fake-clusterman-sfrs', Key='fake-region/sfr-3.json', Body=json.dumps({
        'cluster_autoscaling_resources': {
            'aws_spot_fleet_request': {
                'id': 'sfr-3',
                'pool': 'not-my-pool'
            }
        }
    }).encode())

    with mock.patch(
        'clusterman.aws.spot_fleet_resource_group.SpotFleetResourceGroup',
    ):
        sfrgs = load_spot_fleets_from_s3(
            bucket='fake-clusterman-sfrs',
            prefix='fake-region',
            pool='my-pool',
        )
        assert len(sfrgs) == 2
        assert {sfr_id for sfr_id in sfrgs} == {'sfr-1', 'sfr-2'}


def test_load_spot_fleets():
    with mock.patch(
        'clusterman.aws.spot_fleet_resource_group.AWSResourceGroup.load',
    ) as mock_tag_load, mock.patch(
        'clusterman.aws.spot_fleet_resource_group.load_spot_fleets_from_s3',
    ) as mock_s3_load:
        mock_tag_load.return_value = {'sfr-1': mock.Mock(id='sfr-1'), 'sfr-2': mock.Mock(id='sfr-2')}
        mock_s3_load.return_value = {'sfr-3': mock.Mock(id='sfr-3', status='cancelled'), 'sfr-4': mock.Mock(id='sfr-4')}
        spot_fleets = SpotFleetResourceGroup.load(
            cluster='westeros-prod',
            pool='my-pool',
            config={
                'tag': 'puppet:role::paasta',
                's3': {
                    'bucket': 'fake-clusterman-sfrs',
                    'prefix': 'fake-region',
                },
            },
        )
        assert {sf for sf in spot_fleets} == {'sfr-1', 'sfr-2', 'sfr-4'}


def test_get_spot_fleet_request_tags(mock_spot_fleet_resource_group):
    # doing this the old fashioned way until
    # https://github.com/spulec/moto/issues/1644 is fixed
    with mock.patch(
        'clusterman.aws.spot_fleet_resource_group.ec2.describe_spot_fleet_requests',
    ) as mock_describe_spot_fleet_requests:
        mock_describe_spot_fleet_requests.return_value = {
            'SpotFleetRequestConfigs': [{
                'SpotFleetRequestId': 'sfr-12',
                'SpotFleetRequestConfig': {
                    'LaunchSpecifications': [{
                        'TagSpecifications': [{
                            'Tags': [{
                                'Key': 'foo',
                                'Value': 'bar',
                            }],
                        }],
                    }],
                },
            },
                {
                'SpotFleetRequestId': 'sfr-34',
                'SpotFleetRequestConfig': {
                    'LaunchSpecifications': [{}],
                },
            },
                {
                'SpotFleetRequestId': 'sfr-56',
                'SpotFleetRequestConfig': {
                    'LaunchSpecifications': [],
                },
            },
                {
                'SpotFleetRequestId': 'sfr-78',
                'SpotFleetRequestConfig': {
                    'LaunchSpecifications': [{
                        'TagSpecifications': [{
                            'Tags': [{
                                'Key': 'foo',
                                'Value': 'bar',
                            },
                                {
                                'Key': 'spam',
                                'Value': 'baz',
                            }],
                        }],
                    }],
                },
            }],
        }
        sfrs = SpotFleetResourceGroup._get_resource_group_tags()
        expected = {
            'sfr-12': {
                'foo': 'bar'
            },
            'sfr-34': {},
            'sfr-56': {},
            'sfr-78': {
                'foo': 'bar',
                'spam': 'baz'
            }
        }
        assert sfrs == expected


# NOTE: These tests are fairly brittle, as it depends on the implementation of modify_spot_fleet_request
# inside moto.  So if moto's implementation changes, these tests could break.  However, I still think
# these tests cover important functionality, and I can't think of a way to make them less brittle.
def test_fulfilled_capacity(mock_spot_fleet_resource_group):
    assert mock_spot_fleet_resource_group.fulfilled_capacity == 11


def test_modify_target_capacity_stale(mock_spot_fleet_resource_group):
    with mock.patch('clusterman.aws.spot_fleet_resource_group.ec2.describe_spot_fleet_requests') as mock_describe:
        mock_describe.return_value = {
            'SpotFleetRequestConfigs': [
                {'SpotFleetRequestState': 'cancelled_running'}
            ],
        }
        mock_spot_fleet_resource_group.modify_target_capacity(20)
        assert mock_spot_fleet_resource_group.target_capacity == 0


def test_modify_target_capacity_up(mock_spot_fleet_resource_group):
    mock_spot_fleet_resource_group.modify_target_capacity(20)
    assert mock_spot_fleet_resource_group.target_capacity == 20
    assert len(mock_spot_fleet_resource_group.instance_ids) == 13


def test_modify_target_capacity_down(mock_spot_fleet_resource_group):
    mock_spot_fleet_resource_group.modify_target_capacity(5)
    assert mock_spot_fleet_resource_group.target_capacity == 5
    assert mock_spot_fleet_resource_group.fulfilled_capacity == 11
    assert len(mock_spot_fleet_resource_group.instance_ids) == 7


def test_modify_target_capacity_dry_run(mock_spot_fleet_resource_group):
    mock_spot_fleet_resource_group.modify_target_capacity(5, dry_run=True)
    assert mock_spot_fleet_resource_group.target_capacity == 10
    assert mock_spot_fleet_resource_group.fulfilled_capacity == 11


def test_modify_target_capacity_error(mock_spot_fleet_resource_group):
    with mock.patch('clusterman.aws.spot_fleet_resource_group.ec2.modify_spot_fleet_request') as mock_modify, \
            pytest.raises(ResourceGroupError):
        mock_modify.return_value = {'Return': False}
        mock_spot_fleet_resource_group.modify_target_capacity(5)
    assert mock_spot_fleet_resource_group.target_capacity == 10
    assert mock_spot_fleet_resource_group.fulfilled_capacity == 11


def test_instances(mock_spot_fleet_resource_group):
    assert len(mock_spot_fleet_resource_group.instance_ids) == 7


def test_market_capacities(mock_spot_fleet_resource_group, mock_subnet):
    assert mock_spot_fleet_resource_group.market_capacities == {
        InstanceMarket('c3.8xlarge', mock_subnet['Subnet']['AvailabilityZone']): 8,
        InstanceMarket('i2.4xlarge', mock_subnet['Subnet']['AvailabilityZone']): 3,
    }


def test_is_stale(mock_spot_fleet_resource_group):
    assert not mock_spot_fleet_resource_group.is_stale


def test_is_stale_not_found(mock_spot_fleet_resource_group):
    with mock.patch('clusterman.aws.spot_fleet_resource_group.ec2.describe_spot_fleet_requests') as mock_describe:
        mock_describe.side_effect = botocore.exceptions.ClientError(
            {'Error': {'Code': 'InvalidSpotFleetRequestId.NotFound'}},
            'foo',
        )
        assert mock_spot_fleet_resource_group.is_stale


def test_is_stale_error(mock_spot_fleet_resource_group):
    with mock.patch('clusterman.aws.spot_fleet_resource_group.ec2.describe_spot_fleet_requests') as mock_describe, \
            pytest.raises(botocore.exceptions.ClientError):
        mock_describe.side_effect = botocore.exceptions.ClientError({}, 'foo')
        mock_spot_fleet_resource_group.is_stale
