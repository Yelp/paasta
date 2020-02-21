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
import json

import mock
import pytest

from clusterman.aws.auto_scaling_resource_group import AutoScalingResourceGroup
from clusterman.aws.auto_scaling_resource_group import CLUSTERMAN_STALE_TAG
from clusterman.aws.client import autoscaling
from clusterman.aws.client import ec2
from clusterman.aws.markets import InstanceMarket


@pytest.fixture
def mock_launch_config():
    launch_config = {
        'LaunchConfigurationName': 'fake_launch_config',
        'ImageId': 'ami-785db401',  # this AMI is hard-coded into moto, represents ubuntu xenial
        'InstanceType': 't2.2xlarge',
    }
    autoscaling.create_launch_configuration(**launch_config)
    return launch_config


@pytest.fixture
def mock_asg_name():
    return 'fake_asg'


@pytest.fixture
def mock_cluster():
    return 'fake_cluster'


@pytest.fixture
def mock_pool():
    return 'fake_pool'


@pytest.fixture
def mock_asg_config(mock_subnet, mock_launch_config, mock_asg_name, mock_cluster, mock_pool):
    asg = {
        'AutoScalingGroupName': mock_asg_name,
        'LaunchConfigurationName': mock_launch_config['LaunchConfigurationName'],
        'MinSize': 1,
        'MaxSize': 30,
        'DesiredCapacity': 10,
        'AvailabilityZones': ['us-west-2a'],
        'VPCZoneIdentifier': mock_subnet['Subnet']['SubnetId'],
        'Tags': [
            {
                'Key': 'puppet:role::paasta',
                'Value': json.dumps({
                    'pool': mock_pool,
                    'paasta_cluster': mock_cluster
                }),
            }, {
                'Key': 'fake_tag_key',
                'Value': 'fake_tag_value',
            },
        ],
        'NewInstancesProtectedFromScaleIn': True,
    }
    autoscaling.create_auto_scaling_group(**asg)

    return asg


def test_group_config(mock_asg_config):
    mock_asrg = AutoScalingResourceGroup.__new__(AutoScalingResourceGroup)  # skip init
    mock_asrg.group_id = mock_asg_config['AutoScalingGroupName']

    group_config = mock_asrg._group_config

    assert group_config['AutoScalingGroupName'] == \
        mock_asg_config['AutoScalingGroupName']


@pytest.fixture
def mock_asrg(mock_asg_config):
    return AutoScalingResourceGroup(mock_asg_config['AutoScalingGroupName'])


def test_launch_config(mock_asrg, mock_launch_config):
    launch_config = mock_asrg._launch_config

    assert launch_config['LaunchConfigurationName'] == \
        mock_launch_config['LaunchConfigurationName']


def test_launch_config_retry(mock_asrg, mock_launch_config):
    no_configs = dict(LaunchConfigurations=[])
    good_configs = dict(LaunchConfigurations=[mock_launch_config])
    mock_describe_launch_configs = mock.Mock(side_effect=[
        no_configs, good_configs,
    ])

    with mock.patch(
        'clusterman.aws.client.autoscaling.describe_launch_configurations',
        mock_describe_launch_configs,
    ):
        launch_config = mock_asrg._launch_config

    assert launch_config == mock_launch_config
    assert mock_describe_launch_configs.call_count == 2


@pytest.mark.parametrize('instance_type', ['t2.2xlarge', 'm5.large'])
def test_market_weight(mock_asrg, instance_type):
    market_weight = mock_asrg.market_weight(InstanceMarket(instance_type, 'us-west-2a'))
    assert market_weight == 1.0


@pytest.mark.parametrize('dry_run', [True, False])
def test_mark_stale(mock_asrg, dry_run):
    mock_asrg.mark_stale(dry_run)
    for inst in mock_asrg.instance_ids:
        tags = ec2.describe_tags(
            Filters=[{
                'Name': 'resource-id',
                'Values': [inst],
            }],
        )
        stale_tags = [tag for tag in tags['Tags'] if tag['Key'] == CLUSTERMAN_STALE_TAG]
        if dry_run:
            assert not stale_tags
        else:
            assert len(stale_tags) == 1


@pytest.mark.parametrize('stale_instances', [0, 7])
def test_modify_target_capacity_up(mock_asrg, stale_instances):
    new_desired_capacity = mock_asrg.target_capacity + 5
    with mock.patch(
        'clusterman.aws.auto_scaling_resource_group.AutoScalingResourceGroup.stale_instance_ids',
        mock.PropertyMock(return_value=mock_asrg.instance_ids[:stale_instances])
    ):

        mock_asrg.modify_target_capacity(
            new_desired_capacity,
            dry_run=False,
            honor_cooldown=False,
        )

        assert mock_asrg.target_capacity == new_desired_capacity
        assert mock_asrg.fulfilled_capacity == new_desired_capacity + stale_instances


@pytest.mark.parametrize('stale_instances', [0, 7])
def test_modify_target_capacity_down(mock_asrg, stale_instances):
    old_target_capacity = mock_asrg.target_capacity
    new_target_capacity = old_target_capacity - 5

    with mock.patch(
        'clusterman.aws.auto_scaling_resource_group.AutoScalingResourceGroup.stale_instance_ids',
        mock.PropertyMock(return_value=mock_asrg.instance_ids[:stale_instances])
    ):
        mock_asrg.modify_target_capacity(
            new_target_capacity,
            dry_run=False,
            honor_cooldown=False,
        )

        assert mock_asrg.target_capacity == new_target_capacity
        # because some instances are stale, we might have to _increase_ our "real" target capacity
        # even if we're decreasing our _requested_ target capacity
        assert mock_asrg.fulfilled_capacity == max(old_target_capacity, new_target_capacity + stale_instances)


@pytest.mark.parametrize('new_desired_capacity', [0, 100])
def test_modify_target_capacity_min_max(
    mock_asrg,
    mock_asg_config,
    new_desired_capacity,
):
    mock_asrg.modify_target_capacity(
        new_desired_capacity,
        dry_run=False,
        honor_cooldown=False,
    )

    if new_desired_capacity < mock_asg_config['MinSize']:
        assert mock_asrg.target_capacity == mock_asg_config['MinSize']
    elif new_desired_capacity > mock_asg_config['MaxSize']:
        assert mock_asrg.target_capacity == mock_asg_config['MaxSize']


@pytest.mark.parametrize('stale_instances', [0, 1, 10])
def test_status(mock_asrg, stale_instances):
    is_stale = stale_instances == 10
    with mock.patch(
        'clusterman.aws.auto_scaling_resource_group.AutoScalingResourceGroup.is_stale',
        new_callable=mock.PropertyMock(return_value=is_stale)
    ), mock.patch(
        'clusterman.aws.auto_scaling_resource_group.AutoScalingResourceGroup.stale_instance_ids',
        new_callable=mock.PropertyMock(return_value=mock_asrg.instance_ids[:stale_instances])
    ):
        status = mock_asrg.status
        if stale_instances == 0:
            assert status == 'active'
        elif stale_instances > 0:
            assert status == 'rolling'


def test_get_asg_tags(mock_asrg, mock_asg_config):
    asg_id_to_tags = mock_asrg._get_resource_group_tags()

    assert mock_asg_config['AutoScalingGroupName'] in asg_id_to_tags
    tags = asg_id_to_tags[mock_asg_config['AutoScalingGroupName']]
    assert 'fake_tag_key' in tags
    assert tags['fake_tag_key'] == 'fake_tag_value'
