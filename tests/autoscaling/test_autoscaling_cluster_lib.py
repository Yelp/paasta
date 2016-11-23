# Copyright 2015-2016 Yelp Inc.
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
from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib
import unittest
from math import floor

import mock
from botocore.exceptions import ClientError
from pytest import raises
from requests.exceptions import HTTPError

from paasta_tools.autoscaling import autoscaling_cluster_lib
from paasta_tools.mesos_tools import SlaveTaskCount
from paasta_tools.metrics.metastatus_lib import ResourceInfo
from paasta_tools.utils import TimeoutError


def pid_to_ip_sideeffect(pid):
    pid_to_ip = {'pid1': '10.1.1.1',
                 'pid2': '10.2.2.2',
                 'pid3': '10.3.3.3'}
    return pid_to_ip[pid]


def is_resource_cancelled_sideeffect(self):
    if self.resource['id'] == 'sfr-blah3':
        return True
    return False


def test_autoscale_local_cluster():
    with contextlib.nested(
        mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.load_system_paasta_config', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.autoscale_cluster_resource', autospec=True),
        mock.patch('time.sleep', autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.is_resource_cancelled',
                   autospec=True),
        mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.get_sfr', autospec=True),
    ) as (
        mock_get_paasta_config,
        mock_autoscale_cluster_resource,
        _,
        mock_is_resource_cancelled,
        mock_get_sfr,
    ):

        mock_get_sfr.return_value = False
        mock_scaling_resources = {'id1': {'id': 'sfr-blah1', 'type': 'aws_spot_fleet_request',
                                          'pool': 'default', 'region': 'westeros-1'},
                                  'id2': {'id': 'sfr-blah2', 'type': 'aws_spot_fleet_request',
                                          'pool': 'default', 'region': 'westeros-1'},
                                  'id3': {'id': 'sfr-blah3', 'type': 'aws_spot_fleet_request',
                                          'pool': 'default', 'region': 'westeros-1'}}
        mock_resource_pool_settings = {'default': {'drain_timeout': 123, 'target_utilization': 0.75}}
        mock_get_cluster_autoscaling_resources = mock.Mock(return_value=mock_scaling_resources)
        mock_get_resource_pool_settings = mock.Mock(return_value=mock_resource_pool_settings)
        mock_is_resource_cancelled.side_effect = is_resource_cancelled_sideeffect
        mock_get_resources = mock.Mock(get_cluster_autoscaling_resources=mock_get_cluster_autoscaling_resources,
                                       get_resource_pool_settings=mock_get_resource_pool_settings)
        mock_get_paasta_config.return_value = mock_get_resources

        autoscaling_cluster_lib.autoscale_local_cluster(config_folder='/nail/blah')
        assert mock_get_paasta_config.called
        autoscaled_resources = [call[0][0].resource for call in mock_autoscale_cluster_resource.call_args_list]
        assert autoscaled_resources[0] == mock_scaling_resources['id3']
        assert mock_scaling_resources['id1'] in autoscaled_resources[1:3]
        assert mock_scaling_resources['id2'] in autoscaled_resources[1:3]


def test_autoscale_cluster_resource():
    mock_scaling_resource = {'id': 'sfr-blah', 'type': 'sfr', 'pool': 'default'}
    mock_scaler = mock.Mock()
    mock_metrics_provider = mock.Mock(return_value=(2, 6))
    mock_scale_resource = mock.Mock()
    mock_scaler.metrics_provider = mock_metrics_provider
    mock_scaler.scale_resource = mock_scale_resource
    mock_scaler.resource = mock_scaling_resource

    # test scale up
    autoscaling_cluster_lib.autoscale_cluster_resource(mock_scaler)
    assert mock_metrics_provider.called
    mock_scale_resource.assert_called_with(2, 6)


class TestAsgAutoscaler(unittest.TestCase):

    def setUp(self):
        with contextlib.nested(
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.AsgAutoscaler.get_asg', autospec=True),
        ) as (
            mock_get_asg,
        ):
            mock_get_asg.return_value = {}

            mock_resource = {'id': 'asg-blah', 'type': 'aws_autoscaling_group',
                             'region': 'westeros-1', 'pool': 'default'}
            mock_pool_settings = {'drain_timeout': 123}
            mock_config_folder = '/nail/blah'
            self.autoscaler = autoscaling_cluster_lib.AsgAutoscaler(mock_resource,
                                                                    mock_pool_settings,
                                                                    mock_config_folder,
                                                                    False)
            self.autoscaler.instances = []

    def test_is_asg_cancelled(self):
        self.autoscaler.asg = None
        assert self.autoscaler.is_resource_cancelled()

        self.autoscaler.asg = mock.Mock()
        assert not self.autoscaler.is_resource_cancelled()

    def test_get_asg(self):
        with contextlib.nested(
            mock.patch('boto3.client', autospec=True),
        ) as (
            mock_ec2_client,
        ):
            mock_asg = mock.Mock()
            mock_asgs = {'AutoScalingGroups': [mock_asg]}
            mock_describe_auto_scaling_groups = mock.Mock(return_value=mock_asgs)
            mock_ec2_client.return_value = mock.Mock(describe_auto_scaling_groups=mock_describe_auto_scaling_groups)
            ret = self.autoscaler.get_asg('asg-blah', region='westeros-1')
            mock_describe_auto_scaling_groups.assert_called_with(AutoScalingGroupNames=['asg-blah'])
            assert ret == mock_asg

            mock_asgs = {'AutoScalingGroups': []}
            mock_describe_auto_scaling_groups = mock.Mock(return_value=mock_asgs)
            mock_ec2_client.return_value = mock.Mock(describe_auto_scaling_groups=mock_describe_auto_scaling_groups)
            ret = self.autoscaler.get_asg('asg-blah', region='westeros-1')
            assert ret is None

    def test_set_asg_capacity(self):
        with contextlib.nested(
            mock.patch('boto3.client', autospec=True),
        ) as (
            mock_ec2_client,
        ):
            mock_update_auto_scaling_group = mock.Mock()
            mock_ec2_client.return_value = mock.Mock(update_auto_scaling_group=mock_update_auto_scaling_group)
            self.autoscaler.dry_run = True
            self.autoscaler.set_capacity(2)
            assert not mock_update_auto_scaling_group.called
            self.autoscaler.dry_run = False

            self.autoscaler.set_capacity(2)
            mock_ec2_client.assert_called_with('autoscaling', region_name='westeros-1')
            mock_update_auto_scaling_group.assert_called_with(AutoScalingGroupName='asg-blah',
                                                              DesiredCapacity=2)

            with raises(autoscaling_cluster_lib.FailSetResourceCapacity):
                mock_update_auto_scaling_group.side_effect = ClientError({'Error': {'Code': 1}}, 'blah')
                self.autoscaler.set_capacity(2)

            mock_update_auto_scaling_group.side_effect = ClientError({'Error': {'Code': 'ValidationError'}}, 'blah')
            self.autoscaler.set_capacity(2)

    def test_get_instance_type_weights_asg(self):
        ret = self.autoscaler.get_instance_type_weights()
        assert ret['c4.thing'] == 1

    def test_get_asg_delta(self):
        self.autoscaler.resource = {
            'min_capacity': 2,
            'max_capacity': 10}
        self.autoscaler.asg = {'Instances': [mock.Mock()] * 5}
        ret = self.autoscaler.get_asg_delta(-0.2)
        assert ret == (5, 4)

        self.autoscaler.resource = {
            'min_capacity': 2,
            'max_capacity': 10}
        self.autoscaler.asg = {'Instances': [mock.Mock()] * 5}
        ret = self.autoscaler.get_asg_delta(0.2)
        assert ret == (5, 6)

        self.autoscaler.resource = {
            'min_capacity': 2,
            'max_capacity': 10}
        self.autoscaler.asg = {'Instances': [mock.Mock()] * 10}
        ret = self.autoscaler.get_asg_delta(0.2)
        assert ret == (10, 10)

        self.autoscaler.resource = {
            'min_capacity': 2,
            'max_capacity': 10}
        self.autoscaler.asg = {'Instances': [mock.Mock()] * 2}
        ret = self.autoscaler.get_asg_delta(-0.2)
        assert ret == (2, 2)

        self.autoscaler.resource = {
            'min_capacity': 0,
            'max_capacity': 10}
        self.autoscaler.asg = {'Instances': [mock.Mock()] * 2}
        ret = self.autoscaler.get_asg_delta(-1)
        assert ret == (2, 1)

        self.autoscaler.resource = {
            'min_capacity': 0,
            'max_capacity': 10}
        self.autoscaler.asg = {'Instances': [mock.Mock()] * 1}
        ret = self.autoscaler.get_asg_delta(-1)
        assert ret == (1, 0)

        # zero instances means we should launch one
        self.autoscaler.resource = {
            'min_capacity': 0,
            'max_capacity': 10}
        self.autoscaler.asg = {'Instances': []}
        ret = self.autoscaler.get_asg_delta(-1)
        assert ret == (0, 1)

        self.autoscaler.resource = {
            'min_capacity': 0,
            'max_capacity': 100}
        self.autoscaler.asg = {'Instances': [mock.Mock()] * 20}
        ret = self.autoscaler.get_asg_delta(-0.5)
        assert ret == (20, int(floor(20 * (1.0 - autoscaling_cluster_lib.MAX_CLUSTER_DELTA))))

        self.autoscaler.resource = {
            'min_capacity': 0,
            'max_capacity': 0}
        self.autoscaler.asg = {'Instances': [mock.Mock()] * 20}
        ret = self.autoscaler.get_asg_delta(-0.5)
        assert ret == (20, int(floor(20 * (1.0 - autoscaling_cluster_lib.MAX_CLUSTER_DELTA))))

    def test_asg_metrics_provider(self):
        with contextlib.nested(
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.AsgAutoscaler.get_asg_delta', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.AsgAutoscaler.get_mesos_utilization_error',
                       autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.AsgAutoscaler.get_aws_slaves', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.get_mesos_master', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.AsgAutoscaler.cleanup_cancelled_config',
                       autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.AsgAutoscaler.is_aws_launching_instances',
                       autospec=True),
        ) as (
            mock_get_asg_delta,
            mock_get_mesos_utilization_error,
            mock_get_aws_slaves,
            mock_get_mesos_master,
            mock_cleanup_cancelled_config,
            mock_is_aws_launching_asg_instances
        ):
            mock_get_asg_delta.return_value = 1, 2
            self.autoscaler.pool_settings = {}
            mock_is_aws_launching_asg_instances.return_value = False
            mock_mesos_state = mock.Mock()
            mock_master = mock.Mock(state=mock_mesos_state)
            mock_get_mesos_master.return_value = mock_master

            mock_slaves = mock.Mock()
            mock_get_aws_slaves.return_value = mock_slaves

            # cancelled ASG
            self.autoscaler.asg = None
            ret = self.autoscaler.metrics_provider()
            mock_cleanup_cancelled_config.assert_called_with(self.autoscaler, 'asg-blah', '/nail/blah', dry_run=False)
            assert not mock_get_aws_slaves.called
            assert ret == (0, 0)

            # active ASG
            self.autoscaler.asg = {'some': 'stuff'}
            mock_cleanup_cancelled_config.reset_mock()
            self.autoscaler.instances = [mock.Mock(), mock.Mock()]
            mock_get_mesos_utilization_error.return_value = float(0.3)
            ret = self.autoscaler.metrics_provider()
            mock_get_mesos_utilization_error.assert_called_with(self.autoscaler,
                                                                slaves=mock_get_aws_slaves.return_value,
                                                                mesos_state=mock_mesos_state,
                                                                expected_instances=2)
            mock_get_asg_delta.assert_called_with(self.autoscaler, float(0.3))
            assert not mock_cleanup_cancelled_config.called
            assert ret == (1, 2)

            # active ASG with AWS still provisioning
            mock_cleanup_cancelled_config.reset_mock()
            mock_is_aws_launching_asg_instances.return_value = True
            ret = self.autoscaler.metrics_provider()
            assert ret == (0, 0)

            # ASG with no instances
            self.autoscaler.instances = []
            mock_is_aws_launching_asg_instances.return_value = False
            self.autoscaler.metrics_provider()
            mock_get_asg_delta.assert_called_with(self.autoscaler, 1)

    def test_is_aws_launching_asg_instances(self):
        self.autoscaler.asg = {'DesiredCapacity': 3, 'Instances': [mock.Mock(), mock.Mock()]}
        assert self.autoscaler.is_aws_launching_instances()

        self.autoscaler.asg = {'DesiredCapacity': 1, 'Instances': [mock.Mock(), mock.Mock()]}
        assert not self.autoscaler.is_aws_launching_instances()

        self.autoscaler.asg = {'DesiredCapacity': 2, 'Instances': [mock.Mock(), mock.Mock()]}
        assert not self.autoscaler.is_aws_launching_instances()


class TestSpotAutoscaler(unittest.TestCase):

    def setUp(self):
        with contextlib.nested(
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.get_sfr', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.get_spot_fleet_instances',
                       autospec=True),
        ) as (
            mock_get_sfr,
            mock_get_spot_fleet_instances,
        ):
            mock_get_sfr.return_value = {}
            mock_get_spot_fleet_instances.return_value = []

            mock_resource = {'id': 'sfr-blah', 'type': 'sfr', 'region': 'westeros-1', 'pool': 'default'}
            mock_pool_settings = {'drain_timeout': 123}
            mock_config_folder = '/nail/blah'
            self.autoscaler = autoscaling_cluster_lib.SpotAutoscaler(mock_resource,
                                                                     mock_pool_settings,
                                                                     mock_config_folder,
                                                                     False)

    def test_get_spot_fleet_instances(self):
        with contextlib.nested(
            mock.patch('boto3.client', autospec=True),
        ) as (
            mock_ec2_client,
        ):
            mock_instances = mock.Mock()
            mock_sfr = {'ActiveInstances': mock_instances}
            mock_describe_spot_fleet_instances = mock.Mock(return_value=mock_sfr)
            mock_ec2_client.return_value = mock.Mock(describe_spot_fleet_instances=mock_describe_spot_fleet_instances)
            ret = self.autoscaler.get_spot_fleet_instances('sfr-blah', region='westeros-1')
            assert ret == mock_instances

    def test_is_aws_launching_sfr_instances(self):
        self.autoscaler.sfr = {'SpotFleetRequestConfig': {'FulfilledCapacity': 5,
                                                          'TargetCapacity': 10}}
        assert self.autoscaler.is_aws_launching_instances()

        self.autoscaler.sfr = {'SpotFleetRequestConfig': {'FulfilledCapacity': 10,
                                                          'TargetCapacity': 5}}
        assert not self.autoscaler.is_aws_launching_instances()

        self.autoscaler.sfr = {'SpotFleetRequestConfig': {'FulfilledCapacity': 10,
                                                          'TargetCapacity': 10}}
        assert not self.autoscaler.is_aws_launching_instances()

    def test_is_sfr_cancelled(self):
        self.autoscaler.sfr = {'SpotFleetRequestState': 'cancelled'}
        assert self.autoscaler.is_resource_cancelled()

        self.autoscaler.sfr = {'SpotFleetRequestState': 'cancelled_running'}
        assert self.autoscaler.is_resource_cancelled()

        self.autoscaler.sfr = {'SpotFleetRequestState': 'active'}
        assert not self.autoscaler.is_resource_cancelled()

        self.autoscaler.sfr = None
        assert self.autoscaler.is_resource_cancelled()

    def test_get_sfr(self):
        with contextlib.nested(
            mock.patch('boto3.client', autospec=True),
        ) as (
            mock_ec2_client,
        ):
            mock_sfr_config = mock.Mock()
            mock_sfr = {'SpotFleetRequestConfigs': [mock_sfr_config]}
            mock_describe_spot_fleet_requests = mock.Mock(return_value=mock_sfr)
            mock_ec2_client.return_value = mock.Mock(describe_spot_fleet_requests=mock_describe_spot_fleet_requests)
            ret = self.autoscaler.get_sfr('sfr-blah', region='westeros-1')
            mock_describe_spot_fleet_requests.assert_called_with(SpotFleetRequestIds=['sfr-blah'])
            assert ret == mock_sfr_config

            mock_error = {'Error': {'Code': 'InvalidSpotFleetRequestId.NotFound'}}
            mock_describe_spot_fleet_requests = mock.Mock(side_effect=ClientError(mock_error, 'blah'))
            mock_ec2_client.return_value = mock.Mock(describe_spot_fleet_requests=mock_describe_spot_fleet_requests)
            ret = self.autoscaler.get_sfr('sfr-blah', region='westeros-1')
            assert ret is None

    def test_set_spot_fleet_request_capacity(self):
        with contextlib.nested(
            mock.patch('boto3.client', autospec=True),
            mock.patch('time.sleep', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.get_sfr', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.AWS_SPOT_MODIFY_TIMEOUT', autospec=True)
        ) as (
            mock_ec2_client,
            mock_sleep,
            mock_get_sfr,
            _
        ):
            mock_sleep.side_effect = TimeoutError()
            mock_get_sfr.return_value = {'SpotFleetRequestState': 'modifying'}
            mock_modify_spot_fleet_request = mock.Mock()
            mock_ec2_client.return_value = mock.Mock(modify_spot_fleet_request=mock_modify_spot_fleet_request)
            with raises(autoscaling_cluster_lib.FailSetResourceCapacity):
                ret = self.autoscaler.set_capacity(4)
            assert not mock_modify_spot_fleet_request.called

            mock_modify_spot_fleet_request.side_effect = ClientError({'Error': {}}, 'blah')
            mock_get_sfr.return_value = {'SpotFleetRequestState': 'active'}
            with raises(autoscaling_cluster_lib.FailSetResourceCapacity):
                ret = self.autoscaler.set_capacity(4)

            mock_modify_spot_fleet_request.side_effect = None
            ret = self.autoscaler.set_capacity(4)
            mock_modify_spot_fleet_request.assert_called_with(SpotFleetRequestId='sfr-blah',
                                                              TargetCapacity=4,
                                                              ExcessCapacityTerminationPolicy='noTermination')
            assert ret is not None

    def test_get_instance_type_weights_sfr(self):
        mock_launch_specs = [{'InstanceType': 'c4.blah',
                              'WeightedCapacity': 123},
                             {'InstanceType': 'm4.whatever',
                              'WeightedCapacity': 456}]
        self.autoscaler.sfr = {'SpotFleetRequestConfig': {'LaunchSpecifications': mock_launch_specs}}
        ret = self.autoscaler.get_instance_type_weights()
        assert ret == {'c4.blah': 123, 'm4.whatever': 456}

    def test_get_spot_fleet_delta(self):
        self.autoscaler.resource = {
            'min_capacity': 2,
            'max_capacity': 10}
        self.autoscaler.sfr = {'SpotFleetRequestConfig': {'FulfilledCapacity': 5}}
        ret = self.autoscaler.get_spot_fleet_delta(-0.2)
        assert ret == (5, 4)

        self.autoscaler.resource = {
            'min_capacity': 2,
            'max_capacity': 10}
        self.autoscaler.sfr = {'SpotFleetRequestConfig': {'FulfilledCapacity': 7.3}}
        ret = self.autoscaler.get_spot_fleet_delta(-0.2)
        assert ret == (7.3, 6)

        self.autoscaler.resource = {
            'min_capacity': 2,
            'max_capacity': 10}
        self.autoscaler.sfr = {'SpotFleetRequestConfig': {'FulfilledCapacity': 5}}
        ret = self.autoscaler.get_spot_fleet_delta(0.2)
        assert ret == (5, 6)

        self.autoscaler.resource = {
            'min_capacity': 2,
            'max_capacity': 10}
        self.autoscaler.sfr = {'SpotFleetRequestConfig': {'FulfilledCapacity': 10}}
        ret = self.autoscaler.get_spot_fleet_delta(0.2)
        assert ret == (10, 10)

        self.autoscaler.resource = {
            'min_capacity': 2,
            'max_capacity': 10}
        self.autoscaler.sfr = {'SpotFleetRequestConfig': {'FulfilledCapacity': 2}}
        ret = self.autoscaler.get_spot_fleet_delta(-0.2)
        assert ret == (2, 2)

        self.autoscaler.resource = {
            'min_capacity': 0,
            'max_capacity': 10}
        self.autoscaler.sfr = {'SpotFleetRequestConfig': {'FulfilledCapacity': 1}}
        ret = self.autoscaler.get_spot_fleet_delta(-1)
        assert ret == (1, 1)

        self.autoscaler.resource = {
            'min_capacity': 0,
            'max_capacity': 100}
        self.autoscaler.sfr = {'SpotFleetRequestConfig': {'FulfilledCapacity': 20}}
        ret = self.autoscaler.get_spot_fleet_delta(-0.5)
        assert ret == (20, int(floor(20 * (1.0 - autoscaling_cluster_lib.MAX_CLUSTER_DELTA))))

    def test_spotfleet_metrics_provider(self):
        with contextlib.nested(
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.get_spot_fleet_delta',
                       autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.get_mesos_utilization_error',
                       autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.get_aws_slaves', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.get_pool_slaves',
                       autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.get_mesos_master', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.cleanup_cancelled_config',
                       autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.is_aws_launching_instances',
                       autospec=True),
        ) as (
            mock_get_spot_fleet_delta,
            mock_get_mesos_utilization_error,
            mock_get_aws_slaves,
            mock_get_pool_slaves,
            mock_get_mesos_master,
            mock_cleanup_cancelled_config,
            mock_is_aws_launching_sfr_instances
        ):
            mock_get_spot_fleet_delta.return_value = 1, 2
            self.autoscaler.pool_settings = {}
            mock_is_aws_launching_sfr_instances.return_value = False
            mock_mesos_state = mock.Mock()
            mock_master = mock.Mock(state=mock_mesos_state)
            mock_get_mesos_master.return_value = mock_master

            mock_slaves = mock.Mock()
            mock_get_aws_slaves.return_value = mock_slaves
            mock_get_pool_slaves.return_value = mock_slaves

            # cancelled SFR
            self.autoscaler.instances = [mock.Mock(), mock.Mock()]
            self.autoscaler.sfr = {'SpotFleetRequestState': 'cancelled'}
            ret = self.autoscaler.metrics_provider()
            mock_cleanup_cancelled_config.assert_called_with(self.autoscaler, 'sfr-blah', '/nail/blah', dry_run=False)
            assert not mock_get_mesos_master.called
            assert ret == (0, 0)

            # deleted SFR
            mock_cleanup_cancelled_config.reset_mock()
            self.autoscaler.sfr = None
            ret = self.autoscaler.metrics_provider()
            mock_cleanup_cancelled_config.assert_called_with(self.autoscaler, 'sfr-blah', '/nail/blah', dry_run=False)
            assert not mock_get_mesos_master.called
            assert ret == (0, 0)

            # active SFR
            mock_cleanup_cancelled_config.reset_mock()
            self.autoscaler.sfr = {'SpotFleetRequestState': 'active'}
            mock_get_mesos_utilization_error.return_value = float(0.3)
            ret = self.autoscaler.metrics_provider()
            mock_get_mesos_utilization_error.assert_called_with(self.autoscaler,
                                                                slaves=mock_get_aws_slaves.return_value,
                                                                mesos_state=mock_mesos_state,
                                                                expected_instances=2)
            mock_get_spot_fleet_delta.assert_called_with(self.autoscaler, float(0.3))
            assert not mock_cleanup_cancelled_config.called
            assert ret == (1, 2)

            # active SFR with AWS still provisioning
            mock_get_spot_fleet_delta.reset_mock()
            mock_cleanup_cancelled_config.reset_mock()
            self.autoscaler.sfr = {'SpotFleetRequestState': 'active'}
            mock_is_aws_launching_sfr_instances.return_value = True
            ret = self.autoscaler.metrics_provider()
            assert ret == (0, 0)
            assert not mock_get_spot_fleet_delta.called

            # cancelled_running SFR
            mock_cleanup_cancelled_config.reset_mock()
            mock_get_spot_fleet_delta.reset_mock()
            self.autoscaler.sfr = {'SpotFleetRequestState': 'cancelled_running'}
            ret = self.autoscaler.metrics_provider()
            assert not mock_cleanup_cancelled_config.called
            assert ret == (0, 0)
            mock_get_spot_fleet_delta.return_value = 2, 1
            mock_get_mesos_utilization_error.return_value = -0.2
            mock_get_mesos_utilization_error.reset_mock()
            ret = self.autoscaler.metrics_provider()
            get_utilization_calls = [mock.call(self.autoscaler,
                                               slaves=mock_slaves,
                                               mesos_state=mock_mesos_state,
                                               expected_instances=2),
                                     mock.call(self.autoscaler,
                                               slaves=mock_get_pool_slaves.return_value,
                                               mesos_state=mock_mesos_state)]
            mock_get_mesos_utilization_error.assert_has_calls(get_utilization_calls)

            assert ret == (2, 0)
            mock_get_spot_fleet_delta.return_value = 4, 2
            ret = self.autoscaler.metrics_provider()
            assert ret == (4, 2)

            # cancelled_running SFR with pool underprovisioned
            mock_get_mesos_utilization_error.return_value = 0.2
            ret = self.autoscaler.metrics_provider()
            assert ret == (0, 0)

            # SFR with no instances
            mock_get_mesos_master.reset_mock()
            self.autoscaler.instances = []
            ret = self.autoscaler.metrics_provider()
            assert ret == (0, 0)
            assert not mock_get_mesos_master.called

            # unknown SFR
            mock_get_mesos_master.reset_mock()
            mock_cleanup_cancelled_config.reset_mock()
            self.autoscaler.sfr = {'SpotFleetRequestState': 'not-a-state'}
            with raises(autoscaling_cluster_lib.ClusterAutoscalingError):
                ret = self.autoscaler.metrics_provider()
            assert not mock_get_mesos_master.called


class TestClusterAutoscaler(unittest.TestCase):

    def setUp(self):
        mock_resource = {'id': 'sfr-blah', 'type': 'sfr', 'region': 'westeros-1', 'pool': 'default'}
        mock_pool_settings = {'drain_timeout': 123}
        mock_config_folder = '/nail/blah'
        self.autoscaler = autoscaling_cluster_lib.ClusterAutoscaler(mock_resource,
                                                                    mock_pool_settings,
                                                                    mock_config_folder,
                                                                    False)

    def test_describe_instance(self):
        with contextlib.nested(
            mock.patch('boto3.client', autospec=True),
        ) as (
            mock_ec2_client,
        ):
            mock_instance_1 = mock.Mock()
            mock_instance_2 = mock.Mock()
            mock_instance_3 = mock.Mock()
            mock_instances = {'Reservations': [{'Instances': [mock_instance_1]}, {'Instances': [mock_instance_2]}]}
            mock_describe_instances = mock.Mock(return_value=mock_instances)
            mock_ec2_client.return_value = mock.Mock(describe_instances=mock_describe_instances)
            ret = self.autoscaler.describe_instances(['i-1', 'i-2'],
                                                     region='westeros-1',
                                                     instance_filters=['filter1'])
            mock_describe_instances.assert_called_with(InstanceIds=['i-1', 'i-2'], Filters=['filter1'])
            assert ret == [mock_instance_1, mock_instance_2]

            ret = self.autoscaler.describe_instances(['i-1', 'i-2'], region='westeros-1')
            mock_describe_instances.assert_called_with(InstanceIds=['i-1', 'i-2'], Filters=[])

            mock_error = {'Error': {'Code': 'InvalidInstanceID.NotFound'}}
            mock_describe_instances.side_effect = ClientError(mock_error, 'blah')
            ret = self.autoscaler.describe_instances(['i-1', 'i-2'], region='westeros-1')
            assert ret is None

            mock_instances = {'Reservations': [{'Instances': [mock_instance_1, mock_instance_2]},
                                               {'Instances': [mock_instance_3]}]}
            mock_describe_instances = mock.Mock(return_value=mock_instances)
            mock_ec2_client.return_value = mock.Mock(describe_instances=mock_describe_instances)
            ret = self.autoscaler.describe_instances(['i-1', 'i-2', 'i-3'], region='westeros-1')
            assert ret == [mock_instance_1, mock_instance_2, mock_instance_3]

    def test_scale_resource(self):
        with contextlib.nested(
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.filter_aws_slaves',
                       autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.get_mesos_master', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.get_mesos_task_count_by_slave', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.downscale_aws_resource',
                       autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.set_capacity', autospec=True)
        ) as (
            mock_filter_aws_slaves,
            mock_get_mesos_master,
            mock_get_mesos_task_count_by_slave,
            mock_downscale_aws_resource,
            mock_set_capacity
        ):

            mock_set_capacity.return_value = True
            mock_master = mock.Mock()
            mock_mesos_state = mock.Mock()
            mock_master.state_summary.return_value = mock_mesos_state
            mock_get_mesos_master.return_value = mock_master

            # test no scale
            self.autoscaler.scale_resource(4, 4)
            assert not mock_set_capacity.called

            # test scale up
            self.autoscaler.scale_resource(2, 4)
            mock_set_capacity.assert_called_with(self.autoscaler, 4)

            # test scale down
            mock_slave_1 = {'instance_weight': 1}
            mock_slave_2 = {'instance_weight': 2}
            mock_sfr_sorted_slaves_1 = [mock_slave_1, mock_slave_2]
            mock_filter_aws_slaves.return_value = mock_sfr_sorted_slaves_1
            self.autoscaler.scale_resource(5, 2)
            assert mock_get_mesos_master.called
            mock_get_mesos_task_count_by_slave.assert_called_with(mock_mesos_state,
                                                                  pool='default')
            mock_filter_aws_slaves.assert_called_with(self.autoscaler, mock_get_mesos_task_count_by_slave.return_value)
            mock_downscale_aws_resource.assert_called_with(self.autoscaler,
                                                           filtered_slaves=mock_filter_aws_slaves.return_value,
                                                           current_capacity=5,
                                                           target_capacity=2)

    def test_downscale_aws_resource(self):
        with contextlib.nested(
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.get_mesos_master', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.get_mesos_task_count_by_slave', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.sort_slaves_to_kill',
                       autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.gracefully_terminate_slave',
                       autospec=True)
        ) as (
            mock_get_mesos_master,
            mock_get_mesos_task_count_by_slave,
            mock_sort_slaves_to_kill,
            mock_gracefully_terminate_slave
        ):
            mock_master = mock.Mock()
            mock_mesos_state = mock.Mock()
            mock_master.state_summary.return_value = mock_mesos_state
            mock_get_mesos_master.return_value = mock_master
            mock_slave_1 = {'hostname': 'host1', 'instance_id': 'i-blah123',
                            'instance_weight': 1}
            mock_slave_2 = {'hostname': 'host2', 'instance_id': 'i-blah456',
                            'instance_weight': 2}
            self.autoscaler.resource = {'type': 'aws_spot_fleet_request', 'sfr': {'SpotFleetRequestState': 'active'}}
            mock_filtered_slaves = mock.Mock()
            mock_sfr_sorted_slaves_1 = [mock_slave_1, mock_slave_2]
            mock_sfr_sorted_slaves_2 = [mock_slave_2]
            mock_terminate_call_1 = mock.call(self.autoscaler,
                                              slave_to_kill=mock_slave_1,
                                              current_capacity=5,
                                              new_capacity=4)
            mock_terminate_call_2 = mock.call(self.autoscaler,
                                              slave_to_kill=mock_slave_2,
                                              current_capacity=4,
                                              new_capacity=2)
            # for draining slave 1 failure HTTPError scenario
            mock_terminate_call_3 = mock.call(self.autoscaler,
                                              slave_to_kill=mock_slave_2,
                                              current_capacity=5,
                                              new_capacity=3)

            # test we kill only one instance on scale down and then reach capacity
            mock_sort_slaves_to_kill.return_value = mock_sfr_sorted_slaves_2[:]
            self.autoscaler.downscale_aws_resource(
                filtered_slaves=mock_filtered_slaves,
                current_capacity=5,
                target_capacity=4)
            assert mock_gracefully_terminate_slave.call_count == 1

            # test stop if FailSetSpotCapacity
            mock_gracefully_terminate_slave.side_effect = autoscaling_cluster_lib.FailSetResourceCapacity
            mock_sfr_sorted_slaves_1 = [mock_slave_1, mock_slave_2]
            mock_sfr_sorted_slaves_2 = [mock_slave_2]
            mock_sort_slaves_to_kill.side_effect = [mock_sfr_sorted_slaves_1,
                                                    mock_sfr_sorted_slaves_2,
                                                    []]
            self.autoscaler.downscale_aws_resource(
                filtered_slaves=mock_filtered_slaves,
                current_capacity=5,
                target_capacity=2)
            mock_gracefully_terminate_slave.assert_has_calls([mock_terminate_call_1])

            # test continue if HTTPError
            mock_gracefully_terminate_slave.side_effect = HTTPError
            mock_gracefully_terminate_slave.reset_mock()
            mock_sfr_sorted_slaves_1 = [mock_slave_1, mock_slave_2]
            mock_sfr_sorted_slaves_2 = [mock_slave_2]
            mock_sort_slaves_to_kill.side_effect = [mock_sfr_sorted_slaves_1,
                                                    mock_sfr_sorted_slaves_2,
                                                    []]
            self.autoscaler.downscale_aws_resource(
                filtered_slaves=mock_filtered_slaves,
                current_capacity=5,
                target_capacity=2)
            mock_gracefully_terminate_slave.assert_has_calls([mock_terminate_call_1, mock_terminate_call_3])

            # test normal scale down
            mock_gracefully_terminate_slave.side_effect = None
            mock_gracefully_terminate_slave.reset_mock()
            mock_get_mesos_task_count_by_slave.reset_mock()
            mock_sort_slaves_to_kill.reset_mock()
            mock_sfr_sorted_slaves_1 = [mock_slave_1, mock_slave_2]
            mock_sfr_sorted_slaves_2 = [mock_slave_2]
            mock_sort_slaves_to_kill.side_effect = [mock_sfr_sorted_slaves_1,
                                                    mock_sfr_sorted_slaves_2,
                                                    []]
            self.autoscaler.downscale_aws_resource(
                filtered_slaves=mock_filtered_slaves,
                current_capacity=5,
                target_capacity=2)
            mock_sort_slaves_to_kill.assert_has_calls([mock.call(self.autoscaler, mock_filtered_slaves),
                                                       mock.call(self.autoscaler,
                                                                 mock_get_mesos_task_count_by_slave.return_value)])
            assert mock_get_mesos_master.called
            mock_gracefully_terminate_slave.assert_has_calls([mock_terminate_call_1, mock_terminate_call_2])
            mock_get_task_count_calls = [mock.call(mock_mesos_state, slaves_list=[mock_slave_2])]
            mock_get_mesos_task_count_by_slave.assert_has_calls(mock_get_task_count_calls)

            # test non integer scale down
            # this should result in killing 3 instances,
            # leaving us on 7.1 provisioned of target 7
            mock_slave_1 = {'hostname': 'host1', 'instance_id': 'i-blah123',
                            'instance_weight': 0.3}
            mock_gracefully_terminate_slave.side_effect = None
            mock_gracefully_terminate_slave.reset_mock()
            mock_get_mesos_task_count_by_slave.reset_mock()
            mock_sort_slaves_to_kill.reset_mock()
            mock_sfr_sorted_slaves = [mock_slave_1] * 10
            mock_sort_slaves_to_kill.side_effect = [mock_sfr_sorted_slaves] + \
                [mock_sfr_sorted_slaves[x:-1] for x in range(0, 10)]
            self.autoscaler.downscale_aws_resource(
                filtered_slaves=mock_filtered_slaves,
                current_capacity=8,
                target_capacity=7)
            assert mock_gracefully_terminate_slave.call_count == 3

    def test_gracefully_terminate_slave(self):
        with contextlib.nested(
            mock.patch('time.time', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.drain', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.undrain', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.wait_and_terminate',
                       autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.set_capacity',
                       autospec=True),
        ) as (
            mock_time,
            mock_drain,
            mock_undrain,
            mock_wait_and_terminate,
            mock_set_capacity,
        ):
            self.autoscaler.resource = {'id': 'sfr-blah', 'region': 'westeros-1', 'type': 'sfr'}
            mock_time.return_value = int(1)
            mock_start = (1 + 123) * 1000000000
            mock_slave = {'hostname': 'host1', 'instance_id': 'i-blah123',
                          'pid': 'slave(1)@10.1.1.1:5051', 'instance_weight': 1,
                          'ip': '10.1.1.1'}
            self.autoscaler.gracefully_terminate_slave(
                slave_to_kill=mock_slave,
                current_capacity=5,
                new_capacity=4)

            mock_drain.assert_called_with(['host1|10.1.1.1'], mock_start, 600 * 1000000000)
            set_call_1 = mock.call(self.autoscaler, 4)
            mock_set_capacity.assert_has_calls([set_call_1])
            mock_wait_and_terminate.assert_called_with(self.autoscaler, mock_slave, 123, False, region='westeros-1')
            mock_undrain.assert_called_with(['host1|10.1.1.1'])

            # test we cleanup if a termination fails
            set_call_2 = mock.call(self.autoscaler, 5)
            mock_wait_and_terminate.side_effect = ClientError({'Error': {}}, 'blah')
            self.autoscaler.gracefully_terminate_slave(
                slave_to_kill=mock_slave,
                current_capacity=5,
                new_capacity=4)
            mock_drain.assert_called_with(['host1|10.1.1.1'], mock_start, 600 * 1000000000)
            mock_set_capacity.assert_has_calls([set_call_1, set_call_2])
            mock_wait_and_terminate.assert_called_with(self.autoscaler, mock_slave, 123, False, region='westeros-1')
            mock_undrain.assert_called_with(['host1|10.1.1.1'])

            # test we cleanup if a set spot capacity fails
            mock_wait_and_terminate.side_effect = None
            mock_wait_and_terminate.reset_mock()
            mock_set_capacity.side_effect = autoscaling_cluster_lib.FailSetResourceCapacity
            with raises(autoscaling_cluster_lib.FailSetResourceCapacity):
                self.autoscaler.gracefully_terminate_slave(
                    slave_to_kill=mock_slave,
                    current_capacity=5,
                    new_capacity=4)
            mock_drain.assert_called_with(['host1|10.1.1.1'], mock_start, 600 * 1000000000)
            mock_set_capacity.assert_has_calls([set_call_1])
            mock_undrain.assert_called_with(['host1|10.1.1.1'])
            assert not mock_wait_and_terminate.called

            # test we cleanup if a drain fails
            mock_wait_and_terminate.side_effect = None
            mock_set_capacity.side_effect = None
            mock_set_capacity.reset_mock()
            mock_drain.side_effect = HTTPError
            with raises(HTTPError):
                self.autoscaler.gracefully_terminate_slave(
                    slave_to_kill=mock_slave,
                    current_capacity=5,
                    new_capacity=4)
            mock_drain.assert_called_with(['host1|10.1.1.1'], mock_start, 600 * 1000000000)
            assert not mock_set_capacity.called
            assert not mock_wait_and_terminate.called

    def test_get_instances_from_ip(self):
        mock_instances = []
        ret = self.autoscaler.get_instances_from_ip('10.1.1.1', mock_instances)
        assert ret == []

        mock_instances = [{'InstanceId': 'i-blah', 'PrivateIpAddress': '10.1.1.1'}]
        ret = self.autoscaler.get_instances_from_ip('10.1.1.1', mock_instances)
        assert ret == mock_instances

    def test_wait_and_terminate(self):
        with contextlib.nested(
            mock.patch('boto3.client', autospec=True),
            mock.patch('time.sleep', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.is_safe_to_kill', autospec=True),
        ) as (
            mock_ec2_client,
            _,
            mock_is_safe_to_kill,
        ):
            mock_terminate_instances = mock.Mock()
            mock_ec2_client.return_value = mock.Mock(terminate_instances=mock_terminate_instances)

            mock_is_safe_to_kill.return_value = True
            mock_slave_to_kill = {'ip': '10.1.1.1', 'instance_id': 'i-blah123', 'pid': 'slave(1)@10.1.1.1:5051',
                                  'hostname': 'hostblah'}
            self.autoscaler.wait_and_terminate(mock_slave_to_kill, 600, False, region='westeros-1')
            mock_terminate_instances.assert_called_with(InstanceIds=['i-blah123'], DryRun=False)
            mock_is_safe_to_kill.assert_called_with('hostblah')

            mock_is_safe_to_kill.side_effect = [False, False, True]
            self.autoscaler.wait_and_terminate(mock_slave_to_kill, 600, False, region='westeros-1')
            assert mock_is_safe_to_kill.call_count == 4

    def test_sort_slaves_to_kill(self):
        # test no slaves
        ret = self.autoscaler.sort_slaves_to_kill({})
        assert ret == []

        mock_slave_1 = mock.Mock()
        mock_slave_2 = mock.Mock()
        mock_slave_3 = mock.Mock()
        mock_slave_1 = {'task_counts': SlaveTaskCount(count=3, slave=mock_slave_1, chronos_count=0)}
        mock_slave_2 = {'task_counts': SlaveTaskCount(count=2, slave=mock_slave_2, chronos_count=1)}
        mock_slave_3 = {'task_counts': SlaveTaskCount(count=5, slave=mock_slave_3, chronos_count=0)}
        mock_task_count = [mock_slave_1, mock_slave_2, mock_slave_3]
        ret = self.autoscaler.sort_slaves_to_kill(mock_task_count)
        assert ret == [mock_slave_1, mock_slave_3, mock_slave_2]

    def test_get_instance_ips(self):
        with contextlib.nested(
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.describe_instances',
                       autospec=True),
        ) as (
            mock_describe_instances,
        ):
            mock_instance_ids = [{'InstanceId': 'i-blah1'}, {'InstanceId': 'i-blah2'}]
            mock_instances = [{'PrivateIpAddress': '10.1.1.1'}, {'PrivateIpAddress': '10.2.2.2'}]
            mock_describe_instances.return_value = mock_instances
            ret = self.autoscaler.get_instance_ips(mock_instance_ids, region='westeros-1')
            mock_describe_instances.assert_called_with(self.autoscaler, ['i-blah1', 'i-blah2'], region='westeros-1')
            assert ret == ['10.1.1.1', '10.2.2.2']

    def test_filter_aws_slaves(self):
        with contextlib.nested(
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.get_instance_ips',
                       autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.slave_pid_to_ip', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.get_instances_from_ip',
                       autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.describe_instances',
                       autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.get_instance_type_weights',
                       autospec=True),
        ) as (
            mock_get_instance_ips,
            mock_pid_to_ip,
            mock_get_instances_from_ip,
            mock_describe_instances,
            mock_get_instance_type_weights
        ):
            mock_get_instance_ips.return_value = ['10.1.1.1', '10.3.3.3']
            mock_pid_to_ip.side_effect = ['10.1.1.1', '10.2.2.2', '10.3.3.3',
                                          '10.1.1.1', '10.3.3.3', '10.1.1.1', '10.3.3.3']
            mock_get_instances_from_ip.side_effect = [[{'InstanceId': 'i-1'}], [{'InstanceId': 'i-3'}]]
            mock_instances = [{'InstanceId': 'i-1',
                               'InstanceType': 'c4.blah'},
                              {'InstanceId': 'i-2',
                               'InstanceType': 'm4.whatever'},
                              {'InstanceId': 'i-3',
                               'InstanceType': 'm4.whatever'}]
            self.autoscaler.instances = mock_instances
            mock_describe_instances.return_value = mock_instances
            mock_slave_1 = {'task_counts': SlaveTaskCount(slave={'pid': 'slave(1)@10.1.1.1:5051', 'id': '123',
                                                                 'hostname': 'host123'},
                                                          count=0, chronos_count=0)}
            mock_slave_2 = {'task_counts': SlaveTaskCount(slave={'pid': 'slave(2)@10.2.2.2:5051', 'id': '456',
                                                                 'hostname': 'host456'},
                                                          count=0, chronos_count=0)}
            mock_slave_3 = {'task_counts': SlaveTaskCount(slave={'pid': 'slave(3)@10.3.3.3:5051', 'id': '789',
                                                                 'hostname': 'host789'},
                                                          count=0, chronos_count=0)}

            mock_sfr_sorted_slaves = [mock_slave_1, mock_slave_2, mock_slave_3]
            mock_get_instance_call_1 = mock.call(self.autoscaler, '10.1.1.1', mock_instances)
            mock_get_instance_call_3 = mock.call(self.autoscaler, '10.3.3.3', mock_instances)
            mock_get_ip_call_1 = mock.call('slave(1)@10.1.1.1:5051')
            mock_get_ip_call_2 = mock.call('slave(2)@10.2.2.2:5051')
            mock_get_ip_call_3 = mock.call('slave(3)@10.3.3.3:5051')
            mock_get_instance_type_weights.return_value = {'c4.blah': 2, 'm4.whatever': 5}
            ret = self.autoscaler.filter_aws_slaves(mock_sfr_sorted_slaves)
            mock_get_instance_ips.assert_called_with(self.autoscaler, mock_instances, region='westeros-1')
            mock_pid_to_ip.assert_has_calls([mock_get_ip_call_1, mock_get_ip_call_2, mock_get_ip_call_3,
                                             mock_get_ip_call_1, mock_get_ip_call_3])
            mock_get_instances_from_ip.assert_has_calls([mock_get_instance_call_1, mock_get_instance_call_3])
            mock_describe_instances.assert_called_with(self.autoscaler, [], region='westeros-1',
                                                       instance_filters=[{'Values': ['10.1.1.1', '10.3.3.3'],
                                                                          'Name': 'private-ip-address'}])
            mock_get_instance_type_weights.assert_called_with(self.autoscaler)
            expected = [{'pid': 'slave(1)@10.1.1.1:5051',
                         'instance_id': 'i-1',
                         'id': '123',
                         'instance_type': 'c4.blah',
                         'task_counts': mock_slave_1['task_counts'],
                         'hostname': 'host123',
                         'ip': '10.1.1.1',
                         'instance_weight': 2},
                        {'pid': 'slave(3)@10.3.3.3:5051',
                         'instance_id': 'i-3',
                         'id': '789',
                         'instance_type': 'm4.whatever',
                         'task_counts': mock_slave_3['task_counts'],
                         'hostname': 'host789',
                         'ip': '10.3.3.3',
                         'instance_weight': 5}]
            assert ret == expected

    def test_get_aws_slaves(self):
        with contextlib.nested(
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.get_instance_ips',
                       autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.slave_pid_to_ip', autospec=True),
        ) as (
            mock_get_instance_ips,
            mock_slave_pid_to_ip
        ):
            mock_slave_pid_to_ip.side_effect = pid_to_ip_sideeffect
            mock_get_instance_ips.return_value = ['10.1.1.1', '10.3.3.3', '10.4.4.4']
            self.autoscaler.instances = [mock.Mock(), mock.Mock(), mock.Mock()]
            mock_mesos_state = {'slaves': [{'id': 'id1',
                                            'attributes': {'pool': 'default'},
                                            'pid': 'pid1'},
                                           {'id': 'id2',
                                            'attributes': {'pool': 'default'},
                                            'pid': 'pid2'},
                                           {'id': 'id3',
                                            'attributes': {'pool': 'notdefault'},
                                            'pid': 'pid3'}]}
            ret = self.autoscaler.get_aws_slaves(mock_mesos_state)
            mock_get_instance_ips.assert_called_with(self.autoscaler, self.autoscaler.instances, region='westeros-1')
            assert ret == {'id1': mock_mesos_state['slaves'][0]}

    def test_cleanup_cancelled_config(self):
        with contextlib.nested(
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.os.walk', autospec=True),
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.os.remove', autospec=True),
        ) as (
            mock_os_walk,
            mock_os_remove
        ):
            mock_os_walk.return_value = [('/nail/blah', [], ['sfr-blah.json', 'sfr-another.json']),
                                         ('/nail/another', [], ['something'])]
            self.autoscaler.cleanup_cancelled_config('sfr-blah', '/nail')
            mock_os_walk.assert_called_with('/nail')
            mock_os_remove.assert_called_with('/nail/blah/sfr-blah.json')

            mock_os_remove.reset_mock()
            self.autoscaler.cleanup_cancelled_config('sfr-blah-not-exist', '/nail')
            assert not mock_os_remove.called

    def test_get_mesos_utilization_error(self):
        with contextlib.nested(
            mock.patch('paasta_tools.autoscaling.autoscaling_cluster_lib.get_resource_utilization_by_grouping',
                       autospec=True),
        ) as (
            mock_get_resource_utilization_by_grouping,
        ):

            mock_mesos_state = {'slaves': [{'attributes': {'pool': 'default'}},
                                           {'attributes': {'pool': 'default'}}]}
            mock_utilization = {'free': ResourceInfo(cpus=7.0, mem=2048.0, disk=30.0),
                                'total': ResourceInfo(cpus=10.0, mem=4096.0, disk=40.0)}
            mock_get_resource_utilization_by_grouping.return_value = {'default': mock_utilization}
            self.autoscaler.pool_settings = {'target_utilization': 0.8}

            ret = self.autoscaler.get_mesos_utilization_error(
                slaves=mock_mesos_state['slaves'],
                mesos_state=mock_mesos_state,
                expected_instances=2)
            assert ret == 0.5 - 0.8

            mock_mesos_state['slaves'].pop()
            with raises(autoscaling_cluster_lib.ClusterAutoscalingError):
                self.autoscaler.get_mesos_utilization_error(
                    slaves=mock_mesos_state['slaves'],
                    mesos_state=mock_mesos_state,
                    expected_instances=2)

    def test_get_pool_slaves(self):
        self.autoscaler.resource = {'pool': 'default'}
        mock_mesos_state = {'slaves': [{'id': 'id1',
                                        'attributes': {'pool': 'default'},
                                        'pid': 'pid1'},
                                       {'id': 'id3',
                                        'attributes': {'pool': 'notdefault'},
                                        'pid': 'pid3'}]}
        ret = self.autoscaler.get_pool_slaves(mock_mesos_state)
        assert ret == {'id1': mock_mesos_state['slaves'][0]}
