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
import asyncio
import contextlib
import unittest
import warnings
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from math import ceil
from math import floor

import asynctest
import mock
from botocore.exceptions import ClientError
from pytest import mark
from pytest import raises
from requests.exceptions import HTTPError

from paasta_tools.autoscaling import autoscaling_cluster_lib
from paasta_tools.mesos_tools import SlaveTaskCount
from paasta_tools.metrics.metastatus_lib import ResourceInfo
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import TimeoutError


warnings.filterwarnings("error", category=RuntimeWarning)
asyncio.get_event_loop().set_debug(True)


def _run(coro):
    asyncio.set_event_loop(asyncio.new_event_loop())
    asyncio.get_event_loop().set_debug(True)
    return asyncio.get_event_loop().run_until_complete(coro)


def get_coro_with_exception(error):
    async def f(*args, **kwargs):
        await asyncio.sleep(0)
        raise error

    return f


class AsyncNone:
    """Same as asyncio.sleep(0), but needed to be able to patch asyncio.sleep"""

    def __await__(self):
        yield


async def just_sleep(*a, **k):
    await AsyncNone()


def pid_to_ip_sideeffect(pid):
    pid_to_ip = {"pid1": "10.1.1.1", "pid2": "10.2.2.2", "pid3": "10.3.3.3"}
    return pid_to_ip[pid]


def is_resource_cancelled_sideeffect(self):
    if self.resource["id"] == "sfr-blah3":
        return True
    return False


def test_get_mesos_utilization_error():
    mock_system_config = mock.Mock(return_value={})
    with mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.get_resource_utilization_by_grouping",
        autospec=True,
    ) as mock_get_resource_utilization_by_grouping:
        mock_mesos_state = {
            "slaves": [
                {"attributes": {"pool": "default"}},
                {"attributes": {"pool": "default"}},
            ]
        }
        mock_utilization = {
            "free": ResourceInfo(cpus=7.0, mem=2048.0, disk=30.0),
            "total": ResourceInfo(cpus=10.0, mem=4096.0, disk=40.0),
        }
        mock_get_resource_utilization_by_grouping.return_value = {
            (("pool", "default"), ("region", "westeros-1")): mock_utilization
        }

        ret = autoscaling_cluster_lib.get_mesos_utilization_error(
            mesos_state=mock_mesos_state,
            system_config=mock_system_config,
            region="westeros-1",
            pool="default",
            target_utilization=0.8,
        )
        assert ret == 0.5 - 0.8

        ret = autoscaling_cluster_lib.get_mesos_utilization_error(
            mesos_state=mock_mesos_state,
            system_config=mock_system_config,
            region="westeros-1",
            pool="fake-pool",
            target_utilization=0.8,
        )
        assert ret == 0


def test_get_instances_from_ip():
    mock_instances = []
    ret = autoscaling_cluster_lib.get_instances_from_ip("10.1.1.1", mock_instances)
    assert ret == []

    mock_instances = [{"InstanceId": "i-blah", "PrivateIpAddress": "10.1.1.1"}]
    ret = autoscaling_cluster_lib.get_instances_from_ip("10.1.1.1", mock_instances)
    assert ret == mock_instances


@mark.asyncio
async def test_autoscale_local_cluster_with_cancelled():
    with mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.load_system_paasta_config",
        autospec=True,
    ) as mock_get_paasta_config, mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.autoscale_cluster_resource",
        autospec=True,
    ) as mock_autoscale_cluster_resource, mock.patch(
        "time.sleep", autospec=True
    ), mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.asyncio.sleep",
        autospec=True,
        side_effect=just_sleep,
    ), mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.is_resource_cancelled",
        autospec=True,
    ) as mock_is_resource_cancelled, mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.get_sfr",
        autospec=True,
    ) as mock_get_sfr, mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.get_all_utilization_errors",
        autospec=True,
    ) as mock_get_all_utilization_errors, mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.get_mesos_master",
        autospec=True,
    ) as mock_get_mesos_master, mock.patch(
        "paasta_tools.metrics.metrics_lib.load_system_paasta_config", autospec=True
    ) as mock_get_metrics_system_paasta_config:
        mock_get_sfr.return_value = False
        mock_scaling_resources = {
            "id1": {
                "id": "sfr-blah1",
                "type": "aws_spot_fleet_request",
                "pool": "default",
                "region": "westeros-1",
            },
            "id2": {
                "id": "sfr-blah2",
                "type": "aws_spot_fleet_request",
                "pool": "default",
                "region": "westeros-1",
            },
            "id3": {
                "id": "sfr-blah3",
                "type": "aws_spot_fleet_request",
                "pool": "default",
                "region": "westeros-1",
            },
        }
        mock_resource_pool_settings = {
            "default": {"drain_timeout": 123, "target_utilization": 0.75}
        }
        mock_get_cluster_autoscaling_resources = mock.Mock(
            return_value=mock_scaling_resources
        )
        mock_get_resource_pool_settings = mock.Mock(
            return_value=mock_resource_pool_settings
        )
        mock_is_resource_cancelled.side_effect = is_resource_cancelled_sideeffect
        mock_get_resources = mock.Mock(
            get_cluster_autoscaling_resources=mock_get_cluster_autoscaling_resources,
            get_resource_pool_settings=mock_get_resource_pool_settings,
            get_metrics_provider=lambda: None,
        )
        mock_get_paasta_config.return_value = mock_get_resources
        mock_get_metrics_system_paasta_config.return_value = mock_get_resources
        mock_get_all_utilization_errors.return_value = {("westeros-1", "default"): -0.2}
        mock_mesos_state = mock.Mock()
        mock_master = mock.Mock(
            state=asynctest.CoroutineMock(return_value=mock_mesos_state)
        )
        mock_get_mesos_master.return_value = mock_master
        calls = []

        async def fake_autoscale(scaler, state):
            calls.append(scaler)
            await asyncio.sleep(0)

        mock_autoscale_cluster_resource.side_effect = fake_autoscale

        asyncio.set_event_loop(asyncio.new_event_loop())
        await autoscaling_cluster_lib.autoscale_local_cluster(
            config_folder="/nail/blah"
        )
        assert mock_get_paasta_config.called
        autoscaled_resources = [
            call[0][0].resource
            for call in mock_autoscale_cluster_resource.call_args_list
        ]
        assert autoscaled_resources[0] == mock_scaling_resources["id3"]
        assert len(calls) == 1


@mark.asyncio
async def test_autoscale_local_cluster():
    with mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.load_system_paasta_config",
        autospec=True,
    ) as mock_get_paasta_config, mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.autoscale_cluster_resource",
        autospec=True,
    ) as mock_autoscale_cluster_resource, mock.patch(
        "time.sleep", autospec=True
    ), mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.asyncio.sleep", autospec=True
    ) as mock_sleep, mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.is_resource_cancelled",
        autospec=True,
    ) as mock_is_resource_cancelled, mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.get_sfr",
        autospec=True,
    ) as mock_get_sfr, mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.get_all_utilization_errors",
        autospec=True,
    ) as mock_get_all_utilization_errors, mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.get_mesos_master",
        autospec=True,
    ) as mock_get_mesos_master, mock.patch(
        "paasta_tools.metrics.metrics_lib.load_system_paasta_config", autospec=True
    ) as mock_get_metrics_system_paasta_config:
        mock_sleep.side_effect = just_sleep
        mock_get_sfr.return_value = False
        mock_scaling_resources = {
            "id1": {
                "id": "sfr-blah1",
                "type": "aws_spot_fleet_request",
                "pool": "default",
                "region": "westeros-1",
            },
            "id2": {
                "id": "sfr-blah2",
                "type": "aws_spot_fleet_request",
                "pool": "default",
                "region": "westeros-1",
            },
            "id4": {
                "id": "sfr-blah4",
                "type": "aws_spot_fleet_request",
                "pool": "default",
                "region": "westeros-1",
            },
        }
        mock_resource_pool_settings = {
            "default": {"drain_timeout": 123, "target_utilization": 0.75}
        }
        mock_get_cluster_autoscaling_resources = mock.Mock(
            return_value=mock_scaling_resources
        )
        mock_get_resource_pool_settings = mock.Mock(
            return_value=mock_resource_pool_settings
        )
        mock_is_resource_cancelled.side_effect = is_resource_cancelled_sideeffect
        mock_get_resources = mock.Mock(
            get_cluster_autoscaling_resources=mock_get_cluster_autoscaling_resources,
            get_resource_pool_settings=mock_get_resource_pool_settings,
            get_metrics_provider=lambda: None,
        )
        mock_get_paasta_config.return_value = mock_get_resources
        mock_get_metrics_system_paasta_config.return_value = mock_get_resources
        mock_get_all_utilization_errors.return_value = {("westeros-1", "default"): -0.2}
        mock_mesos_state = mock.Mock()
        mock_master = mock.Mock(
            state=asynctest.CoroutineMock(return_value=mock_mesos_state)
        )
        mock_get_mesos_master.return_value = mock_master
        calls = []

        async def fake_autoscale(scaler, state):
            calls.append(scaler)
            await asyncio.sleep(0)

        mock_autoscale_cluster_resource.side_effect = fake_autoscale

        asyncio.set_event_loop(asyncio.new_event_loop())
        await autoscaling_cluster_lib.autoscale_local_cluster(
            config_folder="/nail/blah"
        )
        assert mock_get_paasta_config.called
        autoscaled_resources = [
            call[0][0].resource
            for call in mock_autoscale_cluster_resource.call_args_list
        ]
        assert mock_scaling_resources["id2"] in autoscaled_resources
        assert mock_scaling_resources["id1"] in autoscaled_resources
        assert mock_scaling_resources["id4"] in autoscaled_resources
        assert len(calls) == 3


def test_filter_scalers():
    resource1 = mock.Mock(is_resource_cancelled=lambda: False)
    resource2 = mock.Mock(is_resource_cancelled=lambda: False)
    resource3 = mock.Mock(is_resource_cancelled=lambda: True)
    resource4 = mock.Mock(is_resource_cancelled=lambda: False)
    resource5 = mock.Mock(is_resource_cancelled=lambda: True)
    resource6 = mock.Mock(is_resource_cancelled=lambda: False)
    autoscaling_scalers = {
        ("westeros-1", "default"): [resource1, resource2],
        ("westeros-2", "default"): [resource3, resource4],
        ("westeros-3", "default"): [resource5, resource6],
    }
    utilization_errors = {
        ("westeros-1", "default"): -0.2,
        ("westeros-2", "default"): -0.2,
        ("westeros-3", "default"): 0.2,
    }

    ret = autoscaling_cluster_lib.filter_scalers(
        autoscaling_scalers, utilization_errors
    )
    assert len(ret) == 5
    assert resource4 not in ret


def test_autoscale_cluster_resource():
    call = []

    async def mock_scale_resource(current, target):
        call.append((current, target))
        await asyncio.sleep(0)

    mock_scaling_resource = {"id": "sfr-blah", "type": "sfr", "pool": "default"}
    mock_scaler = mock.Mock()
    mock_metrics_provider = mock.Mock(return_value=(2, 6))
    mock_scaler.metrics_provider = mock_metrics_provider
    mock_scaler.scale_resource = mock_scale_resource
    mock_scaler.resource = mock_scaling_resource
    mock_state = mock.Mock()

    # test scale up
    _run(autoscaling_cluster_lib.autoscale_cluster_resource(mock_scaler, mock_state))
    assert mock_metrics_provider.called
    assert (2, 6) in call


def test_get_autoscaling_info_for_all_resources():
    mock_resource_1 = {"region": "westeros-1", "pool": "default"}
    mock_resource_2 = {"region": "westeros-1", "pool": "not-default"}
    mock_resources = {"id1": mock_resource_1, "id2": mock_resource_2}
    mock_get_cluster_autoscaling_resources = mock.Mock(return_value=mock_resources)
    mock_system_config = mock.Mock(
        get_cluster_autoscaling_resources=mock_get_cluster_autoscaling_resources,
        get_resource_pool_settings=mock.Mock(
            return_value={"default": {}, "not-default": {}}
        ),
    )

    mock_autoscaling_info = mock.Mock()

    def mock_autoscaling_info_for_resource_side_effect(
        resource,
        pool_settings,
        mesos_state,
        utilization_errors,
        max_increase,
        max_decrease,
    ):
        return {
            (mock_resource_1["region"], mock_resource_1["pool"]): None,
            (mock_resource_2["region"], mock_resource_2["pool"]): mock_autoscaling_info,
        }[(resource["region"], resource["pool"])]

    with mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.load_system_paasta_config",
        autospec=True,
        return_value=mock_system_config,
    ), mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.autoscaling_info_for_resource",
        autospec=True,
    ) as mock_autoscaling_info_for_resource, mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.get_mesos_utilization_error",
        autospec=True,
    ) as mock_get_utilization_error:
        mock_autoscaling_info_for_resource.side_effect = (
            mock_autoscaling_info_for_resource_side_effect
        )
        mock_state = mock.Mock()
        mock_get_utilization_error.return_value = 0
        ret = autoscaling_cluster_lib.get_autoscaling_info_for_all_resources(mock_state)
        utilization_errors = autoscaling_cluster_lib.get_all_utilization_errors(
            mock_resources, {}, mock_state, mock_system_config
        )
        calls = [
            mock.call(
                resource=mock_resource_1,
                pool_settings={},
                mesos_state=mock_state,
                utilization_errors=utilization_errors,
                max_increase=mock_system_config.get_cluster_autoscaler_max_increase(),
                max_decrease=mock_system_config.get_cluster_autoscaler_max_decrease(),
            ),
            mock.call(
                resource=mock_resource_2,
                pool_settings={},
                mesos_state=mock_state,
                utilization_errors=utilization_errors,
                max_increase=mock_system_config.get_cluster_autoscaler_max_increase(),
                max_decrease=mock_system_config.get_cluster_autoscaler_max_decrease(),
            ),
        ]
        mock_autoscaling_info_for_resource.assert_has_calls(calls, any_order=True)
        assert ret == [mock_autoscaling_info]


def test_autoscaling_info_for_resources():
    mock_resources = {
        "sfr-blah": {
            "id": "sfr-blah",
            "min_capacity": 1,
            "max_capacity": 5,
            "pool": "default",
            "type": "sfr",
            "region": "westeros-1",
        }
    }

    with mock.patch(
        "paasta_tools.autoscaling.autoscaling_cluster_lib.get_scaler", autospec=True
    ) as mock_get_scaler:
        # test cancelled
        mock_metrics_provider = mock.Mock(return_value=(2, 4))
        mock_scaler = mock.Mock(
            metrics_provider=mock_metrics_provider,
            resource=mock_resources["sfr-blah"],
            is_resource_cancelled=mock.Mock(return_value=True),
            instances=["mock_instance"],
        )
        mock_scaler_class = mock.Mock(return_value=mock_scaler)
        mock_get_scaler.return_value = mock_scaler_class
        mock_state = mock.Mock()
        mock_utilization_errors = {("westeros-1", "default"): 0}
        ret = autoscaling_cluster_lib.autoscaling_info_for_resource(
            resource=mock_resources["sfr-blah"],
            pool_settings={},
            mesos_state=mock_state,
            utilization_errors=mock_utilization_errors,
            max_increase=0.2,
            max_decrease=0.1,
        )
        assert mock_metrics_provider.called
        mock_scaler_class.assert_called_with(
            resource=mock_resources["sfr-blah"],
            pool_settings={},
            config_folder=None,
            dry_run=True,
            utilization_error=0,
            max_increase=0.2,
            max_decrease=0.1,
        )
        assert ret == autoscaling_cluster_lib.AutoscalingInfo(
            resource_id="sfr-blah",
            pool="default",
            state="cancelled",
            current="2",
            target="4",
            min_capacity="1",
            max_capacity="5",
            instances="1",
        )

        # test active
        mock_scaler = mock.Mock(
            metrics_provider=mock_metrics_provider,
            resource=mock_resources["sfr-blah"],
            is_resource_cancelled=mock.Mock(return_value=False),
            instances=["mock_instance"],
        )
        mock_scaler_class = mock.Mock(return_value=mock_scaler)
        mock_get_scaler.return_value = mock_scaler_class
        ret = autoscaling_cluster_lib.autoscaling_info_for_resource(
            resource=mock_resources["sfr-blah"],
            pool_settings={},
            mesos_state=mock_state,
            utilization_errors=mock_utilization_errors,
            max_increase=0.2,
            max_decrease=0.1,
        )
        assert ret == autoscaling_cluster_lib.AutoscalingInfo(
            resource_id="sfr-blah",
            pool="default",
            state="active",
            current="2",
            target="4",
            min_capacity="1",
            max_capacity="5",
            instances="1",
        )

        # Test exception getting target
        mock_metrics_provider = mock.Mock(
            side_effect=autoscaling_cluster_lib.ClusterAutoscalingError
        )
        mock_scaler = mock.Mock(
            metrics_provider=mock_metrics_provider,
            resource=mock_resources["sfr-blah"],
            is_resource_cancelled=mock.Mock(return_value=False),
            current_capacity=2,
            instances=["mock_instance"],
        )
        mock_scaler_class = mock.Mock(return_value=mock_scaler)
        mock_get_scaler.return_value = mock_scaler_class
        ret = autoscaling_cluster_lib.autoscaling_info_for_resource(
            resource=mock_resources["sfr-blah"],
            pool_settings={},
            mesos_state=mock_state,
            utilization_errors=mock_utilization_errors,
            max_increase=0.2,
            max_decrease=0.1,
        )
        assert ret == autoscaling_cluster_lib.AutoscalingInfo(
            resource_id="sfr-blah",
            pool="default",
            state="active",
            current="2",
            target="Exception",
            min_capacity="1",
            max_capacity="5",
            instances="1",
        )


class TestAsgAutoscaler(unittest.TestCase):

    mock_resource = {
        "id": "asg-blah",
        "type": "aws_autoscaling_group",
        "region": "westeros-1",
        "pool": "default",
    }
    mock_config_folder = "/nail/blah"
    mock_pool_settings = {"drain_timeout": 123}

    def setUp(self):
        self.autoscaler = self.create_autoscaler()

    def create_autoscaler(self, utilization_error=0.3, resource=None, asg=None):
        config = SystemPaastaConfig(
            {"monitoring_config": {"check_registered_slave_threshold": 3600}},
            "/etc/paasta",
        )
        with mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.AsgAutoscaler.get_asg",
            autospec=True,
            return_value=asg or {},
        ):
            with mock.patch(
                "paasta_tools.autoscaling.autoscaling_cluster_lib.load_system_paasta_config",
                autospec=True,
                return_value=config,
            ):
                print(config.get_monitoring_config())
                autoscaler = autoscaling_cluster_lib.AsgAutoscaler(
                    resource=resource or self.mock_resource,
                    pool_settings=self.mock_pool_settings,
                    config_folder=self.mock_config_folder,
                    dry_run=False,
                    utilization_error=utilization_error,
                    max_increase=0.2,
                    max_decrease=0.1,
                )
                autoscaler.instances = []
                return autoscaler

    def create_mock_resource(self, **kwargs):
        mock_resource = self.mock_resource.copy()
        mock_resource.update(**kwargs)
        return mock_resource

    def test_exists(self):
        self.autoscaler.asg = mock.Mock()
        assert self.autoscaler.exists

        self.autoscaler.asg = None
        assert not self.autoscaler.exists

    def test_current_capacity(self):
        self.autoscaler.asg = {"Instances": [mock.Mock()] * 3}
        assert self.autoscaler.current_capacity == 3

    def test_is_asg_cancelled(self):
        self.autoscaler.asg = None
        assert self.autoscaler.is_resource_cancelled()

        self.autoscaler.asg = mock.Mock()
        assert not self.autoscaler.is_resource_cancelled()

    def test_is_new_autoscaling_resource_when_asg_is_above_threshold(self):
        asg = {
            "Instances": [mock.Mock()],
            "CreatedTime": datetime.now(timezone.utc) - timedelta(seconds=3600 + 60),
        }
        autoscaler = self.create_autoscaler(asg=asg)
        assert not autoscaler.is_new_autoscaling_resource()

    def test_is_new_autoscaling_resource_when_asg_is_below_threshold(self):
        asg = {"Instances": [mock.Mock()], "CreatedTime": datetime.now(timezone.utc)}
        autoscaler = self.create_autoscaler(asg=asg)
        assert autoscaler.is_new_autoscaling_resource()

    def test_get_asg(self):
        with mock.patch("boto3.client", autospec=True) as mock_ec2_client:
            mock_asg = mock.Mock()
            mock_asgs = {"AutoScalingGroups": [mock_asg]}
            mock_describe_auto_scaling_groups = mock.Mock(return_value=mock_asgs)
            mock_ec2_client.return_value = mock.Mock(
                describe_auto_scaling_groups=mock_describe_auto_scaling_groups
            )
            ret = self.autoscaler.get_asg("asg-blah", region="westeros-1")
            mock_describe_auto_scaling_groups.assert_called_with(
                AutoScalingGroupNames=["asg-blah"]
            )
            assert ret == mock_asg

            mock_asgs = {"AutoScalingGroups": []}
            mock_describe_auto_scaling_groups = mock.Mock(return_value=mock_asgs)
            mock_ec2_client.return_value = mock.Mock(
                describe_auto_scaling_groups=mock_describe_auto_scaling_groups
            )
            ret = self.autoscaler.get_asg("asg-blah", region="westeros-1")
            assert ret is None

    def test_set_asg_capacity(self):
        with mock.patch("boto3.client", autospec=True) as mock_ec2_client, mock.patch(
            "time.sleep", autospec=True
        ):
            mock_update_auto_scaling_group = mock.Mock()
            mock_ec2_client.return_value = mock.Mock(
                update_auto_scaling_group=mock_update_auto_scaling_group
            )
            self.autoscaler.dry_run = True
            self.autoscaler.set_capacity(2)
            assert not mock_update_auto_scaling_group.called
            self.autoscaler.dry_run = False

            self.autoscaler.set_capacity(2)
            mock_ec2_client.assert_called_with("autoscaling", region_name="westeros-1")
            mock_update_auto_scaling_group.assert_called_with(
                AutoScalingGroupName="asg-blah", DesiredCapacity=2
            )

            with raises(autoscaling_cluster_lib.FailSetResourceCapacity):
                mock_update_auto_scaling_group.side_effect = ClientError(
                    {"Error": {"Code": 1}}, "blah"
                )
                self.autoscaler.set_capacity(2)
            assert self.autoscaler.capacity == 2

    def test_get_instance_type_weights_asg(self):
        ret = self.autoscaler.get_instance_type_weights()
        assert ret is None

    def test_get_asg_delta(self):
        resource = self.create_mock_resource(min_capacity=2, max_capacity=10)
        asg = {"Instances": [mock.Mock()] * 5}

        autoscaler = self.create_autoscaler(
            utilization_error=-0.2, resource=resource, asg=asg
        )
        ret = autoscaler.get_asg_delta()
        assert ret == (5, 4)

        autoscaler = self.create_autoscaler(
            utilization_error=0.2, resource=resource, asg=asg
        )
        ret = autoscaler.get_asg_delta()
        assert ret == (5, 6)

        big_asg = {"Instances": [mock.Mock()] * 10}
        autoscaler = self.create_autoscaler(
            utilization_error=0.2, resource=resource, asg=big_asg
        )
        ret = autoscaler.get_asg_delta()
        assert ret == (10, 10)

        small_asg = {"Instances": [mock.Mock()] * 2}
        autoscaler = self.create_autoscaler(
            utilization_error=-0.2, resource=resource, asg=small_asg
        )
        ret = autoscaler.get_asg_delta()
        assert ret == (2, 2)

        resource_zero_min = self.create_mock_resource(min_capacity=0, max_capacity=10)
        autoscaler = self.create_autoscaler(
            utilization_error=-1, resource=resource_zero_min, asg=small_asg
        )
        ret = autoscaler.get_asg_delta()
        assert ret == (2, 1)

        tiny_asg = {"Instances": [mock.Mock()] * 1}
        autoscaler = self.create_autoscaler(
            utilization_error=-1, resource=resource_zero_min, asg=tiny_asg
        )
        ret = autoscaler.get_asg_delta()
        assert ret == (1, 0)

        empty_asg = {"Instances": []}
        autoscaler = self.create_autoscaler(
            utilization_error=-1, resource=resource_zero_min, asg=empty_asg
        )
        ret = autoscaler.get_asg_delta()
        assert ret == (0, 1)

        resource_big_max = self.create_mock_resource(min_capacity=0, max_capacity=100)
        bigger_asg = {"Instances": [mock.Mock()] * 21}
        autoscaler = self.create_autoscaler(
            utilization_error=-0.5, resource=resource_big_max, asg=bigger_asg
        )
        ret = autoscaler.get_asg_delta()
        assert ret == (21, int(floor(21 * (1.0 - autoscaler.max_decrease))))

        resource_big_max = self.create_mock_resource(min_capacity=0, max_capacity=100)
        bigger_asg = {"Instances": [mock.Mock()] * 21}
        autoscaler = self.create_autoscaler(
            utilization_error=0.5, resource=resource_big_max, asg=bigger_asg
        )
        ret = autoscaler.get_asg_delta()
        assert ret == (21, int(ceil(21 * (1.0 + autoscaler.max_increase))))

        resource_zeroes = self.create_mock_resource(min_capacity=0, max_capacity=0)
        autoscaler = self.create_autoscaler(
            utilization_error=-0.5, resource=resource_zeroes, asg=bigger_asg
        )
        ret = autoscaler.get_asg_delta()
        assert ret == (21, int(floor(21 * (1.0 - autoscaler.max_decrease))))

        resource = self.create_mock_resource(min_capacity=10, max_capacity=40)
        current_instances = int((10 * (1 - autoscaler.max_decrease)) - 1)
        asg = {"Instances": [mock.Mock()] * current_instances}
        autoscaler = self.create_autoscaler(
            utilization_error=-1, resource=resource, asg=asg
        )
        ret = autoscaler.get_asg_delta()
        assert ret == (current_instances, 10)

    def test_asg_metrics_provider(self):
        with mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.AsgAutoscaler.get_asg_delta",
            autospec=True,
        ) as mock_get_asg_delta, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.AsgAutoscaler.get_aws_slaves",
            autospec=True,
        ) as mock_get_aws_slaves, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.get_mesos_master",
            autospec=True,
        ) as mock_get_mesos_master, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.AsgAutoscaler.cleanup_cancelled_config",
            autospec=True,
        ) as mock_cleanup_cancelled_config, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.AsgAutoscaler.is_aws_launching_instances",
            autospec=True,
        ) as mock_is_aws_launching_asg_instances, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.emit_metrics",
            autospec=True,
        ) as mock_emit_metrics, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.log",
            autospec=True,
        ):
            mock_get_asg_delta.return_value = 1, 2
            self.autoscaler.pool_settings = {}
            mock_is_aws_launching_asg_instances.return_value = False
            mock_mesos_state = mock.Mock()
            mock_master = mock.Mock(state=mock_mesos_state)
            mock_get_mesos_master.return_value = mock_master

            mock_slaves = ["one", "two"]
            mock_get_aws_slaves.return_value = mock_slaves

            # cancelled ASG
            self.autoscaler.asg = None
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            mock_cleanup_cancelled_config.assert_called_with(
                self.autoscaler, "asg-blah", "/nail/blah", dry_run=False
            )
            assert not mock_get_aws_slaves.called
            assert ret == (0, 0)

            # active ASG
            self.autoscaler.asg = {"some": "stuff"}
            mock_cleanup_cancelled_config.reset_mock()
            mock_emit_metrics.reset_mock()
            self.autoscaler.instances = [mock.Mock(), mock.Mock()]
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            mock_get_asg_delta.assert_called_with(self.autoscaler)
            mock_emit_metrics.assert_called_once_with(
                self.autoscaler, 1, 2, mesos_slave_count=len(mock_slaves)
            )
            assert not mock_cleanup_cancelled_config.called
            assert ret == (1, 2)

            # active ASG with AWS still provisioning
            mock_cleanup_cancelled_config.reset_mock()
            mock_is_aws_launching_asg_instances.return_value = True
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            assert ret == (0, 0)

            # ASG with no instances
            self.autoscaler.instances = []
            mock_is_aws_launching_asg_instances.return_value = False
            self.autoscaler.metrics_provider(mock_mesos_state)
            mock_get_asg_delta.assert_called_with(self.autoscaler)

            # ASG scaling up with too many unregistered instances
            self.autoscaler.log.reset_mock()
            self.autoscaler.instances = [mock.Mock() for _ in range(10)]
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            assert ret == (1, 2)
            assert self.autoscaler.log.warn.call_count == 1

            # ASG scaling down with many unregistered instances
            mock_emit_metrics.reset_mock()
            self.autoscaler.log.reset_mock()
            self.autoscaler.utilization_error = -0.1
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            mock_emit_metrics.assert_called_once_with(
                self.autoscaler, 1, 2, mesos_slave_count=len(mock_slaves)
            )
            assert ret == (1, 2)
            assert self.autoscaler.log.warn.call_count == 1

    def test_is_aws_launching_asg_instances(self):
        self.autoscaler.asg = {
            "DesiredCapacity": 3,
            "Instances": [mock.Mock(), mock.Mock()],
        }
        assert self.autoscaler.is_aws_launching_instances()

        self.autoscaler.asg = {
            "DesiredCapacity": 1,
            "Instances": [mock.Mock(), mock.Mock()],
        }
        assert not self.autoscaler.is_aws_launching_instances()

        self.autoscaler.asg = {
            "DesiredCapacity": 2,
            "Instances": [mock.Mock(), mock.Mock()],
        }
        assert not self.autoscaler.is_aws_launching_instances()


class TestSpotAutoscaler(unittest.TestCase):

    mock_resource = {
        "id": "sfr-blah",
        "type": "sfr",
        "region": "westeros-1",
        "pool": "default",
    }
    mock_pool_settings = {"drain_timeout": 123}
    mock_config_folder = "/nail/blah"

    def setUp(self):
        self.autoscaler = self.create_autoscaler()

    def create_autoscaler(self, utilization_error=0.3, resource=None, sfr=None):
        with mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.get_sfr",
            autospec=True,
        ) as mock_get_sfr, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.get_spot_fleet_instances",
            autospec=True,
        ) as mock_get_spot_fleet_instances:
            mock_get_sfr.return_value = sfr or {}
            mock_get_spot_fleet_instances.return_value = []
            with mock.patch(
                "paasta_tools.autoscaling.autoscaling_cluster_lib.load_system_paasta_config",
                autospec=True,
                return_value=SystemPaastaConfig(
                    {"monitoring_config": {"check_registered_slave_threshold": 3600}},
                    "/etc/paasta",
                ),
            ):

                return autoscaling_cluster_lib.SpotAutoscaler(
                    resource=resource or self.mock_resource,
                    pool_settings=self.mock_pool_settings,
                    config_folder=self.mock_config_folder,
                    dry_run=False,
                    utilization_error=utilization_error,
                    max_increase=0.2,
                    max_decrease=0.1,
                )

    def create_mock_resource(self, **kwargs):
        mock_resource = self.mock_resource.copy()
        mock_resource.update(**kwargs)
        return mock_resource

    def create_mock_sfr(self, fulfilled_capacity, request_state="active"):
        return {
            "SpotFleetRequestState": request_state,
            "SpotFleetRequestConfig": {"FulfilledCapacity": fulfilled_capacity},
        }

    def test_exists(self):
        self.autoscaler.sfr = {"SpotFleetRequestState": "active"}
        assert self.autoscaler.exists

        self.autoscaler.sfr = {"SpotFleetRequestState": "cancelled"}
        assert not self.autoscaler.exists

        self.autoscaler.sfr = None
        assert not self.autoscaler.exists

    def test_current_capacity(self):
        self.autoscaler.sfr = {"SpotFleetRequestConfig": {"FulfilledCapacity": 2}}
        assert self.autoscaler.current_capacity == 2

    def test_get_spot_fleet_instances(self):
        with mock.patch("boto3.client", autospec=True) as mock_ec2_client:
            mock_instances = mock.Mock()
            mock_sfr = {"ActiveInstances": mock_instances}
            mock_describe_spot_fleet_instances = mock.Mock(return_value=mock_sfr)
            mock_ec2_client.return_value = mock.Mock(
                describe_spot_fleet_instances=mock_describe_spot_fleet_instances
            )
            ret = self.autoscaler.get_spot_fleet_instances(
                "sfr-blah", region="westeros-1"
            )
            assert ret == mock_instances

    def test_is_aws_launching_sfr_instances(self):
        self.autoscaler.sfr = {
            "SpotFleetRequestConfig": {"FulfilledCapacity": 5, "TargetCapacity": 10}
        }
        assert self.autoscaler.is_aws_launching_instances()

        self.autoscaler.sfr = {
            "SpotFleetRequestConfig": {"FulfilledCapacity": 10, "TargetCapacity": 5}
        }
        assert not self.autoscaler.is_aws_launching_instances()

        self.autoscaler.sfr = {
            "SpotFleetRequestConfig": {"FulfilledCapacity": 10, "TargetCapacity": 10}
        }
        assert not self.autoscaler.is_aws_launching_instances()

    def test_is_sfr_cancelled(self):
        self.autoscaler.sfr = {"SpotFleetRequestState": "cancelled"}
        assert self.autoscaler.is_resource_cancelled()

        self.autoscaler.sfr = {"SpotFleetRequestState": "cancelled_running"}
        assert self.autoscaler.is_resource_cancelled()

        self.autoscaler.sfr = {"SpotFleetRequestState": "active"}
        assert not self.autoscaler.is_resource_cancelled()

        self.autoscaler.sfr = None
        assert self.autoscaler.is_resource_cancelled()

    def test_cancelled_running_scale_down(self):
        sfr = self.create_mock_sfr(
            fulfilled_capacity=4, request_state="cancelled_running"
        )
        resource = self.create_mock_resource(min_capacity=4, max_capacity=10)
        autoscaler = self.create_autoscaler(
            utilization_error=-0.1, resource=resource, sfr=sfr
        )

        assert autoscaler.utilization_error == -1
        assert autoscaler.resource["min_capacity"] == 0

    def test_cancelled_running_scale_up(self):
        sfr = self.create_mock_sfr(
            fulfilled_capacity=4, request_state="cancelled_running"
        )
        resource = self.create_mock_resource(min_capacity=4, max_capacity=10)
        autoscaler = self.create_autoscaler(
            utilization_error=0.1, resource=resource, sfr=sfr
        )

        assert autoscaler.utilization_error == 0.1
        assert autoscaler.resource["min_capacity"] == 4

    def test_get_sfr(self):
        with mock.patch("boto3.client", autospec=True) as mock_ec2_client:
            mock_sfr_config = mock.Mock()
            mock_sfr = {"SpotFleetRequestConfigs": [mock_sfr_config]}
            mock_describe_spot_fleet_requests = mock.Mock(return_value=mock_sfr)
            mock_ec2_client.return_value = mock.Mock(
                describe_spot_fleet_requests=mock_describe_spot_fleet_requests
            )
            ret = self.autoscaler.get_sfr("sfr-blah", region="westeros-1")
            mock_describe_spot_fleet_requests.assert_called_with(
                SpotFleetRequestIds=["sfr-blah"]
            )
            assert ret == mock_sfr_config

            mock_error = {"Error": {"Code": "InvalidSpotFleetRequestId.NotFound"}}
            mock_describe_spot_fleet_requests = mock.Mock(
                side_effect=ClientError(mock_error, "blah")
            )
            mock_ec2_client.return_value = mock.Mock(
                describe_spot_fleet_requests=mock_describe_spot_fleet_requests
            )
            ret = self.autoscaler.get_sfr("sfr-blah", region="westeros-1")
            assert ret is None

    def test_is_new_autoscaling_resource_when_sfr_is_above_threshold(self):
        sfr = {
            "SpotFleetRequestConfig": {"FulfilledCapacity": 2},
            "SpotFleetRequestState": "active",
            "Instances": [mock.Mock()],
            "CreateTime": datetime.now(timezone.utc) - timedelta(seconds=3600 + 60),
        }
        autoscaler = self.create_autoscaler(sfr=sfr)
        assert not autoscaler.is_new_autoscaling_resource()

    def test_is_new_autoscaling_resource_when_sfr_is_below_threshold(self):
        sfr = {
            "SpotFleetRequestConfig": {"FulfilledCapacity": 2},
            "SpotFleetRequestState": "active",
            "Instances": [mock.Mock()],
            "CreateTime": datetime.now(timezone.utc),
        }
        autoscaler = self.create_autoscaler(sfr=sfr)
        assert autoscaler.is_new_autoscaling_resource()

    def test_set_spot_fleet_request_capacity(self):
        with mock.patch("boto3.client", autospec=True) as mock_ec2_client, mock.patch(
            "time.sleep", autospec=True
        ) as mock_sleep, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.get_sfr",
            autospec=True,
        ) as mock_get_sfr, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.AWS_SPOT_MODIFY_TIMEOUT",
            autospec=True,
        ):
            mock_sleep.side_effect = TimeoutError()
            mock_get_sfr.return_value = {"SpotFleetRequestState": "modifying"}
            mock_modify_spot_fleet_request = mock.Mock()
            mock_ec2_client.return_value = mock.Mock(
                modify_spot_fleet_request=mock_modify_spot_fleet_request
            )
            with raises(autoscaling_cluster_lib.FailSetResourceCapacity):
                ret = self.autoscaler.set_capacity(4.1)
            assert not mock_modify_spot_fleet_request.called

            mock_modify_spot_fleet_request.side_effect = ClientError(
                {"Error": {}}, "blah"
            )
            mock_get_sfr.return_value = {"SpotFleetRequestState": "active"}
            with raises(autoscaling_cluster_lib.FailSetResourceCapacity):
                ret = self.autoscaler.set_capacity(4.1)

            mock_modify_spot_fleet_request.side_effect = None
            ret = self.autoscaler.set_capacity(4.1)
            mock_modify_spot_fleet_request.assert_called_with(
                SpotFleetRequestId="sfr-blah",
                TargetCapacity=4,
                ExcessCapacityTerminationPolicy="noTermination",
            )
            assert ret is not None
            assert self.autoscaler.capacity == 4.1

    def test_get_instance_type_weights_sfr(self):
        mock_launch_specs = [
            {"InstanceType": "c4.blah", "WeightedCapacity": 123},
            {"InstanceType": "m4.whatever", "WeightedCapacity": 456},
        ]
        self.autoscaler.sfr = {
            "SpotFleetRequestConfig": {"LaunchSpecifications": mock_launch_specs}
        }
        ret = self.autoscaler.get_instance_type_weights()
        assert ret == {"c4.blah": 123, "m4.whatever": 456}

    def test_get_spot_fleet_delta(self):
        resource = self.create_mock_resource(min_capacity=2, max_capacity=10)

        sfr = self.create_mock_sfr(fulfilled_capacity=5)
        autoscaler = self.create_autoscaler(
            utilization_error=-0.2, resource=resource, sfr=sfr
        )
        ret = autoscaler.get_spot_fleet_delta()
        assert ret == (5, 4)

        sfr = self.create_mock_sfr(fulfilled_capacity=7.3)
        autoscaler = self.create_autoscaler(
            utilization_error=-0.2, resource=resource, sfr=sfr
        )
        ret = autoscaler.get_spot_fleet_delta()
        assert ret == (7.3, 6)

        sfr = self.create_mock_sfr(fulfilled_capacity=5)
        autoscaler = self.create_autoscaler(
            utilization_error=0.2, resource=resource, sfr=sfr
        )
        ret = autoscaler.get_spot_fleet_delta()
        assert ret == (5, 6)

        sfr = self.create_mock_sfr(fulfilled_capacity=10)
        autoscaler = self.create_autoscaler(
            utilization_error=0.2, resource=resource, sfr=sfr
        )
        ret = autoscaler.get_spot_fleet_delta()
        assert ret == (10, 10)

        sfr = self.create_mock_sfr(fulfilled_capacity=2)
        autoscaler = self.create_autoscaler(
            utilization_error=-0.2, resource=resource, sfr=sfr
        )
        ret = autoscaler.get_spot_fleet_delta()
        assert ret == (2, 2)

        resource = self.create_mock_resource(min_capacity=0, max_capacity=10)
        sfr = self.create_mock_sfr(fulfilled_capacity=1)
        autoscaler = self.create_autoscaler(
            utilization_error=-1, resource=resource, sfr=sfr
        )
        ret = autoscaler.get_spot_fleet_delta()
        assert ret == (1, 1)

        resource = self.create_mock_resource(min_capacity=0, max_capacity=100)
        sfr = self.create_mock_sfr(fulfilled_capacity=21)
        autoscaler = self.create_autoscaler(
            utilization_error=0.5, resource=resource, sfr=sfr
        )
        ret = autoscaler.get_spot_fleet_delta()
        assert ret == (21, int(ceil(21 * (1.0 + autoscaler.max_increase))))

        resource = self.create_mock_resource(min_capacity=0, max_capacity=100)
        sfr = self.create_mock_sfr(fulfilled_capacity=21)
        autoscaler = self.create_autoscaler(
            utilization_error=-0.5, resource=resource, sfr=sfr
        )
        ret = autoscaler.get_spot_fleet_delta()
        assert ret == (21, int(floor(21 * (1.0 - autoscaler.max_decrease))))

        resource = self.create_mock_resource(min_capacity=10, max_capacity=100)
        current_instances = (10 * (1 - autoscaler.max_decrease)) - 1
        sfr = self.create_mock_sfr(fulfilled_capacity=current_instances)
        autoscaler = self.create_autoscaler(
            utilization_error=-1, resource=resource, sfr=sfr
        )
        ret = autoscaler.get_spot_fleet_delta()
        assert ret == (current_instances, 10)

        resource = self.create_mock_resource(min_capacity=1, max_capacity=10)
        sfr = self.create_mock_sfr(
            fulfilled_capacity=1, request_state="cancelled_running"
        )
        autoscaler = self.create_autoscaler(
            utilization_error=-1, resource=resource, sfr=sfr
        )
        ret = autoscaler.get_spot_fleet_delta()
        assert ret == (1, 0)

    def test_spotfleet_metrics_provider(self):
        with mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.get_spot_fleet_delta",
            autospec=True,
        ) as mock_get_spot_fleet_delta, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.get_aws_slaves",
            autospec=True,
        ) as mock_get_aws_slaves, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.get_mesos_master",
            autospec=True,
        ) as mock_get_mesos_master, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.cleanup_cancelled_config",
            autospec=True,
        ) as mock_cleanup_cancelled_config, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.SpotAutoscaler.is_aws_launching_instances",
            autospec=True,
        ) as mock_is_aws_launching_sfr_instances, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.emit_metrics",
            autospec=True,
        ) as mock_emit_metrics, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.log",
            autospec=True,
        ):
            mock_get_spot_fleet_delta.return_value = 1, 2
            self.autoscaler.pool_settings = {}
            mock_is_aws_launching_sfr_instances.return_value = False
            mock_mesos_state = mock.Mock()
            mock_master = mock.Mock(state=mock_mesos_state)
            mock_get_mesos_master.return_value = mock_master

            mock_slaves = ["one", "two"]
            mock_get_aws_slaves.return_value = mock_slaves

            # cancelled SFR
            self.autoscaler.instances = [mock.Mock(), mock.Mock()]
            self.autoscaler.sfr = {"SpotFleetRequestState": "cancelled"}
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            mock_cleanup_cancelled_config.assert_called_with(
                self.autoscaler, "sfr-blah", "/nail/blah", dry_run=False
            )
            assert not mock_get_mesos_master.called
            assert ret == (0, 0)

            # deleted SFR
            mock_cleanup_cancelled_config.reset_mock()
            self.autoscaler.sfr = None
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            mock_cleanup_cancelled_config.assert_called_with(
                self.autoscaler, "sfr-blah", "/nail/blah", dry_run=False
            )
            assert not mock_get_mesos_master.called
            assert ret == (0, 0)

            # active SFR
            mock_cleanup_cancelled_config.reset_mock()
            mock_emit_metrics.reset_mock()
            self.autoscaler.sfr = {"SpotFleetRequestState": "active"}
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            mock_get_spot_fleet_delta.assert_called_with(self.autoscaler)
            mock_emit_metrics.assert_called_once_with(
                self.autoscaler, 1, 2, mesos_slave_count=len(mock_slaves)
            )
            assert not mock_cleanup_cancelled_config.called
            assert ret == (1, 2)

            # SFR scaling up with too many unregistered instances
            self.autoscaler.log.reset_mock()
            mock_emit_metrics.reset_mock()
            self.autoscaler.instances = [mock.Mock() for _ in range(10)]
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            assert ret == (1, 2)
            assert self.autoscaler.log.warn.call_count == 1

            # SFR scaling down with many unregistered instances
            self.autoscaler.log.reset_mock()
            mock_emit_metrics.reset_mock()
            self.autoscaler.utilization_error = -0.1
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            mock_emit_metrics.assert_called_once_with(
                self.autoscaler, 1, 2, mesos_slave_count=len(mock_slaves)
            )
            assert ret == (1, 2)
            assert self.autoscaler.log.warn.call_count == 1

            self.autoscaler.instances = [mock.Mock(), mock.Mock()]
            self.autoscaler.utilization_error = 0.3

            # active SFR with AWS still provisioning
            mock_emit_metrics.reset_mock()
            mock_get_spot_fleet_delta.reset_mock()
            mock_cleanup_cancelled_config.reset_mock()
            self.autoscaler.sfr = {"SpotFleetRequestState": "active"}
            mock_is_aws_launching_sfr_instances.return_value = True
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            mock_emit_metrics.assert_called_once_with(
                self.autoscaler, 1, 2, mesos_slave_count=len(mock_slaves)
            )
            assert ret == (1, 2)
            assert mock_get_spot_fleet_delta.called

            # cancelled_running SFR trying to scale up
            mock_emit_metrics.reset_mock()
            mock_cleanup_cancelled_config.reset_mock()
            mock_get_spot_fleet_delta.reset_mock()
            self.autoscaler.sfr = {"SpotFleetRequestState": "cancelled_running"}
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            assert not mock_cleanup_cancelled_config.called
            assert ret == (0, 0)

            # cancelled_running SFR scaling down
            mock_get_spot_fleet_delta.return_value = 2, 1
            self.autoscaler.utilization_error = -0.2
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            mock_emit_metrics.assert_called_once_with(
                self.autoscaler, 2, 1, mesos_slave_count=len(mock_slaves)
            )
            assert ret == (2, 1)
            mock_get_spot_fleet_delta.return_value = 4, 2
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            assert ret == (4, 2)

            # cancelled_running SFR with pool underprovisioned
            self.autoscaler.utilization_error = 0.2
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            assert ret == (0, 0)

            # cancelled_running SFR with no instances
            mock_cleanup_cancelled_config.reset_mock()
            self.autoscaler.instances = []
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            assert ret == (0, 0)
            mock_cleanup_cancelled_config.assert_called_with(
                self.autoscaler, "sfr-blah", "/nail/blah", dry_run=False
            )

            # SFR with no instances
            mock_get_mesos_master.reset_mock()
            self.autoscaler.sfr = {"SpotFleetRequestState": "active"}
            self.autoscaler.instances = []
            ret = self.autoscaler.metrics_provider(mock_mesos_state)
            assert ret == (0, 0)
            assert not mock_get_mesos_master.called

            # unknown SFR
            mock_get_mesos_master.reset_mock()
            mock_cleanup_cancelled_config.reset_mock()
            self.autoscaler.sfr = {"SpotFleetRequestState": "not-a-state"}
            with raises(autoscaling_cluster_lib.ClusterAutoscalingError):
                ret = self.autoscaler.metrics_provider(mock_mesos_state)
            assert not mock_get_mesos_master.called


class TestClusterAutoscaler(unittest.TestCase):
    def setUp(self):
        mock_resource = {
            "id": "sfr-blah",
            "type": "sfr",
            "region": "westeros-1",
            "pool": "default",
            "min_capacity": 3,
            "max_capacity": 10,
        }
        mock_pool_settings = {"drain_timeout": 123}
        mock_config_folder = "/nail/blah"
        mock_utilization_error = 0.3

        with mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.get_metrics_interface",
            autospec=True,
        ), mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.load_system_paasta_config",
            autospec=True,
        ):
            self.autoscaler = autoscaling_cluster_lib.ClusterAutoscaler(
                resource=mock_resource,
                pool_settings=mock_pool_settings,
                config_folder=mock_config_folder,
                dry_run=False,
                utilization_error=mock_utilization_error,
                max_increase=0.2,
                max_decrease=0.1,
                enable_metrics=True,
            )

    def test_emit_metrics(self):
        patchers = [
            mock.patch.object(self.autoscaler, gauge)
            for gauge in (
                "target_gauge",
                "current_gauge",
                "ideal_gauge",
                "max_gauge",
                "min_gauge",
                "mesos_error_gauge",
                "aws_instances_gauge",
                "mesos_slaves_gauge",
            )
        ]
        with contextlib.ExitStack() as stack:
            for patcher in patchers:
                stack.enter_context(patcher)

            self.autoscaler.ideal_capacity = 4
            self.autoscaler.emit_metrics(
                current_capacity=1, target_capacity=2, mesos_slave_count=5
            )

            self.autoscaler.current_gauge.set.assert_called_once_with(1)
            self.autoscaler.target_gauge.set.assert_called_once_with(2)
            self.autoscaler.ideal_gauge.set.assert_called_once_with(4)
            self.autoscaler.min_gauge.set.assert_called_once_with(3)
            self.autoscaler.max_gauge.set.assert_called_once_with(10)
            self.autoscaler.mesos_error_gauge.set.assert_called_once_with(0.3)
            self.autoscaler.aws_instances_gauge.set.assert_called_once_with(0)
            self.autoscaler.mesos_slaves_gauge.set.assert_called_once_with(5)

    def test_is_new_autoscaling_resource(self):
        self.assertRaises(
            NotImplementedError, self.autoscaler.is_new_autoscaling_resource
        )

    def test_describe_instance(self):
        with mock.patch("boto3.client", autospec=True) as mock_ec2_client:
            mock_instance_1 = mock.Mock()
            mock_instance_2 = mock.Mock()
            mock_instance_3 = mock.Mock()
            mock_instances = {
                "Reservations": [
                    {"Instances": [mock_instance_1]},
                    {"Instances": [mock_instance_2]},
                ]
            }
            mock_describe_instances = mock.Mock(return_value=mock_instances)
            mock_ec2_client.return_value = mock.Mock(
                describe_instances=mock_describe_instances
            )
            ret = self.autoscaler.describe_instances(
                ["i-1", "i-2"], region="westeros-1", instance_filters=["filter1"]
            )
            mock_describe_instances.assert_called_with(
                InstanceIds=["i-1", "i-2"], Filters=["filter1"]
            )
            assert ret == [mock_instance_1, mock_instance_2]

            ret = self.autoscaler.describe_instances(
                ["i-1", "i-2"], region="westeros-1"
            )
            mock_describe_instances.assert_called_with(
                InstanceIds=["i-1", "i-2"], Filters=[]
            )

            mock_error = {"Error": {"Code": "InvalidInstanceID.NotFound"}}
            mock_describe_instances.side_effect = ClientError(mock_error, "blah")
            ret = self.autoscaler.describe_instances(
                ["i-1", "i-2"], region="westeros-1"
            )
            assert ret is None

            mock_instances = {
                "Reservations": [
                    {"Instances": [mock_instance_1, mock_instance_2]},
                    {"Instances": [mock_instance_3]},
                ]
            }
            mock_describe_instances = mock.Mock(return_value=mock_instances)
            mock_ec2_client.return_value = mock.Mock(
                describe_instances=mock_describe_instances
            )
            ret = self.autoscaler.describe_instances(
                ["i-1", "i-2", "i-3"], region="westeros-1"
            )
            assert ret == [mock_instance_1, mock_instance_2, mock_instance_3]

    def test_scale_resource(self):
        with mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.filter_aws_slaves",
            autospec=True,
        ) as mock_filter_aws_slaves, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.get_mesos_master",
            autospec=True,
        ) as mock_get_mesos_master, asynctest.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.get_mesos_task_count_by_slave",
            autospec=True,
        ) as mock_get_mesos_task_count_by_slave, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.downscale_aws_resource",
            autospec=True,
        ) as mock_downscale_aws_resource, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.set_capacity",
            autospec=True,
        ) as mock_set_capacity, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.terminate_instances",
            autospec=True,
        ) as mock_terminate_instances:
            mock_set_capacity.return_value = True
            mock_master = mock.Mock()
            mock_mesos_state = mock.Mock()
            mock_master.state_summary = asynctest.CoroutineMock(
                return_value=mock_mesos_state,
                func=asynctest.CoroutineMock(),  # https://github.com/notion/a_sync/pull/40
            )
            mock_get_mesos_master.return_value = mock_master
            mock_downscale_aws_resource.side_effect = just_sleep

            # test no scale
            _run(self.autoscaler.scale_resource(4, 4))
            assert not mock_set_capacity.called

            # test scale up
            _run(self.autoscaler.scale_resource(2, 4))
            mock_set_capacity.assert_called_with(self.autoscaler, 4)

            # test scale down
            mock_slave_1 = mock.Mock(instance_weight=1.099999999)
            mock_slave_2 = mock.Mock(instance_weight=2.2)
            mock_sfr_sorted_slaves_1 = [mock_slave_1, mock_slave_2]
            mock_filter_aws_slaves.return_value = mock_sfr_sorted_slaves_1
            _run(self.autoscaler.scale_resource(3.3, 0))
            assert mock_get_mesos_master.called
            mock_get_mesos_task_count_by_slave.assert_called_with(
                mock_mesos_state, pool="default"
            )
            mock_filter_aws_slaves.assert_called_with(
                self.autoscaler, mock_get_mesos_task_count_by_slave.return_value
            )
            mock_downscale_aws_resource.assert_called_with(
                self.autoscaler,
                filtered_slaves=mock_filter_aws_slaves.return_value,
                current_capacity=3.3,
                target_capacity=0,
            )
            mock_set_capacity.reset_mock()

            # test scale down when not all slaves have joined cluster
            mock_slave_1 = mock.Mock(instance_weight=0.7, instance_id="abc")
            mock_slave_2 = mock.Mock(instance_weight=1.1, instance_id="def")
            mock_filter_aws_slaves.return_value = [mock_slave_1, mock_slave_2]
            self.autoscaler.instances = [
                {"InstanceId": "abc"},
                {"InstanceId": "def"},
                {"InstanceId": "ghi"},
            ]
            _run(self.autoscaler.scale_resource(3.5, 1.8))
            mock_set_capacity.assert_called_once_with(self.autoscaler, 1.8)
            mock_terminate_instances.assert_called_once_with(self.autoscaler, ["ghi"])

    def test_downscale_aws_resource(self):
        with asynctest.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.get_mesos_task_count_by_slave",
            autospec=True,
        ) as mock_get_mesos_task_count_by_slave, mock.patch(
            "paasta_tools.autoscaling.ec2_fitness.sort_by_ec2_fitness", autospec=True
        ) as mock_sort_slaves_to_kill, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.gracefully_terminate_slave",
            autospec=True,
        ) as mock_gracefully_terminate_slave, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.Timer", autospec=True
        ) as mock_timer:
            mock_timer_value = mock.Mock()
            mock_timer.return_value = mock_timer_value

            mock_gracefully_terminate_slave.side_effect = just_sleep
            mock_task_counts = mock.Mock()
            mock_slave_1 = mock.Mock(
                hostname="host1",
                instance_id="i-blah123",
                task_counts=mock_task_counts,
                instance_weight=1,
            )
            mock_slave_2 = mock.Mock(
                hostname="host2",
                instance_id="i-blah456",
                task_counts=mock_task_counts,
                instance_weight=2,
            )
            mock_get_mesos_task_count_by_slave.return_value = [
                {"task_counts": mock_task_counts}
            ]
            self.autoscaler.resource = {
                "type": "aws_spot_fleet_request",
                "sfr": {"SpotFleetRequestState": "active"},
            }
            self.autoscaler.sfr = {"SpotFleetRequestState": "active"}
            mock_filtered_slaves = mock.Mock()
            mock_sfr_sorted_slaves_1 = [mock_slave_1, mock_slave_2]
            mock_sfr_sorted_slaves_2 = [mock_slave_2]

            # test we kill only one instance on scale down and then reach capacity
            mock_sort_slaves_to_kill.return_value = mock_sfr_sorted_slaves_2
            _run(
                self.autoscaler.downscale_aws_resource(
                    filtered_slaves=mock_filtered_slaves,
                    current_capacity=5,
                    target_capacity=4,
                )
            )
            assert mock_gracefully_terminate_slave.call_count == 1

            mock_gracefully_terminate_slave.reset_mock()
            # test we always kill one SFR instance at least to stop getting wedged
            mock_slave_1 = mock.Mock(
                hostname="host1",
                instance_id="i-blah123",
                task_counts=mock_task_counts,
                instance_weight=0.3,
            )
            mock_slave_2 = mock.Mock(
                hostname="host2",
                instance_id="i-blah456",
                task_counts=mock_task_counts,
                instance_weight=2,
            )
            mock_sort_slaves_to_kill.return_value = mock_sfr_sorted_slaves_2
            _run(
                self.autoscaler.downscale_aws_resource(
                    filtered_slaves=mock_filtered_slaves,
                    current_capacity=5,
                    target_capacity=4,
                )
            )
            assert mock_gracefully_terminate_slave.call_count == 1

            mock_gracefully_terminate_slave.reset_mock()
            # but not if it takes us to setting 0 capacity
            mock_slave_1 = mock.Mock(
                hostname="host1",
                instance_id="i-blah123",
                task_counts=mock_task_counts,
                instance_weight=1.1,
            )
            mock_slave_2 = mock.Mock(
                hostname="host2",
                instance_id="i-blah456",
                task_counts=mock_task_counts,
                instance_weight=2,
            )
            mock_sort_slaves_to_kill.return_value = mock_sfr_sorted_slaves_2
            _run(
                self.autoscaler.downscale_aws_resource(
                    filtered_slaves=mock_filtered_slaves,
                    current_capacity=2,
                    target_capacity=1,
                )
            )
            assert not mock_gracefully_terminate_slave.called

            mock_gracefully_terminate_slave.reset_mock()
            # unless this is a cancelled SFR in which case we can go to 0
            self.autoscaler.sfr = {"SpotFleetRequestState": "cancelled"}
            _run(
                self.autoscaler.downscale_aws_resource(
                    filtered_slaves=mock_filtered_slaves,
                    current_capacity=2,
                    target_capacity=1,
                )
            )
            assert mock_gracefully_terminate_slave.call_count == 1

            mock_gracefully_terminate_slave.reset_mock()
            # test stop if FailSetSpotCapacity
            mock_slave_1 = mock.Mock(
                hostname="host1",
                instance_id="i-blah123",
                task_counts=mock_task_counts,
                instance_weight=1,
            )
            mock_terminate_call_1 = mock.call(
                self.autoscaler,
                slave_to_kill=mock_slave_1,
                capacity_diff=-1,
                timer=mock_timer_value,
            )
            mock_terminate_call_2 = mock.call(
                self.autoscaler,
                slave_to_kill=mock_slave_2,
                capacity_diff=-2,
                timer=mock_timer_value,
            )
            # for draining slave 1 failure HTTPError scenario
            mock_terminate_call_3 = mock.call(
                self.autoscaler,
                slave_to_kill=mock_slave_2,
                capacity_diff=-2,
                timer=mock_timer_value,
            )

            mock_gracefully_terminate_slave.side_effect = get_coro_with_exception(
                autoscaling_cluster_lib.FailSetResourceCapacity
            )
            mock_sfr_sorted_slaves_1 = [mock_slave_2, mock_slave_1]
            mock_sfr_sorted_slaves_2 = [mock_slave_2]
            mock_sort_slaves_to_kill.side_effect = [
                mock_sfr_sorted_slaves_1,
                mock_sfr_sorted_slaves_2,
                [],
            ]
            _run(
                self.autoscaler.downscale_aws_resource(
                    filtered_slaves=mock_filtered_slaves,
                    current_capacity=5,
                    target_capacity=2,
                )
            )
            mock_gracefully_terminate_slave.assert_has_calls([mock_terminate_call_1])

            # test continue if HTTPError
            mock_gracefully_terminate_slave.reset_mock()
            mock_gracefully_terminate_slave.side_effect = get_coro_with_exception(
                HTTPError
            )
            mock_sfr_sorted_slaves_1 = [mock_slave_2, mock_slave_1]
            mock_sfr_sorted_slaves_2 = [mock_slave_2]
            mock_sort_slaves_to_kill.side_effect = [
                mock_sfr_sorted_slaves_1,
                mock_sfr_sorted_slaves_2,
                [],
            ]
            _run(
                self.autoscaler.downscale_aws_resource(
                    filtered_slaves=mock_filtered_slaves,
                    current_capacity=5,
                    target_capacity=2,
                )
            )
            mock_gracefully_terminate_slave.assert_has_calls(
                [mock_terminate_call_1, mock_terminate_call_3]
            )

            # test normal scale down
            mock_gracefully_terminate_slave.side_effect = just_sleep
            mock_gracefully_terminate_slave.reset_mock()
            mock_get_mesos_task_count_by_slave.reset_mock()
            mock_sort_slaves_to_kill.reset_mock()
            mock_sfr_sorted_slaves_1 = [mock_slave_2, mock_slave_1]
            mock_sfr_sorted_slaves_2 = [mock_slave_2]
            mock_sort_slaves_to_kill.side_effect = [
                mock_sfr_sorted_slaves_1,
                mock_sfr_sorted_slaves_2,
                [],
            ]
            _run(
                self.autoscaler.downscale_aws_resource(
                    filtered_slaves=mock_filtered_slaves,
                    current_capacity=5,
                    target_capacity=2,
                )
            )
            mock_gracefully_terminate_slave.assert_has_calls(
                [mock_terminate_call_1, mock_terminate_call_2]
            )

            # test non integer scale down
            # this should result in killing 3 instances,
            # leaving us on 7.1 provisioned of target 7
            mock_slave_1 = mock.Mock(
                hostname="host1", instance_id="i-blah123", instance_weight=0.3
            )
            mock_gracefully_terminate_slave.side_effect = just_sleep
            mock_gracefully_terminate_slave.reset_mock()
            mock_get_mesos_task_count_by_slave.reset_mock()
            mock_sort_slaves_to_kill.reset_mock()
            mock_sfr_sorted_slaves = [mock_slave_1] * 10
            mock_sort_slaves_to_kill.side_effect = [mock_sfr_sorted_slaves] + [
                mock_sfr_sorted_slaves[x:-1] for x in range(0, 10)
            ]
            mock_get_mesos_task_count_by_slave.return_value = [
                {"task_counts": mock_slave_1} for x in range(0, 9)
            ]
            _run(
                self.autoscaler.downscale_aws_resource(
                    filtered_slaves=mock_filtered_slaves,
                    current_capacity=8,
                    target_capacity=7,
                )
            )
            assert mock_gracefully_terminate_slave.call_count == 3

    def test_filter_instance_description_for_ip(self):
        fake_description = [
            {"PrivateIpAddress": "10.1.1.1"},
            {"PrivateIpAddress": "10.1.1.2"},
        ]
        actual = self.autoscaler.filter_instance_description_for_ip(
            "10.1.1.1", fake_description
        )
        assert actual == [fake_description[0]]

    def test_filter_instance_status_for_instance_id(self):
        fake_status = {
            "InstanceStatuses": [{"InstanceId": "foo"}, {"InstanceId": "bar"}]
        }
        actual = self.autoscaler.filter_instance_status_for_instance_id(
            instance_id="foo", instance_statuses=fake_status
        )
        assert actual == [fake_status["InstanceStatuses"][0]]

    def test_instance_status_for_instance_ids_batches_calls(self):
        instance_ids = [{"foo": i} for i in range(0, 100)]
        with mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.describe_instance_status",
            autospec=True,
        ) as mock_describe_instance_status:
            mock_describe_instance_status.return_value = {
                "InstanceStatuses": [{"foo": "bar"}]
            }
            res = self.autoscaler.instance_status_for_instance_ids(
                instance_ids=instance_ids
            )
            assert len(res["InstanceStatuses"]) == 2

    def test_gracefully_terminate_slave(self):
        with mock.patch("time.time", autospec=True) as mock_time, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.drain", autospec=True
        ) as mock_drain, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.undrain", autospec=True
        ) as mock_undrain, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.wait_and_terminate",
            autospec=True,
        ) as mock_wait_and_terminate, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.set_capacity",
            autospec=True,
        ) as mock_set_capacity:
            mock_timer = mock.Mock()
            mock_wait_and_terminate.side_effect = just_sleep
            self.autoscaler.resource = {
                "id": "sfr-blah",
                "region": "westeros-1",
                "type": "sfr",
            }
            mock_time.return_value = int(1)
            mock_start = (1 + 123) * 1000000000
            mock_slave = mock.Mock(
                hostname="host1",
                instance_id="i-blah123",
                pid="slave(1)@10.1.1.1:5051",
                instance_weight=1,
                ip="10.1.1.1",
                instance_status={
                    "SystemStatus": {"Status": "ok"},
                    "InstanceStatus": {"Status": "ok"},
                },
            )
            self.autoscaler.capacity = 5
            _run(
                self.autoscaler.gracefully_terminate_slave(
                    slave_to_kill=mock_slave, capacity_diff=-1, timer=mock_timer
                )
            )

            def _set_capacity(self, capacity):
                self.capacity = capacity

            mock_set_capacity.side_effect = _set_capacity
            mock_drain.assert_called_with(
                hostnames=["host1|10.1.1.1"],
                start=mock_start,
                duration=600 * 1000000000,
                reserve_resources=True,
            )
            set_call_1 = mock.call(self.autoscaler, 4)
            mock_set_capacity.assert_has_calls([set_call_1])
            mock_wait_and_terminate.assert_called_with(
                self.autoscaler,
                slave=mock_slave,
                drain_timeout=123,
                dry_run=False,
                region="westeros-1",
                should_drain=True,
                timer=mock_timer,
            )

            # test we cleanup if a termination fails
            mock_set_capacity.reset_mock()
            set_call_2 = mock.call(self.autoscaler, 5)
            mock_wait_and_terminate.side_effect = get_coro_with_exception(
                ClientError({"Error": {}}, "blah")
            )
            self.autoscaler.capacity = 5
            _run(
                self.autoscaler.gracefully_terminate_slave(
                    slave_to_kill=mock_slave, capacity_diff=-1, timer=mock_timer
                )
            )
            mock_drain.assert_called_with(
                hostnames=["host1|10.1.1.1"],
                start=mock_start,
                duration=600 * 1000000000,
                reserve_resources=True,
            )
            mock_set_capacity.assert_has_calls([set_call_1, set_call_2])
            mock_wait_and_terminate.assert_called_with(
                self.autoscaler,
                slave=mock_slave,
                drain_timeout=123,
                dry_run=False,
                region="westeros-1",
                should_drain=True,
                timer=mock_timer,
            )
            mock_undrain.assert_called_with(
                hostnames=["host1|10.1.1.1"], unreserve_resources=True
            )

            # test we cleanup if a set spot capacity fails
            mock_wait_and_terminate.side_effect = just_sleep
            mock_wait_and_terminate.reset_mock()
            mock_set_capacity.side_effect = (
                autoscaling_cluster_lib.FailSetResourceCapacity
            )
            with raises(autoscaling_cluster_lib.FailSetResourceCapacity):
                self.autoscaler.capacity = 5
                _run(
                    self.autoscaler.gracefully_terminate_slave(
                        slave_to_kill=mock_slave, capacity_diff=-1, timer=mock_timer
                    )
                )
            mock_drain.assert_called_with(
                hostnames=["host1|10.1.1.1"],
                start=mock_start,
                duration=600 * 1000000000,
                reserve_resources=True,
            )
            mock_set_capacity.assert_has_calls([set_call_1])
            mock_undrain.assert_called_with(
                hostnames=["host1|10.1.1.1"], unreserve_resources=True
            )
            assert not mock_wait_and_terminate.called

            # test we cleanup if a drain fails
            mock_wait_and_terminate.side_effect = None
            mock_set_capacity.side_effect = None
            mock_set_capacity.reset_mock()
            mock_drain.side_effect = HTTPError
            with raises(HTTPError):
                self.autoscaler.capacity = 5
                _run(
                    self.autoscaler.gracefully_terminate_slave(
                        slave_to_kill=mock_slave, capacity_diff=-1, timer=mock_timer
                    )
                )
            mock_drain.assert_called_with(
                hostnames=["host1|10.1.1.1"],
                start=mock_start,
                duration=600 * 1000000000,
                reserve_resources=True,
            )
            assert not mock_set_capacity.called
            assert not mock_wait_and_terminate.called

    def test_wait_and_terminate(self):
        with mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.terminate_instances",
            autospec=True,
        ) as mock_terminate_instances, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.asyncio.sleep",
            autospec=True,
        ) as mock_sleep, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.is_safe_to_kill",
            autospec=True,
        ) as mock_is_safe_to_kill:
            mock_timer = mock.Mock()
            mock_timer.ready = lambda: False
            mock_sleep.side_effect = just_sleep

            mock_is_safe_to_kill.return_value = True
            mock_slave_to_kill = mock.Mock(
                hostname="hostblah",
                instance_id="i-blah123",
                pid="slave(1)@10.1.1.1:5051",
                ip="10.1.1.1",
            )
            _run(
                self.autoscaler.wait_and_terminate(
                    slave=mock_slave_to_kill,
                    drain_timeout=600,
                    dry_run=False,
                    timer=mock_timer,
                    region="westeros-1",
                    should_drain=True,
                )
            )
            mock_terminate_instances.assert_called_with(self.autoscaler, ["i-blah123"])
            mock_is_safe_to_kill.assert_called_with("hostblah")

            mock_is_safe_to_kill.side_effect = [False, False, True]
            _run(
                self.autoscaler.wait_and_terminate(
                    slave=mock_slave_to_kill,
                    drain_timeout=600,
                    dry_run=False,
                    timer=mock_timer,
                    region="westeros-1",
                    should_drain=True,
                )
            )
            assert mock_is_safe_to_kill.call_count == 4

    def test_get_instance_ips(self):
        with mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.describe_instances",
            autospec=True,
        ) as mock_describe_instances:
            mock_instance_ids = [{"InstanceId": "i-blah1"}, {"InstanceId": "i-blah2"}]
            mock_instances = [
                {"PrivateIpAddress": "10.1.1.1"},
                {"PrivateIpAddress": "10.2.2.2"},
            ]
            mock_describe_instances.return_value = mock_instances
            ret = self.autoscaler.get_instance_ips(
                mock_instance_ids, region="westeros-1"
            )
            mock_describe_instances.assert_called_with(
                self.autoscaler, ["i-blah1", "i-blah2"], region="westeros-1"
            )
            assert ret == ["10.1.1.1", "10.2.2.2"]

    def mock_pid_to_ip_side(self, pid):
        return {
            "slave(1)@10.1.1.1:5051": "10.1.1.1",
            "slave(2)@10.2.2.2:5051": "10.2.2.2",
            "slave(3)@10.3.3.3:5051": "10.3.3.3",
        }[pid]

    def test_filter_aws_slaves(self):
        with mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.get_instance_ips",
            autospec=True,
        ) as mock_get_instance_ips, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.slave_pid_to_ip",
            autospec=True,
        ) as mock_pid_to_ip, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.describe_instances",
            autospec=True,
        ) as mock_describe_instances, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.get_instance_type_weights",
            autospec=True,
        ) as mock_get_instance_type_weights, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.PaastaAwsSlave",
            autospec=True,
        ) as mock_paasta_aws_slave, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.describe_instance_status",
            autospec=True,
        ) as mock_describe_instance_status:
            mock_get_instance_ips.return_value = ["10.1.1.1", "10.3.3.3"]
            mock_pid_to_ip.side_effect = self.mock_pid_to_ip_side
            mock_instances = [
                {
                    "InstanceId": "i-1",
                    "InstanceType": "c4.blah",
                    "PrivateIpAddress": "10.1.1.1",
                },
                {
                    "InstanceId": "i-2",
                    "InstanceType": "m4.whatever",
                    "PrivateIpAddress": "10.3.3.3",
                },
                {
                    "InstanceId": "i-3",
                    "InstanceType": "m4.whatever",
                    "PrivateIpAddress": "10.1.1.3",
                },
            ]
            self.autoscaler.instances = mock_instances
            mock_describe_instances.return_value = mock_instances
            mock_instance_status = {
                "InstanceStatuses": [
                    {"InstanceId": "i-1"},
                    {"InstanceId": "i-2"},
                    {"InstanceId": "i-3"},
                ]
            }
            mock_describe_instance_status.return_value = mock_instance_status
            mock_slave_1 = {
                "task_counts": SlaveTaskCount(
                    slave={
                        "pid": "slave(1)@10.1.1.1:5051",
                        "id": "123",
                        "hostname": "host123",
                    },
                    count=0,
                    batch_count=0,
                )
            }
            mock_slave_2 = {
                "task_counts": SlaveTaskCount(
                    slave={
                        "pid": "slave(2)@10.2.2.2:5051",
                        "id": "456",
                        "hostname": "host456",
                    },
                    count=0,
                    batch_count=0,
                )
            }
            mock_slave_3 = {
                "task_counts": SlaveTaskCount(
                    slave={
                        "pid": "slave(3)@10.3.3.3:5051",
                        "id": "789",
                        "hostname": "host789",
                    },
                    count=0,
                    batch_count=0,
                )
            }

            mock_sfr_sorted_slaves = [mock_slave_1, mock_slave_2, mock_slave_3]
            mock_get_ip_call_1 = mock.call("slave(1)@10.1.1.1:5051")
            mock_get_ip_call_2 = mock.call("slave(2)@10.2.2.2:5051")
            mock_get_ip_call_3 = mock.call("slave(3)@10.3.3.3:5051")

            ret = self.autoscaler.filter_aws_slaves(mock_sfr_sorted_slaves)

            mock_get_instance_ips.assert_called_with(
                self.autoscaler, mock_instances, region="westeros-1"
            )
            mock_pid_to_ip.assert_has_calls(
                [mock_get_ip_call_1, mock_get_ip_call_2, mock_get_ip_call_3]
            )
            mock_describe_instances.assert_called_with(
                self.autoscaler,
                instance_ids=[],
                region="westeros-1",
                instance_filters=[
                    {"Values": ["10.1.1.1", "10.3.3.3"], "Name": "private-ip-address"}
                ],
            )
            mock_get_instance_type_weights.assert_called_with(self.autoscaler)
            mock_aws_slave_call_1 = mock.call(
                slave=mock_slave_1,
                instance_status=mock_instance_status["InstanceStatuses"][0],
                instance_description=mock_instances[0],
                instance_type_weights=mock_get_instance_type_weights.return_value,
            )
            mock_aws_slave_call_2 = mock.call(
                slave=mock_slave_3,
                instance_status=mock_instance_status["InstanceStatuses"][1],
                instance_description=mock_instances[1],
                instance_type_weights=mock_get_instance_type_weights.return_value,
            )
            mock_paasta_aws_slave.assert_has_calls(
                [mock_aws_slave_call_1, mock_aws_slave_call_2]
            )
            assert len(ret) == 2

    def test_get_aws_slaves(self):
        with mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.get_instance_ips",
            autospec=True,
        ) as mock_get_instance_ips, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.slave_pid_to_ip",
            autospec=True,
        ) as mock_slave_pid_to_ip:
            mock_slave_pid_to_ip.side_effect = pid_to_ip_sideeffect
            mock_get_instance_ips.return_value = ["10.1.1.1", "10.3.3.3", "10.4.4.4"]
            self.autoscaler.instances = [mock.Mock(), mock.Mock(), mock.Mock()]
            mock_mesos_state = {
                "slaves": [
                    {"id": "id1", "attributes": {"pool": "default"}, "pid": "pid1"},
                    {"id": "id2", "attributes": {"pool": "default"}, "pid": "pid2"},
                    {"id": "id3", "attributes": {"pool": "notdefault"}, "pid": "pid3"},
                ]
            }
            ret = self.autoscaler.get_aws_slaves(mock_mesos_state)
            mock_get_instance_ips.assert_called_with(
                self.autoscaler, self.autoscaler.instances, region="westeros-1"
            )
            assert ret == {"id1": mock_mesos_state["slaves"][0]}

    def test_cleanup_cancelled_config(self):
        with mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.os.walk", autospec=True
        ) as mock_os_walk, mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.os.remove", autospec=True
        ) as mock_os_remove:
            mock_os_walk.return_value = [
                ("/nail/blah", [], ["sfr-blah.json", "sfr-another.json"]),
                ("/nail/another", [], ["something"]),
            ]
            self.autoscaler.cleanup_cancelled_config("sfr-blah", "/nail")
            mock_os_walk.assert_called_with("/nail")
            mock_os_remove.assert_called_with("/nail/blah/sfr-blah.json")

            mock_os_remove.reset_mock()
            self.autoscaler.cleanup_cancelled_config("sfr-blah-not-exist", "/nail")
            assert not mock_os_remove.called

    def test_instace_descriptions_for_ips_splits_ips(self):
        with mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.ClusterAutoscaler.describe_instances",
            autospec=True,
        ) as mock_describe_instances:
            ips = list(range(567))

            def mock_describe_instance(self, instance_ids, region, instance_filters):
                return instance_filters[0]["Values"]

            mock_describe_instances.side_effect = mock_describe_instance

            ret = self.autoscaler.instance_descriptions_for_ips(ips)
            assert len(ret) == 567
            assert ret == ips
            assert mock_describe_instances.call_count == 3

    def test_terminate_instances(self):
        with mock.patch("boto3.client", autospec=True) as mock_boto_client:
            mock_terminate_instances = mock.Mock()
            mock_boto_client.return_value = mock.Mock(
                terminate_instances=mock_terminate_instances
            )

            instances_to_terminate = ["abc", "def"]
            self.autoscaler.terminate_instances(instances_to_terminate)
            mock_boto_client.assert_called_once_with(
                "ec2", region_name=self.autoscaler.resource["region"]
            )
            mock_terminate_instances.assert_called_once_with(
                InstanceIds=instances_to_terminate, DryRun=False
            )

            # DryRunOperation error should be swallowed during dry run
            self.autoscaler.dry_run = True
            mock_terminate_instances.side_effect = ClientError(
                {"Error": {"Code": "DryRunOperation"}}, "TerminateInstances"
            )
            self.autoscaler.terminate_instances(instances_to_terminate)


class TestPaastaAwsSlave(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            "paasta_tools.autoscaling.autoscaling_cluster_lib.get_instances_from_ip",
            autospec=True,
        ) as mock_get_instances_from_ip:
            mock_get_instances_from_ip.return_value = [{"InstanceId": "i-1"}]
            self.mock_instances = [
                {"InstanceId": "i-1", "InstanceType": "c4.blah"},
                {"InstanceId": "i-2", "InstanceType": "m4.whatever"},
                {"InstanceId": "i-3", "InstanceType": "m4.whatever"},
            ]
            self.mock_slave_1 = {
                "task_counts": SlaveTaskCount(
                    slave={
                        "pid": "slave(1)@10.1.1.1:5051",
                        "id": "123",
                        "hostname": "host123",
                    },
                    count=0,
                    batch_count=0,
                )
            }
            mock_instance_type_weights = {"c4.blah": 2, "m4.whatever": 5}
            self.mock_slave = autoscaling_cluster_lib.PaastaAwsSlave(
                slave=self.mock_slave_1,
                instance_description=self.mock_instances[0],
                instance_type_weights=mock_instance_type_weights,
            )
            self.mock_asg_slave = autoscaling_cluster_lib.PaastaAwsSlave(
                slave=self.mock_slave_1,
                instance_description=self.mock_instances,
                instance_type_weights=None,
            )
            mock_get_instances_from_ip.return_value = []
            self.mock_slave_no_instance = autoscaling_cluster_lib.PaastaAwsSlave(
                slave=self.mock_slave_1,
                instance_description=self.mock_instances,
                instance_type_weights=None,
            )
            mock_get_instances_from_ip.return_value = [
                {"InstanceId": "i-1"},
                {"InstanceId": "i-2"},
            ]
            self.mock_slave_extra_instance = autoscaling_cluster_lib.PaastaAwsSlave(
                slave=self.mock_slave_1,
                instance_description=self.mock_instances,
                instance_type_weights=None,
            )

    def test_instance_id(self):
        assert self.mock_slave.instance_id == "i-1"

    def test_hostname(self):
        assert self.mock_slave.hostname == "host123"

    def test_pid(self):
        assert self.mock_slave.pid == "slave(1)@10.1.1.1:5051"

    def test_instance_type(self):
        assert self.mock_slave.instance_type == "c4.blah"

    def test_instance_weight(self):
        assert self.mock_slave.instance_weight == 2
        assert self.mock_asg_slave.instance_weight == 1
