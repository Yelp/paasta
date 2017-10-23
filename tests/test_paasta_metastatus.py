#!/usr/bin/env python
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
from mock import Mock
from mock import patch
from pytest import raises

from paasta_tools import paasta_metastatus
from paasta_tools.metrics.metastatus_lib import HealthCheckResult


def test_main_no_marathon_config():
    with patch(
        'paasta_tools.paasta_metastatus.load_system_paasta_config', autospec=True,
    ), patch(
        'paasta_tools.marathon_tools.load_marathon_config', autospec=True,
    ) as load_marathon_config_patch, patch(
        'paasta_tools.paasta_metastatus.load_chronos_config', autospec=True,
    ), patch(
        'paasta_tools.metrics.metastatus_lib.get_chronos_status', autospec=True,
    ), patch(
        'paasta_tools.paasta_metastatus.get_mesos_master', autospec=True,
    ) as get_mesos_master, patch(
        'paasta_tools.metrics.metastatus_lib.get_mesos_state_status', autospec=True,
        return_value=([('fake_output', True)]),
    ) as get_mesos_state_status_patch, patch(
        'paasta_tools.metrics.metastatus_lib.get_mesos_resource_utilization_health', autospec=True,
    ) as get_mesos_resource_utilization_health_patch, patch(
        'paasta_tools.metrics.metastatus_lib.get_marathon_status',
        autospec=True,
        return_value=([HealthCheckResult(message='fake_output', healthy=True)]),
    ):
        fake_master = Mock(autospace=True)
        fake_master.state.return_value = {}
        get_mesos_master.return_value = fake_master

        get_mesos_state_status_patch.return_value = []
        get_mesos_resource_utilization_health_patch.return_value = []

        load_marathon_config_patch.return_value = {}
        with raises(SystemExit) as excinfo:
            paasta_metastatus.main(())
        assert excinfo.value.code == 0


def test_main_no_chronos_config():
    with patch(
        'paasta_tools.paasta_metastatus.load_system_paasta_config', autospec=True,
    ), patch(
        'paasta_tools.marathon_tools.load_marathon_config', autospec=True,
    ) as load_marathon_config_patch, patch(
        'paasta_tools.paasta_metastatus.load_chronos_config', autospec=True,
    ) as load_chronos_config_patch, patch(
        'paasta_tools.paasta_metastatus.get_mesos_master', autospec=True,
    ) as get_mesos_master, patch(
        'paasta_tools.metrics.metastatus_lib.get_mesos_state_status', autospec=True,
        return_value=([('fake_output', True)]),
    ) as get_mesos_state_status_patch, patch(
        'paasta_tools.metrics.metastatus_lib.get_mesos_resource_utilization_health', autospec=True,
    ) as get_mesos_resource_utilization_health_patch, patch(
        'paasta_tools.metrics.metastatus_lib.get_marathon_status',
        autospec=True,
        return_value=([HealthCheckResult(message='fake_output', healthy=True)]),
    ):
        fake_master = Mock(autospace=True)
        fake_master.state.return_value = {}
        get_mesos_master.return_value = fake_master

        load_marathon_config_patch.return_value = {}

        get_mesos_state_status_patch.return_value = []
        get_mesos_resource_utilization_health_patch.return_value = []

        load_chronos_config_patch.return_value = {}
        with raises(SystemExit) as excinfo:
            paasta_metastatus.main(())
        assert excinfo.value.code == 0


def test_main_marathon_jsondecode_error():
    with patch(
        'paasta_tools.paasta_metastatus.load_system_paasta_config', autospec=True,
    ), patch(
        'paasta_tools.marathon_tools.load_marathon_config', autospec=True,
    ) as load_marathon_config_patch, patch(
        'paasta_tools.paasta_metastatus.load_chronos_config', autospec=True,
    ), patch(
        'paasta_tools.paasta_metastatus.get_mesos_master', autospec=True,
    ) as get_mesos_master, patch(
        'paasta_tools.metrics.metastatus_lib.get_mesos_state_status', autospec=True,
        return_value=([('fake_output', True)]),
    ) as get_mesos_state_status_patch, patch(
        'paasta_tools.metrics.metastatus_lib.get_mesos_resource_utilization_health', autospec=True,
    ) as get_mesos_resource_utilization_health_patch, patch(
        'paasta_tools.metrics.metastatus_lib.get_marathon_client', autospec=True,
    ) as get_marathon_client_patch, patch(
        'paasta_tools.metrics.metastatus_lib.get_marathon_status', autospec=True,
    ) as get_marathon_status_patch:
        fake_master = Mock(autospace=True)
        fake_master.state.return_value = {}
        get_mesos_master.return_value = fake_master

        load_marathon_config_patch.return_value = {"url": "http://foo"}
        get_marathon_client_patch.return_value = Mock()

        get_marathon_status_patch.side_effect = ValueError('could not decode json')

        get_mesos_state_status_patch.return_value = []
        get_mesos_resource_utilization_health_patch.return_value = []

        with raises(SystemExit) as excinfo:
            paasta_metastatus.main(())
        assert excinfo.value.code == 2
