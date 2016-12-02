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
from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib

from mock import Mock
from mock import patch
from pytest import raises

from paasta_tools import paasta_metastatus


def test_main_no_marathon_config():
    with contextlib.nested(
        patch('paasta_tools.marathon_tools.load_marathon_config', autospec=True),
        patch('paasta_tools.paasta_metastatus.load_chronos_config', autospec=True),
        patch('paasta_tools.metrics.metastatus_lib.get_chronos_status', autospec=True),
        patch('paasta_tools.paasta_metastatus.get_mesos_master', autospec=True),
        patch('paasta_tools.metrics.metastatus_lib.get_mesos_state_status', autospec=True,
              return_value=([('fake_output', True)])),
        patch('paasta_tools.metrics.metastatus_lib.get_mesos_resource_utilization_health', autospec=True),
        patch('paasta_tools.metrics.metastatus_lib.get_marathon_status', autospec=True,
              return_value=([('fake_output', True)])),
        patch('paasta_tools.paasta_metastatus.parse_args', autospec=True),
    ) as (
        load_marathon_config_patch,
        load_chronos_config_patch,
        load_get_chronos_status_patch,
        get_mesos_master,
        get_mesos_state_status_patch,
        get_mesos_resource_utilization_health_patch,
        load_get_marathon_status_patch,
        parse_args_patch,
    ):
        fake_args = Mock(
            verbose=0,
        )
        fake_master = Mock(autospace=True)
        fake_master.metrics_snapshot.return_value = {
            'master/frameworks_active': 2,
            'master/frameworks_inactive': 0,
            'master/frameworks_connected': 2,
            'master/frameworks_disconnected': 0,
        }
        fake_master.state.return_value = {}
        get_mesos_master.return_value = fake_master

        get_mesos_state_status_patch.return_value = []
        get_mesos_resource_utilization_health_patch.return_value = []

        parse_args_patch.return_value = fake_args
        load_marathon_config_patch.return_value = {}
        with raises(SystemExit) as excinfo:
            paasta_metastatus.main()
        assert excinfo.value.code == 0


def test_main_no_chronos_config():
    with contextlib.nested(
        patch('paasta_tools.marathon_tools.load_marathon_config', autospec=True),
        patch('paasta_tools.paasta_metastatus.load_chronos_config', autospec=True),
        patch('paasta_tools.paasta_metastatus.get_mesos_master', autospec=True),
        patch('paasta_tools.metrics.metastatus_lib.get_mesos_state_status', autospec=True,
              return_value=([('fake_output', True)])),
        patch('paasta_tools.metrics.metastatus_lib.get_mesos_resource_utilization_health', autospec=True),
        patch('paasta_tools.metrics.metastatus_lib.get_marathon_status', autospec=True,
              return_value=([('fake_output', True)])),
        patch('paasta_tools.paasta_metastatus.parse_args', autospec=True),
    ) as (
        load_marathon_config_patch,
        load_chronos_config_patch,
        get_mesos_master,
        get_mesos_state_status_patch,
        get_mesos_resource_utilization_health_patch,
        load_get_marathon_status_patch,
        parse_args_patch,
    ):

        fake_args = Mock(
            verbose=0,
        )
        fake_master = Mock(autospace=True)
        fake_master.metrics_snapshot.return_value = {
            'master/frameworks_active': 2,
            'master/frameworks_inactive': 0,
            'master/frameworks_connected': 2,
            'master/frameworks_disconnected': 0,
        }
        fake_master.state.return_value = {}
        get_mesos_master.return_value = fake_master

        parse_args_patch.return_value = fake_args
        load_marathon_config_patch.return_value = {}

        get_mesos_state_status_patch.return_value = []
        get_mesos_resource_utilization_health_patch.return_value = []

        load_chronos_config_patch.return_value = {}
        with raises(SystemExit) as excinfo:
            paasta_metastatus.main()
        assert excinfo.value.code == 0
