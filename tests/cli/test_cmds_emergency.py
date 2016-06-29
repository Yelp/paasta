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

from paasta_tools.cli.cmds import emergency_restart
from paasta_tools.cli.cmds import emergency_scale
from paasta_tools.cli.cmds import emergency_start
from paasta_tools.cli.cmds import emergency_stop


@patch('paasta_tools.cli.cmds.emergency_start.load_system_paasta_config', return_value={}, autospec=True)
@patch('paasta_tools.cli.cmds.emergency_start.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.emergency_start.figure_out_service_name', return_value='fake_service')
def test_emergency_start(mock_service_name, mock_execute, mock_load_config):
    args = Mock()
    args.service = 'fake_service'
    args.soa_dir = 'fakesoadir/'
    args.cluster = 'fakecluster'
    args.instance = 'fakeinstance'
    emergency_start.paasta_emergency_start(args)
    mock_execute.assert_called_with(
        subcommand='start',
        cluster=args.cluster,
        service=args.service,
        instances=args.instance,
        system_paasta_config={}
    )


@patch('paasta_tools.cli.cmds.emergency_stop.load_system_paasta_config', return_value={}, autospec=True)
@patch('paasta_tools.cli.cmds.emergency_stop.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.emergency_stop.figure_out_service_name', return_value='fake_service')
def test_emergency_stop(mock_service_name, mock_execute, mock_load_config):
    args = Mock()
    args.service = 'fake_service'
    args.soa_dir = 'fakesoadir/'
    args.appid = 'fakeappid'
    emergency_stop.paasta_emergency_stop(args)
    mock_execute.assert_called_with(
        subcommand='stop',
        cluster=args.cluster,
        service=args.service,
        instances=args.instance,
        system_paasta_config={},
        app_id=args.appid
    )


@patch('paasta_tools.cli.cmds.emergency_restart.load_system_paasta_config', return_value={}, autospec=True)
@patch('paasta_tools.cli.cmds.emergency_restart.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.emergency_restart.figure_out_service_name', return_value='fake_service')
def test_emergency_restart(mock_service_name, mock_execute, mock_load_config):
    args = Mock()
    args.service = 'fake_service'
    args.soa_dir = 'fakesoadir/'
    args.appid = 'fakeappid'
    emergency_restart.paasta_emergency_restart(args)
    mock_execute.assert_called_with(
        subcommand='restart',
        cluster=args.cluster,
        service=args.service,
        instances=args.instance,
        system_paasta_config={}
    )


@patch('paasta_tools.cli.cmds.emergency_scale.load_system_paasta_config', return_value={}, autospec=True)
@patch('paasta_tools.cli.cmds.emergency_scale.execute_paasta_serviceinit_on_remote_master', autospec=True)
@patch('paasta_tools.cli.cmds.emergency_scale.figure_out_service_name', return_value='fake_service')
def test_emergency_scale(mock_service_name, mock_execute, mock_load_config):
    args = Mock()
    args.service = 'fake_service'
    args.soa_dir = 'fakesoadir/'
    args.appid = 'fakeappid'
    args.delta = 'fakedelta'
    emergency_scale.paasta_emergency_scale(args)
    mock_execute.assert_called_with(
        subcommand='scale',
        cluster=args.cluster,
        service=args.service,
        instances=args.instance,
        system_paasta_config={},
        app_id=args.appid,
        delta=args.delta
    )
