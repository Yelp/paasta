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
import sys

import mock
from mock import patch
from pytest import raises

from paasta_tools.cli.cmds import sysdig


@patch("paasta_tools.cli.cmds.sysdig.get_subparser", autospec=True)
def test_add_subparser(mock_get_subparser):
    mock_subparsers = mock.Mock()
    sysdig.add_subparser(mock_subparsers)
    assert mock_get_subparser.called


@patch.object(sys, "argv", ["paasta", "sysdig", "blah", "blah"])
@patch("paasta_tools.cli.cmds.sysdig.load_marathon_service_config", autospec=True)
@patch("paasta_tools.cli.cmds.sysdig.load_system_paasta_config", autospec=True)
@patch("paasta_tools.cli.cmds.sysdig.format_mesos_command", autospec=True)
@patch("paasta_tools.cli.cmds.sysdig.get_mesos_master", autospec=True)
@patch("paasta_tools.cli.cmds.sysdig._run", autospec=True)
@patch("paasta_tools.cli.cmds.sysdig.get_any_mesos_master", autospec=True)
@patch("paasta_tools.cli.cmds.sysdig.subprocess", autospec=True)
@patch("paasta_tools.cli.cmds.sysdig.pick_slave_from_status", autospec=True)
@patch("paasta_tools.cli.cmds.sysdig.get_status_for_instance", autospec=True)
def test_paasta_sysdig(
    mock_get_status_for_instance,
    mock_pick_slave_from_status,
    mock_subprocess,
    mock_get_any_mesos_master,
    mock__run,
    mock_get_mesos_master,
    mock_format_mesos_command,
    mock_load_system_paasta_config,
    mock_load_marathon_service_config,
):

    mock_status = mock.Mock(marathon=mock.Mock(app_id="appID1"))
    mock_get_status_for_instance.return_value = mock_status
    mock_args = mock.Mock(
        cluster="cluster1",
        service="mock_service",
        instance="mock_instance",
        host="host1",
        mesos_id=None,
        local=False,
    )
    mock_pick_slave_from_status.return_value = "host1"
    mock_get_any_mesos_master.return_value = "master1"
    mock__run.return_value = (0, "slave:command123")

    sysdig.paasta_sysdig(mock_args)
    mock_get_any_mesos_master.assert_called_with(
        cluster="cluster1",
        system_paasta_config=mock_load_system_paasta_config.return_value,
    )
    mock__run.assert_called_with(
        "ssh -At -o StrictHostKeyChecking=no -o LogLevel=QUIET master1 "
        '"sudo paasta sysdig blah blah --local"'
    )
    mock_subprocess.call.assert_called_with(["ssh", "-tA", "slave", "command123"])

    mock__run.return_value = (1, "slave:command123")
    with raises(SystemExit):
        sysdig.paasta_sysdig(mock_args)

    mock_args = mock.Mock(
        cluster="cluster1",
        service="mock_service",
        instance="mock_instance",
        host="host1",
        mesos_id=None,
        local=True,
    )
    mock_pick_slave_from_status.return_value = "slave1"
    fake_server_config = {"url": ["http://blah"], "user": "user", "password": "pass"}
    mock_load_system_paasta_config.return_value.get_marathon_servers = mock.Mock(
        return_value=[fake_server_config]
    )
    mock_load_system_paasta_config.return_value.get_previous_marathon_servers = mock.Mock(
        return_value=[fake_server_config]
    )
    mock_load_marathon_service_config().get_marathon_shard.return_value = None

    mock_get_mesos_master.return_value = mock.Mock(host="http://foo")
    sysdig.paasta_sysdig(mock_args)
    mock_get_status_for_instance.assert_called_with(
        cluster="cluster1", service="mock_service", instance="mock_instance"
    )
    mock_pick_slave_from_status.assert_called_with(status=mock_status, host="host1")
    mock_format_mesos_command.assert_called_with(
        "slave1", "appID1", "http://foo", "http://user:pass@blah"
    )


def test_format_mesos_command():
    ret = sysdig.format_mesos_command(
        "slave1", "appID1", "http://foo", "http://user:pass@blah"
    )
    expected = 'slave1:sudo csysdig -m http://foo,http://user:pass@blah marathon.app.id="/appID1" -v mesos_tasks'
    assert ret == expected


@patch("paasta_tools.cli.cmds.sysdig.calculate_remote_masters", autospec=True)
@patch("paasta_tools.cli.cmds.sysdig.find_connectable_master", autospec=True)
def test_get_any_mesos_master(
    mock_find_connectable_master, mock_calculate_remote_masters, system_paasta_config
):
    mock_calculate_remote_masters.return_value = ([], "fakeERROR")

    with raises(SystemExit):
        sysdig.get_any_mesos_master("cluster1", system_paasta_config)
    mock_calculate_remote_masters.assert_called_with("cluster1", system_paasta_config)

    mock_masters = mock.Mock()
    mock_calculate_remote_masters.return_value = (mock_masters, "fake")
    mock_find_connectable_master.return_value = (False, "fakeERROR")
    with raises(SystemExit):
        sysdig.get_any_mesos_master("cluster1", system_paasta_config)

    mock_master = mock.Mock()
    mock_find_connectable_master.return_value = (mock_master, "fake")
    assert sysdig.get_any_mesos_master("cluster1", system_paasta_config) == mock_master
