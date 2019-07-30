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
import mock
from mock import patch

from paasta_tools.cli.cmds import docker_inspect


@patch("paasta_tools.cli.cmds.docker_inspect.get_subparser", autospec=True)
def test_add_subparser(mock_get_subparser):
    mock_subparsers = mock.Mock()
    docker_inspect.add_subparser(mock_subparsers)
    assert mock_get_subparser.called


@patch("paasta_tools.cli.cmds.docker_inspect.subprocess", autospec=True)
@patch("paasta_tools.cli.cmds.docker_inspect.get_container_name", autospec=True)
@patch("paasta_tools.cli.cmds.docker_inspect.get_task_from_instance", autospec=True)
def test_paasta_docker_inspect(
    mock_get_task_from_instance, mock_get_container_name, mock_subprocess
):
    mock_task = mock.Mock(slave={"hostname": "host1"})
    mock_get_task_from_instance.return_value = mock_task
    mock_args = mock.Mock(
        cluster="cluster1",
        service="mock_service",
        instance="mock_instance",
        host="host1",
        mesos_id=None,
    )

    docker_inspect.paasta_docker_inspect(mock_args)

    mock_get_task_from_instance.assert_called_with(
        cluster="cluster1",
        service="mock_service",
        instance="mock_instance",
        slave_hostname="host1",
        task_id=None,
    )

    mock_get_container_name.assert_called_with(mock_task)
    expected = [
        "ssh",
        "-o",
        "LogLevel=QUIET",
        "-tA",
        "host1",
        f"sudo docker inspect {mock_get_container_name.return_value}",
    ]
    mock_subprocess.call.assert_called_with(expected)
