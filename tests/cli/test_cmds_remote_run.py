# Copyright 2015-2017 Yelp Inc.
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
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import patch

from paasta_tools.cli.cmds.remote_run import paasta_remote_run_start
from paasta_tools.cli.cmds.remote_run import paasta_remote_run_stop
from paasta_tools.paastaapi.model.remote_run_start import RemoteRunStart
from paasta_tools.paastaapi.model.remote_run_stop import RemoteRunStop


@patch("paasta_tools.cli.cmds.remote_run.time", autospec=True)
@patch(
    "paasta_tools.cli.cmds.remote_run.get_username", return_value="pippo", autospec=True
)
@patch("paasta_tools.cli.cmds.remote_run.pty", autospec=True)
@patch(
    "paasta_tools.cli.cmds.remote_run.get_paasta_oapi_client_with_auth", autospec=True
)
def test_paasta_remote_run_start(mock_get_client, mock_pty, *_):
    mock_config = MagicMock()
    mock_args = MagicMock(
        service="foo",
        instance="bar",
        cluster="dev",
        interactive=True,
        recreate=False,
        max_duration=100,
    )
    mock_client = mock_get_client.return_value
    mock_client.remote_run.remote_run_start.return_value = MagicMock(
        status=200, message="started", job_name="foobar"
    )
    mock_client.remote_run.remote_run_poll.side_effect = [
        MagicMock(status=204, message="waiting"),
        MagicMock(status=204, message="waiting"),
        MagicMock(
            status=200, message="started", pod_name="foobar-123", namespace="svcfoo"
        ),
    ]
    mock_client.remote_run.remote_run_token.return_value = MagicMock(token="aaabbbccc")
    paasta_remote_run_start(mock_args, mock_config)
    mock_client.remote_run.remote_run_start.assert_called_once_with(
        "foo",
        "bar",
        RemoteRunStart(
            user="pippo", interactive=True, recreate=False, max_duration=100
        ),
    )
    mock_client.remote_run.remote_run_poll.assert_has_calls(
        [call("foo", "bar", "foobar")] * 3
    )
    mock_client.remote_run.remote_run_token.assert_called_once_with(
        "foo", "bar", "pippo"
    )
    mock_pty.spawn.assert_called_once_with(
        [
            "kubectl-eks-dev",
            "--token",
            "aaabbbccc",
            "exec",
            "-it",
            "-n",
            "svcfoo",
            "foobar-123",
            "--",
            "/bin/bash",
        ]
    )


@patch(
    "paasta_tools.cli.cmds.remote_run.get_username", return_value="pippo", autospec=True
)
@patch(
    "paasta_tools.cli.cmds.remote_run.get_paasta_oapi_client_with_auth", autospec=True
)
def test_paasta_remote_run_stop(mock_get_client, _):
    mock_config = MagicMock()
    mock_args = MagicMock(service="foo", instance="bar")
    mock_client = mock_get_client.return_value
    mock_client.remote_run.remote_run_stop.return_value = MagicMock(
        status=200, message="stopped"
    )
    paasta_remote_run_stop(mock_args, mock_config)
    mock_client.remote_run.remote_run_stop.assert_called_once_with(
        "foo",
        "bar",
        RemoteRunStop(user="pippo"),
    )
