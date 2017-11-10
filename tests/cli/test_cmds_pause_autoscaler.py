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

from paasta_tools.cli.cmds.pause_autoscaler import MAX_PAUSE_DURATION
from paasta_tools.cli.cmds.pause_autoscaler import pause_autoscaler


def test_pause_autoscaler_defaults():
    args = mock.Mock(
        cluster='cluster1',
        duration=30,
    )

    with mock.patch(
        'paasta_tools.cli.cmds.pause_autoscaler.execute_pause_service_autoscaler_on_remote_master',
        autospec=True,
    ) as mock_exc, mock.patch(
        'paasta_tools.cli.cmds.pause_autoscaler.load_system_paasta_config', autospec=True,
    ) as mock_config_fn:

        mock_config = mock.Mock()
        mock_config_fn.return_value = mock_config
        mock_exc.return_value = (0, '')

        return_code = pause_autoscaler(args)
        mock_exc.assert_called_once_with('cluster1', mock_config, 30)
        assert return_code == 0


def test_pause_autoscaler_long():
    args = mock.Mock(
        cluster='cluster1',
        duration=MAX_PAUSE_DURATION + 10,
        force=False,
    )

    with mock.patch(
        'paasta_tools.cli.cmds.pause_autoscaler.execute_pause_service_autoscaler_on_remote_master',
        autospec=True,
    ) as mock_exc, mock.patch(
        'paasta_tools.cli.cmds.pause_autoscaler.load_system_paasta_config', autospec=True,
    ) as mock_config_fn:

        mock_config = mock.Mock()
        mock_config_fn.return_value = mock_config
        mock_exc.return_value = (0, '')

        return_code = pause_autoscaler(args)
        assert return_code == 2


def test_pause_autoscaler_force():
    args = mock.Mock(
        cluster='cluster1',
        duration=MAX_PAUSE_DURATION + 10,
        force=True,
    )

    with mock.patch(
        'paasta_tools.cli.cmds.pause_autoscaler.execute_pause_service_autoscaler_on_remote_master',
        autospec=True,
    ) as mock_exc, mock.patch(
        'paasta_tools.cli.cmds.pause_autoscaler.load_system_paasta_config', autospec=True,
    ) as mock_config_fn:

        mock_config = mock.Mock()
        mock_config_fn.return_value = mock_config
        mock_exc.return_value = (0, '')

        return_code = pause_autoscaler(args)
        mock_exc.assert_called_once_with('cluster1', mock_config, 130)
        assert return_code == 0
