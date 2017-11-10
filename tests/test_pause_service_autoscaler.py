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

from paasta_tools import pause_service_autoscaler


def test_main_default():
    with patch(
        'paasta_tools.pause_service_autoscaler.datetime', autospec=True,
    ) as time_mock, patch(
        'paasta_tools.utils.KazooClient', autospec=True,
    ) as zk_mock, patch(
        'paasta_tools.pause_service_autoscaler.parse_args', autospec=True,
    ) as args_mock, patch(
        'paasta_tools.pause_service_autoscaler.load_system_paasta_config', autospec=True,
    ), patch(
        'paasta_tools.utils.load_system_paasta_config', autospec=True,
    ):
        mock_ts = Mock()
        mock_ts.timestamp.return_value = '0'
        time_mock.now.return_value = mock_ts

        parsed_args_mock = Mock()
        parsed_args_mock.timeout = pause_service_autoscaler.DEFAULT_PAUSE_DURATION
        args_mock.return_value = parsed_args_mock

        mock_zk_set = Mock()
        mock_zk_ensure = Mock()
        zk_mock.return_value = Mock(set=mock_zk_set, ensure_path=mock_zk_ensure)

        pause_service_autoscaler.main()
        mock_zk_ensure.assert_called_once_with('/autoscaling/paused')
        mock_zk_set.assert_called_once_with('/autoscaling/paused', b'1800')
