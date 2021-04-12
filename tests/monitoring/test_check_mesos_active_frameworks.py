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
from unittest import mock

import pytest
from asynctest import CoroutineMock

from paasta_tools.monitoring.check_mesos_active_frameworks import (
    check_mesos_active_frameworks,
)


def test_check_mesos_active_frameworks_fails(capfd):
    with mock.patch(
        "paasta_tools.monitoring.check_mesos_active_frameworks.parse_args",
        autospec=True,
    ) as mock_parse_args, mock.patch(
        "paasta_tools.monitoring.check_mesos_active_frameworks.get_mesos_master",
        autospec=True,
    ) as mock_get_mesos_master:
        mock_opts = mock.MagicMock()
        mock_opts.expected = "foo,bar"
        mock_parse_args.return_value = mock_opts
        mock_master = mock.MagicMock()
        mock_master.state = CoroutineMock(
            func=CoroutineMock(),  # https://github.com/notion/a_sync/pull/40
            return_value={"frameworks": [{"name": "foo"}]},
        )
        mock_get_mesos_master.return_value = mock_master

        with pytest.raises(SystemExit) as error:
            check_mesos_active_frameworks()
        out, err = capfd.readouterr()
        assert "CRITICAL" in out
        assert "bar" in out
        assert "foo" not in out
        assert error.value.code == 2


def test_check_mesos_active_frameworks_succeeds(capfd):
    with mock.patch(
        "paasta_tools.monitoring.check_mesos_active_frameworks.parse_args",
        autospec=True,
    ) as mock_parse_args, mock.patch(
        "paasta_tools.monitoring.check_mesos_active_frameworks.get_mesos_master",
        autospec=True,
    ) as mock_get_mesos_master:
        mock_opts = mock.MagicMock()
        mock_opts.expected = "foo,bar"
        mock_parse_args.return_value = mock_opts
        mock_master = mock.MagicMock()
        mock_master.state = CoroutineMock(
            func=CoroutineMock(),  # https://github.com/notion/a_sync/pull/40
            return_value={"frameworks": [{"name": "foo"}, {"name": "bar"}]},
        )
        mock_get_mesos_master.return_value = mock_master

        with pytest.raises(SystemExit) as error:
            check_mesos_active_frameworks()
        out, err = capfd.readouterr()
        assert "OK" in out
        assert error.value.code == 0
