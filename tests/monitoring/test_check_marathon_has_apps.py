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
import pytest

from paasta_tools.monitoring.check_marathon_has_apps import check_marathon_apps


def test_check_marathon_jobs_no_config(capfd):
    with mock.patch(
        "paasta_tools.marathon_tools.get_list_of_marathon_clients",
        autospec=True,
        return_value=[],
    ):
        with pytest.raises(SystemExit) as error:
            check_marathon_apps()
        out, err = capfd.readouterr()
        assert "UNKNOWN" in out
        assert error.value.code == 3


def test_marathon_jobs_no_jobs(capfd):
    mock_client = mock.MagicMock()
    mock_client.list_apps.return_value = []
    with mock.patch(
        # We expect this is tested properly elsewhere
        "paasta_tools.marathon_tools.get_list_of_marathon_clients",
        autospec=True,
        return_value=[mock_client],
    ):
        with pytest.raises(SystemExit) as error:
            check_marathon_apps()
        out, err = capfd.readouterr()
        assert "CRITICAL" in out
        assert error.value.code == 2


def test_marathon_jobs_some_jobs(capfd):
    mock_client = mock.MagicMock()
    with mock.patch(
        # We expect this is tested properly elsewhere
        "paasta_tools.marathon_tools.get_list_of_marathon_clients",
        autospec=True,
        return_value=[mock_client],
    ), mock.patch(
        "paasta_tools.metrics.metastatus_lib.get_all_marathon_apps",
        autospec=True,
        return_value=["foo", "bar"],
    ):
        with pytest.raises(SystemExit) as error:
            check_marathon_apps()
        out, err = capfd.readouterr()
        assert "OK" in out
        assert "2" in out
        assert error.value.code == 0
