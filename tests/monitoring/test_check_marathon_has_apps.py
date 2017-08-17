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


def test_check_chronos_jobs_no_config(capfd):
    with mock.patch(
        'paasta_tools.monitoring.check_marathon_has_apps.load_marathon_config', autospec=True,
        return_value=None,
    ):
        with pytest.raises(SystemExit) as error:
            check_marathon_apps()
        out, err = capfd.readouterr()
        assert "UNKNOWN" in out
        assert error.value.code == 3


def test_chronos_jobs_no_jobs(capfd):
    with mock.patch(
        # We expect this is tested properly elsewhere
        'paasta_tools.monitoring.check_marathon_has_apps.get_marathon_client', autospec=True,
    ) as mock_get_marathon_config, mock.patch(
        'paasta_tools.monitoring.check_marathon_has_apps.load_marathon_config', autospec=True,
    ):
        l = mock.MagicMock()
        l.list_apps = lambda: []
        mock_get_marathon_config.return_value = l
        with pytest.raises(SystemExit) as error:
            check_marathon_apps()
        out, err = capfd.readouterr()
        assert "CRITICAL" in out
        assert error.value.code == 2


def test_chronos_jobs_some_jobs(capfd):
    with mock.patch(
        # We expect this is tested properly elsewhere
        'paasta_tools.monitoring.check_marathon_has_apps.get_marathon_client', autospec=True,
    ) as mock_get_marathon_config, mock.patch(
        'paasta_tools.monitoring.check_marathon_has_apps.load_marathon_config', autospec=True,
    ):
        l = mock.MagicMock()
        l.list_apps = lambda: ['foo', 'bar']
        mock_get_marathon_config.return_value = l
        with pytest.raises(SystemExit) as error:
            check_marathon_apps()
        out, err = capfd.readouterr()
        assert "OK" in out
        assert "2" in out
        assert error.value.code == 0
