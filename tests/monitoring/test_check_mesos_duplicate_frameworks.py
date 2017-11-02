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

from paasta_tools.monitoring.check_mesos_duplicate_frameworks import check_mesos_no_duplicate_frameworks


def test_check_mesos_no_duplicate_frameworks_ok(capfd):
    with mock.patch(
        'paasta_tools.marathon_tools.get_list_of_marathon_clients',
        autospec=True,
    ), mock.patch(
        'paasta_tools.monitoring.check_mesos_duplicate_frameworks.get_marathon_framework_ids',
        autospec=True,
        return_value=['id_marathon'],
    ), mock.patch(
        'paasta_tools.monitoring.check_mesos_duplicate_frameworks.get_mesos_master', autospec=True,
    ) as mock_get_mesos_master:
        mock_master = mock.MagicMock()
        mock_master.state = {
            'frameworks': [
                {'name': 'marathon', 'id': 'id_marathon'},
                {'name': 'chronos', 'id': 'id_chronos'},
                {'name': 'foobar', 'id': 'id_foobar_1'},
                {'name': 'foobar', 'id': 'id_foobar_2'},
            ],
        }
        mock_get_mesos_master.return_value = mock_master

        with pytest.raises(SystemExit) as error:
            check_mesos_no_duplicate_frameworks()
        out, err = capfd.readouterr()
        assert "OK" in out
        assert "marathon" in out
        assert "chronos" in out
        assert "foobar" not in out
        assert error.value.code == 0


def test_check_mesos_no_duplicate_frameworks_critical(capfd):
    with mock.patch(
        'paasta_tools.marathon_tools.get_list_of_marathon_clients',
        autospec=True,
    ), mock.patch(
        'paasta_tools.monitoring.check_mesos_duplicate_frameworks.get_marathon_framework_ids',
        autospec=True,
        return_value=['id_marathon_1', 'id_marathon_2'],
    ), mock.patch(
        'paasta_tools.monitoring.check_mesos_duplicate_frameworks.get_mesos_master', autospec=True,
    ) as mock_get_mesos_master:
        mock_master = mock.MagicMock()
        mock_master.state = {
            'frameworks': [
                {'name': 'marathon', 'id': 'id_marathon_1'},
                {'name': 'marathon', 'id': 'id_marathon_3'},
                {'name': 'chronos', 'id': 'id_chronos'},
                {'name': 'foobar', 'id': 'id_foobar_1'},
                {'name': 'foobar', 'id': 'id_foobar_2'},
            ],
        }
        mock_get_mesos_master.return_value = mock_master

        with pytest.raises(SystemExit) as error:
            check_mesos_no_duplicate_frameworks()
        out, err = capfd.readouterr()
        assert "CRITICAL" in out
        assert "Disconnected marathon framework IDs: id_marathon_2" in out
        assert "chronos" in out
        assert "foobar" not in out
        assert error.value.code == 2
