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

from paasta_tools.monitoring.check_chronos_has_jobs import check_chronos_jobs


def test_check_chronos_jobs_no_config(capfd):
    with mock.patch(
        "paasta_tools.monitoring.check_chronos_has_jobs.load_chronos_config",
        autospec=True,
        return_value=None,
    ):
        with pytest.raises(SystemExit) as error:
            check_chronos_jobs()
        out, err = capfd.readouterr()
        assert "UNKNOWN" in out
        assert error.value.code == 3


def test_chronos_jobs_no_jobs(capfd):
    with mock.patch(
        # We expect this is tested properly elsewhere
        "paasta_tools.metrics.metastatus_lib.chronos_tools.filter_enabled_jobs",
        autospec=True,
        return_value=[],
    ), mock.patch(
        "paasta_tools.monitoring.check_chronos_has_jobs.load_chronos_config",
        autospec=True,
    ), mock.patch(
        "paasta_tools.monitoring.check_chronos_has_jobs.get_chronos_client",
        autospec=True,
    ):
        with pytest.raises(SystemExit) as error:
            check_chronos_jobs()
        out, err = capfd.readouterr()
        assert "CRITICAL" in out
        assert error.value.code == 2


def test_chronos_jobs_some_jobs(capfd):
    with mock.patch(
        # We expect this is tested properly elsewhere
        "paasta_tools.metrics.metastatus_lib.chronos_tools.filter_enabled_jobs",
        autospec=True,
        return_value=["foo", "bar"],
    ), mock.patch(
        "paasta_tools.monitoring.check_chronos_has_jobs.load_chronos_config",
        autospec=True,
    ), mock.patch(
        "paasta_tools.monitoring.check_chronos_has_jobs.get_chronos_client",
        autospec=True,
    ):
        with pytest.raises(SystemExit) as error:
            check_chronos_jobs()
        out, err = capfd.readouterr()
        assert "OK" in out
        assert "2" in out
        assert error.value.code == 0
