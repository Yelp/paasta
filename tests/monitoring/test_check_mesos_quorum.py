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

from paasta_tools.monitoring.check_mesos_quorum import check_mesos_quorum


def test_check_mesos_quorum_ok(capfd):
    with mock.patch(
        "paasta_tools.metrics.metastatus_lib.get_num_masters",
        autospec=True,
        return_value=3,
    ), mock.patch(
        "paasta_tools.metrics.metastatus_lib.get_mesos_quorum",
        autospec=True,
        return_value=2,
    ):
        with pytest.raises(SystemExit) as error:
            check_mesos_quorum()
        out, err = capfd.readouterr()
        assert "OK" in out
        assert error.value.code == 0


def test_check_mesos_quorum_critical(capfd):
    with mock.patch(
        "paasta_tools.metrics.metastatus_lib.get_num_masters",
        autospec=True,
        return_value=1,
    ), mock.patch(
        "paasta_tools.metrics.metastatus_lib.get_mesos_quorum",
        autospec=True,
        return_value=2,
    ):
        with pytest.raises(SystemExit) as error:
            check_mesos_quorum()
        out, err = capfd.readouterr()
        assert "CRITICAL" in out
        assert error.value.code == 2
