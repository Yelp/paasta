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
import sys

import mock
import pytest

from paasta_tools.monitoring.check_synapse_replication import check_replication
from paasta_tools.monitoring.check_synapse_replication import parse_range
from paasta_tools.monitoring.check_synapse_replication import run_synapse_check


def test_check_replication():
    """Codes and messages should conform to the nagios api and
    the specification of the check_replication function"""

    # replication < 2 => warn
    mock_warn_range = (2, 1000)
    # replication < 1 => critical
    mock_crit_range = (1, 1000)

    code, message = check_replication("foo", 0, mock_warn_range, mock_crit_range)
    assert code == 2 and "foo" in message

    code, message = check_replication("foo", 1, mock_warn_range, mock_crit_range)
    assert code == 1 and "foo" in message

    code, message = check_replication("bar", 2, mock_warn_range, mock_crit_range)
    assert code == 0 and "bar" in message


def test_parse_range():
    range_data = ["0:1", "1:", "100:", "20:30", ":2", ":200"]
    expected_ranges = [
        (0, 1),
        (1, sys.maxsize),
        (100, sys.maxsize),
        (20, 30),
        (0, 2),
        (0, 200),
    ]

    computed_ranges = [parse_range(x) for x in range_data]
    assert computed_ranges == expected_ranges


def test_run_synapse_check(system_paasta_config):
    module = "paasta_tools.monitoring.check_synapse_replication"
    parse_method = module + ".parse_synapse_check_options"
    replication_method = module + ".get_replication_for_services"
    check_replication_method = module + ".check_replication"

    mock_parse_options = mock.Mock()
    mock_parse_options.synapse_host_port = "foo"
    mock_parse_options.services = ["wat", "service", "is"]

    mock_replication = {"wat": 2, "service": 1, "is": 0}

    for return_value in range(0, 4):
        with mock.patch(
            parse_method, return_value=mock_parse_options, autospec=True
        ), mock.patch(
            replication_method, return_value=mock_replication, autospec=True
        ), mock.patch(
            check_replication_method,
            return_value=(return_value, "CHECK"),
            autospec=True,
        ), mock.patch(
            "paasta_tools.utils.load_system_paasta_config",
            autospec=True,
            return_value=system_paasta_config,
        ):
            with pytest.raises(SystemExit) as error:
                run_synapse_check()
            assert error.value.code == return_value

    # Mock check results for those services
    mock_service_status = {"wat": 0, "service": 1, "is": 2}

    def mock_check(name, replication, warn, crit):
        return mock_service_status[name], "CHECK"

    with mock.patch(
        parse_method, return_value=mock_parse_options, autospec=True
    ), mock.patch(
        replication_method, return_value=mock_replication, autospec=True
    ), mock.patch(
        check_replication_method, new=mock_check, autospec=None
    ), mock.patch(
        "paasta_tools.utils.load_system_paasta_config",
        autospec=True,
        return_value=system_paasta_config,
    ):
        with pytest.raises(SystemExit) as error:
            run_synapse_check()
        assert error.value.code == 2
