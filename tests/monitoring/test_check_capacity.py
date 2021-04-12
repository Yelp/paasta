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
from unittest.mock import sentinel

import pytest

from paasta_tools.monitoring.check_capacity import calc_percent_usage
from paasta_tools.monitoring.check_capacity import get_check_from_overrides
from paasta_tools.monitoring.check_capacity import run_capacity_check


overrides = [
    {
        "groupings": {"foo": "bar"},
        "crit": {"cpus": 90, "mem": 95, "disk": 99},
        "warn": {"cpus": 80, "mem": 85, "disk": 89},
    }
]

check_types = ["cpus", "mem", "disk"]


def test_calc_percent_usage():
    item = {
        "cpus": {"free": 10, "total": 20, "used": 10},
        "mem": {"free": 100, "total": 200, "used": 100},
        "disk": {"free": 1000, "total": 2000, "used": 1000},
    }

    for v in check_types:
        assert calc_percent_usage(item, v) == 50

    item = {"cpus": {"free": 0, "total": 0, "used": 0}}

    assert calc_percent_usage(item, "cpus") == 0


def test_get_check_from_overrides_default():
    default_check = sentinel.default
    groupings = {"foo": "baz"}

    assert (
        get_check_from_overrides(overrides, default_check, groupings) == default_check
    )


def test_get_check_from_overrides_override():
    default_check = sentinel.default
    groupings = {"foo": "bar"}

    assert get_check_from_overrides(overrides, default_check, groupings) == overrides[0]


def test_get_check_from_overrides_error():
    default_check = sentinel.default_check
    bad_overrides = overrides + [{"groupings": {"foo": "bar"}}]
    groupings = {"foo": "bar"}

    with pytest.raises(SystemExit) as error:
        get_check_from_overrides(bad_overrides, default_check, groupings)
    assert error.value.code == 3


def test_capacity_check_ok(capfd):
    mock_api_client = mock.MagicMock()
    mock_api_client.resources.resources.result.return_value.value = [
        {
            "groupings": {"foo", "baz"},
            "cpus": {"total": 2, "free": 1, "used": 1},
            "mem": {"total": 2, "free": 1, "used": 1},
            "disk": {"total": 2, "free": 1, "used": 1},
        }
    ]

    for t in check_types:
        options = mock.MagicMock()
        options.type = t
        options.overrides = None
        options.cluster = "fake_cluster"
        options.attributes = "foo"
        options.warn = 80
        options.crit = 90

        with mock.patch(
            "paasta_tools.monitoring.check_capacity.parse_capacity_check_options",
            autospec=True,
            return_value=options,
        ), mock.patch(
            "paasta_tools.monitoring.check_capacity.load_system_paasta_config",
            autospec=True,
        ), mock.patch(
            "paasta_tools.monitoring.check_capacity.get_paasta_oapi_client",
            autospec=True,
            return_value=mock_api_client,
        ):
            with pytest.raises(SystemExit) as error:
                run_capacity_check()
            out, err = capfd.readouterr()
            assert error.value.code == 0
            assert "OK" in out
            assert "fake_cluster" in out
            assert t in out


def test_capacity_check_warn(capfd):
    mock_api_client = mock.MagicMock()
    mock_api_client.resources.resources.return_value.value = [
        {
            "groupings": {"foo": "baz"},
            "cpus": {"total": 2, "free": 1, "used": 1},
            "mem": {"total": 2, "free": 1, "used": 1},
            "disk": {"total": 2, "free": 1, "used": 1},
        }
    ]

    for t in check_types:
        options = mock.MagicMock()
        options.type = t
        options.overrides = None
        options.cluster = "fake_cluster"
        options.attributes = "foo"
        options.warn = 45
        options.crit = 80

        with mock.patch(
            "paasta_tools.monitoring.check_capacity.parse_capacity_check_options",
            autospec=True,
            return_value=options,
        ), mock.patch(
            "paasta_tools.monitoring.check_capacity.load_system_paasta_config",
            autospec=True,
        ), mock.patch(
            "paasta_tools.monitoring.check_capacity.get_paasta_oapi_client",
            autospec=True,
            return_value=mock_api_client,
        ):
            with pytest.raises(SystemExit) as error:
                run_capacity_check()
            out, err = capfd.readouterr()
            assert error.value.code == 1, out
            assert "WARNING" in out
            assert "fake_cluster" in out
            assert t in out


def test_capacity_check_crit(capfd):
    mock_api_client = mock.MagicMock()
    mock_api_client.resources.resources.return_value.value = [
        {
            "groupings": {"foo": "baz"},
            "cpus": {"total": 2, "free": 1, "used": 1},
            "mem": {"total": 2, "free": 1, "used": 1},
            "disk": {"total": 2, "free": 1, "used": 1},
        }
    ]

    for t in check_types:
        options = mock.MagicMock()
        options.type = t
        options.overrides = None
        options.cluster = "fake_cluster"
        options.attributes = "foo"
        options.warn = 45
        options.crit = 49

        with mock.patch(
            "paasta_tools.monitoring.check_capacity.parse_capacity_check_options",
            autospec=True,
            return_value=options,
        ), mock.patch(
            "paasta_tools.monitoring.check_capacity.load_system_paasta_config",
            autospec=True,
        ), mock.patch(
            "paasta_tools.monitoring.check_capacity.get_paasta_oapi_client",
            autospec=True,
            return_value=mock_api_client,
        ):
            with pytest.raises(SystemExit) as error:
                run_capacity_check()
            out, err = capfd.readouterr()
            assert error.value.code == 2, out
            assert "CRITICAL" in out
            assert "fake_cluster" in out
            assert t in out


def test_capacity_check_overrides(capfd):
    mock_api_client = mock.MagicMock()
    mock_api_client.resources.resources.return_value.value = [
        {
            "groupings": {"foo": "bar"},
            "cpus": {"total": 2, "free": 1, "used": 1},
            "mem": {"total": 2, "free": 1, "used": 1},
            "disk": {"total": 2, "free": 1, "used": 1},
        },
        {
            "groupings": {"foo": "baz"},
            "cpus": {"total": 2, "free": 1, "used": 1},
            "mem": {"total": 2, "free": 1, "used": 1},
            "disk": {"total": 2, "free": 1, "used": 1},
        },
    ]

    mock_overrides = [
        {
            "groupings": {"foo": "bar"},
            "warn": {"cpus": 99, "mem": 99, "disk": 99},
            "crit": {"cpus": 10, "mem": 10, "disk": 10},
        }
    ]

    for t in check_types:
        options = mock.MagicMock()
        options.type = t
        options.overrides = "/fake/file.json"
        options.cluster = "fake_cluster"
        options.attributes = "foo"
        options.warn = 99
        options.crit = 99

        with mock.patch(
            "paasta_tools.monitoring.check_capacity.parse_capacity_check_options",
            autospec=True,
            return_value=options,
        ), mock.patch(
            "paasta_tools.monitoring.check_capacity.load_system_paasta_config",
            autospec=True,
        ), mock.patch(
            "paasta_tools.monitoring.check_capacity.get_paasta_oapi_client",
            autospec=True,
            return_value=mock_api_client,
        ), mock.patch(
            "paasta_tools.monitoring.check_capacity.read_overrides",
            autospec=True,
            return_value=mock_overrides,
        ):
            with pytest.raises(SystemExit) as error:
                run_capacity_check()
            out, err = capfd.readouterr()
            assert error.value.code == 2, out
            assert "CRITICAL" in out
            assert "fake_cluster" in out
            assert t in out
            assert "baz" not in out
