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
import argparse
import datetime
import json
from socket import gaierror

import mock
import pytest
from dateutil import tz
from requests.exceptions import HTTPError

from paasta_tools.mesos_maintenance import _make_request_payload
from paasta_tools.mesos_maintenance import are_hosts_forgotten_down
from paasta_tools.mesos_maintenance import are_hosts_forgotten_draining
from paasta_tools.mesos_maintenance import build_maintenance_payload
from paasta_tools.mesos_maintenance import build_maintenance_schedule_payload
from paasta_tools.mesos_maintenance import build_reservation_payload
from paasta_tools.mesos_maintenance import components_to_hosts
from paasta_tools.mesos_maintenance import datetime_seconds_from_now
from paasta_tools.mesos_maintenance import datetime_to_nanoseconds
from paasta_tools.mesos_maintenance import down
from paasta_tools.mesos_maintenance import drain
from paasta_tools.mesos_maintenance import friendly_status
from paasta_tools.mesos_maintenance import get_down_hosts
from paasta_tools.mesos_maintenance import get_draining_hosts
from paasta_tools.mesos_maintenance import get_hosts_forgotten_down
from paasta_tools.mesos_maintenance import get_hosts_forgotten_draining
from paasta_tools.mesos_maintenance import get_hosts_past_maintenance_end
from paasta_tools.mesos_maintenance import get_hosts_past_maintenance_start
from paasta_tools.mesos_maintenance import get_hosts_with_state
from paasta_tools.mesos_maintenance import get_machine_ids
from paasta_tools.mesos_maintenance import get_maintenance_schedule
from paasta_tools.mesos_maintenance import get_maintenance_status
from paasta_tools.mesos_maintenance import Hostname
from paasta_tools.mesos_maintenance import hostnames_to_components
from paasta_tools.mesos_maintenance import is_host_down
from paasta_tools.mesos_maintenance import is_host_drained
from paasta_tools.mesos_maintenance import is_host_draining
from paasta_tools.mesos_maintenance import is_host_past_maintenance_end
from paasta_tools.mesos_maintenance import is_host_past_maintenance_start
from paasta_tools.mesos_maintenance import load_credentials
from paasta_tools.mesos_maintenance import parse_datetime
from paasta_tools.mesos_maintenance import parse_timedelta
from paasta_tools.mesos_maintenance import raw_status
from paasta_tools.mesos_maintenance import reserve
from paasta_tools.mesos_maintenance import Resource
from paasta_tools.mesos_maintenance import schedule
from paasta_tools.mesos_maintenance import seconds_to_nanoseconds
from paasta_tools.mesos_maintenance import status
from paasta_tools.mesos_maintenance import undrain
from paasta_tools.mesos_maintenance import unreserve
from paasta_tools.mesos_maintenance import up


def test_parse_timedelta_none():
    with pytest.raises(argparse.ArgumentTypeError):
        parse_timedelta(value=None)


def test_parse_timedelta_invalid():
    with pytest.raises(argparse.ArgumentTypeError):
        parse_timedelta(value="fake value")


def test_parse_timedelta():
    assert parse_timedelta(value="1 hour") == 3600 * 1000000000


def test_parse_datetime_none():
    with pytest.raises(argparse.ArgumentTypeError):
        parse_datetime(value=None)


def test_parse_datetime_invalid():
    with pytest.raises(argparse.ArgumentTypeError):
        parse_datetime(value="fake value")


def test_parse_datetime():
    assert parse_datetime("November 11, 2011 11:11:11Z") == 1321009871000000000


@mock.patch("paasta_tools.mesos_maintenance.now", autospec=True)
def test_datetime_seconds_from_now(
    mock_now,
):
    mock_now.return_value = datetime.datetime(
        2016, 4, 16, 0, 23, 25, 157145, tzinfo=tz.tzutc()
    )
    expected = datetime.datetime(2016, 4, 16, 0, 23, 40, 157145, tzinfo=tz.tzutc())
    assert datetime_seconds_from_now(15) == expected


def test_seconds_to_nanoseconds():
    assert seconds_to_nanoseconds(60) == 60 * 1000000000


def test_datetime_to_nanoseconds():
    dt = datetime.datetime(2016, 4, 16, 0, 23, 40, 157145, tzinfo=tz.tzutc())
    expected = 1460766220000000000
    assert datetime_to_nanoseconds(dt) == expected


@mock.patch("paasta_tools.mesos_maintenance.gethostbyname", autospec=True)
def test_build_maintenance_payload(
    mock_gethostbyname,
):
    ip = "169.254.121.212"
    mock_gethostbyname.return_value = ip
    hostname = "fqdn1.example.org"
    hostnames = [hostname]

    assert build_maintenance_payload(hostnames, "start_maintenance")[
        "start_maintenance"
    ]["machines"] == get_machine_ids(hostnames)


@mock.patch("paasta_tools.mesos_maintenance.gethostbyname", autospec=True)
def test_get_machine_ids_one_host(
    mock_gethostbyname,
):
    ip = "169.254.121.212"
    mock_gethostbyname.return_value = ip
    hostname = "fqdn1.example.org"
    hostnames = [hostname]
    expected = [{"hostname": hostname, "ip": ip}]
    assert get_machine_ids(hostnames) == expected


@mock.patch("paasta_tools.mesos_maintenance.gethostbyname", autospec=True)
def test_get_machine_ids_multiple_hosts(
    mock_gethostbyname,
):
    ip1 = "169.254.121.212"
    ip2 = "169.254.121.213"
    ip3 = "169.254.121.214"
    mock_gethostbyname.side_effect = [ip1, ip2, ip3]
    hostname1 = "fqdn1.example.org"
    hostname2 = "fqdn2.example.org"
    hostname3 = "fqdn3.example.org"
    hostnames = [hostname1, hostname2, hostname3]
    expected = [
        {"hostname": hostname1, "ip": ip1},
        {"hostname": hostname2, "ip": ip2},
        {"hostname": hostname3, "ip": ip3},
    ]
    assert get_machine_ids(hostnames) == expected


def test_get_machine_ids_multiple_hosts_ips():
    ip1 = "169.254.121.212"
    ip2 = "169.254.121.213"
    ip3 = "169.254.121.214"
    hostname1 = "fqdn1.example.org"
    hostname2 = "fqdn2.example.org"
    hostname3 = "fqdn3.example.org"
    hostnames = [hostname1 + "|" + ip1, hostname2 + "|" + ip2, hostname3 + "|" + ip3]
    expected = [
        {"hostname": hostname1, "ip": ip1},
        {"hostname": hostname2, "ip": ip2},
        {"hostname": hostname3, "ip": ip3},
    ]
    assert get_machine_ids(hostnames) == expected


@mock.patch("paasta_tools.mesos_maintenance.get_maintenance_schedule", autospec=True)
@mock.patch("paasta_tools.mesos_maintenance.get_machine_ids", autospec=True)
def test_build_maintenance_schedule_payload_no_schedule(
    mock_get_machine_ids, mock_get_maintenance_schedule
):
    mock_get_maintenance_schedule.return_value.json.return_value = {
        "get_maintenance_schedule": {"schedule": {}}
    }
    machine_ids = [{"hostname": "machine2", "ip": "10.0.0.2"}]
    mock_get_machine_ids.return_value = machine_ids
    hostnames = ["fake-hostname"]
    start = "1443830400000000000"
    duration = "3600000000000"
    actual = build_maintenance_schedule_payload(hostnames, start, duration, drain=True)
    assert mock_get_maintenance_schedule.call_count == 1
    assert mock_get_machine_ids.call_count == 1
    assert mock_get_machine_ids.call_args == mock.call(hostnames)
    expected = {
        "type": "UPDATE_MAINTENANCE_SCHEDULE",
        "update_maintenance_schedule": {
            "schedule": {
                "windows": [
                    {
                        "machine_ids": machine_ids,
                        "unavailability": {
                            "start": {"nanoseconds": int(start)},
                            "duration": {"nanoseconds": int(duration)},
                        },
                    }
                ]
            }
        },
    }
    assert actual == expected


@mock.patch("paasta_tools.mesos_maintenance.get_maintenance_schedule", autospec=True)
@mock.patch("paasta_tools.mesos_maintenance.get_machine_ids", autospec=True)
def test_build_maintenance_schedule_payload_no_schedule_undrain(
    mock_get_machine_ids, mock_get_maintenance_schedule
):
    mock_get_maintenance_schedule.return_value.json.return_value = {
        "get_maintenance_schedule": {"schedule": {}}
    }
    machine_ids = [{"hostname": "machine2", "ip": "10.0.0.2"}]
    mock_get_machine_ids.return_value = machine_ids
    hostnames = ["fake-hostname"]
    start = "1443830400000000000"
    duration = "3600000000000"
    actual = build_maintenance_schedule_payload(hostnames, start, duration, drain=False)
    assert mock_get_maintenance_schedule.call_count == 1
    assert mock_get_machine_ids.call_count == 1
    assert mock_get_machine_ids.call_args == mock.call(hostnames)
    expected = {
        "type": "UPDATE_MAINTENANCE_SCHEDULE",
        "update_maintenance_schedule": {"schedule": {"windows": []}},
    }
    assert actual == expected


@mock.patch("paasta_tools.mesos_maintenance.load_credentials", autospec=True)
def test_build_reservation_payload(
    mock_load_credentials,
):
    fake_username = "username"
    mock_load_credentials.return_value = mock.MagicMock(
        principal=fake_username, secret="password"
    )
    resource = "cpus"
    amount = 42
    resources = [Resource(name=resource, amount=amount)]
    actual = build_reservation_payload(resources)
    expected = [
        {
            "name": resource,
            "type": "SCALAR",
            "scalar": {"value": amount},
            "role": "maintenance",
            "reservation": {"principal": fake_username},
        }
    ]
    assert actual == expected


def test_make_request_payload():
    ret = _make_request_payload("slave_id", {"name": "res+ource"})
    assert ret == {"slaveId": b"slave_id", "resources": b'{"name": "res%20ource"}'}
    assert type(ret["slaveId"]) is bytes
    assert type(ret["resources"]) is bytes


@mock.patch("paasta_tools.mesos_maintenance.get_maintenance_schedule", autospec=True)
@mock.patch("paasta_tools.mesos_maintenance.get_machine_ids", autospec=True)
def test_build_maintenance_schedule_payload_schedule(
    mock_get_machine_ids, mock_get_maintenance_schedule
):
    mock_get_maintenance_schedule.return_value.json.return_value = {
        "type": "GET_MAINTENANCE_SCHEDULE",
        "get_maintenance_schedule": {
            "schedule": {
                "windows": [
                    {
                        "machine_ids": [
                            {"hostname": "machine1", "ip": "10.0.0.1"},
                            {"hostname": "machine2", "ip": "10.0.0.2"},
                        ],
                        "unavailability": {
                            "start": {"nanoseconds": 1443830400000000000},
                            "duration": {"nanoseconds": 3600000000000},
                        },
                    },
                    {
                        "machine_ids": [{"hostname": "machine3", "ip": "10.0.0.3"}],
                        "unavailability": {
                            "start": {"nanoseconds": 1443834000000000000},
                            "duration": {"nanoseconds": 3600000000000},
                        },
                    },
                ]
            }
        },
    }
    machine_ids = [{"hostname": "machine2", "ip": "10.0.0.2"}]
    mock_get_machine_ids.return_value = machine_ids
    hostnames = ["machine2"]
    start = "1443830400000000000"
    duration = "3600000000000"
    actual = build_maintenance_schedule_payload(hostnames, start, duration, drain=True)
    assert mock_get_maintenance_schedule.call_count == 1
    assert mock_get_machine_ids.call_count == 1
    assert mock_get_machine_ids.call_args == mock.call(hostnames)
    expected = {
        "type": "UPDATE_MAINTENANCE_SCHEDULE",
        "update_maintenance_schedule": {
            "schedule": {
                "windows": [
                    {
                        "machine_ids": [{"hostname": "machine1", "ip": "10.0.0.1"}],
                        "unavailability": {
                            "start": {"nanoseconds": 1443830400000000000},
                            "duration": {"nanoseconds": 3600000000000},
                        },
                    },
                    {
                        "machine_ids": [{"hostname": "machine3", "ip": "10.0.0.3"}],
                        "unavailability": {
                            "start": {"nanoseconds": 1443834000000000000},
                            "duration": {"nanoseconds": 3600000000000},
                        },
                    },
                    {
                        "machine_ids": machine_ids,
                        "unavailability": {
                            "start": {"nanoseconds": int(start)},
                            "duration": {"nanoseconds": int(duration)},
                        },
                    },
                ]
            }
        },
    }
    assert actual == expected


@mock.patch("paasta_tools.mesos_maintenance.get_maintenance_schedule", autospec=True)
@mock.patch("paasta_tools.mesos_maintenance.get_machine_ids", autospec=True)
def test_build_maintenance_schedule_payload_schedule_undrain(
    mock_get_machine_ids, mock_get_maintenance_schedule
):
    mock_get_maintenance_schedule.return_value.json.return_value = {
        "type": "GET_MAINTENANCE_SCHEDULE",
        "get_maintenance_schedule": {
            "schedule": {
                "windows": [
                    {
                        "machine_ids": [
                            {"hostname": "machine1", "ip": "10.0.0.1"},
                            {"hostname": "machine2", "ip": "10.0.0.2"},
                        ],
                        "unavailability": {
                            "start": {"nanoseconds": 1443830400000000000},
                            "duration": {"nanoseconds": 3600000000000},
                        },
                    },
                    {
                        "machine_ids": [{"hostname": "machine3", "ip": "10.0.0.3"}],
                        "unavailability": {
                            "start": {"nanoseconds": 1443834000000000000},
                            "duration": {"nanoseconds": 3600000000000},
                        },
                    },
                ]
            }
        },
    }
    machine_ids = [{"hostname": "machine2", "ip": "10.0.0.2"}]
    mock_get_machine_ids.return_value = machine_ids
    hostnames = ["machine2"]
    start = "1443830400000000000"
    duration = "3600000000000"
    actual = build_maintenance_schedule_payload(hostnames, start, duration, drain=False)
    assert mock_get_maintenance_schedule.call_count == 1
    assert mock_get_machine_ids.call_count == 1
    assert mock_get_machine_ids.call_args == mock.call(hostnames)
    expected = {
        "type": "UPDATE_MAINTENANCE_SCHEDULE",
        "update_maintenance_schedule": {
            "schedule": {
                "windows": [
                    {
                        "machine_ids": [{"hostname": "machine1", "ip": "10.0.0.1"}],
                        "unavailability": {
                            "start": {"nanoseconds": 1443830400000000000},
                            "duration": {"nanoseconds": 3600000000000},
                        },
                    },
                    {
                        "machine_ids": [{"hostname": "machine3", "ip": "10.0.0.3"}],
                        "unavailability": {
                            "start": {"nanoseconds": 1443834000000000000},
                            "duration": {"nanoseconds": 3600000000000},
                        },
                    },
                ]
            }
        },
    }
    assert actual == expected


@mock.patch("paasta_tools.mesos_maintenance.open", create=True, autospec=None)
def test_load_credentials(
    mock_open,
):
    principal = "username"
    secret = "password"
    credentials = {"principal": principal, "secret": secret}

    mock_open.side_effect = mock.mock_open(read_data=json.dumps(credentials))

    credentials = load_credentials()

    assert credentials.principal == principal
    assert credentials.secret == secret


@mock.patch(
    "paasta_tools.mesos_maintenance.open",
    create=True,
    side_effect=IOError,
    autospec=None,
)
def test_load_credentials_missing_file(
    mock_open,
):
    with pytest.raises(IOError):
        assert load_credentials()


@mock.patch("paasta_tools.mesos_maintenance.open", create=True, autospec=None)
def test_load_credentials_keyerror(
    mock_open,
):
    credentials = {}

    mock_open.side_effect = mock.mock_open(read_data=json.dumps(credentials))

    with pytest.raises(KeyError):
        assert load_credentials()


@mock.patch("paasta_tools.mesos_maintenance.operator_api", autospec=True)
def test_get_maintenance_status(
    mock_operator_api,
):
    get_maintenance_status()
    assert mock_operator_api.call_count == 1
    assert mock_operator_api.return_value.call_count == 1
    assert mock_operator_api.return_value.call_args == mock.call(
        data={"type": "GET_MAINTENANCE_STATUS"}
    )


@mock.patch("paasta_tools.mesos_maintenance.operator_api", autospec=True)
def test_get_maintenance_schedule(
    mock_operator_api,
):
    get_maintenance_schedule()
    assert mock_operator_api.call_count == 1
    assert mock_operator_api.return_value.call_count == 1
    assert mock_operator_api.return_value.call_args == mock.call(
        data={"type": "GET_MAINTENANCE_SCHEDULE"}
    )


@mock.patch("paasta_tools.mesos_maintenance.operator_api", autospec=True)
@mock.patch(
    "paasta_tools.mesos_maintenance.build_maintenance_schedule_payload", autospec=True
)
@mock.patch("paasta_tools.mesos_maintenance.reserve_all_resources", autospec=True)
def test_drain(
    mock_reserve_all_resources,
    mock_build_maintenance_schedule_payload,
    mock_operator_api,
):
    fake_schedule = {"fake_schedule": "fake_value"}
    mock_build_maintenance_schedule_payload.return_value = fake_schedule
    drain(hostnames=["some-host"], start="some-start", duration="some-duration")

    assert mock_build_maintenance_schedule_payload.call_count == 1
    expected_args = mock.call(["some-host"], "some-start", "some-duration", drain=True)
    assert mock_build_maintenance_schedule_payload.call_args == expected_args

    assert mock_reserve_all_resources.call_count == 1
    expected_args = mock.call(["some-host"])
    assert mock_reserve_all_resources.call_args == expected_args

    assert mock_operator_api.call_count == 1
    assert mock_operator_api.return_value.call_count == 1
    expected_args = mock.call(data=fake_schedule)
    assert mock_operator_api.return_value.call_args == expected_args

    mock_reserve_all_resources.side_effect = HTTPError()
    drain(hostnames=["some-host"], start="some-start", duration="some-duration")
    assert mock_operator_api.call_count == 2

    mock_reserve_all_resources.reset_mock()
    mock_operator_api.reset_mock()
    drain(
        hostnames=["some-host"],
        start="some-start",
        duration="some-duration",
        reserve_resources=False,
    )
    assert mock_reserve_all_resources.call_count == 0
    assert mock_operator_api.return_value.call_count == 1


@mock.patch("paasta_tools.mesos_maintenance.operator_api", autospec=True)
@mock.patch(
    "paasta_tools.mesos_maintenance.build_maintenance_schedule_payload", autospec=True
)
@mock.patch("paasta_tools.mesos_maintenance.unreserve_all_resources", autospec=True)
def test_undrain(
    mock_unreserve_all_resources,
    mock_build_maintenance_schedule_payload,
    mock_operator_api,
):
    fake_schedule = {"fake_schedule": "fake_value"}
    mock_build_maintenance_schedule_payload.return_value = fake_schedule
    undrain(hostnames=["some-host"])

    assert mock_build_maintenance_schedule_payload.call_count == 1
    expected_args = mock.call(["some-host"], drain=False)
    assert mock_build_maintenance_schedule_payload.call_args == expected_args

    assert mock_unreserve_all_resources.call_count == 1
    expected_args = mock.call(["some-host"])
    assert mock_unreserve_all_resources.call_args == expected_args

    assert mock_operator_api.call_count == 1
    assert mock_operator_api.return_value.call_count == 1
    expected_args = mock.call(data=fake_schedule)
    assert mock_operator_api.return_value.call_args == expected_args

    mock_unreserve_all_resources.side_effect = HTTPError()
    undrain(hostnames=["some-host"])
    assert mock_operator_api.call_count == 2

    mock_operator_api.reset_mock()
    mock_unreserve_all_resources.reset_mock()
    undrain(hostnames=["some-host"], unreserve_resources=False)
    assert mock_operator_api.call_count == 1
    assert mock_unreserve_all_resources.call_count == 0


@mock.patch(
    "paasta_tools.mesos_maintenance.build_reservation_payload",
    autospec=True,
    return_value={"name": "payload"},
)
@mock.patch("paasta_tools.mesos_maintenance.operator_api", autospec=True)
def test_reserve(mock_operator_api, mock_build_reservation_payload):
    fake_slave_id = "fake-id"
    fake_resource = "cpus"
    fake_amount = 42
    resources = [Resource(name=fake_resource, amount=fake_amount)]
    reserve(fake_slave_id, resources)
    assert mock_build_reservation_payload.call_count == 1
    expected_args = mock.call(resources)
    assert mock_build_reservation_payload.call_args == expected_args

    assert mock_operator_api.call_count == 1
    assert mock_operator_api.return_value.call_count == 1


@mock.patch(
    "paasta_tools.mesos_maintenance.build_reservation_payload",
    autospec=True,
    return_value={"name": "payload"},
)
@mock.patch("paasta_tools.mesos_maintenance.operator_api", autospec=True)
def test_unreserve(mock_operator_api, mock_build_reservation_payload):
    fake_slave_id = "fake-id"
    fake_resource = "cpus"
    fake_amount = 42
    resources = [Resource(name=fake_resource, amount=fake_amount)]
    unreserve(fake_slave_id, resources)
    assert mock_build_reservation_payload.call_count == 1
    expected_args = mock.call(resources)
    assert mock_build_reservation_payload.call_args == expected_args

    assert mock_operator_api.call_count == 1
    assert mock_operator_api.return_value.call_count == 1


@mock.patch("paasta_tools.mesos_maintenance.operator_api", autospec=True)
@mock.patch("paasta_tools.mesos_maintenance.build_maintenance_payload", autospec=True)
def test_down(mock_build_maintenance_payload, mock_operator_api):
    fake_payload = [{"fake_schedule": "fake_value"}]
    mock_build_maintenance_payload.return_value = fake_payload
    down(hostnames=["some-host"])
    assert mock_build_maintenance_payload.call_count == 1
    assert mock_build_maintenance_payload.call_args == mock.call(
        ["some-host"], "start_maintenance"
    )
    assert mock_operator_api.call_count == 1
    assert mock_operator_api.return_value.call_count == 1
    expected_args = mock.call(data=fake_payload)
    assert mock_operator_api.return_value.call_args == expected_args


@mock.patch("paasta_tools.mesos_maintenance.operator_api", autospec=True)
@mock.patch("paasta_tools.mesos_maintenance.build_maintenance_payload", autospec=True)
def test_up(mock_build_maintenance_payload, mock_operator_api):
    fake_payload = [{"fake_schedule": "fake_value"}]
    mock_build_maintenance_payload.return_value = fake_payload
    up(hostnames=["some-host"])
    assert mock_build_maintenance_payload.call_count == 1
    assert mock_build_maintenance_payload.call_args == mock.call(
        ["some-host"], "stop_maintenance"
    )
    assert mock_operator_api.call_count == 1
    assert mock_operator_api.return_value.call_count == 1
    expected_args = mock.call(data=fake_payload)
    assert mock_operator_api.return_value.call_args == expected_args


@mock.patch("paasta_tools.mesos_maintenance.get_maintenance_status", autospec=True)
def test_raw_status(
    mock_get_maintenance_status,
):
    raw_status()
    assert mock_get_maintenance_status.call_count == 1


@mock.patch("paasta_tools.mesos_maintenance.raw_status", autospec=True)
def test_status(
    mock_raw_status,
):
    status()
    assert mock_raw_status.call_count == 1


@mock.patch("paasta_tools.mesos_maintenance.raw_status", autospec=True)
def test_friendly_status(
    mock_raw_status,
):
    friendly_status()
    assert mock_raw_status.call_count == 1


@mock.patch("paasta_tools.mesos_maintenance.get_maintenance_schedule", autospec=True)
def test_schedule(
    mock_get_maintenance_schedule,
):
    schedule()
    assert mock_get_maintenance_schedule.call_count == 1


@mock.patch("paasta_tools.mesos_maintenance.get_mesos_config_path", autospec=True)
@mock.patch("paasta_tools.mesos_maintenance.get_maintenance_status", autospec=True)
def test_get_hosts_with_state_none(
    mock_get_maintenance_status, mock_get_mesos_config_path
):
    mock_get_mesos_config_path.return_value = "/dev/null"
    fake_status = {"get_maintenance_status": {"status": {}}}
    mock_get_maintenance_status.return_value = mock.Mock()
    mock_get_maintenance_status.return_value.json.return_value = fake_status
    assert get_hosts_with_state(state="fake_state") == []


@mock.patch("paasta_tools.mesos_maintenance.get_mesos_config_path", autospec=True)
@mock.patch("paasta_tools.mesos_maintenance.get_maintenance_status", autospec=True)
def test_get_hosts_with_state_draining(
    mock_get_maintenance_status, mock_get_mesos_config_path
):
    fake_status = {
        "type": "GET_MAINTENANCE_STATUS",
        "get_maintenance_status": {
            "status": {
                "draining_machines": [
                    {"hostname": "fake-host1.fakesite.something", "ip": "0.0.0.0"},
                    {"hostname": "fake-host2.fakesite.something", "ip": "0.0.0.1"},
                ]
            }
        },
    }
    mock_get_maintenance_status.return_value = mock.Mock()
    mock_get_maintenance_status.return_value.json.return_value = fake_status
    expected = sorted(
        ["fake-host1.fakesite.something", "fake-host2.fakesite.something"]
    )
    assert sorted(get_hosts_with_state(state="draining_machines")) == expected


@mock.patch("paasta_tools.mesos_maintenance.get_mesos_config_path", autospec=True)
@mock.patch("paasta_tools.mesos_maintenance.get_maintenance_status", autospec=True)
def test_get_hosts_with_state_down(
    mock_get_maintenance_status, mock_get_mesos_config_path
):
    fake_status = {
        "type": "GET_MAINTENANCE_STATUS",
        "get_maintenance_status": {
            "status": {
                "down_machines": [
                    {"hostname": "fake-host1.fakesite.something", "ip": "0.0.0.0"},
                    {"hostname": "fake-host2.fakesite.something", "ip": "0.0.0.1"},
                ]
            }
        },
    }
    mock_get_maintenance_status.return_value = mock.Mock()
    mock_get_maintenance_status.return_value.json.return_value = fake_status
    expected = sorted(
        ["fake-host1.fakesite.something", "fake-host2.fakesite.something"]
    )
    assert sorted(get_hosts_with_state(state="down_machines")) == expected


@mock.patch("paasta_tools.mesos_maintenance.get_hosts_with_state", autospec=True)
def test_get_draining_hosts(
    mock_get_hosts_with_state,
):
    get_draining_hosts()
    assert mock_get_hosts_with_state.call_count == 1
    expected_args = mock.call(state="draining_machines", system_paasta_config=None)
    assert mock_get_hosts_with_state.call_args == expected_args


@mock.patch("paasta_tools.mesos_maintenance.get_hosts_with_state", autospec=True)
def test_get_down_hosts(
    mock_get_hosts_with_state,
):
    get_down_hosts()
    assert mock_get_hosts_with_state.call_count == 1
    expected_args = mock.call(state="down_machines")
    assert mock_get_hosts_with_state.call_args == expected_args


@mock.patch("paasta_tools.mesos_maintenance.get_draining_hosts", autospec=True)
def test_is_host_draining(
    mock_get_draining_hosts,
):
    mock_get_draining_hosts.return_value = [
        "fake-host1.fakesite.something",
        "fake-host2.fakesite.something",
    ]
    assert is_host_draining("fake-host1.fakesite.something")
    assert not is_host_draining("fake-host3.fakesite.something")


@mock.patch("paasta_tools.mesos_maintenance.get_down_hosts", autospec=True)
def test_is_host_down(
    mock_get_down_hosts,
):
    mock_get_down_hosts.return_value = [
        "fake-host1.fakesite.something",
        "fake-host2.fakesite.something",
    ]
    assert is_host_down("fake-host1.fakesite.something")
    assert not is_host_down("fake-host3.fakesite.something")


def sideeffect_mock_get_count_running_tasks_on_slave(hostname):
    if hostname == "host1":
        return 3
    else:
        return 0


@mock.patch(
    "paasta_tools.mesos_maintenance.get_count_running_tasks_on_slave", autospec=True
)
@mock.patch("paasta_tools.mesos_maintenance.is_host_draining", autospec=True)
def test_is_host_drained(mock_is_host_draining, mock_get_count_running_tasks_on_slave):
    mock_get_count_running_tasks_on_slave.side_effect = (
        sideeffect_mock_get_count_running_tasks_on_slave
    )
    mock_is_host_draining.return_value = True

    assert not is_host_drained("host1")
    mock_get_count_running_tasks_on_slave.assert_called_with("host1")
    mock_is_host_draining.assert_called_with(hostname="host1")

    mock_is_host_draining.return_value = True
    assert is_host_drained("host2")

    mock_is_host_draining.return_value = False
    assert not is_host_drained("host1")

    mock_is_host_draining.return_value = False
    assert not is_host_drained("host2")

    assert not is_host_drained("host3")

    mock_is_host_draining.return_value = True
    assert is_host_drained("host3")


@mock.patch("paasta_tools.mesos_maintenance.get_maintenance_schedule", autospec=True)
@mock.patch("paasta_tools.mesos_maintenance.datetime_to_nanoseconds", autospec=True)
def test_get_hosts_past_maintenance_start(
    mock_datetime_to_nanoseconds, mock_get_maintenance_schedule
):
    mock_schedule = {
        "type": "GET_MAINTENANCE_SCHEDULE",
        "get_maintenance_schedule": {
            "schedule": {
                "windows": [
                    {
                        "machine_ids": [{"hostname": "host3"}],
                        "unavailability": {"start": {"nanoseconds": 10}},
                    },
                    {
                        "machine_ids": [{"hostname": "host2"}],
                        "unavailability": {"start": {"nanoseconds": 5}},
                    },
                ]
            }
        },
    }
    mock_maintenance_dict = mock.Mock(return_value=mock_schedule)
    mock_get_maintenance_schedule.return_value = mock.Mock(json=mock_maintenance_dict)
    mock_datetime_to_nanoseconds.return_value = 7
    ret = get_hosts_past_maintenance_start()
    assert ret == ["host2"]

    mock_schedule = {
        "type": "GET_MAINTENANCE_SCHEDULE",
        "get_maintenance_schedule": {"schedule": {}},
    }
    mock_maintenance_dict = mock.Mock(return_value=mock_schedule)
    mock_get_maintenance_schedule.return_value = mock.Mock(json=mock_maintenance_dict)
    ret = get_hosts_past_maintenance_start()
    assert ret == []


@mock.patch("paasta_tools.mesos_maintenance.get_maintenance_schedule", autospec=True)
@mock.patch("paasta_tools.mesos_maintenance.datetime_to_nanoseconds", autospec=True)
def test_get_hosts_past_maintenance_start_grace(
    mock_datetime_to_nanoseconds, mock_get_maintenance_schedule
):
    mock_schedule = {
        "type": "GET_MAINTENANCE_SCHEDULE",
        "get_maintenance_schedule": {
            "schedule": {
                "windows": [
                    {
                        "machine_ids": [{"hostname": "host3"}],
                        "unavailability": {"start": {"nanoseconds": 10}},
                    },
                    {
                        "machine_ids": [{"hostname": "host2"}],
                        "unavailability": {"start": {"nanoseconds": 5}},
                    },
                ]
            }
        },
    }
    mock_maintenance_dict = mock.Mock(return_value=mock_schedule)
    mock_get_maintenance_schedule.return_value = mock.Mock(json=mock_maintenance_dict)
    mock_datetime_to_nanoseconds.return_value = 7
    ret = get_hosts_past_maintenance_start(grace=1)
    assert ret == ["host2"]

    ret = get_hosts_past_maintenance_start(grace=5)
    assert ret == []


@mock.patch("paasta_tools.mesos_maintenance.get_maintenance_schedule", autospec=True)
@mock.patch("paasta_tools.mesos_maintenance.datetime_to_nanoseconds", autospec=True)
def test_get_hosts_past_maintenance_end(
    mock_datetime_to_nanoseconds, mock_get_maintenance_schedule
):
    mock_schedule = {
        "type": "GET_MAINTENANCE_SCHEDULE",
        "get_maintenance_schedule": {
            "schedule": {
                "windows": [
                    {
                        "machine_ids": [{"hostname": "host3"}],
                        "unavailability": {
                            "start": {"nanoseconds": 10},
                            "duration": {"nanoseconds": 20},
                        },
                    },
                    {
                        "machine_ids": [{"hostname": "host2"}],
                        "unavailability": {
                            "start": {"nanoseconds": 5},
                            "duration": {"nanoseconds": 10},
                        },
                    },
                ]
            }
        },
    }
    mock_maintenance_dict = mock.Mock(return_value=mock_schedule)
    mock_get_maintenance_schedule.return_value = mock.Mock(json=mock_maintenance_dict)
    mock_datetime_to_nanoseconds.return_value = 19
    actual = get_hosts_past_maintenance_end()
    assert actual == ["host2"]

    mock_schedule = {
        "type": "GET_MAINTENANCE_SCHEDULE",
        "get_maintenance_schedule": {"schedule": {}},
    }
    mock_maintenance_dict = mock.Mock(return_value=mock_schedule)
    mock_get_maintenance_schedule.return_value = mock.Mock(json=mock_maintenance_dict)
    mock_datetime_to_nanoseconds.return_value = 19
    actual = get_hosts_past_maintenance_end()
    assert actual == []


@mock.patch("paasta_tools.mesos_maintenance.get_maintenance_schedule", autospec=True)
@mock.patch("paasta_tools.mesos_maintenance.datetime_to_nanoseconds", autospec=True)
def test_get_hosts_past_maintenance_end_grace(
    mock_datetime_to_nanoseconds, mock_get_maintenance_schedule
):
    mock_schedule = {
        "type": "GET_MAINTENANCE_SCHEDULE",
        "get_maintenance_schedule": {
            "schedule": {
                "windows": [
                    {
                        "machine_ids": [{"hostname": "host3"}],
                        "unavailability": {
                            "start": {"nanoseconds": 10},
                            "duration": {"nanoseconds": 20},
                        },
                    },
                    {
                        "machine_ids": [{"hostname": "host2"}],
                        "unavailability": {
                            "start": {"nanoseconds": 5},
                            "duration": {"nanoseconds": 10},
                        },
                    },
                ]
            }
        },
    }
    mock_maintenance_dict = mock.Mock(return_value=mock_schedule)
    mock_get_maintenance_schedule.return_value = mock.Mock(json=mock_maintenance_dict)
    mock_datetime_to_nanoseconds.return_value = 19
    actual = get_hosts_past_maintenance_end(grace=2)
    assert actual == ["host2"]

    actual = get_hosts_past_maintenance_end(grace=5)
    assert actual == []


@mock.patch(
    "paasta_tools.mesos_maintenance.get_hosts_past_maintenance_start", autospec=True
)
def test_is_host_past_maintenance_start(
    mock_get_hosts_past_maintenance_start,
):
    mock_get_hosts_past_maintenance_start.return_value = ["fake_host"]
    assert is_host_past_maintenance_start("fake_host")
    assert not is_host_past_maintenance_start("fake_host2")


@mock.patch(
    "paasta_tools.mesos_maintenance.get_hosts_past_maintenance_end", autospec=True
)
def test_is_host_past_maintenance_end(
    mock_get_hosts_past_maintenance_end,
):
    mock_get_hosts_past_maintenance_end.return_value = ["fake_host"]
    assert is_host_past_maintenance_end("fake_host")
    assert not is_host_past_maintenance_end("fake_host2")


def test_hostnames_to_components_simple():
    hostname = "fake-host"
    ip = None
    expected = [Hostname(host=hostname, ip=ip)]
    actual = hostnames_to_components([hostname])
    assert actual == expected


def test_hostnames_to_components_pipe():
    hostname = "fake-host"
    ip = "127.0.0.1"
    expected = [Hostname(host=hostname, ip=ip)]
    actual = hostnames_to_components([f"{hostname}|{ip}"])
    assert actual == expected


@mock.patch("paasta_tools.mesos_maintenance.gethostbyname", autospec=True)
def test_hostnames_to_components_resolve(
    mock_gethostbyname,
):
    hostname = "fake-host"
    ip = "127.0.0.1"
    mock_gethostbyname.return_value = ip
    expected = [Hostname(host=hostname, ip=ip)]
    actual = hostnames_to_components([hostname], resolve=True)
    assert actual == expected


@mock.patch("paasta_tools.mesos_maintenance.gethostbyname", autospec=True)
def test_hostnames_to_components_resolve_failure(
    mock_gethostbyname,
):
    mock_gethostbyname.side_effect = ["10.1.1.1", gaierror]
    expected = [Hostname(host="host1", ip="10.1.1.1")]
    actual = hostnames_to_components(["host1", "host2"], resolve=True)
    assert actual == expected


def test_components_to_hosts():
    host = "fake-host"
    ip = "127.0.0.1"
    expected = [host]
    actual = components_to_hosts([Hostname(host=host, ip=ip)])
    assert actual == expected


@mock.patch("paasta_tools.mesos_maintenance.get_draining_hosts", autospec=True)
@mock.patch(
    "paasta_tools.mesos_maintenance.get_hosts_past_maintenance_start", autospec=True
)
def test_get_hosts_forgotten_draining(
    mock_get_hosts_past_maintenance_start, mock_get_draining_hosts
):
    mock_get_draining_hosts.return_value = ["fake-host1", "fake-host2"]
    mock_get_hosts_past_maintenance_start.return_value = ["fake-host2"]
    assert get_hosts_forgotten_draining() == ["fake-host2"]


@mock.patch(
    "paasta_tools.mesos_maintenance.get_hosts_forgotten_draining", autospec=True
)
def test_are_hosts_forgotten_draining(
    mock_get_hosts_forgotten_draining,
):
    mock_get_hosts_forgotten_draining.return_value = ["fake-host"]
    assert are_hosts_forgotten_draining()

    mock_get_hosts_forgotten_draining.return_value = []
    assert not are_hosts_forgotten_draining()


@mock.patch("paasta_tools.mesos_maintenance.get_down_hosts", autospec=True)
@mock.patch(
    "paasta_tools.mesos_maintenance.get_hosts_past_maintenance_end", autospec=True
)
def test_get_hosts_forgotten_down(
    mock_get_hosts_past_maintenance_end, mock_get_down_hosts
):
    mock_get_down_hosts.return_value = ["fake-host1", "fake-host2"]
    mock_get_hosts_past_maintenance_end.return_value = ["fake-host2"]
    assert get_hosts_forgotten_down() == ["fake-host2"]


@mock.patch("paasta_tools.mesos_maintenance.get_hosts_forgotten_down", autospec=True)
def test_are_hosts_forgotten_down(
    mock_get_hosts_forgotten_down,
):
    mock_get_hosts_forgotten_down.return_value = ["fake-host"]
    assert are_hosts_forgotten_down()

    mock_get_hosts_forgotten_down.return_value = []
    assert not are_hosts_forgotten_down()
