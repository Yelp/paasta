# Copyright 2015 Yelp Inc.
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
from StringIO import StringIO

import mock
import pytest
from dateutil import tz

from paasta_tools.paasta_maintenance import build_maintenance_schedule_payload
from paasta_tools.paasta_maintenance import build_start_maintenance_payload
from paasta_tools.paasta_maintenance import datetime_seconds_from_now
from paasta_tools.paasta_maintenance import datetime_to_nanoseconds
from paasta_tools.paasta_maintenance import down
from paasta_tools.paasta_maintenance import drain
from paasta_tools.paasta_maintenance import get_down_hosts
from paasta_tools.paasta_maintenance import get_draining_hosts
from paasta_tools.paasta_maintenance import get_hosts_with_state
from paasta_tools.paasta_maintenance import get_machine_ids
from paasta_tools.paasta_maintenance import get_maintenance_schedule
from paasta_tools.paasta_maintenance import get_maintenance_status
from paasta_tools.paasta_maintenance import is_host_down
from paasta_tools.paasta_maintenance import is_host_draining
from paasta_tools.paasta_maintenance import load_credentials
from paasta_tools.paasta_maintenance import parse_timedelta
from paasta_tools.paasta_maintenance import schedule
from paasta_tools.paasta_maintenance import seconds_to_nanoseconds
from paasta_tools.paasta_maintenance import status
from paasta_tools.paasta_maintenance import undrain
from paasta_tools.paasta_maintenance import up


def test_parse_timedelta_none():
    with pytest.raises(argparse.ArgumentTypeError):
        parse_timedelta(value=None)


def test_parse_timedelta_invalid():
    with pytest.raises(argparse.ArgumentTypeError):
        parse_timedelta(value='fake value')


def test_parse_timedelta():
    assert parse_timedelta(value='1 hour') == 3600 * 1000000000


@mock.patch('paasta_tools.paasta_maintenance.now')
def test_datetime_seconds_from_now(
    mock_now,
):
    mock_now.return_value = datetime.datetime(2016, 4, 16, 0, 23, 25, 157145, tzinfo=tz.tzutc())
    expected = datetime.datetime(2016, 4, 16, 0, 23, 40, 157145, tzinfo=tz.tzutc())
    assert datetime_seconds_from_now(15) == expected


def test_seconds_to_nanoseconds():
    assert seconds_to_nanoseconds(60) == 60 * 1000000000


def test_datetime_to_nanoseconds():
    dt = datetime.datetime(2016, 4, 16, 0, 23, 40, 157145, tzinfo=tz.tzutc())
    expected = 1460766220000000000
    assert datetime_to_nanoseconds(dt) == expected


@mock.patch('paasta_tools.paasta_maintenance.gethostbyname')
def test_build_start_maintenance_payload(
    mock_gethostbyname,
):
    ip = '169.254.121.212'
    mock_gethostbyname.return_value = ip
    hostname = 'fqdn1.example.org'
    hostnames = [hostname]

    assert build_start_maintenance_payload(hostnames) == get_machine_ids(hostnames)


@mock.patch('paasta_tools.paasta_maintenance.gethostbyname')
def test_get_machine_ids_one_host(
    mock_gethostbyname,
):
    ip = '169.254.121.212'
    mock_gethostbyname.return_value = ip
    hostname = 'fqdn1.example.org'
    hostnames = [hostname]
    expected = [
        {
            'hostname': hostname,
            'ip': ip
        }
    ]
    assert get_machine_ids(hostnames) == expected


@mock.patch('paasta_tools.paasta_maintenance.gethostbyname')
def test_get_machine_ids_multiple_hosts(
    mock_gethostbyname,
):
    ip1 = '169.254.121.212'
    ip2 = '169.254.121.213'
    ip3 = '169.254.121.214'
    mock_gethostbyname.side_effect = [ip1, ip2, ip3]
    hostname1 = 'fqdn1.example.org'
    hostname2 = 'fqdn2.example.org'
    hostname3 = 'fqdn3.example.org'
    hostnames = [hostname1, hostname2, hostname3]
    expected = [
        {
            'hostname': hostname1,
            'ip': ip1
        },
        {
            'hostname': hostname2,
            'ip': ip2
        },
        {
            'hostname': hostname3,
            'ip': ip3
        }
    ]
    assert get_machine_ids(hostnames) == expected


def test_get_machine_ids_multiple_hosts_ips():
    ip1 = '169.254.121.212'
    ip2 = '169.254.121.213'
    ip3 = '169.254.121.214'
    hostname1 = 'fqdn1.example.org'
    hostname2 = 'fqdn2.example.org'
    hostname3 = 'fqdn3.example.org'
    hostnames = [hostname1 + '|' + ip1, hostname2 + '|' + ip2, hostname3 + '|' + ip3]
    expected = [
        {
            'hostname': hostname1,
            'ip': ip1
        },
        {
            'hostname': hostname2,
            'ip': ip2
        },
        {
            'hostname': hostname3,
            'ip': ip3
        }
    ]
    assert get_machine_ids(hostnames) == expected


@mock.patch('paasta_tools.paasta_maintenance.get_maintenance_schedule')
@mock.patch('paasta_tools.paasta_maintenance.get_machine_ids')
def test_build_maintenance_schedule_payload_no_schedule(
    mock_get_machine_ids,
    mock_get_maintenance_schedule,
):
    mock_get_maintenance_schedule.return_value.json.return_value = {}
    machine_ids = [{'hostname': 'machine2', 'ip': '10.0.0.2'}]
    mock_get_machine_ids.return_value = machine_ids
    hostnames = ['fake-hostname']
    start = '1443830400000000000'
    duration = '3600000000000'
    actual = build_maintenance_schedule_payload(hostnames, start, duration, drain=True)
    assert mock_get_maintenance_schedule.call_count == 1
    assert mock_get_machine_ids.call_count == 1
    assert mock_get_machine_ids.call_args == mock.call(hostnames)
    expected = {
        'windows': [
            {
                'machine_ids': machine_ids,
                'unavailability': {
                    'start': {
                        'nanoseconds': int(start),
                    },
                    'duration': {
                        'nanoseconds': int(duration),
                    },
                },
            },
        ],
    }
    assert actual == expected


@mock.patch('paasta_tools.paasta_maintenance.get_maintenance_schedule')
@mock.patch('paasta_tools.paasta_maintenance.get_machine_ids')
def test_build_maintenance_schedule_payload_no_schedule_undrain(
    mock_get_machine_ids,
    mock_get_maintenance_schedule,
):
    mock_get_maintenance_schedule.return_value.json.return_value = {}
    machine_ids = [{'hostname': 'machine2', 'ip': '10.0.0.2'}]
    mock_get_machine_ids.return_value = machine_ids
    hostnames = ['fake-hostname']
    start = '1443830400000000000'
    duration = '3600000000000'
    actual = build_maintenance_schedule_payload(hostnames, start, duration, drain=False)
    assert mock_get_maintenance_schedule.call_count == 1
    assert mock_get_machine_ids.call_count == 1
    assert mock_get_machine_ids.call_args == mock.call(hostnames)
    expected = {
        'windows': [],
    }
    assert actual == expected


@mock.patch('paasta_tools.paasta_maintenance.get_maintenance_schedule')
@mock.patch('paasta_tools.paasta_maintenance.get_machine_ids')
def test_build_maintenance_schedule_payload_schedule(
    mock_get_machine_ids,
    mock_get_maintenance_schedule,
):
    mock_get_maintenance_schedule.return_value.json.return_value = {
        "windows": [
            {
                "machine_ids": [
                    {"hostname": "machine1", "ip": "10.0.0.1"},
                    {"hostname": "machine2", "ip": "10.0.0.2"}
                ],
                "unavailability": {
                    "start": {"nanoseconds": 1443830400000000000},
                    "duration": {"nanoseconds": 3600000000000}
                }
            },
            {
                "machine_ids": [
                    {"hostname": "machine3", "ip": "10.0.0.3"}
                ],
                "unavailability": {
                    "start": {"nanoseconds": 1443834000000000000},
                    "duration": {"nanoseconds": 3600000000000}
                }
            }
        ]
    }
    machine_ids = [{'hostname': 'machine2', 'ip': '10.0.0.2'}]
    mock_get_machine_ids.return_value = machine_ids
    hostnames = ['machine2']
    start = '1443830400000000000'
    duration = '3600000000000'
    actual = build_maintenance_schedule_payload(hostnames, start, duration, drain=True)
    assert mock_get_maintenance_schedule.call_count == 1
    assert mock_get_machine_ids.call_count == 1
    assert mock_get_machine_ids.call_args == mock.call(hostnames)
    expected = {
        "windows": [
            {
                "machine_ids": [
                    {"hostname": "machine1", "ip": "10.0.0.1"},
                ],
                "unavailability": {
                    "start": {"nanoseconds": 1443830400000000000},
                    "duration": {"nanoseconds": 3600000000000}
                }
            },
            {
                "machine_ids": [
                    {"hostname": "machine3", "ip": "10.0.0.3"}
                ],
                "unavailability": {
                    "start": {"nanoseconds": 1443834000000000000},
                    "duration": {"nanoseconds": 3600000000000}
                }
            },
            {
                "machine_ids": machine_ids,
                "unavailability": {
                    "start": {"nanoseconds": int(start)},
                    "duration": {"nanoseconds": int(duration)}
                }
            }
        ]
    }
    assert actual == expected


@mock.patch('paasta_tools.paasta_maintenance.get_maintenance_schedule')
@mock.patch('paasta_tools.paasta_maintenance.get_machine_ids')
def test_build_maintenance_schedule_payload_schedule_undrain(
    mock_get_machine_ids,
    mock_get_maintenance_schedule,
):
    mock_get_maintenance_schedule.return_value.json.return_value = {
        "windows": [
            {
                "machine_ids": [
                    {"hostname": "machine1", "ip": "10.0.0.1"},
                    {"hostname": "machine2", "ip": "10.0.0.2"}
                ],
                "unavailability": {
                    "start": {"nanoseconds": 1443830400000000000},
                    "duration": {"nanoseconds": 3600000000000}
                }
            },
            {
                "machine_ids": [
                    {"hostname": "machine3", "ip": "10.0.0.3"}
                ],
                "unavailability": {
                    "start": {"nanoseconds": 1443834000000000000},
                    "duration": {"nanoseconds": 3600000000000}
                }
            }
        ]
    }
    machine_ids = [{'hostname': 'machine2', 'ip': '10.0.0.2'}]
    mock_get_machine_ids.return_value = machine_ids
    hostnames = ['machine2']
    start = '1443830400000000000'
    duration = '3600000000000'
    actual = build_maintenance_schedule_payload(hostnames, start, duration, drain=False)
    assert mock_get_maintenance_schedule.call_count == 1
    assert mock_get_machine_ids.call_count == 1
    assert mock_get_machine_ids.call_args == mock.call(hostnames)
    expected = {
        "windows": [
            {
                "machine_ids": [
                    {"hostname": "machine1", "ip": "10.0.0.1"},
                ],
                "unavailability": {
                    "start": {"nanoseconds": 1443830400000000000},
                    "duration": {"nanoseconds": 3600000000000}
                }
            },
            {
                "machine_ids": [
                    {"hostname": "machine3", "ip": "10.0.0.3"}
                ],
                "unavailability": {
                    "start": {"nanoseconds": 1443834000000000000},
                    "duration": {"nanoseconds": 3600000000000}
                }
            },
        ]
    }
    assert actual == expected


@mock.patch('paasta_tools.paasta_maintenance.open', create=True)
def test_load_credentials(
    mock_open,
):
    credentials = {
        'principal': 'username',
        'secret': 'password'
    }

    mock_open.side_effect = mock.mock_open(read_data=json.dumps(credentials))

    assert load_credentials() == ('username', 'password')


@mock.patch('paasta_tools.paasta_maintenance.open', create=True, side_effect=IOError)
def test_load_credentials_missing_file(
    mock_open,
):
    with pytest.raises(IOError):
        assert load_credentials()


@mock.patch('paasta_tools.paasta_maintenance.open', create=True)
def test_load_credentials_keyerror(
    mock_open,
):
    credentials = {}

    mock_open.side_effect = mock.mock_open(read_data=json.dumps(credentials))

    with pytest.raises(KeyError):
        assert load_credentials()


@mock.patch('paasta_tools.paasta_maintenance.get_schedule_client')
def test_get_maintenance_status(
    mock_get_schedule_client,
):
    get_maintenance_status()
    assert mock_get_schedule_client.call_count == 1
    assert mock_get_schedule_client.return_value.call_count == 1
    assert mock_get_schedule_client.return_value.call_args == mock.call(method="GET", endpoint="/status")


@mock.patch('paasta_tools.paasta_maintenance.get_schedule_client')
def test_get_maintenance_schedule(
    mock_get_schedule_client,
):
    get_maintenance_schedule()
    assert mock_get_schedule_client.call_count == 1
    assert mock_get_schedule_client.return_value.call_count == 1
    assert mock_get_schedule_client.return_value.call_args == mock.call(method="GET", endpoint="")


@mock.patch('paasta_tools.paasta_maintenance.get_schedule_client')
@mock.patch('paasta_tools.paasta_maintenance.build_maintenance_schedule_payload')
def test_drain(
    mock_build_maintenance_schedule_payload,
    mock_get_schedule_client,
):
    fake_schedule = {'fake_schedule': 'fake_value'}
    mock_build_maintenance_schedule_payload.return_value = fake_schedule
    drain(hostnames=['some-host'], start='some-start', duration='some-duration')
    assert mock_build_maintenance_schedule_payload.call_count == 1
    expected_args = mock.call(['some-host'], 'some-start', 'some-duration', drain=True)
    assert mock_build_maintenance_schedule_payload.call_args == expected_args
    assert mock_get_schedule_client.call_count == 1
    assert mock_get_schedule_client.return_value.call_count == 1
    expected_args = mock.call(method="POST", endpoint="", data=json.dumps(fake_schedule))
    assert mock_get_schedule_client.return_value.call_args == expected_args


@mock.patch('paasta_tools.paasta_maintenance.get_schedule_client')
@mock.patch('paasta_tools.paasta_maintenance.build_maintenance_schedule_payload')
def test_undrain(
    mock_build_maintenance_schedule_payload,
    mock_get_schedule_client,
):
    fake_schedule = {'fake_schedule': 'fake_value'}
    mock_build_maintenance_schedule_payload.return_value = fake_schedule
    undrain(hostnames=['some-host'])
    assert mock_build_maintenance_schedule_payload.call_count == 1
    expected_args = mock.call(['some-host'], drain=False)
    assert mock_build_maintenance_schedule_payload.call_args == expected_args
    assert mock_get_schedule_client.call_count == 1
    assert mock_get_schedule_client.return_value.call_count == 1
    expected_args = mock.call(method="POST", endpoint="", data=json.dumps(fake_schedule))
    assert mock_get_schedule_client.return_value.call_args == expected_args


@mock.patch('paasta_tools.paasta_maintenance.master_api')
@mock.patch('paasta_tools.paasta_maintenance.build_start_maintenance_payload')
def test_down(
    mock_build_start_maintenance_payload,
    mock_master_api,
):
    fake_payload = [{'fake_schedule': 'fake_value'}]
    mock_build_start_maintenance_payload.return_value = fake_payload
    down(hostnames=['some-host'])
    assert mock_build_start_maintenance_payload.call_count == 1
    assert mock_build_start_maintenance_payload.call_args == mock.call(['some-host'])
    assert mock_master_api.call_count == 1
    assert mock_master_api.return_value.call_count == 1
    expected_args = mock.call(method="POST", endpoint="/machine/down", data=json.dumps(fake_payload))
    assert mock_master_api.return_value.call_args == expected_args


@mock.patch('paasta_tools.paasta_maintenance.master_api')
@mock.patch('paasta_tools.paasta_maintenance.build_start_maintenance_payload')
def test_up(
    mock_build_start_maintenance_payload,
    mock_master_api,
):
    fake_payload = [{'fake_schedule': 'fake_value'}]
    mock_build_start_maintenance_payload.return_value = fake_payload
    up(hostnames=['some-host'])
    assert mock_build_start_maintenance_payload.call_count == 1
    assert mock_build_start_maintenance_payload.call_args == mock.call(['some-host'])
    assert mock_master_api.call_count == 1
    assert mock_master_api.return_value.call_count == 1
    expected_args = mock.call(method="POST", endpoint="/machine/up", data=json.dumps(fake_payload))
    assert mock_master_api.return_value.call_args == expected_args


@mock.patch('sys.stdout', new_callable=StringIO)
@mock.patch('paasta_tools.paasta_maintenance.get_maintenance_status')
def test_status(
    mock_get_maintenance_status,
    mock_stdout,
):
    mock_get_maintenance_status.return_value.__str__ = mock.Mock()
    mock_get_maintenance_status.return_value.__str__.return_value = 'fake_status'
    mock_get_maintenance_status.return_value.text = 'fake_text'
    status()
    output = mock_stdout.getvalue()
    assert mock_get_maintenance_status.call_count == 1
    assert output == "fake_status:fake_text\n"


@mock.patch('sys.stdout', new_callable=StringIO)
@mock.patch('paasta_tools.paasta_maintenance.get_maintenance_schedule')
def test_schedule(
    mock_get_maintenance_schedule,
    mock_stdout,
):
    mock_get_maintenance_schedule.return_value.__str__ = mock.Mock()
    mock_get_maintenance_schedule.return_value.__str__.return_value = 'fake_status'
    mock_get_maintenance_schedule.return_value.text = 'fake_text'
    schedule()
    output = mock_stdout.getvalue()
    assert mock_get_maintenance_schedule.call_count == 1
    assert output == "fake_status:fake_text\n"


@mock.patch('paasta_tools.paasta_maintenance.get_maintenance_status')
def test_get_hosts_with_state_none(
    mock_get_maintenance_status,
):
    mock_get_maintenance_status.return_value = {}
    assert get_hosts_with_state(state='fake_state') == []


@mock.patch('paasta_tools.paasta_maintenance.get_maintenance_status')
def test_get_hosts_with_state_draining(
    mock_get_maintenance_status,
):
    fake_status = {
        "draining_machines": [
            {
                "id": {
                    "hostname": "fake-host1.fakesite.something",
                    "ip": "0.0.0.0"
                }
            },
            {
                "id": {
                    "hostname": "fake-host2.fakesite.something",
                    "ip": "0.0.0.1"
                }
            }
        ]
    }
    mock_get_maintenance_status.return_value = fake_status
    expected = sorted(['fake-host1.fakesite.something', 'fake-host2.fakesite.something'])
    assert sorted(get_hosts_with_state(state='draining_machines')) == expected


@mock.patch('paasta_tools.paasta_maintenance.get_maintenance_status')
def test_get_hosts_with_state_down(
    mock_get_maintenance_status,
):
    fake_status = {
        "down_machines": [
            {
                "id": {
                    "hostname": "fake-host1.fakesite.something",
                    "ip": "0.0.0.0"
                }
            },
            {
                "id": {
                    "hostname": "fake-host2.fakesite.something",
                    "ip": "0.0.0.1"
                }
            }
        ]
    }
    mock_get_maintenance_status.return_value = fake_status
    expected = sorted(['fake-host1.fakesite.something', 'fake-host2.fakesite.something'])
    assert sorted(get_hosts_with_state(state='down_machines')) == expected


@mock.patch('paasta_tools.paasta_maintenance.get_hosts_with_state')
def test_get_draining_hosts(
    mock_get_hosts_with_state,
):
    get_draining_hosts()
    assert mock_get_hosts_with_state.call_count == 1
    expected_args = mock.call(state='draining_machines')
    assert mock_get_hosts_with_state.call_args == expected_args


@mock.patch('paasta_tools.paasta_maintenance.get_hosts_with_state')
def test_get_down_hosts(
    mock_get_hosts_with_state,
):
    get_down_hosts()
    assert mock_get_hosts_with_state.call_count == 1
    expected_args = mock.call(state='down_machines')
    assert mock_get_hosts_with_state.call_args == expected_args


@mock.patch('paasta_tools.paasta_maintenance.get_draining_hosts')
def test_is_host_draining(
    mock_get_draining_hosts,
):
    mock_get_draining_hosts.return_value = ['fake-host1.fakesite.something', 'fake-host2.fakesite.something']
    assert is_host_draining('fake-host1.fakesite.something')
    assert not is_host_draining('fake-host3.fakesite.something')


@mock.patch('paasta_tools.paasta_maintenance.get_down_hosts')
def test_is_host_down(
    mock_get_down_hosts,
):
    mock_get_down_hosts.return_value = ['fake-host1.fakesite.something', 'fake-host2.fakesite.something']
    assert is_host_down('fake-host1.fakesite.something')
    assert not is_host_down('fake-host3.fakesite.something')
