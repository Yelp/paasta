import json

import mock
import pytest

from clusterman.draining.mesos import build_maintenance_payload
from clusterman.draining.mesos import build_maintenance_schedule_payload
from clusterman.draining.mesos import down
from clusterman.draining.mesos import drain
from clusterman.draining.mesos import get_machine_ids
from clusterman.draining.mesos import get_maintenance_schedule
from clusterman.draining.mesos import Hostname
from clusterman.draining.mesos import hostnames_to_components
from clusterman.draining.mesos import load_credentials
from clusterman.draining.mesos import up


@mock.patch('clusterman.draining.mesos.gethostbyname', autospec=True)
def test_build_maintenance_payload(
    mock_gethostbyname,
):
    ip = '169.254.121.212'
    mock_gethostbyname.return_value = ip
    hostname = 'fqdn1.example.org'
    hostnames = [hostname]

    assert build_maintenance_payload(
        hostnames, 'start_maintenance',
    )['start_maintenance']['machines'] == get_machine_ids(hostnames)


@mock.patch('clusterman.draining.mesos.gethostbyname', autospec=True)
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
            'ip': ip,
        },
    ]
    assert get_machine_ids(hostnames) == expected


@mock.patch('clusterman.draining.mesos.gethostbyname', autospec=True)
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
            'ip': ip1,
        },
        {
            'hostname': hostname2,
            'ip': ip2,
        },
        {
            'hostname': hostname3,
            'ip': ip3,
        },
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
            'ip': ip1,
        },
        {
            'hostname': hostname2,
            'ip': ip2,
        },
        {
            'hostname': hostname3,
            'ip': ip3,
        },
    ]
    assert get_machine_ids(hostnames) == expected


@mock.patch('clusterman.draining.mesos.get_maintenance_schedule', autospec=True)
@mock.patch('clusterman.draining.mesos.get_machine_ids', autospec=True)
def test_build_maintenance_schedule_payload_no_schedule(
    mock_get_machine_ids,
    mock_get_maintenance_schedule,
):
    mock_get_maintenance_schedule.return_value.json.return_value = {
        'get_maintenance_schedule': {'schedule': {}},
    }
    machine_ids = [{'hostname': 'machine2', 'ip': '10.0.0.2'}]
    mock_get_machine_ids.return_value = machine_ids
    hostnames = ['fake-hostname']
    start = '1443830400000000000'
    duration = '3600000000000'
    actual = build_maintenance_schedule_payload(mock.Mock(), hostnames, start, duration, drain=True)
    assert mock_get_maintenance_schedule.call_count == 1
    assert mock_get_machine_ids.call_count == 1
    assert mock_get_machine_ids.call_args == mock.call(hostnames)
    expected = {
        'type': 'UPDATE_MAINTENANCE_SCHEDULE',
        'update_maintenance_schedule': {'schedule': {'windows': [
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
        ]}},
    }
    assert actual == expected


@mock.patch('clusterman.draining.mesos.get_maintenance_schedule', autospec=True)
@mock.patch('clusterman.draining.mesos.get_machine_ids', autospec=True)
def test_build_maintenance_schedule_payload_no_schedule_undrain(
    mock_get_machine_ids,
    mock_get_maintenance_schedule,
):
    mock_get_maintenance_schedule.return_value.json.return_value = {
        'get_maintenance_schedule': {'schedule': {}},
    }
    machine_ids = [{'hostname': 'machine2', 'ip': '10.0.0.2'}]
    mock_get_machine_ids.return_value = machine_ids
    hostnames = ['fake-hostname']
    start = '1443830400000000000'
    duration = '3600000000000'
    actual = build_maintenance_schedule_payload(mock.Mock(), hostnames, start, duration, drain=False)
    assert mock_get_maintenance_schedule.call_count == 1
    assert mock_get_machine_ids.call_count == 1
    assert mock_get_machine_ids.call_args == mock.call(hostnames)
    expected = {
        'type': 'UPDATE_MAINTENANCE_SCHEDULE',
        'update_maintenance_schedule': {'schedule': {
            'windows': [],
        }},
    }
    assert actual == expected


@mock.patch('clusterman.draining.mesos.get_maintenance_schedule', autospec=True)
@mock.patch('clusterman.draining.mesos.get_machine_ids', autospec=True)
def test_build_maintenance_schedule_payload_schedule(
    mock_get_machine_ids,
    mock_get_maintenance_schedule,
):
    mock_get_maintenance_schedule.return_value.json.return_value = {
        'type': 'GET_MAINTENANCE_SCHEDULE',
        'get_maintenance_schedule': {'schedule': {'windows': [
            {
                'machine_ids': [
                    {'hostname': 'machine1', 'ip': '10.0.0.1'},
                    {'hostname': 'machine2', 'ip': '10.0.0.2'},
                ],
                'unavailability': {
                    'start': {'nanoseconds': 1443830400000000000},
                    'duration': {'nanoseconds': 3600000000000},
                },
            },
            {
                'machine_ids': [
                    {'hostname': 'machine3', 'ip': '10.0.0.3'},
                ],
                'unavailability': {
                    'start': {'nanoseconds': 1443834000000000000},
                    'duration': {'nanoseconds': 3600000000000},
                },
            },
        ]}},
    }
    machine_ids = [{'hostname': 'machine2', 'ip': '10.0.0.2'}]
    mock_get_machine_ids.return_value = machine_ids
    hostnames = ['machine2']
    start = '1443830400000000000'
    duration = '3600000000000'
    actual = build_maintenance_schedule_payload(mock.Mock(), hostnames, start, duration, drain=True)
    assert mock_get_maintenance_schedule.call_count == 1
    assert mock_get_machine_ids.call_count == 1
    assert mock_get_machine_ids.call_args == mock.call(hostnames)
    expected = {
        'type': 'UPDATE_MAINTENANCE_SCHEDULE',
        'update_maintenance_schedule': {'schedule': {'windows': [
            {
                'machine_ids': [
                    {'hostname': 'machine1', 'ip': '10.0.0.1'},
                ],
                'unavailability': {
                    'start': {'nanoseconds': 1443830400000000000},
                    'duration': {'nanoseconds': 3600000000000},
                },
            },
            {
                'machine_ids': [
                    {'hostname': 'machine3', 'ip': '10.0.0.3'},
                ],
                'unavailability': {
                    'start': {'nanoseconds': 1443834000000000000},
                    'duration': {'nanoseconds': 3600000000000},
                },
            },
            {
                'machine_ids': machine_ids,
                'unavailability': {
                    'start': {'nanoseconds': int(start)},
                    'duration': {'nanoseconds': int(duration)},
                },
            },
        ]}},
    }
    assert actual == expected


@mock.patch('clusterman.draining.mesos.get_maintenance_schedule', autospec=True)
@mock.patch('clusterman.draining.mesos.get_machine_ids', autospec=True)
def test_build_maintenance_schedule_payload_schedule_undrain(
    mock_get_machine_ids,
    mock_get_maintenance_schedule,
):
    mock_get_maintenance_schedule.return_value.json.return_value = {
        'type': 'GET_MAINTENANCE_SCHEDULE',
        'get_maintenance_schedule': {'schedule': {'windows': [
            {
                'machine_ids': [
                    {'hostname': 'machine1', 'ip': '10.0.0.1'},
                    {'hostname': 'machine2', 'ip': '10.0.0.2'},
                ],
                'unavailability': {
                    'start': {'nanoseconds': 1443830400000000000},
                    'duration': {'nanoseconds': 3600000000000},
                },
            },
            {
                'machine_ids': [
                    {'hostname': 'machine3', 'ip': '10.0.0.3'},
                ],
                'unavailability': {
                    'start': {'nanoseconds': 1443834000000000000},
                    'duration': {'nanoseconds': 3600000000000},
                },
            },
        ]}},
    }
    machine_ids = [{'hostname': 'machine2', 'ip': '10.0.0.2'}]
    mock_get_machine_ids.return_value = machine_ids
    hostnames = ['machine2']
    start = '1443830400000000000'
    duration = '3600000000000'
    actual = build_maintenance_schedule_payload(mock.Mock(), hostnames, start, duration, drain=False)
    assert mock_get_maintenance_schedule.call_count == 1
    assert mock_get_machine_ids.call_count == 1
    assert mock_get_machine_ids.call_args == mock.call(hostnames)
    expected = {
        'type': 'UPDATE_MAINTENANCE_SCHEDULE',
        'update_maintenance_schedule': {'schedule': {'windows': [
            {
                'machine_ids': [
                    {'hostname': 'machine1', 'ip': '10.0.0.1'},
                ],
                'unavailability': {
                    'start': {'nanoseconds': 1443830400000000000},
                    'duration': {'nanoseconds': 3600000000000},
                },
            },
            {
                'machine_ids': [
                    {'hostname': 'machine3', 'ip': '10.0.0.3'},
                ],
                'unavailability': {
                    'start': {'nanoseconds': 1443834000000000000},
                    'duration': {'nanoseconds': 3600000000000},
                },
            },
        ]}},
    }
    assert actual == expected


@mock.patch('clusterman.draining.mesos.open', create=True, autospec=None)
def test_load_credentials(
    mock_open,
):
    principal = 'username'
    secret = 'password'
    credentials = {
        'principal': principal,
        'secret': secret,
    }

    mock_open.side_effect = mock.mock_open(read_data=json.dumps(credentials))

    credentials = load_credentials('/nail/blah')

    assert credentials.principal == principal
    assert credentials.secret == secret


@mock.patch('clusterman.draining.mesos.open', create=True, side_effect=IOError, autospec=None)
def test_load_credentials_missing_file(
    mock_open,
):
    with pytest.raises(IOError):
        assert load_credentials('/nail/blah')


@mock.patch('clusterman.draining.mesos.open', create=True, autospec=None)
def test_load_credentials_keyerror(
    mock_open,
):
    credentials = {}

    mock_open.side_effect = mock.mock_open(read_data=json.dumps(credentials))

    with pytest.raises(KeyError):
        assert load_credentials('/nail/blah')


def test_get_maintenance_schedule():
    mock_operator_api = mock.Mock()
    get_maintenance_schedule(mock_operator_api)
    assert mock_operator_api.call_count == 1
    assert mock_operator_api.call_args == mock.call(data={'type': 'GET_MAINTENANCE_SCHEDULE'})


@mock.patch('clusterman.draining.mesos.build_maintenance_schedule_payload', autospec=True)
def test_drain(
    mock_build_maintenance_schedule_payload,
):
    mock_operator_api = mock.Mock()
    fake_schedule = {'fake_schedule': 'fake_value'}
    mock_build_maintenance_schedule_payload.return_value = fake_schedule
    drain(mock_operator_api, hostnames=['some-host'], start='some-start', duration='some-duration')

    assert mock_build_maintenance_schedule_payload.call_count == 1
    expected_args = mock.call(mock_operator_api, ['some-host'], 'some-start', 'some-duration', drain=True)
    assert mock_build_maintenance_schedule_payload.call_args == expected_args

    expected_args = mock.call(['some-host'])

    assert mock_operator_api.call_count == 1
    expected_args = mock.call(data=fake_schedule)
    assert mock_operator_api.call_args == expected_args


@mock.patch('clusterman.draining.mesos.build_maintenance_payload', autospec=True)
def test_down(
    mock_build_maintenance_payload,
):
    mock_operator_api = mock.Mock()
    fake_payload = [{'fake_schedule': 'fake_value'}]
    mock_build_maintenance_payload.return_value = fake_payload
    down(mock_operator_api, hostnames=['some-host'])
    assert mock_build_maintenance_payload.call_count == 1
    assert mock_build_maintenance_payload.call_args == mock.call(['some-host'], 'start_maintenance')
    assert mock_operator_api.call_count == 1
    expected_args = mock.call(data=fake_payload)
    assert mock_operator_api.call_args == expected_args


@mock.patch('clusterman.draining.mesos.build_maintenance_payload', autospec=True)
def test_up(
    mock_build_maintenance_payload,
):
    mock_operator_api = mock.Mock()
    fake_payload = [{'fake_schedule': 'fake_value'}]
    mock_build_maintenance_payload.return_value = fake_payload
    up(mock_operator_api, hostnames=['some-host'])
    assert mock_build_maintenance_payload.call_count == 1
    assert mock_build_maintenance_payload.call_args == mock.call(['some-host'], 'stop_maintenance')
    assert mock_operator_api.call_count == 1
    expected_args = mock.call(data=fake_payload)
    assert mock_operator_api.call_args == expected_args


def sideeffect_mock_get_count_running_tasks_on_slave(hostname):
    if hostname == 'host1':
        return 3
    else:
        return 0


def test_hostnames_to_components_simple():
    hostname = 'fake-host'
    ip = None
    expected = [Hostname(host=hostname, ip=ip)]
    actual = hostnames_to_components([hostname])
    assert actual == expected


def test_hostnames_to_components_pipe():
    hostname = 'fake-host'
    ip = '127.0.0.1'
    expected = [Hostname(host=hostname, ip=ip)]
    actual = hostnames_to_components([f'{hostname}|{ip}'])
    assert actual == expected


@mock.patch('clusterman.draining.mesos.gethostbyname', autospec=True)
def test_hostnames_to_components_resolve(
    mock_gethostbyname,
):
    hostname = 'fake-host'
    ip = '127.0.0.1'
    mock_gethostbyname.return_value = ip
    expected = [Hostname(host=hostname, ip=ip)]
    actual = hostnames_to_components([hostname], resolve=True)
    assert actual == expected
