import os

import mock
import requests

from paasta_tools.monitoring.replication_utils import (
    get_registered_marathon_tasks,
    get_replication_for_services,
    ip_port_hostname_from_svname,
    match_backends_and_tasks,
    backend_is_up,
)


def test_get_replication_for_service():
    testdir = os.path.dirname(os.path.realpath(__file__))
    testdata = os.path.join(testdir, 'haproxy_snapshot.txt')
    with open(testdata, 'r') as fd:
        mock_haproxy_data = fd.read()

    mock_response = mock.Mock()
    mock_response.text = mock_haproxy_data
    mock_get = mock.Mock(return_value=(mock_response))

    with mock.patch.object(requests.Session, 'get', mock_get):
        replication_result = get_replication_for_services(
            'fake_host',
            6666,
            ['service1', 'service2', 'service3', 'service4']
        )
        expected = {
            'service1': 18,
            'service2': 19,
            'service3': 0,
            'service4': 3
        }
        assert expected == replication_result


def test_get_registered_marathon_tasks():
    backends = [
        {"pxname": "servicename.main", "svname": "10.50.2.4:31000_box4", "status": "UP"},
        {"pxname": "servicename.main", "svname": "10.50.2.5:31001_box5", "status": "UP"},
        {"pxname": "servicename.main", "svname": "10.50.2.6:31001_box6", "status": "UP"},
        {"pxname": "servicename.main", "svname": "10.50.2.6:31002_box7", "status": "UP"},
        {"pxname": "servicename.main", "svname": "10.50.2.8:31000_box8", "status": "UP"},
    ]

    hostnames = {
        'box4': '10.50.2.4',
        'box5': '10.50.2.5',
        'box6': '10.50.2.6',
        'box7': '10.50.2.7',
        'box8': '10.50.2.8',
    }

    good_task1 = mock.Mock(host='box4', ports=[31000])
    good_task2 = mock.Mock(host='box5', ports=[31001])
    bad_task = mock.Mock(host='box7', ports=[31000])

    marathon_tasks = [
        good_task1,
        good_task2,
        bad_task,
    ]

    with mock.patch(
        'paasta_tools.monitoring.replication_utils.get_multiple_backends',
        return_value=backends
    ):
        with mock.patch(
            'paasta_tools.monitoring.replication_utils.'
                'socket.gethostbyname',
            side_effect=lambda x: hostnames[x],
        ):
            actual = get_registered_marathon_tasks(
                'fake_host',
                6666,
                'servicename.main',
                marathon_tasks,
            )

            expected = [good_task1, good_task2]
            assert actual == expected


def test_backend_is_up():
    assert True == backend_is_up({"status": "UP"})
    assert True == backend_is_up({"status": "UP 1/2"})
    assert False == backend_is_up({"status": "DOWN"})
    assert False == backend_is_up({"status": "DOWN 1/2"})
    assert False == backend_is_up({"status": "MAINT"})


def test_ip_port_hostname_from_svname():
    assert ("1.2.3.4", 5, "six") == ip_port_hostname_from_svname("1.2.3.4:5_six")


def test_match_backends_and_tasks():
    backends = [
        {"pxname": "servicename.main", "svname": "10.50.2.4:31000_box4", "status": "UP"},
        {"pxname": "servicename.main", "svname": "10.50.2.5:31001_box5", "status": "UP"},
        {"pxname": "servicename.main", "svname": "10.50.2.6:31001_box6", "status": "UP"},
        {"pxname": "servicename.main", "svname": "10.50.2.6:31002_box7", "status": "UP"},
        {"pxname": "servicename.main", "svname": "10.50.2.8:31000_box8", "status": "UP"},
    ]

    hostnames = {
        'box4': '10.50.2.4',
        'box5': '10.50.2.5',
        'box6': '10.50.2.6',
        'box7': '10.50.2.7',
        'box8': '10.50.2.8',
    }

    good_task1 = mock.Mock(host='box4', ports=[31000])
    good_task2 = mock.Mock(host='box5', ports=[31001])
    bad_task = mock.Mock(host='box7', ports=[31000])
    tasks = [good_task1, good_task2, bad_task]

    with mock.patch(
        'paasta_tools.monitoring.replication_utils.'
            'socket.gethostbyname',
        side_effect=lambda x: hostnames[x],
    ):
        expected = [
            (backends[0], good_task1),
            (backends[1], good_task2),
            (None, bad_task),
            (backends[2], None),
            (backends[3], None),
            (backends[4], None),
        ]
        actual = match_backends_and_tasks(backends, tasks)
        assert sorted(actual) == sorted(expected)
