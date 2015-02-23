import os

import mock
import requests

from paasta_tools.monitoring.replication_utils import (
    get_all_registered_ip_ports_for_services,
    get_registered_marathon_tasks,
    get_replication_for_services,
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
            'foo',
            ['service1', 'service2', 'service3', 'service4']
        )
        expected = {
            'service1': 18,
            'service2': 19,
            'service3': 0,
            'service4': 3
        }
        assert expected == replication_result


def test_get_all_registered_ip_ports_for_services():
    testdir = os.path.dirname(os.path.realpath(__file__))
    testdata = os.path.join(testdir, 'haproxy_snapshot.txt')
    with open(testdata, 'r') as fd:
        mock_haproxy_data = fd.read()

    mock_response = mock.Mock()
    mock_response.text = mock_haproxy_data
    mock_get = mock.Mock(return_value=(mock_response))

    with mock.patch.object(requests.Session, 'get', mock_get):
        replication_result = get_all_registered_ip_ports_for_services(
            'foo',
            ['service1', 'service2', 'service3', 'service4']
        )

        expected = [
            ('10.46.2.32', 14902),
            ('10.46.1.32', 14902),
            ('10.46.3.32', 14902),
        ]
        assert set(replication_result['service4']) == set(expected)


def test_get_registered_marathon_tasks():
    haproxy_csv = [
        {"# pxname": "servicename.main", "svname": "10.50.2.4:31000_box4", "status": "UP", '': ''},
        {"# pxname": "servicename.main", "svname": "10.50.2.5:31001_box5", "status": "UP", '': ''},
        {"# pxname": "servicename.main", "svname": "10.50.2.6:31001_box6", "status": "UP", '': ''},
        {"# pxname": "servicename.main", "svname": "10.50.2.6:31002_box7", "status": "UP", '': ''},
        {"# pxname": "servicename.main", "svname": "10.50.2.8:31000_box8", "status": "UP", '': ''},
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
        'paasta_tools.monitoring.replication_utils.'
            'retrieve_haproxy_csv',
        return_value=haproxy_csv
    ):
        with mock.patch(
            'paasta_tools.monitoring.replication_utils.'
                'socket.gethostbyname',
            side_effect=lambda x: hostnames[x],
        ):
            actual = get_registered_marathon_tasks(
                'foo',
                'servicename.main',
                marathon_tasks,
            )

            expected = [good_task1, good_task2]
            assert actual == expected
