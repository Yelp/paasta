import os

import mock
import requests

from paasta_tools.monitoring.replication_utils import (
    get_replication_for_services,
    get_all_registered_ip_ports_for_services,
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
