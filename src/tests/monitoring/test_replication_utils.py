import os

import mock

from service_deployment_tools.monitoring.replication_utils import get_replication_for_services


def test_get_replication_for_service():
    testdir = os.path.dirname(os.path.realpath(__file__))
    testdata = os.path.join(testdir, 'haproxy_snapshot.txt')
    with open(testdata, 'r') as fd:
        mock_haproxy_data = fd.read()

    mock_response = mock.Mock()
    mock_response.text = mock_haproxy_data

    with mock.patch('requests.get', return_value=mock_response):
        replication_result = get_replication_for_services(
            'foo',
            ['wordtime', 'video', 'lucy', 'query_lm']
        )
        expected = {
            'wordtime': 18,
            'video': 19,
            'lucy': 0,
            'query_lm': 3
        }
        assert expected == replication_result
