import mock
import pytest


@pytest.fixture
def mock_agents_response():
    response = mock.Mock()
    response.json.return_value = {
        'slaves': [
            {
                'pid': 'foo@10.10.10.10:1',
                'attributes': {
                    'blah': 10,
                    'pool': 'asdf',
                },
                'hostname': 'not-in-the-pool.yelpcorp.com',
            },
            {
                'pid': 'foo@10.10.10.11:1',
                'hostname': 'asdf.yelpcorp.com',
                'used_resources': {'mem': 10},
            },
            {
                'pid': 'foo@10.10.10.12:1',
                'attributes': {
                    'blah': 10,
                    'pool': 'bar',
                    'ssss': 'hjkl',
                },
                'hostname': 'im-in-the-pool.yelpcorp.com',
                'used_resources': {
                    'mem': 20,
                    'cpus': 10,
                },
            },
        ]
    }
    return response
