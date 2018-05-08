import mock
import pytest

from paasta_tools.tron.client import TronClient


@pytest.fixture
def mock_requests():
    with mock.patch(
        'paasta_tools.tron.client.requests',
        autospec=True,
    ) as mock_requests:
        yield mock_requests


class TestTronClient:

    tron_url = 'http://tron.test:9000'
    client = TronClient(tron_url)

    def test_update_namespace(self, mock_requests):
        new_config = 'yaml: stuff'
        mock_requests.get.return_value.json.return_value = {
            'config': 'old: things',
            'hash': '01abcd',
        }
        self.client.update_namespace('some_service', new_config)

        assert mock_requests.get.call_count == 1
        _, kwargs = mock_requests.get.call_args
        assert kwargs['url'] == self.tron_url + '/api/config'
        assert kwargs['params'] == {'name': 'some_service'}

        assert mock_requests.post.call_count == 1
        _, kwargs = mock_requests.post.call_args
        assert kwargs['url'] == self.tron_url + '/api/config'
        assert kwargs['data'] == {
            'name': 'some_service',
            'config': new_config,
            'hash': '01abcd',
            'check': 0,
        }

    @pytest.mark.parametrize('skip_if_unchanged', [True, False])
    def test_update_namespace_unchanged(self, mock_requests, skip_if_unchanged):
        new_config = 'yaml: stuff'
        mock_requests.get.return_value.json.return_value = {
            'config': new_config,
            'hash': '01abcd',
        }
        self.client.update_namespace('some_service', new_config, skip_if_unchanged)
        assert mock_requests.post.call_count == int(not skip_if_unchanged)

    def test_list_namespaces(self, mock_requests):
        mock_requests.get.return_value.json.return_value = {
            'jobs': {},
            'namespaces': ['a', 'b'],
        }
        assert self.client.list_namespaces() == ['a', 'b']
        assert mock_requests.get.call_count == 1
        _, kwargs = mock_requests.get.call_args
        assert kwargs['url'] == self.tron_url + '/api'
        assert kwargs['params'] is None
