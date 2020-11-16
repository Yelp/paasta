import mock
import pytest

from paasta_tools.tron.client import TronClient
from paasta_tools.tron.client import TronRequestError


@pytest.fixture
def mock_requests():
    with mock.patch(
        "paasta_tools.tron.client.requests", autospec=True
    ) as mock_requests:
        yield mock_requests


class TestTronClient:

    tron_url = "http://tron.test:9000"
    client = TronClient(tron_url)

    def test_get(self, mock_requests):
        response = self.client._get("/some/thing", {"check": 1})
        assert response == mock_requests.get.return_value.json.return_value
        mock_requests.get.assert_called_once_with(
            headers=mock.ANY, url=self.tron_url + "/some/thing", params={"check": 1}
        )

    def test_post(self, mock_requests):
        response = self.client._post("/some/thing", {"check": 1})
        assert response == mock_requests.post.return_value.json.return_value
        mock_requests.post.assert_called_once_with(
            headers=mock.ANY, url=self.tron_url + "/some/thing", data={"check": 1}
        )

    @pytest.mark.parametrize("okay_status", [True, False])
    def test_returned_error_message(self, mock_requests, okay_status):
        mock_requests.post.return_value.ok = okay_status
        mock_requests.post.return_value.json.return_value = {
            "error": "config was invalid"
        }
        with pytest.raises(TronRequestError, match="config was invalid"):
            self.client._post("/api/test")

    def test_unexpected_error(self, mock_requests):
        mock_requests.get.return_value.ok = False
        mock_requests.get.return_value.text = "Server error"
        mock_requests.get.return_value.json.side_effect = ValueError
        with pytest.raises(TronRequestError):
            self.client._get("/some/thing")

    def test_okay_not_json(self, mock_requests):
        mock_requests.get.return_value.ok = True
        mock_requests.get.return_value.text = "Hi, you have reached Tron."
        mock_requests.get.return_value.json.side_effect = ValueError
        assert self.client._get("/some/thing") == "Hi, you have reached Tron."

    def test_update_namespace(self, mock_requests):
        new_config = "yaml: stuff"
        mock_requests.get.return_value.json.return_value = {
            "config": "old: things",
            "hash": "01abcd",
        }
        self.client.update_namespace("some_service", new_config)

        assert mock_requests.get.call_count == 1
        _, kwargs = mock_requests.get.call_args
        assert kwargs["url"] == self.tron_url + "/api/config"
        assert kwargs["params"] == {"name": "some_service", "no_header": 1}

        assert mock_requests.post.call_count == 1
        _, kwargs = mock_requests.post.call_args
        assert kwargs["url"] == self.tron_url + "/api/config"
        assert kwargs["data"] == {
            "name": "some_service",
            "config": new_config,
            "hash": "01abcd",
            "check": 0,
        }

    @pytest.mark.parametrize("skip_if_unchanged", [True, False])
    def test_update_namespace_unchanged(self, mock_requests, skip_if_unchanged):
        new_config = "yaml: stuff"
        mock_requests.get.return_value.json.return_value = {
            "config": new_config,
            "hash": "01abcd",
        }
        self.client.update_namespace("some_service", new_config, skip_if_unchanged)
        assert mock_requests.post.call_count == int(not skip_if_unchanged)

    def test_list_namespaces(self, mock_requests):
        mock_requests.get.return_value.json.return_value = {
            "jobs": {},
            "namespaces": ["a", "b"],
        }
        assert self.client.list_namespaces() == ["a", "b"]
        assert mock_requests.get.call_count == 1
        _, kwargs = mock_requests.get.call_args
        assert kwargs["url"] == self.tron_url + "/api"
        assert kwargs["params"] is None
