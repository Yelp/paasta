from unittest import mock

from paasta_tools.cli.cmds.tui.data.fetcher import PaastaDataFetcher
from paasta_tools.cli.cmds.tui.data.models import ClusterInfo
from paasta_tools.cli.cmds.tui.data.models import ServiceInfo


def test_get_clusters():
    mock_config = mock.MagicMock()
    mock_config.get_api_endpoints.return_value = {
        "prod": "https://api.prod.example.com",
        "dev": "https://api.dev.example.com",
    }
    fetcher = PaastaDataFetcher(system_config=mock_config)
    clusters = fetcher.get_clusters()
    assert clusters == [
        ClusterInfo(name="dev", api_endpoint="https://api.dev.example.com"),
        ClusterInfo(name="prod", api_endpoint="https://api.prod.example.com"),
    ]


@mock.patch(
    "paasta_tools.cli.cmds.tui.data.fetcher.get_paasta_oapi_client", autospec=True
)
def test_get_services(mock_get_client):
    mock_client = mock.MagicMock()
    mock_client.service.list_services_for_cluster.return_value = {
        "services": [
            ["zservice", "web"],
            ["aservice", "main"],
            ["mservice", "worker"],
            ["aservice", "canary"],
        ]
    }
    mock_get_client.return_value = mock_client

    mock_config = mock.MagicMock()
    fetcher = PaastaDataFetcher(system_config=mock_config)
    services = fetcher.get_services("prod")

    mock_get_client.assert_called_once_with(
        cluster="prod", system_paasta_config=mock_config
    )
    assert services == [
        ServiceInfo(name="aservice"),
        ServiceInfo(name="mservice"),
        ServiceInfo(name="zservice"),
    ]
