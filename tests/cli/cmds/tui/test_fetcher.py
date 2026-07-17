from unittest import mock

from paasta_tools.cli.cmds.tui.data.fetcher import PaastaDataFetcher
from paasta_tools.cli.cmds.tui.data.models import ClusterInfo
from paasta_tools.cli.cmds.tui.data.models import InstanceInfo


@mock.patch("os.path.isdir", autospec=True, return_value=True)
@mock.patch(
    "paasta_tools.cli.cmds.tui.data.fetcher.read_service_configuration", autospec=True
)
@mock.patch("paasta_tools.cli.cmds.tui.data.fetcher.get_runbook", autospec=True)
@mock.patch("paasta_tools.cli.cmds.tui.data.fetcher.get_team", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.tui.data.fetcher.paasta_list_services", autospec=True
)
def test_get_all_services(
    mock_list_services, mock_get_team, mock_get_runbook, mock_read_config, mock_isdir
):
    mock_list_services.return_value = [
        "zservice",
        "aservice",
        ".hidden",
        "_internal",
        "mservice",
    ]
    mock_read_config.return_value = {
        "description": "A service",
        "external_link": "http://x",
    }
    mock_get_team.return_value = "myteam"
    mock_get_runbook.return_value = "y/rb"
    fetcher = PaastaDataFetcher(system_config=mock.MagicMock())
    services = fetcher.get_all_services()
    assert len(services) == 3
    assert services[0].name == "aservice"
    assert services[0].description == "A service"
    assert services[0].team == "myteam"
    assert services[0].runbook == "y/rb"


@mock.patch(
    "paasta_tools.cli.cmds.tui.data.fetcher.paasta_list_clusters", autospec=True
)
def test_get_clusters_for_service(mock_list_clusters):
    mock_list_clusters.return_value = ["prod", "dev"]
    mock_config = mock.MagicMock()
    mock_config.get_api_endpoints.return_value = {
        "prod": "https://api.prod.example.com",
        "dev": "https://api.dev.example.com",
        "staging": "https://api.staging.example.com",
    }
    fetcher = PaastaDataFetcher(system_config=mock_config)
    clusters = fetcher.get_clusters_for_service("myservice")
    mock_list_clusters.assert_called_once_with(service="myservice")
    assert clusters == [
        ClusterInfo(name="dev", api_endpoint="https://api.dev.example.com"),
        ClusterInfo(name="prod", api_endpoint="https://api.prod.example.com"),
    ]


@mock.patch(
    "paasta_tools.cli.cmds.tui.data.fetcher.get_paasta_oapi_client", autospec=True
)
def test_get_instances_running(mock_get_client):
    mock_client = mock.MagicMock()
    mock_client.service.list_instances.return_value = {"instances": ["web", "worker"]}
    mock_client.service.status_instance.return_value = {
        "kubernetes_v2": {
            "desired_state": "start",
            "desired_instances": 3,
            "versions": [
                {"ready_replicas": 3, "replicas": 3, "git_sha": "abc12345def"}
            ],
            "error_message": None,
        }
    }
    mock_get_client.return_value = mock_client

    mock_config = mock.MagicMock()
    fetcher = PaastaDataFetcher(system_config=mock_config)
    instances = fetcher.get_instances("prod", "myservice")

    assert len(instances) == 2
    assert instances[0] == InstanceInfo(
        name="web",
        instance_type="kubernetes_v2",
        state="Running",
        ready=3,
        desired=3,
        git_sha="abc12345",
        num_versions=1,
        error="",
    )


@mock.patch(
    "paasta_tools.cli.cmds.tui.data.fetcher.get_paasta_oapi_client", autospec=True
)
def test_get_instances_bouncing(mock_get_client):
    mock_client = mock.MagicMock()
    mock_client.service.list_instances.return_value = {"instances": ["web"]}
    mock_client.service.status_instance.return_value = {
        "kubernetes_v2": {
            "desired_state": "start",
            "desired_instances": 3,
            "versions": [
                {"ready_replicas": 2, "replicas": 2, "git_sha": "newsha12345"},
                {"ready_replicas": 1, "replicas": 1, "git_sha": "oldsha12345"},
            ],
            "error_message": None,
        }
    }
    mock_get_client.return_value = mock_client

    mock_config = mock.MagicMock()
    fetcher = PaastaDataFetcher(system_config=mock_config)
    instances = fetcher.get_instances("prod", "myservice")

    assert instances[0].state == "Bouncing"
    assert instances[0].num_versions == 2
    assert instances[0].git_sha == "newsha12"


@mock.patch(
    "paasta_tools.cli.cmds.tui.data.fetcher.get_paasta_oapi_client", autospec=True
)
def test_get_instances_flink_none_git_sha(mock_get_client):
    mock_client = mock.MagicMock()
    mock_client.service.list_instances.return_value = {"instances": ["flink-job"]}
    mock_client.service.status_instance.return_value = {
        "flink": {"status": "running"},
        "desired_state": "start",
        "git_sha": None,
    }
    mock_get_client.return_value = mock_client

    mock_config = mock.MagicMock()
    fetcher = PaastaDataFetcher(system_config=mock_config)
    instances = fetcher.get_instances("prod", "myservice")

    assert len(instances) == 1
    assert instances[0].name == "flink-job"
    assert instances[0].instance_type == "flink"
    assert instances[0].git_sha == ""
    assert instances[0].state == "start"


@mock.patch(
    "paasta_tools.cli.cmds.tui.data.fetcher.get_paasta_oapi_client", autospec=True
)
def test_get_instances_with_error(mock_get_client):
    mock_client = mock.MagicMock()
    mock_client.service.list_instances.return_value = {"instances": ["web"]}
    mock_client.service.status_instance.return_value = {
        "kubernetes_v2": {
            "desired_state": "start",
            "desired_instances": 3,
            "versions": [
                {"ready_replicas": 0, "replicas": 0, "git_sha": "abc12345def"}
            ],
            "error_message": "ImagePullBackOff: failed to pull image",
        }
    }
    mock_get_client.return_value = mock_client

    mock_config = mock.MagicMock()
    fetcher = PaastaDataFetcher(system_config=mock_config)
    instances = fetcher.get_instances("prod", "myservice")

    assert instances[0].state == "Starting"
    assert instances[0].error == "ImagePullBackOff: failed to pull image"
