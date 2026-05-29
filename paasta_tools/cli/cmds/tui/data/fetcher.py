from paasta_tools.api.client import get_paasta_oapi_client
from paasta_tools.cli.cmds.tui.data.models import ClusterInfo
from paasta_tools.cli.cmds.tui.data.models import ServiceInfo
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import load_system_paasta_config


class PaastaDataFetcher:
    def __init__(self, system_config: SystemPaastaConfig | None = None) -> None:
        self._system_config = system_config

    @property
    def system_config(self) -> SystemPaastaConfig:
        if self._system_config is None:
            self._system_config = load_system_paasta_config()
        return self._system_config

    def get_clusters(self) -> list[ClusterInfo]:
        endpoints = self.system_config.get_api_endpoints()
        return [
            ClusterInfo(name=cluster, api_endpoint=endpoint)
            for cluster, endpoint in sorted(endpoints.items())
        ]

    def get_services(self, cluster: str) -> list[ServiceInfo]:
        client = get_paasta_oapi_client(
            cluster=cluster, system_paasta_config=self.system_config
        )
        response = client.service.list_services_for_cluster()
        service_instance_pairs = response.get("services", [])
        unique_services = sorted({pair[0] for pair in service_instance_pairs})
        return [ServiceInfo(name=name) for name in unique_services]
