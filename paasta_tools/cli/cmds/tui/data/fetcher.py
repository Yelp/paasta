import os
from concurrent.futures import ThreadPoolExecutor

from paasta_tools.api.client import get_paasta_oapi_client
from paasta_tools.cli.cmds.tui.data.models import ClusterInfo
from paasta_tools.cli.cmds.tui.data.models import InstanceInfo
from paasta_tools.cli.cmds.tui.data.models import ServiceInfo
from paasta_tools.monitoring_tools import get_runbook
from paasta_tools.monitoring_tools import get_team
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import list_clusters as paasta_list_clusters
from paasta_tools.utils import list_services as paasta_list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import read_service_configuration


class PaastaDataFetcher:
    def __init__(self, system_config: SystemPaastaConfig | None = None) -> None:
        self._system_config = system_config

    @property
    def system_config(self) -> SystemPaastaConfig:
        if self._system_config is None:
            self._system_config = load_system_paasta_config()
        return self._system_config

    def get_all_services(self) -> list[ServiceInfo]:
        soa_dir = DEFAULT_SOA_DIR
        names = [
            name
            for name in paasta_list_services(soa_dir=soa_dir)
            if not name.startswith((".", "_"))
            and os.path.isdir(os.path.join(soa_dir, name))
        ]
        results = []
        for name in sorted(names):
            try:
                config = read_service_configuration(name, soa_dir)
            except Exception:
                config = {}
            results.append(
                ServiceInfo(
                    name=name,
                    description=config.get("description", ""),
                    team=get_team(service=name, overrides={}, soa_dir=soa_dir),
                    runbook=get_runbook(service=name, overrides={}, soa_dir=soa_dir),
                    external_link=config.get("external_link", ""),
                    git_repo=config.get("git_url", ""),
                )
            )
        return results

    def get_clusters_for_service(self, service: str) -> list[ClusterInfo]:
        endpoints = self.system_config.get_api_endpoints()
        service_clusters = paasta_list_clusters(service=service)
        return [
            ClusterInfo(name=cluster, api_endpoint=endpoints.get(cluster, ""))
            for cluster in sorted(service_clusters)
            if cluster in endpoints
        ]

    def _get_client_for_cluster(self, cluster: str):
        endpoints = self.system_config.get_api_endpoints()
        eks_cluster = f"eks-{cluster}"
        if eks_cluster in endpoints:
            client = get_paasta_oapi_client(
                cluster=eks_cluster, system_paasta_config=self.system_config
            )
            if client is not None:
                return client
        return get_paasta_oapi_client(
            cluster=cluster, system_paasta_config=self.system_config
        )

    def list_instance_names(self, cluster: str, service: str) -> list[str]:
        client = self._get_client_for_cluster(cluster)
        if client is None:
            return []
        response = client.service.list_instances(service=service)
        return sorted(response.get("instances", []))

    def get_instances(self, cluster: str, service: str) -> list[InstanceInfo]:
        client = self._get_client_for_cluster(cluster)
        if client is None:
            return []
        response = client.service.list_instances(service=service)
        instance_names: list[str] = response.get("instances", [])
        with ThreadPoolExecutor(max_workers=20) as pool:
            futures = {
                name: pool.submit(self._get_instance_info, client, service, name)
                for name in sorted(instance_names)
            }
            return [
                result
                for name in sorted(instance_names)
                if (result := futures[name].result()) is not None
            ]

    _INSTANCE_TYPES = (
        "kubernetes_v2",
        "tron",
        "flink",
        "flinkeks",
        "kafkacluster",
        "cassandracluster",
        "cassandraclustereks",
        "adhoc",
    )

    def _detect_instance_type(self, status) -> str:
        for itype in self._INSTANCE_TYPES:
            if status.get(itype) is not None:
                return itype
        return "unknown"

    def _get_instance_info(
        self, client, service: str, instance: str
    ) -> InstanceInfo | None:
        try:
            status = client.service.status_instance(
                service=service, instance=instance, verbose=1, new=True
            )
        except Exception:
            return None
        instance_type = self._detect_instance_type(status)
        tron = status.get("tron")
        if tron is not None:
            action_state = tron.get("action_state", "unknown")
            if len(action_state) > 25:
                action_state = action_state[:25] + "..."
            is_ok = action_state in ("succeeded", "running", "waiting", "scheduled")
            return InstanceInfo(
                name=instance,
                instance_type="tron",
                state=action_state.capitalize(),
                ready=1 if action_state == "succeeded" else 0,
                desired=1,
                git_sha="",
                num_versions=0,
                error="" if is_ok else action_state,
            )
        k8s = status.get("kubernetes_v2")
        if k8s is None:
            raw_state = status.get("desired_state", "Unknown")
            if len(raw_state) > 25:
                raw_state = raw_state[:25] + "..."
            return InstanceInfo(
                name=instance,
                instance_type=instance_type,
                state=raw_state,
                ready=0,
                desired=0,
                git_sha=(status.get("git_sha") or "")[:8],
                num_versions=0,
                error="",
            )
        desired_state = k8s.get("desired_state", "Unknown")
        desired_instances = k8s.get("desired_instances", 0)
        versions = k8s.get("versions", [])
        ready_replicas = sum(v.get("ready_replicas", 0) for v in versions)
        total_replicas = sum(v.get("replicas", 0) for v in versions)
        git_sha = ""
        if versions:
            git_sha = versions[0].get("git_sha", "")[:8]
        if desired_state == "stop":
            if total_replicas == 0:
                state = "Stopped"
            else:
                state = "Stopping"
        elif len(versions) > 1:
            state = "Bouncing"
        elif len(versions) == 1:
            if ready_replicas >= desired_instances and desired_instances > 0:
                state = "Running"
            elif ready_replicas > 0:
                state = "Launching replicas"
            else:
                state = "Starting"
        elif desired_instances == 0:
            state = "Stopped"
        else:
            state = "Starting"
        error = k8s.get("error_message", "") or ""
        if len(error) > 50:
            error = error[:50] + "..."
        return InstanceInfo(
            name=instance,
            instance_type=instance_type,
            state=state,
            ready=ready_replicas,
            desired=desired_instances,
            git_sha=git_sha,
            num_versions=len(versions),
            error=error,
        )
