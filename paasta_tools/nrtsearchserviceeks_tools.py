from typing import Optional

import service_configuration_lib

from paasta_tools.autoscaling.utils import MetricsProviderDict
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_PROMQL
from paasta_tools.nrtsearchservice_tools import NrtsearchServiceDeploymentConfig
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import load_service_instance_config
from paasta_tools.utils import load_v2_deployments_json


class NrtsearchServiceEksDeploymentConfig(NrtsearchServiceDeploymentConfig):
    config_filename_prefix = "nrtsearchserviceeks"

    def _get_autoscalable_server_set(self) -> Optional[dict]:
        """Return the non-primary serverSet that has targetGpuUtilization > 0."""
        server_sets = self.config_dict.get("serverSets", [])
        for server_set in server_sets:
            if server_set.get("primary", False):
                continue
            autoscaling = server_set.get("autoscaling")
            if not autoscaling:
                continue
            if autoscaling.get("targetGpuUtilization", 0) > 0:
                return server_set
        return None

    def get_autoscaling_metrics_provider(
        self, provider_type: str
    ) -> Optional[MetricsProviderDict]:
        if provider_type != METRICS_PROVIDER_PROMQL:
            return None
        server_set = self._get_autoscalable_server_set()
        if server_set is None:
            return None

        instance_name = "replica"
        deployment_name = self.get_sanitised_deployment_name()
        namespace = self.get_namespace()
        paasta_cluster = self.get_cluster()
        service = self.get_service()

        metrics_query = (
            "avg("
            "DCGM_FI_DEV_GPU_UTIL"
            " * on(kube_pod, kube_namespace) group_left()"
            " (kube_pod_labels{"
            f"label_paasta_yelp_com_service='{service}',"
            f"label_yelp_com_paasta_instance='{instance_name}',"
            f"paasta_cluster='{paasta_cluster}'"
            "})"
            ")"
        )
        return MetricsProviderDict(
            type=METRICS_PROVIDER_PROMQL,
            metrics_query=metrics_query,
            series_query=(
                f"kube_deployment_labels{{"
                f"deployment='{deployment_name}',"
                f"paasta_cluster='{paasta_cluster}',"
                f"namespace='{namespace}'"
                f"}}"
            ),
            setpoint=1.0,
        )

    def get_sanitised_deployment_name(self) -> str:
        return f"{self.instance}-replica-dep"

    def namespace_custom_prometheus_metric_name(self, metric_name: str) -> str:
        return f"{self.get_sanitised_deployment_name()}-gpu-prom"


def load_nrtsearchserviceeks_instance_config(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> NrtsearchServiceEksDeploymentConfig:
    """Read a service instance's configuration for Nrtsearch.

    If a branch isn't specified for a config, the 'branch' key defaults to
    paasta-${cluster}.${instance}.

    :param service: The service name
    :param instance: The instance of the service to retrieve
    :param cluster: The cluster to read the configuration for
    :param load_deployments: A boolean indicating if the corresponding deployments.json for this service
                             should also be loaded
    :param soa_dir: The SOA configuration directory to read from
    :returns: A dictionary of whatever was in the config for the service instance"""
    general_config = service_configuration_lib.read_service_configuration(
        service, soa_dir=soa_dir
    )
    instance_config = load_service_instance_config(
        service, instance, "nrtsearchserviceeks", cluster, soa_dir=soa_dir
    )
    general_config = deep_merge_dictionaries(
        overrides=instance_config, defaults=general_config
    )

    branch_dict: Optional[BranchDictV2] = None
    if load_deployments:
        deployments_json = load_v2_deployments_json(service, soa_dir=soa_dir)
        temp_instance_config = NrtsearchServiceEksDeploymentConfig(
            service=service,
            cluster=cluster,
            instance=instance,
            config_dict=general_config,
            branch_dict=None,
            soa_dir=soa_dir,
        )
        branch = temp_instance_config.get_branch()
        deploy_group = temp_instance_config.get_deploy_group()
        branch_dict = deployments_json.get_branch_dict(service, branch, deploy_group)

    return NrtsearchServiceEksDeploymentConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=general_config,
        branch_dict=branch_dict,
        soa_dir=soa_dir,
    )
