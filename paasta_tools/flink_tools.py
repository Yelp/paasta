# Copyright 2015-2019 Yelp Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import glob
import json
import os
import re
import subprocess
from typing import Any
from typing import List
from typing import Mapping
from typing import Optional
from urllib.parse import urljoin
from urllib.parse import urlparse

import requests
import service_configuration_lib
import yaml
from mypy_extensions import TypedDict

from paasta_tools.api import settings
from paasta_tools.api.client import PaastaOApiClient
from paasta_tools.async_utils import async_timeout
from paasta_tools.kubernetes_tools import get_cr
from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.kubernetes_tools import sanitised_cr_name
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import LongRunningServiceConfigDict
from paasta_tools.paastaapi.exceptions import ApiException
from paasta_tools.paastaapi.model.flink_cluster_overview import FlinkClusterOverview
from paasta_tools.paastaapi.model.flink_config import FlinkConfig
from paasta_tools.paastaapi.model.flink_job_details import FlinkJobDetails
from paasta_tools.paastaapi.model.flink_jobs import FlinkJobs
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_service_instance_config
from paasta_tools.utils import load_v2_deployments_json

FLINK_INGRESS_PORT = 31080
FLINK_DASHBOARD_TIMEOUT_SECONDS = 5
CONFIG_KEYS = {"flink-version", "flink-revision"}
OVERVIEW_KEYS = {
    "taskmanagers",
    "slots-total",
    "slots-available",
    "jobs-running",
    "jobs-finished",
    "jobs-cancelled",
    "jobs-failed",
}
JOB_DETAILS_KEYS = {"jid", "name", "state", "start-time"}

# URL constants for external services
PIPELINE_STUDIO_BASE = "https://pipeline_studio_v2.yelpcorp.com"
KAFKA_VIEW_PROD_DOMAIN = "kafka-view.admin.yelp.com"
KAFKA_VIEW_DEVC_DOMAIN = "kafka-view.paasta-norcal-devc.yelp"
GRAFANA_BASE = "https://grafana.yelpcorp.com"
SRV_CONFIGS_FULL_REPO = "/nail/etc/srv-configs/.client/public/ecosystem"


def _safe_str(value: Any) -> Optional[str]:
    """Convert value to string, handling None safely.

    :param value: Value to convert
    :returns: String representation or None
    """
    return str(value) if value is not None else None


class TaskManagerConfig(TypedDict, total=False):
    instances: int


class FlinkDeploymentConfigDict(LongRunningServiceConfigDict, total=False):
    taskmanager: TaskManagerConfig
    spot: bool


class FlinkDeploymentConfig(LongRunningServiceConfig):
    config_dict: FlinkDeploymentConfigDict

    config_filename_prefix = "flink"

    def __init__(
        self,
        service: str,
        cluster: str,
        instance: str,
        config_dict: FlinkDeploymentConfigDict,
        branch_dict: Optional[BranchDictV2],
        soa_dir: str = DEFAULT_SOA_DIR,
    ) -> None:

        super().__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            soa_dir=soa_dir,
            config_dict=config_dict,
            branch_dict=branch_dict,
        )

    def validate(
        self,
        params: List[str] = [
            "cpus",
            "mem",
            "security",
            "dependencies_reference",
            "deploy_group",
        ],
    ) -> List[str]:
        # Use InstanceConfig to validate shared config keys like cpus and mem
        error_msgs = super().validate(params=params)

        if error_msgs:
            name = self.get_instance()
            return [f"{name}: {msg}" for msg in error_msgs]
        else:
            return []

    # Since Flink services are stateful, losing capacity is not transparent to the users
    def get_replication_crit_percentage(self) -> int:
        return self.config_dict.get("replication_threshold", 100)

    def get_pool(self) -> Optional[str]:
        """
        Parses flink_pool from a specific Flink Deployment instance's configuration data, using key 'spot'.

        Args:
            flink_deployment_config_data: The FlinkDeploymentConfig for a specific Flink yelpsoa instance

        Returns:
            The flink pool string.
        """
        spot_config = self.config_dict.get("spot", None)
        if spot_config is False:
            return "flink"
        else:
            # if not set or True, Flink instance defaults to use flink-spot pool
            return "flink-spot"


def load_flink_instance_config(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> FlinkDeploymentConfig:
    """Read a service instance's configuration for Flink.

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
        service, instance, "flink", cluster, soa_dir=soa_dir
    )
    general_config = deep_merge_dictionaries(
        overrides=instance_config, defaults=general_config
    )

    branch_dict: Optional[BranchDictV2] = None
    if load_deployments:
        deployments_json = load_v2_deployments_json(service, soa_dir=soa_dir)
        temp_instance_config = FlinkDeploymentConfig(
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

    return FlinkDeploymentConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=general_config,
        branch_dict=branch_dict,
        soa_dir=soa_dir,
    )


# TODO: read this from CRD in service configs
def cr_id(service: str, instance: str) -> Mapping[str, str]:
    return dict(
        group="yelp.com",
        version="v1alpha1",
        namespace="paasta-flinks",
        plural="flinks",
        name=sanitised_cr_name(service, instance),
    )


def get_flink_ingress_url_root(cluster: str, is_eks: bool) -> str:
    if is_eks:
        return f"http://flink.eks.{cluster}.paasta:{FLINK_INGRESS_PORT}/"
    else:
        return f"http://flink.k8s.{cluster}.paasta:{FLINK_INGRESS_PORT}/"


def _dashboard_get(cr_name: str, cluster: str, path: str, is_eks: bool) -> str:
    root = get_flink_ingress_url_root(cluster, is_eks)
    url = f"{root}{cr_name}/{path}"
    response = requests.get(url, timeout=FLINK_DASHBOARD_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text


def _filter_for_endpoint(json_response: Any, endpoint: str) -> Mapping[str, Any]:
    """
    Filter json response to include only a subset of fields.
    """
    if endpoint == "config":
        return {
            key: value for (key, value) in json_response.items() if key in CONFIG_KEYS
        }
    if endpoint == "overview":
        return {
            key: value for (key, value) in json_response.items() if key in OVERVIEW_KEYS
        }
    if endpoint == "jobs":
        return json_response
    if endpoint.startswith("jobs"):
        return {
            key: value
            for (key, value) in json_response.items()
            if key in JOB_DETAILS_KEYS
        }
    return json_response


def _get_jm_rest_api_base_url(cr: Mapping[str, Any]) -> str:
    metadata = cr["metadata"]
    cluster = metadata["labels"][paasta_prefixed("cluster")]
    is_eks = metadata["labels"].get("paasta.yelp.com/eks", "False")
    base_url = get_flink_ingress_url_root(cluster, is_eks == "True")

    # this will look something like http://flink-jobmanager-host:port/paasta-service-cr-name
    _, _, service_cr_name, *_ = urlparse(
        metadata["annotations"]["flink.yelp.com/dashboard_url"]
    )

    return urljoin(base_url, service_cr_name)


def curl_flink_endpoint(cr_id: Mapping[str, str], endpoint: str) -> Mapping[str, Any]:
    try:
        cr = get_cr(settings.kubernetes_client, cr_id)
        if cr is None:
            raise ValueError(f"failed to get CR for id: {cr_id}")
        base_url = _get_jm_rest_api_base_url(cr)

        # Closing 'base_url' with '/' to force urljoin to append 'endpoint' to the path.
        # If not, urljoin replaces the 'base_url' path with 'endpoint'.
        url = urljoin(base_url + "/", endpoint)
        response = requests.get(url, timeout=FLINK_DASHBOARD_TIMEOUT_SECONDS)
        if not response.ok:
            return {
                "status": response.status_code,
                "error": response.reason,
                "text": response.text,
            }
        return _filter_for_endpoint(response.json(), endpoint)
    except requests.RequestException as e:
        url = e.request.url
        err = e.response or str(e)
        raise ValueError(f"failed HTTP request to flink API {url}: {err}")
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON decoding error from flink API: {e}")
    except ConnectionError as e:
        raise ValueError(f"failed HTTP request to flink API: {e}")
    except ApiException as e:
        raise ValueError(f"failed HTTP request to flink API: {e}")


def get_flink_jobmanager_overview(
    cr_name: str, cluster: str, is_eks: bool
) -> Mapping[str, Any]:
    try:
        response = _dashboard_get(cr_name, cluster, "overview", is_eks)
        return json.loads(response)
    except requests.RequestException as e:
        url = e.request.url
        err = e.response or str(e)
        raise ValueError(f"failed HTTP request to Jobmanager dashboard {url}: {err}")
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON decoding error from Jobmanager dashboard: {e}")
    except ConnectionError as e:
        raise ValueError(f"failed HTTP request to Jobmanager dashboard: {e}")


def get_flink_jobs_from_paasta_api_client(
    service: str, instance: str, client: PaastaOApiClient
) -> FlinkJobs:
    """Get flink jobs for (service, instance) pair by connecting to the paasta api endpoint.

    Appends exception to output list if any.

    :param service: The service name
    :param instance: The instance of the service to retrieve
    :param client: The paasta api client
    :returns: Flink jobs in the flink cluster"""
    return client.service.list_flink_cluster_jobs(
        service=service,
        instance=instance,
    )


@async_timeout()
async def get_flink_job_details_from_paasta_api_client(
    service: str, instance: str, job_id: str, client: PaastaOApiClient
) -> FlinkJobDetails:
    """Get flink job details for (service, instance) pair by connecting to the paasta api endpoint.

    Appends exception to output list if any.

    :param service: The service name
    :param instance: The instance of the service to retrieve
    :param client: The paasta api client
    :returns: Flink jobs in the flink cluster"""
    return client.service.get_flink_cluster_job_details(
        service=service,
        instance=instance,
        job_id=job_id,
    )


def get_flink_config_from_paasta_api_client(
    service: str, instance: str, client: PaastaOApiClient
) -> FlinkConfig:
    """Get flink config for (service, instance) pair by connecting to the paasta api endpoint.

    Appends exception to output list if any.

    :param service: The service name
    :param instance: The instance of the service to retrieve
    :param client: The paasta api client
    :returns: Flink cluster configurations"""
    return client.service.get_flink_cluster_config(
        service=service,
        instance=instance,
    )


def get_flink_overview_from_paasta_api_client(
    service: str, instance: str, client: PaastaOApiClient
) -> FlinkClusterOverview:
    """Get flink cluster overview for (service, instance) pair by connecting to the paasta api endpoint.

    Appends exception to output list if any.

    :param service: The service name
    :param instance: The instance of the service to retrieve
    :param client: The paasta api client
    :returns: Flink cluster overview"""
    return client.service.get_flink_cluster_overview(
        service=service,
        instance=instance,
    )


def get_sqlclient_job_config(
    service: str,
    instance: str,
    cluster: str,
    job_name: Optional[str] = None,
) -> Mapping[str, Any]:
    """Get job configuration for a SQLClient Flink job from srv-configs.

    :param service: The service name (should be 'sqlclient')
    :param instance: The instance name
    :param cluster: The cluster name
    :param job_name: Optional job name (if different from instance)
    :returns: Dict with job config including parallelism, sources, sinks
    """
    try:
        # Read job config from hiera-merged srv-configs
        # Path: /nail/srv/configs/{service}/{instance}/job.d/{job_name}.yaml
        # Note: SQLClient instances can have multiple jobs
        from paasta_tools.utils import load_system_paasta_config
        from service_configuration_lib import read_yaml_file

        system_paasta_config = load_system_paasta_config()
        ecosystem = system_paasta_config.get_ecosystem_for_cluster(cluster)

        # Read from full srv-configs repo (has all instances, not just deployed ones)
        # Path: /nail/etc/srv-configs/.client/public/ecosystem/{ecosystem}/{service}/{instance}/job.d/
        job_d_path = os.path.join(
            SRV_CONFIGS_FULL_REPO,
            str(ecosystem),
            str(service),
            str(instance),
            "job.d",
        )

        if not os.path.exists(job_d_path):
            return {
                "error": f"Job config directory not found for instance '{instance}'."
            }

        # If job_name is provided, use it; otherwise try instance name or find first YAML
        if job_name:
            job_config_path = os.path.join(job_d_path, f"{job_name}.yaml")
        else:
            # Try instance name first
            job_config_path = os.path.join(job_d_path, f"{instance}.yaml")

        # If specific file doesn't exist, find any YAML file in job.d
        if not os.path.exists(job_config_path):
            yaml_files = glob.glob(os.path.join(job_d_path, "*.yaml"))
            if yaml_files:
                job_config_path = yaml_files[0]  # Use first YAML file found
            else:
                return {
                    "error": f"No job config files found in {job_d_path}"
                }

        # Use service_configuration_lib to read YAML (handles caching, validation, etc.)
        config = read_yaml_file(job_config_path)

        sources = config.get("sources", [])
        sinks = config.get("sinks", [])
        parallelism = config.get("parallelism")
        udf_config = config.get("udf_config")

        # Query datapipe for each source
        sources_info = []
        for source in sources:
            source_config = source.get("config", {})
            table_name = source.get("table_name")

            schema_id = source_config.get("schema_id")
            namespace = source_config.get("namespace")
            source_name = source_config.get("source")
            alias = source_config.get("alias")

            # Convert all values to strings to handle YAML float parsing (e.g., "2.0" -> 2.0)
            info = {
                "table_name": _safe_str(table_name),
                "schema_id": int(schema_id) if schema_id else None,
                "namespace": _safe_str(namespace),
                "source": _safe_str(source_name),
                "alias": _safe_str(alias),
            }

            sources_info.append(info)

        # Query datapipe for each sink
        sinks_info = []
        for sink in sinks:
            sink_config = sink.get("config", {})
            table_name = sink.get("table_name")

            namespace = sink_config.get("namespace")
            source_name = sink_config.get("source")
            alias = sink_config.get("alias")
            pkeys = sink_config.get("pkeys")

            # Convert all values to strings to handle YAML float parsing
            info = {
                "table_name": _safe_str(table_name),
                "namespace": _safe_str(namespace),
                "source": _safe_str(source_name),
                "alias": _safe_str(alias),
                "pkeys": _safe_str(pkeys),
            }

            sinks_info.append(info)

        return {
            "sources": sources_info,
            "sinks": sinks_info,
            "ecosystem": ecosystem,
            "parallelism": parallelism,
            "udf_config": udf_config,
        }

    except Exception as e:
        import traceback
        return {"error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()}


def get_sqlclient_parallelism(
    service: str,
    instance: str,
    cluster: str,
) -> Optional[int]:
    """Get parallelism value for a SQLClient Flink job.

    :param service: The service name (should be 'sqlclient')
    :param instance: The instance name
    :param cluster: The cluster name
    :returns: Parallelism value or None if not found
    """
    job_config = get_sqlclient_job_config(service, instance, cluster)
    return job_config.get("parallelism")


def get_sqlclient_udf_plugin(
    service: str,
    instance: str,
    cluster: str,
) -> Optional[str]:
    """Get UDF plugin name for a SQLClient Flink job.

    :param service: The service name (should be 'sqlclient')
    :param instance: The instance name
    :param cluster: The cluster name
    :returns: UDF plugin name or None if not configured
    """
    job_config = get_sqlclient_job_config(service, instance, cluster)
    udf_config = job_config.get("udf_config")
    if udf_config and isinstance(udf_config, dict):
        return udf_config.get("plugin_name")
    return None


def analyze_slot_utilization(
    overview: FlinkClusterOverview,
    instance_config: FlinkDeploymentConfig,
) -> Mapping[str, Any]:
    """Analyze Flink slot utilization and suggest optimizations.

    :param overview: Flink cluster overview with slot information
    :param instance_config: Flink instance configuration
    :returns: Dict with current config, utilization, and recommendations
    """
    total_slots = overview.slots_total
    available_slots = overview.slots_available
    used_slots = total_slots - available_slots
    taskmanagers = overview.taskmanagers

    # Calculate utilization
    utilization_pct = (used_slots / total_slots * 100) if total_slots > 0 else 0

    # Get current config from yelpsoa-configs
    taskmanager_config = instance_config.config_dict.get("taskmanager", {})
    current_instances = taskmanager_config.get("instances", taskmanagers)

    # Calculate slots per taskmanager
    slots_per_tm = total_slots // taskmanagers if taskmanagers > 0 else 1

    analysis = {
        "current_instances": current_instances,
        "current_slots_per_tm": slots_per_tm,
        "total_slots": total_slots,
        "used_slots": used_slots,
        "idle_slots": available_slots,
        "utilization_pct": utilization_pct,
    }

    # Suggest optimization if utilization is very low (< 50%) or very high (> 90%)
    recommendation = None

    if utilization_pct < 50 and used_slots > 0:
        # Too many idle slots - suggest reducing instances
        # Target 75% utilization with at least 1 slot buffer
        target_slots = max(used_slots + 1, int(used_slots / 0.75))
        recommended_instances = max(1, (target_slots + slots_per_tm - 1) // slots_per_tm)

        if recommended_instances < current_instances:
            recommendation = {
                "action": "reduce",
                "new_instances": recommended_instances,
                "new_slots_per_tm": slots_per_tm,
                "new_total_slots": recommended_instances * slots_per_tm,
                "expected_utilization": (used_slots / (recommended_instances * slots_per_tm) * 100) if (recommended_instances * slots_per_tm) > 0 else 0,
            }

    elif utilization_pct > 90:
        # Too high utilization - suggest adding capacity
        # Target 75% utilization
        target_slots = int(used_slots / 0.75)
        recommended_instances = max(current_instances, (target_slots + slots_per_tm - 1) // slots_per_tm)

        if recommended_instances > current_instances:
            recommendation = {
                "action": "increase",
                "new_instances": recommended_instances,
                "new_slots_per_tm": slots_per_tm,
                "new_total_slots": recommended_instances * slots_per_tm,
                "expected_utilization": (used_slots / (recommended_instances * slots_per_tm) * 100) if (recommended_instances * slots_per_tm) > 0 else 0,
            }

    analysis["recommendation"] = recommendation
    return analysis


def format_resource_optimization(
    service: str,
    instance: str,
    overview: FlinkClusterOverview,
    instance_config: FlinkDeploymentConfig,
) -> List[str]:
    """Format resource optimization suggestions for display.

    :param service: The service name
    :param instance: The instance name
    :param overview: Flink cluster overview
    :param instance_config: Flink instance configuration
    :returns: List of formatted strings for output
    """
    output = []

    analysis = analyze_slot_utilization(overview, instance_config)

    output.append("    Resource Utilization & Optimization:")
    output.append("      Current Configuration:")
    output.append(f"        Taskmanagers:     {analysis['current_instances']} instances")
    output.append(f"        Slots per TM:     {analysis['current_slots_per_tm']} slots")
    output.append(f"        Total Slots:      {analysis['total_slots']} slots")
    output.append(f"        Used Slots:       {analysis['used_slots']} slots ({analysis['utilization_pct']:.0f}% utilization)")
    output.append(f"        Idle Slots:       {analysis['idle_slots']} slots")

    recommendation = analysis.get("recommendation")
    if recommendation and recommendation["action"] == "reduce":
        output.append("")
        output.append("      💡 OPTIMIZATION OPPORTUNITY:")
        output.append(f"        Recommended:      {recommendation['new_instances']} taskmanagers × {recommendation['new_slots_per_tm']} slots = {recommendation['new_total_slots']} total slots")
        output.append(f"        Expected Usage:   {analysis['used_slots']} slots ({recommendation['expected_utilization']:.0f}% utilization)")
        output.append(f"        Benefit:          Reduce overprovisioning, save resources")

        output.append("")
        output.append("      Configuration Changes Needed:")
        output.append("        taskmanager:")
        output.append(f"          instances: {recommendation['new_instances']}  # currently: {analysis['current_instances']}")

        if recommendation['new_slots_per_tm'] != analysis['current_slots_per_tm']:
            output.append(f"          taskSlots: {recommendation['new_slots_per_tm']}  # currently: {analysis['current_slots_per_tm']}")
        else:
            output.append(f"          # taskSlots: {analysis['current_slots_per_tm']}  (no change needed)")
    else:
        output.append("")
        output.append("      ✅ Resource utilization is optimal")

    return output


def _format_topic_links_and_commands(
    schema_id: Optional[int],
    namespace: Optional[str],
    source_name: Optional[str],
    alias: Optional[str],
    ecosystem: str,
) -> List[str]:
    """Format Pipeline Studio link, describe command, and tail command for a topic.

    :param schema_id: Schema ID (if available)
    :param namespace: Namespace name
    :param source_name: Source name
    :param alias: Alias version
    :param ecosystem: Ecosystem (prod, devc, etc.)
    :returns: List of formatted strings
    """
    output = []

    # Pipeline Studio link
    if schema_id and not (namespace and source_name):
        # Use schema_id based URL when only schema_id is available
        pipeline_url = f"{PIPELINE_STUDIO_BASE}/?search_by=2&ecosystem={ecosystem}&schema_id={schema_id}"
        output.append(f"         Pipeline Studio: {pipeline_url}")
    elif namespace and source_name:
        # Use namespace/source based URL when available
        pipeline_url = f"{PIPELINE_STUDIO_BASE}/namespaces/{namespace}/sources/{source_name}/asset-details"
        if alias:
            pipeline_url += f"?alias={alias}"
        output.append(f"         Pipeline Studio: {pipeline_url}")

    # Schema describe command
    if schema_id:
        describe_cmd = f"datapipe schema describe --schema-id {schema_id}"
    elif namespace and source_name:
        describe_cmd = f"datapipe schema describe --namespace {namespace} --source {source_name}"
        if alias:
            describe_cmd += f" --alias {alias}"
    else:
        describe_cmd = None

    if describe_cmd:
        output.append(f"         Describe:        {describe_cmd}")

    # Datapipe tail command
    if schema_id:
        tail_cmd = f"datapipe stream tail --schema-id {schema_id} --all-fields --json"
    elif namespace and source_name:
        tail_cmd = f"datapipe stream tail --namespace {namespace} --source {source_name}"
        if alias:
            tail_cmd += f" --alias {alias}"
        tail_cmd += " --all-fields --json"
    else:
        tail_cmd = None

    if tail_cmd:
        output.append(f"         Tail:            {tail_cmd}")

    return output


def _format_consumer_group_info(
    service: str,
    instance: str,
    job_name: str,
    ecosystem: str,
) -> List[str]:
    """Format consumer group information with monitoring links.

    :param service: The service name
    :param instance: The instance name
    :param job_name: The Flink job name
    :param ecosystem: Ecosystem (prod, devc, etc.)
    :returns: List of formatted strings
    """
    output = []
    consumer_group = f"flink.{service}.{instance}.{job_name}"
    output.append(f"      Consumer Group: {consumer_group}")

    # Determine Kafka View domain and cluster based on ecosystem
    if ecosystem == "prod":
        kafka_view_domain = KAFKA_VIEW_PROD_DOMAIN
        kafka_cluster = "scribe.uswest2-prod"
    else:
        kafka_view_domain = KAFKA_VIEW_DEVC_DOMAIN
        kafka_cluster = f"buff-high.uswest1-{ecosystem}"

    kafka_view_url = f"http://{kafka_view_domain}/clusters/{kafka_cluster}/groups/{consumer_group}"
    output.append(f"        Kafka View: {kafka_view_url}")

    # Grafana consumer metrics link
    grafana_url = f"{GRAFANA_BASE}/d/kcHXkIBnz/consumer-metrics?orgId=1&var-cluster_type=All&var-cluster_name=All&var-consumergroup={consumer_group}&var-topic=.%2A"
    output.append(f"        Grafana: {grafana_url}")

    return output


def _format_source_topics(sources: List[Mapping[str, Any]], ecosystem: str) -> List[str]:
    """Format source topics with details and links.

    :param sources: List of source topic info dicts
    :param ecosystem: Ecosystem (prod, devc, etc.)
    :returns: List of formatted strings
    """
    output = []
    if not sources:
        return output

    output.append("")
    output.append("    Source Topics:")
    for idx, source in enumerate(sources, 1):
        table_name = source.get("table_name", "unknown")
        schema_id = source.get("schema_id")
        namespace = source.get("namespace")
        source_name = source.get("source")
        alias = source.get("alias")

        output.append(f"      {idx}. {table_name}")

        if schema_id or namespace:
            if schema_id:
                output.append(f"         Schema ID:       {schema_id}")
            if namespace:
                output.append(f"         Namespace:       {namespace}")
            if source_name:
                output.append(f"         Source:          {source_name}")
            if alias:
                output.append(f"         Alias:           {alias}")

            # Use helper to format links and commands
            output.extend(_format_topic_links_and_commands(
                schema_id, namespace, source_name, alias, ecosystem
            ))

        output.append("")

    return output


def _format_sink_topics(sinks: List[Mapping[str, Any]], ecosystem: str) -> List[str]:
    """Format sink topics with details and links.

    :param sinks: List of sink topic info dicts
    :param ecosystem: Ecosystem (prod, devc, etc.)
    :returns: List of formatted strings
    """
    output = []
    if not sinks:
        return output

    output.append("    Sink Topics:")
    for idx, sink in enumerate(sinks, 1):
        table_name = sink.get("table_name", "unknown")
        namespace = sink.get("namespace")
        source_name = sink.get("source")
        alias = sink.get("alias")
        pkeys = sink.get("pkeys")

        output.append(f"      {idx}. {table_name}")

        if namespace:
            output.append(f"         Namespace:       {namespace}")
        if source_name:
            output.append(f"         Source:          {source_name}")
        if alias:
            output.append(f"         Alias:           {alias}")
        if pkeys:
            output.append(f"         Primary Keys:    {pkeys}")

        # Use helper to format links and commands
        if namespace and source_name:
            output.extend(_format_topic_links_and_commands(
                None, namespace, source_name, alias, ecosystem
            ))

        output.append("")

    return output


def format_kafka_topics(
    service: str,
    instance: str,
    cluster: str,
    job_name: Optional[str] = None,
) -> List[str]:
    """Format Kafka topic information for SQLClient Flink jobs.

    :param service: The service name
    :param instance: The instance name
    :param cluster: The cluster name
    :param job_name: The Flink job name (for consumer group)
    :returns: List of formatted strings for output
    """
    output = []

    # Get topic information
    topics_info = get_sqlclient_job_config(service, instance, cluster)

    if "error" in topics_info:
        error_msg = topics_info['error']
        output.append(f"    Kafka Topics: Unable to fetch")
        output.append(f"      Error: {error_msg}")
        if "traceback" in topics_info:
            output.append(f"      Debug traceback available (check logs)")
        return output

    sources = topics_info.get("sources", [])
    sinks = topics_info.get("sinks", [])
    ecosystem = topics_info.get("ecosystem", "prod")

    # Header
    output.append(f"    Data Pipeline Topology:")
    output.append(f"      Sources: {len(sources)} topics")
    output.append(f"      Sinks:   {len(sinks)} topics")

    # Consumer group info
    if job_name:
        output.extend(_format_consumer_group_info(service, instance, job_name, ecosystem))

    # Source topics
    output.extend(_format_source_topics(sources, ecosystem))

    # Sink topics
    output.extend(_format_sink_topics(sinks, ecosystem))

    return output
