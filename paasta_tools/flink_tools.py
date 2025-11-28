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
import json
import shutil
from datetime import datetime
from itertools import groupby
from typing import Any
from typing import List
from typing import Mapping
from typing import Optional
from urllib.parse import urljoin
from urllib.parse import urlparse

import humanize
import requests
import service_configuration_lib
from mypy_extensions import TypedDict

from paasta_tools.api import settings
from paasta_tools.api.client import PaastaOApiClient
from paasta_tools.async_utils import async_timeout
from paasta_tools.kubernetes_tools import get_cr
from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.kubernetes_tools import sanitised_cr_name
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import LongRunningServiceConfigDict
from paasta_tools.monitoring_tools import get_runbook
from paasta_tools.monitoring_tools import get_team
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
from paasta_tools.utils import PaastaColors

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


class TaskManagerConfig(TypedDict, total=False):
    instances: int


class PodCounts(TypedDict):
    """Pod count statistics."""

    running: int
    evicted: int
    other: int
    total: int


class JobCounts(TypedDict):
    """Job count statistics."""

    running: int
    finished: int
    failed: int
    cancelled: int
    total: int


class FlinkJobDetailsDict(TypedDict):
    """Collected Flink job details dictionary."""

    state: str
    pod_counts: PodCounts
    job_counts: Optional[JobCounts]
    taskmanagers: Optional[int]
    slots_available: Optional[int]
    slots_total: Optional[int]
    jobs: List[FlinkJobDetails]


class FlinkInstanceDetails(TypedDict):
    """Flink instance metadata dictionary."""

    config_sha: str
    version: Optional[str]
    version_revision: Optional[str]
    dashboard_url: Optional[str]
    pool: str
    team: str
    runbook: str


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


def get_flink_instance_details(
    metadata: Mapping[str, Any],
    flink_config: Optional[FlinkConfig],
    flink_instance_config: FlinkDeploymentConfig,
    service: str,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> FlinkInstanceDetails:
    """Collect Flink instance metadata and configuration details.

    :param metadata: Kubernetes metadata from flink status
    :param flink_config: Flink configuration (None if not running)
    :param flink_instance_config: Flink instance config from yelpsoa-configs
    :param service: Service name
    :param soa_dir: SOA directory path
    :returns: Dict with instance details
    """
    labels = metadata.get("labels", {})
    annotations = metadata.get("annotations", {})

    # Validate that config_sha exists
    config_sha = labels.get(paasta_prefixed("config_sha"))
    if config_sha is None:
        raise ValueError(f"expected config sha on Flink, but received {metadata}")

    version = flink_config.flink_version if flink_config else None
    version_revision = flink_config.flink_revision if flink_config else None

    dashboard_url = annotations.get("flink.yelp.com/dashboard_url")

    pool = flink_instance_config.get_pool()
    # Use per-instance monitoring config if set, otherwise fall back to service-level config
    team = flink_instance_config.get_team() or get_team(
        overrides={}, service=service, soa_dir=soa_dir
    )
    runbook = flink_instance_config.get_runbook() or get_runbook(
        overrides={}, service=service, soa_dir=soa_dir
    )

    return {
        "config_sha": config_sha,
        "version": version,
        "version_revision": version_revision,
        "dashboard_url": dashboard_url,
        "pool": pool,
        "team": team,
        "runbook": runbook,
    }


def format_flink_instance_header(
    details: FlinkInstanceDetails, verbose: int
) -> List[str]:
    """Format basic instance information (config SHA, version, URL).

    :param details: Instance details from get_flink_instance_details()
    :param verbose: Verbosity level (>0 shows version revision)
    :returns: List of formatted strings
    """
    output: List[str] = []

    # Config SHA (always shown)
    if details.get("config_sha"):
        output.append(f"    Config SHA: {details['config_sha']}")

    # Version (with optional revision)
    if details.get("version"):
        if verbose and details.get("version_revision"):
            output.append(
                f"    Flink version: {details['version']} {details['version_revision']}"
            )
        else:
            output.append(f"    Flink version: {details['version']}")

    # Dashboard URL
    if details.get("dashboard_url"):
        output.append(f"    URL: {details['dashboard_url']}/")

    return output


def format_flink_instance_metadata(
    details: FlinkInstanceDetails, service: str
) -> List[str]:
    """Format verbose instance metadata (repo links, pool, owner, runbook).

    :param details: Instance details from get_flink_instance_details()
    :param service: Service name
    :returns: List of formatted strings
    """
    output: List[str] = []

    # Repo links
    output.append(f"    Repo(git): https://github.yelpcorp.com/services/{service}")
    output.append(
        f"    Repo(sourcegraph): https://sourcegraph.yelpcorp.com/services/{service}"
    )

    # Pool, owner, runbook
    if details.get("pool"):
        output.append(f"    Flink Pool: {details['pool']}")
    if details.get("team"):
        output.append(f"    Owner: {details['team']}")
    if details.get("runbook"):
        output.append(f"    Flink Runbook: {details['runbook']}")

    return output


def format_flink_config_links(service: str, instance: str, ecosystem: str) -> List[str]:
    """Format configuration repository links.

    :param service: Service name
    :param instance: Instance name
    :param ecosystem: Ecosystem (prod, devc, etc.)
    :returns: List of formatted strings
    """
    output: List[str] = []

    output.append(
        f"    Yelpsoa configs: https://github.yelpcorp.com/sysgit/yelpsoa-configs/tree/master/{service}"
    )
    output.append(
        f"    Srv configs: https://github.yelpcorp.com/sysgit/srv-configs/tree/master/ecosystem/{ecosystem}/{service}"
    )

    return output


def format_flink_log_commands(service: str, instance: str, cluster: str) -> List[str]:
    """Format paasta logs commands.

    :param service: Service name
    :param instance: Instance name
    :param cluster: Cluster name
    :returns: List of formatted strings
    """
    output: List[str] = []

    output.append("    Flink Log Commands:")
    output.append(
        f"      Service:     paasta logs -a 1h -c {cluster} -s {service} -i {instance}"
    )
    output.append(
        f"      Taskmanager: paasta logs -a 1h -c {cluster} -s {service} -i {instance}.TASKMANAGER"
    )
    output.append(
        f"      Jobmanager:  paasta logs -a 1h -c {cluster} -s {service} -i {instance}.JOBMANAGER"
    )
    output.append(
        f"      Supervisor:  paasta logs -a 1h -c {cluster} -s {service} -i {instance}.SUPERVISOR"
    )

    return output


def format_flink_monitoring_links(
    service: str, instance: str, ecosystem: str, cluster: str
) -> List[str]:
    """Format Grafana and cost monitoring links.

    :param service: Service name
    :param instance: Instance name
    :param ecosystem: Ecosystem (prod, devc, etc.)
    :param cluster: Cluster name
    :returns: List of formatted strings
    """
    output: List[str] = []

    output.append("    Flink Monitoring:")
    output.append(
        f"      Job Metrics: https://grafana.yelpcorp.com/d/flink-metrics/flink-job-metrics?orgId=1&var-datasource=Prometheus-flink&var-region=uswest2-{ecosystem}&var-service={service}&var-instance={instance}&var-job=All&from=now-24h&to=now"
    )
    output.append(
        f"      Container Metrics: https://grafana.yelpcorp.com/d/flink-container-metrics/flink-container-metrics?orgId=1&var-datasource=Prometheus-flink&var-region=uswest2-{ecosystem}&var-service={service}&var-instance={instance}&from=now-24h&to=now"
    )
    output.append(
        f"      JVM Metrics: https://grafana.yelpcorp.com/d/flink-jvm-metrics/flink-jvm-metrics?orgId=1&var-datasource=Prometheus-flink&var-region=uswest2-{ecosystem}&var-service={service}&var-instance={instance}&from=now-24h&to=now"
    )
    output.append(
        f"      Flink Cost: https://app.cloudzero.com/explorer?activeCostType=invoiced_amortized_cost&partitions=costcontext%3AResource%20Summary&dateRange=Last%2030%20Days&costcontext%3AKube%20Paasta%20Cluster={cluster}&costcontext%3APaasta%20Instance={instance}&costcontext%3APaasta%20Service={service}&showRightFlyout=filters"
    )

    return output


def collect_flink_job_details(
    status: Mapping[str, Any],
    overview: Optional[FlinkClusterOverview],
    jobs: List[FlinkJobDetails],
) -> FlinkJobDetailsDict:
    """Collect job, pod, and resource information.

    :param status: Status dict from flink CR containing state and pod_status
    :param overview: Flink cluster overview (or None if not running)
    :param jobs: List of Flink job details
    :returns: Dictionary containing:
        - state: Cluster state string
        - pod_counts: Dict with running, evicted, other, total pod counts
        - job_counts: Dict with running, finished, failed, cancelled, total job counts (or None)
        - taskmanagers: Number of taskmanagers (or None)
        - slots_available: Number of available slots (or None)
        - slots_total: Total number of slots (or None)
        - jobs: List of FlinkJobDetails
    """
    # Collect pod counts
    pod_running_count = 0
    pod_evicted_count = 0
    pod_other_count = 0

    for pod in status.get("pod_status", []):
        if pod["phase"] == "Running":
            pod_running_count += 1
        elif pod["phase"] == "Failed" and pod.get("reason") == "Evicted":
            pod_evicted_count += 1
        else:
            pod_other_count += 1

    pods_total_count = pod_running_count + pod_evicted_count + pod_other_count

    pod_counts: PodCounts = {
        "running": pod_running_count,
        "evicted": pod_evicted_count,
        "other": pod_other_count,
        "total": pods_total_count,
    }

    # Collect job counts if overview is available
    job_counts: Optional[JobCounts] = None
    taskmanagers = None
    slots_available = None
    slots_total = None

    if overview:
        jobs_total_count = (
            overview.jobs_running
            + overview.jobs_finished
            + overview.jobs_failed
            + overview.jobs_cancelled
        )
        job_counts = {
            "running": overview.jobs_running,
            "finished": overview.jobs_finished,
            "failed": overview.jobs_failed,
            "cancelled": overview.jobs_cancelled,
            "total": jobs_total_count,
        }
        taskmanagers = overview.taskmanagers
        slots_available = overview.slots_available
        slots_total = overview.slots_total

    return {
        "state": status["state"],
        "pod_counts": pod_counts,
        "job_counts": job_counts,
        "taskmanagers": taskmanagers,
        "slots_available": slots_available,
        "slots_total": slots_total,
        "jobs": jobs,
    }


def format_flink_state_and_pods(job_details: FlinkJobDetailsDict) -> List[str]:
    """Format state, pods, jobs summary, taskmanagers, and slots.

    :param job_details: Collected job details from collect_flink_job_details()
    :returns: List of formatted strings like:
        State: Running
        Pods: 3 running, 0 evicted, 0 other, 3 total
        Jobs: 1 running, 0 finished, 0 failed, 0 cancelled, 1 total
        1 taskmanagers, 0/1 slots available
    """
    output: List[str] = []

    state = job_details["state"]
    pod_counts = job_details["pod_counts"]
    job_counts = job_details.get("job_counts")
    taskmanagers = job_details.get("taskmanagers")
    slots_available = job_details.get("slots_available")
    slots_total = job_details.get("slots_total")

    # Format state with color
    color = PaastaColors.green if state == "running" else PaastaColors.yellow
    output.append(f"    State: {color(state.title())}")

    # Format pod counts
    formatted_evictions = (
        PaastaColors.red(f"{pod_counts['evicted']}")
        if pod_counts["evicted"] > 0
        else f"{pod_counts['evicted']}"
    )
    output.append(
        "    Pods:"
        f" {pod_counts['running']} running,"
        f" {formatted_evictions} evicted,"
        f" {pod_counts['other']} other,"
        f" {pod_counts['total']} total"
    )

    # Format job counts if available
    if job_counts is not None:
        output.append(
            "    Jobs:"
            f" {job_counts['running']} running,"
            f" {job_counts['finished']} finished,"
            f" {job_counts['failed']} failed,"
            f" {job_counts['cancelled']} cancelled,"
            f" {job_counts['total']} total"
        )

    # Format taskmanagers and slots if available
    if (
        taskmanagers is not None
        and slots_available is not None
        and slots_total is not None
    ):
        output.append(
            "    "
            f" {taskmanagers} taskmanagers,"
            f" {slots_available}/{slots_total} slots available"
        )

    return output


def get_flink_job_name(flink_job: FlinkJobDetails) -> str:
    """Extract the job name from a Flink job details object.

    :param flink_job: Flink job details
    :returns: Job name extracted from the full job name
    """
    return flink_job["name"].split(".", 2)[-1]


def format_flink_jobs_table(
    jobs: List[FlinkJobDetails],
    dashboard_url: str,
    verbose: int,
) -> List[str]:
    """Format jobs table with job details.

    :param jobs: List of Flink job details
    :param dashboard_url: Base Flink dashboard URL
    :param verbose: Verbosity level (>1 shows job ID and dashboard URL)
    :returns: Formatted table lines like:
        Jobs:
          Job Name  State       Job ID                           Started
          happyhour Running c654f6a0238dc957aa3faf70cd759b0f 2025-11-17 15:20:52 (16 hours ago)
    """
    output: List[str] = []

    # Calculate max job name length for column width
    if jobs:
        max_job_name_length = max([len(get_flink_job_name(job)) for job in jobs])
    else:
        max_job_name_length = 10

    # Apart from this column total length of one row is around 52 columns, using remaining terminal columns for job name
    # Note: for terminals smaller than 90 columns the row will overflow in verbose printing
    allowed_max_job_name_length = min(
        max(10, shutil.get_terminal_size().columns - 52), max_job_name_length
    )

    # Print table header
    output.append("    Jobs:")
    if verbose > 1:
        output.append(
            f'      {"Job Name": <{allowed_max_job_name_length}} State       Job ID                           Started'
        )
    else:
        output.append(
            f'      {"Job Name": <{allowed_max_job_name_length}} State       Started'
        )

    # Get unique jobs (most recent per job name)
    unique_jobs = (
        sorted(jobs, key=lambda j: -j["start_time"])[0]  # type: ignore
        for _, jobs in groupby(
            sorted(
                (j for j in jobs if j.get("name") and j.get("start_time")),
                key=lambda j: j["name"],
            ),
            lambda j: j["name"],
        )
    )

    # Print job rows
    allowed_max_jobs_printed = 3
    job_printed_count = 0

    for job in unique_jobs:
        job_id = job["jid"]
        if verbose > 1:
            fmt = """      {job_name: <{allowed_max_job_name_length}.{allowed_max_job_name_length}} {state: <11} {job_id} {start_time}
        {dashboard_url}"""
        else:
            fmt = "      {job_name: <{allowed_max_job_name_length}.{allowed_max_job_name_length}} {state: <11} {start_time}"

        start_time = datetime.fromtimestamp(int(job["start_time"]) // 1000)

        if verbose or job_printed_count < allowed_max_jobs_printed:
            job_printed_count += 1
            color_fn = (
                PaastaColors.green
                if job.get("state") and job.get("state") == "RUNNING"
                else PaastaColors.red
                if job.get("state") and job.get("state") in ("FAILED", "FAILING")
                else PaastaColors.yellow
            )
            job_info_str = fmt.format(
                job_id=job_id,
                job_name=get_flink_job_name(job),
                allowed_max_job_name_length=allowed_max_job_name_length,
                state=color_fn((job.get("state") or "Unknown").title()),
                start_time=f"{str(start_time)} ({humanize.naturaltime(start_time)})",
                dashboard_url=PaastaColors.grey(f"{dashboard_url}/#/jobs/{job_id}"),
            )
            output.append(job_info_str)
        else:
            output.append(
                PaastaColors.yellow(
                    f"    Only showing {allowed_max_jobs_printed} Flink jobs, use -v to show all"
                )
            )
            break

    return output
