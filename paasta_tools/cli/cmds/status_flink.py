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
import asyncio
import shutil
from datetime import datetime
from itertools import groupby
from typing import Any
from typing import List
from typing import Mapping

import a_sync
import humanize

from paasta_tools import flink_tools
from paasta_tools.api.client import get_paasta_oapi_client
from paasta_tools.api.client import PaastaOApiClient
from paasta_tools.cli.utils import append_pod_status
from paasta_tools.cli.utils import get_paasta_oapi_api_clustername
from paasta_tools.flink_tools import FlinkDeploymentConfig
from paasta_tools.flink_tools import get_flink_config_from_paasta_api_client
from paasta_tools.flink_tools import get_flink_jobs_from_paasta_api_client
from paasta_tools.flink_tools import get_flink_overview_from_paasta_api_client
from paasta_tools.flink_tools import load_flink_instance_config
from paasta_tools.flinkeks_tools import load_flinkeks_instance_config
from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.monitoring_tools import get_runbook
from paasta_tools.monitoring_tools import get_team
from paasta_tools.paastaapi.model.flink_job_details import FlinkJobDetails
from paasta_tools.paastaapi.model.flink_jobs import FlinkJobs
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import SystemPaastaConfig

FLINK_STATUS_MAX_THREAD_POOL_WORKERS = 50

OUTPUT_HORIZONTAL_RULE = (
    "=================================================================="
)


def should_job_info_be_shown(cluster_state):
    return (
        cluster_state == "running"
        or cluster_state == "stoppingsupervisor"
        or cluster_state == "cleanupsupervisor"
    )


def _print_flink_status_from_job_manager(
    service: str,
    instance: str,
    cluster: str,
    output: List[str],
    flink: Mapping[str, Any],
    client: PaastaOApiClient,
    system_paasta_config: "SystemPaastaConfig",
    flink_instance_config: FlinkDeploymentConfig,
    verbose: int,
) -> int:
    status = flink.get("status")
    if status is None:
        output.append(PaastaColors.red("    Flink cluster is not available yet"))
        return 1

    # Print Flink Config SHA
    # Since metadata should be available no matter the state, we show it first. If this errors out
    # then we cannot really do much to recover, because cluster is not in usable state anyway
    metadata = flink.get("metadata")
    labels = metadata.get("labels")
    config_sha = labels.get(paasta_prefixed("config_sha"))
    if config_sha is None:
        raise ValueError(f"expected config sha on Flink, but received {metadata}")
    if config_sha.startswith("config"):
        config_sha = config_sha[6:]
    output.append(f"    Config SHA: {config_sha}")

    if verbose:
        # Print Flink repo links
        output.append(f"    Repo(git): https://github.yelpcorp.com/services/{service}")
        output.append(
            f"    Repo(sourcegraph): https://sourcegraph.yelpcorp.com/services/{service}"
        )

        # Print Flink Pool information
        flink_pool = flink_instance_config.get_pool()
        output.append(f"    Flink Pool: {flink_pool}")

        # Print ownership information
        flink_monitoring_team = flink_instance_config.get_team() or get_team(
            overrides={}, service=service, soa_dir=DEFAULT_SOA_DIR
        )
        output.append(f"    Owner: {flink_monitoring_team}")

        # Print rb information
        flink_rb_for_instance = flink_instance_config.get_runbook() or get_runbook(
            overrides={}, service=service, soa_dir=DEFAULT_SOA_DIR
        )
        output.append(f"    Flink Runbook: {flink_rb_for_instance}")

    # Print Flink Version
    if status["state"] == "running":
        try:
            flink_config = get_flink_config_from_paasta_api_client(
                service=service, instance=instance, client=client
            )
        except Exception as e:
            output.append(PaastaColors.red(f"Exception when talking to the API:"))
            output.append(str(e))
            return 1

        if verbose:
            output.append(
                f"    Flink version: {flink_config.flink_version} {flink_config.flink_revision}"
            )
        else:
            output.append(f"    Flink version: {flink_config.flink_version}")

        # Print Flink Dashboard URL
        # Annotation "flink.yelp.com/dashboard_url" is populated by flink-operator
        dashboard_url = metadata["annotations"].get("flink.yelp.com/dashboard_url")
        output.append(f"    URL: {dashboard_url}/")

    if verbose:
        # Print Flink config link resources
        ecosystem = system_paasta_config.get_ecosystem_for_cluster(cluster)
        output.append(
            f"    Yelpsoa configs: https://github.yelpcorp.com/sysgit/yelpsoa-configs/tree/master/{service}"
        )
        output.append(
            f"    Srv configs: https://github.yelpcorp.com/sysgit/srv-configs/tree/master/ecosystem/{ecosystem}/{service}"
        )

        output.append(f"{OUTPUT_HORIZONTAL_RULE}")

        # Print Flink Log Commands
        output.append(f"    Flink Log Commands:")
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

        output.append(f"{OUTPUT_HORIZONTAL_RULE}")

        # Print Flink Metrics Links
        output.append(f"    Flink Monitoring:")
        output.append(
            f"      Job Metrics: https://grafana.yelpcorp.com/d/flink-metrics/flink-job-metrics?orgId=1&var-datasource=Prometheus-flink&var-region=uswest2-{ecosystem}&var-service={service}&var-instance={instance}&var-job=All&from=now-24h&to=now"
        )
        output.append(
            f"      Container Metrics: https://grafana.yelpcorp.com/d/flink-container-metrics/flink-container-metrics?orgId=1&var-datasource=Prometheus-flink&var-region=uswest2-{ecosystem}&var-service={service}&var-instance={instance}&from=now-24h&to=now"
        )
        output.append(
            f"      JVM Metrics: https://grafana.yelpcorp.com/d/flink-jvm-metrics/flink-jvm-metrics?orgId=1&var-datasource=Prometheus-flink&var-region=uswest2-{ecosystem}&var-service={service}&var-instance={instance}&from=now-24h&to=now"
        )

        # Print Flink Costs Link
        output.append(
            f"      Flink Cost: https://splunk.yelpcorp.com/en-US/app/yelp_computeinfra/paasta_service_utilization?form.service={service}&form.field1.earliest=-30d%40d&form.field1.latest=now&form.instance={instance}&form.cluster={cluster}"
        )

        output.append(f"{OUTPUT_HORIZONTAL_RULE}")

    # Print Flink Cluster State
    color = PaastaColors.green if status["state"] == "running" else PaastaColors.yellow
    output.append(f"    State: {color(status['state'].title())}")

    # Print Flink Cluster Pod Info
    pod_running_count = pod_evicted_count = pod_other_count = 0
    # default for evicted in case where pod status is not available
    evicted = f"{pod_evicted_count}"
    for pod in status["pod_status"]:
        if pod["phase"] == "Running":
            pod_running_count += 1
        elif pod["phase"] == "Failed" and pod["reason"] == "Evicted":
            pod_evicted_count += 1
        else:
            pod_other_count += 1
        evicted = (
            PaastaColors.red(f"{pod_evicted_count}")
            if pod_evicted_count > 0
            else f"{pod_evicted_count}"
        )

    output.append(
        "    Pods:"
        f" {pod_running_count} running,"
        f" {evicted} evicted,"
        f" {pod_other_count} other"
    )

    if not should_job_info_be_shown(status["state"]):
        # In case where the jobmanager of cluster is in crashloopbackoff
        # The pods for the cluster will be available and we need to show the pods.
        # So that paasta status -v and kubectl get pods show the same consistent result.
        if verbose and len(status["pod_status"]) > 0:
            append_pod_status(status["pod_status"], output)
        output.append("    No other information available in non-running state")
        return 0

    if status["state"] == "running":
        # Flink cluster overview from paasta api client
        try:
            overview = get_flink_overview_from_paasta_api_client(
                service=service, instance=instance, client=client
            )
        except Exception as e:
            output.append(PaastaColors.red("Exception when talking to the API:"))
            output.append(str(e))
            return 1

        output.append(
            "    Jobs:"
            f" {overview.jobs_running} running,"
            f" {overview.jobs_finished} finished,"
            f" {overview.jobs_failed} failed,"
            f" {overview.jobs_cancelled} cancelled"
        )
        output.append(
            "   "
            f" {overview.taskmanagers} taskmanagers,"
            f" {overview.slots_available}/{overview.slots_total} slots available"
        )

    flink_jobs = FlinkJobs()
    flink_jobs.jobs = []
    if status["state"] == "running":
        try:
            flink_jobs = get_flink_jobs_from_paasta_api_client(
                service=service, instance=instance, client=client
            )
        except Exception as e:
            output.append(PaastaColors.red("Exception when talking to the API:"))
            output.append(str(e))
            return 1

    jobs: List[FlinkJobDetails] = []
    job_ids: List[str] = []
    if flink_jobs.get("jobs"):
        job_ids = [job.id for job in flink_jobs.get("jobs")]
    try:
        jobs = a_sync.block(get_flink_job_details, service, instance, job_ids, client)
    except Exception as e:
        output.append(PaastaColors.red("Exception when talking to the API:"))
        output.append(str(e))
        return 1

    # Avoid cutting job name. As opposed to default hardcoded value of 32, we will use max length of job name
    if jobs:
        max_job_name_length = max([len(get_flink_job_name(job)) for job in jobs])
    else:
        max_job_name_length = 10

    # Apart from this column total length of one row is around 52 columns, using remaining terminal columns for job name
    # Note: for terminals smaller than 90 columns the row will overflow in verbose printing
    allowed_max_job_name_length = min(
        max(10, shutil.get_terminal_size().columns - 52), max_job_name_length
    )

    output.append("    Jobs:")
    if verbose > 1:
        output.append(
            f'      {"Job Name": <{allowed_max_job_name_length}} State       Job ID                           Started'
        )
    else:
        output.append(
            f'      {"Job Name": <{allowed_max_job_name_length}} State       Started'
        )

    # Use only the most recent jobs
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
                state=color_fn((job.get("state").title() or "Unknown")),
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

    if verbose and len(status["pod_status"]) > 0:
        append_pod_status(status["pod_status"], output)
    return 0


def print_flink_status(
    cluster: str,
    service: str,
    instance: str,
    output: List[str],
    flink: Mapping[str, Any],
    verbose: int,
) -> int:
    system_paasta_config = load_system_paasta_config()

    client = get_paasta_oapi_client(cluster, system_paasta_config)
    if not client:
        output.append(
            PaastaColors.red(
                "paasta-api client unavailable - unable to get flink status"
            )
        )
        return 1

    flink_instance_config = load_flink_instance_config(
        service=service,
        instance=instance,
        cluster=cluster,
    )

    return _print_flink_status_from_job_manager(
        service,
        instance,
        cluster,
        output,
        flink,
        client,
        system_paasta_config,
        flink_instance_config,
        verbose,
    )


def print_flinkeks_status(
    cluster: str,
    service: str,
    instance: str,
    output: List[str],
    flink: Mapping[str, Any],
    verbose: int,
) -> int:
    system_paasta_config = load_system_paasta_config()

    client = get_paasta_oapi_client(
        cluster=get_paasta_oapi_api_clustername(cluster=cluster, is_eks=True),
        system_paasta_config=system_paasta_config,
    )
    if not client:
        output.append(
            PaastaColors.red(
                "paasta-api client unavailable - unable to get flink status"
            )
        )
        return 1
    flink_eks_instance_config = load_flinkeks_instance_config(
        service=service,
        instance=instance,
        cluster=cluster,
    )
    return _print_flink_status_from_job_manager(
        service,
        instance,
        cluster,
        output,
        flink,
        client,
        system_paasta_config,
        flink_eks_instance_config,
        verbose,
    )


def get_flink_job_name(flink_job: FlinkJobDetails) -> str:
    return flink_job["name"].split(".", 2)[-1]


async def get_flink_job_details(
    service: str, instance: str, job_ids: List[str], client: PaastaOApiClient
) -> List[FlinkJobDetails]:
    jobs_details = await asyncio.gather(
        *[
            flink_tools.get_flink_job_details_from_paasta_api_client(
                service, instance, job_id, client
            )
            for job_id in job_ids
        ]
    )
    return [jd for jd in jobs_details]
