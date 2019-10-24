#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
#
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
import concurrent.futures
import difflib
import os
import sys
import traceback
from collections import defaultdict
from collections import OrderedDict
from datetime import datetime
from datetime import timedelta
from distutils.util import strtobool
from itertools import groupby
from typing import Any
from typing import Callable
from typing import DefaultDict
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Sequence
from typing import Set
from typing import Tuple
from typing import Type

import humanize
from bravado.exception import BravadoConnectionError
from bravado.exception import BravadoTimeoutError
from bravado.exception import HTTPError
from service_configuration_lib import read_deploy

from paasta_tools import kubernetes_tools
from paasta_tools.adhoc_tools import AdhocJobConfig
from paasta_tools.api.client import get_paasta_api_client
from paasta_tools.cassandracluster_tools import CassandraClusterDeploymentConfig
from paasta_tools.cli.utils import execute_paasta_serviceinit_on_remote_master
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import get_instance_configs_for_service
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import NoSuchService
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.flink_tools import FlinkDeploymentConfig
from paasta_tools.kafkacluster_tools import KafkaClusterDeploymentConfig
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import KubernetesDeployStatus
from paasta_tools.marathon_serviceinit import bouncing_status_human
from paasta_tools.marathon_serviceinit import desired_state_human
from paasta_tools.marathon_serviceinit import haproxy_backend_report
from paasta_tools.marathon_serviceinit import marathon_app_deploy_status_human
from paasta_tools.marathon_serviceinit import status_marathon_job_human
from paasta_tools.marathon_tools import MarathonDeployStatus
from paasta_tools.mesos_tools import format_tail_lines_for_mesos_task
from paasta_tools.monitoring_tools import get_team
from paasta_tools.monitoring_tools import list_teams
from paasta_tools.tron_tools import TronActionConfig
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import datetime_from_utc_to_local
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import format_table
from paasta_tools.utils import get_soa_cluster_deploy_files
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import list_all_instances_for_service
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import load_deployments_json
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import remove_ansi_escape_sequences
from paasta_tools.utils import SystemPaastaConfig


HTTP_ONLY_INSTANCE_CONFIG: Sequence[Type[InstanceConfig]] = [
    FlinkDeploymentConfig,
    CassandraClusterDeploymentConfig,
    KafkaClusterDeploymentConfig,
    KubernetesDeploymentConfig,
    AdhocJobConfig,
]
SSH_ONLY_INSTANCE_CONFIG: Sequence[Type[InstanceConfig]] = []


def add_subparser(subparsers,) -> None:
    status_parser = subparsers.add_parser(
        "status",
        help="Display the status of a PaaSTA service.",
        description=(
            "'paasta status' works by SSH'ing to remote PaaSTA masters and "
            "inspecting the local APIs, and reports on the overal health "
            "of a service."
        ),
        epilog=(
            "Note: This command requires SSH and sudo privileges on the remote PaaSTA "
            "masters."
        ),
    )
    status_parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        dest="verbose",
        default=0,
        help="Print out more output regarding the state of the service. "
        "A second -v will also print the stdout/stderr tail.",
    )
    status_parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    add_instance_filter_arguments(status_parser)
    status_parser.set_defaults(command=paasta_status)


def add_instance_filter_arguments(status_parser, verb: str = "inspect") -> None:
    status_parser.add_argument(
        "-s", "--service", help=f"The name of the service you wish to {verb}"
    ).completer = lazy_choices_completer(list_services)
    status_parser.add_argument(
        "-c",
        "--clusters",
        help=f"A comma-separated list of clusters to {verb}. By default, will {verb} all clusters.\n"
        f"For example: --clusters norcal-prod,nova-prod",
    ).completer = lazy_choices_completer(list_clusters)
    status_parser.add_argument(
        "-i",
        "--instances",
        help=f"A comma-separated list of instances to {verb}. By default, will {verb} all instances.\n"
        f"For example: --instances canary,main",
    )  # No completer because we need to know service first and we can't until some other stuff has happened
    status_parser.add_argument(
        "-l",
        "--deploy-group",
        help=(
            f"Name of the deploy group which you want to {verb}. "
            f"If specified together with --instances and/or --clusters, will {verb} common instances only."
        ),
    ).completer = lazy_choices_completer(list_deploy_groups)
    status_parser.add_argument(
        "-o",
        "--owner",
        help=f"Only {verb} instances with this owner specified in soa-configs.",
    ).completer = lazy_choices_completer(list_teams)
    status_parser.add_argument(
        "-r", "--registration", help=f"Only {verb} instances with this registration."
    )


def missing_deployments_message(service: str,) -> str:
    message = (
        f"{service} has no deployments in deployments.json yet.\n  " "Has Jenkins run?"
    )
    return message


def get_deploy_info(deploy_file_path: str,) -> Mapping:
    deploy_info = read_deploy(deploy_file_path)
    if not deploy_info:
        paasta_print("Error encountered with %s" % deploy_file_path)

        exit(1)
    return deploy_info


def get_planned_deployments(service: str, soa_dir: str) -> Iterable[str]:
    for cluster, cluster_deploy_file in get_soa_cluster_deploy_files(
        service=service, soa_dir=soa_dir
    ):
        for instance in get_deploy_info(cluster_deploy_file):
            yield f"{cluster}.{instance}"


def list_deployed_clusters(
    pipeline: Sequence[str], actual_deployments: Sequence[str]
) -> Sequence[str]:
    """Returns a list of clusters that a service is deployed to given
    an input deploy pipeline and the actual deployments"""
    deployed_clusters: List[str] = []
    for namespace in pipeline:
        cluster, instance = namespace.split(".")
        if namespace in actual_deployments:
            if cluster not in deployed_clusters:
                deployed_clusters.append(cluster)
    return deployed_clusters


def get_actual_deployments(service: str, soa_dir: str) -> Mapping[str, str]:
    deployments_json = load_deployments_json(service, soa_dir)
    if not deployments_json:
        paasta_print(
            "Warning: it looks like %s has not been deployed anywhere yet!" % service,
            file=sys.stderr,
        )
    # Create a dictionary of actual $service Jenkins deployments
    actual_deployments = {}
    for key, branch_dict in deployments_json.config_dict.items():
        service, namespace = key.split(":")
        if service == service:
            value = branch_dict["docker_image"]
            sha = value[value.rfind("-") + 1 :]
            actual_deployments[namespace.replace("paasta-", "", 1)] = sha
    return actual_deployments


def paasta_status_on_api_endpoint(
    cluster: str,
    service: str,
    instance: str,
    output: List[str],
    system_paasta_config: SystemPaastaConfig,
    verbose: int,
) -> int:
    output.append("    instance: %s" % PaastaColors.blue(instance))
    client = get_paasta_api_client(cluster, system_paasta_config)
    if not client:
        paasta_print("Cannot get a paasta-api client")
        exit(1)
    try:
        status = client.service.status_instance(
            service=service, instance=instance, verbose=verbose
        ).result()
    except HTTPError as exc:
        output.append(PaastaColors.red(exc.response.text))
        return exc.status_code
    except (BravadoConnectionError, BravadoTimeoutError) as exc:
        output.append(
            PaastaColors.red(f"Could not connect to API: {exc.__class__.__name__}")
        )
        return 1
    except Exception:
        tb = sys.exc_info()[2]
        output.append(PaastaColors.red(f"Exception when talking to the API:"))
        output.extend(line.strip() for line in traceback.format_tb(tb))
        return 1

    if status.git_sha != "":
        output.append("    Git sha:    %s (desired)" % status.git_sha)

    if status.marathon is not None:
        return print_marathon_status(service, instance, output, status.marathon)
    elif status.kubernetes is not None:
        return print_kubernetes_status(service, instance, output, status.kubernetes)
    elif status.tron is not None:
        return print_tron_status(service, instance, output, status.tron, verbose)
    elif status.adhoc is not None:
        return print_adhoc_status(
            cluster, service, instance, output, status.adhoc, verbose
        )
    elif status.flink is not None:
        return print_flink_status(
            cluster, service, instance, output, status.flink, verbose
        )
    else:
        paasta_print(
            "Not implemented: Looks like %s is not a Marathon or Kubernetes instance"
            % instance
        )
        return 0


def print_adhoc_status(
    cluster: str,
    service: str,
    instance: str,
    output: List[str],
    status,
    verbose: int = 0,
) -> int:
    output.append(f"    Job: {instance}")
    for run in status:
        output.append(
            "Launch time: %s, run id: %s, framework id: %s"
            % (run["launch_time"], run["run_id"], run["framework_id"])
        )
    if status:
        output.append(
            (
                "    Use `paasta remote-run stop -s {} -c {} -i {} [-R <run id> "
                "    | -F <framework id>]` to stop."
            ).format(service, cluster, instance)
        )
    else:
        output.append("    Nothing found.")

    return 0


def print_marathon_status(
    service: str, instance: str, output: List[str], marathon_status
) -> int:
    if marathon_status.error_message:
        output.append(marathon_status.error_message)
        return 1

    bouncing_status = bouncing_status_human(
        marathon_status.app_count, marathon_status.bounce_method
    )
    desired_state = desired_state_human(
        marathon_status.desired_state, marathon_status.expected_instance_count
    )
    output.append(f"    Desired state:      {bouncing_status} and {desired_state}")

    job_status_human = status_marathon_job_human(
        service=service,
        instance=instance,
        deploy_status=marathon_status.deploy_status,
        desired_app_id=marathon_status.desired_app_id,
        app_count=marathon_status.app_count,
        running_instances=marathon_status.running_instance_count,
        normal_instance_count=marathon_status.expected_instance_count,
    )
    output.append(f"    {job_status_human}")

    if marathon_status.autoscaling_info:
        autoscaling_info_table = create_autoscaling_info_table(
            marathon_status.autoscaling_info
        )
        output.extend([f"      {line}" for line in autoscaling_info_table])

    for app_status in marathon_status.app_statuses:
        app_status_human = marathon_app_status_human(
            marathon_status.desired_app_id, app_status
        )
        output.extend([f"      {line}" for line in app_status_human])

    mesos_status_human = marathon_mesos_status_human(
        marathon_status.mesos.error_message,
        marathon_status.mesos.running_task_count or 0,
        marathon_status.expected_instance_count,
        marathon_status.mesos.running_tasks,
        marathon_status.mesos.non_running_tasks,
    )
    output.extend([f"    {line}" for line in mesos_status_human])

    if marathon_status.smartstack is not None:
        smartstack_status_human = get_smartstack_status_human(
            marathon_status.smartstack.registration,
            marathon_status.smartstack.expected_backends_per_location,
            marathon_status.smartstack.locations,
        )
        output.extend([f"    {line}" for line in smartstack_status_human])

    return 0


autoscaling_fields_to_headers = OrderedDict(
    current_instances="Current instances",
    max_instances="Max instances",
    min_instances="Min instances",
    current_utilization="Current utilization",
    target_instances="Target instances",
)


def create_autoscaling_info_table(autoscaling_info):
    output = ["Autoscaling Info:"]

    if autoscaling_info.current_utilization is not None:
        autoscaling_info.current_utilization = "{:.1f}%".format(
            autoscaling_info.current_utilization * 100
        )
    else:
        autoscaling_info.current_utilization = "Exception"

    if autoscaling_info.target_instances is None:
        autoscaling_info.target_instances = "Exception"

    headers = list(autoscaling_fields_to_headers.values())
    row = [
        str(getattr(autoscaling_info, field)) for field in autoscaling_fields_to_headers
    ]
    table = [f"  {line}" for line in format_table([headers, row])]
    output.extend(table)
    return output


def marathon_mesos_status_human(
    error_message,
    running_task_count,
    expected_instance_count,
    running_tasks,
    non_running_tasks,
):
    if error_message:
        return [f"Mesos: {PaastaColors.red(error_message)}"]

    output = []
    output.append(
        marathon_mesos_status_summary(running_task_count, expected_instance_count)
    )

    if running_tasks or non_running_tasks:
        output.append("  Running Tasks:")
        running_tasks_table = create_mesos_running_tasks_table(running_tasks)
        output.extend([f"    {line}" for line in running_tasks_table])

        output.append(PaastaColors.grey("  Non-running Tasks:"))
        non_running_tasks_table = create_mesos_non_running_tasks_table(
            non_running_tasks
        )
        output.extend([f"    {line}" for line in non_running_tasks_table])

    return output


def create_mesos_running_tasks_table(running_tasks):
    rows = []
    table_header = [
        "Mesos Task ID",
        "Host deployed to",
        "Ram",
        "CPU",
        "Deployed at what localtime",
    ]
    rows.append(table_header)
    for task in running_tasks or []:
        mem_string = get_mesos_task_memory_string(task)
        cpu_string = get_mesos_task_cpu_string(task)
        deployed_at = datetime.fromtimestamp(task.deployed_timestamp)
        deployed_at_string = "{} ({})".format(
            deployed_at.strftime("%Y-%m-%dT%H:%M"), humanize.naturaltime(deployed_at)
        )

        rows.append(
            [task.id, task.hostname, mem_string, cpu_string, deployed_at_string]
        )
        rows.extend(format_tail_lines_for_mesos_task(task.tail_lines, task.id))

    return format_table(rows)


def get_mesos_task_memory_string(task):
    if task.rss.value is None or task.mem_limit.value is None:
        return task.rss.error_message or task.mem_limit.error_message
    elif task.mem_limit.value == 0:
        return "Undef"
    else:
        mem_percent = 100 * task.rss.value / task.mem_limit.value
        mem_string = "%d/%dMB" % (
            (task.rss.value / 1024 / 1024),
            (task.mem_limit.value / 1024 / 1024),
        )
        if mem_percent > 90:
            return PaastaColors.red(mem_string)
        else:
            return mem_string


def get_mesos_task_cpu_string(task):
    if task.cpu_shares.value is None or task.cpu_used_seconds.value is None:
        return task.cpu_shares.error_message
    else:
        # The total time a task has been allocated is the total time the task has
        # been running multiplied by the "shares" a task has.
        # (see https://github.com/mesosphere/mesos/blob/0b092b1b0/src/webui/master/static/js/controllers.js#L140)
        allocated_seconds = task.cpu_shares.value * task.duration_seconds
        if allocated_seconds == 0:
            return "Undef"
        else:
            cpu_percent = round(
                100 * (task.cpu_used_seconds.value / allocated_seconds), 1
            )
            cpu_string = "%s%%" % cpu_percent
            if cpu_percent > 90:
                return PaastaColors.red(cpu_string)
            else:
                return cpu_string


def create_mesos_non_running_tasks_table(non_running_tasks):
    rows = []
    table_header = [
        "Mesos Task ID",
        "Host deployed to",
        "Deployed at what localtime",
        "Status",
    ]
    rows.append(table_header)

    for task in non_running_tasks or []:
        if task.deployed_timestamp is None:
            deployed_at_string = "Unknown"
        else:
            deployed_at = datetime.fromtimestamp(task.deployed_timestamp)
            deployed_at_string = "{} ({})".format(
                deployed_at.strftime("%Y-%m-%dT%H:%M"),
                humanize.naturaltime(deployed_at),
            )

        rows.append([task.id, task.hostname, deployed_at_string, task.state])
        rows.extend(format_tail_lines_for_mesos_task(task.tail_lines, task.id))

    table = format_table(rows)
    return [PaastaColors.grey(formatted_row) for formatted_row in table]


def marathon_mesos_status_summary(mesos_task_count, expected_instance_count) -> str:
    if mesos_task_count >= expected_instance_count:
        status = PaastaColors.green("Healthy")
        count_str = PaastaColors.green(
            "(%d/%d)" % (mesos_task_count, expected_instance_count)
        )
    elif mesos_task_count == 0:
        status = PaastaColors.red("Critical")
        count_str = PaastaColors.red(
            "(%d/%d)" % (mesos_task_count, expected_instance_count)
        )
    else:
        status = PaastaColors.yellow("Warning")
        count_str = PaastaColors.yellow(
            "(%d/%d)" % (mesos_task_count, expected_instance_count)
        )
    running_string = PaastaColors.bold("TASK_RUNNING")
    return f"Mesos:      {status} - {count_str} tasks in the {running_string} state."


def marathon_app_status_human(app_id, app_status) -> List[str]:
    output = []

    if app_status.dashboard_url:
        output.append(f"Dashboard: {PaastaColors.blue(app_status.dashboard_url)}")
    else:
        output.append(f"App ID: {PaastaColors.blue(app_id)}")

    output.append(
        "  "
        + " ".join(
            [
                f"{app_status.tasks_running} running,",
                f"{app_status.tasks_healthy} healthy,",
                f"{app_status.tasks_staged} staged",
                f"out of {app_status.tasks_total}",
            ]
        )
    )

    create_datetime = datetime.fromtimestamp(app_status.create_timestamp)
    output.append(
        "  App created: {} ({})".format(
            create_datetime, humanize.naturaltime(create_datetime)
        )
    )

    deploy_status = MarathonDeployStatus.fromstring(app_status.deploy_status)
    deploy_status_human = marathon_app_deploy_status_human(
        deploy_status, app_status.backoff_seconds
    )
    output.append(f"  Status: {deploy_status_human}")

    if app_status.tasks:
        output.append("  Tasks:")
        tasks_table = format_marathon_task_table(app_status.tasks)
        output.extend([f"    {line}" for line in tasks_table])

    if app_status.unused_offer_reason_counts is not None:
        output.append("  Possibly stalled for:")
        output.extend(
            [
                f"    {reason}: {count}"
                for reason, count in app_status.unused_offer_reason_counts.items()
            ]
        )

    return output


def format_marathon_task_table(tasks):
    rows = [
        ("Mesos Task ID", "Host deployed to", "Deployed at what localtime", "Health")
    ]
    for task in tasks:
        local_deployed_datetime = datetime_from_utc_to_local(
            datetime.fromtimestamp(task.deployed_timestamp)
        )
        if task.host is not None:
            hostname = f"{task.host}:{task.port}"
        else:
            hostname = "Unknown"

        if task.is_healthy is None:
            health_check_status = PaastaColors.grey("N/A")
        elif task.is_healthy:
            health_check_status = PaastaColors.green("Healthy")
        else:
            health_check_status = PaastaColors.red("Unhealthy")

        rows.append(
            (
                task.id,
                hostname,
                "{} ({})".format(
                    local_deployed_datetime.strftime("%Y-%m-%dT%H:%M"),
                    humanize.naturaltime(local_deployed_datetime),
                ),
                health_check_status,
            )
        )

    return format_table(rows)


def format_kubernetes_pod_table(pods):
    rows = [("Pod ID", "Host deployed to", "Deployed at what localtime", "Health")]
    for pod in pods:
        local_deployed_datetime = datetime_from_utc_to_local(
            datetime.fromtimestamp(pod.deployed_timestamp)
        )
        hostname = f"{pod.host}" if pod.host is not None else "Unknown"

        if pod.phase is None or pod.phase == "Pending":
            health_check_status = PaastaColors.grey("N/A")
        elif pod.phase == "Running":
            health_check_status = PaastaColors.green("Healthy")
        else:
            health_check_status = PaastaColors.red("Unhealthy")

        rows.append(
            (
                pod.name,
                hostname,
                "{} ({})".format(
                    local_deployed_datetime.strftime("%Y-%m-%dT%H:%M"),
                    humanize.naturaltime(local_deployed_datetime),
                ),
                health_check_status,
            )
        )

    return format_table(rows)


def format_kubernetes_replicaset_table(replicasets):
    rows = [("ReplicaSet Name", "Ready / Desired", "Created at what localtime")]
    for replicaset in replicasets:
        local_created_datetime = datetime_from_utc_to_local(
            datetime.fromtimestamp(replicaset.create_timestamp)
        )

        replica_status = f"{replicaset.ready_replicas}/{replicaset.replicas}"
        if replicaset.ready_replicas >= replicaset.replicas:
            replica_status = PaastaColors.green(replica_status)
        else:
            replica_status = PaastaColors.red(replica_status)

        rows.append(
            (
                replicaset.name,
                replica_status,
                "{} ({})".format(
                    local_created_datetime.strftime("%Y-%m-%dT%H:%M"),
                    humanize.naturaltime(local_created_datetime),
                ),
            )
        )

    return format_table(rows)


def get_smartstack_status_human(
    registration, expected_backends_per_location, locations
) -> List[str]:
    if len(locations) == 0:
        return [f"Smartstack: ERROR - {registration} is NOT in smartstack at all!"]

    output = ["Smartstack:"]
    output.append(f"  Haproxy Service Name: {registration}")
    output.append(f"  Backends:")
    for location in locations:
        backend_status = haproxy_backend_report(
            expected_backends_per_location, location.running_backends_count
        )
        output.append(f"    {location.name} - {backend_status}")

        if location.backends:
            backends_table = build_smartstack_backends_table(location.backends)
            output.extend([f"      {line}" for line in backends_table])

    return output


def build_smartstack_backends_table(backends):
    rows = [("Name", "LastCheck", "LastChange", "Status")]
    for backend in backends:
        if backend.status == "UP":
            status = PaastaColors.default(backend.status)
        elif backend.status == "DOWN":
            status = PaastaColors.red(backend.status)
        elif backend.status == "MAINT":
            status = PaastaColors.grey(backend.status)
        else:
            status = PaastaColors.yellow(backend.status)

        if backend.check_duration is None:
            check_duration = ""
        else:
            check_duration = str(backend.check_duration)

        row = (
            f"{backend.hostname}:{backend.port}",
            f"{backend.check_status}/{backend.check_code} in {check_duration}ms",
            humanize.naturaltime(timedelta(seconds=backend.last_change)),
            status,
        )

        if not backend.has_associated_task:
            row = tuple(
                PaastaColors.grey(remove_ansi_escape_sequences(col)) for col in row
            )

        rows.append(row)

    return format_table(rows)


def kubernetes_app_deploy_status_human(status, backoff_seconds=None):
    status_string = kubernetes_tools.KubernetesDeployStatus.tostring(status)

    if status == kubernetes_tools.KubernetesDeployStatus.Waiting:
        deploy_status = (
            "%s (new tasks waiting for capacity to become available)"
            % PaastaColors.red(status_string)
        )
    elif status == kubernetes_tools.KubernetesDeployStatus.Deploying:
        deploy_status = PaastaColors.yellow(status_string)
    elif status == kubernetes_tools.KubernetesDeployStatus.Running:
        deploy_status = PaastaColors.bold(status_string)
    else:
        deploy_status = status_string

    return deploy_status


def status_kubernetes_job_human(
    service: str,
    instance: str,
    deploy_status: str,
    desired_app_id: str,
    app_count: int,
    running_instances: int,
    normal_instance_count: int,
) -> str:
    name = PaastaColors.cyan(compose_job_id(service, instance))

    if app_count >= 0:
        if running_instances >= normal_instance_count:
            status = PaastaColors.green("Healthy")
            instance_count = PaastaColors.green(
                "(%d/%d)" % (running_instances, normal_instance_count)
            )
        elif running_instances == 0:
            status = PaastaColors.yellow("Critical")
            instance_count = PaastaColors.red(
                "(%d/%d)" % (running_instances, normal_instance_count)
            )
        else:
            status = PaastaColors.yellow("Warning")
            instance_count = PaastaColors.yellow(
                "(%d/%d)" % (running_instances, normal_instance_count)
            )
        return "Kubernetes:   {} - up with {} instances. Status: {}".format(
            status, instance_count, deploy_status
        )
    else:
        status = PaastaColors.yellow("Warning")
        return "Kubernetes:   {} - {} (app {}) is not configured in Kubernetes yet (waiting for bounce)".format(
            status, name, desired_app_id
        )


def print_flink_status(
    cluster: str,
    service: str,
    instance: str,
    output: List[str],
    flink: Mapping[str, Any],
    verbose: int,
) -> int:
    status = flink.get("status")
    if status is None:
        output.append(PaastaColors.red("    Flink cluster is not available yet"))
        return 1

    # Since metadata should be available no matter the state, we show it first. If this errors out
    # then we cannot really do much to recover, because cluster is not in usable state anyway
    metadata = flink.get("metadata")
    config_sha = metadata.labels.get("yelp.com/paasta_config_sha")
    if config_sha is None:
        raise ValueError(f"expected config sha on Flink, but received {metadata}")
    if config_sha.startswith("config"):
        config_sha = config_sha[6:]

    output.append(f"    Config SHA: {config_sha}")

    if status.state != "running":
        output.append(
            "    State: {state}".format(state=PaastaColors.yellow(status.state))
        )
        output.append(f"    No other information available in non-running state")
        return 0

    dashboard_url = metadata.annotations.get("yelp.com/dashboard_url")
    if verbose:
        output.append(
            f"    Flink version: {status.config['flink-version']} {status.config['flink-revision']}"
        )
    else:
        output.append(f"    Flink version: {status.config['flink-version']}")
    output.append(f"    URL: {dashboard_url}/")
    output.append(f"    State: {status.state}")
    output.append(
        "    Jobs:"
        f" {status.overview['jobs-running']} running,"
        f" {status.overview['jobs-finished']} finished,"
        f" {status.overview['jobs-failed']} failed,"
        f" {status.overview['jobs-cancelled']} cancelled"
    )
    output.append(
        "   "
        f" {status.overview['taskmanagers']} taskmanagers,"
        f" {status.overview['slots-available']}/{status.overview['slots-total']} slots available"
    )

    output.append(f"    Jobs:")
    if verbose:
        output.append(
            f"      Job Name                         State       Job ID                           Started"
        )
    else:
        output.append(f"      Job Name                         State       Started")

    # Use only the most recent jobs
    unique_jobs = (
        sorted(jobs, key=lambda j: -j["start-time"])[0]
        for _, jobs in groupby(
            sorted(
                (j for j in status.jobs if j.get("name") and j.get("start-time")),
                key=lambda j: j["name"],
            ),
            lambda j: j["name"],
        )
    )
    for job in unique_jobs:
        job_id = job["jid"]
        if verbose:
            fmt = """      {job_name: <32.32} {state: <11} {job_id} {start_time}
        {dashboard_url}"""
        else:
            fmt = "      {job_name: <32.32} {state: <11} {start_time}"
        start_time = datetime_from_utc_to_local(
            datetime.utcfromtimestamp(int(job["start-time"]) // 1000)
        )
        output.append(
            fmt.format(
                job_id=job_id,
                job_name=job["name"].split(".", 2)[2],
                state=(job.get("state") or "unknown"),
                start_time=f"{str(start_time)} ({humanize.naturaltime(start_time)})",
                dashboard_url=PaastaColors.grey(f"{dashboard_url}/#/jobs/{job_id}"),
            )
        )
        if verbose and job_id in status.exceptions:
            exceptions = status.exceptions[job_id]
            root_exception = exceptions["root-exception"]
            if root_exception is not None:
                output.append(f"        Exception: {root_exception}")
                ts = exceptions["timestamp"]
                if ts is not None:
                    exc_ts = datetime_from_utc_to_local(
                        datetime.utcfromtimestamp(int(ts) // 1000)
                    )
                    output.append(
                        f"            {str(exc_ts)} ({humanize.naturaltime(exc_ts)})"
                    )
    return 0


def print_kubernetes_status(
    service: str, instance: str, output: List[str], kubernetes_status
) -> int:
    if kubernetes_status.error_message:
        output.append(kubernetes_status.error_message)
        return 1

    bouncing_status = bouncing_status_human(
        kubernetes_status.app_count, kubernetes_status.bounce_method
    )
    desired_state = desired_state_human(
        kubernetes_status.desired_state, kubernetes_status.expected_instance_count
    )
    output.append(f"    State:      {bouncing_status} - Desired state: {desired_state}")

    status = KubernetesDeployStatus.fromstring(kubernetes_status.deploy_status)
    deploy_status = kubernetes_app_deploy_status_human(status)

    output.append(
        "    {}".format(
            status_kubernetes_job_human(
                service=service,
                instance=instance,
                deploy_status=deploy_status,
                desired_app_id=kubernetes_status.app_id,
                app_count=kubernetes_status.app_count,
                running_instances=kubernetes_status.running_instance_count,
                normal_instance_count=kubernetes_status.expected_instance_count,
            )
        )
    )
    if kubernetes_status.create_timestamp:
        create_datetime = datetime.fromtimestamp(kubernetes_status.create_timestamp)
        output.append(
            "      App created: {} ({}). Namespace: {}".format(
                create_datetime,
                humanize.naturaltime(create_datetime),
                kubernetes_status.namespace,
            )
        )

    if kubernetes_status.pods and len(kubernetes_status.pods) > 0:
        output.append("      Pods:")
        pods_table = format_kubernetes_pod_table(kubernetes_status.pods)
        output.extend([f"        {line}" for line in pods_table])

    if kubernetes_status.replicasets and len(kubernetes_status.replicasets) > 0:
        output.append("      ReplicaSets:")
        replicasets_table = format_kubernetes_replicaset_table(
            kubernetes_status.replicasets
        )
        output.extend([f"        {line}" for line in replicasets_table])

    if kubernetes_status.smartstack is not None:
        smartstack_status_human = get_smartstack_status_human(
            kubernetes_status.smartstack.registration,
            kubernetes_status.smartstack.expected_backends_per_location,
            kubernetes_status.smartstack.locations,
        )
        output.extend([f"    {line}" for line in smartstack_status_human])
    return 0


def print_tron_status(
    service: str, instance: str, output: List[str], tron_status, verbose: int = 0
) -> int:
    output.append(f"    Tron job: {tron_status.job_name}")
    if verbose:
        output.append(f"      Status: {tron_status.job_status}")
        output.append(f"      Schedule: {tron_status.job_schedule}")
    output.append("      Dashboard: {}".format(PaastaColors.blue(tron_status.job_url)))

    output.append(f"    Action: {tron_status.action_name}")
    output.append(f"      Status: {tron_status.action_state}")
    if verbose:
        output.append(f"      Start time: {tron_status.action_start_time}")
    output.append(f"      Command: {tron_status.action_command}")
    if verbose > 1:
        output.append(f"      Raw Command: {tron_status.action_raw_command}")
        output.append(f"      Stdout: \n{tron_status.action_stdout}")
        output.append(f"      Stderr: \n{tron_status.action_stderr}")

    return 0


def report_status_for_cluster(
    service: str,
    cluster: str,
    deploy_pipeline: Sequence[str],
    actual_deployments: Mapping[str, str],
    instance_whitelist: Mapping[str, Type[InstanceConfig]],
    system_paasta_config: SystemPaastaConfig,
    verbose: int = 0,
    use_api_endpoint: bool = False,
) -> Tuple[int, Sequence[str]]:
    """With a given service and cluster, prints the status of the instances
    in that cluster"""
    output = ["", "service: %s" % service, "cluster: %s" % cluster]
    seen_instances = []
    deployed_instances = []
    instances = instance_whitelist.keys()
    http_only_instances = [
        instance
        for instance, instance_config_class in instance_whitelist.items()
        if instance_config_class in HTTP_ONLY_INSTANCE_CONFIG
    ]
    ssh_only_instances = [
        instance
        for instance, instance_config_class in instance_whitelist.items()
        if instance_config_class in SSH_ONLY_INSTANCE_CONFIG
    ]

    tron_jobs = [
        instance
        for instance, instance_config_class in instance_whitelist.items()
        if instance_config_class == TronActionConfig
    ]

    for namespace in deploy_pipeline:
        cluster_in_pipeline, instance = namespace.split(".")
        seen_instances.append(instance)

        if cluster_in_pipeline != cluster:
            continue
        if instances and instance not in instances:
            continue

        # Case: service deployed to cluster.instance
        if namespace in actual_deployments:
            deployed_instances.append(instance)

        # Case: flink instances don't use `deployments.json`
        elif instance_whitelist.get(instance) == FlinkDeploymentConfig:
            deployed_instances.append(instance)

        # Case: service NOT deployed to cluster.instance
        else:
            output.append("  instance: %s" % PaastaColors.red(instance))
            output.append("    Git sha:    None (not deployed yet)")

    api_return_code = 0
    ssh_return_code = 0
    if len(deployed_instances) > 0:
        http_only_deployed_instances = [
            deployed_instance
            for deployed_instance in deployed_instances
            if (
                deployed_instance in http_only_instances
                or deployed_instance not in ssh_only_instances
                and use_api_endpoint
            )
        ]
        if len(http_only_deployed_instances):
            return_codes = [
                paasta_status_on_api_endpoint(
                    cluster=cluster,
                    service=service,
                    instance=deployed_instance,
                    output=output,
                    system_paasta_config=system_paasta_config,
                    verbose=verbose,
                )
                for deployed_instance in http_only_deployed_instances
            ]
            if any(return_codes):
                api_return_code = 1
        ssh_only_deployed_instances = [
            deployed_instance
            for deployed_instance in deployed_instances
            if (
                deployed_instance in ssh_only_instances
                or deployed_instance not in http_only_instances
                and not use_api_endpoint
            )
        ]
        if len(ssh_only_deployed_instances):
            ssh_return_code, status = execute_paasta_serviceinit_on_remote_master(
                "status",
                cluster,
                service,
                ",".join(
                    deployed_instance
                    for deployed_instance in ssh_only_deployed_instances
                ),
                system_paasta_config,
                stream=False,
                verbose=verbose,
                ignore_ssh_output=True,
            )
            # Status results are streamed. This print is for possible error messages.
            if status is not None:
                for line in status.rstrip().split("\n"):
                    output.append("    %s" % line)

    if len(tron_jobs) > 0:
        return_codes = [
            paasta_status_on_api_endpoint(
                cluster=cluster,
                service=service,
                instance=tron_job,
                output=output,
                system_paasta_config=system_paasta_config,
                verbose=verbose,
            )
            for tron_job in tron_jobs
        ]
        seen_instances.extend(tron_jobs)

    output.append(
        report_invalid_whitelist_values(instances, seen_instances, "instance")
    )

    if ssh_return_code:
        return_code = ssh_return_code
    elif api_return_code:
        return_code = api_return_code
    else:
        return_code = 0

    return return_code, output


def report_invalid_whitelist_values(
    whitelist: Iterable[str], items: Sequence[str], item_type: str
) -> str:
    """Warns the user if there are entries in ``whitelist`` which don't
    correspond to any item in ``items``. Helps highlight typos.
    """
    return_string = ""
    bogus_entries = []
    if whitelist is None:
        return ""
    for entry in whitelist:
        if entry not in items:
            bogus_entries.append(entry)
    if len(bogus_entries) > 0:
        return_string = (
            "\n" "Warning: This service does not have any %s matching these names:\n%s"
        ) % (item_type, ",".join(bogus_entries))
    return return_string


def verify_instances(
    args_instances: str, service: str, clusters: Sequence[str]
) -> Sequence[str]:
    """Verify that a list of instances specified by user is correct for this service.

    :param args_instances: a list of instances.
    :param service: the service name
    :param cluster: a list of clusters
    :returns: a list of instances specified in args_instances without any exclusions.
    """
    unverified_instances = args_instances.split(",")
    service_instances: Set[str] = list_all_instances_for_service(
        service, clusters=clusters
    )

    misspelled_instances: Sequence[str] = [
        i for i in unverified_instances if i not in service_instances
    ]

    if misspelled_instances:
        suggestions: List[str] = []
        for instance in misspelled_instances:
            matches = difflib.get_close_matches(
                instance, service_instances, n=5, cutoff=0.5
            )
            suggestions.extend(matches)  # type: ignore
        suggestions = list(set(suggestions))

        if clusters:
            message = "{} doesn't have any instances matching {} on {}.".format(
                service,
                ", ".join(sorted(misspelled_instances)),
                ", ".join(sorted(clusters)),
            )
        else:
            message = "{} doesn't have any instances matching {}.".format(
                service, ", ".join(sorted(misspelled_instances))
            )

        paasta_print(PaastaColors.red(message))

        if suggestions:
            paasta_print("Did you mean any of these?")
            for instance in sorted(suggestions):
                paasta_print("  %s" % instance)

    return unverified_instances


def normalize_registrations(
    service: str, registrations: Sequence[str]
) -> Sequence[str]:
    ret = []
    for reg in registrations:
        if "." not in reg:
            ret.append(f"{service}.{reg}")
        else:
            ret.append(reg)
    return ret


def get_filters(args,) -> Sequence[Callable[[InstanceConfig], bool]]:
    """Figures out which filters to apply from an args object, and returns them

    :param args: args object
    :returns: list of functions that take an instance config and returns if the instance conf matches the filter
    """
    filters = []

    if args.service:
        filters.append(lambda conf: conf.get_service() in args.service.split(","))

    if args.clusters:
        filters.append(lambda conf: conf.get_cluster() in args.clusters.split(","))

    if args.instances:
        filters.append(lambda conf: conf.get_instance() in args.instances.split(","))

    if args.deploy_group:
        filters.append(
            lambda conf: conf.get_deploy_group() in args.deploy_group.split(",")
        )

    if args.registration:
        normalized_regs = normalize_registrations(
            service=args.service, registrations=args.registration.split(",")
        )
        filters.append(
            lambda conf: any(
                reg in normalized_regs
                for reg in (
                    conf.get_registrations()
                    if hasattr(conf, "get_registrations")
                    else []
                )
            )
        )

    if args.owner:
        owners = args.owner.split(",")

        filters.append(
            # If the instance owner is None, check the service owner, else check the instance owner
            lambda conf: get_team(
                overrides={}, service=conf.get_service(), soa_dir=args.soa_dir
            )
            in owners
            if conf.get_team() is None
            else conf.get_team() in owners
        )

    return filters


def apply_args_filters(
    args,
) -> Mapping[str, Mapping[str, Mapping[str, Type[InstanceConfig]]]]:
    """
    Take an args object and returns the dict of cluster:service:instances
    Currently, will filter by clusters, instances, services, and deploy_groups
    If no instances are found, will print a message and try to find matching instances
    for each service

    :param args: args object containing attributes to filter by
    :returns: Dict of dicts, in format {cluster_name: {service_name: {instance1, instance2}}}
    """
    clusters_services_instances: DefaultDict[
        str, DefaultDict[str, Dict[str, Type[InstanceConfig]]]
    ] = defaultdict(lambda: defaultdict(dict))

    if args.service:
        try:
            validate_service_name(args.service, soa_dir=args.soa_dir)
        except NoSuchService:
            paasta_print(
                PaastaColors.red(f'The service "{args.service}" does not exist.')
            )
            all_services = list_services(soa_dir=args.soa_dir)
            suggestions = difflib.get_close_matches(
                args.service, all_services, n=5, cutoff=0.5
            )
            if suggestions:
                paasta_print(PaastaColors.red(f"Did you mean any of these?"))
                for suggestion in suggestions:
                    paasta_print(PaastaColors.red(f"  {suggestion}"))
            return clusters_services_instances

        all_services = [args.service]
    else:
        all_services = list_services(soa_dir=args.soa_dir)

    if args.service is None and args.owner is None:
        args.service = figure_out_service_name(args, soa_dir=args.soa_dir)

    if args.clusters:
        clusters = args.clusters.split(",")
    else:
        clusters = list_clusters()

    if args.instances:
        instances = args.instances.split(",")
    else:
        instances = None

    filters = get_filters(args)

    i_count = 0
    for service in all_services:
        if args.service and service != args.service:
            continue

        for instance_conf in get_instance_configs_for_service(
            service, soa_dir=args.soa_dir, clusters=clusters, instances=instances
        ):
            if all([f(instance_conf) for f in filters]):
                cluster_service = clusters_services_instances[
                    instance_conf.get_cluster()
                ][service]
                cluster_service[instance_conf.get_instance()] = instance_conf.__class__
                i_count += 1

    if i_count == 0 and args.service and args.instances:
        for service in args.service.split(","):
            verify_instances(args.instances, service, clusters)

    return clusters_services_instances


def paasta_status(args) -> int:
    """Print the status of a Yelp service running on PaaSTA.
    :param args: argparse.Namespace obj created from sys.args by cli"""
    soa_dir = args.soa_dir
    system_paasta_config = load_system_paasta_config()

    if "USE_API_ENDPOINT" in os.environ:
        # bool will throw a ValueError if it doesn't recognize $USE_API_ENDPOINT
        use_api_endpoint = bool(strtobool(os.environ["USE_API_ENDPOINT"]))
    else:
        use_api_endpoint = True

    return_codes = [0]
    tasks = []
    clusters_services_instances = apply_args_filters(args)
    for cluster, service_instances in clusters_services_instances.items():
        for service, instances in service_instances.items():
            all_flink = all(i == FlinkDeploymentConfig for i in instances.values())
            actual_deployments: Mapping[str, str]
            if all_flink:
                actual_deployments = {}
            else:
                actual_deployments = get_actual_deployments(service, soa_dir)
            if all_flink or actual_deployments:
                deploy_pipeline = list(get_planned_deployments(service, soa_dir))
                tasks.append(
                    (
                        report_status_for_cluster,
                        dict(
                            service=service,
                            cluster=cluster,
                            deploy_pipeline=deploy_pipeline,
                            actual_deployments=actual_deployments,
                            instance_whitelist=instances,
                            system_paasta_config=system_paasta_config,
                            verbose=args.verbose,
                            use_api_endpoint=use_api_endpoint,
                        ),
                    )
                )
            else:
                paasta_print(missing_deployments_message(service))
                return_codes.append(1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        tasks = [executor.submit(t[0], **t[1]) for t in tasks]  # type: ignore
        for future in concurrent.futures.as_completed(tasks):  # type: ignore
            return_code, output = future.result()
            paasta_print("\n".join(output))
            return_codes.append(return_code)

    return max(return_codes)
