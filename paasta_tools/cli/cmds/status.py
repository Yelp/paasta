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
import shutil
import sys
from collections import Counter
from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from enum import Enum
from itertools import groupby
from typing import Any
from typing import Callable
from typing import Collection
from typing import DefaultDict
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Type
from typing import Union

import humanize
from mypy_extensions import Arg
from service_configuration_lib import read_deploy

from paasta_tools import kubernetes_tools
from paasta_tools.adhoc_tools import AdhocJobConfig
from paasta_tools.api.client import get_paasta_oapi_client
from paasta_tools.cassandracluster_tools import CassandraClusterDeploymentConfig
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import get_instance_configs_for_service
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import NoSuchService
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.cli.utils import verify_instances
from paasta_tools.flink_tools import FlinkDeploymentConfig
from paasta_tools.kafkacluster_tools import KafkaClusterDeploymentConfig
from paasta_tools.kubernetes_tools import format_pod_event_messages
from paasta_tools.kubernetes_tools import format_tail_lines_for_kubernetes_pod
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import KubernetesDeployStatus
from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.marathon_tools import MarathonDeployStatus
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.mesos_tools import format_tail_lines_for_mesos_task
from paasta_tools.monitoring_tools import get_team
from paasta_tools.monitoring_tools import list_teams
from paasta_tools.paastaapi.models import InstanceStatusKubernetesV2
from paasta_tools.paastaapi.models import KubernetesContainerV2
from paasta_tools.paastaapi.models import KubernetesPodV2
from paasta_tools.paastaapi.models import KubernetesVersion
from paasta_tools.tron_tools import TronActionConfig
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import format_table
from paasta_tools.utils import get_soa_cluster_deploy_files
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import is_under_replicated
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import load_deployments_json
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import remove_ansi_escape_sequences
from paasta_tools.utils import SystemPaastaConfig

FLINK_STATUS_MAX_THREAD_POOL_WORKERS = 50
ALLOWED_INSTANCE_CONFIG: Sequence[Type[InstanceConfig]] = [
    FlinkDeploymentConfig,
    CassandraClusterDeploymentConfig,
    KafkaClusterDeploymentConfig,
    KubernetesDeploymentConfig,
    AdhocJobConfig,
    MarathonServiceConfig,
    TronActionConfig,
]

InstanceStatusWriter = Callable[
    [
        Arg(str, "cluster"),
        Arg(str, "service"),
        Arg(str, "instance"),
        Arg(List[str], "output"),
        Arg(Any),
        Arg(int, "verbose"),
    ],
    int,
]


def add_subparser(
    subparsers,
) -> None:
    status_parser = subparsers.add_parser(
        "status",
        help="Display the status of a PaaSTA service.",
        description=(
            "'paasta status' queries the PaaSTA API in order to report "
            "on the overall health of a service."
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

    version = status_parser.add_mutually_exclusive_group()

    version.add_argument(
        "--new",
        dest="new",
        action="store_true",
        default=False,
        help="Use experimental new version of paasta status for services",
    )
    version.add_argument(
        "--old",
        dest="old",
        default=False,
        action="store_true",
        help="Use the old version of paasta status for services",
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
    status_parser.add_argument(
        "service_instance",
        nargs="?",
        help=f'A shorthand notation to {verb} instances. For example: "paasta status example_happyhour.canary,main"',
    )


def missing_deployments_message(
    service: str,
) -> str:
    message = (
        f"{service} has no deployments in deployments.json yet.\n  " "Has Jenkins run?"
    )
    return message


def get_deploy_info(
    deploy_file_path: str,
) -> Mapping:
    deploy_info = read_deploy(deploy_file_path)
    if not deploy_info:
        print("Error encountered with %s" % deploy_file_path)

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
        print(
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
    new: bool = False,
) -> int:
    output.append("    instance: %s" % PaastaColors.cyan(instance))
    client = get_paasta_oapi_client(cluster, system_paasta_config)
    if not client:
        print("Cannot get a paasta-api client")
        exit(1)
    try:
        status = client.service.status_instance(
            service=service,
            instance=instance,
            verbose=verbose,
            new=new,
            include_smartstack=False,
        )
    except client.api_error as exc:
        output.append(PaastaColors.red(exc.reason))
        return exc.status
    except (client.connection_error, client.timeout_error) as exc:
        output.append(
            PaastaColors.red(f"Could not connect to API: {exc.__class__.__name__}")
        )
        return 1
    except Exception as e:
        output.append(PaastaColors.red(f"Exception when talking to the API:"))
        output.append(str(e))
        return 1

    if status.git_sha != "":
        output.append("    Git sha:    %s (desired)" % status.git_sha)

    instance_type = find_instance_type(status)

    if system_paasta_config.get_enable_custom_cassandra_status_writer():
        if status.get("cassandracluster") is not None:
            instance_type = "cassandracluster"

    if instance_type is not None:
        # check the actual status value and call the corresponding status writer
        service_status_value = getattr(status, instance_type)
        writer_callable = INSTANCE_TYPE_WRITERS.get(instance_type)
        return writer_callable(
            cluster, service, instance, output, service_status_value, verbose
        )
    else:
        print(
            "Not implemented: Looks like %s is not a Marathon or Kubernetes instance"
            % instance
        )
        return 0


def find_instance_type(status: Any) -> str:
    """
    find_instance_type finds the instance type from the status api response it
    iterates over all instance type registered in `INSTANCE_TYPE_WRITERS`

    :param status: paasta api status object
    :return: the first matching instance type or else None
    """
    for instance_type in INSTANCE_TYPE_WRITERS.keys():
        if status.get(instance_type) is not None:
            return instance_type
    return None


def print_adhoc_status(
    cluster: str,
    service: str,
    instance: str,
    output: List[str],
    status,
    verbose: int = 0,
) -> int:
    output.append(f"    Job: {instance}")
    for run in status.value:
        output.append(
            "Launch time: %s, run id: %s, framework id: %s"
            % (run["launch_time"], run["run_id"], run["framework_id"])
        )
    if status.value:
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
    cluster: str,
    service: str,
    instance: str,
    output: List[str],
    marathon_status,
    verbose: int = 0,
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
        marathon_status.mesos, marathon_status.expected_instance_count
    )
    output.extend([f"    {line}" for line in mesos_status_human])

    smartstack = marathon_status.smartstack
    if smartstack is not None:
        smartstack_status_human = get_smartstack_status_human(
            smartstack.registration,
            smartstack.expected_backends_per_location,
            smartstack.locations,
        )
        output.extend([f"    {line}" for line in smartstack_status_human])

    envoy = marathon_status.envoy
    if envoy is not None:
        envoy_status_human = get_envoy_status_human(
            envoy.registration,
            envoy.expected_backends_per_location,
            envoy.locations,
        )
        output.extend([f"    {line}" for line in envoy_status_human])

    return 0


def create_autoscaling_info_table(autoscaling_info):
    output = ["Autoscaling Info:"]

    if autoscaling_info.current_utilization is not None:
        current_utilization = "{:.1f}%".format(
            autoscaling_info.current_utilization * 100
        )
    else:
        current_utilization = "Exception"

    target_instances = autoscaling_info.target_instances
    if target_instances is None:
        target_instances = "Exception"

    headers = [
        "Current instances",
        "Max instances",
        "Min instances",
        "Current utilization",
        "Target instances",
    ]
    row = [
        autoscaling_info.current_instances,
        autoscaling_info.max_instances,
        autoscaling_info.min_instances,
        current_utilization,
        target_instances,
    ]
    row = [str(e) for e in row]
    table = [f"  {line}" for line in format_table([headers, row])]
    output.extend(table)
    return output


def marathon_mesos_status_human(
    mesos_status,
    expected_instance_count,
):
    if mesos_status.error_message:
        return [f"Mesos: {PaastaColors.red(mesos_status.error_message)}"]

    output = []
    output.append(
        marathon_mesos_status_summary(
            mesos_status.get("running_task_count", 0), expected_instance_count
        )
    )

    running_tasks = mesos_status.running_tasks
    non_running_tasks = mesos_status.non_running_tasks
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

    if "tasks" in app_status and app_status.tasks:
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
        local_deployed_datetime = datetime.fromtimestamp(task.deployed_timestamp)
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


def format_kubernetes_pod_table(pods, verbose: int):
    rows: List[Union[tuple, str]] = [
        ("Pod ID", "Host deployed to", "Deployed at what localtime", "Health")
    ]
    for pod in pods:
        local_deployed_datetime = datetime.fromtimestamp(pod.deployed_timestamp)
        hostname = f"{pod.host}" if pod.host is not None else PaastaColors.grey("N/A")
        phase = pod.phase
        reason = pod.reason
        if phase is None or phase == "Pending":
            health_check_status = PaastaColors.grey("N/A")
        elif phase == "Running":
            health_check_status = PaastaColors.green("Healthy")
            if not pod.ready:
                health_check_status = PaastaColors.red("Unhealthy")
        elif phase == "Failed" and reason == "Evicted":
            health_check_status = PaastaColors.red("Evicted")
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
        if pod.events and verbose > 1:
            rows.extend(format_pod_event_messages(pod.events, pod.name))
        if pod.message is not None:
            rows.append(PaastaColors.grey(f"  {pod.message}"))
        if len(pod.containers) > 0:
            rows.extend(format_tail_lines_for_kubernetes_pod(pod.containers, pod.name))

    return format_table(rows)


def format_kubernetes_replicaset_table(replicasets):
    rows = [
        (
            "ReplicaSet Name",
            "Ready / Desired",
            "Created at what localtime",
            "Service git SHA",
            "Config hash",
        )
    ]
    for replicaset in replicasets:
        local_created_datetime = datetime.fromtimestamp(replicaset.create_timestamp)

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
                replicaset.git_sha if replicaset.git_sha else "Unknown",
                replicaset.config_sha if replicaset.config_sha else "Unknown",
            )
        )

    return format_table(rows)


def get_smartstack_status_human(
    registration: str,
    expected_backends_per_location: int,
    locations: Collection[Any],
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


def build_smartstack_backends_table(backends: Iterable[Any]) -> List[str]:
    rows: List[Tuple[str, ...]] = [("Name", "LastCheck", "LastChange", "Status")]
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

        row: Tuple[str, ...] = (
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


def get_envoy_status_human(
    registration: str,
    expected_backends_per_location: int,
    locations: Collection[Any],
) -> List[str]:
    if len(locations) == 0:
        return [f"Envoy: ERROR - {registration} is NOT in Envoy at all!"]

    output = ["Envoy:"]
    output.append(f"  Service Name: {registration}")
    output.append(f"  Backends:")
    for location in locations:
        backend_status = envoy_backend_report(
            expected_backends_per_location, location.running_backends_count
        )
        output.append(f"    {location.name} - {backend_status}")

        if location.backends:
            color = (
                PaastaColors.green
                if location.is_proxied_through_casper
                else PaastaColors.grey
            )
            is_proxied_through_casper_output = color(
                f"{location.is_proxied_through_casper}"
            )
            output.append(
                f"      Proxied through Casper: {is_proxied_through_casper_output}"
            )

            backends_table = build_envoy_backends_table(location.backends)
            output.extend([f"      {line}" for line in backends_table])

    return output


def build_envoy_backends_table(backends: Iterable[Any]) -> List[str]:
    rows: List[Tuple[str, ...]] = [("Hostname:Port", "Weight", "Status")]
    for backend in backends:
        if backend.eds_health_status == "HEALTHY":
            status = PaastaColors.default(backend.eds_health_status)
        elif backend.eds_health_status == "UNHEALTHY":
            status = PaastaColors.red(backend.eds_health_status)
        else:
            status = PaastaColors.yellow(backend.eds_health_status)

        row: Tuple[str, ...] = (
            f"{backend.hostname}:{backend.port_value}",
            f"{backend.weight}",
            status,
        )

        if not backend.has_associated_task:
            row = tuple(
                PaastaColors.grey(remove_ansi_escape_sequences(col)) for col in row
            )

        rows.append(row)

    return format_table(rows)


def kubernetes_app_deploy_status_human(status, message, backoff_seconds=None):
    status_string = kubernetes_tools.KubernetesDeployStatus.tostring(status)

    if status in {
        kubernetes_tools.KubernetesDeployStatus.Waiting,
        kubernetes_tools.KubernetesDeployStatus.Stopped,
    }:
        deploy_status = PaastaColors.red(status_string)
    elif status == kubernetes_tools.KubernetesDeployStatus.Deploying:
        deploy_status = PaastaColors.yellow(status_string)
    elif status == kubernetes_tools.KubernetesDeployStatus.Running:
        deploy_status = PaastaColors.bold(status_string)
    else:
        deploy_status = status_string

    if message:
        deploy_status += f" ({message})"
    return deploy_status


def status_kubernetes_job_human(
    service: str,
    instance: str,
    deploy_status: str,
    desired_app_id: str,
    app_count: int,
    running_instances: int,
    normal_instance_count: int,
    evicted_count: int,
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

        evicted = (
            PaastaColors.red(str(evicted_count))
            if evicted_count > 0
            else PaastaColors.green(str(evicted_count))
        )
        return (
            "Kubernetes:   {} - up with {} instances ({} evicted). Status: {}".format(
                status, instance_count, evicted, deploy_status
            )
        )
    else:
        status = PaastaColors.yellow("Warning")
        return "Kubernetes:   {} - {} (app {}) is not configured in Kubernetes yet (waiting for bounce)".format(
            status, name, desired_app_id
        )


def get_flink_job_name(flink_job):
    return flink_job["name"].split(".", 2)[-1]


def should_job_info_be_shown(cluster_state):
    return (
        cluster_state == "running"
        or cluster_state == "stoppingsupervisor"
        or cluster_state == "cleanupsupervisor"
    )


def get_pod_uptime(pod_deployed_timestamp: str):
    pod_creation_time = datetime.strptime(pod_deployed_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    pod_uptime = datetime.utcnow() - pod_creation_time
    pod_uptime_total_seconds = pod_uptime.total_seconds()
    pod_uptime_days = divmod(pod_uptime_total_seconds, 86400)
    pod_uptime_hours = divmod(pod_uptime_days[1], 3600)
    pod_uptime_minutes = divmod(pod_uptime_hours[1], 60)
    pod_uptime_seconds = divmod(pod_uptime_minutes[1], 1)
    return f"{int(pod_uptime_days[0])}d{int(pod_uptime_hours[0])}h{int(pod_uptime_minutes[0])}m{int(pod_uptime_seconds[0])}s"


def append_pod_status(pod_status, output: List[str]):
    output.append(f"    Pods:")
    rows: List[Union[str, Tuple[str, str, str, str]]] = [
        ("Pod Name", "Host", "Phase", "Uptime")
    ]
    for pod in pod_status:
        color_fn = (
            PaastaColors.green
            if pod["phase"] == "Running" and pod["container_state"] == "Running"
            else PaastaColors.red
            # pods can get stuck in phase: Running and state: CrashLoopBackOff, so check for that
            if pod["phase"] == "Failed"
            or pod["container_state_reason"] == "CrashLoopBackOff"
            else PaastaColors.yellow
        )

        rows.append(
            (
                pod["name"],
                pod["host"],
                color_fn(pod["phase"]),
                get_pod_uptime(pod["deployed_timestamp"]),
            )
        )
        if "reason" in pod and pod["reason"] != "":
            rows.append(PaastaColors.grey(f"  {pod['reason']}: {pod['message']}"))
        if "container_state" in pod and pod["container_state"] != "Running":
            rows.append(
                PaastaColors.grey(
                    f"  {pod['container_state']}: {pod['container_state_reason']}"
                )
            )
    pods_table = format_table(rows)
    output.extend([f"      {line}" for line in pods_table])


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
    labels = metadata.get("labels")
    config_sha = labels.get(paasta_prefixed("config_sha"))
    if config_sha is None:
        raise ValueError(f"expected config sha on Flink, but received {metadata}")
    if config_sha.startswith("config"):
        config_sha = config_sha[6:]

    output.append(f"    Config SHA: {config_sha}")

    status_config = status["config"]
    if verbose:
        output.append(
            f"    Flink version: {status_config['flink-version']} {status_config['flink-revision']}"
        )
    else:
        output.append(f"    Flink version: {status_config['flink-version']}")
    # Annotation "flink.yelp.com/dashboard_url" is populated by flink-operator
    dashboard_url = metadata["annotations"].get("flink.yelp.com/dashboard_url")
    output.append(f"    URL: {dashboard_url}/")

    color = PaastaColors.green if status["state"] == "running" else PaastaColors.yellow
    output.append(f"    State: {color(status['state'].title())}")

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
        output.append(f"    No other information available in non-running state")
        return 0

    output.append(
        "    Jobs:"
        f" {status['overview']['jobs-running']} running,"
        f" {status['overview']['jobs-finished']} finished,"
        f" {status['overview']['jobs-failed']} failed,"
        f" {status['overview']['jobs-cancelled']} cancelled"
    )
    output.append(
        "   "
        f" {status['overview']['taskmanagers']} taskmanagers,"
        f" {status['overview']['slots-available']}/{status['overview']['slots-total']} slots available"
    )

    # Avoid cutting job name. As opposed to default hardcoded value of 32, we will use max length of job name
    if status["jobs"]:
        max_job_name_length = max(
            [len(get_flink_job_name(job)) for job in status["jobs"]]
        )
    else:
        max_job_name_length = 10
    # Apart from this column total length of one row is around 52 columns, using remaining terminal columns for job name
    # Note: for terminals smaller than 90 columns the row will overflow in verbose printing
    allowed_max_job_name_length = min(
        max(10, shutil.get_terminal_size().columns - 52), max_job_name_length
    )

    output.append(f"    Jobs:")
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
        sorted(jobs, key=lambda j: -j["start-time"])[0]
        for _, jobs in groupby(
            sorted(
                (j for j in status["jobs"] if j.get("name") and j.get("start-time")),
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
        start_time = datetime.fromtimestamp(int(job["start-time"]) // 1000)
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


def print_kubernetes_status_v2(
    cluster: str,
    service: str,
    instance: str,
    output: List[str],
    status: InstanceStatusKubernetesV2,
    verbose: int = 0,
) -> int:
    instance_state = get_instance_state(status)
    output.append(f"    State: {instance_state}")
    output.append("    Running versions:")
    if not verbose:
        output.append(
            "      " + PaastaColors.green("Rerun with -v to see all replicas")
        )
    elif verbose < 2:
        output.append(
            "      "
            + PaastaColors.green(
                "You can use paasta logs to view stdout/stderr or rerun with -vv for even more information."
            )
        )
    output.extend(
        [
            f"      {line}"
            for line in get_versions_table(
                status.versions, service, instance, cluster, verbose
            )
        ]
    )

    if verbose > 1:
        output.extend(get_autoscaling_table(status.autoscaling_status, verbose))

    if status.error_message:
        output.append("    " + PaastaColors.red(status.error_message))
        return 1
    else:
        return 0


# TODO: Make an enum class or similar for the various instance states
def get_instance_state(status: InstanceStatusKubernetesV2) -> str:
    num_versions = len(status.versions)
    num_ready_replicas = sum(r.ready_replicas for r in status.versions)
    if status.desired_state == "stop":
        if all(version.replicas == 0 for version in status.versions):
            return PaastaColors.red("Stopped")
        else:
            return PaastaColors.red("Stopping")
    elif status.desired_state == "start":
        if num_versions == 0:
            if status.desired_instances == 0:
                return PaastaColors.red("Stopped")
            else:
                return PaastaColors.yellow("Starting")
        if num_versions == 1:
            if num_ready_replicas < status.desired_instances:
                return PaastaColors.yellow("Launching replicas")
            else:
                return PaastaColors.green("Running")
        else:
            versions = sorted(status.versions, key=lambda x: x.create_timestamp)
            git_shas = {r.git_sha for r in versions}
            config_shas = {r.config_sha for r in versions}
            bouncing_to = []
            if len(git_shas) > 1:
                bouncing_to.append(versions[0].git_sha[:8])
            if len(config_shas) > 1:
                bouncing_to.append(versions[0].config_sha)

            bouncing_to_str = ", ".join(bouncing_to)
            return PaastaColors.yellow(f"Bouncing to {bouncing_to_str}")
    else:
        return PaastaColors.red("Unknown")


def get_versions_table(
    versions: List[KubernetesVersion],
    service: str,
    instance: str,
    cluster: str,
    verbose: int = 0,
) -> List[str]:
    if len(versions) == 0:
        return [PaastaColors.red("There are no running versions for this instance")]
    elif len(versions) == 1:
        return get_version_table_entry(
            versions[0], service, instance, cluster, verbose=verbose
        )
    else:
        versions = sorted(versions, key=lambda x: x.create_timestamp, reverse=True)
        config_shas = {v.config_sha for v in versions}
        if len(config_shas) > 1:
            show_config_sha = True
        else:
            show_config_sha = False

        table: List[str] = []
        table.extend(
            get_version_table_entry(
                versions[0],
                service,
                instance,
                cluster,
                version_name_suffix="new",
                show_config_sha=show_config_sha,
                verbose=verbose,
            )
        )
        for version in versions[1:]:
            table.extend(
                get_version_table_entry(
                    version,
                    service,
                    instance,
                    cluster,
                    version_name_suffix="old",
                    show_config_sha=show_config_sha,
                    verbose=verbose,
                )
            )
        return table


def get_version_table_entry(
    version: KubernetesVersion,
    service: str,
    instance: str,
    cluster: str,
    version_name_suffix: str = None,
    show_config_sha: bool = False,
    verbose: int = 0,
) -> List[str]:
    version_name = version.git_sha[:8]
    if show_config_sha or verbose > 1:
        version_name += f", {version.config_sha}"
    if version_name_suffix is not None:
        version_name += f" ({version_name_suffix})"
    version_name = PaastaColors.blue(version_name)

    start_datetime = datetime.fromtimestamp(version.create_timestamp)
    humanized_start_time = humanize.naturaltime(start_datetime)
    entry = [f"{version_name} - Started {start_datetime} ({humanized_start_time})"]
    replica_states = get_replica_states(version.pods)
    replica_states = sorted(replica_states, key=lambda s: s[1].create_timestamp)
    if len(replica_states) == 0:
        message = PaastaColors.red("0 pods found")
        entry.append(f"  {message}")
    if replica_states:
        # If no replica_states, there were no pods found
        replica_state_counts = Counter([state for state, pod in replica_states])
        replica_state_display = [
            state.color(f"{replica_state_counts[state]} {state.message}")
            for state in ReplicaState
            if state in replica_state_counts
        ]
        entry.append(f"  Replica States: {' / '.join(replica_state_display)}")
        if not verbose:
            unhealthy_replicas = [
                (state, pod) for state, pod in replica_states if state.is_unhealthy()
            ]
            if unhealthy_replicas:
                entry.append(f"    Unhealthy Replicas:")
                replica_table = create_replica_table(
                    unhealthy_replicas, service, instance, cluster, verbose
                )
                for line in replica_table:
                    entry.append(f"      {line}")
        else:
            replica_table = create_replica_table(
                replica_states, service, instance, cluster, verbose
            )
            for line in replica_table:
                entry.append(f"    {line}")
    return entry


class ReplicaState(Enum):
    # Order will be preserved in count summary
    RUNNING = "Healthy", PaastaColors.green
    UNREACHABLE = "Unreachable", PaastaColors.red
    NOT_RUNNING = "Not Running", PaastaColors.red
    WARNING = "Warning", PaastaColors.yellow
    UNSCHEDULED = "Unscheduled", PaastaColors.yellow
    STARTING = "Starting", PaastaColors.yellow
    WARMING_UP = "Warming Up", PaastaColors.cyan
    TERMINATING = "Terminating", PaastaColors.cyan
    UNKNOWN = "Unknown", PaastaColors.yellow

    def is_unhealthy(self):
        return self.name in ["UNREACHABLE", "NOT_RUNNING"]

    @property
    def color(self) -> Callable:
        return self.value[1]

    @property
    def formatted_message(self):
        return self.value[1](self.value[0])

    @property
    def message(self):
        return self.value[0]


def recent_liveness_failure(pod: KubernetesPodV2) -> bool:
    if not pod.events:
        return False
    return any(
        [evt for evt in pod.events if "Liveness probe failed" in evt.get("message", "")]
    )


def recent_container_restart(
    container: Optional[KubernetesContainerV2], time_window: int = 900
) -> bool:
    if container:
        return kubernetes_tools.recent_container_restart(
            container.restart_count,
            container.last_state,
            container.last_timestamp,
            time_window_s=time_window,
        )
    return False


def get_main_container(pod: KubernetesPodV2) -> Optional[KubernetesContainerV2]:
    return next(
        (
            c
            for c in pod.containers
            if c.name not in kubernetes_tools.SIDECAR_CONTAINER_NAMES
        ),
        None,
    )


def get_replica_state(pod: KubernetesPodV2) -> ReplicaState:
    phase = pod.phase
    state = ReplicaState.UNKNOWN
    if phase is None or not pod.scheduled:
        state = ReplicaState.UNSCHEDULED
    elif pod.delete_timestamp:
        state = ReplicaState.TERMINATING
    elif phase == "Pending":
        if not pod.containers or all([c.state == "waiting" for c in pod.containers]):
            state = ReplicaState.STARTING
        else:
            state = ReplicaState.NOT_RUNNING
    elif phase == "Running":
        ####
        # TODO: Take sidecar containers into account
        #   This logic likely needs refining
        main_container = get_main_container(pod)
        if main_container:
            # K8s API is returning timestamps in YST, so we use now() instead of utcnow()
            warming_up = (
                pod.create_timestamp + main_container.healthcheck_grace_period
                > datetime.now().timestamp()
            )
            if pod.mesh_ready is False:
                if main_container.state != "running":
                    state = ReplicaState.NOT_RUNNING
                else:
                    state = ReplicaState.UNREACHABLE
            elif not pod.ready:
                state = ReplicaState.NOT_RUNNING
            else:
                if recent_liveness_failure(pod) or recent_container_restart(
                    main_container
                ):
                    state = ReplicaState.WARNING
                else:
                    state = ReplicaState.RUNNING

            if state != ReplicaState.RUNNING and warming_up:
                state = ReplicaState.WARMING_UP

        else:
            state = ReplicaState.UNKNOWN

        # Catch
    elif phase == "Failed":
        # e.g. pod.reason == evicted
        state = ReplicaState.NOT_RUNNING
    return state


def get_replica_states(
    pods: List[KubernetesPodV2],
) -> List[Tuple[ReplicaState, KubernetesPodV2]]:
    return [(get_replica_state(pod), pod) for pod in pods]


def create_replica_table(
    pods: List[Tuple[ReplicaState, KubernetesPodV2]],
    service: str,
    instance: str,
    cluster: str,
    verbose: int = 0,
) -> List[str]:
    header = ["ID", "IP/Port", "Host deployed to", "Started at what localtime", "State"]
    table: List[Union[List[str], str]] = [header]
    for state, pod in pods:
        start_datetime = datetime.fromtimestamp(pod.create_timestamp)
        humanized_start_time = humanize.naturaltime(start_datetime)
        row = [
            pod.name,
            f"{pod.ip}:8888" if pod.ip else "None",
            pod.host or "None",
            humanized_start_time,
            state.formatted_message,
        ]
        table.append(row)

        # Adding additional context/tips
        if pod.reason == "Evicted":
            table.append(
                PaastaColors.red(
                    f'  Evicted: {pod.message if pod.message else "Unknown reason"}'
                )
            )

        main_container = get_main_container(pod)
        if main_container:
            if main_container.timestamp:
                timestamp = datetime.fromtimestamp(main_container.timestamp)
            elif main_container.last_timestamp:
                timestamp = datetime.fromtimestamp(main_container.last_timestamp)
            else:
                # if no container timestamps are found, use pod's creation
                timestamp = start_datetime

            humanized_timestamp = humanize.naturaltime(timestamp)
            if recent_container_restart(main_container):
                table.append(
                    PaastaColors.red(
                        f"  Restarted {humanized_timestamp}. {main_container.restart_count} restarts since starting"
                    )
                )
            if (
                main_container.reason == "OOMKilled"
                or main_container.last_reason == "OOMKilled"
            ):
                if main_container.reason == "OOMKilled":
                    oom_kill_timestamp = timestamp
                    human_oom_kill_timestamp = humanized_timestamp
                elif main_container.last_reason == "OOMKilled":
                    oom_kill_timestamp = datetime.fromtimestamp(
                        main_container.last_timestamp
                    )
                    human_oom_kill_timestamp = humanize.naturaltime(oom_kill_timestamp)
                table.extend(
                    [
                        PaastaColors.red(
                            f"  OOM Killed {human_oom_kill_timestamp} ({oom_kill_timestamp})."
                        ),
                        PaastaColors.red(
                            f"    Check y/check-oom-events and consider increasing memory in yelpsoa_configs"
                        ),
                    ]
                )
            if state == ReplicaState.WARMING_UP:
                if verbose > 0:
                    warmup_duration = datetime.now().timestamp() - pod.create_timestamp
                    humanized_duration = humanize.naturaldelta(
                        timedelta(seconds=warmup_duration)
                    )
                    grace_period_remaining = (
                        pod.create_timestamp
                        + main_container.healthcheck_grace_period
                        - datetime.now().timestamp()
                    )
                    humanized_remaining = humanize.naturaldelta(
                        timedelta(seconds=grace_period_remaining)
                    )
                    table.append(
                        PaastaColors.cyan(
                            f"  Still warming up, {humanized_duration} elapsed, {humanized_remaining} before healthchecking starts"
                        )
                    )
        if recent_liveness_failure(pod) and state != ReplicaState.TERMINATING:
            healthcheck_string = (
                "check your healthcheck configuration in yelpsoa_configs"
            )
            if main_container and main_container.healthcheck_cmd:
                if main_container.healthcheck_cmd.http_url:
                    healthcheck_string = (
                        f"run `curl {main_container.healthcheck_cmd.http_url}`"
                    )
                elif main_container.healthcheck_cmd.tcp_port:
                    healthcheck_string = f"verify your service is listening on {main_container.healthcheck_cmd.tcp_port}"
                elif main_container.healthcheck_cmd.cmd:
                    healthcheck_string = f"check why the following may be failing: `{main_container.healthcheck_cmd.cmd}`"
            table.append(
                PaastaColors.red(
                    f"  Healthchecks are failing. To investigate further, {healthcheck_string}"
                )
            )
        if state.is_unhealthy() or recent_container_restart(main_container):
            if verbose < 2:
                table.append(
                    PaastaColors.red(
                        f"  Consider checking logs with `paasta logs -c {cluster} -s {service} -i {instance} -p {pod.name}`"
                    )
                )
            else:
                if pod.events:
                    table.extend(format_pod_event_messages(pod.events, pod.name))
                if len(pod.containers) > 0:
                    table.extend(
                        format_tail_lines_for_kubernetes_pod(pod.containers, pod.name)
                    )
        elif state == ReplicaState.UNSCHEDULED:
            if pod.reason == "Unschedulable":
                table.append(PaastaColors.red(f"  Pod is unschedulable: {pod.message}"))
        elif state == ReplicaState.UNKNOWN:
            table.append(
                PaastaColors.red(
                    f"  Cannot determine pod state, please try again. If you continue to see this state, please contact #paasta"
                )
            )
    return format_table(table)


def get_autoscaling_table(
    autoscaling_status: Dict[str, Any], verbose: int = 0
) -> List[str]:
    table = []
    if autoscaling_status and verbose > 1:
        table.append("    Autoscaling status:")
        table.append(f"       min_instances: {autoscaling_status['min_instances']}")
        table.append(f"       max_instances: {autoscaling_status['max_instances']}")
        table.append(
            f"       Desired instances: {autoscaling_status['desired_replicas']}"
        )
        table.append(f"       Last scale time: {autoscaling_status['last_scale_time']}")
        table.append(f"       Dashboard: y/sfx-autoscaling")
        NA = PaastaColors.red("N/A")
        if len(autoscaling_status["metrics"]) > 0:
            table.append(f"       Metrics:")

        metrics_table: List[List[str]] = [["Metric", "Current", "Target"]]
        for metric in autoscaling_status["metrics"]:
            current_metric = (
                NA
                if getattr(metric, "current_value") is None
                else getattr(metric, "current_value")
            )
            target_metric = (
                NA
                if getattr(metric, "target_value") is None
                else getattr(metric, "target_value")
            )
            metrics_table.append([metric["name"], current_metric, target_metric])
        table.extend(["         " + s for s in format_table(metrics_table)])

    return format_table(table)


def print_kubernetes_status(
    cluster: str,
    service: str,
    instance: str,
    output: List[str],
    kubernetes_status,
    verbose: int = 0,
) -> int:
    bouncing_status = bouncing_status_human(
        kubernetes_status.app_count, kubernetes_status.bounce_method
    )
    desired_state = desired_state_human(
        kubernetes_status.desired_state, kubernetes_status.expected_instance_count
    )
    output.append(f"    State:      {bouncing_status} - Desired state: {desired_state}")

    status = KubernetesDeployStatus.fromstring(kubernetes_status.deploy_status)
    deploy_status = kubernetes_app_deploy_status_human(
        status, kubernetes_status.deploy_status_message
    )

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
                evicted_count=kubernetes_status.evicted_count,
            )
        )
    )
    if kubernetes_status.create_timestamp and verbose > 0:
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
        pods_table = format_kubernetes_pod_table(kubernetes_status.pods, verbose)
        output.extend([f"        {line}" for line in pods_table])

    if kubernetes_status.replicasets and len(kubernetes_status.replicasets) > 0:
        output.append("      ReplicaSets:")
        replicasets_table = format_kubernetes_replicaset_table(
            kubernetes_status.replicasets
        )
        output.extend([f"        {line}" for line in replicasets_table])

    autoscaling_status = kubernetes_status.autoscaling_status
    if autoscaling_status and verbose > 0:
        output.append("    Autoscaling status:")
        output.append(f"       min_instances: {autoscaling_status['min_instances']}")
        output.append(f"       max_instances: {autoscaling_status['max_instances']}")
        output.append(
            f"       Desired instances: {autoscaling_status['desired_replicas']}"
        )
        output.append(
            f"       Last scale time: {autoscaling_status['last_scale_time']}"
        )
        output.append(f"       Dashboard: y/was-it-the-autoscaler")
        NA = PaastaColors.red("N/A")
        if len(autoscaling_status["metrics"]) > 0:
            output.append(f"       Metrics:")

        metrics_table: List[List[str]] = [["Metric", "Current", "Target"]]
        for metric in autoscaling_status["metrics"]:
            current_metric = (
                NA
                if getattr(metric, "current_value") is None
                else getattr(metric, "current_value")
            )
            target_metric = (
                NA
                if getattr(metric, "target_value") is None
                else getattr(metric, "target_value")
            )
            metrics_table.append([metric["name"], current_metric, target_metric])
        output.extend(["         " + s for s in format_table(metrics_table)])

    if kubernetes_status.smartstack is not None:
        smartstack_status_human = get_smartstack_status_human(
            kubernetes_status.smartstack.registration,
            kubernetes_status.smartstack.expected_backends_per_location,
            kubernetes_status.smartstack.locations,
        )
        output.extend([f"    {line}" for line in smartstack_status_human])

    if kubernetes_status.envoy is not None:
        envoy_status_human = get_envoy_status_human(
            kubernetes_status.envoy.registration,
            kubernetes_status.envoy.expected_backends_per_location,
            kubernetes_status.envoy.locations,
        )
        output.extend([f"    {line}" for line in envoy_status_human])

    error_message = kubernetes_status.error_message
    if error_message:
        output.append("    " + PaastaColors.red(error_message))
        return 1
    return 0


def print_tron_status(
    cluster: str,
    service: str,
    instance: str,
    output: List[str],
    tron_status,
    verbose: int = 0,
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


def print_cassandra_status(
    cluster: str,
    service: str,
    instance: str,
    output: List[str],
    cassandra_status,
    verbose: int = 0,
) -> int:
    tab = "    "
    indent = 1

    status = cassandra_status.get("status")
    if status is None:
        output.append(
            indent * tab + PaastaColors.red("Cassandra cluster is not available yet")
        )
        return 1

    output.append(indent * tab + "Cassandra cluster:")
    indent += 1

    status = cassandra_status.get("status")
    state = status.get("state")

    if state == "Running":
        state = PaastaColors.green(state)
    else:
        state = PaastaColors.red(state)

    nodes: List[Dict[str, Any]] = status.get("nodes") or []
    output.append(indent * tab + "State: " + state)

    if not nodes:
        output.append(
            indent * tab + "Nodes: " + PaastaColors.red("No node status available")
        )
        return 0

    output.append(indent * tab + "Nodes:")
    indent += 1
    all_rows: List[CassandraNodeStatusRow] = []

    for node in nodes:
        if node.get("properties"):
            row: CassandraNodeStatusRow = {}
            for prop in node.get("properties"):
                verbosity = prop.get("verbosity", 0)
                name = prop["name"]

                if verbosity > verbose:
                    continue
                if not prop.get("name"):
                    continue

                row[name] = node_property_to_str(prop, verbose)
            all_rows.append(row)
        else:
            # TODO: Remove this section when properties is deployed on all
            # clusters (see DREIMP-7953):
            now = datetime.now(timezone.utc)
            ip = node.get("ip")
            err = node.get("error")
            start_age = "None"
            if node.get("startTime"):
                start_time = datetime.strptime(
                    node.get("startTime"), "%Y-%m-%dT%H:%M:%SZ"
                ).replace(tzinfo=timezone.utc)
                start_age = (
                    humanize.naturaldelta(
                        timedelta(seconds=(now - start_time).total_seconds())
                    )
                    + " ago"
                )
            inspect_time = datetime.strptime(
                node.get("inspectTime"), "%Y-%m-%dT%H:%M:%SZ"
            ).replace(tzinfo=timezone.utc)
            details = node.get("details")

            inspect_age = (
                humanize.naturaldelta(
                    timedelta(seconds=(now - inspect_time).total_seconds())
                )
                + " ago"
            )

            if err is not None:
                row = {}
                row["IP"] = ip
                row["StartTime"] = start_age
                row["InspectedAt"] = inspect_age
                row["Error"] = PaastaColors.red(err)
                if verbose > 0:
                    # Show absolute times:
                    row["StartTime"] = node.get("startTime", "None")
                    row["InspectedAt"] = node.get("inspectTime", "None")
                all_rows.append(row)
            elif details:
                row = {}
                row["IP"] = ip
                row["Available"] = "Yes" if details.get("available") else "No"
                row["OperationMode"] = details.get("operationMode")
                row["Joined"] = "Yes" if details.get("joined") else "No"
                row["Datacenter"] = details.get("datacenter")
                row["Rack"] = details.get("rack")
                row["Load"] = details.get("loadString")
                row["Tokens"] = str(details.get("tokenRangesCount"))
                row["StartTime"] = start_age
                row["InspectedAt"] = inspect_age
                if verbose > 0:
                    row["Starting"] = "Yes" if details.get("starting") else "No"
                    row["Initialized"] = "Yes" if details.get("initialized") else "No"
                    row["Drained"] = "Yes" if details.get("drained") else "No"
                    row["Draining"] = "Yes" if details.get("draining") else "No"
                    # Show absolute times:
                    row["StartTime"] = node.get("startTime", "None")
                    row["InspectedAt"] = node.get("inspectTime", "None")
                if verbose > 1:
                    row["LocalHostID"] = details.get("localHostId")
                    row["Schema"] = details.get("schemaVersion")
                    row["RemovalStatus"] = details.get("removalStatus")
                    row["DrainProgress"] = details.get("drainProgress")
                    row["RPCServerRunning"] = (
                        "Yes" if details.get("rpcServerRunning") else "No"
                    )
                    row["NativeTransportRunning"] = (
                        "Yes" if details.get("nativeTransportRunning") else "No"
                    )
                    row["GossipRunning"] = (
                        "Yes" if details.get("gossipRunning") else "No"
                    )
                    row["IncBackupEnabled"] = (
                        "Yes" if details.get("incrementalBackupsEnabled") else "No"
                    )
                    row["Version"] = details.get("releaseVersion")
                    row["ClusterName"] = details.get("clusterName")
                    row["HintsInProgress"] = str(details.get("hintsInProgress"))
                    row["ReadRepairAttempted"] = str(details.get("readRepairAttempted"))
                    row["NumberOfTables"] = str(details.get("numberOfTables"))
                    row["TotalHints"] = str(details.get("totalHints"))
                    row["HintedHandoffEnabled"] = (
                        "Yes" if details.get("hintedHandoffEnabled") else "No"
                    )
                    row["LoggingLevels"] = str(details.get("loggingLevels"))
                all_rows.append(row)

    if verbose < 2:
        for rows in group_nodes_by_header(all_rows):
            lines = nodes_to_lines(verbose, rows)
            ftable = format_table(lines)
            output.extend([indent * tab + line for line in ftable])
            output.extend([indent * tab])
    else:
        for rows in group_nodes_by_header(all_rows):
            for node in rows:
                output.append(indent * tab + "Node:")
                indent += 1
                for key in node.keys():
                    output.append(
                        indent * tab + "{key}: {value}".format(key=key, value=node[key])
                    )
                indent -= 1
    return 0


CassandraNodeStatusRow = Dict[str, str]


# group_nodes_by_header groups the given nodes into several lists of rows. The
# rows in each group have the same headers.
def group_nodes_by_header(
    rows: List[CassandraNodeStatusRow] = [],
) -> List[List[CassandraNodeStatusRow]]:
    groups: Dict[str, List[CassandraNodeStatusRow]] = {}
    for row in rows:
        header = list(row.keys())
        header.sort()
        # "\0" is just a character that is unlikely to be in the header names.
        header_id = "\0".join(header)
        group = groups.get(header_id, [])
        group.append(row)
        groups[header_id] = group

    return list(groups.values())


def nodes_to_lines(
    verbose: int = 0,
    rows: List[CassandraNodeStatusRow] = [],
) -> List[List[str]]:
    header: List[str] = []
    lines: List[List[str]] = []
    for row in rows:
        if len(header) == 0:
            header = list(row.keys())
            lines.append(list(header))
        line: List[str] = []
        for key in header:
            line.append(row.get(key, ""))
        lines.append(line)
    return lines


def node_property_to_str(prop: Dict[str, Any], verbose: int) -> str:
    typ = prop.get("type")
    value = prop.get("value")

    if value is None:
        return "None"

    if typ == "string":
        return value
    elif typ in ["int", "float64"]:
        return str(value)
    elif typ == "bool":
        return "Yes" if value else "No"
    elif typ == "error":
        return PaastaColors.red(value)
    elif typ == "time":
        if verbose > 0:
            return value
        parsed_time = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
        now = datetime.now(timezone.utc)
        return (
            humanize.naturaldelta(
                timedelta(seconds=(now - parsed_time).total_seconds())
            )
            + " ago"
        )
    else:
        return str(value)


def print_kafka_status(
    cluster: str,
    service: str,
    instance: str,
    output: List[str],
    kafka_status: Mapping[str, Any],
    verbose: int = 0,
) -> int:
    status = kafka_status.get("status")
    if status is None:
        output.append(PaastaColors.red("    Kafka cluster is not available yet"))
        return 1

    # print kafka view url before operator status because if the kafka cluster is not available for some reason
    # atleast the user can get a hold the kafka view url
    if status.get("kafka_view_url") is not None:
        output.append(f"    Kafka View Url: {status.get('kafka_view_url')}")

    output.append(f"    Zookeeper: {status['zookeeper']}")

    annotations = kafka_status.get("metadata").get("annotations")
    desired_state = annotations.get(paasta_prefixed("desired_state"))
    if desired_state is None:
        raise ValueError(
            f"expected desired state in kafka annotation, but received none"
        )
    output.append(f"    State: {desired_state}")

    cluster_ready = "true" if status.get("cluster_ready") else PaastaColors.red("false")
    cluster_restarting = (
        " (rolling-restart in progress)" if status["health"]["restarting"] else ""
    )
    output.append(f"    Ready: {cluster_ready}{cluster_restarting}")

    if status.get("cluster_ready"):
        health: Mapping[str, Any] = status["health"]
        cluster_health = (
            PaastaColors.green("healthy")
            if health["healthy"]
            else PaastaColors.red("unhealthy")
        )
        output.append(f"    Health: {cluster_health}")
        if not health.get("healthy"):
            output.append(f"     Reason: {health['message']}")
            output.append(f"     Offline Partitions: {health['offline_partitions']}")
            output.append(
                f"     Under Replicated Partitions: {health['under_replicated_partitions']}"
            )

    brokers = status["brokers"]
    output.append(f"    Brokers:")

    if verbose:
        headers = ["Id", "Phase", "IP", "Pod Name", "Started"]
    else:
        headers = ["Id", "Phase", "Started"]

    rows = [headers]
    for broker in brokers:
        color_fn = (
            PaastaColors.green if broker["phase"] == "Running" else PaastaColors.red
        )

        start_time = datetime.strptime(
            broker["deployed_timestamp"], "%Y-%m-%dT%H:%M:%SZ"
        )
        delta = datetime.utcnow() - start_time
        formatted_start_time = f"{str(start_time)} ({humanize.naturaltime(delta)})"

        if verbose:
            row = [
                str(broker["id"]),
                color_fn(broker["phase"]),
                str(broker["ip"]),
                str(broker["name"]),
                formatted_start_time,
            ]
        else:
            row = [
                str(broker["id"]),
                color_fn(broker["phase"]),
                formatted_start_time,
            ]

        rows.append(row)

    brokers_table = format_table(rows)
    output.extend([f"     {line}" for line in brokers_table])

    if verbose and len(brokers) > 0:
        append_pod_status(brokers, output)

    return 0


def report_status_for_cluster(
    service: str,
    cluster: str,
    deploy_pipeline: Sequence[str],
    actual_deployments: Mapping[str, str],
    instance_whitelist: Mapping[str, Type[InstanceConfig]],
    system_paasta_config: SystemPaastaConfig,
    verbose: int = 0,
    new: bool = False,
) -> Tuple[int, Sequence[str]]:
    """With a given service and cluster, prints the status of the instances
    in that cluster"""
    output = ["", "service: %s" % service, "cluster: %s" % cluster]
    deployed_instances = []
    instances = [
        instance
        for instance, instance_config_class in instance_whitelist.items()
        if instance_config_class in ALLOWED_INSTANCE_CONFIG
    ]

    # Tron instance are not present in the deploy pipeline, so treat them as
    # seen by default to avoid error messages
    seen_instances = [
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

    return_code = 0
    return_codes = [
        paasta_status_on_api_endpoint(
            cluster=cluster,
            service=service,
            instance=deployed_instance,
            output=output,
            system_paasta_config=system_paasta_config,
            verbose=verbose,
            new=new,
        )
        for deployed_instance in instances
    ]
    if any(return_codes):
        return_code = 1

    output.append(
        report_invalid_whitelist_values(instances, seen_instances, "instance")
    )

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


def get_filters(
    args,
) -> Sequence[Callable[[InstanceConfig], bool]]:
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
    if args.service_instance:
        if args.service or args.instances:
            print(
                PaastaColors.red(
                    f"Invalid command. Do not include optional arguments -s or -i "
                    f"when using shorthand notation."
                )
            )
            return clusters_services_instances
        if "." in args.service_instance:
            args.service, args.instances = args.service_instance.split(".", 1)
        else:
            print(PaastaColors.red(f'Use a "." to separate service and instance name'))
            return clusters_services_instances
    if args.service:
        try:
            validate_service_name(args.service, soa_dir=args.soa_dir)
        except NoSuchService:
            print(PaastaColors.red(f'The service "{args.service}" does not exist.'))
            all_services = list_services(soa_dir=args.soa_dir)
            suggestions = difflib.get_close_matches(
                args.service, all_services, n=5, cutoff=0.5
            )
            if suggestions:
                print(PaastaColors.red(f"Did you mean any of these?"))
                for suggestion in suggestions:
                    print(PaastaColors.red(f"  {suggestion}"))
            return clusters_services_instances

        all_services = [args.service]
    else:
        args.service = None
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
                new = _use_new_paasta_status(args, system_paasta_config)
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
                            new=new,
                        ),
                    )
                )
            else:
                print(missing_deployments_message(service))
                return_codes.append(1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        tasks = [executor.submit(t[0], **t[1]) for t in tasks]  # type: ignore
        for future in concurrent.futures.as_completed(tasks):  # type: ignore
            return_code, output = future.result()
            print("\n".join(output))
            return_codes.append(return_code)

    return max(return_codes)


def bouncing_status_human(app_count, bounce_method):
    if app_count == 0:
        return PaastaColors.red("Disabled")
    elif app_count == 1:
        return PaastaColors.green("Configured")
    elif app_count > 1:
        return PaastaColors.yellow("Bouncing (%s)" % bounce_method)
    else:
        return PaastaColors.red("Unknown (count: %s)" % app_count)


def desired_state_human(desired_state, instances):
    if desired_state == "start" and instances != 0:
        return PaastaColors.bold("Started")
    elif desired_state == "start" and instances == 0:
        return PaastaColors.bold("Stopped")
    elif desired_state == "stop":
        return PaastaColors.red("Stopped")
    else:
        return PaastaColors.red("Unknown (desired_state: %s)" % desired_state)


class BackendType(Enum):
    ENVOY = "Envoy"
    HAPROXY = "haproxy"


def envoy_backend_report(normal_instance_count: int, up_backends: int) -> str:
    return _backend_report(normal_instance_count, up_backends, BackendType.ENVOY)


def haproxy_backend_report(normal_instance_count: int, up_backends: int) -> str:
    return _backend_report(normal_instance_count, up_backends, BackendType.HAPROXY)


def _backend_report(
    normal_instance_count: int, up_backends: int, system_name: BackendType
) -> str:
    """Given that a service is in smartstack, this returns a human readable
    report of the up backends"""
    # TODO: Take into account a configurable threshold, PAASTA-1102
    crit_threshold = 50
    under_replicated, ratio = is_under_replicated(
        num_available=up_backends,
        expected_count=normal_instance_count,
        crit_threshold=crit_threshold,
    )
    if under_replicated:
        status = PaastaColors.red("Critical")
        count = PaastaColors.red(
            "(%d/%d, %d%%)" % (up_backends, normal_instance_count, ratio)
        )
    else:
        status = PaastaColors.green("Healthy")
        count = PaastaColors.green("(%d/%d)" % (up_backends, normal_instance_count))
    up_string = PaastaColors.bold("UP")
    return f"{status} - in {system_name} with {count} total backends {up_string} in this namespace."


def marathon_app_deploy_status_human(status, backoff_seconds=None):
    status_string = MarathonDeployStatus.tostring(status)

    if status == MarathonDeployStatus.Waiting:
        deploy_status = (
            "%s (new tasks waiting for capacity to become available)"
            % PaastaColors.red(status_string)
        )
    elif status == MarathonDeployStatus.Delayed:
        deploy_status = (
            "{} (tasks are crashing, next won't launch for another {} seconds)".format(
                PaastaColors.red(status_string), backoff_seconds
            )
        )
    elif status == MarathonDeployStatus.Deploying:
        deploy_status = PaastaColors.yellow(status_string)
    elif status == MarathonDeployStatus.Stopped:
        deploy_status = PaastaColors.grey(status_string)
    elif status == MarathonDeployStatus.Running:
        deploy_status = PaastaColors.bold(status_string)
    else:
        deploy_status = status_string

    return deploy_status


def status_marathon_job_human(
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
        return "Marathon:   {} - up with {} instances. Status: {}".format(
            status, instance_count, deploy_status
        )
    else:
        status = PaastaColors.yellow("Warning")
        return "Marathon:   {} - {} (app {}) is not configured in Marathon yet (waiting for bounce)".format(
            status, name, desired_app_id
        )


def _use_new_paasta_status(args, system_paasta_config) -> bool:
    if args.new:
        return True
    elif args.old:
        return False
    else:
        if system_paasta_config.get_paasta_status_version() == "old":
            return False
        elif system_paasta_config.get_paasta_status_version() == "new":
            return True
        else:
            return True


# Add other custom status writers here
# See `print_tron_status` for reference
INSTANCE_TYPE_WRITERS: Mapping[str, InstanceStatusWriter] = defaultdict(
    marathon=print_marathon_status,
    kubernetes=print_kubernetes_status,
    kubernetes_v2=print_kubernetes_status_v2,
    tron=print_tron_status,
    adhoc=print_adhoc_status,
    flink=print_flink_status,
    kafkacluster=print_kafka_status,
    cassandracluster=print_cassandra_status,
)
