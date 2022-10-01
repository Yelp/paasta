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
"""
PaaSTA service instance status/start/stop etc.
"""
import asyncio
import datetime
import logging
import re
import traceback
from typing import Any
from typing import Dict
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple

import a_sync
import isodate
from marathon import MarathonClient
from marathon.models.app import MarathonApp
from marathon.models.app import MarathonTask
from pyramid.response import Response
from pyramid.view import view_config
from requests.exceptions import ReadTimeout

import paasta_tools.mesos.exceptions as mesos_exceptions
from paasta_tools import envoy_tools
from paasta_tools import marathon_tools
from paasta_tools import paasta_remote_run
from paasta_tools import smartstack_tools
from paasta_tools import tron_tools
from paasta_tools.api import settings
from paasta_tools.api.views.exception import ApiFailure
from paasta_tools.autoscaling.autoscaling_service_lib import get_autoscaling_info
from paasta_tools.cli.cmds.status import get_actual_deployments
from paasta_tools.instance import kubernetes as pik
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
from paasta_tools.marathon_tools import get_short_task_id
from paasta_tools.mesos.task import Task
from paasta_tools.mesos_tools import (
    get_cached_list_of_not_running_tasks_from_frameworks,
)
from paasta_tools.mesos_tools import get_cached_list_of_running_tasks_from_frameworks
from paasta_tools.mesos_tools import get_cpu_shares
from paasta_tools.mesos_tools import get_first_status_timestamp
from paasta_tools.mesos_tools import get_mesos_slaves_grouped_by_attribute
from paasta_tools.mesos_tools import get_short_hostname_from_task
from paasta_tools.mesos_tools import get_slaves
from paasta_tools.mesos_tools import get_tail_lines_for_mesos_task
from paasta_tools.mesos_tools import get_task
from paasta_tools.mesos_tools import get_tasks_from_app_id
from paasta_tools.mesos_tools import results_or_unknown
from paasta_tools.mesos_tools import select_tasks_by_id
from paasta_tools.mesos_tools import TaskNotFound
from paasta_tools.utils import calculate_tail_lines
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DeploymentVersion
from paasta_tools.utils import get_git_sha_from_dockerurl
from paasta_tools.utils import get_image_version_from_dockerurl
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import TimeoutError
from paasta_tools.utils import validate_service_instance

log = logging.getLogger(__name__)


def tron_instance_status(
    instance_status: Mapping[str, Any], service: str, instance: str, verbose: int
) -> Mapping[str, Any]:
    status: Dict[str, Any] = {}
    client = tron_tools.get_tron_client()
    short_job, action = instance.split(".")
    job = f"{service}.{short_job}"
    job_content = client.get_job_content(job=job)

    try:
        latest_run_id = client.get_latest_job_run_id(job_content=job_content)
        if latest_run_id is None:
            action_run = {"state": "Hasn't run yet (no job run id found)"}
        else:
            action_run = client.get_action_run(
                job=job, action=action, run_id=latest_run_id
            )
    except Exception as e:
        action_run = {"state": f"Failed to get latest run info: {e}"}

    # job info
    status["job_name"] = short_job
    status["job_status"] = job_content["status"]
    status["job_schedule"] = "{} {}".format(
        job_content["scheduler"]["type"], job_content["scheduler"]["value"]
    )
    status["job_url"] = (
        tron_tools.get_tron_dashboard_for_cluster(settings.cluster) + f"#job/{job}"
    )

    if action:
        status["action_name"] = action
    if action_run.get("state"):
        status["action_state"] = action_run["state"]
    if action_run.get("start_time"):
        status["action_start_time"] = action_run["start_time"]
    if action_run.get("raw_command"):
        status["action_raw_command"] = action_run["raw_command"]
    if action_run.get("stdout"):
        status["action_stdout"] = "\n".join(action_run["stdout"])
    if action_run.get("stderr"):
        status["action_stderr"] = "\n".join(action_run["stderr"])
    if action_run.get("command"):
        status["action_command"] = action_run["command"]

    return status


def adhoc_instance_status(
    instance_status: Mapping[str, Any], service: str, instance: str, verbose: int
) -> List[Dict[str, Any]]:
    status = []
    filtered = paasta_remote_run.remote_run_filter_frameworks(service, instance)
    filtered.sort(key=lambda x: x.name)
    for f in filtered:
        launch_time, run_id = re.match(
            r"paasta-remote [^\s]+ (\w+) (\w+)", f.name
        ).groups()
        status.append(
            {"launch_time": launch_time, "run_id": run_id, "framework_id": f.id}
        )
    return status


def marathon_instance_status(
    instance_status: Mapping[str, Any],
    service: str,
    instance: str,
    verbose: int,
    include_smartstack: bool,
    include_envoy: bool,
    include_mesos: bool,
) -> Mapping[str, Any]:
    mstatus: Dict[str, Any] = {}

    job_config = marathon_tools.load_marathon_service_config(
        service, instance, settings.cluster, soa_dir=settings.soa_dir
    )
    marathon_apps_with_clients = marathon_tools.get_marathon_apps_with_clients(
        clients=settings.marathon_clients.get_all_clients_for_service(job_config),
        embed_tasks=True,
        service_name=service,
    )
    matching_apps_with_clients = marathon_tools.get_matching_apps_with_clients(
        service, instance, marathon_apps_with_clients
    )

    mstatus.update(
        marathon_job_status(
            service, instance, job_config, matching_apps_with_clients, verbose
        )
    )

    if include_smartstack or include_envoy:
        service_namespace_config = marathon_tools.load_service_namespace_config(
            service=service,
            namespace=job_config.get_nerve_namespace(),
            soa_dir=settings.soa_dir,
        )
        if "proxy_port" in service_namespace_config:
            tasks = [
                task for app, _ in matching_apps_with_clients for task in app.tasks
            ]
            if include_smartstack:
                mstatus["smartstack"] = marathon_service_mesh_status(
                    service,
                    pik.ServiceMesh.SMARTSTACK,
                    instance,
                    job_config,
                    service_namespace_config,
                    tasks,
                    should_return_individual_backends=verbose > 0,
                )
            if include_envoy:
                mstatus["envoy"] = marathon_service_mesh_status(
                    service,
                    pik.ServiceMesh.ENVOY,
                    instance,
                    job_config,
                    service_namespace_config,
                    tasks,
                    should_return_individual_backends=verbose > 0,
                )

    if include_mesos:
        mstatus["mesos"] = marathon_mesos_status(service, instance, verbose)

    return mstatus


def get_active_versions_for_marathon_apps(
    marathon_apps_with_clients: List[Tuple[MarathonApp, MarathonClient]],
) -> Set[Tuple[DeploymentVersion, str]]:
    ret = set()
    for (app, client) in marathon_apps_with_clients:
        git_sha = get_git_sha_from_dockerurl(app.container.docker.image, long=True)
        image_version = get_image_version_from_dockerurl(app.container.docker.image)
        _, _, _, config_sha = marathon_tools.deformat_job_id(app.id)
        if config_sha.startswith("config"):
            config_sha = config_sha[len("config") :]
        ret.add(
            (DeploymentVersion(sha=git_sha, image_version=image_version), config_sha)
        )
    return ret


def marathon_job_status(
    service: str,
    instance: str,
    job_config: marathon_tools.MarathonServiceConfig,
    marathon_apps_with_clients: List[Tuple[MarathonApp, MarathonClient]],
    verbose: int,
) -> MutableMapping[str, Any]:
    active_versions = get_active_versions_for_marathon_apps(marathon_apps_with_clients)
    job_status_fields: MutableMapping[str, Any] = {
        "app_statuses": [],
        "app_count": len(marathon_apps_with_clients),
        "desired_state": job_config.get_desired_state(),
        "bounce_method": job_config.get_bounce_method(),
        "expected_instance_count": job_config.get_instances(),
        "active_shas": [
            (deployment_version.sha, config_sha)
            for deployment_version, config_sha in active_versions
        ],
        "active_versions": [
            (deployment_version.sha, deployment_version.image_version, config_sha)
            for deployment_version, config_sha in active_versions
        ],
    }

    try:
        desired_app_id = job_config.format_marathon_app_dict()["id"]
    except NoDockerImageError:
        error_msg = "Docker image is not in deployments.json."
        job_status_fields["error_message"] = error_msg
        return job_status_fields

    job_status_fields["desired_app_id"] = desired_app_id

    deploy_status_for_desired_app = None
    dashboard_links = get_marathon_dashboard_links(
        settings.marathon_clients, settings.system_paasta_config
    )
    tasks_running = 0
    for app, marathon_client in marathon_apps_with_clients:
        deploy_status = marathon_tools.get_marathon_app_deploy_status(
            marathon_client, app
        )

        app_status = marathon_app_status(
            app,
            marathon_client,
            dashboard_links.get(marathon_client) if dashboard_links else None,
            deploy_status,
            list_tasks=verbose > 0,
        )
        job_status_fields["app_statuses"].append(app_status)

        if app.id.lstrip("/") == desired_app_id.lstrip("/"):
            deploy_status_for_desired_app = (
                marathon_tools.MarathonDeployStatus.tostring(deploy_status)
            )
        tasks_running += app.tasks_running

    job_status_fields["deploy_status"] = (
        deploy_status_for_desired_app or "Waiting for bounce"
    )
    job_status_fields["running_instance_count"] = tasks_running

    if verbose > 0:
        autoscaling_info = get_autoscaling_info(marathon_apps_with_clients, job_config)
        if autoscaling_info is not None:
            autoscaling_info_dict = autoscaling_info._asdict()

            for field in ("current_utilization", "target_instances"):
                if autoscaling_info_dict[field] is None:
                    del autoscaling_info_dict[field]

            job_status_fields["autoscaling_info"] = autoscaling_info_dict

    return job_status_fields


def marathon_app_status(
    app: MarathonApp,
    marathon_client: MarathonClient,
    dashboard_link: Optional[str],
    deploy_status: int,
    list_tasks: bool = False,
) -> MutableMapping[str, Any]:
    app_status = {
        "tasks_running": app.tasks_running,
        "tasks_healthy": app.tasks_healthy,
        "tasks_staged": app.tasks_staged,
        "tasks_total": app.instances,
        "create_timestamp": isodate.parse_datetime(app.version).timestamp(),
        "deploy_status": marathon_tools.MarathonDeployStatus.tostring(deploy_status),
    }

    app_queue = marathon_tools.get_app_queue(marathon_client, app.id)
    if deploy_status == marathon_tools.MarathonDeployStatus.Delayed:
        _, backoff_seconds = marathon_tools.get_app_queue_status_from_queue(app_queue)
        app_status["backoff_seconds"] = backoff_seconds

    unused_offers_summary = marathon_tools.summarize_unused_offers(app_queue)
    if unused_offers_summary is not None:
        app_status["unused_offers"] = unused_offers_summary

    if dashboard_link:
        app_status["dashboard_url"] = "{}/ui/#/apps/%2F{}".format(
            dashboard_link.rstrip("/"), app.id.lstrip("/")
        )

    if list_tasks is True:
        app_status["tasks"] = []
        for task in app.tasks:
            app_status["tasks"].append(build_marathon_task_dict(task))

    return app_status


def build_marathon_task_dict(marathon_task: MarathonTask) -> MutableMapping[str, Any]:
    task_dict = {
        "id": get_short_task_id(marathon_task.id),
        "host": marathon_task.host.split(".")[0],
        "port": marathon_task.ports[0],
        "deployed_timestamp": marathon_task.staged_at.timestamp(),
    }

    if marathon_task.health_check_results:
        task_dict["is_healthy"] = marathon_tools.is_task_healthy(marathon_task)

    return task_dict


def _build_smartstack_location_dict_for_backends(
    synapse_host: str,
    registration: str,
    tasks: Sequence[MarathonTask],
    location: str,
    should_return_individual_backends: bool,
) -> MutableMapping[str, Any]:
    sorted_smartstack_backends = sorted(
        smartstack_tools.get_backends(
            registration,
            synapse_host=synapse_host,
            synapse_port=settings.system_paasta_config.get_synapse_port(),
            synapse_haproxy_url_format=settings.system_paasta_config.get_synapse_haproxy_url_format(),
        ),
        key=lambda backend: backend["status"],
        reverse=True,  # put 'UP' backends above 'MAINT' backends
    )
    matched_smartstack_backends_and_tasks = smartstack_tools.match_backends_and_tasks(
        sorted_smartstack_backends, tasks
    )
    return smartstack_tools.build_smartstack_location_dict(
        location,
        matched_smartstack_backends_and_tasks,
        should_return_individual_backends,
    )


def _build_envoy_location_dict_for_backends(
    envoy_host: str,
    registration: str,
    tasks: Sequence[MarathonTask],
    location: str,
    should_return_individual_backends: bool,
) -> MutableMapping[str, Any]:
    backends = envoy_tools.get_backends(
        registration,
        envoy_host=envoy_host,
        envoy_admin_port=settings.system_paasta_config.get_envoy_admin_port(),
        envoy_admin_endpoint_format=settings.system_paasta_config.get_envoy_admin_endpoint_format(),
    )
    sorted_envoy_backends = sorted(
        [
            backend[0]
            for _, service_backends in backends.items()
            for backend in service_backends
        ],
        key=lambda backend: backend["eds_health_status"],
    )
    casper_proxied_backends = {
        (backend["address"], backend["port_value"])
        for _, service_backends in backends.items()
        for backend, is_casper_proxied_backend in service_backends
        if is_casper_proxied_backend
    }

    matched_envoy_backends_and_tasks = envoy_tools.match_backends_and_tasks(
        sorted_envoy_backends,
        tasks,
    )

    return envoy_tools.build_envoy_location_dict(
        location,
        matched_envoy_backends_and_tasks,
        should_return_individual_backends,
        casper_proxied_backends,
    )


def marathon_service_mesh_status(
    service: str,
    service_mesh: pik.ServiceMesh,
    instance: str,
    job_config: marathon_tools.MarathonServiceConfig,
    service_namespace_config: ServiceNamespaceConfig,
    tasks: Sequence[MarathonTask],
    should_return_individual_backends: bool = False,
) -> Mapping[str, Any]:
    registration = job_config.get_registrations()[0]
    discover_location_type = service_namespace_config.get_discover()

    grouped_slaves = get_mesos_slaves_grouped_by_attribute(
        slaves=get_slaves(), attribute=discover_location_type
    )

    # rebuild the dict, replacing the slave object with just their hostname
    slave_hostname_by_location = {
        attribute_value: [slave["hostname"] for slave in slaves]
        for attribute_value, slaves in grouped_slaves.items()
    }

    expected_instance_count = marathon_tools.get_expected_instance_count_for_namespace(
        service, instance, settings.cluster
    )
    expected_count_per_location = int(
        expected_instance_count / len(slave_hostname_by_location)
    )
    service_mesh_status: MutableMapping[str, Any] = {
        "registration": registration,
        "expected_backends_per_location": expected_count_per_location,
        "locations": [],
    }

    for location, hosts in slave_hostname_by_location.items():
        if service_mesh == pik.ServiceMesh.SMARTSTACK:
            service_mesh_status["locations"].append(
                _build_smartstack_location_dict_for_backends(
                    synapse_host=hosts[0],
                    registration=registration,
                    tasks=tasks,
                    location=location,
                    should_return_individual_backends=should_return_individual_backends,
                )
            )
        elif service_mesh == pik.ServiceMesh.ENVOY:
            service_mesh_status["locations"].append(
                _build_envoy_location_dict_for_backends(
                    envoy_host=hosts[0],
                    registration=registration,
                    tasks=tasks,
                    location=location,
                    should_return_individual_backends=should_return_individual_backends,
                )
            )

    return service_mesh_status


@a_sync.to_blocking
async def marathon_mesos_status(
    service: str, instance: str, verbose: int
) -> MutableMapping[str, Any]:
    mesos_status: MutableMapping[str, Any] = {}

    job_id = marathon_tools.format_job_id(service, instance)
    job_id_filter_string = f"{job_id}{marathon_tools.MESOS_TASK_SPACER}"

    try:
        running_and_active_tasks = select_tasks_by_id(
            await get_cached_list_of_running_tasks_from_frameworks(),
            job_id=job_id_filter_string,
        )
    except (ReadTimeout, asyncio.TimeoutError):
        return {"error_message": "Talking to Mesos timed out. It may be overloaded."}

    mesos_status["running_task_count"] = len(running_and_active_tasks)

    if verbose > 0:
        num_tail_lines = calculate_tail_lines(verbose)
        running_task_dict_futures = []

        for task in running_and_active_tasks:
            running_task_dict_futures.append(
                asyncio.ensure_future(get_mesos_running_task_dict(task, num_tail_lines))
            )

        non_running_tasks = select_tasks_by_id(
            await get_cached_list_of_not_running_tasks_from_frameworks(),
            job_id=job_id_filter_string,
        )
        non_running_tasks.sort(key=lambda task: get_first_status_timestamp(task) or 0)
        non_running_tasks = list(reversed(non_running_tasks[-10:]))
        non_running_task_dict_futures = []
        for task in non_running_tasks:
            non_running_task_dict_futures.append(
                asyncio.ensure_future(
                    get_mesos_non_running_task_dict(task, num_tail_lines)
                )
            )

        all_task_dict_futures = (
            running_task_dict_futures + non_running_task_dict_futures
        )
        if len(all_task_dict_futures):
            await asyncio.wait(all_task_dict_futures)

        mesos_status["running_tasks"] = [
            task_future.result() for task_future in running_task_dict_futures
        ]
        mesos_status["non_running_tasks"] = [
            task_future.result() for task_future in non_running_task_dict_futures
        ]

    return mesos_status


async def get_mesos_running_task_dict(
    task: Task, num_tail_lines: int
) -> MutableMapping[str, Any]:
    short_hostname_future = asyncio.ensure_future(
        results_or_unknown(get_short_hostname_from_task(task))
    )
    mem_limit_future = asyncio.ensure_future(_task_result_or_error(task.mem_limit()))
    rss_future = asyncio.ensure_future(_task_result_or_error(task.rss()))
    cpu_shares_future = asyncio.ensure_future(
        _task_result_or_error(get_cpu_shares(task))
    )
    cpu_time_future = asyncio.ensure_future(_task_result_or_error(task.cpu_time()))

    futures = [
        short_hostname_future,
        mem_limit_future,
        rss_future,
        cpu_shares_future,
        cpu_time_future,
    ]
    if num_tail_lines > 0:
        tail_lines_future = asyncio.ensure_future(
            get_tail_lines_for_mesos_task(task, get_short_task_id, num_tail_lines)
        )
        futures.append(tail_lines_future)
    else:
        tail_lines_future = None

    await asyncio.wait(futures)

    task_dict = {
        "id": get_short_task_id(task["id"]),
        "hostname": short_hostname_future.result(),
        "mem_limit": mem_limit_future.result(),
        "rss": rss_future.result(),
        "cpu_shares": cpu_shares_future.result(),
        "cpu_used_seconds": cpu_time_future.result(),
        "tail_lines": tail_lines_future.result() if tail_lines_future else {},
    }

    task_start_time = get_first_status_timestamp(task)
    if task_start_time is not None:
        task_dict["deployed_timestamp"] = task_start_time
        current_time = int(datetime.datetime.now().strftime("%s"))
        task_dict["duration_seconds"] = current_time - round(task_start_time)

    return task_dict


async def _task_result_or_error(future):
    try:
        return {"value": await future}
    except (AttributeError, mesos_exceptions.SlaveDoesNotExist):
        return {"error_message": "None"}
    except TimeoutError:
        return {"error_message": "Timed Out"}
    except Exception:
        return {"error_message": "Unknown"}


async def get_mesos_non_running_task_dict(
    task: Task, num_tail_lines: int
) -> MutableMapping[str, Any]:
    if num_tail_lines > 0:
        tail_lines = await get_tail_lines_for_mesos_task(
            task, get_short_task_id, num_tail_lines
        )
    else:
        tail_lines = {}

    task_dict = {
        "id": get_short_task_id(task["id"]),
        "hostname": await results_or_unknown(get_short_hostname_from_task(task)),
        "state": task["state"],
        "tail_lines": tail_lines,
    }

    task_start_time = get_first_status_timestamp(task)
    if task_start_time is not None:
        task_dict["deployed_timestamp"] = task_start_time

    return task_dict


def no_configuration_for_service_message(cluster, service, instance):
    return (
        f"No instance named '{compose_job_id(service, instance)}' has been "
        f"configured to run in the {settings.cluster} cluster"
    )


@view_config(
    route_name="service.instance.status", request_method="GET", renderer="json"
)
def instance_status(request):
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    verbose = request.swagger_data.get("verbose") or 0
    use_new = request.swagger_data.get("new") or False
    include_smartstack = request.swagger_data.get("include_smartstack")
    if include_smartstack is None:
        include_smartstack = True
    include_envoy = request.swagger_data.get("include_envoy")
    if include_envoy is None:
        include_envoy = True
    include_mesos = request.swagger_data.get("include_mesos")
    if include_mesos is None:
        include_mesos = True

    instance_status: Dict[str, Any] = {}
    instance_status["service"] = service
    instance_status["instance"] = instance
    try:
        instance_type = validate_service_instance(
            service, instance, settings.cluster, settings.soa_dir
        )
    except NoConfigurationForServiceError:
        error_message = no_configuration_for_service_message(
            settings.cluster,
            service,
            instance,
        )
        raise ApiFailure(error_message, 404)
    except Exception:
        error_message = traceback.format_exc()
        raise ApiFailure(error_message, 500)

    if instance_type != "tron":
        try:
            actual_deployments = get_actual_deployments(service, settings.soa_dir)
        except Exception:
            error_message = traceback.format_exc()
            raise ApiFailure(error_message, 500)

        version = get_deployment_version(actual_deployments, settings.cluster, instance)
        # exit if the deployment key is not found
        if not version:
            error_message = (
                "Deployment key %s not found.  Try to execute the corresponding pipeline if it's a fresh instance"
                % ".".join([settings.cluster, instance])
            )
            raise ApiFailure(error_message, 404)

        instance_status["version"] = version.short_sha_repr()
        # Kept for backwards compatibility
        # TODO: Remove once all clients+clusters updated to use deploymentversion
        instance_status["git_sha"] = version.sha[:8]
    else:
        instance_status["version"] = ""
        instance_status["git_sha"] = ""
    try:
        if instance_type == "marathon":
            instance_status["marathon"] = marathon_instance_status(
                instance_status,
                service,
                instance,
                verbose,
                include_smartstack=include_smartstack,
                include_envoy=include_envoy,
                include_mesos=include_mesos,
            )
        elif instance_type == "adhoc":
            instance_status["adhoc"] = adhoc_instance_status(
                instance_status, service, instance, verbose
            )
        elif pik.can_handle(instance_type):
            instance_status.update(
                pik.instance_status(
                    service=service,
                    instance=instance,
                    verbose=verbose,
                    include_smartstack=include_smartstack,
                    include_envoy=include_envoy,
                    use_new=use_new,
                    instance_type=instance_type,
                    settings=settings,
                )
            )
        elif instance_type == "tron":
            instance_status["tron"] = tron_instance_status(
                instance_status, service, instance, verbose
            )
        else:
            error_message = (
                f"Unknown instance_type {instance_type} of {service}.{instance}"
            )
            raise ApiFailure(error_message, 404)
    except Exception:
        error_message = traceback.format_exc()
        raise ApiFailure(error_message, 500)

    return instance_status


@view_config(
    route_name="service.instance.set_state", request_method="POST", renderer="json"
)
def instance_set_state(
    request,
) -> None:
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    desired_state = request.swagger_data.get("desired_state")

    try:
        instance_type = validate_service_instance(
            service, instance, settings.cluster, settings.soa_dir
        )
    except NoConfigurationForServiceError:
        error_message = no_configuration_for_service_message(
            settings.cluster,
            service,
            instance,
        )
        raise ApiFailure(error_message, 404)
    except Exception:
        error_message = traceback.format_exc()
        raise ApiFailure(error_message, 500)

    if pik.can_set_state(instance_type):
        try:
            pik.set_cr_desired_state(
                kube_client=settings.kubernetes_client,
                service=service,
                instance=instance,
                instance_type=instance_type,
                desired_state=desired_state,
            )
        except RuntimeError as e:
            raise ApiFailure(e, 500)
    else:
        error_message = (
            f"instance_type {instance_type} of {service}.{instance} doesn't "
            "support set_state"
        )
        raise ApiFailure(error_message, 500)


@view_config(
    route_name="service.instance.tasks.task", request_method="GET", renderer="json"
)
def instance_task(request):
    status = instance_status(request)
    task_id = request.swagger_data.get("task_id", None)
    verbose = request.swagger_data.get("verbose", False)
    try:
        mstatus = status["marathon"]
    except KeyError:
        raise ApiFailure("Only marathon tasks supported", 400)
    try:
        task = a_sync.block(get_task, task_id, app_id=mstatus["app_id"])
    except TaskNotFound:
        raise ApiFailure(f"Task with id {task_id} not found", 404)
    except Exception:
        error_message = traceback.format_exc()
        raise ApiFailure(error_message, 500)
    if verbose:
        task = add_slave_info(task)
        task = add_executor_info(task)
    return task._Task__items


@view_config(route_name="service.instance.tasks", request_method="GET", renderer="json")
def instance_tasks(request):
    status = instance_status(request)
    slave_hostname = request.swagger_data.get("slave_hostname", None)
    verbose = request.swagger_data.get("verbose", False)
    try:
        mstatus = status["marathon"]
    except KeyError:
        raise ApiFailure("Only marathon tasks supported", 400)
    tasks = a_sync.block(
        get_tasks_from_app_id, mstatus["desired_app_id"], slave_hostname=slave_hostname
    )
    if verbose:
        tasks = [add_executor_info(task) for task in tasks]
        tasks = [add_slave_info(task) for task in tasks]
    return [task._Task__items for task in tasks]


@view_config(route_name="service.instance.delay", request_method="GET", renderer="json")
def instance_delay(request):
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    job_config = marathon_tools.load_marathon_service_config(
        service, instance, settings.cluster, soa_dir=settings.soa_dir
    )
    client = settings.marathon_clients.get_current_client_for_service(job_config)
    app_id = job_config.format_marathon_app_dict()["id"]
    app_queue = marathon_tools.get_app_queue(client, app_id)
    unused_offers_summary = marathon_tools.summarize_unused_offers(app_queue)

    if len(unused_offers_summary) != 0:
        return unused_offers_summary
    else:
        response = Response()
        response.status_int = 204
        return response


@view_config(
    route_name="service.instance.bounce_status",
    request_method="GET",
    renderer="json",
)
def bounce_status(request):
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    try:
        instance_type = validate_service_instance(
            service, instance, settings.cluster, settings.soa_dir
        )
    except NoConfigurationForServiceError:
        error_message = no_configuration_for_service_message(
            settings.cluster,
            service,
            instance,
        )
        raise ApiFailure(error_message, 404)
    except Exception:
        error_message = traceback.format_exc()
        raise ApiFailure(error_message, 500)

    if instance_type != "kubernetes":
        # We are using HTTP 204 to indicate that the instance exists but has
        # no bounce status to be returned.  The client should just mark the
        # instance as bounced.
        response = Response()
        response.status_int = 204
        return response

    try:
        return pik.bounce_status(service, instance, settings)
    except asyncio.TimeoutError:
        raise ApiFailure(
            "Temporary issue fetching bounce status. Please try again.", 599
        )
    except Exception:
        error_message = traceback.format_exc()
        raise ApiFailure(error_message, 500)


def add_executor_info(task):
    task._Task__items["executor"] = a_sync.block(task.executor).copy()
    task._Task__items["executor"].pop("tasks", None)
    task._Task__items["executor"].pop("completed_tasks", None)
    task._Task__items["executor"].pop("queued_tasks", None)
    return task


def add_slave_info(task):
    task._Task__items["slave"] = a_sync.block(task.slave)._MesosSlave__items.copy()
    return task


def get_marathon_dashboard_links(marathon_clients, system_paasta_config):
    """Return a dict of marathon clients and their corresponding dashboard URLs"""
    cluster = system_paasta_config.get_cluster()
    try:
        links = (
            system_paasta_config.get_dashboard_links().get(cluster).get("Marathon RO")
        )
    except KeyError:
        pass
    if isinstance(links, list) and len(links) >= len(marathon_clients.current):
        return {client: url for client, url in zip(marathon_clients.current, links)}
    return None


def get_deployment_version(
    actual_deployments: Mapping[str, DeploymentVersion], cluster: str, instance: str
) -> Optional[DeploymentVersion]:
    key = ".".join((cluster, instance))
    return actual_deployments[key] if key in actual_deployments else None


@view_config(
    route_name="service.instance.mesh_status",
    request_method="GET",
    renderer="json",
)
def instance_mesh_status(request):
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    include_smartstack = request.swagger_data.get("include_smartstack")
    include_envoy = request.swagger_data.get("include_envoy")

    instance_mesh: Dict[str, Any] = {}
    instance_mesh["service"] = service
    instance_mesh["instance"] = instance

    try:
        instance_type = validate_service_instance(
            service, instance, settings.cluster, settings.soa_dir
        )
    except NoConfigurationForServiceError:
        error_message = (
            f"No instance named '{compose_job_id(service, instance)}' has been "
            f"configured to run in the {settings.cluster} cluster"
        )
        raise ApiFailure(error_message, 404)
    except Exception:
        error_message = traceback.format_exc()
        raise ApiFailure(error_message, 500)

    try:
        instance_mesh.update(
            pik.kubernetes_mesh_status(
                service=service,
                instance=instance,
                instance_type=instance_type,
                settings=settings,
                include_smartstack=include_smartstack,
                include_envoy=include_envoy,
            )
        )
    except RuntimeError as e:
        raise ApiFailure(str(e), 405)
    except Exception:
        error_message = traceback.format_exc()
        raise ApiFailure(error_message, 500)

    return instance_mesh
