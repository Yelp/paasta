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
import logging
import re
import traceback
from typing import Any
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional

import a_sync
from pyramid.response import Response
from pyramid.view import view_config

import paasta_tools.mesos.exceptions as mesos_exceptions
from paasta_tools import paasta_remote_run
from paasta_tools import tron_tools
from paasta_tools.api import settings
from paasta_tools.api.views.exception import ApiFailure
from paasta_tools.cli.cmds.status import get_actual_deployments
from paasta_tools.instance import kubernetes as pik
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DeploymentVersion
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import PAASTA_K8S_INSTANCE_TYPES
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


async def _task_result_or_error(future):
    try:
        return {"value": await future}
    except (AttributeError, mesos_exceptions.SlaveDoesNotExist):
        return {"error_message": "None"}
    except TimeoutError:
        return {"error_message": "Timed Out"}
    except Exception:
        return {"error_message": "Unknown"}


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
        if instance_type == "adhoc":
            instance_status["adhoc"] = adhoc_instance_status(
                instance_status, service, instance, verbose
            )
        elif pik.can_handle(instance_type):
            instance_status.update(
                pik.instance_status(
                    service=service,
                    instance=instance,
                    verbose=verbose,
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

    if instance_type not in PAASTA_K8S_INSTANCE_TYPES:
        # We are using HTTP 204 to indicate that the instance exists but has
        # no bounce status to be returned.  The client should just mark the
        # instance as bounced.
        response = Response()
        response.status_int = 204
        return response

    try:
        return pik.bounce_status(
            service, instance, settings, is_eks=(instance_type == "eks")
        )
    except NoConfigurationForServiceError:
        # Handle race condition where instance has been removed since the above validation
        error_message = no_configuration_for_service_message(
            settings.cluster,
            service,
            instance,
        )
        raise ApiFailure(error_message, 404)
    except asyncio.TimeoutError:
        raise ApiFailure(
            "Temporary issue fetching bounce status. Please try again.", 599
        )
    except Exception as e:
        error_message = traceback.format_exc()
        if getattr(e, "status", None) == 404:
            # some bounces delete the app & recreate
            # in this case, we relay the 404 and cli handles gracefully
            raise ApiFailure(error_message, 404)
        # for all others, treat as a 500
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
                include_envoy=include_envoy,
            )
        )
    except RuntimeError as e:
        raise ApiFailure(str(e), 405)
    except Exception:
        error_message = traceback.format_exc()
        raise ApiFailure(error_message, 500)

    return instance_mesh
