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
import queue
import re
import threading
import time
import traceback
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple

import isodate
import pytz
from kubernetes.client import V1Pod
from kubernetes.client.rest import ApiException
from pyramid.request import Request
from pyramid.response import Response
from pyramid.view import view_config

import paasta_tools.mesos.exceptions as mesos_exceptions
from paasta_tools import tron_tools
from paasta_tools.api import settings
from paasta_tools.api.views.exception import ApiFailure
from paasta_tools.async_utils import run_sync
from paasta_tools.cli.cmds.status import get_actual_deployments
from paasta_tools.instance import kubernetes as pik
from paasta_tools.mesos_tools import get_all_frameworks as get_all_mesos_frameworks
from paasta_tools.utils import PAASTA_K8S_INSTANCE_TYPES
from paasta_tools.utils import DeploymentVersion
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import TimeoutError
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import validate_service_instance

log = logging.getLogger(__name__)

# Follow streams are closed cleanly before gunicorn's default 120-second
# max-request-seconds timeout kills the worker. This gives clients a clear,
# expected EOF at which to reconnect instead of an abrupt connection reset.
POD_LOGS_FOLLOW_STREAM_SECONDS = 115


def _stream_kube_log_response(
    kube_response: Any, follow_timeout: Optional[float]
) -> Iterator[bytes]:
    """Yield chunks from a Kubernetes pod log response and close it afterwards.

    If follow_timeout is set, return cleanly after that many seconds, even if
    the upstream Kubernetes stream is idle. A reader thread is needed because
    urllib3's stream() otherwise blocks indefinitely waiting for the next
    chunk, which would prevent us from enforcing a total stream deadline.
    """
    if follow_timeout is None:
        try:
            for chunk in kube_response.stream(4096):
                yield chunk
        finally:
            kube_response.release_conn()
            kube_response.close()
        return

    chunks: queue.Queue = queue.Queue()
    stream_errors: List[Exception] = []

    def read_upstream() -> None:
        try:
            for chunk in kube_response.stream(4096):
                chunks.put(chunk)
        except Exception as e:
            stream_errors.append(e)
        finally:
            # Tell the response generator that the upstream stream is done.
            chunks.put(None)

    reader = threading.Thread(target=read_upstream, daemon=True)
    reader.start()
    deadline = time.monotonic() + follow_timeout
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # Expected periodic reconnect point. Any chunks still queued
                # here will be refetched by the client from its last timestamp.
                return
            try:
                chunk = chunks.get(timeout=min(remaining, 1.0))
            except queue.Empty:
                continue
            if chunk is None:
                if stream_errors:
                    raise stream_errors[0]
                # Kubernetes ended the stream, e.g. because the container
                # finished. This is also a clean EOF, but it happens before
                # our deadline rather than at it.
                return
            yield chunk
    finally:
        # Closing the response also unblocks a reader waiting on the socket.
        kube_response.release_conn()
        kube_response.close()
        reader.join(timeout=1)


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


def legacy_remote_run_filter_frameworks(service, instance, frameworks=None):
    if frameworks is None:
        frameworks = get_all_mesos_frameworks(active_only=True)

    prefix = f"paasta-remote {service}.{instance}"
    return [f for f in frameworks if f.name.startswith(prefix)]


def adhoc_instance_status(
    instance_status: Mapping[str, Any], service: str, instance: str, verbose: int
) -> List[Dict[str, Any]]:
    status = []
    filtered = legacy_remote_run_filter_frameworks(service, instance)
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
def instance_status(
    request: Request,
) -> dict[str, Any]:  # godspeed to anyone typing the retval here
    # NOTE: swagger_data is populated by pyramid_swagger
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    verbose = request.swagger_data.get("verbose") or 0
    use_new = request.swagger_data.get("new") or False
    all_namespaces = request.swagger_data.get("all_namespaces") or False
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
                    all_namespaces=all_namespaces,
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
    task._Task__items["executor"] = run_sync(task.executor).copy()
    task._Task__items["executor"].pop("tasks", None)
    task._Task__items["executor"].pop("completed_tasks", None)
    task._Task__items["executor"].pop("queued_tasks", None)
    return task


def add_slave_info(task):
    task._Task__items["slave"] = run_sync(task.slave)._MesosSlave__items.copy()
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
def instance_mesh_status(
    request: Request,
) -> dict[str, Any]:  # godspeed to anyone typing the retval here
    # NOTE: swagger_data is populated by pyramid_swagger
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


@view_config(
    route_name="service.instance.replica.restart",
    request_method="POST",
    renderer="json",
)
def instance_replica_restart(
    request: Request,
) -> dict[str, Any]:
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    replica_name = request.swagger_data.get("replica_name")
    force = request.swagger_data.get("force", False)

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

    if pik.can_restart_replica(instance_type):
        try:
            replica_restarted = pik.restart_replica_by_name(
                service=service,
                instance=instance,
                instance_type=instance_type,
                replica_name=replica_name,
                settings=settings,
                force=force,
            )
            if replica_restarted:
                return {
                    "message": f"Initiated replica restart of {replica_name} successfully",
                    "service": service,
                    "instance": instance,
                    "replica_name": replica_name,
                }
            else:
                raise ApiFailure(
                    f"Replica {replica_name} not found in service {service}.{instance}",
                    404,
                )
        except RuntimeError as e:
            raise ApiFailure(str(e), 500)
    else:
        error_message = (
            f"instance_type {instance_type} of {service}.{instance} doesn't "
            "support replica restart"
        )
        raise ApiFailure(error_message, 500)


def _pod_paasta_identity(pod: V1Pod) -> Tuple[Optional[str], Optional[str]]:
    """Return the full PaaSTA service/instance for a pod, preferring
    annotations because Tron and Spark can truncate the corresponding labels."""
    labels = pod.metadata.labels or {}
    annotations = pod.metadata.annotations or {}
    service = annotations.get("paasta.yelp.com/service") or labels.get(
        "paasta.yelp.com/service"
    )
    instance = annotations.get("paasta.yelp.com/instance") or labels.get(
        "paasta.yelp.com/instance"
    )
    return service, instance


def _find_pods_by_name(
    pod_name: str,
    service: Optional[str] = None,
    instance: Optional[str] = None,
) -> List[Tuple[V1Pod, Optional[str], Optional[str]]]:
    """Find PaaSTA pods by exact name cluster-wide, optionally filtering by
    full service/instance identity. The apiserver applies the name selector;
    identity filtering also considers annotations so hashed labels work."""
    pods = settings.kubernetes_client.core.list_pod_for_all_namespaces(
        field_selector=f"metadata.name={pod_name}",
        # Restrict the lookup to PaaSTA-managed pods without requiring an
        # exact instance label (which Tron/Spark may truncate).
        label_selector="paasta.yelp.com/service",
    ).items
    matches = []
    for pod in pods:
        resolved_service, resolved_instance = _pod_paasta_identity(pod)
        labels = pod.metadata.labels or {}
        if service is not None and service not in (
            resolved_service,
            labels.get("paasta.yelp.com/service"),
        ):
            continue
        if instance is not None and instance not in (
            resolved_instance,
            labels.get("paasta.yelp.com/instance"),
        ):
            continue
        matches.append((pod, resolved_service, resolved_instance))
    return matches


@view_config(route_name="pods.lookup", request_method="GET", renderer="json")
def lookup_pod(request: Request) -> Dict[str, Any]:
    """Look up a PaaSTA pod by name across all namespaces in this cluster.

    Optional service/instance filters are matched against both labels and
    full-name annotations. All matches are returned so the caller can detect
    ambiguity across namespaces.
    """
    if settings.kubernetes_client is None:
        raise ApiFailure("kubernetes client is not available on this cluster", 500)
    matches = _find_pods_by_name(
        pod_name=request.swagger_data.get("pod_name"),
        service=request.swagger_data.get("service"),
        instance=request.swagger_data.get("instance"),
    )
    return {
        "pods": [
            {
                "name": pod.metadata.name,
                "namespace": pod.metadata.namespace,
                "service": service,
                "instance": instance,
            }
            for pod, service, instance in matches
        ]
    }


@view_config(
    route_name="service.instance.pod_logs",
    request_method="GET",
)
def pod_logs(request: Request) -> Response:
    """Stream logs for a single pod of service.instance, directly from the
    Kubernetes API. With follow=true the response is a chunked stream which
    ends cleanly after POD_LOGS_FOLLOW_STREAM_SECONDS; the CLI reconnects and
    resumes from its last timestamp. The stream also ends if the pod's log
    stream finishes before that deadline."""
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    pod_name = request.swagger_data.get("pod_name")
    follow = bool(request.swagger_data.get("follow") or False)
    previous = bool(request.swagger_data.get("previous") or False)
    tail_lines = request.swagger_data.get("tail_lines")
    container = request.swagger_data.get("container")
    since_time_raw = request.swagger_data.get("since_time")

    if settings.kubernetes_client is None:
        raise ApiFailure("kubernetes client is not available on this cluster", 500)

    # The name plus service/instance identity restricts which pod can be read.
    # Matching annotations as well as labels supports Tron/Spark's truncated
    # labels and pods whose service config has since been deleted.
    matches = _find_pods_by_name(pod_name, service=service, instance=instance)
    if not matches:
        raise ApiFailure(
            f"No pod named {pod_name} found for {service}.{instance}",
            404,
        )
    if len(matches) > 1:
        namespaces = sorted(pod.metadata.namespace for pod, _, _ in matches)
        raise ApiFailure(
            f"Multiple pods named {pod_name} found for {service}.{instance} "
            f"in namespaces: {', '.join(namespaces)}",
            409,
        )
    pod = matches[0][0]

    if since_time_raw:
        try:
            since_time = isodate.parse_datetime(since_time_raw)
        except (ValueError, TypeError):
            raise ApiFailure(
                f"Could not parse since_time {since_time_raw} as an ISO8601 timestamp",
                400,
            )
        if since_time.tzinfo is None:
            since_time = pytz.utc.localize(since_time)
    else:
        since_time = None

    if tail_lines == 0:
        # A zero-line request is a cheap existence check: the pod lookup above
        # has already established that this exact pod name belongs to
        # service.instance, so there is no need to open a Kubernetes log
        # stream (or require a container name on multi-container pods).
        return Response(content_type="text/plain")

    try:
        kube_response = settings.kubernetes_client.core.read_namespaced_pod_log(
            name=pod_name,
            namespace=pod.metadata.namespace,
            container=container,
            follow=follow,
            previous=previous,
            tail_lines=tail_lines,
            since_time=since_time,
            # kube prefixes every line with an RFC3339Nano timestamp, which the
            # CLI relies on to reconnect without skipping or duplicating lines
            timestamps=True,
            _preload_content=False,
        )
    except ApiException as e:
        raise ApiFailure(
            f"kubernetes API error while reading logs of pod {pod_name}: {e.reason}",
            e.status,
        )
    except Exception:
        error_message = traceback.format_exc()
        raise ApiFailure(error_message, 500)

    response = Response(content_type="text/plain")
    response.app_iter = _stream_kube_log_response(
        kube_response,
        follow_timeout=POD_LOGS_FOLLOW_STREAM_SECONDS if follow else None,
    )
    return response
