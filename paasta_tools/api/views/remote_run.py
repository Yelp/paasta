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
import traceback

from pyramid.view import view_config

from paasta_tools.api import settings
from paasta_tools.api.views.exception import ApiFailure
from paasta_tools.kubernetes.remote_run import get_max_job_duration_limit
from paasta_tools.kubernetes.remote_run import remote_run_ready
from paasta_tools.kubernetes.remote_run import remote_run_start
from paasta_tools.kubernetes.remote_run import remote_run_stop
from paasta_tools.kubernetes.remote_run import remote_run_token


DEFAULT_MAX_DURATION = 60 * 60  # 1 hour


@view_config(route_name="remote_run.start", request_method="POST", renderer="json")
def view_remote_run_start(request):
    service = request.swagger_data["service"]
    instance = request.swagger_data["instance"]
    user = request.swagger_data["json_body"]["user"]
    interactive = request.swagger_data["json_body"].get("interactive", True)
    recreate = request.swagger_data["json_body"].get("recreate", False)
    is_toolbox = request.swagger_data["json_body"].get("toolbox", False)
    command = request.swagger_data["json_body"].get("command", None)
    max_duration = min(
        request.swagger_data["json_body"].get("max_duration", DEFAULT_MAX_DURATION),
        get_max_job_duration_limit(),
    )
    try:
        return remote_run_start(
            service=service,
            instance=instance,
            cluster=settings.cluster,
            user=user,
            interactive=interactive,
            recreate=recreate,
            max_duration=max_duration,
            is_toolbox=is_toolbox,
            command=command,
        )
    except Exception:
        error_message = traceback.format_exc()
        raise ApiFailure(error_message, 500)


@view_config(route_name="remote_run.poll", request_method="GET", renderer="json")
def view_remote_run_poll(request):
    service = request.swagger_data["service"]
    instance = request.swagger_data["instance"]
    job_name = request.swagger_data["job_name"]
    user = request.swagger_data["user"]
    is_toolbox = request.swagger_data.get("toolbox", False)
    try:
        return remote_run_ready(
            service=service,
            instance=instance,
            cluster=settings.cluster,
            job_name=job_name,
            user=user,
            is_toolbox=is_toolbox,
        )
    except Exception:
        error_message = traceback.format_exc()
        raise ApiFailure(error_message, 500)


@view_config(route_name="remote_run.stop", request_method="POST", renderer="json")
def view_remote_run_stop(request):
    service = request.swagger_data["service"]
    instance = request.swagger_data["instance"]
    user = request.swagger_data["json_body"]["user"]
    is_toolbox = request.swagger_data["json_body"].get("toolbox", False)
    try:
        return remote_run_stop(
            service=service,
            instance=instance,
            cluster=settings.cluster,
            user=user,
            is_toolbox=is_toolbox,
        )
    except Exception:
        error_message = traceback.format_exc()
        raise ApiFailure(error_message, 500)


@view_config(route_name="remote_run.token", request_method="GET", renderer="json")
def view_remote_run_token(request):
    service = request.swagger_data["service"]
    instance = request.swagger_data["instance"]
    user = request.swagger_data["user"]
    try:
        token = remote_run_token(
            service=service,
            instance=instance,
            cluster=settings.cluster,
            user=user,
        )
        return {"token": token}
    except Exception:
        error_message = traceback.format_exc()
        raise ApiFailure(error_message, 500)
