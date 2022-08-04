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
PaaSTA flink service list jobs, overview and config.
"""
from pyramid.view import view_config

from paasta_tools.api.views.exception import ApiFailure
from paasta_tools.flink_tools import cr_id
from paasta_tools.flink_tools import curl_flink_endpoint


@view_config(
    route_name="flink.service.instance.jobs", request_method="GET", renderer="json"
)
def list_flink_cluster_jobs(request):
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    try:
        return curl_flink_endpoint(cr_id(service, instance), "jobs")
    except ValueError as e:
        raise ApiFailure(e, 500)


@view_config(
    route_name="flink.service.instance.job_details",
    request_method="GET",
    renderer="json",
)
def get_flink_cluster_job_details(request):
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    job_id = request.swagger_data.get("job_id")
    try:
        return curl_flink_endpoint(cr_id(service, instance), f"jobs/{job_id}")
    except ValueError as e:
        raise ApiFailure(e, 500)


@view_config(
    route_name="flink.service.instance.overview", request_method="GET", renderer="json"
)
def get_flink_cluster_overview(request):
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    try:
        return curl_flink_endpoint(cr_id(service, instance), "overview")
    except ValueError as e:
        raise ApiFailure(e, 500)


@view_config(
    route_name="flink.service.instance.config", request_method="GET", renderer="json"
)
def get_flink_cluster_config(request):
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    try:
        return curl_flink_endpoint(cr_id(service, instance), "config")
    except ValueError as e:
        raise ApiFailure(e, 500)
