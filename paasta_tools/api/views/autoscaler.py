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
PaaSTA service list (instances) etc.
"""
from pyramid.response import Response
from pyramid.view import view_config

from paasta_tools.api import settings
from paasta_tools.api.views.exception import ApiFailure
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.marathon_tools import MarathonServiceConfig


@view_config(route_name="service.autoscaler.get", request_method="GET", renderer="json")
def get_autoscaler_count(request):
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    cluster = settings.cluster
    soa_dir = settings.soa_dir

    instance_config = get_instance_config(service, instance, cluster, soa_dir)
    if not isinstance(
        instance_config, (KubernetesDeploymentConfig, MarathonServiceConfig)
    ):
        error_message = (
            f"Autoscaling is not supported for {service}.{instance} because instance type is not "
            f"marathon or kubernetes."
        )
        raise ApiFailure(error_message, 501)

    response_body = {
        "desired_instances": instance_config.get_instances(),
        "calculated_instances": instance_config.get_instances(with_limit=False),
    }
    return Response(json_body=response_body, status_code=200)


@view_config(
    route_name="service.autoscaler.post", request_method="POST", renderer="json"
)
def update_autoscaler_count(request):
    service = request.swagger_data.get("service")
    instance = request.swagger_data.get("instance")
    cluster = settings.cluster
    soa_dir = settings.soa_dir
    desired_instances = request.swagger_data.get("json_body")["desired_instances"]
    if not isinstance(desired_instances, int):
        error_message = 'The provided body does not have an integer value for "desired_instances": {}'.format(
            request.swagger_data.get("json_body")
        )
        raise ApiFailure(error_message, 500)

    instance_config = get_instance_config(service, instance, cluster, soa_dir, True)
    if not isinstance(
        instance_config, (KubernetesDeploymentConfig, MarathonServiceConfig)
    ):
        error_message = (
            f"Autoscaling is not supported for {service}.{instance} because instance type is not "
            f"marathon or kubernetes."
        )
        raise ApiFailure(error_message, 501)

    max_instances = instance_config.get_max_instances()
    if max_instances is None:
        error_message = f"Autoscaling is not enabled for {service}.{instance}"
        raise ApiFailure(error_message, 404)
    min_instances = instance_config.get_min_instances()

    status = "SUCCESS"
    if desired_instances > max_instances:
        desired_instances = max_instances
        status = (
            "WARNING desired_instances is greater than max_instances %d" % max_instances
        )
    elif desired_instances < min_instances:
        desired_instances = min_instances
        status = (
            "WARNING desired_instances is less than min_instances %d" % min_instances
        )
    try:
        if isinstance(instance_config, KubernetesDeploymentConfig):
            instance_config.set_autoscaled_instances(
                instance_count=desired_instances, kube_client=settings.kubernetes_client
            )
        else:
            instance_config.set_autoscaled_instances(instance_count=desired_instances)
    except Exception as err:
        raise ApiFailure(err, 500)

    response_body = {"desired_instances": desired_instances, "status": status}
    return Response(json_body=response_body, status_code=202)
