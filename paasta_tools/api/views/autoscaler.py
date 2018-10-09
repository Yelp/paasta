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
from paasta_tools.long_running_service_tools import set_instances_for_marathon_service
from paasta_tools.marathon_tools import load_marathon_service_config


@view_config(route_name='service.autoscaler.get', request_method='GET', renderer='json')
def get_autoscaler_count(request):
    service = request.swagger_data.get('service')
    instance = request.swagger_data.get('instance')
    cluster = settings.cluster
    soa_dir = settings.soa_dir
    try:
        service_config = load_marathon_service_config(
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            load_deployments=False,
        )
    except Exception:
        error_message = f'Unable to load service config for {service}.{instance}'
        raise ApiFailure(error_message, 404)

    response_body = {
        'desired_instances': service_config.get_instances(),
        'calculated_instances': service_config.get_instances(with_limit=False),
    }
    return Response(json_body=response_body, status_code=200)


@view_config(route_name='service.autoscaler.post', request_method='POST', renderer='json')
def update_autoscaler_count(request):
    service = request.swagger_data.get('service')
    instance = request.swagger_data.get('instance')
    desired_instances = request.swagger_data.get('json_body')['desired_instances']
    if not isinstance(desired_instances, int):
        error_message = 'The provided body does not have an integer value for "desired_instances": {}'.format(
            request.swagger_data.get('json_body'),
        )
        raise ApiFailure(error_message, 500)

    try:
        service_config = load_marathon_service_config(
            service=service,
            instance=instance,
            cluster=settings.cluster,
            soa_dir=settings.soa_dir,
            load_deployments=False,
        )
    except Exception:
        error_message = f'Unable to load service config for {service}.{instance}'
        raise ApiFailure(error_message, 404)

    max_instances = service_config.get_max_instances()
    if max_instances is None:
        error_message = f'Autoscaling is not enabled for {service}.{instance}'
        raise ApiFailure(error_message, 404)

    min_instances = service_config.get_min_instances()

    # Dump whatever number from the client to zk. get_instances() will limit
    # readings from zk to [min_instances, max_instances].
    set_instances_for_marathon_service(service=service, instance=instance, instance_count=desired_instances)
    status = 'SUCCESS'
    if desired_instances > max_instances:
        desired_instances = max_instances
        status = 'WARNING desired_instances is greater than max_instances %d' % max_instances
    elif desired_instances < min_instances:
        desired_instances = min_instances
        status = 'WARNING desired_instances is less than min_instances %d' % min_instances

    response_body = {'desired_instances': desired_instances, 'status': status}
    return Response(json_body=response_body, status_code=202)
