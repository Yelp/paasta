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
from pyramid.view import view_config

from paasta_tools.marathon_tools import get_instances_from_zookeeper
from paasta_tools.marathon_tools import set_instances_for_marathon_service


@view_config(route_name='service.autoscaler.get', request_method='GET', renderer='json')
def get_autoscaler_count(request):
    service = request.swagger_data.get('service')
    instance = request.swagger_data.get('instance')
    desired_instances = get_instances_from_zookeeper(service=service, instance=instance)
    return desired_instances


@view_config(route_name='service.autoscaler.post', request_method='POST', renderer='json')
def update_autoscaler_count(request):
    service = request.swagger_data.get('service')
    instance = request.swagger_data.get('instance')
    desired_instances = request.swagger_data.get('desired_instances')
    set_instances_for_marathon_service(service=service, instance=instance, instance_count=desired_instances)
    return desired_instances
