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
import time

from kazoo.exceptions import NoNodeError
from pyramid.view import view_config

from paasta_tools.api.views.exception import ApiFailure
from paasta_tools.long_running_service_tools import AUTOSCALING_ZK_ROOT
from paasta_tools.utils import ZookeeperPool


@view_config(route_name='pause_service_autoscaler.get', request_method='GET', renderer='json')
def get_service_autoscaler_pause(request):
    zk_pause_autoscale_path = '{}/paused'.format(AUTOSCALING_ZK_ROOT)
    with ZookeeperPool() as zk:
        try:
            pause_until = zk.get(zk_pause_autoscale_path)[0].decode('utf8')
        except (NoNodeError, ValueError):
            pause_until = '0'
        except Exception as e:
            raise ApiFailure(e, 500)

    return pause_until


@view_config(route_name='pause_service_autoscaler.post', request_method='POST', renderer='json')
def update_service_autoscaler_pause(request):
    minutes = request.swagger_data.get('json_body')['minutes']
    current_time = time.time()
    expiry_time = current_time + minutes * 60
    zk_pause_autoscale_path = '{}/paused'.format(AUTOSCALING_ZK_ROOT)
    with ZookeeperPool() as zk:
        try:
            zk.ensure_path(zk_pause_autoscale_path)
            zk.set(zk_pause_autoscale_path, str(expiry_time).encode('utf-8'))
        except Exception as e:
            raise ApiFailure(e, 500)

    return
