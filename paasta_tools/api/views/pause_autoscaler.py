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
from paasta_tools.long_running_service_tools import ZK_PAUSE_AUTOSCALE_PATH
from paasta_tools.utils import ZookeeperPool


@view_config(
    route_name="service_autoscaler.pause.get", request_method="GET", renderer="json"
)
def get_service_autoscaler_pause(request):
    with ZookeeperPool() as zk:
        try:
            pause_until = zk.get(ZK_PAUSE_AUTOSCALE_PATH)[0].decode("utf8")
        except (NoNodeError, ValueError):
            pause_until = "0"
        except Exception as e:
            raise ApiFailure(e, 500)

    return pause_until


@view_config(
    route_name="service_autoscaler.pause.post", request_method="POST", renderer="json"
)
def update_service_autoscaler_pause(request):
    minutes = request.swagger_data.get("json_body")["minutes"]
    current_time = time.time()
    expiry_time = current_time + minutes * 60
    with ZookeeperPool() as zk:
        try:
            zk.ensure_path(ZK_PAUSE_AUTOSCALE_PATH)
            zk.set(ZK_PAUSE_AUTOSCALE_PATH, str(expiry_time).encode("utf-8"))
        except Exception as e:
            raise ApiFailure(e, 500)
    return


@view_config(
    route_name="service_autoscaler.pause.delete",
    request_method="DELETE",
    renderer="json",
)
def delete_service_autoscaler_pause(request):
    with ZookeeperPool() as zk:
        try:
            zk.ensure_path(ZK_PAUSE_AUTOSCALE_PATH)
            zk.delete(ZK_PAUSE_AUTOSCALE_PATH)
        except Exception as e:
            raise ApiFailure(e, 500)
    return
