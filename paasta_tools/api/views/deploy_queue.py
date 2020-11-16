#!/usr/bin/env python
# Copyright 2015-2020 Yelp Inc.
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
from typing import Any
from typing import Mapping

from kazoo.client import KazooClient
from pyramid.view import view_config

from paasta_tools.api import settings
from paasta_tools.deployd.queue import ZKDelayDeadlineQueue


@view_config(route_name="deploy_queue.list", request_method="GET", renderer="json")
def list_deploy_queue(request) -> Mapping[str, Any]:
    zk_client = KazooClient(hosts=settings.system_paasta_config.get_zk_hosts())
    zk_client.start()

    queue = ZKDelayDeadlineQueue(client=zk_client)
    available_service_instances = queue.get_available_service_instances(
        fetch_service_instances=True
    )
    unavailable_service_instances = queue.get_unavailable_service_instances(
        fetch_service_instances=True
    )

    available_service_instance_dicts = [
        service_instance._asdict()
        for _, service_instance in available_service_instances
    ]
    unavailable_service_instance_dicts = [
        service_instance._asdict()
        for _, __, service_instance in unavailable_service_instances
    ]

    return {
        "available_service_instances": available_service_instance_dicts,
        "unavailable_service_instances": unavailable_service_instance_dicts,
    }
