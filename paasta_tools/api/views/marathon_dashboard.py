#!/usr/bin/env python
# Copyright 2017 Yelp Inc.
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
Marathon Dashboard
"""
import logging

from pyramid.view import view_config

from paasta_tools.api import settings
from paasta_tools.marathon_dashboard import create_marathon_dashboard

log = logging.getLogger(__name__)


@view_config(route_name="marathon_dashboard", request_method="GET", renderer="json")
def marathon_dashboard(request):
    log.debug("marathon_dashboard view")
    return create_marathon_dashboard(
        cluster=settings.cluster,
        soa_dir=settings.soa_dir,
        marathon_clients=settings.marathon_clients,
        system_paasta_config=settings.system_paasta_config,
    )
