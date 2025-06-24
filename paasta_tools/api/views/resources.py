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
PaaSTA resource utilization, etc.
"""
from pyramid.response import Response
from pyramid.view import view_config


def parse_filters(filters):
    # The swagger config verifies that the data is in this format
    #  "pattern": "(.*):(.*,)*(.*)"
    if filters is None:
        return {}
    f = {s[0]: s[1] for s in [e.split(":") for e in filters]}
    f = {k: v.split(",") for k, v in f.items()}
    return f


@view_config(route_name="resources.utilization", request_method="GET", renderer="json")
def resources_utilization(request):
    # Mesos support has been removed - resource utilization now only available via Kubernetes
    response_body = []
    return Response(json_body=response_body, status_code=200)
