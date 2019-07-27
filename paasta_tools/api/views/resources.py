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
from a_sync import block
from pyramid.response import Response
from pyramid.view import view_config

from paasta_tools.mesos_tools import get_mesos_master
from paasta_tools.metrics import metastatus_lib


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
    master = get_mesos_master()
    mesos_state = block(master.state)

    groupings = request.swagger_data.get("groupings", ["superregion"])
    # swagger actually makes the key None if it's not set
    if groupings is None:
        groupings = ["superregion"]
    grouping_function = metastatus_lib.key_func_for_attribute_multi(groupings)
    sorting_function = metastatus_lib.sort_func_for_attributes(groupings)

    filters = request.swagger_data.get("filter", [])
    filters = parse_filters(filters)
    filter_funcs = [
        metastatus_lib.make_filter_slave_func(attr, vals)
        for attr, vals in filters.items()
    ]

    resource_info_dict = metastatus_lib.get_resource_utilization_by_grouping(
        grouping_func=grouping_function,
        mesos_state=mesos_state,
        filters=filter_funcs,
        sort_func=sorting_function,
    )

    response_body = []
    for k, v in resource_info_dict.items():
        group = {"groupings": {}}
        for grouping, value in k:
            group["groupings"][grouping] = value
        for resource, value in v["total"]._asdict().items():
            group[resource] = {"total": value}
        for resource, value in v["free"]._asdict().items():
            group[resource]["free"] = value
        for resource in v["free"]._fields:
            group[resource]["used"] = group[resource]["total"] - group[resource]["free"]

        response_body.append(group)

    return Response(json_body=response_body, status_code=200)
