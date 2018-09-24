#!/usr/bin/env python
# Copyright 2015-2018 Yelp Inc.
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
PaaSTA objgraph
"""
import io
import logging
import os

import objgraph
if os.environ.get("PAASTA_API_OBJGRAPH"):
    from pyramid.view import view_config
else:
    def view_config(*args, **kwargs):
        def noop_wrapper(func):
            return func
        return noop_wrapper

log = logging.getLogger(__name__)


@view_config(route_name='objgraph_show_most_common_types', request_method='GET', renderer='json')
def objgraph_show_most_common_types(request):
    limit = request.swagger_data.get('limit')
    if limit is None or limit <= 0:
        limit = 20
    output = io.StringIO()
    print(f'PID={os.getpid()}', file=output)
    print(f'>>> objgraph.show_most_common_types(limit={limit})', file=output)
    objgraph.show_most_common_types(limit=limit, file=output)
    log.error(output.getvalue())
    return "See the paasta-api log for the output"


@view_config(route_name='objgraph_show_backrefs_for_type', request_method='GET', renderer='json')
def objgraph_show_backrefs_for_type(request):
    type_name = request.swagger_data.get('type')
    max_objects = request.swagger_data.get('max_objects')
    if max_objects is None or max_objects <= 0:
        max_objects = 20
    max_depth = request.swagger_data.get('max_depth')
    if max_depth is None or max_depth <= 0:
        max_depth = 3
    output = io.StringIO()
    print(f'PID={os.getpid()}', file=output)
    print(
        f'>>> objgraph.show_backrefs(objgraph.by_type("{type_name}")'
        f'[:{max_objects}], max_depth={max_depth})', file=output,
    )
    objs = objgraph.by_type(type_name)
    objgraph.show_backrefs(objs[:max_objects], max_depth=max_depth)
    log.error(output.getvalue())
    return "See the paasta-api log for the output"
