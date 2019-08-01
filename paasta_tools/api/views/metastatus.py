#!/usr/bin/env python
# Copyright 2018 Yelp Inc.
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
PaaSTA metastatus.
"""
from pyramid.view import view_config

from paasta_tools.paasta_metastatus import get_output


@view_config(route_name="metastatus", request_method="GET", renderer="json")
def metastatus(request):
    cmd_args = request.swagger_data.get("cmd_args", None)
    output, exit_code = get_output(cmd_args)
    return {"output": output, "exit_code": exit_code}
