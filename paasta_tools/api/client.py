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
Client interface for the Paasta rest api.
"""
import json
import logging
import os
from urllib.parse import urlparse

from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient

import paasta_tools.api
from paasta_tools.api.auth_decorator import AuthClientDecorator
from paasta_tools.utils import load_system_paasta_config


log = logging.getLogger(__name__)


def get_paasta_api_client(cluster=None, system_paasta_config=None, http_res=False, use_auth=True, paasta_token=''):
    if not system_paasta_config:
        system_paasta_config = load_system_paasta_config()

    if not cluster:
        cluster = system_paasta_config.get_cluster()

    api_endpoints = system_paasta_config.get_api_endpoints()
    if cluster not in api_endpoints:
        log.error('Cluster %s not in paasta-api endpoints config', cluster)
        return None

    url = str(api_endpoints[cluster])
    parsed = urlparse(url)
    if not parsed:
        log.error('Unsupported paasta-api url %s', url)
        return None
    api_server = parsed.netloc

    # Get swagger spec from file system instead of the api server
    paasta_api_path = os.path.dirname(paasta_tools.api.__file__)
    swagger_file = os.path.join(paasta_api_path, 'api_docs/swagger.json')
    if not os.path.isfile(swagger_file):
        log.error('paasta-api swagger spec %s does not exist', swagger_file)
        return None

    with open(swagger_file) as f:
        spec_dict = json.load(f)
    # replace localhost in swagger.json with actual api server
    spec_dict['host'] = api_server
    http_client = RequestsClient()
    # REQUEST_OPTIONS_DEFAULTS['response_callbacks'] = [r_call]
    print(api_server)
    if use_auth:
        http_client.set_api_key(
            host=api_server.split(":")[0],
            api_key=paasta_token,
            param_name='X-Paasta-Token',
            param_in='header',
        )

    # sometimes we want the status code
    if http_res:
        config = {'also_return_response': True}
        c = SwaggerClient.from_spec(spec_dict=spec_dict, config=config, http_client=http_client)
    else:
        c = SwaggerClient.from_spec(spec_dict=spec_dict, http_client=http_client)
    return AuthClientDecorator(c, cluster_name=cluster)


def r_call(incoming_response, operation):
    pass
