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
import logging
from dataclasses import dataclass
from typing import Optional
from typing import Type
from urllib.parse import ParseResult
from urllib.parse import urlparse

import paasta_tools.paastaapi.apis as paastaapis
from paasta_tools import paastaapi
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import SystemPaastaConfig


log = logging.getLogger(__name__)


@dataclass
class PaastaOApiClient:
    autoscaler: paastaapis.AutoscalerApi
    default: paastaapis.DefaultApi
    resources: paastaapis.ResourcesApi
    service: paastaapis.ServiceApi
    api_error: Type[paastaapi.ApiException]
    connection_error: Type[paastaapi.ApiException]
    timeout_error: Type[paastaapi.ApiException]
    request_error: Type[paastaapi.ApiException]


def get_paasta_oapi_client_by_url(
    parsed_url: ParseResult,
    cert_file: Optional[str] = None,
    key_file: Optional[str] = None,
    ssl_ca_cert: Optional[str] = None,
) -> PaastaOApiClient:
    server_variables = dict(scheme=parsed_url.scheme, host=parsed_url.netloc)
    config = paastaapi.Configuration(
        server_variables=server_variables,
        discard_unknown_keys=True,
    )
    config.cert_file = cert_file
    config.key_file = key_file
    config.ssl_ca_cert = ssl_ca_cert

    client = paastaapi.ApiClient(configuration=config)
    # PAASTA-18005: Adds default timeout to paastaapi client
    client.rest_client.pool_manager.connection_pool_kw[
        "timeout"
    ] = load_system_paasta_config().get_api_client_timeout()
    return PaastaOApiClient(
        autoscaler=paastaapis.AutoscalerApi(client),
        default=paastaapis.DefaultApi(client),
        resources=paastaapis.ResourcesApi(client),
        service=paastaapis.ServiceApi(client),
        api_error=paastaapi.ApiException,
        connection_error=paastaapi.ApiException,
        timeout_error=paastaapi.ApiException,
        request_error=paastaapi.ApiException,
    )


def get_paasta_oapi_client(
    cluster: str = None,
    system_paasta_config: SystemPaastaConfig = None,
    http_res: bool = False,
) -> Optional[PaastaOApiClient]:
    if not system_paasta_config:
        system_paasta_config = load_system_paasta_config()

    if not cluster:
        cluster = system_paasta_config.get_cluster()

    api_endpoints = system_paasta_config.get_api_endpoints()
    if cluster not in api_endpoints:
        log.error("Cluster %s not in paasta-api endpoints config", cluster)
        return None

    parsed = urlparse(api_endpoints[cluster])
    cert_file = key_file = ssl_ca_cert = None

    return get_paasta_oapi_client_by_url(parsed, cert_file, key_file, ssl_ca_cert)
