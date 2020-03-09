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
from typing import Any
from urllib.parse import urlparse

from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient

import paasta_tools.api.auth_decorator
from paasta_tools.secret_tools import get_secret_provider
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import SystemPaastaConfig


log = logging.getLogger(__name__)


def get_paasta_api_client(
    cluster: str = None,
    system_paasta_config: SystemPaastaConfig = None,
    http_res: bool = False,
) -> Any:
    if not system_paasta_config:
        system_paasta_config = load_system_paasta_config()

    if not cluster:
        cluster = system_paasta_config.get_cluster()

    api_endpoints = system_paasta_config.get_api_endpoints()
    if cluster not in api_endpoints:
        log.error("Cluster %s not in paasta-api endpoints config", cluster)
        return None

    url = str(api_endpoints[cluster])
    parsed = urlparse(url)
    if not parsed:
        log.error("Unsupported paasta-api url %s", url)
        return None
    api_server = parsed.netloc

    # Get swagger spec from file system instead of the api server
    paasta_api_path = os.path.dirname(paasta_tools.api.__file__)
    swagger_file = os.path.join(paasta_api_path, "api_docs/swagger.json")
    if not os.path.isfile(swagger_file):
        log.error("paasta-api swagger spec %s does not exist", swagger_file)
        return None

    with open(swagger_file) as f:
        spec_dict = json.load(f)
    # replace localhost in swagger.json with actual api server
    spec_dict["host"] = api_server
    spec_dict["schemes"] = [parsed.scheme]

    # sometimes we want the status code
    requests_client = PaastaRequestsClient(
        scheme=parsed.scheme, cluster=cluster, system_paasta_config=system_paasta_config
    )
    if http_res:
        config = {"also_return_response": True}
        c = SwaggerClient.from_spec(
            spec_dict=spec_dict, config=config, http_client=requests_client
        )
    else:
        c = SwaggerClient.from_spec(spec_dict=spec_dict, http_client=requests_client)
    return paasta_tools.api.auth_decorator.AuthClientDecorator(c, cluster_name=cluster)


class PaastaRequestsClient(RequestsClient):
    def __init__(
        self, scheme: str, cluster: str, system_paasta_config: SystemPaastaConfig
    ) -> None:
        if scheme == "https":
            ca_cert_path = None
            if system_paasta_config.get_enable_client_cert_auth():
                ecosystem = system_paasta_config.get_vault_cluster_config()[cluster]
                paasta_dir = os.path.expanduser("~/.paasta/pki")
                if (
                    not os.path.isfile(f"{paasta_dir}/{ecosystem}.crt")
                    or not os.path.isfile(f"{paasta_dir}/{ecosystem}.key")
                    or not os.path.isfile(f"{paasta_dir}/{ecosystem}_ca.crt")
                ):
                    renew_issue_cert(
                        system_paasta_config=system_paasta_config, cluster=cluster
                    )
                ca_cert_path = f"{paasta_dir}/{ecosystem}_ca.crt"
                ssl_cert = (
                    f"{paasta_dir}/{ecosystem}.crt",
                    f"{paasta_dir}/{ecosystem}.key",
                )
            else:
                ssl_cert = None
            super().__init__(ssl_verify=True, ssl_cert=ssl_cert)
            if ca_cert_path:
                self.session.verify = ca_cert_path
        else:
            super().__init__()


def renew_issue_cert(system_paasta_config: SystemPaastaConfig, cluster: str) -> None:
    secret_provider_kwargs = {
        "vault_cluster_config": system_paasta_config.get_vault_cluster_config()
    }
    sp = get_secret_provider(
        secret_provider_name=system_paasta_config.get_secret_provider_name(),
        cluster_names=[cluster],
        secret_provider_kwargs=secret_provider_kwargs,
        soa_dir=None,
        service_name=None,
    )
    sp.renew_issue_cert(
        pki_backend=system_paasta_config.get_pki_backend(),
        ttl=system_paasta_config.get_auth_certificate_ttl(),
    )
