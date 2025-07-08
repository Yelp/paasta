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
import json
import logging
import os
from typing import NamedTuple
from typing import Optional

import cachetools.func
import pyramid
import requests
from pyramid.config import Configurator
from pyramid.httpexceptions import HTTPForbidden
from pyramid.registry import Registry
from pyramid.request import Request
from pyramid.response import Response

from paasta_tools.api.tweens import Handler


logger = logging.getLogger(__name__)
AUTH_CACHE_SIZE = 50000
AUTH_CACHE_TTL = 30 * 60


class AuthorizationOutcome(NamedTuple):
    authorized: bool
    reason: str


class AuthTweenFactory:
    def __init__(self, handler: Handler, registry: Registry) -> None:
        self.handler = handler
        self.registry = registry
        self.enforce = bool(os.getenv("PAASTA_API_AUTH_ENFORCE", ""))
        self.endpoint = os.getenv("PAASTA_API_AUTH_ENDPOINT")
        self.session = requests.Session()

    def __call__(self, request: Request) -> Response:
        """
        Extracts relevant metadata from request, and checks if it is authorized
        """
        token = request.headers.get("Authorization", "").strip()
        token = token.split()[-1] if token else ""  # removes "Bearer" prefix
        auth_outcome = self.is_request_authorized(
            request.path,
            token,
            request.method,
            request.swagger_data.get("service", None),
        )
        if self.enforce and not auth_outcome.authorized:
            return HTTPForbidden(
                body=json.dumps({"reason": auth_outcome.reason}),
                headers={"X-Auth-Failure-Reason": auth_outcome.reason},
                content_type="application/json",
                charset="utf-8",
            )
        return self.handler(request)

    @cachetools.func.ttl_cache(maxsize=AUTH_CACHE_SIZE, ttl=AUTH_CACHE_TTL)
    def is_request_authorized(
        self,
        path: str,
        token: str,
        method: str,
        service: Optional[str],
    ) -> AuthorizationOutcome:
        """
        Check if API request is authorized

        :param str path: API path
        :param str token: authentication token
        :param str method: http method
        :return: auth outcome
        """
        try:
            response = self.session.post(
                url=self.endpoint,
                json={
                    "input": {
                        "path": path,
                        "backend": "paasta",
                        "token": token,
                        "method": method,
                        "service": service,
                    },
                },
                timeout=2,
            ).json()
        except Exception as e:
            logger.exception(f"Issue communicating with auth endpoint: {e}")
            return AuthorizationOutcome(False, "Auth backend error")

        auth_result_allowed = response.get("result", {}).get("allowed")
        if auth_result_allowed is None:
            return AuthorizationOutcome(False, "Malformed auth response")

        if not auth_result_allowed:
            reason = response["result"].get("reason", "Denied")
            return AuthorizationOutcome(False, reason)

        reason = response["result"].get("reason", "Ok")
        return AuthorizationOutcome(True, reason)


def includeme(config: Configurator):
    if os.getenv("PAASTA_API_AUTH_ENDPOINT"):
        config.add_tween(
            "paasta_tools.api.tweens.auth.AuthTweenFactory",
            under=(
                pyramid.tweens.INGRESS,
                "pyramid_swagger.tween.validation_tween_factory",
            ),
        )
