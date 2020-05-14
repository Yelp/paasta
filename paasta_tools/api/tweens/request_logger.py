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
Creates a tween that logs information about requests.
"""
import json
import logging
import time
import traceback

import pyramid

from paasta_tools import utils
from paasta_tools.api import settings

try:
    import clog
except ImportError:
    clog = None


DEFAULT_REQUEST_LOG_NAME = "tmp_paasta_requests"


def includeme(config):
    if clog is not None:
        config.add_tween(
            "paasta_tools.api.tweens.request_logger.request_logger_tween_factory",
            under=pyramid.tweens.INGRESS,
        )


class request_logger_tween_factory:
    """Tween that logs information about requests"""

    def __init__(self, handler, registry):
        self.handler = handler
        self.registry = registry
        self.log_name = registry.settings.get(
            "request_log_name", DEFAULT_REQUEST_LOG_NAME,
        )

    def _log(
        self, endpoint, level=logging.INFO, additional_fields=None,
    ):
        if clog is not None:
            dct = {
                "timestamp": time.time(),
                "hostname": utils.get_hostname(),
                "level": logging.getLevelName(level),
                "cluster": settings.cluster,
                "endpoint": endpoint,
            }
            if additional_fields is not None:
                dct.update(additional_fields)
            line = json.dumps(dct, sort_keys=True)
            clog.log_line(self.log_name, line)

    def __call__(self, request):
        start_time = time.time()  # start clock for response time
        request_fields = {
            "path": request.path,
            "params": request.params.mixed(),
        }
        response_fields = {}
        log_level = logging.INFO

        try:
            response = self.handler(request)

            response_fields["status_code"] = response.status_int
            if 300 <= response.status_int < 400:
                log_level = logging.WARNING
            elif 400 <= response.status_int < 600:
                log_level = logging.ERROR
                response_fields["body"] = response.body.decode("utf-8")

            return response

        except Exception as e:
            log_level = logging.ERROR
            response_fields.update(
                {
                    "status_code": 500,
                    "exc_type": type(e).__name__,
                    "exc_info": traceback.format_exc(),
                }
            )
            raise

        finally:
            response_time_ms = (time.time() - start_time) * 1000
            response_fields["response_time_ms"] = response_time_ms

            self._log(
                endpoint=request.matched_route.name if request.matched_route else None,
                level=log_level,
                additional_fields={
                    "request": request_fields,
                    "response": response_fields,
                },
            )
