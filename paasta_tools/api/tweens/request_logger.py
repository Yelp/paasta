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
import traceback
from datetime import datetime

import pyramid
import pytz

from paasta_tools.api import settings

try:
    import clog
except ImportError:
    clog = None


DEFAULT_REQUEST_LOG_NAME = "tmp_paasta_api_requests"


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
        self, timestamp=None, level="INFO", additional_fields=None,
    ):
        if clog is not None:
            # `settings` values are set by paasta_tools.api.api:setup_paasta_api
            if not timestamp:
                timestamp = datetime.now(pytz.utc)
            dct = {
                "human_timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S%Z"),
                "unix_timestamp": timestamp.timestamp(),
                "hostname": settings.hostname,
                "level": level,
                "cluster": settings.cluster,
            }
            if additional_fields is not None:
                dct.update(additional_fields)
            line = json.dumps(dct, sort_keys=True)
            clog.log_line(self.log_name, line)

    def __call__(self, request):
        start_time = datetime.now(pytz.utc)  # start clock for response time
        request_fields = {
            "path": request.path,
            "params": request.params.mixed(),
            "client_addr": request.client_addr,
            "http_method": request.method,
            "headers": dict(request.headers),  # incls user agent
        }
        response_fields = {}
        log_level = "INFO"

        try:
            response = self.handler(request)

            response_fields["status_code"] = response.status_int
            if 300 <= response.status_int < 400:
                log_level = "WARNING"
            elif 400 <= response.status_int < 600:
                log_level = "ERROR"
                response_fields["body"] = response.body.decode("utf-8")

            return response

        except Exception as e:
            log_level = "ERROR"
            response_fields.update(
                {
                    "status_code": 500,
                    "exc_type": type(e).__name__,
                    "exc_info": traceback.format_exc(),
                }
            )
            raise

        finally:
            response_time_ms = (
                datetime.now(pytz.utc) - start_time
            ).total_seconds() * 1000
            response_fields["response_time_ms"] = response_time_ms

            self._log(
                timestamp=start_time,
                level=log_level,
                additional_fields={
                    "request": request_fields,
                    "response": response_fields,
                },
            )
