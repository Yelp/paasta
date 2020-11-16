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
PaaSTA API error handling.
"""
import logging

from pyramid.response import Response
from pyramid.view import view_config


log = logging.getLogger(__name__)


class ApiFailure(Exception):
    def __init__(self, msg, err):
        self.msg = msg
        self.err = err


@view_config(context=ApiFailure)
def api_failure_response(exc, request):
    """Construct an HTTP response with an error status code. This happens when
    the API service has to stop on a 'hard' error. In contrast, the API service
    continues to produce results on a 'soft' error. It will place a 'message'
    field in the output. Multiple 'soft' errors are concatenated in the same
    'message' field when errors happen in the same hierarchy.
    """
    log.error(exc.msg)

    response = Response("ERROR: %s" % exc.msg)
    response.status_int = exc.err
    return response
