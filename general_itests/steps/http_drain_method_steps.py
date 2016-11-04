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
import BaseHTTPServer
import threading

import mock
from behave import given
from behave import then
from behave import when

from paasta_tools import drain_lib


@given('a fake HTTP server')
def make_fake_http_server(context):
    context.fake_http_server = FakeHTTPServer()
    context.fake_http_server.start()


@given('a HTTPDrainMethod configured to point at that server')
def make_http_drain_method(context):
    context.http_drain_method = drain_lib.HTTPDrainMethod(
        service='fake_service',
        instance='fake_instance',
        nerve_ns='fake_nerve_ns',
        drain={
            "url_format": "http://localhost:%d/drain" % context.fake_http_server.server.server_port,
            "success_codes": "200",
        },
        stop_draining={},
        is_draining={
            "url_format": "http://localhost:%d/is_draining" % context.fake_http_server.server.server_port,
            "success_codes": "200",
        },
        is_safe_to_kill={},
    )


@when('we call {method}() and get status code {status_code:d}')
def call_drain(context, method, status_code):
    fake_task = mock.Mock(host="fake_host", ports=[654321])
    FakeHTTPRequestHandler.status_code = status_code

    func = {
        'drain': context.http_drain_method.drain,
        'is_draining': context.http_drain_method.is_draining,
    }[method]
    context.retval = func(fake_task)


@then('the server should see a request to {path}')
def check_http_server(context, path):
    assert context.fake_http_server.paths == [path]


@then('the return value should be {expected_retval}')
def check_retval(context, expected_retval):
    assert repr(context.retval) == expected_retval


class FakeHTTPServer(object):
    paths = []

    def start(self):
        self.server = BaseHTTPServer.HTTPServer(("localhost", 0), FakeHTTPRequestHandler)
        self.server_thread = threading.Thread(target=self.serve)
        self.server_thread.daemon = True
        self.server_thread.start()

    def serve(self):
        self.server.serve_forever()

    def shutdown(self):
        FakeHTTPServer.paths = []
        self.server.shutdown()
        self.server_thread.join()


class FakeHTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    status_code = 200

    def do_GET(self):
        print("Got GET for %s" % self.path)
        try:
            FakeHTTPServer.paths.append(self.path)
            self.send_response(self.status_code)
        except Exception as e:
            print(e)
