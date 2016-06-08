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
import mock
from pytest import raises

from paasta_tools import drain_lib


def test_register_drain_method():

    with mock.patch.dict(drain_lib._drain_methods):
        @drain_lib.register_drain_method('FAKEDRAINMETHOD')
        class FakeDrainMethod(drain_lib.DrainMethod):
            pass

        assert type(drain_lib.get_drain_method('FAKEDRAINMETHOD', 'srv', 'inst', 'ns')) == FakeDrainMethod


class TestHacheckDrainMethod(object):
    drain_method = drain_lib.HacheckDrainMethod("srv", "inst", "ns", hacheck_port=12345)

    def test_spool_url(self):
        fake_task = mock.Mock(host="fake_host", ports=[54321])
        actual = self.drain_method.spool_url(fake_task)
        # Nerve hits /{mode}/{service}.{namespace}/{port}/status
        expected = 'http://fake_host:12345/spool/srv.ns/54321/status'
        assert actual == expected

    def test_get_spool(self):
        fake_response = mock.Mock(
            status_code=503,
            text="Service service in down state since 1435694078.778886 until 1435694178.780000: Drained by Paasta",
        )
        fake_task = mock.Mock(host="fake_host", ports=[54321])
        with mock.patch('requests.get', return_value=fake_response):
            actual = self.drain_method.get_spool(fake_task)

        expected = {
            'service': 'service',
            'state': 'down',
            'reason': 'Drained by Paasta',
            'since': 1435694078.778886,
            'until': 1435694178.780000,
        }
        assert actual == expected

    def test_is_draining_yes(self):
        fake_response = mock.Mock(
            status_code=503,
            text="Service service in down state since 1435694078.778886 until 1435694178.780000: Drained by Paasta",
        )
        fake_task = mock.Mock(host="fake_host", ports=[54321])
        with mock.patch('requests.get', return_value=fake_response):
            assert self.drain_method.is_draining(fake_task) is True

    def test_is_draining_no(self):
        fake_response = mock.Mock(
            status_code=200,
            text="",
        )
        fake_task = mock.Mock(host="fake_host", ports=[54321])
        with mock.patch('requests.get', return_value=fake_response):
            assert self.drain_method.is_draining(fake_task) is False


class TestHTTPDrainMethod(object):
    def test_get_format_params(self):
        fake_task = mock.Mock(host="fake_host", ports=[54321])
        drain_method = drain_lib.HTTPDrainMethod('fake_service', 'fake_instance', 'fake_nerve_ns', {}, {}, {}, {})
        assert drain_method.get_format_params(fake_task) == {
            'host': 'fake_host',
            'port': 54321,
            'service': 'fake_service',
            'instance': 'fake_instance',
            'nerve_ns': 'fake_nerve_ns',
        }

    def test_format_url(self):
        drain_method = drain_lib.HTTPDrainMethod('fake_service', 'fake_instance', 'fake_nerve_ns', {}, {}, {}, {})
        url_format = "foo_{host}"
        format_params = {"host": "fake_host"}
        assert drain_method.format_url(url_format, format_params) == "foo_fake_host"

    def test_parse_success_codes(self):
        drain_method = drain_lib.HTTPDrainMethod('fake_service', 'fake_instance', 'fake_nerve_ns', {}, {}, {}, {})
        assert drain_method.parse_success_codes('200') == set([200])
        assert drain_method.parse_success_codes('200-203') == set([200, 201, 202, 203])
        assert drain_method.parse_success_codes('200-202,302,305-306') == set([200, 201, 202, 302, 305, 305, 306])

    def test_check_response_code(self):
        drain_method = drain_lib.HTTPDrainMethod('fake_service', 'fake_instance', 'fake_nerve_ns', {}, {}, {}, {})

        # Happy case
        drain_method.check_response_code(200, '200-299')

        # Sad case
        with raises(drain_lib.StatusCodeNotAcceptableException):
            drain_method.check_response_code(500, '200-299')

    def test_issue_request(self):
        drain_method = drain_lib.HTTPDrainMethod('fake_service', 'fake_instance', 'fake_nerve_ns', {}, {}, {}, {})
        fake_task = mock.Mock(host='fake_host', ports=[54321])
        url_spec = {
            'url_format': 'http://localhost:654321/fake/{host}',
            'method': 'get',
            'success_codes': '1234',
        }

        fake_resp = mock.Mock(status_code=1234)

        with mock.patch('paasta_tools.drain_lib.requests.get', autospec=True, return_value=fake_resp) as mock_get:
            drain_method.issue_request(
                url_spec=url_spec,
                task=fake_task,
            )

        mock_get.assert_called_once_with('http://localhost:654321/fake/fake_host')
