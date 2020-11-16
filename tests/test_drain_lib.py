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
import contextlib

import asynctest
import mock
import pytest

from paasta_tools import drain_lib


def test_register_drain_method():

    with mock.patch.dict(drain_lib._drain_methods):

        @drain_lib.register_drain_method("FAKEDRAINMETHOD")
        class FakeDrainMethod(drain_lib.DrainMethod):
            pass

        assert (
            type(drain_lib.get_drain_method("FAKEDRAINMETHOD", "srv", "inst", "ns"))
            == FakeDrainMethod
        )


@contextlib.contextmanager
def mock_ClientSession(**fake_session_kwargs):
    fake_session = asynctest.MagicMock(name="session", **fake_session_kwargs)

    class FakeClientSession:
        def __init__(self, *args, **kwargs):
            ...

        async def __aenter__(*args):
            return fake_session

        async def __aexit__(*args):
            pass

    with mock.patch("aiohttp.ClientSession", new=FakeClientSession, autospec=False):
        yield


class TestHacheckDrainMethod:
    drain_method = drain_lib.HacheckDrainMethod(
        service="srv",
        instance="inst",
        registrations=["ns_one", "ns_two"],
        hacheck_port=12345,
    )

    async def _async_id(self, x):
        return x

    def test_spool_urls(self):
        fake_task = mock.Mock(host="fake_host", ports=[54321])
        actual = self.drain_method.spool_urls(fake_task)
        # Nerve hits /{mode}/{service}.{namespace}/{port}/status
        expected = [
            f"http://fake_host:12345/spool/{ns}/54321/status"
            for ns in self.drain_method.registrations
        ]
        assert actual == expected

    @pytest.mark.asyncio
    async def test_for_each_registration_with_no_ports(self):
        fake_task = mock.Mock(host="fake_host", ports=[])
        actual = await self.drain_method.for_each_registration(
            task=fake_task, func=self._async_id
        )
        assert actual is None

    @pytest.mark.asyncio
    async def test_for_each_registration(self):
        fake_task = mock.Mock(host="fake_host", ports=[54321])
        actual = await self.drain_method.for_each_registration(
            task=fake_task, func=self._async_id
        )
        assert actual == self.drain_method.spool_urls(fake_task)

    @pytest.mark.asyncio
    async def test_is_draining_yes(self):
        fake_response = mock.Mock(
            status=503,
            text=asynctest.CoroutineMock(
                return_value="Service service in down state since 1435694078.778886 "
                "until 1435694178.780000: Drained by Paasta"
            ),
        )
        fake_task = mock.Mock(host="fake_host", ports=[54321])
        with mock_ClientSession(
            get=mock.Mock(
                return_value=asynctest.MagicMock(
                    __aenter__=asynctest.CoroutineMock(return_value=fake_response)
                )
            )
        ):
            assert await self.drain_method.is_draining(fake_task) is True

    @pytest.mark.asyncio
    async def test_is_draining_no(self):
        fake_response = mock.Mock(
            status=200, text=asynctest.CoroutineMock(return_value="")
        )
        fake_task = mock.Mock(host="fake_host", ports=[54321])
        with mock_ClientSession(
            get=mock.Mock(
                return_value=asynctest.MagicMock(
                    __aenter__=asynctest.CoroutineMock(return_value=fake_response)
                )
            )
        ):
            assert await self.drain_method.is_draining(fake_task) is False


class TestHTTPDrainMethod:
    def test_get_format_params(self):
        fake_task = mock.Mock(host="fake_host", ports=[54321])
        drain_method = drain_lib.HTTPDrainMethod(
            "fake_service", "fake_instance", ["fake_nerve_ns"], {}, {}, {}, {}
        )
        assert drain_method.get_format_params(fake_task) == [
            {
                "host": "fake_host",
                "port": 54321,
                "service": "fake_service",
                "instance": "fake_instance",
                "nerve_ns": "fake_nerve_ns",
            }
        ]

    def test_format_url(self):
        drain_method = drain_lib.HTTPDrainMethod(
            "fake_service", "fake_instance", ["fake_nerve_ns"], {}, {}, {}, {}
        )
        url_format = "foo_{host}"
        format_params = {"host": "fake_host"}
        assert drain_method.format_url(url_format, format_params) == "foo_fake_host"

    def test_parse_success_codes(self):
        drain_method = drain_lib.HTTPDrainMethod(
            "fake_service", "fake_instance", ["fake_nerve_ns"], {}, {}, {}, {}
        )
        assert drain_method.parse_success_codes("200") == {200}
        assert drain_method.parse_success_codes("200-203") == {200, 201, 202, 203}
        assert drain_method.parse_success_codes("200-202,302,305-306") == {
            200,
            201,
            202,
            302,
            305,
            305,
            306,
        }
        assert drain_method.parse_success_codes(200) == {200}

    def test_check_response_code(self):
        drain_method = drain_lib.HTTPDrainMethod(
            "fake_service", "fake_instance", ["fake_nerve_ns"], {}, {}, {}, {}
        )

        # Happy case
        assert drain_method.check_response_code(200, "200-299") is True

        # Sad case
        assert drain_method.check_response_code(500, "200-299") is False

    @pytest.mark.asyncio
    async def test_issue_request(self):
        drain_method = drain_lib.HTTPDrainMethod(
            "fake_service", "fake_instance", ["fake_nerve_ns"], {}, {}, {}, {}
        )
        fake_task = mock.Mock(host="fake_host", ports=[54321])
        url_spec = {
            "url_format": "http://localhost:654321/fake/{host}",
            "method": "get",
            "success_codes": "1234",
        }

        fake_resp = mock.Mock(status=1234)
        mock_request = mock.Mock(
            return_value=asynctest.CoroutineMock(return_value=fake_resp)()
        )
        with mock_ClientSession(request=mock_request):
            await drain_method.issue_request(url_spec=url_spec, task=fake_task)

        mock_request.assert_called_once_with(
            method="GET",
            url="http://localhost:654321/fake/fake_host",
            headers=mock.ANY,
            timeout=15,
        )
