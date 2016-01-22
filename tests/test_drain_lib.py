# Copyright 2015 Yelp Inc.
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
