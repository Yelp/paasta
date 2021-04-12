# Copyright 2015-2020 Yelp Inc.
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
from unittest import mock

from zake.fake_client import FakeClient

from paasta_tools.api import settings
from paasta_tools.api.views import deploy_queue
from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.utils import SystemPaastaConfig


@mock.patch("paasta_tools.api.views.deploy_queue.KazooClient", autospec=True)
@mock.patch("paasta_tools.api.views.deploy_queue.ZKDelayDeadlineQueue", autospec=True)
def test_list_deploy_queue(mock_delay_deadline_queue_class, mock_kazoo_client):
    mock_request = mock.Mock()
    settings.system_paasta_config = mock.create_autospec(SystemPaastaConfig)
    mock_kazoo_client.return_value = FakeClient()

    available_service_instance = ServiceInstance(
        service="fake_service1",
        instance="fake_instance1",
        watcher="worker0",
        bounce_by=1577952000,
        wait_until=1577952000,
        enqueue_time=1577952000,
        bounce_start_time=1577952000,
        failures=1,
        processed_count=2,
    )
    unavailable_service_instance = ServiceInstance(
        service="fake_service2",
        instance="fake_instance2",
        watcher="worker1",
        bounce_by=1577952100,
        wait_until=1577952200,
        enqueue_time=1577952100,
        bounce_start_time=1577952100,
        failures=2,
        processed_count=3,
    )

    mock_delay_deadline_queue = mock_delay_deadline_queue_class.return_value
    mock_delay_deadline_queue.get_available_service_instances.return_value = [
        (mock.Mock(), available_service_instance)
    ]
    mock_delay_deadline_queue.get_unavailable_service_instances.return_value = [
        (mock.Mock(), mock.Mock(), unavailable_service_instance)
    ]

    output = deploy_queue.list_deploy_queue(mock_request)
    assert output == {
        "available_service_instances": [
            {
                "service": "fake_service1",
                "instance": "fake_instance1",
                "watcher": "worker0",
                "bounce_by": 1577952000,
                "wait_until": 1577952000,
                "enqueue_time": 1577952000,
                "bounce_start_time": 1577952000,
                "failures": 1,
                "processed_count": 2,
            }
        ],
        "unavailable_service_instances": [
            {
                "service": "fake_service2",
                "instance": "fake_instance2",
                "watcher": "worker1",
                "bounce_by": 1577952100,
                "wait_until": 1577952200,
                "enqueue_time": 1577952100,
                "bounce_start_time": 1577952100,
                "failures": 2,
                "processed_count": 3,
            }
        ],
    }
