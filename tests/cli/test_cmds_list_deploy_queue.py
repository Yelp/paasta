#!/usr/bin/env python
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
import json

import mock
import pytest

from paasta_tools.cli.cmds.list_deploy_queue import list_deploy_queue
from paasta_tools.paastaapi import ApiException
from paasta_tools.paastaapi.models import DeployQueue
from paasta_tools.paastaapi.models import DeployQueueServiceInstance


@pytest.fixture(autouse=True)
def mock_load_system_paasta_config():
    with mock.patch(
        "paasta_tools.cli.cmds.list_deploy_queue.load_system_paasta_config",
        autospec=True,
    ):
        yield


@pytest.fixture(autouse=True)
def mock_list_clusters():
    with mock.patch(
        "paasta_tools.cli.cmds.list_deploy_queue.list_clusters", autospec=True,
    ) as _mock_list_clusters:
        _mock_list_clusters.return_value = ["westeros-prod"]
        yield


@pytest.fixture()
def mock_api():
    with mock.patch(
        "paasta_tools.cli.cmds.list_deploy_queue.get_paasta_oapi_client", autospec=True,
    ) as m:
        yield m.return_value


def test_list_deploy_queue(mock_api, capfd):
    args = mock.Mock(cluster="westeros-prod", json=False)
    mock_api.default.deploy_queue.return_value = DeployQueue(
        available_service_instances=[
            DeployQueueServiceInstance(
                service="service1",
                instance="instance1",
                watcher="watcher1",
                bounce_by=1578038400.0,
                wait_until=1578038400.0,
                enqueue_time=1578038400.0,
                bounce_start_time=1578038400.0,
                failures=0,
                processed_count=0,
            ),
        ],
        unavailable_service_instances=[
            DeployQueueServiceInstance(
                service="service2",
                instance="instance2",
                watcher="watcher2",
                bounce_by=1577952000.0,
                wait_until=1577952000.0,
                enqueue_time=1577952000.0,
                bounce_start_time=1577952000.0,
                failures=5,
                processed_count=10,
            ),
        ],
    )

    return_value = list_deploy_queue(args)

    assert return_value == 0
    stdout, stderr = capfd.readouterr()
    lines = stdout.split("\n")
    assert args.cluster in lines[0]
    assert "service1.instance1" in lines[3]
    assert "service2.instance2" in lines[6]


def test_list_deploy_queue_json(mock_api, capfd):
    args = mock.Mock(cluster="westeros-prod", json=True)
    mock_api.default.deploy_queue.return_value = DeployQueue(
        available_service_instances=[], unavailable_service_instances=[],
    )

    return_value = list_deploy_queue(args)
    assert return_value == 0

    stdout, stderr = capfd.readouterr()
    assert stdout.strip() == json.dumps(
        {"available_service_instances": [], "unavailable_service_instances": []}
    )


def test_http_error(mock_api):
    args = mock.Mock(cluster="westeros-prod")
    mock_api.api_error = ApiException
    mock_api.default.deploy_queue.side_effect = ApiException(
        status=500, reason="Internal Server Error"
    )

    assert list_deploy_queue(args) == 500
