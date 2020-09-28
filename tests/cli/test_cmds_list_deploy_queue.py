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
from bravado.exception import HTTPError
from bravado.requests_client import RequestsResponseAdapter

from paasta_tools.cli.cmds.list_deploy_queue import list_deploy_queue
from tests.cli.test_cmds_status import Struct


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


@pytest.fixture
def mock_get_paasta_api_client():
    with mock.patch(
        "paasta_tools.cli.cmds.list_deploy_queue.get_paasta_api_client", autospec=True,
    ) as _mock_get_paasta_api_client:
        yield _mock_get_paasta_api_client


def test_list_deploy_queue(mock_get_paasta_api_client, capfd):
    args = mock.Mock()
    args.cluster = "westeros-prod"
    args.json = False

    mock_deploy_queues_response = Struct(
        available_service_instances=[
            Struct(
                service="service1",
                instance="instance1",
                watcher="watcher1",
                bounce_by=1578038400,
                wait_until=1578038400,
                enqueue_time=1578038400,
                bounce_start_time=1578038400,
                failures=0,
                processed_count=0,
            ),
        ],
        unavailable_service_instances=[
            Struct(
                service="service2",
                instance="instance2",
                watcher="watcher2",
                bounce_by=1577952000,
                wait_until=1577952000,
                enqueue_time=1577952000,
                bounce_start_time=1577952000,
                failures=5,
                processed_count=10,
            ),
        ],
    )
    mock_raw_response = Struct(text="abc")

    mock_api_client = mock_get_paasta_api_client.return_value
    mock_api_client.deploy_queue.deploy_queue.return_value.result.return_value = (
        mock_deploy_queues_response,
        mock_raw_response,
    )

    return_value = list_deploy_queue(args)

    assert return_value == 0
    stdout, stderr = capfd.readouterr()
    lines = stdout.split("\n")
    assert args.cluster in lines[0]
    assert "service1.instance1" in lines[3]
    assert "service2.instance2" in lines[6]


def test_list_deploy_queue_json(mock_get_paasta_api_client, capfd):
    args = mock.Mock()
    args.cluster = "westeros-prod"
    args.json = True

    mock_json_return = json.dumps(
        {"available_service_instances": [], "unavailable_service_instances": []}
    )
    mock_raw_response = Struct(text=mock_json_return)

    mock_api_client = mock_get_paasta_api_client.return_value
    mock_api_client.deploy_queue.deploy_queue.return_value.result.return_value = (
        Struct(),
        mock_raw_response,
    )

    return_value = list_deploy_queue(args)
    assert return_value == 0
    stdout, stderr = capfd.readouterr()
    assert stdout.strip() == mock_json_return


def test_http_error(mock_get_paasta_api_client):
    args = mock.Mock()
    args.cluster = "westeros-prod"

    mock_response = mock.Mock(status_code=500, text="Internal Server Error")
    response_adapter = RequestsResponseAdapter(mock_response)

    mock_api_client = mock_get_paasta_api_client.return_value
    mock_api_client.api_error = HTTPError
    mock_api_client.deploy_queue.deploy_queue.return_value.result.side_effect = HTTPError(
        response_adapter
    )

    assert list_deploy_queue(args) == 500
