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
import json
from unittest import mock

import asynctest
from pyramid import testing

from paasta_tools.api.views.resources import parse_filters
from paasta_tools.api.views.resources import resources_utilization
from paasta_tools.metrics import metastatus_lib


def test_parse_filters_empty():
    filters = None
    parsed = parse_filters(filters)

    assert parsed == {}


def test_parse_filters_good():
    filters = ["foo:bar,baz", "qux:zol"]

    parsed = parse_filters(filters)

    assert "foo" in parsed.keys()
    assert "qux" in parsed.keys()
    assert "bar" in parsed["foo"]
    assert "baz" in parsed["foo"]
    assert "zol" in parsed["qux"]


@mock.patch(
    "paasta_tools.api.views.resources.metastatus_lib.get_resource_utilization_by_grouping",
    autospec=True,
)
@mock.patch("paasta_tools.api.views.resources.get_mesos_master", autospec=True)
def test_resources_utilization_nothing_special(
    mock_get_mesos_master, mock_get_resource_utilization_by_grouping
):
    request = testing.DummyRequest()
    request.swagger_data = {"groupings": None, "filter": None}
    mock_mesos_state = mock.Mock()
    mock_master = mock.Mock(
        state=asynctest.CoroutineMock(return_value=mock_mesos_state)
    )
    mock_get_mesos_master.return_value = mock_master

    mock_get_resource_utilization_by_grouping.return_value = {
        frozenset([("superregion", "unknown")]): {
            "total": metastatus_lib.ResourceInfo(cpus=10.0, mem=512.0, disk=100.0),
            "free": metastatus_lib.ResourceInfo(cpus=8.0, mem=312.0, disk=20.0),
        }
    }

    resp = resources_utilization(request)
    body = json.loads(resp.body.decode("utf-8"))

    assert resp.status_int == 200
    assert len(body) == 1
    assert set(body[0].keys()) == {"disk", "mem", "groupings", "cpus", "gpus"}


mock_mesos_state = {
    "slaves": [
        {
            "id": "foo1",
            "resources": {"disk": 100, "cpus": 10, "mem": 50},
            "attributes": {"pool": "default", "region": "top"},
            "reserved_resources": {},
        },
        {
            "id": "bar1",
            "resources": {"disk": 100, "cpus": 10, "mem": 50},
            "attributes": {"pool": "default", "region": "bottom"},
            "reserved_resources": {},
        },
        {
            "id": "foo2",
            "resources": {"disk": 100, "cpus": 10, "mem": 50},
            "attributes": {"pool": "other", "region": "top"},
            "reserved_resources": {},
        },
        {
            "id": "bar2",
            "resources": {"disk": 100, "cpus": 10, "mem": 50},
            "attributes": {"pool": "other", "region": "bottom"},
            "reserved_resources": {},
        },
        {
            "id": "foo3",
            "resources": {"disk": 100, "cpus": 10, "mem": 50},
            "attributes": {"pool": "other", "region": "top"},
            "reserved_resources": {},
        },
        {
            "id": "bar2",
            "resources": {"disk": 100, "cpus": 10, "mem": 50},
            "attributes": {"pool": "other", "region": "bottom"},
            "reserved_resources": {},
        },
    ],
    "frameworks": [
        {
            "tasks": [
                {
                    "state": "TASK_RUNNING",
                    "resources": {"cpus": 1, "mem": 10, "disk": 10},
                    "slave_id": "foo1",
                },
                {
                    "state": "TASK_RUNNING",
                    "resources": {"cpus": 1, "mem": 10, "disk": 10},
                    "slave_id": "bar1",
                },
            ]
        }
    ],
}


@mock.patch("paasta_tools.api.views.resources.get_mesos_master", autospec=True)
def test_resources_utilization_with_grouping(mock_get_mesos_master):
    request = testing.DummyRequest()
    request.swagger_data = {"groupings": ["region", "pool"], "filter": None}
    mock_master = mock.Mock(
        state=asynctest.CoroutineMock(
            func=asynctest.CoroutineMock(),  # https://github.com/notion/a_sync/pull/40
            return_value=mock_mesos_state,
        )
    )
    mock_get_mesos_master.return_value = mock_master

    resp = resources_utilization(request)
    body = json.loads(resp.body.decode("utf-8"))

    assert resp.status_int == 200
    # 4 groupings, 2x2 attrs for 5 slaves
    assert len(body) == 4


@mock.patch("paasta_tools.api.views.resources.get_mesos_master", autospec=True)
def test_resources_utilization_with_filter(mock_get_mesos_master):
    request = testing.DummyRequest()
    request.swagger_data = {
        "groupings": ["region", "pool"],
        "filter": ["region:top", "pool:default,other"],
    }
    mock_master = mock.Mock(
        state=asynctest.CoroutineMock(
            func=asynctest.CoroutineMock(),  # https://github.com/notion/a_sync/pull/40
            return_value=mock_mesos_state,
        )
    )
    mock_get_mesos_master.return_value = mock_master

    resp = resources_utilization(request)
    body = json.loads(resp.body.decode("utf-8"))

    assert resp.status_int == 200
    assert len(body) == 2

    request.swagger_data = {
        "groupings": ["region", "pool"],
        "filter": ["region:non-exist", "pool:default,other"],
    }
    resp = resources_utilization(request)
    body = json.loads(resp.body.decode("utf-8"))

    assert resp.status_int == 200
    assert len(body) == 0
