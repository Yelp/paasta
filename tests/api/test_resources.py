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

from pyramid import testing

from paasta_tools.api.views.resources import parse_filters
from paasta_tools.api.views.resources import resources_utilization


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


def test_resources_utilization_nothing_special():
    request = testing.DummyRequest()
    request.swagger_data = {"groupings": None, "filter": None}

    # Since Mesos is removed, resources_utilization should return empty response
    resp = resources_utilization(request)
    body = json.loads(resp.body.decode("utf-8"))

    assert resp.status_int == 200
    assert len(body) == 0


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


def test_resources_utilization_with_grouping():
    request = testing.DummyRequest()
    request.swagger_data = {"groupings": ["region", "pool"], "filter": None}

    # Since Mesos is removed, resources_utilization should return empty response
    resp = resources_utilization(request)
    body = json.loads(resp.body.decode("utf-8"))

    assert resp.status_int == 200
    assert len(body) == 0


def test_resources_utilization_with_filter():
    request = testing.DummyRequest()
    request.swagger_data = {
        "groupings": ["region", "pool"],
        "filter": ["region:top", "pool:default,other"],
    }

    # Since Mesos is removed, resources_utilization should return empty response
    resp = resources_utilization(request)
    body = json.loads(resp.body.decode("utf-8"))

    assert resp.status_int == 200
    assert len(body) == 0

    request.swagger_data = {
        "groupings": ["region", "pool"],
        "filter": ["region:non-exist", "pool:default,other"],
    }
    resp = resources_utilization(request)
    body = json.loads(resp.body.decode("utf-8"))

    assert resp.status_int == 200
    assert len(body) == 0
