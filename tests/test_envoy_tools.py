# Copyright 2015-2019 Yelp Inc.
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
import os
import socket

import mock
import pytest
import requests
from staticconf.testing import MockConfiguration

from paasta_tools.envoy_tools import ENVOY_DEFAULT_ENABLED
from paasta_tools.envoy_tools import ENVOY_TOGGLES_CONFIG_NAMESPACE
from paasta_tools.envoy_tools import get_backends
from paasta_tools.envoy_tools import get_casper_endpoints
from paasta_tools.envoy_tools import get_frontends
from paasta_tools.envoy_tools import match_backends_and_tasks
from paasta_tools.envoy_tools import service_is_in_envoy


@pytest.fixture
def mock_paasta_tools_settings(system_paasta_config):
    with mock.patch(
        "paasta_tools.envoy_tools.settings", autospec=True
    ) as mock_settings:
        mock_settings.system_paasta_config = system_paasta_config
        yield


def test_get_backends(mock_paasta_tools_settings):
    testdir = os.path.dirname(os.path.realpath(__file__))
    testdata = os.path.join(testdir, "envoy_admin_clusters_snapshot.txt")
    with open(testdata, "r") as fd:
        mock_envoy_admin_clusters_data = json.load(fd)

    mock_response = mock.Mock()
    mock_response.json.return_value = mock_envoy_admin_clusters_data
    mock_get = mock.Mock(return_value=(mock_response))

    hosts = {
        "10.46.6.90": ("host2.two.com", None, None),
        "10.46.6.88": ("host3.three.com", None, None),
        "10.46.6.103": ("host4.four.com", None, None),
    }

    with mock.patch.object(requests.Session, "get", mock_get):
        with mock.patch(
            "socket.gethostbyaddr", side_effect=lambda x: hosts[x], autospec=True,
        ):
            expected = [
                (
                    {
                        "address": "10.46.6.88",
                        "port_value": 13833,
                        "hostname": "host3",
                        "eds_health_status": "HEALTHY",
                        "weight": 1,
                    },
                    False,
                ),
                (
                    {
                        "address": "10.46.6.90",
                        "port_value": 13833,
                        "hostname": "host2",
                        "eds_health_status": "HEALTHY",
                        "weight": 1,
                    },
                    False,
                ),
            ]
            assert expected == get_backends("service1.main", "host", 123)


def test_get_frontends(mock_paasta_tools_settings):
    testdir = os.path.dirname(os.path.realpath(__file__))
    testdata = os.path.join(testdir, "envoy_admin_clusters_snapshot.txt")
    with open(testdata, "r") as fd:
        mock_envoy_admin_clusters_data = json.load(fd)

    mock_response = mock.Mock()
    mock_response.json.return_value = mock_envoy_admin_clusters_data
    mock_get = mock.Mock(return_value=(mock_response))

    with mock.patch.object(requests.Session, "get", mock_get):
        with mock.patch(
            "socket.gethostbyaddr", side_effect=socket.herror, autospec=True,
        ):
            expected = [
                (
                    {
                        "address": "0.0.0.0",
                        "port_value": 8888,
                        "hostname": "0.0.0.0",
                        "eds_health_status": "HEALTHY",
                        "weight": 1,
                    },
                    False,
                ),
            ]
            assert expected == get_frontends("service1.main", "host", 123)


def test_get_casper_endpoints():
    testdir = os.path.dirname(os.path.realpath(__file__))
    testdata = os.path.join(testdir, "envoy_admin_clusters_snapshot.txt")
    with open(testdata, "r") as fd:
        mock_envoy_admin_clusters_data = json.load(fd)

    expected = frozenset([("10.46.6.106", 13819)])

    assert expected == get_casper_endpoints(mock_envoy_admin_clusters_data)


def test_match_backends_and_tasks():
    backends = [
        {
            "address": "10.50.2.4",
            "port_value": 31000,
            "eds_health_status": "HEALTHY",
            "weight": 1,
            "has_associated_task": False,
        },
        {
            "address": "10.50.2.5",
            "port_value": 31001,
            "eds_health_status": "HEALTHY",
            "weight": 1,
            "has_associated_task": False,
        },
        {
            "address": "10.50.2.6",
            "port_value": 31001,
            "eds_health_status": "HEALTHY",
            "weight": 1,
            "has_associated_task": False,
        },
        {
            "address": "10.50.2.6",
            "port_value": 31002,
            "eds_health_status": "HEALTHY",
            "weight": 1,
            "has_associated_task": False,
        },
        {
            "address": "10.50.2.8",
            "port_value": 31000,
            "eds_health_status": "HEALTHY",
            "weight": 1,
            "has_associated_task": False,
        },
    ]
    good_task1 = mock.Mock(host="box4", ports=[31000])
    good_task2 = mock.Mock(host="box5", ports=[31001])
    bad_task = mock.Mock(host="box7", ports=[31000])
    tasks = [good_task1, good_task2, bad_task]

    hostnames = {
        "box4": "10.50.2.4",
        "box5": "10.50.2.5",
        "box6": "10.50.2.6",
        "box7": "10.50.2.7",
        "box8": "10.50.2.8",
    }

    with mock.patch(
        "paasta_tools.envoy_tools.socket.gethostbyname",
        side_effect=lambda x: hostnames[x],
        autospec=True,
    ):
        expected = [
            (backends[0], good_task1),
            (backends[1], good_task2),
            (None, bad_task),
            (backends[2], None),
            (backends[3], None),
            (backends[4], None),
        ]
        actual = match_backends_and_tasks(backends, tasks)

        def keyfunc(t):
            return tuple(sorted((t[0] or {}).items())), t[1]

        assert sorted(actual, key=keyfunc) == sorted(expected, key=keyfunc)


def test_service_is_in_envoy_no_config():
    assert (
        service_is_in_envoy("service.instance", config_file="does.not.exist")
        == ENVOY_DEFAULT_ENABLED
    )


def test_service_is_in_envoy_default():
    with MockConfiguration({}, namespace=ENVOY_TOGGLES_CONFIG_NAMESPACE):
        assert service_is_in_envoy("service.instance") == ENVOY_DEFAULT_ENABLED


def test_service_is_in_envoy_false():
    with MockConfiguration(
        {"service.instance": False}, namespace=ENVOY_TOGGLES_CONFIG_NAMESPACE
    ):
        assert not service_is_in_envoy("service.instance")


def test_service_is_in_envoy_true():
    with MockConfiguration(
        {"service.instance": True}, namespace=ENVOY_TOGGLES_CONFIG_NAMESPACE
    ):
        assert service_is_in_envoy("service.instance")
