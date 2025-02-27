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

import mock
import pytest
import requests

from paasta_tools.envoy_tools import are_namespaces_up_in_eds
from paasta_tools.envoy_tools import are_services_up_in_pod
from paasta_tools.envoy_tools import get_backends
from paasta_tools.envoy_tools import get_backends_from_eds
from paasta_tools.envoy_tools import get_casper_endpoints
from paasta_tools.envoy_tools import match_backends_and_pods


def test_get_backends():
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
            "socket.gethostbyaddr",
            side_effect=lambda x: hosts[x],
            autospec=True,
        ):
            expected = {
                "service1.main": [
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
            }
            assert expected == get_backends("service1.main", "host", 123, "something")


def test_get_casper_endpoints():
    testdir = os.path.dirname(os.path.realpath(__file__))
    testdata = os.path.join(testdir, "envoy_admin_clusters_snapshot.txt")
    with open(testdata, "r") as fd:
        mock_envoy_admin_clusters_data = json.load(fd)

    expected = frozenset([("10.46.6.106", 13819)])

    assert expected == get_casper_endpoints(mock_envoy_admin_clusters_data)


@pytest.fixture
def mock_backends():
    return [
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


def test_match_backends_and_pods(mock_backends):
    backends = mock_backends

    good_pod_1 = mock.Mock(status=mock.Mock(pod_ip="10.50.2.4"))
    good_pod_2 = mock.Mock(status=mock.Mock(pod_ip="10.50.2.5"))
    bad_pod_1 = mock.Mock(status=mock.Mock(pod_ip="10.50.2.10"))
    pods = [good_pod_1, good_pod_2, bad_pod_1]

    expected = [
        (backends[0], good_pod_1),
        (backends[1], good_pod_2),
        (None, bad_pod_1),
        (backends[2], None),
        (backends[3], None),
        (backends[4], None),
    ]
    actual = match_backends_and_pods(backends, pods)

    def keyfunc(t):
        sorted_backend = tuple(sorted((t[0] or {}).items()))
        pod_ip = t[1].status.pod_ip if t[1] else ""
        return sorted_backend, pod_ip

    assert sorted(actual, key=keyfunc) == sorted(expected, key=keyfunc)


class TestServicesUpInPod:
    pod_ip = "10.40.1.1"
    pod_port = 8888

    @pytest.fixture
    def cluster(self):
        def _make_cluster(health, ip=self.pod_ip):
            return (
                {
                    "eds_health_status": health,
                    "address": ip,
                    "port_value": self.pod_port,
                },
                False,
            )

        return _make_cluster

    @pytest.fixture
    def mock_get_multiple_backends(self):
        with mock.patch(
            "paasta_tools.envoy_tools.get_multiple_backends", autospec=True
        ) as mock_get_multiple_backends:
            yield mock_get_multiple_backends

    def test_are_services_up_on_port_no_clusters(self, mock_get_multiple_backends):
        mock_get_multiple_backends.return_value = {}
        assert not are_services_up_in_pod(
            envoy_host="1.2.3.4",
            envoy_admin_port=3212,
            envoy_admin_endpoint_format="http://{bla}:{more_bla}",
            registrations=["service1.instance1", "service1.instance2"],
            pod_ip=self.pod_ip,
            pod_port=self.pod_port,
        )

    def test_are_services_up_on_port_all_backends_healthy(
        self, mock_get_multiple_backends, cluster
    ):
        mock_get_multiple_backends.side_effect = [
            {
                "service1.instance1": [
                    cluster("HEALTHY"),
                    cluster("HEALTHY"),
                    cluster("HEALTHY"),
                ]
            },
            {
                "service1.instance2": [
                    cluster("HEALTHY"),
                    cluster("HEALTHY"),
                    cluster("HEALTHY"),
                ]
            },
        ]
        assert are_services_up_in_pod(
            envoy_host="1.2.3.4",
            envoy_admin_port=3212,
            envoy_admin_endpoint_format="http://{bla}:{more_bla}",
            registrations=["service1.instance1", "service1.instance2"],
            pod_ip=self.pod_ip,
            pod_port=self.pod_port,
        )

    def test_are_services_up_on_port_unhealthy_service(
        self, mock_get_multiple_backends, cluster
    ):
        mock_get_multiple_backends.side_effect = [
            {
                "service1.instance1": [
                    cluster("HEALTHY"),
                    cluster("HEALTHY"),
                    cluster("HEALTHY"),
                ]
            },
            {
                "service1.instance2": [
                    cluster("UNHEALTHY"),
                    cluster("UNHEALTHY"),
                    cluster("UNHEALTHY"),
                ]
            },
        ]
        assert not are_services_up_in_pod(
            envoy_host="1.2.3.4",
            envoy_admin_port=3212,
            envoy_admin_endpoint_format="http://{bla}:{more_bla}",
            registrations=["service1.instance1", "service1.instance2"],
            pod_ip=self.pod_ip,
            pod_port=self.pod_port,
        )

    def test_are_services_up_on_port_partial_health_backend(
        self, mock_get_multiple_backends, cluster
    ):
        mock_get_multiple_backends.return_value = [
            cluster("HEALTHY"),
            cluster("HEALTHY"),
            cluster("UNHEALTHY"),
        ]
        mock_get_multiple_backends.side_effect = [
            {
                "service1.instance1": [
                    cluster("HEALTHY"),
                    cluster("HEALTHY"),
                    cluster("UNHEALTHY"),
                ]
            },
            {
                "service1.instance2": [
                    cluster("HEALTHY"),
                    cluster("UNHEALTHY"),
                    cluster("HEALTHY"),
                ]
            },
        ]
        assert are_services_up_in_pod(
            envoy_host="1.2.3.4",
            envoy_admin_port=3212,
            envoy_admin_endpoint_format="http://{bla}:{more_bla}",
            registrations=["service1.instance1", "service1.instance2"],
            pod_ip=self.pod_ip,
            pod_port=self.pod_port,
        )

    def test_are_services_up_on_port_missing_backend(
        self, mock_get_multiple_backends, cluster
    ):
        mock_get_multiple_backends.side_effect = [
            [cluster("HEALTHY"), cluster("HEALTHY"), cluster("HEALTHY")],
            [cluster("HEALTHY"), cluster("HEALTHY"), cluster("HEALTHY")],
            [],
        ]
        mock_get_multiple_backends.side_effect = [
            {
                "service1.instance1": [
                    cluster("HEALTHY"),
                    cluster("HEALTHY"),
                    cluster("HEALTHY"),
                ]
            },
            {
                "service1.instance2": [
                    cluster("HEALTHY"),
                    cluster("UNHEALTHY"),
                    cluster("HEALTHY"),
                ]
            },
            {},
        ]
        # all up and present but service1.instance3 not present
        assert not are_services_up_in_pod(
            envoy_host="1.2.3.4",
            envoy_admin_port=3212,
            envoy_admin_endpoint_format="http://{bla}:{more_bla}",
            registrations=[
                "service1.instance1",
                "service1.instance2",
                "service1.instance3",
            ],
            pod_ip=self.pod_ip,
            pod_port=self.pod_port,
        )

    @mock.patch("paasta_tools.envoy_tools.open", autospec=False)
    @mock.patch("paasta_tools.envoy_tools.os.access", autospec=True)
    @mock.patch("paasta_tools.envoy_tools.yaml.safe_load", autospec=True)
    def test_get_backends_from_eds(self, mock_yaml, mock_os_access, mock_open):

        mock_yaml.return_value = {"resources": [{"endpoints": None}]}
        backends = get_backends_from_eds("my-namespace", "/var/bla")
        assert len(backends) == 0

        mock_yaml.return_value = {
            "resources": [
                {
                    "endpoints": [
                        {
                            "priority": 0,
                            "lb_endpoints": [
                                {
                                    "endpoint": {
                                        "address": {
                                            "socket_address": {
                                                "address": "1.2.3.4",
                                                "port_value": 5555,
                                            }
                                        }
                                    }
                                },
                                {
                                    "endpoint": {
                                        "address": {
                                            "socket_address": {
                                                "address": "5.6.7.8",
                                                "port_value": 5555,
                                            }
                                        }
                                    }
                                },
                            ],
                        },
                        {
                            "priority": 1,
                            "lb_endpoints": [
                                {
                                    "endpoint": {
                                        "address": {
                                            "socket_address": {
                                                "address": "9.10.11.12",
                                                "port_value": 5555,
                                            }
                                        }
                                    }
                                },
                                {
                                    "endpoint": {
                                        "address": {
                                            "socket_address": {
                                                "address": "13.14.15.16",
                                                "port_value": 5555,
                                            }
                                        }
                                    }
                                },
                            ],
                        },
                    ]
                }
            ]
        }
        expected_backends = [
            ("1.2.3.4", 5555),
            ("5.6.7.8", 5555),
            ("9.10.11.12", 5555),
            ("13.14.15.16", 5555),
        ]
        backends = get_backends_from_eds("my-namespace", "/var/bla")
        assert sorted(backends) == sorted(expected_backends)

    @mock.patch("paasta_tools.envoy_tools.get_backends_from_eds", autospec=True)
    def test_are_namespaces_up_in_eds(self, mock_get_backends_from_eds):
        # No backends for service1.instance1 and service1.instance2
        mock_get_backends_from_eds.return_value = []
        assert not are_namespaces_up_in_eds(
            envoy_eds_path="/eds/path",
            namespaces=["service1.instance1", "service1.instance2"],
            pod_ip="1.2.3.4",
            pod_port=50000,
        )

        # No backends for service1.instance2
        mock_get_backends_from_eds.side_effect = [
            [("1.2.3.4", 50000), ("5.6.7.8", 60000)],
            [],
        ]
        assert not are_namespaces_up_in_eds(
            envoy_eds_path="/eds/path",
            namespaces=["service1.instance1", "service1.instance2"],
            pod_ip="1.2.3.4",
            pod_port=50000,
        )

        # Missing backend for service1.instance2
        mock_get_backends_from_eds.side_effect = [
            [("1.2.3.4", 50000), ("5.6.7.8", 60000)],
            [("5.6.7.8", 60000)],
        ]
        assert not are_namespaces_up_in_eds(
            envoy_eds_path="/eds/path",
            namespaces=["service1.instance1", "service1.instance2"],
            pod_ip="1.2.3.4",
            pod_port=50000,
        )

        # Backends returned for both namespaces
        mock_get_backends_from_eds.side_effect = [
            [("1.2.3.4", 50000), ("5.6.7.8", 60000)],
            [("5.6.7.8", 60000), ("1.2.3.4", 50000)],
        ]
        assert are_namespaces_up_in_eds(
            envoy_eds_path="/eds/path",
            namespaces=["service1.instance1", "service1.instance2"],
            pod_ip="1.2.3.4",
            pod_port=50000,
        )
