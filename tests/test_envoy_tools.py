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

from paasta_tools.envoy_tools import are_services_up_in_pod
from paasta_tools.envoy_tools import ENVOY_DEFAULT_ENABLED
from paasta_tools.envoy_tools import ENVOY_DEFAULT_FULL_MESH
from paasta_tools.envoy_tools import ENVOY_FULL_MESH_CONFIG_NAMESPACE
from paasta_tools.envoy_tools import ENVOY_TOGGLES_CONFIG_NAMESPACE
from paasta_tools.envoy_tools import get_backends
from paasta_tools.envoy_tools import get_casper_endpoints
from paasta_tools.envoy_tools import get_frontends
from paasta_tools.envoy_tools import match_backends_and_tasks
from paasta_tools.envoy_tools import service_is_full_mesh
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


def test_service_is_full_mesh_no_config():
    assert (
        service_is_full_mesh("service.instance", config_file="does.not.exist")
        == ENVOY_DEFAULT_FULL_MESH
    )


def test_service_is_full_mesh_default():
    with MockConfiguration({}, namespace=ENVOY_FULL_MESH_CONFIG_NAMESPACE):
        assert service_is_full_mesh("service.instance") == ENVOY_DEFAULT_FULL_MESH


def test_service_is_full_mesh_false():
    with MockConfiguration(
        {"service.instance": False}, namespace=ENVOY_FULL_MESH_CONFIG_NAMESPACE
    ):
        assert not service_is_full_mesh("service.instance")


def test_service_is_full_mesh_true():
    with MockConfiguration(
        {"service.instance": True}, namespace=ENVOY_FULL_MESH_CONFIG_NAMESPACE
    ):
        assert service_is_full_mesh("service.instance")


class TestServicesUpInPod:
    host_ip = "10.1.1.1"
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
    def mock_get_multiple_clusters(self):
        with mock.patch(
            "paasta_tools.envoy_tools.get_multiple_clusters", autospec=True
        ) as mock_get_multiple_clusters:
            yield mock_get_multiple_clusters

    @pytest.fixture
    def enable_full_mesh(self, registrations):
        with MockConfiguration(
            {registration: True for registration in registrations},
            namespace=ENVOY_FULL_MESH_CONFIG_NAMESPACE,
        ):
            yield

    @pytest.fixture
    def mock_load_system_paasta_config(self, system_paasta_config):
        with mock.patch(
            "paasta_tools.envoy_tools.load_system_paasta_config", autospec=True
        ) as mock_load_system_paasta_config:
            mock_load_system_paasta_config.return_value = system_paasta_config
            yield

    def test_are_services_up_on_port_no_clusters(
        self, mock_load_system_paasta_config, mock_get_multiple_clusters
    ):
        assert not are_services_up_in_pod(
            envoy_host="1.2.3.4",
            envoy_admin_port=3212,
            registrations=["service1.instance1", "service1.instance2"],
            host_ip=self.host_ip,
            pod_ip=self.pod_ip,
            pod_port=self.pod_port,
        )

    def test_are_services_up_on_port_all_backends_healthy(
        self, mock_load_system_paasta_config, mock_get_multiple_clusters, cluster
    ):
        mock_get_multiple_clusters.return_value = [
            cluster("HEALTHY"),
            cluster("HEALTHY"),
            cluster("HEALTHY"),
        ]
        assert are_services_up_in_pod(
            envoy_host="1.2.3.4",
            envoy_admin_port=3212,
            registrations=["service1.instance1", "service1.instance2"],
            host_ip=self.host_ip,
            pod_ip=self.pod_ip,
            pod_port=self.pod_port,
        )

    def test_are_services_up_on_port_unhealthy_service(
        self, mock_load_system_paasta_config, mock_get_multiple_clusters, cluster
    ):
        mock_get_multiple_clusters.side_effect = [
            [
                # service1.instance1
                cluster("HEALTHY"),
                cluster("HEALTHY"),
                cluster("HEALTHY"),
            ],
            [
                # service1.instance2
                cluster("UNHEALTHY"),
                cluster("UNHEALTHY"),
                cluster("UNHEALTHY"),
            ],
        ]
        assert not are_services_up_in_pod(
            envoy_host="1.2.3.4",
            envoy_admin_port=3212,
            registrations=["service1.instance1", "service1.instance2"],
            host_ip=self.host_ip,
            pod_ip=self.pod_ip,
            pod_port=self.pod_port,
        )

    def test_are_services_up_on_port_partial_health_backend(
        self, mock_load_system_paasta_config, mock_get_multiple_clusters, cluster
    ):
        mock_get_multiple_clusters.return_value = [
            cluster("HEALTHY"),
            cluster("HEALTHY"),
            cluster("UNHEALTHY"),
        ]
        assert are_services_up_in_pod(
            envoy_host="1.2.3.4",
            envoy_admin_port=3212,
            registrations=["service1.instance1", "service1.instance2"],
            host_ip=self.host_ip,
            pod_ip=self.pod_ip,
            pod_port=self.pod_port,
        )

    def test_are_services_up_on_port_missing_backend(
        self, mock_load_system_paasta_config, mock_get_multiple_clusters, cluster
    ):
        mock_get_multiple_clusters.side_effect = [
            [cluster("HEALTHY"), cluster("HEALTHY"), cluster("HEALTHY")],
            [cluster("HEALTHY"), cluster("HEALTHY"), cluster("HEALTHY")],
            [],
        ]
        # all up and present but service1.instance3 not present
        assert not are_services_up_in_pod(
            envoy_host="1.2.3.4",
            envoy_admin_port=3212,
            registrations=[
                "service1.instance1",
                "service1.instance2",
                "service1.instance3",
            ],
            host_ip=self.host_ip,
            pod_ip=self.pod_ip,
            pod_port=self.pod_port,
        )

    @pytest.mark.parametrize(
        "registrations", [["service1.instance1", "service1.instance2"]]
    )
    def test_are_services_up_on_port_all_backends_healthy_no_frontend(
        self,
        mock_load_system_paasta_config,
        mock_get_multiple_clusters,
        cluster,
        enable_full_mesh,
    ):
        mock_get_multiple_clusters.side_effect = [
            [
                # service1.instance1 backennd
                cluster("HEALTHY", ip=self.host_ip),
                cluster("HEALTHY", ip=self.host_ip),
                cluster("HEALTHY", ip=self.host_ip),
            ],
            [
                # service1.instance1 frontend
                cluster("HEALTHY"),
            ],
            [
                # service1.instance2 backennd
                cluster("HEALTHY", ip=self.host_ip),
                cluster("HEALTHY", ip=self.host_ip),
                cluster("HEALTHY", ip=self.host_ip),
            ],
            [],  # service1.instance2 frontend
        ]

        assert not are_services_up_in_pod(
            envoy_host="1.2.3.4",
            envoy_admin_port=3212,
            registrations=["service1.instance1", "service1.instance2"],
            host_ip=self.host_ip,
            pod_ip=self.pod_ip,
            pod_port=self.pod_port,
        )

    @pytest.mark.parametrize(
        "registrations", [["service1.instance1", "service1.instance2"]]
    )
    def test_are_services_up_on_port_all_backends_and_frontends_healthy(
        self,
        mock_load_system_paasta_config,
        mock_get_multiple_clusters,
        cluster,
        enable_full_mesh,
    ):
        mock_get_multiple_clusters.side_effect = [
            [
                # service1.instance1 backennd
                cluster("HEALTHY", ip=self.host_ip),
                cluster("HEALTHY", ip=self.host_ip),
                cluster("HEALTHY", ip=self.host_ip),
            ],
            [
                # service1.instance1 frontend
                cluster("HEALTHY"),
            ],
            [
                # service1.instance2 backennd
                cluster("HEALTHY", ip=self.host_ip),
                cluster("HEALTHY", ip=self.host_ip),
                cluster("HEALTHY", ip=self.host_ip),
            ],
            [
                # service1.instance2 frontend
                cluster("HEALTHY"),
            ],
        ]

        assert are_services_up_in_pod(
            envoy_host="1.2.3.4",
            envoy_admin_port=3212,
            registrations=["service1.instance1", "service1.instance2"],
            host_ip=self.host_ip,
            pod_ip=self.pod_ip,
            pod_port=self.pod_port,
        )

    @pytest.mark.parametrize(
        "registrations", [["service1.instance1", "service1.instance2"]]
    )
    def test_are_services_up_on_port_all_backends_healthy_frontend_unhealthy(
        self,
        mock_load_system_paasta_config,
        mock_get_multiple_clusters,
        cluster,
        enable_full_mesh,
    ):
        mock_get_multiple_clusters.side_effect = [
            [
                # service1.instance1 backennd
                cluster("HEALTHY", ip=self.host_ip),
                cluster("HEALTHY", ip=self.host_ip),
                cluster("HEALTHY", ip=self.host_ip),
            ],
            [
                # service1.instance1 frontend
                cluster("UNHEALTHY"),
            ],
            [
                # service1.instance2 backennd
                cluster("HEALTHY", ip=self.host_ip),
                cluster("HEALTHY", ip=self.host_ip),
                cluster("HEALTHY", ip=self.host_ip),
            ],
            [
                # service1.instance2 frontend
                cluster("HEALTHY"),
            ],
        ]

        assert not are_services_up_in_pod(
            envoy_host="1.2.3.4",
            envoy_admin_port=3212,
            registrations=["service1.instance1", "service1.instance2"],
            host_ip=self.host_ip,
            pod_ip=self.pod_ip,
            pod_port=self.pod_port,
        )

    @pytest.mark.parametrize(
        "registrations", [["service1.instance1", "service1.instance2"]]
    )
    def test_are_services_up_on_port_unhealthy_backend_healthy_frontend(
        self,
        mock_load_system_paasta_config,
        mock_get_multiple_clusters,
        cluster,
        enable_full_mesh,
    ):
        mock_get_multiple_clusters.side_effect = [
            [
                # service1.instance1
                cluster("HEALTHY", ip=self.host_ip),
                cluster("HEALTHY", ip=self.host_ip),
                cluster("HEALTHY", ip=self.host_ip),
            ],
            [
                # service1.instance1 frontend
                cluster("HEALTHY"),
            ],
            [
                # service1.instance2
                cluster("UNHEALTHY", ip=self.host_ip),
                cluster("UNHEALTHY", ip=self.host_ip),
                cluster("UNHEALTHY", ip=self.host_ip),
            ],
            [
                # service1.instance2 frontend
                cluster("HEALTHY"),
            ],
        ]
        assert not are_services_up_in_pod(
            envoy_host="1.2.3.4",
            envoy_admin_port=3212,
            registrations=["service1.instance1", "service1.instance2"],
            host_ip=self.host_ip,
            pod_ip=self.pod_ip,
            pod_port=self.pod_port,
        )
