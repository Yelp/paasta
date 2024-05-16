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
import os

import mock
import pytest
import requests

from paasta_tools import smartstack_tools
from paasta_tools.smartstack_tools import backend_is_up
from paasta_tools.smartstack_tools import DiscoveredHost
from paasta_tools.smartstack_tools import get_replication_for_services
from paasta_tools.smartstack_tools import ip_port_hostname_from_svname
from paasta_tools.smartstack_tools import match_backends_and_pods
from paasta_tools.utils import DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT


def test_load_smartstack_info_for_service(system_paasta_config):
    with mock.patch(
        "paasta_tools.smartstack_tools.long_running_service_tools.load_service_namespace_config",
        autospec=True,
    ), mock.patch(
        "paasta_tools.smartstack_tools.get_smartstack_replication_for_attribute",
        autospec=True,
    ):
        # just a smoke test for now.
        smartstack_tools.load_smartstack_info_for_service(
            service="service",
            namespace="namespace",
            soa_dir="fake",
            blacklist=[],
            system_paasta_config=system_paasta_config,
        )


def test_get_smartstack_replication_for_attribute(system_paasta_config):
    fake_namespace = "fake_main"
    fake_service = "fake_service"
    mock_filtered_slaves = [
        {"hostname": "hostone", "attributes": {"fake_attribute": "foo"}},
        {"hostname": "hostone", "attributes": {"fake_attribute": "bar"}},
    ]

    with mock.patch(
        "paasta_tools.mesos_tools.get_all_slaves_for_blacklist_whitelist",
        return_value=mock_filtered_slaves,
        autospec=True,
    ) as mock_get_all_slaves_for_blacklist_whitelist, mock.patch(
        "paasta_tools.smartstack_tools.get_replication_for_services",
        return_value={},
        autospec=True,
    ) as mock_get_replication_for_services:
        expected = {"foo": {}, "bar": {}}
        actual = smartstack_tools.get_smartstack_replication_for_attribute(
            attribute="fake_attribute",
            service=fake_service,
            namespace=fake_namespace,
            blacklist=[],
            system_paasta_config=system_paasta_config,
        )
        mock_get_all_slaves_for_blacklist_whitelist.assert_called_once_with(
            blacklist=[], whitelist=None
        )
        assert actual == expected
        assert mock_get_replication_for_services.call_count == 2

        mock_get_replication_for_services.assert_any_call(
            synapse_host="hostone",
            synapse_port=system_paasta_config.get_synapse_port(),
            synapse_haproxy_url_format=system_paasta_config.get_synapse_haproxy_url_format(),
            services=["fake_service.fake_main"],
        )


def test_get_replication_for_service():
    testdir = os.path.dirname(os.path.realpath(__file__))
    testdata = os.path.join(testdir, "haproxy_snapshot.txt")
    with open(testdata, "r") as fd:
        mock_haproxy_data = fd.read()

    mock_response = mock.Mock()
    mock_response.text = mock_haproxy_data
    mock_get = mock.Mock(return_value=(mock_response))

    with mock.patch.object(requests.Session, "get", mock_get):
        replication_result = get_replication_for_services(
            "fake_host",
            6666,
            DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
            ["service1", "service2", "service3", "service4"],
        )
        expected = {"service1": 18, "service2": 19, "service3": 0, "service4": 3}
        assert expected == replication_result


def test_backend_is_up():
    assert True is backend_is_up({"status": "UP"})
    assert True is backend_is_up({"status": "UP 1/2"})
    assert False is backend_is_up({"status": "DOWN"})
    assert False is backend_is_up({"status": "DOWN 1/2"})
    assert False is backend_is_up({"status": "MAINT"})


def test_ip_port_hostname_from_svname_new_format():
    assert ("10.40.10.155", 31219, "myhost") == ip_port_hostname_from_svname(
        "myhost_10.40.10.155:31219"
    )


def test_ip_port_hostname_from_svname_old_format():
    assert ("10.85.5.101", 3744, "myhost") == ip_port_hostname_from_svname(
        "10.85.5.101:3744_myhost"
    )


def test_match_backends_and_pods():
    backends = [
        {
            "pxname": "servicename.main",
            "svname": "10.50.2.4:31000_box4",
            "status": "UP",
        },
        {
            "pxname": "servicename.main",
            "svname": "10.50.2.5:31000_box5",
            "status": "UP",
        },
        {
            "pxname": "servicename.main",
            "svname": "10.50.2.6:31000_box6",
            "status": "UP",
        },
        {
            "pxname": "servicename.main",
            "svname": "10.50.2.8:31000_box8",
            "status": "UP",
        },
    ]

    good_pod1 = mock.Mock(status=mock.Mock(pod_ip="10.50.2.4"))
    good_pod2 = mock.Mock(status=mock.Mock(pod_ip="10.50.2.5"))
    bad_pod = mock.Mock(status=mock.Mock(pod_ip="10.50.2.7"))
    pods = [good_pod1, good_pod2, bad_pod]

    expected = [
        (backends[0], good_pod1),
        (backends[1], good_pod2),
        (None, bad_pod),
        (backends[2], None),
        (backends[3], None),
    ]
    actual = match_backends_and_pods(backends, pods)

    def keyfunc(t):
        return tuple(sorted((t[0] or {}).items())), t[1]

    assert sorted(actual, key=keyfunc) == sorted(expected, key=keyfunc)


@mock.patch("paasta_tools.smartstack_tools.get_multiple_backends", autospec=True)
def test_get_replication_for_all_services(mock_get_multiple_backends):
    mock_get_multiple_backends.return_value = [
        {
            "pxname": "servicename.main",
            "svname": "10.50.2.4:31000_box4",
            "status": "UP",
        },
        {
            "pxname": "servicename.main",
            "svname": "10.50.2.5:31001_box5",
            "status": "UP",
        },
        {
            "pxname": "servicename.main",
            "svname": "10.50.2.6:31001_box6",
            "status": "UP",
        },
        {
            "pxname": "servicename.main",
            "svname": "10.50.2.6:31002_box7",
            "status": "UP",
        },
        {
            "pxname": "servicename.main",
            "svname": "10.50.2.8:31000_box8",
            "status": "UP",
        },
    ]
    assert {"servicename.main": 5} == smartstack_tools.get_replication_for_all_services(
        "", 8888, ""
    )


def test_are_services_up_on_port():
    with mock.patch(
        "paasta_tools.smartstack_tools.get_multiple_backends", autospec=True
    ) as mock_get_multiple_backends, mock.patch(
        "paasta_tools.smartstack_tools.ip_port_hostname_from_svname", autospec=True
    ) as mock_ip_port_hostname_from_svname, mock.patch(
        "paasta_tools.smartstack_tools.backend_is_up", autospec=True
    ) as mock_backend_is_up:
        # none present
        assert not smartstack_tools.are_services_up_on_ip_port(
            synapse_host="1.2.3.4",
            synapse_port=3212,
            synapse_haproxy_url_format="thing",
            services=["service1.instance1", "service1.instance2"],
            host_ip="10.1.1.1",
            host_port=8888,
        )

        mock_get_multiple_backends.return_value = [
            {"svname": "thing1", "pxname": "service1.instance1"},
            {"svname": "thing2", "pxname": "service1.instance2"},
            {"svname": "thing3", "pxname": "service1.instance2"},
        ]
        mock_ip_port_hostname_from_svname.return_value = ("10.1.1.1", 8888, "a")
        mock_backend_is_up.return_value = True
        # all backends present and up
        assert smartstack_tools.are_services_up_on_ip_port(
            synapse_host="1.2.3.4",
            synapse_port=3212,
            synapse_haproxy_url_format="thing",
            services=["service1.instance1", "service1.instance2"],
            host_ip="10.1.1.1",
            host_port=8888,
        )

        mock_get_multiple_backends.return_value = [
            {"svname": "thing1", "pxname": "service1.instance1"},
            {"svname": "thing2", "pxname": "service1.instance2"},
            {"svname": "thing3", "pxname": "service1.instance2"},
        ]
        mock_ip_port_hostname_from_svname.return_value = ("10.1.1.1", 8888, "a")
        mock_backend_is_up.side_effect = [True, False, False]
        # all present bu both of service1.instance2 are DOWN
        assert not smartstack_tools.are_services_up_on_ip_port(
            synapse_host="1.2.3.4",
            synapse_port=3212,
            synapse_haproxy_url_format="thing",
            services=["service1.instance1", "service1.instance2"],
            host_ip="10.1.1.1",
            host_port=8888,
        )

        mock_get_multiple_backends.return_value = [
            {"svname": "thing1", "pxname": "service1.instance1"},
            {"svname": "thing2", "pxname": "service1.instance2"},
            {"svname": "thing3", "pxname": "service1.instance2"},
        ]
        mock_ip_port_hostname_from_svname.return_value = ("10.1.1.1", 8888, "a")
        mock_backend_is_up.side_effect = [True, True, False]
        # all present but 1 of service1.instance2 is UP
        assert smartstack_tools.are_services_up_on_ip_port(
            synapse_host="1.2.3.4",
            synapse_port=3212,
            synapse_haproxy_url_format="thing",
            services=["service1.instance1", "service1.instance2"],
            host_ip="10.1.1.1",
            host_port=8888,
        )

        mock_get_multiple_backends.return_value = [
            {"svname": "thing1", "pxname": "service1.instance1"},
            {"svname": "thing2", "pxname": "service1.instance2"},
            {"svname": "thing3", "pxname": "service1.instance2"},
        ]
        mock_ip_port_hostname_from_svname.return_value = ("10.1.1.1", 8888, "a")
        mock_backend_is_up.return_value = True
        mock_backend_is_up.side_effect = None
        # all up and present but service1.instance3 not present
        assert not smartstack_tools.are_services_up_on_ip_port(
            synapse_host="1.2.3.4",
            synapse_port=3212,
            synapse_haproxy_url_format="thing",
            services=["service1.instance1", "service1.instance2", "service1.instance3"],
            host_ip="10.1.1.1",
            host_port=8888,
        )


@pytest.fixture
def mock_replication_checker():
    smartstack_tools.BaseReplicationChecker.__abstractmethods__ = frozenset()
    system_paasta_config = mock.Mock()
    return smartstack_tools.BaseReplicationChecker(
        system_paasta_config=system_paasta_config,
        service_discovery_providers=[
            smartstack_tools.SmartstackServiceDiscovery(
                system_paasta_config=system_paasta_config
            )
        ],
    )


@pytest.fixture
def mock_kube_replication_checker():
    mock_nodes = [mock.Mock()]
    mock_system_paasta_config = mock.Mock()
    mock_system_paasta_config.get_service_discovery_providers.return_value = {
        "smartstack": {},
        "envoy": {},
    }
    return smartstack_tools.KubeSmartstackEnvoyReplicationChecker(
        nodes=mock_nodes,
        system_paasta_config=mock_system_paasta_config,
    )


def test_kube_get_allowed_locations_and_hosts(mock_kube_replication_checker):
    with mock.patch(
        "paasta_tools.kubernetes_tools.load_service_namespace_config", autospec=True
    ) as mock_load_service_namespace_config, mock.patch(
        "paasta_tools.kubernetes_tools.get_nodes_grouped_by_attribute", autospec=True
    ) as mock_get_nodes_grouped_by_attribute:
        mock_instance_config = mock.Mock(
            service="blah", instance="foo", soa_dir="/nail/thing"
        )
        mock_node_1 = mock.MagicMock(
            metadata=mock.MagicMock(
                labels={"yelp.com/hostname": "foo1", "yelp.com/pool": "default"}
            )
        )
        mock_node_2 = mock.MagicMock(
            metadata=mock.MagicMock(
                labels={"yelp.com/hostname": "foo2", "yelp.com/pool": "default"}
            )
        )
        mock_load_service_namespace_config.return_value = mock.Mock(
            get_discover=mock.Mock(return_value="region")
        )
        mock_get_nodes_grouped_by_attribute.return_value = {
            "us-west-1": [mock_node_1, mock_node_2]
        }
        ret = mock_kube_replication_checker.get_allowed_locations_and_hosts(
            mock_instance_config
        )
        assert ret == {
            "us-west-1": [
                DiscoveredHost(hostname="foo1", pool="default"),
                DiscoveredHost(hostname="foo2", pool="default"),
            ]
        }


def test_get_allowed_locations_and_hosts(mock_replication_checker):
    mock_replication_checker.get_allowed_locations_and_hosts(
        instance_config=mock.Mock()
    )


def test_get_replication_for_instance(mock_replication_checker):
    with mock.patch(
        "paasta_tools.smartstack_tools.BaseReplicationChecker.get_allowed_locations_and_hosts",
        autospec=True,
    ) as mock_get_allowed_locations_and_hosts, mock.patch(
        "paasta_tools.smartstack_tools.BaseReplicationChecker.get_hostnames_in_pool",
        autospec=True,
    ) as mock_get_hostnames_in_pool, mock.patch(
        "paasta_tools.smartstack_tools.BaseReplicationChecker._get_replication_info",
        autospec=True,
    ) as mock_get_replication_info:
        mock_get_hostnames_in_pool.return_value = [mock.Mock()]
        mock_get_allowed_locations_and_hosts.return_value = {
            "westeros-prod": [mock.Mock()],
            "middleearth-prod": [mock.Mock(), mock.Mock()],
        }
        expected = {
            "Smartstack": {
                "westeros-prod": mock_get_replication_info.return_value,
                "middleearth-prod": mock_get_replication_info.return_value,
            }
        }
        assert (
            mock_replication_checker.get_replication_for_instance(mock.Mock())
            == expected
        )
        assert mock_get_hostnames_in_pool.call_count == 2


def test_get_first_host_in_pool(mock_replication_checker):
    mock_host_0 = mock.Mock(hostname="host0", pool="some")
    mock_host_1 = mock.Mock(hostname="host123", pool="default")
    mock_host_2 = mock.Mock(hostname="host456", pool="default")
    mock_host_3 = mock.Mock(hostname="host789", pool="special")
    mock_hosts = [mock_host_0, mock_host_1, mock_host_2, mock_host_3]
    ret = mock_replication_checker.get_first_host_in_pool(mock_hosts, "default")
    assert ret == "host123"
    ret = mock_replication_checker.get_first_host_in_pool(mock_hosts, "special")
    assert ret == "host789"
    ret = mock_replication_checker.get_first_host_in_pool(mock_hosts, "what")
    assert ret == "host0"


def test_hostnames_in_pool(mock_replication_checker):
    mock_host_0 = mock.Mock(hostname="host0", pool="some")
    mock_host_1 = mock.Mock(hostname="host123", pool="default")
    mock_host_2 = mock.Mock(hostname="host456", pool="default")
    mock_host_3 = mock.Mock(hostname="host789", pool="special")
    mock_hosts = [mock_host_0, mock_host_1, mock_host_2, mock_host_3]
    ret = mock_replication_checker.get_hostnames_in_pool(mock_hosts, "default")
    assert ret == ["host123", "host456"]
    ret = mock_replication_checker.get_hostnames_in_pool(mock_hosts, "special")
    assert ret == ["host789"]
    ret = mock_replication_checker.get_hostnames_in_pool(mock_hosts, "what")
    assert ret == ["host0"]


def test_get_replication_info(mock_replication_checker):
    mock_replication_checker._cache = {}
    mock_instance_config = mock.Mock(service="thing", instance="main")
    mock_service_discovery_provider = mock.MagicMock()
    ret = mock_replication_checker._get_replication_info(
        "westeros-prod", "host1", mock_instance_config, mock_service_discovery_provider
    )
    assert ret == {
        "thing.main": mock_service_discovery_provider.get_replication_for_all_services.return_value[
            "thing.main"
        ]
    }
    assert mock_replication_checker._cache == {
        (
            "westeros-prod",
            mock_service_discovery_provider.NAME,
        ): mock_service_discovery_provider.get_replication_for_all_services.return_value
    }
