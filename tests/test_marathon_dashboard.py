#!/usr/bin/env python
# Copyright 2017 Yelp Inc.
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

from paasta_tools import marathon_dashboard
from paasta_tools.marathon_tools import MarathonClients
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.utils import SystemPaastaConfig


@mock.patch("paasta_tools.marathon_dashboard.load_system_paasta_config", autospec=True)
def test_main(mock_load_system_paasta_config):
    soa_dir = "/fake/soa/dir"
    cluster = "fake_cluster"
    mock_load_system_paasta_config.return_value = SystemPaastaConfig(
        {}, "fake_directory"
    )
    with mock.patch(
        "paasta_tools.marathon_dashboard.create_marathon_dashboard",
        autospec=True,
        return_value={},
    ) as create_marathon_dashboard:
        marathon_dashboard.main(("--soa-dir", soa_dir, "--cluster", cluster))
        create_marathon_dashboard.assert_called_once_with(
            cluster=cluster, soa_dir=soa_dir
        )


@mock.patch("paasta_tools.marathon_dashboard.load_system_paasta_config", autospec=True)
@mock.patch("paasta_tools.marathon_dashboard.PaastaServiceConfigLoader", autospec=True)
@mock.patch("paasta_tools.marathon_dashboard.get_services_for_cluster", autospec=True)
def test_create_marathon_dashboard(
    mock_get_services_for_cluster, mock_pscl, mock_load_system_paasta_config
):
    soa_dir = "/fake/soa/dir"
    cluster = "fake_cluster"
    mock_load_system_paasta_config.return_value = SystemPaastaConfig(
        {"dashboard_links": {}}, "fake_directory"
    )
    mock_get_services_for_cluster.return_value = [
        ("fake_service", "foo"),
        ("fake_service", "bar"),
    ]
    mock_pscl.return_value.instance_configs.return_value = [
        MarathonServiceConfig("fake_service", "fake_cluster", "foo", {}, {}, soa_dir),
        MarathonServiceConfig("fake_service", "fake_cluster", "bar", {}, {}, soa_dir),
    ]

    mock_client = mock.Mock(servers=["hi"])
    mock_clients = MarathonClients(current=[mock_client], previous=[mock_client])

    expected_output = {
        "fake_cluster": [
            {"service": "fake_service", "instance": "foo", "shard_url": "hi"},
            {"service": "fake_service", "instance": "bar", "shard_url": "hi"},
        ]
    }
    assert (
        marathon_dashboard.create_marathon_dashboard(
            cluster=cluster, soa_dir=soa_dir, marathon_clients=mock_clients
        )
        == expected_output
    )
