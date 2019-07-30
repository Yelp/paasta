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
from pyramid import testing

from paasta_tools import marathon_tools
from paasta_tools.api import settings
from paasta_tools.api.views.marathon_dashboard import marathon_dashboard
from paasta_tools.utils import SystemPaastaConfig


def test_list_instances():
    settings.cluster = "fake_cluster"
    system_paasta_config_dict = {
        "marathon_servers": [
            {
                "user": "fake_user",
                "password": "fake_password",
                "url": ["http://marathon:8080"],
            },
            {
                "user": "fake_user",
                "password": "fake_password",
                "url": ["http://marathon1:8080"],
            },
            {
                "user": "fake_user",
                "password": "fake_password",
                "url": ["http://marathon2:8080"],
            },
        ],
        "dashboard_links": {
            "testcluster": {
                "Marathon RO": [
                    "http://accessible-marathon",
                    "http://accessible-marathon1",
                    "http://accessible-marathon2",
                ]
            }
        },
    }
    system_paasta_config = SystemPaastaConfig(
        config=system_paasta_config_dict, directory="unused"
    )
    marathon_servers = marathon_tools.get_marathon_servers(system_paasta_config)
    settings.marathon_clients = marathon_tools.get_marathon_clients(
        marathon_servers=marathon_servers, cached=False
    )
    request = testing.DummyRequest()

    settings.system_paasta_config = system_paasta_config
    response = marathon_dashboard(request)
    expected_output = {settings.cluster: []}
    assert response == expected_output
