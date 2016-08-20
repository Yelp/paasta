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

import mock
import requests_mock

from paasta_tools.api.client import PaastaApiClient
from paasta_tools.cli.cmds.status import paasta_status_on_api_endpoint
from paasta_tools.utils import SystemPaastaConfig


@requests_mock.mock()
def test_list_instances(m):
    fake_response = '{"instances": ["main", "test"]}'
    m.get('http://fake_cluster:5054/v1/services/fake_service', text=fake_response)

    with mock.patch('paasta_tools.api.client.load_system_paasta_config',
                    autospec=True) as mock_load_system_paasta_config:
        mock_load_system_paasta_config.return_value = SystemPaastaConfig({
            'api_endpoints': {
                'fake_cluster': "http://fake_cluster:5054"
            },
            'cluster': 'fake_cluster'
        }, 'fake_directory')

        client = PaastaApiClient(cluster="fake_cluster")
        assert client.list_instances("fake_service") == ["main", "test"]


@requests_mock.mock()
def test_instance_status(m):
    fake_response = '{"service": "fake_service", "instance": "fake_instance"}'
    m.get('http://fake_cluster:5054/v1/services/fake_service/fake_instance/status', text=fake_response)

    with mock.patch('paasta_tools.api.client.load_system_paasta_config',
                    autospec=True) as mock_load_system_paasta_config:
        mock_load_system_paasta_config.return_value = SystemPaastaConfig({
            'api_endpoints': {
                'fake_cluster': "http://fake_cluster:5054"
            },
            'cluster': 'fake_cluster'
        }, 'fake_directory')

        client = PaastaApiClient(cluster="fake_cluster")
        assert client.instance_status("fake_service", "fake_instance") == json.loads(fake_response)


@requests_mock.mock()
def test_paasta_status(m):
    fake_response = '{"git_sha": "fake_git_sha", "instance": "fake_instance", "service": "fake_service",\
                      "marathon": {"desired_state": "start", "app_id": "fake_app_id",\
                                   "running_instance_count": 2, "expected_instance_count": 2,\
                                   "deploy_status": "Running", "app_count": 1, "bounce_method": "crossover"}}'
    m.get('http://fake_cluster:5054/v1/services/fake_service/fake_instance/status', text=fake_response)

    system_paasta_config = SystemPaastaConfig({
        'api_endpoints': {
            'fake_cluster': "http://fake_cluster:5054"
        },
        'cluster': 'fake_cluster'
    }, 'fake_directory')

    paasta_status_on_api_endpoint('fake_cluster', 'fake_service', 'fake_instance', system_paasta_config, verbose=False)
