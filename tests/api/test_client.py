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
import mock

from paasta_tools.api.client import get_paasta_api_client
from paasta_tools.cli.cmds.status import paasta_status_on_api_endpoint
from paasta_tools.utils import SystemPaastaConfig


def test_get_paasta_api_client():
    with mock.patch('paasta_tools.api.client.load_system_paasta_config',
                    autospec=True) as mock_load_system_paasta_config:
        mock_load_system_paasta_config.return_value = SystemPaastaConfig({
            'api_endpoints': {
                'fake_cluster': "http://fake_cluster:5054"
            },
            'cluster': 'fake_cluster'
        }, 'fake_directory')

        client = get_paasta_api_client()
        assert client


class Struct(object):
    """
    convert a dictionary to an object
    """

    def __init__(self, **entries):
        self.__dict__.update(entries)


def test_paasta_status():
    fake_dict = {"git_sha": "fake_git_sha", "instance": "fake_instance", "service": "fake_service"}
    fake_dict2 = {"error_message": None, "desired_state": "start",
                  "app_id": "fake_app_id", "app_count": 1,
                  "running_instance_count": 2, "expected_instance_count": 2,
                  "deploy_status": "Running", "bounce_method": "crossover"}
    fake_status_obj = Struct(**fake_dict)
    fake_status_obj.marathon = Struct(**fake_dict2)

    system_paasta_config = SystemPaastaConfig({
        'api_endpoints': {
            'fake_cluster': "http://fake_cluster:5054"
        },
        'cluster': 'fake_cluster'
    }, 'fake_directory')

    with mock.patch('bravado.http_future.HttpFuture.result', autospec=True) as mock_result:
        mock_result.return_value = fake_status_obj
        paasta_status_on_api_endpoint('fake_cluster', 'fake_service', 'fake_instance',
                                      system_paasta_config, verbose=False)
