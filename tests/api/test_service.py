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
from pyramid import testing

from paasta_tools.api.views.service import list_instances


@mock.patch('paasta_tools.api.views.service.list_all_instances_for_service', autospec=True)
@mock.patch('paasta_tools.api.views.service.get_instance_config_for_service', autospec=True)
def test_list_instances(
    mock_get_instance_config_for_service,
    mock_list_all_instances_for_service
):
    fake_instances = ['fake_instance_a', 'fake_instance_b', 'fake_instance_c']
    mock_list_all_instances_for_service.return_value = fake_instances

    request = testing.DummyRequest()
    request.swagger_data = {'service': 'fake_service'}

    response = list_instances(request)
    assert response['instances'] == fake_instances

    mock_instance_config1 = mock.Mock()
    mock_instance_config2 = mock.Mock()
    mock_instance_config3 = mock.Mock()
    mock_instance_config1.get_deploy_group = mock.Mock(return_value='deploy_group1')
    mock_instance_config1.get_instance = mock.Mock(return_value='fake_instance_a')
    mock_instance_config2.get_deploy_group = mock.Mock(return_value='deploy_group1')
    mock_instance_config2.get_instance = mock.Mock(return_value='fake_instance_b')
    mock_instance_config3.get_deploy_group = mock.Mock(return_value='deploy_group2')
    mock_instance_config3.get_instance = mock.Mock(return_value='fake_instance_c')
    mock_get_instance_config_for_service.return_value = [mock_instance_config1,
                                                         mock_instance_config2,
                                                         mock_instance_config3]

    request.swagger_data = {'service': 'fake_service', 'deploy_group': 'deploy_group1'}

    response = list_instances(request)
    assert response['instances'] == ['fake_instance_a', 'fake_instance_b']
