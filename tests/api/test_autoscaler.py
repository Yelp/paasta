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

from paasta_tools.api.views import autoscaler


def test_get_autoscaler_count():
    request = testing.DummyRequest()
    request.swagger_data = {
        'service': 'fake_service',
        'instance': 'fake_instance',
    }

    with mock.patch('paasta_tools.api.views.autoscaler.get_instances_from_zookeeper') as mock_get_zk_instances:
        mock_get_zk_instances.return_value = 123
        response = autoscaler.get_autoscaler_count(request)
        assert response == 123
        mock_get_zk_instances.assert_called_once_with('fake_service', 'fake_instance')


def test_update_autoscaler_count():
    request = testing.DummyRequest()
    request.swagger_data = {
        'service': 'fake_service',
        'instance': 'fake_instance',
    }

    with mock.patch('paasta_tools.api.views.autoscaler.get_instances_from_zookeeper') as mock_get_zk_instances:
        mock_get_zk_instances.return_value = 123
        response = autoscaler.get_autoscaler_count(request)
        assert response == 123
        mock_get_zk_instances.assert_called_once_with('fake_service', 'fake_instance')
