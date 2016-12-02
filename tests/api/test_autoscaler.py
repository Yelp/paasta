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
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
from pyramid import testing

from paasta_tools.api.views import autoscaler


def test_get_autoscaler_count():
    request = testing.DummyRequest()
    request.swagger_data = {
        'service': 'fake_service',
        'instance': 'fake_instance',
    }

    with mock.patch('paasta_tools.api.views.autoscaler.load_marathon_service_config',
                    autospec=True) as mock_load_marathon_service_config:
        mock_load_marathon_service_config.return_value = mock.MagicMock(get_instances=mock.MagicMock(return_value=123))
        response = autoscaler.get_autoscaler_count(request)
        assert response.json_body['desired_instances'] == 123


@mock.patch('paasta_tools.api.views.autoscaler.load_marathon_service_config', autospec=True)
def test_update_autoscaler_count(mock_load_marathon_service_config):
    request = testing.DummyRequest()
    request.swagger_data = {
        'service': 'fake_service',
        'instance': 'fake_instance',
        'json_body': {'desired_instances': 123},
    }

    mock_load_marathon_service_config.return_value = mock.MagicMock(
        get_min_instances=mock.MagicMock(return_value=100),
        get_max_instances=mock.MagicMock(return_value=200)
    )

    with mock.patch('paasta_tools.api.views.autoscaler.set_instances_for_marathon_service',
                    autospec=True) as mock_set_instances:
        response = autoscaler.update_autoscaler_count(request)
        assert response.json_body['desired_instances'] == 123
        mock_set_instances.assert_called_once_with(service='fake_service', instance='fake_instance', instance_count=123)


@mock.patch('paasta_tools.api.views.autoscaler.load_marathon_service_config', autospec=True)
@mock.patch('paasta_tools.api.views.autoscaler.set_instances_for_marathon_service', autospec=True)
def test_update_autoscaler_count_warning(
    mock_set_instances_for_marathon_service,
    mock_load_marathon_service_config
):
    request = testing.DummyRequest()
    request.swagger_data = {
        'service': 'fake_service',
        'instance': 'fake_instance',
        'json_body': {'desired_instances': 123},
    }

    mock_load_marathon_service_config.return_value = mock.MagicMock(
        get_min_instances=mock.MagicMock(return_value=10),
        get_max_instances=mock.MagicMock(return_value=100)
    )

    response = autoscaler.update_autoscaler_count(request)
    assert response.json_body['desired_instances'] == 100
    assert 'WARNING' in response.json_body['status']
