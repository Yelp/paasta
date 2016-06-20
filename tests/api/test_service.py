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
def test_list_instances(
    mock_list_all_instances_for_service,
):
    fake_instances = ['fake_instance_a', 'fake_instance_b', 'fake_instance_c']
    mock_list_all_instances_for_service.return_value = fake_instances

    request = testing.DummyRequest()
    request.matchdict = {'service': 'fake_service'}

    response = list_instances(request)
    assert response['instances'] == fake_instances
