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

from paasta_tools.api.client import get_paasta_oapi_client


def test_get_paasta_oapi_client(system_paasta_config):
    with mock.patch(
        "paasta_tools.api.client.load_system_paasta_config", autospec=True
    ) as mock_load_system_paasta_config:
        mock_load_system_paasta_config.return_value = system_paasta_config

        client = get_paasta_oapi_client()
        assert client


