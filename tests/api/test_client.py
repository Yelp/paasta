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
from paasta_tools.api.client import renew_issue_cert


def test_get_paasta_api_client(system_paasta_config):
    with mock.patch(
        "paasta_tools.api.client.load_system_paasta_config", autospec=True
    ) as mock_load_system_paasta_config:
        mock_load_system_paasta_config.return_value = system_paasta_config

        client = get_paasta_api_client()
        assert client


def test_renew_issue_cert():
    with mock.patch(
        "paasta_tools.api.client.get_secret_provider", autospec=True
    ) as mock_get_secret_provider:
        mock_config = mock.Mock()
        renew_issue_cert(mock_config, "westeros-prod")
        mock_get_secret_provider.return_value.renew_issue_cert.assert_called_with(
            pki_backend=mock_config.get_pki_backend(),
            ttl=mock_config.get_auth_certificate_ttl(),
        )
