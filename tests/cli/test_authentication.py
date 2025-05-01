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
from unittest.mock import patch

from paasta_tools.cli.authentication import get_service_auth_token


@patch("paasta_tools.cli.authentication.load_system_paasta_config", autospec=True)
@patch("paasta_tools.cli.authentication.get_current_ecosystem", autospec=True)
@patch("paasta_tools.cli.authentication.InstanceMetadataProvider", autospec=True)
@patch("paasta_tools.cli.authentication.InstanceMetadataFetcher", autospec=True)
@patch("paasta_tools.cli.authentication.get_vault_client", autospec=True)
@patch("paasta_tools.cli.authentication.get_vault_url", autospec=True)
@patch("paasta_tools.cli.authentication.get_vault_ca", autospec=True)
def test_get_service_auth_token(
    mock_vault_ca,
    mock_vault_url,
    mock_get_vault_client,
    mock_metadata_fetcher,
    mock_metadata_provider,
    mock_ecosystem,
    mock_config,
):
    mock_ecosystem.return_value = "dev"
    mock_config.return_value.get_service_auth_vault_role.return_value = "foobar"
    mock_vault_client = mock_get_vault_client.return_value
    mock_vault_client.secrets.identity.generate_signed_id_token.return_value = {
        "data": {"token": "sometoken"},
    }
    assert get_service_auth_token() == "sometoken"
    mock_instance_creds = (
        mock_metadata_provider.return_value.load.return_value.get_frozen_credentials.return_value
    )
    mock_metadata_provider.assert_called_once_with(
        iam_role_fetcher=mock_metadata_fetcher.return_value
    )
    mock_vault_url.assert_called_once_with("dev")
    mock_vault_ca.assert_called_once_with("dev")
    mock_get_vault_client.assert_called_once_with(
        mock_vault_url.return_value, mock_vault_ca.return_value
    )
    mock_vault_client.auth.aws.iam_login.assert_called_once_with(
        mock_instance_creds.access_key,
        mock_instance_creds.secret_key,
        mock_instance_creds.token,
        mount_point="aws-iam",
        role="foobar",
        use_token=True,
    )
    mock_vault_client.secrets.identity.generate_signed_id_token.assert_called_once_with(
        name="foobar"
    )
