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
from functools import lru_cache

from botocore.credentials import InstanceMetadataFetcher
from botocore.credentials import InstanceMetadataProvider

from paasta_tools.utils import load_system_paasta_config


try:
    from vault_tools.paasta_secret import get_client as get_vault_client
    from vault_tools.paasta_secret import get_vault_url
    from vault_tools.paasta_secret import get_vault_ca
    from okta_auth import get_and_cache_jwt_default
except ImportError:

    def get_vault_client(url: str, capath: str) -> None:
        pass

    def get_vault_url(ecosystem: str) -> str:
        return ""

    def get_vault_ca(ecosystem: str) -> str:
        return ""

    def get_and_cache_jwt_default(client_id: str) -> str:
        return ""


def get_current_ecosystem() -> str:
    """Get current ecosystem from host configs, defaults to dev if no config is found"""
    try:
        with open("/nail/etc/ecosystem") as f:
            return f.read().strip()
    except IOError:
        pass
    return "devc"


@lru_cache(maxsize=1)
def get_service_auth_token() -> str:
    """Uses instance profile to authenticate with Vault and generate token for service authentication"""
    ecosystem = get_current_ecosystem()
    vault_client = get_vault_client(get_vault_url(ecosystem), get_vault_ca(ecosystem))
    vault_role = load_system_paasta_config().get_service_auth_vault_role()
    metadata_provider = InstanceMetadataProvider(
        iam_role_fetcher=InstanceMetadataFetcher(),
    )
    instance_credentials = metadata_provider.load().get_frozen_credentials()
    vault_client.auth.aws.iam_login(
        instance_credentials.access_key,
        instance_credentials.secret_key,
        instance_credentials.token,
        mount_point="aws-iam",
        role=vault_role,
        use_token=True,
    )
    response = vault_client.secrets.identity.generate_signed_id_token(name=vault_role)
    return response["data"]["token"]


def get_sso_service_auth_token() -> str:
    """Generate an authentication token for the calling user from the Single Sign On provider"""
    client_id = load_system_paasta_config().get_service_auth_sso_oidc_client_id()
    return get_and_cache_jwt_default(client_id)
