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
from typing import Optional

from paasta_tools.utils import load_system_paasta_config


try:
    from vault_tools.oidc import get_instance_oidc_identity_token
    from okta_auth import get_and_cache_jwt_default
except ImportError:

    def get_instance_oidc_identity_token(
        role: str, ecosystem: Optional[str] = None
    ) -> str:
        return ""

    def get_and_cache_jwt_default(client_id: str) -> str:
        return ""


@lru_cache(maxsize=1)
def get_service_auth_token() -> str:
    """Uses instance profile to authenticate with Vault and generate token for service authentication"""
    vault_role = load_system_paasta_config().get_service_auth_vault_role()
    return get_instance_oidc_identity_token(vault_role)


def get_sso_auth_token(paasta_apis: bool = False) -> str:
    """Generate an authentication token for the calling user from the Single Sign On provider

    :param bool paasta_apis: authenticate for PaaSTA APIs
    """
    system_config = load_system_paasta_config()
    client_id = (
        system_config.get_api_auth_sso_oidc_client_id()
        if paasta_apis
        else system_config.get_service_auth_sso_oidc_client_id()
    )
    return get_and_cache_jwt_default(client_id)
