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
import multiprocessing
import os
import sys
import time
from functools import lru_cache
from typing import Optional

from paasta_tools.utils import atomic_file_write
from paasta_tools.utils import load_system_paasta_config

try:
    from okta_auth import get_and_cache_jwt_default
    from vault_tools.oidc import get_instance_oidc_identity_token
except ImportError:

    def get_instance_oidc_identity_token(
        role: str, ecosystem: Optional[str] = None
    ) -> str:
        return ""

    def get_and_cache_jwt_default(
        client_id: str, refreshable: bool = False, force: bool = False
    ) -> str:
        return ""


MAX_SSO_REFRESH_ATTEMPTS = 3


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


def maintain_valid_sso_token(
    directory: str,
    token_file: str = "token",
    check_interval: int = 300,
    max_token_age: int = 2400,
) -> None:
    """Generates and keeps up to date SSO token in a given directory.
    The resulting token path is "{directory}/{token_file}".
    The function starts a process that never returns unless the parent process terminates.

    :param str directory: where to place the token file
    :param str token_file: name of the token file
    :param int check_interval: how often to check if the file needs to be updated (in seconds)
    :param int max_token_age: how long to wait before refreshing the token file (in seconds)
    """
    token_path = os.path.join(directory, token_file)
    system_config = load_system_paasta_config()
    client_id = system_config.get_service_auth_sso_oidc_client_id()

    # We do a first call synchronously to ensure the user has control on the input;
    # force=True ensures that we get a refresh token in memory.
    os.makedirs(directory, exist_ok=True)
    token_val = get_and_cache_jwt_default(client_id, refreshable=True, force=True)
    with atomic_file_write(token_path, mode=0o0600) as f:
        f.write(token_val)

    def auth_handler() -> None:
        while os.getppid() != 1:  # stop when parent process terminates
            time.sleep(check_interval)
            if time.time() - os.path.getmtime(token_path) < max_token_age:
                continue
            for i in range(MAX_SSO_REFRESH_ATTEMPTS):
                try:
                    # we force=True again because the library has validation tolerances of a few seconds,
                    # while we aim at monitoring the file only every few minutes
                    token_val = get_and_cache_jwt_default(
                        client_id, refreshable=True, force=True
                    )
                    break
                except Exception as e:
                    if i == MAX_SSO_REFRESH_ATTEMPTS - 1:
                        raise e
                    print(f"Error refreshing SSO token: {e}", file=sys.stderr)
                    time.sleep(5)
            with atomic_file_write(token_path, mode=0o0600) as f:
                f.write(token_val)
        os.remove(token_path)

    proc = multiprocessing.Process(target=auth_handler)
    proc.start()
