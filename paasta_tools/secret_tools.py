# Copyright 2015-2017 Yelp Inc.
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
import json
import os
import re
import sys
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from paasta_tools.secret_providers import SecretProvider

SECRET_REGEX = r"^(SHARED_)?SECRET\([A-Za-z0-9_-]*\)$"
SHARED_SECRET_SERVICE = "_shared"


def is_secret_ref(env_var_val: str) -> bool:
    pattern = re.compile(SECRET_REGEX)
    try:
        match = pattern.match(env_var_val)
    except TypeError:
        # it can't be a secret ref if it isn't a string
        return False
    return match is not None


def is_shared_secret(env_var_val: str) -> bool:
    return env_var_val.startswith("SHARED_")


def get_hmac_for_secret(
    env_var_val: str, service: str, soa_dir: str, secret_environment: str
) -> Optional[str]:
    secret_name = get_secret_name_from_ref(env_var_val)
    if is_shared_secret(env_var_val):
        service = SHARED_SECRET_SERVICE
    secret_path = os.path.join(soa_dir, service, "secrets", f"{secret_name}.json")
    try:
        with open(secret_path, "r") as json_secret_file:
            secret_file = json.load(json_secret_file)
            try:
                return secret_file["environments"][secret_environment]["signature"]
            except KeyError:
                print(
                    "Failed to get secret signature at environments:{}:signature in json"
                    " file".format(secret_environment),
                    file=sys.stderr,
                )
                return None
    except IOError:
        print(f"Failed to open json secret at {secret_path}", file=sys.stderr)
        return None
    except json.decoder.JSONDecodeError:
        print(f"Failed to deserialise json secret at {secret_path}", file=sys.stderr)
        return None


def get_secret_name_from_ref(env_var_val: str) -> str:
    return env_var_val.split("(")[1][:-1]


def get_secret_provider(
    secret_provider_name: str,
    soa_dir: str,
    service_name: str,
    cluster_names: List[str],
    secret_provider_kwargs: Dict[str, Any],
) -> SecretProvider:
    SecretProvider = __import__(
        secret_provider_name, fromlist=["SecretProvider"]
    ).SecretProvider
    return SecretProvider(
        soa_dir=soa_dir,
        service_name=service_name,
        cluster_names=cluster_names,
        **secret_provider_kwargs,
    )


def get_secret_hashes(
    environment_variables: Dict[str, str],
    secret_environment: str,
    service: str,
    soa_dir: str,
) -> Dict[str, str]:

    secret_hashes = {}
    for env_var_val in environment_variables.values():
        if is_secret_ref(env_var_val):
            secret_hashes[env_var_val] = get_hmac_for_secret(
                env_var_val=env_var_val,
                service=service,
                soa_dir=soa_dir,
                secret_environment=secret_environment,
            )
    return secret_hashes


def decrypt_secret_environment_for_service(
    secret_env_vars: Dict[str, str],
    service_name: str,
    secret_provider_name: str,
    soa_dir: str,
    cluster_name: str,
    secret_provider_kwargs: Dict[str, Any],
) -> Dict[str, str]:
    if not secret_env_vars:
        return {}

    secret_provider = get_secret_provider(
        secret_provider_name=secret_provider_name,
        soa_dir=soa_dir,
        service_name=service_name,
        cluster_names=[cluster_name],
        secret_provider_kwargs=secret_provider_kwargs,
    )
    return secret_provider.decrypt_environment(secret_env_vars)


def decrypt_secret_environment_variables(
    secret_provider_name: str,
    environment: Dict[str, str],
    soa_dir: str,
    service_name: str,
    cluster_name: str,
    secret_provider_kwargs: Dict[str, Any],
) -> Dict[str, str]:
    decrypted_secrets = {}
    service_secret_env = {}
    shared_secret_env = {}
    for k, v in environment.items():
        if is_secret_ref(v):
            if is_shared_secret(v):
                shared_secret_env[k] = v
            else:
                service_secret_env[k] = v
    secret_provider_kwargs["vault_num_uses"] = len(service_secret_env) + len(
        shared_secret_env
    )

    decrypted_secrets.update(
        decrypt_secret_environment_for_service(
            service_secret_env,
            service_name,
            secret_provider_name,
            soa_dir,
            cluster_name,
            secret_provider_kwargs,
        )
    )
    decrypted_secrets.update(
        decrypt_secret_environment_for_service(
            shared_secret_env,
            SHARED_SECRET_SERVICE,
            secret_provider_name,
            soa_dir,
            cluster_name,
            secret_provider_kwargs,
        )
    )
    return decrypted_secrets
