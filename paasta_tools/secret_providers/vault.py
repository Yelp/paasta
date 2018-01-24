import os
from typing import Any
from typing import Dict

try:
    from vault_tools.client.jsonsecret import get_plaintext
    from vault_tools.paasta_secret import get_vault_client
except ImportError:
    get_plaintext = None
    get_vault_client = None

from paasta_tools.secret_providers import BaseSecretProvider
from paasta_tools.utils import paasta_print
from paasta_tools.secret_tools import get_secret_name_from_ref


class SecretProvider(BaseSecretProvider):

    def __init__(self, soa_dir: str, service_name: str, cluster_name: str) -> None:
        super().__init__(soa_dir, service_name, cluster_name)

    def decrypt_environment(
        self,
        environment: Dict[str, str],
        vault_auth_method: str='ldap',
        vault_token_file: str='/root/.vault-token',
        vault_cluster_config: Dict[str, str]={},
        **kwargs: Any,
    ) -> Dict[str, str]:
        try:
            self.ecosystem = vault_cluster_config[self.cluster_name]
        except KeyError:
            paasta_print(
                "Cannot find a vault cluster for the %s paasta cluster. A mapping must exist "
                "in /etc/paasta so we contact the correct vault cluster to get secrets" % self.cluster_name,
            )
            raise
        self.client = get_vault_client(
            ecosystem=self.ecosystem,
            num_uses=len(environment),
            vault_auth_method=vault_auth_method,
            vault_token_file=vault_token_file,
        )
        secret_environment = {}
        for k, v in environment.items():
            secret_name = get_secret_name_from_ref(v)
            secret_path = os.path.join(
                self.secret_dir,
                "{}.json".format(secret_name),
            )
            secret = get_plaintext(
                client=self.client,
                env=self.ecosystem,
                path=secret_path,
                cache_enabled=False,
                cache_dir=None,
                cache_key=None,
                context=self.service_name,
            )[0]
            secret_environment[k] = secret
        return secret_environment
