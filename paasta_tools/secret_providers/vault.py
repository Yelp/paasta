import getpass
import os
from typing import Any
from typing import Dict
from typing import List

try:
    from vault_tools.client.jsonsecret import get_plaintext
    from vault_tools.paasta_secret import get_vault_client
    from vault_tools.gpg import TempGpgKeyring
    from vault_tools.paasta_secret import encrypt_secret
except ImportError:
    def get_plaintext(*args: Any, **kwargs: Any) -> bytes:
        return b"No plain text available without vault_tools"

    def get_vault_client(*args: Any, **kwargs: Any) -> None:
        return None
    TempGpgKeyring = None

    def encrypt_secret(*args: Any, **kwargs: Any) -> None:
        return None

from paasta_tools.secret_providers import BaseSecretProvider
from paasta_tools.utils import paasta_print
from paasta_tools.secret_tools import get_secret_name_from_ref


class SecretProvider(BaseSecretProvider):

    def __init__(
        self,
        soa_dir: str,
        service_name: str,
        cluster_names: List[str],
        vault_cluster_config: Dict[str, str] = {},
        vault_auth_method: str = 'ldap',
        vault_token_file: str = '/root/.vault-token',
        **kwargs: Any,
    ) -> None:
        super().__init__(soa_dir, service_name, cluster_names)
        self.vault_cluster_config = vault_cluster_config
        self.vault_auth_method = vault_auth_method
        self.vault_token_file = vault_token_file

    def decrypt_environment(
        self,
        environment: Dict[str, str],
        **kwargs: Any,
    ) -> Dict[str, str]:
        self.ecosystem = self.get_vault_ecosystems_for_clusters()[0]
        self.client = get_vault_client(
            ecosystem=self.ecosystem,
            num_uses=len(environment),
            vault_auth_method=self.vault_auth_method,
            vault_token_file=self.vault_token_file,
        )
        secret_environment = {}
        for k, v in environment.items():
            secret_name = get_secret_name_from_ref(v)
            secret_path = os.path.join(
                self.secret_dir,
                f"{secret_name}.json",
            )
            secret = get_plaintext(
                client=self.client,
                env=self.ecosystem,
                path=secret_path,
                cache_enabled=False,
                cache_dir=None,
                cache_key=None,
                context=self.service_name,
            ).decode('utf-8')
            secret_environment[k] = secret
        return secret_environment

    def get_vault_ecosystems_for_clusters(self) -> List[str]:
        try:
            return list({self.vault_cluster_config[cluster_name] for cluster_name in self.cluster_names})
        except KeyError as e:
            paasta_print(
                "Cannot find a vault cluster for the %s paasta cluster. A mapping must exist "
                "in /etc/paasta so we contact the correct vault cluster to get/set secrets" % e,
            )
            raise

    def write_secret(
        self,
        action: str,
        secret_name: str,
        plaintext: bytes,
    ) -> None:
        with TempGpgKeyring(overwrite=True):
            ecosystems = self.get_vault_ecosystems_for_clusters()
            if 'VAULT_TOKEN_OVERRIDE' not in os.environ:
                username = getpass.getuser()
                password = getpass.getpass("Please enter your LDAP password to auth with Vault\n")
            else:
                username = None
                password = None
            for ecosystem in ecosystems:
                client = get_vault_client(
                    ecosystem=ecosystem,
                    username=username,
                    password=password,
                )
                encrypt_secret(
                    client=client,
                    action=action,
                    ecosystem=ecosystem,
                    secret_name=secret_name,
                    soa_dir=self.soa_dir,
                    plaintext=plaintext,
                    service_name=self.service_name,
                    transit_key=self.encryption_key,
                )

    def decrypt_secret(self, secret_name: str) -> str:
        ecosystem = self.get_vault_ecosystems_for_clusters()[0]
        if 'VAULT_TOKEN_OVERRIDE' not in os.environ:
            username = getpass.getuser()
            password = getpass.getpass("Please enter your LDAP password to auth with Vault\n")
        else:
            username = None
            password = None
        client = get_vault_client(
            ecosystem=ecosystem,
            username=username,
            password=password,
        )
        secret_path = os.path.join(
            self.secret_dir,
            f"{secret_name}.json",
        )
        return get_plaintext(
            client=client,
            path=secret_path,
            env=ecosystem,
            cache_enabled=False,
            cache_key=None,
            cache_dir=None,
            context=self.service_name,
        ).decode('utf-8')
