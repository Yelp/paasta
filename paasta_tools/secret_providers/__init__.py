import os
from typing import Any
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional

from service_configuration_lib import read_service_configuration


class BaseSecretProvider:
    def __init__(
        self,
        soa_dir: Optional[str],
        service_name: Optional[str],
        cluster_names: List[str],
        **kwargs: Any,
    ) -> None:
        self.soa_dir = soa_dir
        self.cluster_names = cluster_names
        self.service_name = service_name
        if service_name:
            self.secret_dir = os.path.join(self.soa_dir, self.service_name, "secrets")
            service_config = read_service_configuration(self.service_name, self.soa_dir)
            self.encryption_key = service_config.get("encryption_key", "paasta")

    def decrypt_environment(
        self, environment: Dict[str, str], **kwargs: Any
    ) -> Dict[str, str]:
        raise NotImplementedError

    def write_secret(
        self,
        action: str,
        secret_name: str,
        plaintext: bytes,
        cross_environment_motivation: Optional[str] = None,
    ) -> None:
        raise NotImplementedError

    def decrypt_secret(self, secret_name: str) -> str:
        raise NotImplementedError

    def decrypt_secret_raw(self, secret_name: str) -> bytes:
        raise NotImplementedError

    def get_secret_signature_from_data(self, data: Mapping[str, Any]) -> Optional[str]:
        raise NotImplementedError


class SecretProvider(BaseSecretProvider):
    pass
