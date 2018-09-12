import os
from typing import Any
from typing import Dict
from typing import List


class BaseSecretProvider:

    def __init__(
        self,
        soa_dir: str,
        service_name: str,
        cluster_names: List[str],
        **kwargs: Any,
    ) -> None:
        self.soa_dir = soa_dir
        self.service_name = service_name
        self.secret_dir = os.path.join(self.soa_dir, self.service_name, "secrets")
        self.cluster_names = cluster_names

    def decrypt_environment(self, environment: Dict[str, str], **kwargs: Any) -> Dict[str, str]:
        raise NotImplementedError

    def write_secret(self, action: str, secret_name: str, plaintext: bytes) -> None:
        raise NotImplementedError

    def decrypt_secret(self, secret_name: str) -> str:
        raise NotImplementedError


class SecretProvider(BaseSecretProvider):
    pass
