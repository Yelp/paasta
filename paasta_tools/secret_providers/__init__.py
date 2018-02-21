import os
from typing import Any
from typing import Dict


class BaseSecretProvider(object):

    def __init__(self, soa_dir: str, service_name: str, cluster_name: str) -> None:
        self.secret_dir = os.path.join(soa_dir, service_name, "secrets")
        self.service_name = service_name
        self.cluster_name = cluster_name

    def decrypt_environment(self, environment: Dict[str, str], **kwargs: Any) -> Dict[str, str]:
        raise NotImplementedError


class SecretProvider(BaseSecretProvider):
    pass
