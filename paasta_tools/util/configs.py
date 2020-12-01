import os
from typing import Any
from typing import Mapping

import service_configuration_lib


def load_all_configs(
    cluster: str, file_prefix: str, soa_dir: str
) -> Mapping[str, Mapping[str, Any]]:
    config_dicts = {}
    for service in os.listdir(soa_dir):
        config_dicts[
            service
        ] = service_configuration_lib.read_extra_service_information(
            service, f"{file_prefix}-{cluster}", soa_dir=soa_dir
        )
    return config_dicts
