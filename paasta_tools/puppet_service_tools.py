import json
import os
from typing import Dict
from typing import List
from typing import Tuple

import service_configuration_lib

from paasta_tools.long_running_service_tools import load_service_namespace_config
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
from paasta_tools.utils import compose_job_id

PUPPET_SERVICE_DIR = "/etc/nerve/puppet_services.d"


def get_puppet_services_that_run_here() -> Dict[str, List[str]]:
    # find all files in the PUPPET_SERVICE_DIR, but discard broken symlinks
    # this allows us to (de)register services on a machine by
    # breaking/healing a symlink placed by Puppet.
    puppet_service_dir_services = {}
    if os.path.exists(PUPPET_SERVICE_DIR):
        for service_name in os.listdir(PUPPET_SERVICE_DIR):
            if not os.path.exists(os.path.join(PUPPET_SERVICE_DIR, service_name)):
                continue
            with open(os.path.join(PUPPET_SERVICE_DIR, service_name)) as f:
                puppet_service_data = json.load(f)
                puppet_service_dir_services[service_name] = puppet_service_data[
                    "namespaces"
                ]

    return puppet_service_dir_services


def get_puppet_services_running_here_for_nerve(
    soa_dir: str,
) -> List[Tuple[str, ServiceNamespaceConfig]]:
    puppet_services = []
    for service, namespaces in sorted(get_puppet_services_that_run_here().items()):
        for namespace in namespaces:
            puppet_services.append(
                _namespaced_get_classic_service_information_for_nerve(
                    service, namespace, soa_dir
                )
            )
    return puppet_services


def _namespaced_get_classic_service_information_for_nerve(
    name: str, namespace: str, soa_dir: str
) -> Tuple[str, ServiceNamespaceConfig]:
    nerve_dict = load_service_namespace_config(name, namespace, soa_dir)
    port_file = os.path.join(soa_dir, name, "port")
    # If the namespace defines a port, prefer that, otherwise use the
    # service wide port file.
    nerve_dict["port"] = nerve_dict.get(
        "port", None
    ) or service_configuration_lib.read_port(port_file)
    nerve_name = compose_job_id(name, namespace)
    return (nerve_name, nerve_dict)
