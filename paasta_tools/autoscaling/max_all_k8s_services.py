#!/usr/bin/env python
from paasta_tools.kubernetes.application.controller_wrappers import (
    get_application_wrapper,
)
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config


def main() -> None:
    system_paasta_config = load_system_paasta_config()

    kube_client = KubeClient()

    services = {
        service
        for service, instance in get_services_for_cluster(
            cluster=system_paasta_config.get_cluster(), instance_type="kubernetes"
        )
    }

    for service in services:
        pscl = PaastaServiceConfigLoader(service=service, load_deployments=False)
        for instance_config in pscl.instance_configs(
            cluster=system_paasta_config.get_cluster(),
            instance_type_class=KubernetesDeploymentConfig,
        ):
            max_instances = instance_config.get_max_instances()
            if max_instances is not None:
                formatted_application = instance_config.format_kubernetes_app()
                formatted_application.spec.replicas = max_instances
                wrapper = get_application_wrapper(formatted_application)
                wrapper.soa_config = instance_config
                print(f"Scaling up {service}.{instance_config.instance}")
                wrapper.update(kube_client)


if __name__ == "__main__":
    main()
