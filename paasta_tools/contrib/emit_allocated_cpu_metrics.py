#!/usr/bin/env python
import logging

import yelp_meteorite

from paasta_tools.cli.utils import get_instance_config
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config

log = logging.getLogger(__name__)


def emit_metrics_for_type(instance_type):
    cluster = load_system_paasta_config().get_cluster()
    instances = get_services_for_cluster(cluster=cluster, instance_type=instance_type)

    for service, instance in instances:
        service_instance_config = get_instance_config(
            service=service, instance=instance, cluster=cluster
        )
        dimensions = {
            "paasta_service": service_instance_config.service,
            "paasta_cluster": service_instance_config.cluster,
            "paasta_instance": service_instance_config.instance,
            "paasta_pool": service_instance_config.get_pool(),
        }

        log.info(f"Emitting paasta.service.* with dimensions {dimensions}")
        gauge = yelp_meteorite.create_gauge("paasta.service.cpus", dimensions)
        gauge.set(service_instance_config.get_cpus())
        gauge = yelp_meteorite.create_gauge("paasta.service.mem", dimensions)
        gauge.set(service_instance_config.get_mem())
        gauge = yelp_meteorite.create_gauge("paasta.service.disk", dimensions)
        gauge.set(service_instance_config.get_disk())
        if hasattr(service_instance_config, "get_instances"):
            if service_instance_config.get_max_instances() is None:
                gauge = yelp_meteorite.create_gauge(
                    "paasta.service.instances", dimensions
                )
                gauge.set(service_instance_config.get_instances())


def main():
    logging.basicConfig(level=logging.INFO)
    for thing in ["adhoc"]:
        emit_metrics_for_type(thing)


if __name__ == "__main__":
    main()
