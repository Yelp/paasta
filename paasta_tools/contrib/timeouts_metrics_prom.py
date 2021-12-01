#!/opt/venvs/paasta-tools/bin/python
"""
The goal is to walk through all smartstack.yaml in yelpsoa directories
and convert timeout values to the prometheus format.

For every upstream and path, if endpoint timeout is specified, we record
per endpoint timeout value. If timeout_server_ms is specified,
we record timeout_server_ms value in path `/`. Otherwise, we record the default
timeout_server_ms 1000.

Example output:
# HELP endpoint timeout value defined in yelpsoa-config.
# TYPE TYPE yelpsoaconfig_endpoint_timeouts_ms gauge
yelpsoaconfig_endpoint_timeouts_ms{path="/consumer_app/devices/braze/info",upstream="push_notifications.main.egress_cluster"} 10000.0
yelpsoaconfig_endpoint_timeouts_ms{path="/",upstream="mysql_read_security.main.egress_cluster"} 100.0
"""
import os

import yaml
from prometheus_client import CollectorRegistry
from prometheus_client import Gauge
from prometheus_client import write_to_textfile
from prometheus_client.metrics import MetricWrapperBase

from paasta_tools.utils import DEFAULT_SOA_DIR

PROM_OUTPUT_FILE = f"{DEFAULT_SOA_DIR}/.autotune_timeouts.prom"
DEFAULT_TIMEOUT = 1000


def read_and_write_timeouts_metrics(
    root: str, service: str, prom_metric: MetricWrapperBase
) -> None:
    with open(os.path.join(root, "smartstack.yaml")) as smartstack_file:
        smartstack_yaml = yaml.safe_load(smartstack_file)
    for instance_name, info in smartstack_yaml.items():
        upstream = f"{service}.{instance_name}.egress_cluster"
        if "endpoint_timeouts" in info:
            for path, endpoint_timeout in info["endpoint_timeouts"].items():
                prom_metric.labels(path, upstream).set(endpoint_timeout)
        # always record default timeout
        default_timeout = info.get("timeout_server_ms", DEFAULT_TIMEOUT)
        prom_metric.labels("/", upstream).set(default_timeout)


if __name__ == "__main__":
    registry = CollectorRegistry()
    prom_metric = Gauge(
        "yelpsoaconfig_endpoint_timeouts_ms",
        "endpoint timeout value defined in yelpsoa-config",
        ["path", "upstream"],
        registry=registry,
    )
    # Walk through soa config dir and filter smartstack yaml
    for root, dirs, files in os.walk(DEFAULT_SOA_DIR):
        service = root.split("/")[-1]
        # Avoid confusion of the smartstacks.yaml under autotuned_defaults/ in the future
        if "autotuned_defaults" == service:
            continue
        for f in files:
            if f == "smartstack.yaml":
                read_and_write_timeouts_metrics(root, service, prom_metric)

    write_to_textfile(PROM_OUTPUT_FILE, registry)
