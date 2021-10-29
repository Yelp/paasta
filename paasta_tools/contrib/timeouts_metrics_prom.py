#!/opt/venvs/paasta-tools/bin/python
"""
The goal is to walk through all smartstack.yaml in yelpsoa directories
and convert timeout values to the prometheus format.

For every upstream and path, if endpoint timeout is specified, we record
per endpoint timeout value. If not and if timeout_server_ms is specified,
we record timeout_server_ms value in path `default`. Otherwise, we record the default
timeout_server_ms 1000.

# HELP endpoint timeout value defined in yelpsoa-config.
# TYPE TYPE yelpsoaconfig_endpoint_timeouts_ms gauge
yelpsoaconfig_endpoint_timeouts_ms{path="/consumer_app/devices/braze/info",upstream="push_notifications.main.egress_cluster"} 10000.0
yelpsoaconfig_endpoint_timeouts_ms{path="default",upstream="mysql_read_security.main.egress_cluster"} 100.0
"""
import os

import yaml
from prometheus_client import CollectorRegistry
from prometheus_client import Gauge
from prometheus_client import write_to_textfile

SOA_DIR = "/nail/etc/services/"
PROM_OUTPUT_FILE = "/nail/etc/services/.autotune_timeouts.prom"


registry = CollectorRegistry()
prom_metric = Gauge(
    "yelpsoaconfig_endpoint_timeouts_ms",
    "endpoint timeout value defined in yelpsoa-config",
    ["path", "upstream"],
    registry=registry,
)
for root, dirs, files in os.walk(SOA_DIR):
    service = root.split("/")[-1]
    for name in files:
        if name == "smartstack.yaml":
            with open(os.path.join(root, name)) as smartstack_file:
                smartstack_yaml = yaml.safe_load(smartstack_file)
                for instance_name, info in smartstack_yaml.items():
                    upstream = service + "." + instance_name + ".egress_cluster"
                    if "endpoint_timeouts" in info:
                        for path, endpoint_timeouts in info[
                            "endpoint_timeouts"
                        ].items():
                            prom_metric.labels(path, upstream).set(endpoint_timeouts)
                    else:
                        if "timeout_server_ms" in info:
                            prom_metric.labels("default", upstream).set(
                                info["timeout_server_ms"]
                            )
                        else:
                            prom_metric.labels("default", upstream).set(1000)

write_to_textfile(PROM_OUTPUT_FILE, registry)
