import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests
import signalfx

from paasta_tools.hpa_metrics_collector.parsers import http_parser
from paasta_tools.hpa_metrics_collector.parsers import uwsgi_parser
from paasta_tools.hpa_metrics_collector.ticker import Ticker
from paasta_tools.kubernetes_tools import sanitise_kubernetes_name

READ_ONLY_PORT = 10255
PARSER = {"uwsgi": uwsgi_parser, "http": http_parser}
METRICS_ENDPOINT = {"uwsgi": "status/uwsgi", "http": "status"}

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", dest="token", type=str, required=True)
    parser.add_argument("--cluster", dest="cluster", type=str, required=True)
    args = parser.parse_args()
    return args


def sanitize_sfx_dimensions_keys(k):
    return k.replace(".", "_").replace("/", "_")


def update_metrics(name, namespace, metrics_name, value, labels, token, cluster):
    """
    Sanitize all labels and send the metrics to sfx with labels as dimnesions.
    """
    labels.update(
        {
            "kubernetes_namespace": namespace,
            "kubernetes_pod_name": name,
            "kubernetes_cluster": cluster,
        }
    )
    labels = {sanitize_sfx_dimensions_keys(k): v for k, v in labels.items()}
    with signalfx.SignalFx(ingest_endpoint="https://ingest.signalfx.com").ingest(
        token
    ) as sfx:
        t = int(time.time() * 1000)
        sfx.send(
            gauges=[
                {
                    "metric": metrics_name,
                    "value": value,
                    "timestamp": t,
                    "dimensions": labels,
                }
            ]
        )
    log.info(f"updated metrics {metrics_name} with dimensions {labels} to {value}")


def get_container(pod, container_name):
    """
    Find and return container spec of a particular container with container_name
    """
    for container in pod["spec"]["containers"]:
        if container["name"].startswith(container_name):
            return container


def is_container_ready(conditions):
    for cond in conditions:
        if cond["type"] == "Ready" and cond["status"] == "True":
            return True
    return False


def collect(token, cluster):
    log.info("Collecting metrics")
    # Get all pod metadata on this node
    node_info = requests.get(f"http://169.254.255.254:{READ_ONLY_PORT}/pods/").json()
    with ThreadPoolExecutor(max_workers=5) as executor:
        for pod in node_info["items"]:
            # Fiter out instances that are not running
            if pod["status"]["phase"] != "Running":
                continue
            # Fiter out instances that do not use http/uwsgi metrics for autoscaling
            metrics_name = pod["metadata"].get("annotations", {}).get("autoscaling")
            if not metrics_name:
                continue
            # Find port, and metric_provider in order to get metrics from container
            pod_name = pod["metadata"]["name"]
            namespace = pod["metadata"]["namespace"]
            labels = dict(pod["metadata"]["labels"])
            pod_IP = pod["status"]["podIP"]
            # Use the first 45 character of instance name to determine container because some
            # hash might be added to the end of container name
            instance_name = sanitise_kubernetes_name(
                labels["paasta.yelp.com/instance"]
            )[:45]
            container = get_container(pod, instance_name)
            container_port = container["ports"][0]["containerPort"]
            # Fetch metrics from the containers
            try:
                output = requests.get(
                    f"http://{pod_IP}:{container_port}/{METRICS_ENDPOINT[metrics_name]}"
                ).json()
                value = str(PARSER[metrics_name](output))
            except Exception as e:
                log.error(
                    f"hpa-metrics-collector have trouble querying metrics from pod/{pod_name} namespace/{namespace}"
                )
                if output:
                    log.error(output)
                log.error(e)
            else:
                # Update metrics on SFX
                executor.submit(
                    update_metrics,
                    pod_name,
                    namespace,
                    metrics_name,
                    value,
                    labels,
                    token,
                    cluster,
                )
        log.info("Finished updating metrics")


def main():
    args = parse_args()
    # Executes every 5 seconds.
    Ticker(5, collect, args.token, args.cluster)


if __name__ == "__main__":
    main()
