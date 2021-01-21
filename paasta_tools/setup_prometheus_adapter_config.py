"""
Small utility to update the Prometheus adapter's config to match soaconfigs.
"""
# Copyright 2015-2021 Yelp Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import logging
from pathlib import Path
from typing import Any
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import yaml
from kubernetes.client import V1ConfigMap
from kubernetes.client import V1ObjectMeta
from kubernetes.client.rest import ApiException
from mypy_extensions import TypedDict

from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.utils import DEFAULT_SOA_DIR

log = logging.getLogger(__name__)


class PrometheusAdapterRule(TypedDict):
    """
    Typed version of the (minimal) set of Prometheus adapter rule configuration options that we use
    """

    # see https://github.com/DirectXMan12/k8s-prometheus-adapter/blob/master/docs/config.md
    # for more detailed information
    seriesQuery: str  # used for discovering what resources should be scaled
    resources: Dict[str, str]  # used to associate metrics with resources
    metricsQuery: str  # the actual query we want to send to Prometheus


class PrometheusAdapterConfig(TypedDict):
    """
    Typed version of the Prometheus adapter configuration dictionary.
    """

    rules: List[PrometheusAdapterRule]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Syncs the Prometheus metric adapter config with soaconfigs.",
    )

    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=Path(DEFAULT_SOA_DIR),
        help="Directory to read service configs from. Default is %(default)s.",
        type=Path,
    )
    # TODO: do we need to be able to pass multiple clusters in?
    parser.add_argument(
        "-c",
        "--cluster",
        dest="cluster",
        help="PaaSTA cluster to generate configs for.",
        required=True,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="Enable verbose logging.",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=False,
        help="Enable verbose logging.",
    )

    return parser.parse_args()


def should_create_uwsgi_scaling_config(
    instance: str, instance_config: Dict[str, Any],
) -> Tuple[bool, Optional[str]]:
    """
    Determines whether we should configure the prometheus adapter for a given service.

    Returns a 2-tuple of (should_create, reason_to_skip)
    """
    if instance.startswith("_"):
        return (
            False,
            "does not look like an instance name",
        )

    autoscaling_config = instance_config.get("autoscaling")
    if not autoscaling_config:
        return (
            False,
            "not setup for autoscaling.",
        )

    if autoscaling_config.get("metrics_provider") == "uwsgi":
        if not autoscaling_config.get("use_prometheus"):
            return False, "requested uwsgi autoscaling, but not using Prometheus"

        return True, None

    return False, "did not request uwsgi autoscaling"


def generate_instance_uwsgi_scaling_config(
    instance: str, instance_config: Dict[str, Any],
) -> PrometheusAdapterRule:
    """
    Creates a Prometheus adapter rule config for a given service instance.
    """
    return {"seriesQuery": "", "resources": {}, "metricsQuery": ""}


def generate_prometheus_adapter_config(
    paasta_cluster: str, soa_dir: Path
) -> PrometheusAdapterConfig:
    """
    Given a paasta cluster and a soaconfigs directory, create the necessary Prometheus adapter
    config to autoscale services.

    Currently supports the following metrics providers:
        * uwsgi
    """
    config: PrometheusAdapterConfig = {"rules": []}

    for service_config_path in soa_dir.glob(f"*/kubernetes-{paasta_cluster}.yaml"):
        service_config = yaml.safe_load(service_config_path.read_text())
        for instance, instance_config in service_config.items():
            should_create, skip_reason = should_create_uwsgi_scaling_config(
                instance, instance_config
            )
            if not should_create:
                log.debug(
                    "Skipping %s in %s - %s.",
                    instance,
                    service_config_path,
                    skip_reason,
                )
                continue
            config["rules"].append(
                generate_instance_uwsgi_scaling_config(instance, instance_config)
            )

    return config


def update_prometheus_adapter_config(
    kube_client: KubeClient, config: PrometheusAdapterConfig
) -> None:
    kube_client.core.replace_namespaced_config_map(
        name="paasta-prometheus-adapter-configmap",
        namespace="paasta",
        body=V1ConfigMap(
            metadata=V1ObjectMeta(name="paasta-prometheus-adapter-configmap"),
            data=config,
        ),
    )


def create_prometheus_adapter_config(
    kube_client: KubeClient, config: PrometheusAdapterConfig
) -> None:
    kube_client.core.create_namespaced_config_map(
        namespace="paasta",
        body=V1ConfigMap(
            metadata=V1ObjectMeta(name="paasta-prometheus-adapter-configmap"),
            data=config,
        ),
    )


def get_prometheus_adapter_config(
    kube_client: KubeClient,
) -> Optional[PrometheusAdapterConfig]:
    try:
        config = cast(
            # we cast since mypy infers the wrong type since the k8s clientlib is untyped
            V1ConfigMap,
            kube_client.core.read_namespaced_config_map(
                name="paasta-prometheus-adapter-configmap", namespace="paasta"
            ),
        )
    except ApiException as e:
        if e.status == 404:
            return None
        else:
            raise

    if not config:
        return None

    return config.data


def main() -> None:
    # TODO: exit early if not k8s leader
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    log.info("Generating adapter config from soaconfigs.")
    config = generate_prometheus_adapter_config(
        paasta_cluster=args.cluster, soa_dir=args.soa_dir
    )
    log.info("Generated adapter config from soaconfigs.")

    kube_client = KubeClient()
    # TODO: if we keep dry-run, do we wrap this?
    ensure_namespace(kube_client, namespace="paasta")

    existing_config = get_prometheus_adapter_config(kube_client=kube_client)
    if existing_config and existing_config != config:
        log.info("Existing config differs from soaconfigs - updating.")
        if not args.dry_run:
            update_prometheus_adapter_config(kube_client=kube_client, config=config)
        log.info("Updated adapter config.")
    elif existing_config:
        log.info("Existing config matches soaconfigs - exiting.")
        return
    else:
        log.info("No existing config - creating.")
        if not args.dry_run:
            create_prometheus_adapter_config(kube_client=kube_client, config=config)
        log.info("Created adapter config.")


if __name__ == "__main__":
    main()
