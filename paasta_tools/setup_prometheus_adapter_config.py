#!/usr/bin/env python
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
"""
Small utility to update the Prometheus adapter's config to match soaconfigs.
"""
import argparse
import logging
import sys
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import ruamel.yaml as yaml
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
    # used for discovering what resources should be scaled
    seriesQuery: str
    # configuration for how to expose this rule to the HPA
    name: Dict[str, str]
    # used to associate metrics with k8s resources
    resources: Dict[str, str]
    # the actual query we want to send to Prometheus to use for scaling
    metricsQuery: str


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


def should_create_uwsgi_scaling_rule(
    instance: str, instance_config: Dict[str, Any],
) -> Tuple[bool, Optional[str]]:
    """
    Determines whether we should configure the prometheus adapter for a given service.
    Returns a 2-tuple of (should_create, reason_to_skip)
    """
    return False, ""


def create_instance_uwsgi_scaling_rule(
    service: str, instance: str, instance_config: Dict[str, Any], paasta_cluster: str
) -> PrometheusAdapterRule:
    """
    Creates a Prometheus adapter rule config for a given service instance.
    """
    return {
        "name": {"as": ""},
        "seriesQuery": "",
        "resources": {"template": "kube_<<.Resource>>"},
        "metricsQuery": "",
    }


def create_prometheus_adapter_config(
    paasta_cluster: str, soa_dir: Path
) -> PrometheusAdapterConfig:
    """
    Given a paasta cluster and a soaconfigs directory, create the necessary Prometheus adapter
    config to autoscale services.
    Currently supports the following metrics providers:
        * uwsgi
    """
    rules: List[PrometheusAdapterRule] = []
    # we don't know ahead of time what services are autoscaled, so we need to figure out
    # what services are running in our cluster and then check if they're autoscaled
    for service_config_path in soa_dir.glob(f"*/kubernetes-{paasta_cluster}.yaml"):
        service_name = str(service_config_path.relative_to(soa_dir).parent)
        service_config = yaml.safe_load(service_config_path.read_text())
        for instance, instance_config in service_config.items():
            should_create, skip_reason = should_create_uwsgi_scaling_rule(
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
            rules.append(
                create_instance_uwsgi_scaling_rule(
                    service=service_name,
                    instance=instance,
                    instance_config=instance_config,
                    paasta_cluster=paasta_cluster,
                )
            )

    return {
        "rules": rules,
    }


def update_prometheus_adapter_configmap(
    kube_client: KubeClient, config: PrometheusAdapterConfig
) -> None:
    pass


def create_prometheus_adapter_configmap(
    kube_client: KubeClient, config: PrometheusAdapterConfig
) -> None:
    pass


def get_prometheus_adapter_configmap(
    kube_client: KubeClient,
) -> Optional[PrometheusAdapterConfig]:
    return {"rules": []}


def restart_prometheus_adapter(kube_client: KubeClient) -> None:
    pass


def main() -> int:
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    log.info("Generating adapter config from soaconfigs.")
    config = create_prometheus_adapter_config(
        paasta_cluster=args.cluster, soa_dir=args.soa_dir
    )
    log.info("Generated adapter config from soaconfigs.")
    if args.dry_run:
        log.info(
            "Generated the following config:\n%s",
            yaml.dump(
                config, default_flow_style=False, explicit_start=True, width=sys.maxsize
            ),
        )
        return 0  # everything after this point requires creds/updates state
    else:
        log.debug(
            "Generated the following config:\n%s",
            yaml.dump(
                config, default_flow_style=False, explicit_start=True, width=sys.maxsize
            ),
        )

    if not config["rules"]:
        log.error("Got empty rule configuration - refusing to continue.")
        return 1

    kube_client = KubeClient()
    if not args.dry_run:
        ensure_namespace(kube_client, namespace="paasta")
        ensure_namespace(kube_client, namespace="custom-metrics")

    existing_config = get_prometheus_adapter_configmap(kube_client=kube_client)
    if existing_config and existing_config != config:
        log.info("Existing config differs from soaconfigs - updating.")
        update_prometheus_adapter_configmap(kube_client=kube_client, config=config)
        log.info("Updated adapter config.")
    elif existing_config:
        log.info("Existing config matches soaconfigs - exiting.")
        return 0
    else:
        log.info("No existing config - creating.")
        create_prometheus_adapter_configmap(kube_client=kube_client, config=config)
        log.info("Created adapter config.")

    # the prometheus adapter doesn't currently have a good way to reload on config changes
    # so we do the next best thing: restart the pod so that it picks up the new config.
    # see: https://github.com/DirectXMan12/k8s-prometheus-adapter/issues/104
    restart_prometheus_adapter(kube_client=kube_client)

    return 0


if __name__ == "__main__":
    sys.exit(main())
