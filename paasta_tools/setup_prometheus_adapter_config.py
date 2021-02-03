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
import re
import sys
from pathlib import Path
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import ruamel.yaml as yaml
from kubernetes.client import V1ConfigMap
from kubernetes.client import V1ObjectMeta
from kubernetes.client.rest import ApiException
from mypy_extensions import TypedDict

from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import get_kubernetes_app_name
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.long_running_service_tools import AutoscalingParamsDict
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster

log = logging.getLogger(__name__)

PROMETHEUS_ADAPTER_CONFIGMAP_NAMESPACE = "custom-metrics"
PROMETHEUS_ADAPTER_CONFIGMAP_NAME = "adapter-config"
PROMETHEUS_ADAPTER_CONFIGMAP_FILENAME = "config.yaml"


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
    instance: str, autoscaling_config: AutoscalingParamsDict,
) -> Tuple[bool, Optional[str]]:
    """
    Determines whether we should configure the prometheus adapter for a given service.
    Returns a 2-tuple of (should_create, reason_to_skip)
    """
    if autoscaling_config["metrics_provider"] == "uwsgi":
        if not autoscaling_config.get("use_prometheus", False):
            return False, "requested uwsgi autoscaling, but not using Prometheus"

        return True, None

    return False, "did not request uwsgi autoscaling"


def create_instance_uwsgi_scaling_rule(
    service: str,
    instance: str,
    autoscaling_config: AutoscalingParamsDict,
    paasta_cluster: str,
) -> PrometheusAdapterRule:
    """
    Creates a Prometheus adapter rule config for a given service instance.
    """
    setpoint = autoscaling_config["setpoint"]
    moving_average_window = autoscaling_config["moving_average_window_seconds"]
    deployment_name = get_kubernetes_app_name(service=service, instance=instance)
    worker_filter_terms = f"paasta_cluster='{paasta_cluster}',paasta_service='{service}',paasta_instance='{instance}'"
    replica_filter_terms = (
        f"paasta_cluster='{paasta_cluster}',deployment='{deployment_name}'"
    )
    metrics_query = f"""
            avg_over_time(
                (
                    sum(
                        avg(
                            uwsgi_worker_busy{{{worker_filter_terms}}}
                        ) by (kube_pod, kube_deployment)
                    ) by (kube_deployment) / {setpoint}
                )[{moving_average_window}s:]
            ) / scalar(sum(kube_deployment_spec_replicas{{{replica_filter_terms}}}))
    """
    metric_name = (
        f"{deployment_name}-uwsgi-prom"
    )

    return {
        "name": {"as": metric_name},
        "seriesQuery": f"uwsgi_worker_busy{{{worker_filter_terms}}}",
        "resources": {"template": "kube_<<.Resource>>"},
        # our nicely formatted query has enough whitespace that collapsing said
        # whitespace cuts down the size of the string by a meaningful amount
        "metricsQuery": re.sub(pattern=r"\s+", repl=" ", string=metrics_query).strip(),
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
    # get_services_for_cluster() returns a list of (service, instance) tuples, but this
    # is not great for us: if we were to iterate over that we'd end up getting duplicates
    # for every service as PaastaServiceConfigLoader does not expose a way to get configs
    # for a single instance by name. instead, we get the unique set of service names and then
    # let PaastaServiceConfigLoader iterate over instances for us later
    services = {
        service_name
        for service_name, _ in get_services_for_cluster(
            cluster=paasta_cluster, instance_type="kubernetes", soa_dir=str(soa_dir)
        )
    }
    for service_name in services:
        config_loader = PaastaServiceConfigLoader(
            service=service_name, soa_dir=str(soa_dir)
        )
        for instance_config in config_loader.instance_configs(
            cluster=paasta_cluster, instance_type_class=KubernetesDeploymentConfig,
        ):
            instance_name = instance_config.instance
            autoscaling_config = instance_config.get_autoscaling_params()
            should_create, skip_reason = should_create_uwsgi_scaling_rule(
                instance=instance_config.instance,
                autoscaling_config=autoscaling_config,
            )
            if not should_create:
                log.debug(
                    "Skipping %s - %s.", instance_name, skip_reason,
                )
                continue
            rules.append(
                create_instance_uwsgi_scaling_rule(
                    service=service_name,
                    instance=instance_name,
                    autoscaling_config=autoscaling_config,
                    paasta_cluster=paasta_cluster,
                )
            )

    return {
        "rules": rules,
    }


def update_prometheus_adapter_configmap(
    kube_client: KubeClient, config: PrometheusAdapterConfig
) -> None:
    kube_client.core.replace_namespaced_config_map(
        name=PROMETHEUS_ADAPTER_CONFIGMAP_NAME,
        namespace=PROMETHEUS_ADAPTER_CONFIGMAP_NAMESPACE,
        body=V1ConfigMap(
            metadata=V1ObjectMeta(name=PROMETHEUS_ADAPTER_CONFIGMAP_NAME),
            data={
                PROMETHEUS_ADAPTER_CONFIGMAP_FILENAME: yaml.dump(
                    config,
                    default_flow_style=False,
                    explicit_start=True,
                    width=sys.maxsize,
                )
            },
        ),
    )


def create_prometheus_adapter_configmap(
    kube_client: KubeClient, config: PrometheusAdapterConfig
) -> None:
    kube_client.core.create_namespaced_config_map(
        namespace=PROMETHEUS_ADAPTER_CONFIGMAP_NAMESPACE,
        body=V1ConfigMap(
            metadata=V1ObjectMeta(name=PROMETHEUS_ADAPTER_CONFIGMAP_NAME),
            data={
                PROMETHEUS_ADAPTER_CONFIGMAP_FILENAME: yaml.dump(
                    config, default_flow_style=False, explicit_start=True
                )
            },
        ),
    )


def get_prometheus_adapter_configmap(
    kube_client: KubeClient,
) -> Optional[PrometheusAdapterConfig]:
    try:
        config = cast(
            # we cast since mypy infers the wrong type since the k8s clientlib is untyped
            V1ConfigMap,
            kube_client.core.read_namespaced_config_map(
                name=PROMETHEUS_ADAPTER_CONFIGMAP_NAME,
                namespace=PROMETHEUS_ADAPTER_CONFIGMAP_NAMESPACE,
            ),
        )
    except ApiException as e:
        if e.status == 404:
            return None
        else:
            raise

    if not config:
        return None

    return yaml.safe_load(config.data[PROMETHEUS_ADAPTER_CONFIGMAP_FILENAME])


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
