#!/usr/bin/env python
# Copyright 2015-2020 Yelp Inc.
#
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
from collections import defaultdict
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import Union

import requests
from prometheus_client.parser import text_string_to_metric_families

from paasta_tools.metrics.metrics_lib import get_metrics_interface
from paasta_tools.utils import KubeStateMetricsCollectorConfigDict
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print


def collect_and_emit_metrics() -> None:
    kube_state_metrics_url = _get_kube_state_metrics_url()
    if kube_state_metrics_url is None:
        paasta_print("kube-state-metrics is not running on this machine; exiting")
        return

    system_paasta_config = load_system_paasta_config()
    collector_config = system_paasta_config.get_kube_state_metrics_collector_config()

    kube_state_metrics_response = requests.get(kube_state_metrics_url)
    metric_family_name_to_family: Dict[str, Any] = {
        family.name: family
        for family in text_string_to_metric_families(kube_state_metrics_response.text)
    }

    metrics_interface = get_metrics_interface("kubernetes.kube-state-metrics")
    cluster = system_paasta_config.get_cluster()
    default_dimensions = {
        "paasta_cluster": cluster,
        "kubernetes_cluster": cluster,  # required so HPA can find these metrics
    }

    for name, dimensions, value in _yield_metrics(
        metric_family_name_to_family, collector_config,
    ):
        dimensions.update(default_dimensions)
        gauge = metrics_interface.create_gauge(name, **dimensions)
        gauge.set(value)


def _get_kube_state_metrics_url() -> Optional[str]:
    try:
        pods = requests.get("http://127.0.0.1:10255/pods").json()
    except requests.ConnectionError:
        return None

    for pod in pods["items"]:
        if pod["metadata"]["namespace"] != "kube-system":
            pass

        for container in pod["spec"]["containers"]:
            if container["name"] == "kube-state-metrics":
                for port_metadata in container["ports"]:
                    if port_metadata["name"] == "http-metrics":
                        ip = pod["status"]["podIP"]
                        port = port_metadata["containerPort"]
                        return f"http://{ip}:{port}/metrics"
    return None


def _yield_metrics(
    metric_family_name_to_family: Dict[str, Any],
    collector_config: KubeStateMetricsCollectorConfigDict,
) -> Iterator[Tuple[str, Dict[str, str], float]]:
    label_key_to_labels = _extract_label_key_to_labels(
        metric_family_name_to_family,
        collector_config.get("label_metric_to_label_key", {}),
    )
    label_renames = collector_config.get("label_renames", {})

    for metric_name in collector_config.get("unaggregated_metrics", []):
        if metric_name in metric_family_name_to_family:
            family = metric_family_name_to_family[metric_name]
            for sample in family.samples:
                final_dimensions = _update_dimensions(
                    sample.labels, label_key_to_labels, label_renames,
                )
                yield sample.name, final_dimensions, sample.value

    summed_metric_to_group_keys = collector_config.get(
        "summed_metric_to_group_keys", {}
    )
    for metric_name in summed_metric_to_group_keys:
        if metric_name in metric_family_name_to_family:
            yield from _sum_metrics(
                metric_family_name_to_family[metric_name],
                summed_metric_to_group_keys[metric_name],
                label_key_to_labels,
                label_renames,
            )


def _extract_label_key_to_labels(
    metric_family_name_to_family: Dict[str, Any],
    label_metric_to_label_key: Dict[str, Union[List[str], Dict[str, List[str]]]],
) -> Dict[str, Dict[str, Dict[str, str]]]:
    label_key_to_labels: Dict[str, Dict[str, Dict[str, str]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    for label_metric, key_mapping in label_metric_to_label_key.items():
        if isinstance(key_mapping, list):
            label_key, dest_key = key_mapping
            label_key = key_mapping[0]
            dest_keys = [key_mapping[1]]
        else:
            label_key = key_mapping["source_key"]
            dest_keys = key_mapping["destination_keys"]

        family = metric_family_name_to_family[label_metric]
        for sample in family.samples:
            label_key_value = sample.labels.pop(label_key)
            for dest_key in dest_keys:
                label_key_to_labels[dest_key][label_key_value].update(sample.labels)

    return label_key_to_labels


def _sum_metrics(
    family: Any,
    group_keys: List[str],
    label_key_to_labels: Dict[str, Dict[str, Dict[str, str]]],
    label_renames: Dict[str, str],
) -> Iterator[Tuple[str, Dict[str, str], float]]:
    sums: Dict[Tuple, float] = defaultdict(float)
    for sample in family.samples:
        updated_labels = _update_dimensions(
            sample.labels, label_key_to_labels, label_renames,
        )
        group_values = tuple(updated_labels.get(key, "None") for key in group_keys)
        sums[group_values] += sample.value

    for group_values, metric_value in sums.items():
        dimensions = dict(zip(group_keys, group_values))
        yield family.name, dimensions, metric_value


def _update_dimensions(
    dimensions: Dict[str, str],
    label_key_to_labels: Dict[str, Dict[str, Dict[str, str]]],
    label_renames: Dict[str, str],
) -> Dict[str, str]:
    final_dimensions = dict(dimensions)
    for label_key, label_key_value_to_dims in label_key_to_labels.items():
        if label_key in dimensions:
            label_key_value = dimensions[label_key]
            extra_dims: Mapping[str, str] = label_key_value_to_dims.get(
                label_key_value, {}
            )
            final_dimensions.update(extra_dims)

    for label_name, label_new_name in label_renames.items():
        if label_name in final_dimensions:
            final_dimensions[label_new_name] = final_dimensions.pop(label_name)

    return final_dimensions


if __name__ == "__main__":
    collect_and_emit_metrics()
