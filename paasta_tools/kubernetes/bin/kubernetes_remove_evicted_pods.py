#!/usr/bin/env python
# Copyright 2015-2019 Yelp Inc.
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
"""
Usage: ./kubernetes_remove_evicted_pods.py [options]

Removes Evicted pods and notifies service owners
"""
import argparse
import logging
from collections import defaultdict
from collections import namedtuple
from typing import Dict
from typing import List
from typing import Mapping
from typing import Sequence

from kubernetes.client import V1DeleteOptions
from pysensu_yelp import Status

from paasta_tools.kubernetes_tools import get_all_pods
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import V1Pod
from paasta_tools.monitoring_tools import send_event
from paasta_tools.utils import DEFAULT_SOA_DIR


log = logging.getLogger(__name__)
EvictedPod = namedtuple("EvictedPod", ["podname", "namespace", "eviction_msg"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Removes evicted pods and notifies service owners"
    )
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", dest="verbose", default=False
    )
    parser.add_argument(
        "-n", "--dry-run", action="store_true", dest="dry_run", default=False
    )
    args = parser.parse_args()
    return args


def get_evicted_pods(pods: Sequence[V1Pod]) -> Sequence[V1Pod]:
    return [
        pod
        for pod in pods
        if pod.status.phase == "Failed" and pod.status.reason == "Evicted"
    ]


def get_pod_service(pod: V1Pod) -> str:
    if pod.metadata.labels is not None:
        return pod.metadata.labels.get("paasta.yelp.com/service")
    else:
        return None


def notify_service_owners(
    services: Mapping[str, Sequence[EvictedPod]], soa_dir: str, dry_run: bool,
) -> None:
    check_overrides = {
        "page": False,
        "alert_after": "0m",
        "realert_every": 1,
        "tip": "Pods can be Evicted if they go over the allowed quota for a given resource. Check the Eviction message to figure out which resource quota was breached",
    }
    for service in services.keys():
        check_name = f"pod-eviction.{service}"
        check_output = "The following pods have been evicted and will be removed from the cluster:\n"
        for pod in services[service]:
            check_output += f"- {pod.podname}: {pod.eviction_msg}\n"
        if dry_run:
            log.info(f"Would have notified owners for service {service}")
        else:
            log.info(f"Notifying owners for service {service}")
            send_event(
                service,
                check_name,
                check_overrides,
                Status.CRITICAL,
                check_output,
                soa_dir,
            )


def remove_pods(
    client: KubeClient, services: Mapping[str, Sequence[EvictedPod]], dry_run: bool,
) -> None:
    delete_options = V1DeleteOptions()
    for service in services:
        # Do not remove more than 2 pods per run
        for pod in services[service][0:2]:
            if dry_run:
                log.info(f"Would have removed pod {pod.podname}")
            else:
                client.core.delete_namespaced_pod(
                    pod.podname,
                    pod.namespace,
                    body=delete_options,
                    grace_period_seconds=0,
                    propagation_policy="Background",
                )
                log.info(f"Removing pod {pod.podname}")


def evicted_pods_per_service(client: KubeClient,) -> Mapping[str, Sequence[EvictedPod]]:
    all_pods = get_all_pods(kube_client=client, namespace="")
    evicted_pods = get_evicted_pods(all_pods)
    log.info(f"Pods in evicted state: {[pod.metadata.name for pod in evicted_pods]}")
    evicted_pods_aggregated: Dict[str, List[EvictedPod]] = defaultdict(list)
    for pod in evicted_pods:
        service = get_pod_service(pod)
        if service:
            evicted_pods_aggregated[service].append(
                EvictedPod(
                    pod.metadata.name, pod.metadata.namespace, pod.status.message
                )
            )
        else:
            log.info(f"Could not get service name for pod {pod.metadata.name}")
    return evicted_pods_aggregated


def main() -> None:
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    kube_client = KubeClient()

    evicted_pods = evicted_pods_per_service(kube_client)
    remove_pods(kube_client, evicted_pods, args.dry_run)


if __name__ == "__main__":
    main()
