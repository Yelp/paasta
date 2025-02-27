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
import collections
import os
import socket
from typing import AbstractSet
from typing import Any
from typing import Collection
from typing import DefaultDict
from typing import Dict
from typing import FrozenSet
from typing import Iterable
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple

import requests
import yaml
from kubernetes.client import V1Pod
from mypy_extensions import TypedDict

from paasta_tools.utils import get_user_agent


class EnvoyBackend(TypedDict, total=False):
    address: str
    port_value: int
    hostname: str
    eds_health_status: str
    weight: int
    has_associated_task: bool


def are_services_up_in_pod(
    envoy_host: str,
    envoy_admin_port: int,
    envoy_admin_endpoint_format: str,
    registrations: Collection[str],
    pod_ip: str,
    pod_port: int,
) -> bool:
    """Returns whether a service in a k8s pod is reachable via envoy
    :param envoy_host: The host that this check should contact for replication information.
    :param envoy_admin_port: The port that Envoy's admin interface is listening on
    :param registrations: The service_name.instance_name of the services
    :param pod_ip: IP of the pod itself
    :param pod_port: The port to reach the service in the pod
    """

    for registration in registrations:
        backends_per_registration = get_backends(
            registration,
            envoy_host=envoy_host,
            envoy_admin_port=envoy_admin_port,
            envoy_admin_endpoint_format=envoy_admin_endpoint_format,
        )

        healthy_backends = [
            backend
            for backend in backends_per_registration.get(registration, [])
            if backend[0]["address"] == pod_ip
            and backend[0]["port_value"] == pod_port
            and backend[0]["eds_health_status"] == "HEALTHY"
        ]

        if not healthy_backends:
            return False

    return True


def are_namespaces_up_in_eds(
    envoy_eds_path: str,
    namespaces: Collection[str],
    pod_ip: str,
    pod_port: int,
) -> bool:
    """Returns whether a Pod is registered on Envoy through the EDS
    :param envoy_eds_path: path where EDS yaml files are stored
    :param namespaces: list of namespaces to check
    :param pod_ip: IP of the pod
    :param pod_port: The port to reach the service in the pod
    """

    for namespace in namespaces:
        backends_from_eds = get_backends_from_eds(namespace, envoy_eds_path)
        if (pod_ip, pod_port) not in backends_from_eds:
            return False

    return True


def retrieve_envoy_clusters(
    envoy_host: str, envoy_admin_port: int, envoy_admin_endpoint_format: str
) -> Dict[str, Any]:
    envoy_uri = envoy_admin_endpoint_format.format(
        host=envoy_host, port=envoy_admin_port, endpoint="clusters?format=json"
    )

    # timeout after 3 seconds and retry 3 times
    envoy_admin_request = requests.Session()
    envoy_admin_request.headers.update({"User-Agent": get_user_agent()})
    envoy_admin_request.mount("http://", requests.adapters.HTTPAdapter(max_retries=3))
    envoy_admin_request.mount("https://", requests.adapters.HTTPAdapter(max_retries=3))
    envoy_admin_response = envoy_admin_request.get(envoy_uri, timeout=3)
    return envoy_admin_response.json()


def get_casper_endpoints(
    clusters_info: Mapping[str, Any]
) -> FrozenSet[Tuple[str, int]]:
    """Filters out and returns casper endpoints from Envoy clusters."""
    casper_endpoints: Set[Tuple[str, int]] = set()
    for cluster_status in clusters_info["cluster_statuses"]:
        if "host_statuses" in cluster_status:
            if cluster_status["name"].startswith("spectre.") and cluster_status[
                "name"
            ].endswith(".egress_cluster"):
                for host_status in cluster_status["host_statuses"]:
                    casper_endpoints.add(
                        (
                            host_status["address"]["socket_address"]["address"],
                            host_status["address"]["socket_address"]["port_value"],
                        )
                    )
    return frozenset(casper_endpoints)


def get_backends_from_eds(namespace: str, envoy_eds_path: str) -> List[Tuple[str, int]]:
    """Returns a list of backends for a given namespace. Casper backends are also returned (if present).

    :param namespace: return backends for this namespace
    :param envoy_eds_path: path where EDS yaml files are stored
    :returns backends: a list of touples representing the backends for
                       the requested service
    """
    backends = []
    eds_file_for_namespace = f"{envoy_eds_path}/{namespace}/{namespace}.yaml"

    if os.access(eds_file_for_namespace, os.R_OK):
        with open(eds_file_for_namespace) as f:
            eds_yaml = yaml.safe_load(f)
            for resource in eds_yaml.get("resources", []):
                endpoints = resource.get("endpoints")
                # endpoints could be None if there are no backends listed
                if endpoints:
                    for endpoint in endpoints:
                        for lb_endpoint in endpoint.get("lb_endpoints", []):
                            address = lb_endpoint["endpoint"]["address"][
                                "socket_address"
                            ]["address"]
                            port_value = lb_endpoint["endpoint"]["address"][
                                "socket_address"
                            ]["port_value"]
                            backends.append((address, port_value))
    return backends


def get_backends(
    service: str,
    envoy_host: str,
    envoy_admin_port: int,
    envoy_admin_endpoint_format: str,
) -> Dict[str, List[Tuple[EnvoyBackend, bool]]]:
    """Fetches JSON from Envoy admin's /clusters endpoint and returns a list of backends.

    :param service: If None, return backends for all services, otherwise only return backends for this particular
                    service.
    :param envoy_host: The host that this check should contact for replication information.
    :param envoy_admin_port: The port that Envoy's admin interface is listening on
    :param envoy_admin_endpoint_format: The format of Envoy's admin endpoint
    :returns backends: A list of dicts representing the backends of all
                       services or the requested service
    """
    if service:
        services = [service]
    else:
        services = None
    return get_multiple_backends(
        services,
        envoy_host=envoy_host,
        envoy_admin_port=envoy_admin_port,
        envoy_admin_endpoint_format=envoy_admin_endpoint_format,
    )


def get_multiple_backends(
    services: Optional[Sequence[str]],
    envoy_host: str,
    envoy_admin_port: int,
    envoy_admin_endpoint_format: str,
) -> Dict[str, List[Tuple[EnvoyBackend, bool]]]:
    """Fetches JSON from Envoy admin's /clusters endpoint and returns a list of backends.

    :param services: If None, return backends for all services, otherwise only return backends for these particular
                     services.
    :param envoy_host: The host that this check should contact for replication information.
    :param envoy_admin_port: The port that Envoy's admin interface is listening on
    :param envoy_admin_endpoint_format: The format of Envoy's admin endpoint
    :returns backends: A list of dicts representing the backends of all
                       services or the requested service
    """
    clusters_info = retrieve_envoy_clusters(
        envoy_host=envoy_host,
        envoy_admin_port=envoy_admin_port,
        envoy_admin_endpoint_format=envoy_admin_endpoint_format,
    )

    casper_endpoints = get_casper_endpoints(clusters_info)

    backends: DefaultDict[
        str, List[Tuple[EnvoyBackend, bool]]
    ] = collections.defaultdict(list)
    for cluster_status in clusters_info["cluster_statuses"]:
        if "host_statuses" in cluster_status:
            if cluster_status["name"].endswith(".egress_cluster"):
                service_name = cluster_status["name"][: -len(".egress_cluster")]

                if services is None or service_name in services:
                    cluster_backends = []
                    casper_endpoint_found = False
                    for host_status in cluster_status["host_statuses"]:
                        address = host_status["address"]["socket_address"]["address"]
                        port_value = host_status["address"]["socket_address"][
                            "port_value"
                        ]

                        # Check if this endpoint is actually a casper backend
                        # If so, omit from the service's list of backends
                        if not service_name.startswith("spectre."):
                            if (address, port_value) in casper_endpoints:
                                casper_endpoint_found = True
                                continue

                        try:
                            hostname = socket.gethostbyaddr(address)[0].split(".")[0]
                        except socket.herror:
                            # Default to the raw IP address if we can't lookup the hostname
                            hostname = address

                        cluster_backends.append(
                            (
                                EnvoyBackend(
                                    address=address,
                                    port_value=port_value,
                                    hostname=hostname,
                                    eds_health_status=host_status["health_status"][
                                        "eds_health_status"
                                    ],
                                    weight=host_status["weight"],
                                ),
                                casper_endpoint_found,
                            )
                        )
                    backends[service_name] += cluster_backends
    return backends


def match_backends_and_pods(
    backends: Iterable[EnvoyBackend],
    pods: Iterable[V1Pod],
) -> List[Tuple[Optional[EnvoyBackend], Optional[V1Pod]]]:
    """Returns tuples of matching (backend, pod) pairs, as matched by IP. Each backend will be listed exactly
    once. If a backend does not match with a pod, (backend, None) will be included.
    If a pod's IP does not match with any backends, (None, pod) will be included.

    :param backends: An iterable of Envoy backend dictionaries, e.g. the list returned by
                     envoy_tools.get_multiple_backends.
    :param pods: A list of pods
    """

    # { ip : [backend1, backend2], ... }
    backends_by_ip: DefaultDict[str, List[EnvoyBackend]] = collections.defaultdict(list)
    backend_pod_pairs = []

    for backend in backends:
        ip = backend["address"]
        backends_by_ip[ip].append(backend)

    for pod in pods:
        ip = pod.status.pod_ip
        for backend in backends_by_ip.pop(ip, [None]):
            backend_pod_pairs.append((backend, pod))

    # we've been popping in the above loop, so anything left didn't match a k8s pod.
    for backends in backends_by_ip.values():
        for backend in backends:
            backend_pod_pairs.append((backend, None))

    return backend_pod_pairs


def build_envoy_location_dict(
    location: str,
    matched_envoy_backends_and_tasks: Sequence[
        Tuple[Optional[EnvoyBackend], Optional[V1Pod]]
    ],
    should_return_individual_backends: bool,
    casper_proxied_backends: AbstractSet[Tuple[str, int]],
) -> MutableMapping[str, Any]:
    running_backends_count = 0
    envoy_backends = []
    is_proxied_through_casper = False
    for backend, task in matched_envoy_backends_and_tasks:
        if backend is None:
            continue
        if backend["eds_health_status"] == "HEALTHY":
            running_backends_count += 1
        if should_return_individual_backends:
            backend["has_associated_task"] = task is not None
            envoy_backends.append(backend)
        if (backend["address"], backend["port_value"]) in casper_proxied_backends:
            is_proxied_through_casper = True
    return {
        "name": location,
        "running_backends_count": running_backends_count,
        "backends": envoy_backends,
        "is_proxied_through_casper": is_proxied_through_casper,
    }


def get_replication_for_all_services(
    envoy_host: str,
    envoy_admin_port: int,
    envoy_admin_endpoint_format: str,
) -> Dict[str, int]:
    """Returns the replication level for all services known to this Envoy

    :param envoy_host: The host that this check should contact for replication information.
    :param envoy_admin_port: The port number that this check should contact for replication information.
    :param envoy_admin_endpoint_format: The format of Envoy's admin endpoint
    :returns available_instance_counts: A dictionary mapping the service names
                                        to an integer number of available replicas.
    """
    backends = get_multiple_backends(
        services=None,
        envoy_host=envoy_host,
        envoy_admin_port=envoy_admin_port,
        envoy_admin_endpoint_format=envoy_admin_endpoint_format,
    )
    return collections.Counter(
        [
            service_name
            for service_name, service_backends in backends.items()
            for b in service_backends
            if backend_is_up(b[0])
        ]
    )


def backend_is_up(backend: EnvoyBackend) -> bool:
    return backend["eds_health_status"] == "HEALTHY"
