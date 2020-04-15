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
import errno
import socket
from typing import AbstractSet
from typing import Any
from typing import Collection
from typing import DefaultDict
from typing import Dict
from typing import FrozenSet
from typing import Iterable
from typing import List
from typing import MutableMapping
from typing import Optional
from typing import Set
from typing import Tuple

import requests
import staticconf
from mypy_extensions import TypedDict

from paasta_tools import marathon_tools
from paasta_tools.api import settings
from paasta_tools.utils import get_user_agent
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import SystemPaastaConfig


ENVOY_TOGGLES_CONFIG_NAMESPACE = "envoy_toggles"
ENVOY_TOGGLES_CONFIG_FILE = "/nail/srv/configs/envoy_toggles.yaml"
ENVOY_DEFAULT_ENABLED = False
ENVOY_FULL_MESH_CONFIG_NAMESPACE = "envoy_full_mesh_toggles"
ENVOY_FULL_MESH_CONFIG_FILE = "/nail/srv/configs/envoy_full_mesh_toggles.yaml"
ENVOY_DEFAULT_FULL_MESH = False


EnvoyBackend = TypedDict(
    "EnvoyBackend",
    {
        "address": str,
        "port_value": int,
        "hostname": str,
        "eds_health_status": str,
        "weight": int,
        "has_associated_task": bool,
    },
    total=False,
)


def service_is_in_envoy(
    service_name: str, config_file: str = ENVOY_TOGGLES_CONFIG_FILE
) -> bool:
    try:
        staticconf.YamlConfiguration(
            config_file, namespace=ENVOY_TOGGLES_CONFIG_NAMESPACE
        )
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise

    envoy_enabled = staticconf.get_bool(
        service_name,
        default=ENVOY_DEFAULT_ENABLED,
        namespace=ENVOY_TOGGLES_CONFIG_NAMESPACE,
    )

    return envoy_enabled


def service_is_full_mesh(
    service_name: str, config_file: str = ENVOY_FULL_MESH_CONFIG_FILE
) -> bool:
    try:
        staticconf.YamlConfiguration(
            config_file, namespace=ENVOY_FULL_MESH_CONFIG_NAMESPACE
        )
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise

    envoy_enabled = staticconf.get_bool(
        service_name,
        default=ENVOY_DEFAULT_FULL_MESH,
        namespace=ENVOY_FULL_MESH_CONFIG_NAMESPACE,
    )

    return envoy_enabled


def retrieve_envoy_clusters(
    envoy_host: str, envoy_admin_port: int, system_paasta_config: SystemPaastaConfig
) -> Dict[str, Any]:
    envoy_uri = system_paasta_config.get_envoy_admin_endpoint_format().format(
        host=envoy_host, port=envoy_admin_port, endpoint="clusters?format=json"
    )

    # timeout after 1 second and retry 3 times
    envoy_admin_request = requests.Session()
    envoy_admin_request.headers.update({"User-Agent": get_user_agent()})
    envoy_admin_request.mount("http://", requests.adapters.HTTPAdapter(max_retries=3))
    envoy_admin_request.mount("https://", requests.adapters.HTTPAdapter(max_retries=3))
    envoy_admin_response = envoy_admin_request.get(envoy_uri, timeout=1)
    return envoy_admin_response.json()


def get_casper_endpoints(clusters_info: Dict[str, Any]) -> FrozenSet[Tuple[str, int]]:
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


def get_backends(
    service: str, envoy_host: str, envoy_admin_port: int,
) -> List[Tuple[EnvoyBackend, bool]]:
    """Fetches JSON from Envoy admin's /clusters endpoint and returns a list of backends.

    :param service: If None, return backends for all services, otherwise only return backends for this particular
                    service.
    :param envoy_host: The host that this check should contact for replication information.
    :param envoy_admin_port: The port that Envoy's admin interface is listening on
    :returns backends: A list of dicts representing the backends of all
                       services or the requested service
    """
    if service:
        services = [service]
    else:
        services = None
    return get_multiple_clusters(
        services,
        envoy_host=envoy_host,
        envoy_admin_port=envoy_admin_port,
        cluster_type="egress",
    )


def get_frontends(
    service: str, envoy_host: str, envoy_admin_port: int,
) -> List[Tuple[EnvoyBackend, bool]]:
    """Fetches JSON from Envoy admin's /clusters endpoint and returns a list of frontends.

    :param service: If None, return frontends for all services, otherwise only return frontends for this particular
                    service.
    :param envoy_host: The host that this check should contact for replication information.
    :param envoy_admin_port: The port that Envoy's admin interface is listening on
    :returns frontends: A list of dicts representing the frontends of all
                       services or the requested service
    """
    if service:
        services = [service]
    else:
        services = None
    return get_multiple_clusters(
        services,
        envoy_host=envoy_host,
        envoy_admin_port=envoy_admin_port,
        cluster_type="ingress",
    )


def get_multiple_clusters(
    services: Optional[Collection[str]],
    envoy_host: str,
    envoy_admin_port: int,
    cluster_type: str,
):
    """Fetches JSON from Envoy admin's /clusters endpoint and returns a list of clusters.

    :param services: If None, return clusters for all services, otherwise only return clusters for these particular
                     services.
    :param envoy_host: The host that this check should contact for replication information.
    :param envoy_admin_port: The port that Envoy's admin interface is listening on
    :param cluster_type: One of "ingress" or "egress"
    :returns clusters: A list of dicts representing the clusters of all
                       services or the requested service
    """
    clusters_info = retrieve_envoy_clusters(
        envoy_host=envoy_host,
        envoy_admin_port=envoy_admin_port,
        system_paasta_config=settings.system_paasta_config,
    )

    casper_endpoints = get_casper_endpoints(clusters_info)

    clusters: List[Tuple[EnvoyBackend, bool]] = []
    for cluster_status in clusters_info["cluster_statuses"]:
        if "host_statuses" in cluster_status:
            if cluster_status["name"].endswith(f".{cluster_type}_cluster"):

                # Extract service name from the cluster name
                service_name = ".".join(cluster_status["name"].split(".", 3)[0:2])

                if services is None or service_name in services:
                    cluster_sets = []
                    casper_endpoint_found = False
                    for host_status in cluster_status["host_statuses"]:
                        address = host_status["address"]["socket_address"]["address"]
                        port_value = host_status["address"]["socket_address"][
                            "port_value"
                        ]

                        # Check if this endpoint is actually a casper backend (only applies for egress)
                        # If so, omit from the service's list of clusters
                        if not service_name.startswith("spectre."):
                            if (address, port_value) in casper_endpoints:
                                casper_endpoint_found = True
                                continue

                        try:
                            hostname = socket.gethostbyaddr(address)[0].split(".")[0]
                        except socket.herror:
                            # Default to the raw IP address if we can't lookup the hostname
                            hostname = address

                        cluster_sets.append(
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
                    clusters += cluster_sets
    return clusters


def match_backends_and_tasks(
    backends: Iterable[EnvoyBackend], tasks: Iterable[marathon_tools.MarathonTask]
) -> List[Tuple[Optional[EnvoyBackend], Optional[marathon_tools.MarathonTask]]]:
    """Returns tuples of matching (backend, task) pairs, as matched by IP and port. Each backend will be listed exactly
    once, and each task will be listed once per port. If a backend does not match with a task, (backend, None) will
    be included. If a task's port does not match with any backends, (None, task) will be included.

    :param backends: An iterable of Envoy backend dictionaries, e.g. the list returned by
                     envoy_tools.get_multiple_clusters.
    :param tasks: An iterable of MarathonTask objects.
    """

    # { (ip, port) : [backend1, backend2], ... }
    backends_by_ip_port: DefaultDict[
        Tuple[str, int], List[EnvoyBackend]
    ] = collections.defaultdict(list)
    backend_task_pairs = []

    for backend in backends:
        ip = backend["address"]
        port = backend["port_value"]
        backends_by_ip_port[ip, port].append(backend)

    for task in tasks:
        ip = socket.gethostbyname(task.host)
        for port in task.ports:
            for backend in backends_by_ip_port.pop((ip, port), [None]):
                backend_task_pairs.append((backend, task))

    # we've been popping in the above loop, so anything left didn't match a marathon task.
    for backends in backends_by_ip_port.values():
        for backend in backends:
            backend_task_pairs.append((backend, None))

    return backend_task_pairs


def build_envoy_location_dict(
    location: str,
    matched_envoy_backends_and_tasks: List[
        Tuple[Optional[EnvoyBackend], Optional[marathon_tools.MarathonTask]]
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


def are_services_up_in_pod(
    envoy_host: str,
    envoy_admin_port: int,
    registrations: Collection[str],
    host_ip: str,
    pod_ip: str,
    pod_port: int,
) -> bool:
    """Returns whether a service in a k8s pod is reachable via envoy

    :param envoy_host: The host that this check should contact for replication information.
    :param envoy_admin_port: The port that Envoy's admin interface is listening on
    :param registrations: The service_name.instance_name of the services
    :param host_ip: IP of the host where the pod lives
    :param pod_ip: IP of the pod itself
    :param pod_port: The port to reach the service in the pod
    """
    services_with_atleast_one_backend_up = {
        registration: False for registration in registrations
    }
    services_with_atleast_one_frontend_up = {
        registration: False for registration in registrations
    }

    # Needed to find envoy clusters
    settings.system_paasta_config = load_system_paasta_config()

    for registration in registrations:
        backends = get_backends(
            registration, envoy_host=envoy_host, envoy_admin_port=envoy_admin_port
        )

        if service_is_full_mesh(registration):
            # With full mesh, there should exist
            # - an ingress cluster with a single backend matching the
            # pod ip and container port
            # - an egress cluster with one of the backends matching the
            # host ip and an unknown port

            for be in backends:
                if (
                    be[0]["eds_health_status"] == "HEALTHY"
                    and be[0]["address"] == host_ip
                ):
                    services_with_atleast_one_backend_up[registration] = True

            frontends = get_frontends(
                registration, envoy_host=envoy_host, envoy_admin_port=envoy_admin_port,
            )

            for fe in frontends:
                if (
                    fe[0]["eds_health_status"] == "HEALTHY"
                    and fe[0]["address"] == pod_ip
                    and fe[0]["port_value"] == pod_port
                ):
                    services_with_atleast_one_frontend_up[registration] = True
        else:
            # With no full mesh, there should exist
            # - an egress cluster with one of the backends matching the
            # pod ip and container port

            for be in backends:
                if (
                    be[0]["eds_health_status"] == "HEALTHY"
                    and be[0]["address"] == pod_ip
                    and be[0]["port_value"] == pod_port
                ):
                    services_with_atleast_one_backend_up[registration] = True

            # We don't need to validate frontends
            services_with_atleast_one_frontend_up[registration] = True

    return all(services_with_atleast_one_backend_up.values()) and all(
        services_with_atleast_one_frontend_up.values()
    )
