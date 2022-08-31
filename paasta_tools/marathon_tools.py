# Copyright 2015-2016 Yelp Inc.
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
This module contains the meat of the logic for most of the scripts
that interact with marathon. There's config parsers, url composers,
and a number of other things used by other components in order to
make the PaaSTA stack work.
"""
import copy
import datetime
import json
import logging
import multiprocessing
import os
import socket
from collections import defaultdict
from math import ceil
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple
from typing import Type
from typing import TypeVar

import pytz
import requests
import service_configuration_lib
from kazoo.exceptions import NoNodeError
from marathon import MarathonClient
from marathon import MarathonHttpError
from marathon import NotFoundError
from marathon.models.app import MarathonApp
from marathon.models.app import MarathonTask
from marathon.models.queue import MarathonQueueItem
from mypy_extensions import TypedDict

from paasta_tools.long_running_service_tools import BounceMethodConfigDict
from paasta_tools.long_running_service_tools import InvalidHealthcheckMode
from paasta_tools.long_running_service_tools import load_service_namespace_config
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import LongRunningServiceConfigDict
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
from paasta_tools.mesos.exceptions import NoSlavesAvailableError
from paasta_tools.mesos_tools import filter_mesos_slaves_by_blacklist
from paasta_tools.mesos_tools import get_mesos_network_for_net
from paasta_tools.mesos_tools import get_mesos_slaves_grouped_by_attribute
from paasta_tools.mesos_tools import mesos_services_running_here
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.secret_tools import get_secret_hashes
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import Constraint
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import DockerParameter
from paasta_tools.utils import DockerVolume
from paasta_tools.utils import get_code_sha_from_dockerurl
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import get_user_agent
from paasta_tools.utils import load_service_instance_config
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.utils import MarathonConfigDict
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import time_cache
from paasta_tools.utils import ZookeeperPool


# Marathon creates Mesos tasks with an id composed of the app's full name, a
# spacer, and a UUID. This variable is that spacer. Note that we don't control
# this spacer, i.e. you can't change it here and expect the world to change
# with you. We need to know what it is so we can decompose Mesos task ids.
MESOS_TASK_SPACER = "."
PUPPET_SERVICE_DIR = "/etc/nerve/puppet_services.d"
AUTOSCALING_ZK_ROOT = "/autoscaling"


# A set of config attributes that don't get included in the hash of the config.
# These should be things that PaaSTA/Marathon knows how to change without requiring a bounce.
CONFIG_HASH_BLACKLIST = {
    "instances",
    "backoff_seconds",
    "min_instances",
    "max_instances",
}

log = logging.getLogger(__name__)
logging.getLogger("marathon").setLevel(logging.WARNING)


class MarathonServers(NamedTuple):
    current: Sequence["MarathonConfig"]
    previous: Sequence["MarathonConfig"]


_RendezvousHashT = TypeVar("_RendezvousHashT")


def rendezvous_hash(
    choices: Sequence[_RendezvousHashT],
    key: str,
    salt: str = "",
    hash_func: Callable[[str], str] = get_config_hash,
) -> _RendezvousHashT:
    """For each choice, calculate the hash of the index of that choice combined with the key, then return the choice
    whose corresponding hash is highest.

    :param choices: A sequence of arbitrary values. The "winning" value will be returned."""
    max_hash_value = None
    max_hash_choice = None

    if len(choices) == 0:
        raise ValueError("Must pass at least one choice to rendezvous_hash")

    for i, choice in enumerate(choices):
        str_to_hash = MESOS_TASK_SPACER.join([str(i), key, salt])
        hash_value = hash_func(str_to_hash)
        if max_hash_value is None or hash_value > max_hash_value:
            max_hash_value = hash_value
            max_hash_choice = choice

    return max_hash_choice


class MarathonClients:
    def __init__(
        self, current: Sequence[MarathonClient], previous: Sequence[MarathonClient]
    ) -> None:
        self.current = current
        self.previous = previous

    def __repr__(self) -> str:
        return f"MarathonClients(current={self.current!r}, previous={self.previous!r}"

    def get_current_client_for_service(
        self, job_config: "MarathonServiceConfig"
    ) -> MarathonClient:
        service_instance = compose_job_id(job_config.service, job_config.instance)
        if job_config.get_marathon_shard() is not None:
            return self.current[job_config.get_marathon_shard()]
        else:
            return rendezvous_hash(choices=self.current, key=service_instance)

    def get_previous_clients_for_service(
        self, job_config: "MarathonServiceConfig"
    ) -> Sequence[MarathonClient]:
        service_instance = compose_job_id(job_config.service, job_config.instance)
        if job_config.get_previous_marathon_shards() is not None:
            return [self.previous[i] for i in job_config.get_previous_marathon_shards()]
        else:
            try:
                return [rendezvous_hash(choices=self.previous, key=service_instance)]
            except ValueError:
                return []

    def get_all_clients_for_service(
        self, job_config: "MarathonServiceConfig"
    ) -> Sequence[MarathonClient]:
        """Return the set of all clients that a service might have apps on, with no duplicate clients."""
        all_clients = [self.get_current_client_for_service(job_config)]
        all_clients.extend(self.get_previous_clients_for_service(job_config))

        return dedupe_clients(all_clients)

    def get_all_clients(self) -> Sequence[MarathonClient]:
        """Return the set of all unique clients."""
        return dedupe_clients(self.current + self.previous)  # type: ignore


def dedupe_clients(
    all_clients: Iterable[MarathonClient],
) -> Sequence[MarathonClient]:
    """Return a subset of the clients with no servers in common. The assumption here is that if there's any overlap in
    servers, then two clients are talking about the same cluster."""
    all_seen_servers: Set[str] = set()
    deduped_clients: List[MarathonClient] = []
    for client in all_clients:
        if not any(server in all_seen_servers for server in client.servers):
            all_seen_servers.update(client.servers)
            deduped_clients.append(client)

    return deduped_clients


def get_marathon_servers(system_paasta_config: SystemPaastaConfig) -> MarathonServers:
    """
    :param system_paasta_config: A SystemPaastaConfig object representing the system
                                 configuration.
    """
    current = [MarathonConfig(x) for x in system_paasta_config.get_marathon_servers()]
    previous = [
        MarathonConfig(x) for x in system_paasta_config.get_previous_marathon_servers()
    ]
    return MarathonServers(current=current, previous=previous)


class MarathonNotConfigured(Exception):
    pass


class MarathonServiceConfigDict(LongRunningServiceConfigDict, total=False):
    backoff_factor: float
    max_launch_delay_seconds: float
    bounce_method: str
    bounce_health_params: Dict[str, Any]
    accepted_resource_roles: Optional[List[str]]
    host_port: int
    marathon_shard: int
    previous_marathon_shards: List[int]


class CommandDict(TypedDict):
    value: str


class HealthcheckDict(TypedDict, total=False):
    protocol: str
    gracePeriodSeconds: float
    intervalSeconds: float
    portIndex: int
    timeoutSeconds: float
    maxConsecutiveFailures: int
    path: str
    command: str


# These are more-or-less duplicated from native_service_config, but this code uses camelCase, not snake_case.
# We could probably refactor this, and just use snake_case -- Mesos is picky, but marathon-python will auto-convert.
class MarathonDockerPortMapping(TypedDict):
    hostPort: int
    containerPort: int
    protocol: str


class MarathonPortDefinition(TypedDict):
    port: int
    protocol: str


class MarathonDockerInfo(TypedDict, total=False):
    image: str
    network: str
    portMappings: List[MarathonDockerPortMapping]
    parameters: Sequence[DockerParameter]


class MarathonContainerInfo(TypedDict):
    type: str
    docker: MarathonDockerInfo
    volumes: List[DockerVolume]


class FormattedMarathonAppDict(BounceMethodConfigDict, total=False):
    container: MarathonContainerInfo
    uris: List[str]
    backoff_seconds: float
    backoff_factor: float
    max_launch_delay_seconds: float
    health_checks: List[HealthcheckDict]
    env: Dict[str, str]
    mem: float
    cpus: float
    gpus: int
    disk: float
    constraints: List[Constraint]
    cmd: str
    args: List[str]
    id: str
    port_definitions: List[MarathonPortDefinition]
    require_ports: bool
    accepted_resource_roles: List[str]


class MarathonConfig(dict):
    def __init__(self, config: MarathonConfigDict) -> None:
        super().__init__(config)

    @property
    def url(self) -> List[str]:
        return self.get_url()

    @property
    def user(self) -> str:
        return self.get_username()

    @property
    def passwd(self) -> str:
        return self.get_password()

    def get_url(self) -> List[str]:
        """Get the Marathon API url

        :returns: The Marathon API endpoint"""
        try:
            return self["url"]
        except KeyError:
            raise MarathonNotConfigured(
                "Could not find marathon url in system marathon config"
            )

    def get_username(self) -> str:
        """Get the Marathon API username

        :returns: The Marathon API username"""
        try:
            return self["user"]
        except KeyError:
            raise MarathonNotConfigured(
                "Could not find marathon user in system marathon config"
            )

    def get_password(self) -> str:
        """Get the Marathon API password

        :returns: The Marathon API password"""
        try:
            return self["password"]
        except KeyError:
            raise MarathonNotConfigured(
                "Could not find marathon password in system marathon config"
            )


def load_marathon_service_config_no_cache(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> "MarathonServiceConfig":
    """Read a service instance's configuration for marathon.

    If a branch isn't specified for a config, the 'branch' key defaults to
    paasta-${cluster}.${instance}.

    :param name: The service name
    :param instance: The instance of the service to retrieve
    :param cluster: The cluster to read the configuration for
    :param load_deployments: A boolean indicating if the corresponding deployments.json for this service
                             should also be loaded
    :param soa_dir: The SOA configuration directory to read from
    :returns: A dictionary of whatever was in the config for the service instance"""
    general_config = service_configuration_lib.read_service_configuration(
        service, soa_dir=soa_dir
    )
    instance_config = load_service_instance_config(
        service,
        instance,
        "marathon",
        cluster,
        soa_dir=soa_dir,
    )
    general_config = deep_merge_dictionaries(
        overrides=instance_config, defaults=general_config
    )

    branch_dict: Optional[BranchDictV2] = None
    if load_deployments:
        deployments_json = load_v2_deployments_json(service, soa_dir=soa_dir)
        temp_instance_config = MarathonServiceConfig(
            service=service,
            cluster=cluster,
            instance=instance,
            config_dict=general_config,
            branch_dict=None,
            soa_dir=soa_dir,
        )
        branch = temp_instance_config.get_branch()
        deploy_group = temp_instance_config.get_deploy_group()
        branch_dict = deployments_json.get_branch_dict(service, branch, deploy_group)

    return MarathonServiceConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=general_config,
        branch_dict=branch_dict,
        soa_dir=soa_dir,
    )


@time_cache(ttl=5)
def load_marathon_service_config(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> "MarathonServiceConfig":
    """Read a service instance's configuration for marathon.

    If a branch isn't specified for a config, the 'branch' key defaults to
    paasta-${cluster}.${instance}.

    :param name: The service name
    :param instance: The instance of the service to retrieve
    :param cluster: The cluster to read the configuration for
    :param load_deployments: A boolean indicating if the corresponding deployments.json for this service
                             should also be loaded
    :param soa_dir: The SOA configuration directory to read from
    :returns: A dictionary of whatever was in the config for the service instance"""
    return load_marathon_service_config_no_cache(
        service=service,
        instance=instance,
        cluster=cluster,
        load_deployments=load_deployments,
        soa_dir=soa_dir,
    )


class InvalidMarathonConfig(Exception):
    pass


class MarathonServiceConfig(LongRunningServiceConfig):
    config_dict: MarathonServiceConfigDict

    config_filename_prefix = "marathon"

    def __init__(
        self,
        service: str,
        cluster: str,
        instance: str,
        config_dict: MarathonServiceConfigDict,
        branch_dict: Optional[BranchDictV2],
        soa_dir: str = DEFAULT_SOA_DIR,
    ) -> None:
        super().__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
            soa_dir=soa_dir,
        )

    def format_cmd(self) -> str:
        cmd = self.get_cmd()
        if cmd is None:
            return None
        if isinstance(cmd, str):
            return cmd
        elif isinstance(cmd, list):
            return " ".join(cmd)
        else:
            raise ValueError("only list or str accepted for cmd")

    def copy(self) -> "MarathonServiceConfig":
        return self.__class__(
            service=self.service,
            instance=self.instance,
            cluster=self.cluster,
            config_dict=self.config_dict.copy(),
            branch_dict=self.branch_dict.copy()
            if self.branch_dict is not None
            else None,
            soa_dir=self.soa_dir,
        )

    def get_backoff_seconds(self) -> int:
        """backoff_seconds represents a penalization factor for relaunching failing tasks.
        Every time a task fails, Marathon adds this value multiplied by a backoff_factor.
        In PaaSTA we know how many instances a service has, so we adjust the backoff_seconds
        to account for this, which prevents services with large number of instances from
        being penalized more than services with small instance counts. (for example, a service
        with 30 instances will get backed off 10 times faster than a service with 3 instances)."""
        max_instances = self.get_max_instances()
        instances = max_instances if max_instances is not None else self.get_instances()
        if instances == 0:
            return 1
        else:
            return int(ceil(10.0 / instances))

    def get_backoff_factor(self) -> float:
        return self.config_dict.get("backoff_factor", 2)

    def get_max_launch_delay_seconds(self) -> float:
        return self.config_dict.get("max_launch_delay_seconds", 300)

    def get_bounce_method(self) -> str:
        """Get the bounce method specified in the service's marathon configuration.

        :param service_config: The service instance's configuration dictionary
        :returns: The bounce method specified in the config, or 'crossover' if not specified"""
        return self.config_dict.get("bounce_method", "crossover")

    def get_calculated_constraints(
        self,
        system_paasta_config: SystemPaastaConfig,
        service_namespace_config: ServiceNamespaceConfig,
    ) -> List[Constraint]:
        """Gets the calculated constraints for a marathon instance

        If ``constraints`` is specified in the config, it will use that regardless.
        Otherwise it will calculate a good set of constraints from other inputs,
        like ``pool``, blacklist/whitelist, smartstack data, etc.

        :param service_namespace_config: The service instance's configuration dictionary
        :returns: The constraints specified in the config, or defaults described above
        """
        constraints = self.get_constraints()
        if constraints is not None:
            return constraints
        else:
            constraints = self.get_extra_constraints()
            constraints.extend(
                self.get_routing_constraints(
                    service_namespace_config=service_namespace_config,
                    system_paasta_config=system_paasta_config,
                )
            )
            constraints.extend(
                self.get_deploy_constraints(
                    blacklist=self.get_deploy_blacklist(),
                    whitelist=self.get_deploy_whitelist(),
                    system_deploy_blacklist=system_paasta_config.get_deploy_blacklist(),
                    system_deploy_whitelist=system_paasta_config.get_deploy_whitelist(),
                )
            )
            constraints.extend(self.get_pool_constraints())
            constraints.extend(
                self.get_hostname_unique_constraint(
                    system_paasta_config=system_paasta_config
                )
            )
        return constraints

    def get_hostname_unique_constraint(
        self, system_paasta_config: SystemPaastaConfig
    ) -> List[Constraint]:
        """
        "Small" services automatically receive a hostname UNIQUE constraint to reduce
        the risk of all tasks getting launched on the same agent, which might then be lost.

        :param system_paasta_config: A SystemPaastaConfig object representing the system
                                 configuration.
        :returns: a set of constraints for marathon
        """
        auto_hostname_unique_size = system_paasta_config.get_auto_hostname_unique_size()
        app_size = self.get_max_instances() or self.get_desired_instances()
        if app_size <= auto_hostname_unique_size:
            return [["hostname", "UNIQUE"]]
        return []

    def get_routing_constraints(
        self,
        service_namespace_config: ServiceNamespaceConfig,
        system_paasta_config: SystemPaastaConfig,
    ) -> List[Constraint]:
        """
        Returns a set of constraints in order to evenly group a marathon
        application amongst instances of a discovery type.
        If, for example, a given app's 'discover' key is set to 'region', then this function
        computes the constraints required to group the app evenly amongst each
        of the actual 'region' values in the cluster.
        It does so by querying the value of the discover attribute for each expected slave in the cluster (as defined
        by the expected_slave_attributes key in system paasta config), returning a GROUP_BY constraint where the value
        is the number of unique values for that attribute.
        If you have not set expected_slave_attributes in the system paasta config, this function returns an empty list.

        :param service_namespace_config: the config for this service
        :returns: a set of constraints for marathon
        """
        discover_level = service_namespace_config.get_discover()

        expected_slave_attributes = system_paasta_config.get_expected_slave_attributes()
        if expected_slave_attributes is None:
            return []

        fake_slaves = [{"attributes": a} for a in expected_slave_attributes]
        filtered_slaves = filter_mesos_slaves_by_blacklist(
            slaves=fake_slaves,
            blacklist=self.get_deploy_blacklist(),
            whitelist=self.get_deploy_whitelist(),
        )
        # A slave must be allowed by both the instance config's blacklist/whitelist and the system configs' blacklist/
        # whitelist, so we filter twice.
        filtered_slaves = filter_mesos_slaves_by_blacklist(
            slaves=filtered_slaves,
            blacklist=system_paasta_config.get_deploy_blacklist(),
            whitelist=system_paasta_config.get_deploy_whitelist(),
        )

        if not filtered_slaves:
            raise NoSlavesAvailableError(
                (
                    "We do not believe any slaves on the cluster will match the constraints for %s.%s. If you believe "
                    "this is incorrect, have your system administrator adjust the value of expected_slave_attributes "
                    "in the system paasta configs."
                )
                % (self.service, self.instance)
            )

        value_dict = get_mesos_slaves_grouped_by_attribute(
            filtered_slaves, discover_level
        )
        routing_constraints: List[Constraint] = [
            [discover_level, "GROUP_BY", str(len(value_dict.keys()))]
        ]
        return routing_constraints

    def format_marathon_app_dict(
        self, system_paasta_config: Optional[SystemPaastaConfig] = None
    ) -> FormattedMarathonAppDict:
        """Create the configuration that will be passed to the Marathon REST API.

        Currently compiles the following keys into one nice dict:

        - id: the ID of the image in Marathon
        - container: a dict containing the docker url and docker launch options. Needed by deimos.
        - uris: blank.
        - ports: an array containing the port.
        - env: environment variables for the container.
        - mem: the amount of memory required.
        - cpus: the number of cpus required.
        - disk: the amount of disk space required.
        - constraints: the constraints on the Marathon app.
        - instances: the number of instances required.
        - cmd: the command to be executed.
        - args: an alternative to cmd that requires the docker container to have an entrypoint.

        The last 7 keys are retrieved using the get_<key> functions defined above.

        :param app_id: The app id
        :param docker_url: The url to the docker image the app will actually execute
        :param docker_volumes: The docker volumes to run the image with, via the
                               marathon configuration file
        :param service_namespace_config: The service instance's configuration dict
        :returns: A dict containing all of the keys listed above"""

        if system_paasta_config is None:
            system_paasta_config = load_system_paasta_config()
        docker_url = self.get_docker_url(system_paasta_config=system_paasta_config)
        service_namespace_config = load_service_namespace_config(
            service=self.service, namespace=self.get_nerve_namespace()
        )
        docker_volumes = self.get_volumes(
            system_volumes=system_paasta_config.get_volumes()
        )

        net = get_mesos_network_for_net(self.get_net())

        if self.get_container_type() == "DOCKER":
            container_dict: MarathonContainerInfo = {
                "docker": {
                    "image": docker_url,
                    "network": net,
                    "parameters": self.format_docker_parameters(
                        system_paasta_config=system_paasta_config
                    ),
                },
                "type": "DOCKER",
                "volumes": docker_volumes,
            }
        else:
            # Only image and forcePullImage are supported: "Mesos Containerizer and Universal Container Runtime"
            # in http://mesosphere.github.io/marathon/1.4/docs/native-docker.html
            container_dict = {  # type: ignore
                "docker": {"image": docker_url},
                "type": "MESOS",
                "volumes": docker_volumes,
            }

        complete_config: FormattedMarathonAppDict = {
            "container": container_dict,
            "uris": [system_paasta_config.get_dockercfg_location()],
            "backoff_seconds": self.get_backoff_seconds(),
            "backoff_factor": self.get_backoff_factor(),
            "max_launch_delay_seconds": self.get_max_launch_delay_seconds(),
            "health_checks": self.get_healthchecks(
                service_namespace_config=service_namespace_config
            ),
            "env": self.get_env(system_paasta_config=system_paasta_config),
            "mem": float(self.get_mem()),
            "cpus": float(self.get_cpus()),
            "disk": float(self.get_disk()),
            "constraints": self.get_calculated_constraints(
                system_paasta_config=system_paasta_config,
                service_namespace_config=service_namespace_config,
            ),
            "instances": self.get_desired_instances(),
            "cmd": self.format_cmd(),
            "args": self.get_args(),
        }

        if self.get_gpus() is not None:
            complete_config["gpus"] = self.get_gpus()

        # Mesos containerizer does not support portMappings
        if net == "BRIDGE" and complete_config["container"]["type"] == "DOCKER":
            complete_config["container"]["docker"]["portMappings"] = [
                {
                    "containerPort": self.get_container_port(),
                    "hostPort": self.get_host_port(),
                    "protocol": "tcp",
                }
            ]
        else:
            complete_config["port_definitions"] = [
                {"port": self.get_host_port(), "protocol": "tcp"}
            ]
            # Without this, we may end up with multiple containers requiring the same port on the same box.
            complete_config["require_ports"] = self.get_host_port() != 0

        accepted_resource_roles = self.get_accepted_resource_roles()
        if accepted_resource_roles is not None:
            complete_config["accepted_resource_roles"] = accepted_resource_roles

        code_sha = get_code_sha_from_dockerurl(docker_url)

        config_hash = get_config_hash(
            self.sanitize_for_config_hash(complete_config, system_paasta_config),
            force_bounce=self.get_force_bounce(),
        )
        complete_config["id"] = format_job_id(
            self.service, self.instance, code_sha, config_hash
        )

        log.debug("Complete configuration for instance is: %s", complete_config)
        return complete_config

    def sanitize_for_config_hash(
        self, config: FormattedMarathonAppDict, system_paasta_config: SystemPaastaConfig
    ) -> Dict[str, Any]:
        """Removes some data from config to make it suitable for
        calculation of config hash.

        Also adds secret HMACs so that we bounce if secret data has changed.
        We need this because the reference to the secret is all Marathon gets
        and this will not change.

        :param config: complete_config hash to sanitize
        :returns: sanitized copy of complete_config hash
        """
        ahash = {
            key: copy.deepcopy(value)
            for key, value in config.items()
            if key not in CONFIG_HASH_BLACKLIST
        }
        ahash["container"]["docker"][  # type: ignore
            "parameters"
        ] = self.format_docker_parameters(
            with_labels=False, system_paasta_config=system_paasta_config
        )
        secret_hashes = get_secret_hashes(
            environment_variables=config["env"],
            secret_environment=system_paasta_config.get_vault_environment(),
            service=self.service,
            soa_dir=self.soa_dir,
        )
        if secret_hashes:
            ahash["paasta_secrets"] = secret_hashes
        return ahash

    def get_healthchecks(
        self, service_namespace_config: ServiceNamespaceConfig
    ) -> List[HealthcheckDict]:
        """Returns a list of healthchecks per `the Marathon docs`_.

        If you have an http service, it uses the default endpoint that smartstack uses.
        (/status currently)

        Otherwise these do *not* use the same thresholds as smartstack in order to not
        produce a negative feedback loop, where mesos aggressively kills tasks because they
        are slow, which causes other things to be slow, etc.

        If the mode of the service is None, indicating that it was not specified in the service config
        and smartstack is not used by the service, no healthchecks are passed to Marathon. This ensures that
        it falls back to Mesos' knowledge of the task state as described in `the Marathon docs`_.
        In this case, we provide an empty array of healthchecks per `the Marathon API docs`_
        (scroll down to the healthChecks subsection).

        .. _the Marathon docs: https://mesosphere.github.io/marathon/docs/health-checks.html
        .. _the Marathon API docs: https://mesosphere.github.io/marathon/docs/rest-api.html#post-/v2/apps

        :param service_config: service config hash
        :returns: list of healthcheck definitions for marathon"""

        mode = self.get_healthcheck_mode(service_namespace_config)

        graceperiodseconds = self.get_healthcheck_grace_period_seconds()
        intervalseconds = self.get_healthcheck_interval_seconds()
        timeoutseconds = self.get_healthcheck_timeout_seconds()
        maxconsecutivefailures = self.get_healthcheck_max_consecutive_failures()

        if mode == "http" or mode == "https":
            http_path = self.get_healthcheck_uri(service_namespace_config)
            protocol = f"MESOS_{mode.upper()}"
            healthchecks = [
                HealthcheckDict(
                    {
                        "protocol": protocol,
                        "path": http_path,
                        "gracePeriodSeconds": graceperiodseconds,
                        "intervalSeconds": intervalseconds,
                        "portIndex": 0,
                        "timeoutSeconds": timeoutseconds,
                        "maxConsecutiveFailures": maxconsecutivefailures,
                    }
                )
            ]
        elif mode == "tcp":
            healthchecks = [
                HealthcheckDict(
                    {
                        "protocol": "TCP",
                        "gracePeriodSeconds": graceperiodseconds,
                        "intervalSeconds": intervalseconds,
                        "portIndex": 0,
                        "timeoutSeconds": timeoutseconds,
                        "maxConsecutiveFailures": maxconsecutivefailures,
                    }
                )
            ]
        elif mode == "cmd":
            healthchecks = [
                HealthcheckDict(
                    {
                        "protocol": "COMMAND",
                        "command": self.get_healthcheck_cmd(),
                        "gracePeriodSeconds": graceperiodseconds,
                        "intervalSeconds": intervalseconds,
                        "timeoutSeconds": timeoutseconds,
                        "maxConsecutiveFailures": maxconsecutivefailures,
                    }
                )
            ]
        elif mode is None:
            healthchecks = []
        else:
            raise InvalidHealthcheckMode(
                "Unknown mode: %s. Only acceptable healthcheck modes are http/https/tcp/cmd"
                % mode
            )
        return healthchecks

    def get_bounce_health_params(
        self, service_namespace_config: ServiceNamespaceConfig
    ) -> Dict[str, Any]:
        default: Dict[str, Any] = {}
        if service_namespace_config.is_in_smartstack():
            default = {"check_haproxy": True}
        return self.config_dict.get("bounce_health_params", default)

    def get_accepted_resource_roles(self) -> Optional[List[str]]:
        return self.config_dict.get("accepted_resource_roles", None)

    def get_host_port(self) -> int:
        """Map this port on the host to your container's port 8888. Default is 0, which means Marathon picks a port."""
        return self.config_dict.get("host_port", 0)

    def get_marathon_shard(self) -> Optional[int]:
        """Returns the configured shard of Marathon to use.
        Defaults to None, which means MarathonClients will decide which shard to put this app on."""
        return self.config_dict.get("marathon_shard", None)

    def get_previous_marathon_shards(self) -> Optional[List[int]]:
        """Returns a list of Marathon shards a service might have been on previously.
        Useful for graceful shard migrations. Defaults to None"""
        return self.config_dict.get("previous_marathon_shards", None)

    def get_autoscaled_instances(self) -> int:
        try:
            zk_instances = get_instances_from_zookeeper(
                service=self.service, instance=self.instance
            )
            log.debug("Got %d instances out of zookeeper" % zk_instances)
            return zk_instances
        except NoNodeError:
            log.debug("No zookeeper data, returning None")
            return None

    def set_autoscaled_instances(self, instance_count: int) -> None:
        """Set the number of instances in the same way that the autoscaler does."""
        set_instances_for_marathon_service(
            service=self.service,
            instance=self.instance,
            instance_count=instance_count,
        )


class MarathonDeployStatus:
    """An enum to represent Marathon app deploy status.
    Changing name of the keys will affect both the paasta CLI and API.
    """

    Running, Deploying, Stopped, Delayed, Waiting, NotRunning = range(0, 6)

    @classmethod
    def tostring(cls, val: int) -> str:
        for k, v in vars(cls).items():
            if v == val:
                return k
        raise ValueError("Unknown Marathon deploy status %d" % val)

    @classmethod
    def fromstring(cls, _str: str) -> int:
        return getattr(cls, _str, None)


def get_marathon_app_deploy_status(
    client: MarathonClient, app: MarathonApp = None
) -> int:
    # Check the launch queue to see if an app is blocked
    is_overdue, backoff_seconds = get_app_queue_status(client, app.id)

    # Based on conditions at https://mesosphere.github.io/marathon/docs/marathon-ui.html
    if is_overdue:
        deploy_status = MarathonDeployStatus.Waiting
    elif backoff_seconds:
        deploy_status = MarathonDeployStatus.Delayed
    elif len(app.deployments) > 0:
        deploy_status = MarathonDeployStatus.Deploying
    elif app.instances == 0 and app.tasks_running == 0:
        deploy_status = MarathonDeployStatus.Stopped
    else:
        deploy_status = MarathonDeployStatus.Running

    return deploy_status


class CachedMarathonClient(MarathonClient):
    @time_cache(ttl=20)
    def list_apps(self, *args: Any, **kwargs: Any) -> Any:
        return super().list_apps(*args, **kwargs)


def get_marathon_client(
    url: Sequence[str], user: str, passwd: str, cached: bool = False
) -> MarathonClient:
    """Get a new Marathon client connection in the form of a MarathonClient object.

    :param url: The url to connect to Marathon at
    :param user: The username to connect with
    :param passwd: The password to connect with
    :param cached: If true, return CachedMarathonClient
    :returns: A new marathon.MarathonClient object"""
    log.info("Connecting to Marathon server at: %s", url)

    session = requests.Session()
    session.headers.update({"User-Agent": get_user_agent()})

    if cached:
        return CachedMarathonClient(url, user, passwd, timeout=30, session=session)
    else:
        return MarathonClient(url, user, passwd, timeout=30, session=session)


def get_marathon_clients(
    marathon_servers: MarathonServers, cached: bool = False
) -> MarathonClients:
    current_servers = marathon_servers.current
    current_clients = []
    for current_server in current_servers:
        current_clients.append(
            get_marathon_client(
                url=current_server.get_url(),
                user=current_server.get_username(),
                passwd=current_server.get_password(),
                cached=cached,
            )
        )
    previous_servers = marathon_servers.previous
    previous_clients = []
    for previous_server in previous_servers:
        previous_clients.append(
            get_marathon_client(
                url=previous_server.get_url(),
                user=previous_server.get_username(),
                passwd=previous_server.get_password(),
                cached=cached,
            )
        )
    return MarathonClients(current=current_clients, previous=previous_clients)


def get_list_of_marathon_clients(
    system_paasta_config: Optional[SystemPaastaConfig] = None, cached: bool = False
) -> Sequence[MarathonClient]:
    if system_paasta_config is None:
        system_paasta_config = load_system_paasta_config()
    marathon_servers = get_marathon_servers(system_paasta_config)
    return get_marathon_clients(marathon_servers, cached=cached).get_all_clients()


def format_job_id(
    service: str,
    instance: str,
    git_hash: Optional[str] = None,
    config_hash: Optional[str] = None,
) -> str:
    """Compose a Marathon app id formatted to meet Marathon's
    `app id requirements <https://mesosphere.github.io/marathon/docs/rest-api.html#id-string>`_

    :param service: The name of the service
    :param instance: The instance of the service
    :param git_hash: The git_hash portion of the job_id. If git_hash is set,
                     config_hash must also be set.
    :param config_hash: The config_hash portion of the job_id. If config_hash
                        is set, git_hash must also be set.
    :returns: a composed app id in a format that Marathon accepts

    """
    service = str(service).replace("_", "--")
    instance = str(instance).replace("_", "--")
    if git_hash:
        git_hash = str(git_hash).replace("_", "--")
    if config_hash:
        config_hash = str(config_hash).replace("_", "--")
    formatted = compose_job_id(service, instance, git_hash, config_hash)
    return formatted


def deformat_job_id(job_id: str) -> Tuple[str, str, str, str]:
    job_id = job_id.replace("--", "_")
    return decompose_job_id(job_id)


def get_all_namespaces_for_service(
    service: str, soa_dir: str = DEFAULT_SOA_DIR, full_name: bool = True
) -> Sequence[Tuple[str, ServiceNamespaceConfig]]:
    """Get all the smartstack namespaces listed for a given service name.

    :param service: The service name
    :param soa_dir: The SOA config directory to read from
    :param full_name: A boolean indicating if the service name should be prepended to the namespace in the
                      returned tuples as described below (Default: True)
    :returns: A list of tuples of the form (service<SPACER>namespace, namespace_config) if full_name is true,
              otherwise of the form (namespace, namespace_config)
    """
    service_config = service_configuration_lib.read_service_configuration(
        service, soa_dir
    )
    smartstack = service_config.get("smartstack", {})
    namespace_list = []
    for namespace in smartstack:
        if full_name:
            name = compose_job_id(service, namespace)
        else:
            name = namespace
        namespace_list.append((name, smartstack[namespace]))
    return namespace_list


def get_all_namespaces(
    soa_dir: str = DEFAULT_SOA_DIR,
) -> Sequence[Tuple[str, ServiceNamespaceConfig]]:
    """Get all the smartstack namespaces across all services.
    This is mostly so synapse can get everything it needs in one call.

    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of the form (service.namespace, namespace_config)"""
    rootdir = os.path.abspath(soa_dir)
    namespace_list: List[Tuple[str, ServiceNamespaceConfig]] = []
    for srv_dir in os.listdir(rootdir):
        namespace_list.extend(get_all_namespaces_for_service(srv_dir, soa_dir))
    return namespace_list


def get_app_id_and_task_uuid_from_executor_id(executor_id: str) -> Tuple[str, str]:
    """Parse the marathon executor ID and return the (app id, task uuid)"""
    app_id, task_uuid = executor_id.rsplit(".", 1)
    return app_id, task_uuid


def parse_service_instance_from_executor_id(task_id: str) -> Tuple[str, str]:
    app_id, task_uuid = get_app_id_and_task_uuid_from_executor_id(task_id)
    (srv_name, srv_instance, _, __) = deformat_job_id(app_id)
    return srv_name, srv_instance


def marathon_services_running_here() -> List[Tuple[str, str, int]]:
    """See what marathon services are being run by a mesos-slave on this host.
    :returns: A list of triples of (service, instance, port)"""

    return mesos_services_running_here(
        framework_filter=lambda fw: fw["name"].startswith("marathon"),
        parse_service_instance_from_executor_id=parse_service_instance_from_executor_id,
    )


def get_marathon_services_running_here_for_nerve(
    cluster: Optional[str], soa_dir: str
) -> Sequence[Tuple[str, ServiceNamespaceConfig]]:
    if not cluster:
        try:
            system_paasta_config = load_system_paasta_config()
            cluster = system_paasta_config.get_cluster()
        # In the cases where there is *no* cluster or in the case
        # where there isn't a Paasta configuration file at *all*, then
        # there must be no marathon services running here, so we catch
        # these custom exceptions and return [].
        except (PaastaNotConfiguredError):
            return []
        if not system_paasta_config.get_register_marathon_services():
            return []
    # We try to get the hosts IP here so that we can pass it to Nerve
    # Nerve then sends it as X-Nerve-Check-IP to hacheck. HAProxy also
    # sends a header to hacheck with the backends IP. If we want to cache
    # for haproxy + nerve under the same key (and hence reduce number of
    # total healthchecks) then we need the hosts IP.
    try:
        host_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        # BUT the show really must go on. if we can't get the hosts IP
        # then hacheck will have to cache two healthchecks per service
        # but atleast we won't fail to register services.
        host_ip = "127.0.0.1"
    # When a cluster is defined in mesos, let's iterate through marathon services
    marathon_services = marathon_services_running_here()
    nerve_list = []
    for name, instance, port in marathon_services:
        try:
            marathon_service_config = load_marathon_service_config(
                service=name,
                instance=instance,
                cluster=cluster,
                load_deployments=False,
                soa_dir=soa_dir,
            )
            for registration in marathon_service_config.get_registrations():
                reg_service, reg_namespace, _, __ = decompose_job_id(registration)
                nerve_dict = load_service_namespace_config(
                    service=reg_service, namespace=reg_namespace, soa_dir=soa_dir
                )
                if not nerve_dict.is_in_smartstack():
                    continue
                nerve_dict["port"] = port
                nerve_dict["paasta_instance"] = instance
                nerve_dict["deploy_group"] = marathon_service_config.get_deploy_group()
                nerve_dict["extra_healthcheck_headers"] = {"X-Nerve-Check-IP": host_ip}
                nerve_list.append((registration, nerve_dict))
        except (KeyError, NoConfigurationForServiceError):
            continue  # SOA configs got deleted for this app, it'll get cleaned up

    return nerve_list


def get_puppet_services_that_run_here() -> Dict[str, List[str]]:
    # find all files in the PUPPET_SERVICE_DIR, but discard broken symlinks
    # this allows us to (de)register services on a machine by
    # breaking/healing a symlink placed by Puppet.
    puppet_service_dir_services = {}
    if os.path.exists(PUPPET_SERVICE_DIR):
        for service_name in os.listdir(PUPPET_SERVICE_DIR):
            if not os.path.exists(os.path.join(PUPPET_SERVICE_DIR, service_name)):
                continue
            with open(os.path.join(PUPPET_SERVICE_DIR, service_name)) as f:
                puppet_service_data = json.load(f)
                puppet_service_dir_services[service_name] = puppet_service_data[
                    "namespaces"
                ]

    return puppet_service_dir_services


def get_puppet_services_running_here_for_nerve(
    soa_dir: str,
) -> List[Tuple[str, ServiceNamespaceConfig]]:
    puppet_services = []
    for service, namespaces in sorted(get_puppet_services_that_run_here().items()):
        for namespace in namespaces:
            puppet_services.append(
                _namespaced_get_classic_service_information_for_nerve(
                    service, namespace, soa_dir
                )
            )
    return puppet_services


def get_classic_service_information_for_nerve(
    name: str, soa_dir: str
) -> Tuple[str, ServiceNamespaceConfig]:
    return _namespaced_get_classic_service_information_for_nerve(name, "main", soa_dir)


def _namespaced_get_classic_service_information_for_nerve(
    name: str, namespace: str, soa_dir: str
) -> Tuple[str, ServiceNamespaceConfig]:
    try:
        # This max(cpu_count, 10) emulates the previous behavior of configure_nerve.
        cpus = max(multiprocessing.cpu_count(), 10)
    except NotImplementedError:
        cpus = 10

    nerve_dict = load_service_namespace_config(name, namespace, soa_dir)
    port_file = os.path.join(soa_dir, name, "port")
    # If the namespace defines a port, prefer that, otherwise use the
    # service wide port file.
    nerve_dict["port"] = nerve_dict.get(
        "port", None
    ) or service_configuration_lib.read_port(port_file)
    nerve_name = compose_job_id(name, namespace)

    nerve_dict["weight"] = cpus

    return (nerve_name, nerve_dict)


def get_classic_services_running_here_for_nerve(
    soa_dir: str,
) -> Sequence[Tuple[str, ServiceNamespaceConfig]]:
    classic_services = []
    classic_services_here = service_configuration_lib.services_that_run_here()
    for service in sorted(classic_services_here):
        namespaces = [
            x[0]
            for x in get_all_namespaces_for_service(service, soa_dir, full_name=False)
        ]
        for namespace in namespaces:
            classic_services.append(
                _namespaced_get_classic_service_information_for_nerve(
                    service, namespace, soa_dir
                )
            )
    return classic_services


def list_all_marathon_app_ids(
    client: MarathonClient,
) -> Sequence[str]:
    """List all marathon app_ids, regardless of state

    The raw marathon API returns app ids in their URL form, with leading '/'s
    conforming to the Application Group format:
    https://github.com/mesosphere/marathon/blob/master/docs/docs/application-groups.md

    This function wraps the full output of list_apps to return a list
    in the original form, without leading "/"'s.

    returns: List of app ids in the same format they are POSTed."""
    return [app.id.lstrip("/") for app in get_all_marathon_apps(client)]


def is_app_id_running(app_id: str, client: MarathonClient) -> bool:
    """Returns a boolean indicating if the app is in the current list
    of marathon apps

    :param app_id: The app_id to look for
    :param client: A MarathonClient object"""

    all_app_ids = list_all_marathon_app_ids(client)
    return app_id.lstrip("/") in all_app_ids


def app_has_tasks(
    client: MarathonClient,
    app_id: str,
    expected_tasks: int,
    exact_matches_only: bool = False,
) -> bool:
    """A predicate function indicating whether an app has launched *at least* expected_tasks
    tasks.

    Raises a marathon.NotFoundError when no app with matching id is found.

    :param client: the marathon client
    :param app_id: the app_id to which the tasks should belong. The leading / that marathon appends to
        app_ids is added here.
    :param expected_tasks: the number of tasks to check for
    :param exact_matches_only: a boolean indicating whether we require exactly expected_tasks to be running
    :returns: a boolean indicating whether there are atleast expected_tasks tasks with
        an app id matching app_id
    """
    app_id = "/%s" % app_id
    try:
        tasks = client.list_tasks(app_id=app_id)
    except NotFoundError:
        print("no app with id %s found" % app_id)
        raise
    print("app %s has %d of %d expected tasks" % (app_id, len(tasks), expected_tasks))
    if exact_matches_only:
        return len(tasks) == expected_tasks
    else:
        return len(tasks) >= expected_tasks


def get_app_queue(client: MarathonClient, app_id: str) -> Optional[MarathonQueueItem]:
    """Returns the app queue of an application if it exists in Marathon's launch queue

    :param client: The marathon client
    :param app_id: The Marathon app id (without the leading /)
    :returns: The app queue from marathon
    """
    app_id = "/%s" % app_id
    app_queue = client.list_queue(embed_last_unused_offers=True)
    for app_queue_item in app_queue:
        if app_queue_item.app.id == app_id:
            return app_queue_item
    return None


def get_app_queue_status(
    client: MarathonClient, app_id: str
) -> Tuple[Optional[bool], Optional[float]]:
    """Returns the status of an application if it exists in Marathon's launch queue

    :param client: The marathon client
    :param app_id: The Marathon app id (without the leading /)
    :returns: A tuple of the form (is_overdue, current_backoff_delay) or (None, None)
              if the app cannot be found. If is_overdue is True, then Marathon has
              not received a resource offer that satisfies the requirements for the app
    """
    app_queue = get_app_queue(client, app_id)
    return get_app_queue_status_from_queue(app_queue)


def get_app_queue_status_from_queue(
    app_queue_item: Optional[MarathonQueueItem],
) -> Tuple[Optional[bool], Optional[float]]:
    if app_queue_item is None:
        return (None, None)
    return (app_queue_item.delay.overdue, app_queue_item.delay.time_left_seconds)


def get_app_queue_last_unused_offers(
    app_queue_item: Optional[MarathonQueueItem],
) -> Sequence[Dict]:
    """Returns the unused offers for an app

    :param app_queue_item: app_queue_item returned by get_app_queue
    :returns: A list of offers received from mesos, including the reasons they were rejected
    """
    if app_queue_item is None:
        return []
    return app_queue_item.last_unused_offers


def summarize_unused_offers(app_queue: Optional[MarathonQueueItem]) -> Dict[str, int]:
    """Returns a summary of the reasons marathon rejected offers from mesos

    :param app_queue: An app queue item as returned from get_app_queue
    :returns: A dict of rejection_reason: count
    """
    unused_offers = get_app_queue_last_unused_offers(app_queue)
    reasons: Dict[str, int] = defaultdict(lambda: 0)
    for offer in unused_offers:
        for reason in offer["reason"]:
            reasons[reason] += 1
    return reasons


def create_complete_config(
    service: str, instance: str, soa_dir: str = DEFAULT_SOA_DIR
) -> FormattedMarathonAppDict:
    """Generates a complete dictionary to be POST'ed to create an app on Marathon"""
    return load_marathon_service_config(
        service=service,
        instance=instance,
        cluster=load_system_paasta_config().get_cluster(),
        soa_dir=soa_dir,
    ).format_marathon_app_dict()


def get_expected_instance_count_for_namespace(
    service: str,
    namespace: str,
    cluster: str = None,
    instance_type_class: Type[LongRunningServiceConfig] = MarathonServiceConfig,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> int:
    """Get the number of expected instances for a namespace, based on the number
    of instances set to run on that namespace as specified in Marathon service
    configuration files.

    :param service: The service's name
    :param namespace: The namespace for that service to check
    instance_type_class: The type of the instance, options are MarathonServiceConfig and KubernetesDeploymentConfig,
    :param soa_dir: The SOA configuration directory to read from
    :returns: An integer value of the # of expected instances for the namespace"""
    total_expected = 0
    if not cluster:
        cluster = load_system_paasta_config().get_cluster()

    pscl = PaastaServiceConfigLoader(
        service=service, soa_dir=soa_dir, load_deployments=False
    )
    for job_config in pscl.instance_configs(
        cluster=cluster, instance_type_class=instance_type_class
    ):
        if f"{service}.{namespace}" in job_config.get_registrations():
            total_expected += job_config.get_instances()
    return total_expected


def get_matching_appids(
    service: str, instance: str, client: MarathonClient, embed_tasks: bool = False
) -> List[str]:
    """Returns a list of appids given a service and instance.
    Useful for fuzzy matching if you think there are marathon
    apps running but you don't know the full instance id"""
    marathon_apps = get_all_marathon_apps(
        client, service_name=service, instance_name=instance, embed_tasks=embed_tasks
    )
    return [
        app.id for app in marathon_apps if does_app_id_match(service, instance, app.id)
    ]


def get_matching_apps(
    service: str, instance: str, marathon_apps: Sequence[MarathonApp]
) -> Sequence[MarathonApp]:
    """Returns a list of appids given a service and instance.
    Useful for fuzzy matching if you think there are marathon
    apps running but you don't know the full instance id"""
    return [
        app for app in marathon_apps if does_app_id_match(service, instance, app.id)
    ]


def get_matching_apps_with_clients(
    service: str,
    instance: str,
    marathon_apps_with_clients: Sequence[Tuple[MarathonApp, MarathonClient]],
) -> List[Tuple[MarathonApp, MarathonClient]]:
    return [
        (a, c)
        for a, c in marathon_apps_with_clients
        if does_app_id_match(service, instance, a.id)
    ]


def does_app_id_match(service: str, instance: str, app_id: str) -> bool:
    jobid = format_job_id(service, instance)
    expected_prefix = f"/{jobid}{MESOS_TASK_SPACER}"
    return app_id.startswith(expected_prefix)


def get_all_marathon_apps(
    client: MarathonClient,
    service_name: Optional[str] = None,
    instance_name: Optional[str] = None,
    embed_tasks: bool = False,
) -> List[MarathonApp]:
    if service_name:
        return client.list_apps(
            embed_tasks=embed_tasks,
            app_id="/"
            + format_job_id(service=service_name, instance=(instance_name or "")),
        )
    else:
        # Ignore apps inside a folder
        return [
            app
            for app in client.list_apps(embed_tasks=embed_tasks)
            if len(app.id.split("/")) <= 2
        ]


def get_marathon_apps_with_clients(
    clients: Sequence[MarathonClient],
    service_name: Optional[str] = None,
    instance_name: Optional[str] = None,
    embed_tasks: bool = False,
) -> Sequence[Tuple[MarathonApp, MarathonClient]]:
    marathon_apps_with_clients: List[Tuple[MarathonApp, MarathonClient]] = []
    for client in clients:
        for app in get_all_marathon_apps(
            client, service_name, instance_name=instance_name, embed_tasks=embed_tasks
        ):
            marathon_apps_with_clients.append((app, client))
    return marathon_apps_with_clients


def kill_task(
    client: MarathonClient, app_id: str, task_id: str, scale: bool
) -> Optional[MarathonTask]:
    """Wrapper to the official kill_task method that is tolerant of errors"""
    try:
        return client.kill_task(app_id=app_id, task_id=task_id, scale=True)
    except MarathonHttpError as e:
        # Marathon allows you to kill and scale in one action, but this is not
        # idempotent. If you kill&scale the same task ID twice, the number of instances
        # gets decremented twice. This can lead to a situation where kill&scaling the
        # last task decrements the number of instances below zero, causing an "Object is not
        # valid" message or a "Bean is not valid" message.
        if "is not valid" in e.error_message and e.status_code == 422:
            log.warning(
                "Got 'is not valid' when killing task %s. Continuing anyway." % task_id
            )
            return None
        elif "does not exist" in e.error_message and e.status_code == 404:
            log.warning(
                "Got 'does not exist' when killing task %s. Continuing anyway."
                % task_id
            )
            return None
        else:
            raise


def kill_given_tasks(
    client: MarathonClient, task_ids: Sequence[str], scale: bool
) -> bool:
    """Wrapper to the official kill_given_tasks method that is tolerant of errors"""
    if not task_ids:
        log.debug("No task_ids specified, not killing any tasks")
        return False
    try:
        return client.kill_given_tasks(task_ids=task_ids, scale=scale, force=True)
    except MarathonHttpError as e:
        # Marathon's interface is always async, so it is possible for you to see
        # a task in the interface and kill it, yet by the time it tries to kill
        # it, it is already gone. This is not really a failure condition, so we
        # swallow this error.
        if "is not valid" in e.error_message and e.status_code == 422:
            log.debug("Probably tried to kill a task id that didn't exist. Continuing.")
            return False
        else:
            raise


def is_task_healthy(
    task: MarathonTask, require_all: bool = True, default_healthy: bool = False
) -> bool:
    """Check that a marathon task is healthy

    :param task: the marathon task object
    :param require_all: require all the healthchecks to be passing
        false means that only one needs to pass
    :param default_healthy: cause the function to report healthy if
        there are no health check results
    :returns: True if healthy, False if not"""
    if task.health_check_results:
        results = [hcr.alive for hcr in task.health_check_results]
        if require_all:
            return all(results)
        else:
            return any(results)
    return default_healthy


def is_old_task_missing_healthchecks(task: MarathonTask, app: MarathonApp) -> bool:
    """We check this because versions of Marathon (at least up to 1.1)
    sometimes stop healthchecking tasks, leaving no results. We can normally
    assume that an "old" task which has no healthcheck results is still up
    and healthy but marathon has simply decided to stop healthchecking it.
    """
    health_checks = app.health_checks
    if not task.health_check_results and health_checks and task.started_at:
        now_utc = datetime.datetime.now(pytz.utc)
        healthcheck_startup_time = datetime.timedelta(
            seconds=health_checks[0].grace_period_seconds
        ) + datetime.timedelta(seconds=health_checks[0].interval_seconds * 5)
        is_task_old = task.started_at + healthcheck_startup_time < now_utc
        return is_task_old
    return False


def get_num_at_risk_tasks(app: MarathonApp, draining_hosts: Sequence[str]) -> int:
    """Determine how many of an application's tasks are running on
    at-risk (Mesos Maintenance Draining) hosts.

    :param app: A marathon application
    :param draining_hosts: A list of hostnames that are marked as draining.
                           See paasta_tools.mesos_maintenance.get_draining_hosts
    :returns: An integer representing the number of tasks running on at-risk hosts
    """
    hosts_tasks_running_on = [task.host for task in app.tasks]
    num_at_risk_tasks = 0
    for host in hosts_tasks_running_on:
        if host in draining_hosts:
            num_at_risk_tasks += 1
    log.debug("%s has %d tasks running on at-risk hosts." % (app.id, num_at_risk_tasks))
    return num_at_risk_tasks


def take_up_slack(client: MarathonClient, app: MarathonApp) -> None:
    slack = max(app.instances - len(app.tasks), 0)
    if slack > 0:
        log.info(
            "Scaling %s down from %d to %d instances to remove slack."
            % (app.id, app.instances, app.instances - slack)
        )
        client.scale_app(app_id=app.id, instances=(app.instances - slack), force=True)


def get_short_task_id(task_id: str) -> str:
    """Return just the Marathon-generated UUID of a Mesos task id."""
    return task_id.split(MESOS_TASK_SPACER)[-1]


def get_instances_from_zookeeper(service: str, instance: str) -> int:
    with ZookeeperPool() as zookeeper_client:
        (instances, _) = zookeeper_client.get(
            "%s/instances" % compose_autoscaling_zookeeper_root(service, instance)
        )
        return int(instances)


def compose_autoscaling_zookeeper_root(service: str, instance: str) -> str:
    return f"{AUTOSCALING_ZK_ROOT}/{service}/{instance}"


def set_instances_for_marathon_service(
    service: str, instance: str, instance_count: int, soa_dir: str = DEFAULT_SOA_DIR
) -> None:
    zookeeper_path = "%s/instances" % compose_autoscaling_zookeeper_root(
        service, instance
    )
    with ZookeeperPool() as zookeeper_client:
        zookeeper_client.ensure_path(zookeeper_path)
        zookeeper_client.set(zookeeper_path, str(instance_count).encode("utf8"))
