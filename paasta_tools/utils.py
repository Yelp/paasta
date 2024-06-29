# Copyright 2015-2017 Yelp Inc.
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
import contextlib
import copy
import datetime
import difflib
import errno
import fcntl
import getpass
import glob
import hashlib
import io
import json
import logging
import math
import os
import pwd
import queue
import re
import shlex
import signal
import socket
import ssl
import sys
import tempfile
import threading
import time
import warnings
from collections import OrderedDict
from enum import Enum
from fnmatch import fnmatch
from functools import lru_cache
from functools import wraps
from subprocess import PIPE
from subprocess import Popen
from subprocess import STDOUT
from types import FrameType
from typing import Any
from typing import Callable
from typing import cast
from typing import Collection
from typing import ContextManager
from typing import Dict
from typing import FrozenSet
from typing import IO
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Literal
from typing import Mapping
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Set
from typing import TextIO
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union

import choice
import dateutil.tz
import ldap3
import requests_cache
import service_configuration_lib
import yaml
from docker import APIClient
from docker.utils import kwargs_from_env
from kazoo.client import KazooClient
from mypy_extensions import TypedDict
from service_configuration_lib import read_extra_service_information
from service_configuration_lib import read_service_configuration

import paasta_tools.cli.fsm


# DO NOT CHANGE SPACER, UNLESS YOU'RE PREPARED TO CHANGE ALL INSTANCES
# OF IT IN OTHER LIBRARIES (i.e. service_configuration_lib).
# It's used to compose a job's full ID from its name and instance
SPACER = "."
INFRA_ZK_PATH = "/nail/etc/zookeeper_discovery/infrastructure/"
PATH_TO_SYSTEM_PAASTA_CONFIG_DIR = os.environ.get(
    "PAASTA_SYSTEM_CONFIG_DIR", "/etc/paasta/"
)
DEFAULT_SOA_DIR = service_configuration_lib.DEFAULT_SOA_DIR
DEFAULT_VAULT_TOKEN_FILE = "/root/.vault_token"
AUTO_SOACONFIG_SUBDIR = "autotuned_defaults"
DEFAULT_DOCKERCFG_LOCATION = "file:///root/.dockercfg"
DEPLOY_PIPELINE_NON_DEPLOY_STEPS = (
    "itest",
    "itest-and-push-to-registry",
    "security-check",
    "push-to-registry",
)
# Default values for _log
ANY_CLUSTER = "N/A"
ANY_INSTANCE = "N/A"
DEFAULT_LOGLEVEL = "event"
no_escape = re.compile(r"\x1B\[[0-9;]*[mK]")

# instead of the convention of using underscores in this scribe channel name,
# the audit log uses dashes to prevent collisions with a service that might be
# named 'audit_log'
AUDIT_LOG_STREAM = "stream_paasta-audit-log"

DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT = (
    "http://{host:s}:{port:d}/;csv;norefresh;scope={scope:s}"
)

DEFAULT_CPU_PERIOD = 100000
DEFAULT_CPU_BURST_ADD = 1

DEFAULT_SOA_CONFIGS_GIT_URL = "sysgit.yelpcorp.com"

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

INSTANCE_TYPES = (
    "paasta_native",
    "adhoc",
    "kubernetes",
    "eks",
    "tron",
    "flink",
    "flinkeks",
    "cassandracluster",
    "kafkacluster",
    "vitesscluster",
    "monkrelays",
    "nrtsearchservice",
    "nrtsearchserviceeks",
)

PAASTA_K8S_INSTANCE_TYPES = {
    "kubernetes",
    "eks",
}

INSTANCE_TYPE_TO_K8S_NAMESPACE = {
    "marathon": "paasta",
    "adhoc": "paasta",
    "tron": "tron",
    "flink": "paasta-flinks",
    "flinkeks": "paasta-flinks",
    "cassandracluster": "paasta-cassandraclusters",
    "kafkacluster": "paasta-kafkaclusters",
    "vitesscluster": "paasta-vitessclusters",
    "nrtsearchservice": "paasta-nrtsearchservices",
    "nrtsearchserviceeks": "paasta-nrtsearchservices",
}

SHARED_SECRETS_K8S_NAMESPACES = {"paasta-spark", "paasta-cassandraclusters"}

CAPS_DROP = [
    "SETPCAP",
    "MKNOD",
    "AUDIT_WRITE",
    "CHOWN",
    "NET_RAW",
    "DAC_OVERRIDE",
    "FOWNER",
    "FSETID",
    "KILL",
    "SETGID",
    "SETUID",
    "NET_BIND_SERVICE",
    "SYS_CHROOT",
    "SETFCAP",
]


class RollbackTypes(Enum):
    AUTOMATIC_SLO_ROLLBACK = "automatic_slo_rollback"
    AUTOMATIC_METRIC_ROLLBACK = "automatic_metric_rollback"
    USER_INITIATED_ROLLBACK = "user_initiated_rollback"


class TimeCacheEntry(TypedDict):
    data: Any
    fetch_time: float


_CacheRetT = TypeVar("_CacheRetT")


class time_cache:
    def __init__(self, ttl: float = 0) -> None:
        self.configs: Dict[Tuple, TimeCacheEntry] = {}
        self.ttl = ttl

    def __call__(self, f: Callable[..., _CacheRetT]) -> Callable[..., _CacheRetT]:
        def cache(*args: Any, **kwargs: Any) -> _CacheRetT:
            if "ttl" in kwargs:
                ttl = kwargs["ttl"]
                del kwargs["ttl"]
            else:
                ttl = self.ttl
            key = args
            for item in kwargs.items():
                key += item
            if (
                (not ttl)
                or (key not in self.configs)
                or (time.time() - self.configs[key]["fetch_time"] > ttl)
            ):
                self.configs[key] = {
                    "data": f(*args, **kwargs),
                    "fetch_time": time.time(),
                }
            return self.configs[key]["data"]

        return cache


_SortDictsT = TypeVar("_SortDictsT", bound=Mapping)


def sort_dicts(dcts: Iterable[_SortDictsT]) -> List[_SortDictsT]:
    def key(dct: _SortDictsT) -> Tuple:
        return tuple(sorted(dct.items()))

    return sorted(dcts, key=key)


class InvalidInstanceConfig(Exception):
    pass


DeployBlacklist = List[Tuple[str, str]]
DeployWhitelist = Optional[Tuple[str, List[str]]]
# The actual config files will have lists, since tuples are not expressible in base YAML, so we define different types
# here to represent that. The getter functions will convert to the safe versions above.
UnsafeDeployBlacklist = Optional[Sequence[Sequence[str]]]
UnsafeDeployWhitelist = Optional[Sequence[Union[str, Sequence[str]]]]


Constraint = Sequence[str]

# e.g. ['GROUP_BY', 'habitat', 2]. Tron doesn't like that so we'll convert to Constraint later.
UnstringifiedConstraint = Sequence[Union[str, int, float]]

SecurityConfigDict = Dict  # Todo: define me.


class VolumeWithMode(TypedDict):
    mode: str


class DockerVolume(VolumeWithMode):
    hostPath: str
    containerPath: str


class AwsEbsVolume(VolumeWithMode):
    volume_id: str
    fs_type: str
    partition: int
    container_path: str


class PersistentVolume(VolumeWithMode):
    size: int
    container_path: str
    storage_class_name: str


class SecretVolumeItem(TypedDict, total=False):
    key: str
    path: str
    mode: Union[str, int]


class SecretVolume(TypedDict, total=False):
    secret_name: str
    container_path: str
    default_mode: Union[str, int]
    items: List[SecretVolumeItem]


class ProjectedSAVolume(TypedDict, total=False):
    container_path: str
    audience: str
    expiration_seconds: int


class TronSecretVolume(SecretVolume, total=False):
    secret_volume_name: str


class MonitoringDict(TypedDict, total=False):
    alert_after: Union[str, float]
    check_every: str
    check_oom_events: bool
    component: str
    description: str
    notification_email: Union[str, bool]
    page: bool
    priority: str
    project: str
    realert_every: float
    runbook: str
    slack_channels: Union[str, List[str]]
    tags: List[str]
    team: str
    ticket: bool
    tip: str


class InstanceConfigDict(TypedDict, total=False):
    deploy_group: str
    mem: float
    cpus: float
    disk: float
    cmd: str
    namespace: str
    args: List[str]
    cfs_period_us: float
    cpu_burst_add: float
    cap_add: List
    env: Dict[str, str]
    monitoring: MonitoringDict
    deploy_blacklist: UnsafeDeployBlacklist
    deploy_whitelist: UnsafeDeployWhitelist
    pool: str
    persistent_volumes: List[PersistentVolume]
    role: str
    extra_volumes: List[DockerVolume]
    aws_ebs_volumes: List[AwsEbsVolume]
    secret_volumes: List[SecretVolume]
    projected_sa_volumes: List[ProjectedSAVolume]
    security: SecurityConfigDict
    dependencies_reference: str
    dependencies: Dict[str, Dict]
    constraints: List[UnstringifiedConstraint]
    extra_constraints: List[UnstringifiedConstraint]
    net: str
    extra_docker_args: Dict[str, str]
    gpus: int
    branch: str
    iam_role: str
    iam_role_provider: str
    service: str


class BranchDictV1(TypedDict, total=False):
    docker_image: str
    desired_state: str
    force_bounce: Optional[str]


class BranchDictV2(TypedDict):
    git_sha: str
    docker_image: str
    image_version: Optional[str]
    desired_state: str
    force_bounce: Optional[str]


class DockerParameter(TypedDict):
    key: str
    value: str


KubeContainerResourceRequest = TypedDict(
    "KubeContainerResourceRequest",
    {
        "cpu": float,
        "memory": str,
        "ephemeral-storage": str,
    },
    total=False,
)


def safe_deploy_blacklist(input: UnsafeDeployBlacklist) -> DeployBlacklist:
    return [(t, l) for t, l in input]


def safe_deploy_whitelist(input: UnsafeDeployWhitelist) -> DeployWhitelist:
    try:
        location_type, allowed_values = input
        return cast(str, location_type), cast(List[str], allowed_values)
    except TypeError:
        return None


# For mypy typing
InstanceConfig_T = TypeVar("InstanceConfig_T", bound="InstanceConfig")


class InstanceConfig:
    config_filename_prefix: str

    def __init__(
        self,
        cluster: str,
        instance: str,
        service: str,
        config_dict: InstanceConfigDict,
        branch_dict: Optional[BranchDictV2],
        soa_dir: str = DEFAULT_SOA_DIR,
    ) -> None:
        self.config_dict = config_dict
        self.branch_dict = branch_dict
        self.cluster = cluster
        self.instance = instance
        self.service = service
        self.soa_dir = soa_dir
        self._job_id = compose_job_id(service, instance)
        config_interpolation_keys = ("deploy_group",)
        interpolation_facts = self.__get_interpolation_facts()
        for key in config_interpolation_keys:
            if (
                key in self.config_dict
                and self.config_dict[key] is not None  # type: ignore
            ):
                self.config_dict[key] = self.config_dict[key].format(  # type: ignore
                    **interpolation_facts
                )

    def __repr__(self) -> str:
        return "{!s}({!r}, {!r}, {!r}, {!r}, {!r}, {!r})".format(
            self.__class__.__name__,
            self.service,
            self.instance,
            self.cluster,
            self.config_dict,
            self.branch_dict,
            self.soa_dir,
        )

    def __get_interpolation_facts(self) -> Dict[str, str]:
        return {
            "cluster": self.cluster,
            "instance": self.instance,
            "service": self.service,
        }

    def get_cluster(self) -> str:
        return self.cluster

    def get_namespace(self) -> str:
        """Get namespace from config, default to the value from INSTANCE_TYPE_TO_K8S_NAMESPACE for this instance type, 'paasta' if that isn't defined."""
        return self.config_dict.get(
            "namespace",
            INSTANCE_TYPE_TO_K8S_NAMESPACE.get(self.get_instance_type(), "paasta"),
        )

    def get_instance(self) -> str:
        return self.instance

    def get_service(self) -> str:
        return self.service

    @property
    def job_id(self) -> str:
        return self._job_id

    def get_docker_registry(
        self, system_paasta_config: Optional["SystemPaastaConfig"] = None
    ) -> str:
        return get_service_docker_registry(
            self.service, self.soa_dir, system_config=system_paasta_config
        )

    def get_branch(self) -> str:
        return get_paasta_branch(
            cluster=self.get_cluster(), instance=self.get_instance()
        )

    def get_deploy_group(self) -> str:
        return self.config_dict.get("deploy_group", self.get_branch())

    def get_team(self) -> str:
        return self.config_dict.get("monitoring", {}).get("team", None)

    def get_mem(self) -> float:
        """Gets the memory required from the service's configuration.

        Defaults to 4096 (4G) if no value specified in the config.

        :returns: The amount of memory specified by the config, 4096 if not specified"""
        mem = self.config_dict.get("mem", 4096)
        return mem

    def get_mem_swap(self) -> str:
        """Gets the memory-swap value. This value is passed to the docker
        container to ensure that the total memory limit (memory + swap) is the
        same value as the 'mem' key in soa-configs. Note - this value *has* to
        be >= to the mem key, so we always round up to the closest MB and add
        additional 64MB for the docker executor (See PAASTA-12450).
        """
        mem = self.get_mem()
        mem_swap = int(math.ceil(mem + 64))
        return "%sm" % mem_swap

    def get_cpus(self) -> float:
        """Gets the number of cpus required from the service's configuration.

        Defaults to 1 cpu if no value specified in the config.

        :returns: The number of cpus specified in the config, 1 if not specified"""
        cpus = self.config_dict.get("cpus", 1)
        return cpus

    def get_cpu_burst_add(self) -> float:
        """Returns the number of additional cpus a container is allowed to use.
        Defaults to DEFAULT_CPU_BURST_ADD"""
        return self.config_dict.get("cpu_burst_add", DEFAULT_CPU_BURST_ADD)

    def get_cpu_period(self) -> float:
        """The --cpu-period option to be passed to docker
        Comes from the cfs_period_us configuration option

        :returns: The number to be passed to the --cpu-period docker flag"""
        return self.config_dict.get("cfs_period_us", DEFAULT_CPU_PERIOD)

    def get_cpu_quota(self) -> float:
        """Gets the --cpu-quota option to be passed to docker

        Calculation: (cpus + cpus_burst_add) * cfs_period_us

        :returns: The number to be passed to the --cpu-quota docker flag"""
        cpu_burst_add = self.get_cpu_burst_add()
        return (self.get_cpus() + cpu_burst_add) * self.get_cpu_period()

    def get_extra_docker_args(self) -> Dict[str, str]:
        return self.config_dict.get("extra_docker_args", {})

    def get_cap_add(self) -> Iterable[DockerParameter]:
        """Get the --cap-add options to be passed to docker
        Generated from the cap_add configuration option, which is a list of
        capabilities.

        Example configuration: {'cap_add': ['IPC_LOCK', 'SYS_PTRACE']}

        :returns: A generator of cap_add options to be passed as --cap-add flags"""
        for value in self.config_dict.get("cap_add", []):
            yield {"key": "cap-add", "value": f"{value}"}

    def get_cap_drop(self) -> Iterable[DockerParameter]:
        """Generates --cap-drop options to be passed to docker by default, which
        makes them not able to perform special privilege escalation stuff
        https://docs.docker.com/engine/reference/run/#runtime-privilege-and-linux-capabilities
        """
        for cap in CAPS_DROP:
            yield {"key": "cap-drop", "value": cap}

    def get_cap_args(self) -> Iterable[DockerParameter]:
        """Generate all --cap-add/--cap-drop parameters, ensuring not to have overlapping settings"""
        cap_adds = list(self.get_cap_add())
        if cap_adds and is_using_unprivileged_containers():
            log.warning(
                "Unprivileged containerizer detected, adding capabilities will not work properly"
            )
        yield from cap_adds
        added_caps = [cap["value"] for cap in cap_adds]
        for cap in self.get_cap_drop():
            if cap["value"] not in added_caps:
                yield cap

    def format_docker_parameters(
        self,
        with_labels: bool = True,
        system_paasta_config: Optional["SystemPaastaConfig"] = None,
    ) -> List[DockerParameter]:
        """Formats extra flags for running docker.  Will be added in the format
        `["--%s=%s" % (e['key'], e['value']) for e in list]` to the `docker run` command
        Note: values must be strings

        :param with_labels: Whether to build docker parameters with or without labels
        :returns: A list of parameters to be added to docker run"""
        parameters: List[DockerParameter] = [
            {"key": "memory-swap", "value": self.get_mem_swap()},
            {"key": "cpu-period", "value": "%s" % int(self.get_cpu_period())},
            {"key": "cpu-quota", "value": "%s" % int(self.get_cpu_quota())},
        ]
        if self.use_docker_disk_quota(system_paasta_config=system_paasta_config):
            parameters.append(
                {
                    "key": "storage-opt",
                    "value": f"size={int(self.get_disk() * 1024 * 1024)}",
                }
            )
        if with_labels:
            parameters.extend(
                [
                    {"key": "label", "value": "paasta_service=%s" % self.service},
                    {"key": "label", "value": "paasta_instance=%s" % self.instance},
                ]
            )
        extra_docker_args = self.get_extra_docker_args()
        if extra_docker_args:
            for key, value in extra_docker_args.items():
                parameters.extend([{"key": key, "value": value}])
        parameters.extend(self.get_docker_init())
        parameters.extend(self.get_cap_args())
        return parameters

    def use_docker_disk_quota(
        self, system_paasta_config: Optional["SystemPaastaConfig"] = None
    ) -> bool:
        if system_paasta_config is None:
            system_paasta_config = load_system_paasta_config()
        return system_paasta_config.get_enforce_disk_quota()

    def get_docker_init(self) -> Iterable[DockerParameter]:
        return [{"key": "init", "value": "true"}]

    def get_disk(self, default: float = 1024) -> float:
        """Gets the amount of disk space in MiB required from the service's configuration.

        Defaults to 1024 (1GiB) if no value is specified in the config.

        :returns: The amount of disk space specified by the config, 1024 MiB if not specified"""
        disk = self.config_dict.get("disk", default)
        return disk

    def get_gpus(self) -> Optional[int]:
        """Gets the number of gpus required from the service's configuration.

        Default to None if no value is specified in the config.

        :returns: The number of gpus specified by the config, 0 if not specified"""
        gpus = self.config_dict.get("gpus", None)
        return gpus

    def get_container_type(self) -> Optional[str]:
        """Get Mesos containerizer type.

        Default to DOCKER if gpus are not used.

        :returns: Mesos containerizer type, DOCKER or MESOS"""
        if self.get_gpus() is not None:
            container_type = "MESOS"
        else:
            container_type = "DOCKER"
        return container_type

    def get_cmd(self) -> Optional[Union[str, List[str]]]:
        """Get the docker cmd specified in the service's configuration.

        Defaults to None if not specified in the config.

        :returns: A string specified in the config, None if not specified"""
        return self.config_dict.get("cmd", None)

    def get_instance_type(self) -> Optional[str]:
        return getattr(self, "config_filename_prefix", None)

    def get_env_dictionary(
        self, system_paasta_config: Optional["SystemPaastaConfig"] = None
    ) -> Dict[str, str]:
        """A dictionary of key/value pairs that represent environment variables
        to be injected to the container environment"""
        env = {
            "PAASTA_SERVICE": self.service,
            "PAASTA_INSTANCE": self.instance,
            "PAASTA_CLUSTER": self.cluster,
            "PAASTA_DEPLOY_GROUP": self.get_deploy_group(),
            "PAASTA_DOCKER_IMAGE": self.get_docker_image(),
            "PAASTA_RESOURCE_CPUS": str(self.get_cpus()),
            "PAASTA_RESOURCE_MEM": str(self.get_mem()),
            "PAASTA_RESOURCE_DISK": str(self.get_disk()),
        }
        if self.get_gpus() is not None:
            env["PAASTA_RESOURCE_GPUS"] = str(self.get_gpus())
        try:
            env["PAASTA_GIT_SHA"] = get_git_sha_from_dockerurl(
                self.get_docker_url(system_paasta_config=system_paasta_config)
            )
        except Exception:
            pass
        image_version = self.get_image_version()
        if image_version is not None:
            env["PAASTA_IMAGE_VERSION"] = image_version
        team = self.get_team()
        if team:
            env["PAASTA_MONITORING_TEAM"] = team
        instance_type = self.get_instance_type()
        if instance_type:
            env["PAASTA_INSTANCE_TYPE"] = instance_type
        user_env = self.config_dict.get("env", {})
        env.update(user_env)
        return {str(k): str(v) for (k, v) in env.items()}

    def get_env(
        self, system_paasta_config: Optional["SystemPaastaConfig"] = None
    ) -> Dict[str, str]:
        """Basic get_env that simply returns the basic env, other classes
        might need to override this getter for more implementation-specific
        env getting"""
        return self.get_env_dictionary(system_paasta_config=system_paasta_config)

    def get_args(self) -> Optional[List[str]]:
        """Get the docker args specified in the service's configuration.

        If not specified in the config and if cmd is not specified, defaults to an empty array.
        If not specified in the config but cmd is specified, defaults to null.
        If specified in the config and if cmd is also specified, throws an exception. Only one may be specified.

        :param service_config: The service instance's configuration dictionary
        :returns: An array of args specified in the config,
            ``[]`` if not specified and if cmd is not specified,
            otherwise None if not specified but cmd is specified"""
        if self.get_cmd() is None:
            return self.config_dict.get("args", [])
        else:
            args = self.config_dict.get("args", None)
            if args is None:
                return args
            else:
                # TODO validation stuff like this should be moved into a check_*
                raise InvalidInstanceConfig(
                    "Instance configuration can specify cmd or args, but not both."
                )

    def get_monitoring(self) -> MonitoringDict:
        """Get monitoring overrides defined for the given instance"""
        return self.config_dict.get("monitoring", {})

    def get_deploy_constraints(
        self,
        blacklist: DeployBlacklist,
        whitelist: DeployWhitelist,
        system_deploy_blacklist: DeployBlacklist,
        system_deploy_whitelist: DeployWhitelist,
    ) -> List[Constraint]:
        """Return the combination of deploy_blacklist and deploy_whitelist
        as a list of constraints.
        """
        return (
            deploy_blacklist_to_constraints(blacklist)
            + deploy_whitelist_to_constraints(whitelist)
            + deploy_blacklist_to_constraints(system_deploy_blacklist)
            + deploy_whitelist_to_constraints(system_deploy_whitelist)
        )

    def get_deploy_blacklist(self) -> DeployBlacklist:
        """The deploy blacklist is a list of lists, where the lists indicate
        which locations the service should not be deployed"""
        return safe_deploy_blacklist(self.config_dict.get("deploy_blacklist", []))

    def get_deploy_whitelist(self) -> DeployWhitelist:
        """The deploy whitelist is a tuple of (location_type, [allowed value, allowed value, ...]).
        To have tasks scheduled on it, a host must be covered by the deploy whitelist (if present) and not excluded by
        the deploy blacklist."""

        return safe_deploy_whitelist(self.config_dict.get("deploy_whitelist"))

    def get_docker_image(self) -> str:
        """Get the docker image name (with tag) for a given service branch from
        a generated deployments.json file."""
        if self.branch_dict is not None:
            return self.branch_dict["docker_image"]
        else:
            return ""

    def get_image_version(self) -> Optional[str]:
        """Get additional information identifying the Docker image from a
        generated deployments.json file."""
        if self.branch_dict is not None and "image_version" in self.branch_dict:
            return self.branch_dict["image_version"]
        else:
            return None

    def get_docker_url(
        self, system_paasta_config: Optional["SystemPaastaConfig"] = None
    ) -> str:
        """Compose the docker url.
        :returns: '<registry_uri>/<docker_image>'
        """
        registry_uri = self.get_docker_registry(
            system_paasta_config=system_paasta_config
        )
        docker_image = self.get_docker_image()
        if not docker_image:
            raise NoDockerImageError(
                "Docker url not available because there is no docker_image"
            )
        docker_url = f"{registry_uri}/{docker_image}"
        return docker_url

    def get_desired_state(self) -> str:
        """Get the desired state (either 'start' or 'stop') for a given service
        branch from a generated deployments.json file."""
        if self.branch_dict is not None:
            return self.branch_dict["desired_state"]
        else:
            return "start"

    def get_force_bounce(self) -> Optional[str]:
        """Get the force_bounce token for a given service branch from a generated
        deployments.json file. This is a token that, when changed, indicates that
        the instance should be recreated and bounced, even if no other
        parameters have changed. This may be None or a string, generally a
        timestamp.
        """
        if self.branch_dict is not None:
            return self.branch_dict["force_bounce"]
        else:
            return None

    def check_cpus(self) -> Tuple[bool, str]:
        cpus = self.get_cpus()
        if cpus is not None:
            if not isinstance(cpus, (float, int)):
                return (
                    False,
                    'The specified cpus value "%s" is not a valid float or int.' % cpus,
                )
        return True, ""

    def check_mem(self) -> Tuple[bool, str]:
        mem = self.get_mem()
        if mem is not None:
            if not isinstance(mem, (float, int)):
                return (
                    False,
                    'The specified mem value "%s" is not a valid float or int.' % mem,
                )
        return True, ""

    def check_disk(self) -> Tuple[bool, str]:
        disk = self.get_disk()
        if disk is not None:
            if not isinstance(disk, (float, int)):
                return (
                    False,
                    'The specified disk value "%s" is not a valid float or int.' % disk,
                )
        return True, ""

    def check_security(self) -> Tuple[bool, str]:
        security = self.config_dict.get("security")
        if security is None:
            return True, ""

        outbound_firewall = security.get("outbound_firewall")

        if outbound_firewall is None:
            return True, ""

        if outbound_firewall is not None and outbound_firewall not in (
            "block",
            "monitor",
        ):
            return (
                False,
                'Unrecognized outbound_firewall value "%s"' % outbound_firewall,
            )

        unknown_keys = set(security.keys()) - {
            "outbound_firewall",
        }
        if unknown_keys:
            return (
                False,
                'Unrecognized items in security dict of service config: "%s"'
                % ",".join(unknown_keys),
            )

        return True, ""

    def check_dependencies_reference(self) -> Tuple[bool, str]:
        dependencies_reference = self.config_dict.get("dependencies_reference")
        if dependencies_reference is None:
            return True, ""

        dependencies = self.config_dict.get("dependencies")
        if dependencies is None:
            return (
                False,
                'dependencies_reference "%s" declared but no dependencies found'
                % dependencies_reference,
            )

        if dependencies_reference not in dependencies:
            return (
                False,
                'dependencies_reference "%s" not found in dependencies dictionary'
                % dependencies_reference,
            )

        return True, ""

    def check(self, param: str) -> Tuple[bool, str]:
        check_methods = {
            "cpus": self.check_cpus,
            "mem": self.check_mem,
            "security": self.check_security,
            "dependencies_reference": self.check_dependencies_reference,
            "deploy_group": self.check_deploy_group,
        }
        check_method = check_methods.get(param)
        if check_method is not None:
            return check_method()
        else:
            return (
                False,
                'Your service config specifies "%s", an unsupported parameter.' % param,
            )

    def validate(
        self,
        params: Optional[List[str]] = None,
    ) -> List[str]:
        if params is None:
            params = [
                "cpus",
                "mem",
                "security",
                "dependencies_reference",
                "deploy_group",
            ]
        error_msgs = []
        for param in params:
            check_passed, check_msg = self.check(param)
            if not check_passed:
                error_msgs.append(check_msg)
        return error_msgs

    def check_deploy_group(self) -> Tuple[bool, str]:
        deploy_group = self.get_deploy_group()
        if deploy_group is not None:
            pipeline_deploy_groups = get_pipeline_deploy_groups(
                service=self.service, soa_dir=self.soa_dir
            )
            if deploy_group not in pipeline_deploy_groups:
                return (
                    False,
                    f"{self.service}.{self.instance} uses deploy_group {deploy_group}, but {deploy_group} is not deployed to in deploy.yaml",
                )  # noqa: E501
        return True, ""

    def get_extra_volumes(self) -> List[DockerVolume]:
        """Extra volumes are a specially formatted list of dictionaries that should
        be bind mounted in a container The format of the dictionaries should
        conform to the `Mesos container volumes spec
        <https://mesosphere.github.io/marathon/docs/native-docker.html>`_"""
        return self.config_dict.get("extra_volumes", [])

    def get_aws_ebs_volumes(self) -> List[AwsEbsVolume]:
        return self.config_dict.get("aws_ebs_volumes", [])

    def get_secret_volumes(self) -> List[SecretVolume]:
        return self.config_dict.get("secret_volumes", [])

    def get_projected_sa_volumes(self) -> List[ProjectedSAVolume]:
        return self.config_dict.get("projected_sa_volumes", [])

    def get_iam_role(self) -> str:
        return self.config_dict.get("iam_role", "")

    def get_iam_role_provider(self) -> str:
        return self.config_dict.get("iam_role_provider", "aws")

    def get_role(self) -> Optional[str]:
        """Which mesos role of nodes this job should run on."""
        return self.config_dict.get("role")

    def get_pool(self) -> str:
        """Which pool of nodes this job should run on. This can be used to mitigate noisy neighbors, by putting
        particularly noisy or noise-sensitive jobs into different pools.

        This is implemented with an attribute "pool" on each mesos slave and by adding a constraint or node selector.

        Eventually this may be implemented with Mesos roles, once a framework can register under multiple roles.

        :returns: the "pool" attribute in your config dict, or the string "default" if not specified."""
        return self.config_dict.get("pool", "default")

    def get_pool_constraints(self) -> List[Constraint]:
        pool = self.get_pool()
        return [["pool", "LIKE", pool]]

    def get_constraints(self) -> Optional[List[Constraint]]:
        return stringify_constraints(self.config_dict.get("constraints", None))

    def get_extra_constraints(self) -> List[Constraint]:
        return stringify_constraints(self.config_dict.get("extra_constraints", []))

    def get_net(self) -> str:
        """
        :returns: the docker networking mode the container should be started with.
        """
        return self.config_dict.get("net", "bridge")

    def get_volumes(self, system_volumes: Sequence[DockerVolume]) -> List[DockerVolume]:
        volumes = list(system_volumes) + list(self.get_extra_volumes())
        return _reorder_docker_volumes(volumes)

    def get_persistent_volumes(self) -> Sequence[PersistentVolume]:
        return self.config_dict.get("persistent_volumes", [])

    def get_dependencies_reference(self) -> Optional[str]:
        """Get the reference to an entry in dependencies.yaml

        Defaults to None if not specified in the config.

        :returns: A string specified in the config, None if not specified"""
        return self.config_dict.get("dependencies_reference")

    def get_dependencies(self) -> Optional[Dict]:
        """Get the contents of the dependencies_dict pointed to by the dependency_reference or
        'main' if no dependency_reference exists

        Defaults to None if not specified in the config.

        :returns: A list of dictionaries specified in the dependencies_dict, None if not specified"""
        dependencies = self.config_dict.get("dependencies")
        if not dependencies:
            return None
        dependency_ref = self.get_dependencies_reference() or "main"
        return dependencies.get(dependency_ref)

    def get_outbound_firewall(self) -> Optional[str]:
        """Return 'block', 'monitor', or None as configured in security->outbound_firewall

        Defaults to None if not specified in the config

        :returns: A string specified in the config, None if not specified"""
        security = self.config_dict.get("security")
        if not security:
            return None
        return security.get("outbound_firewall")

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, type(self)):
            return (
                self.config_dict == other.config_dict
                and self.branch_dict == other.branch_dict
                and self.cluster == other.cluster
                and self.instance == other.instance
                and self.service == other.service
            )
        else:
            return False


def stringify_constraint(usc: UnstringifiedConstraint) -> Constraint:
    return [str(x) for x in usc]


def stringify_constraints(
    uscs: Optional[List[UnstringifiedConstraint]],
) -> List[Constraint]:
    if uscs is None:
        return None
    return [stringify_constraint(usc) for usc in uscs]


@time_cache(ttl=60)
def validate_service_instance(
    service: str, instance: str, cluster: str, soa_dir: str
) -> str:
    possibilities: List[str] = []
    for instance_type in INSTANCE_TYPES:
        sis = get_service_instance_list(
            service=service,
            cluster=cluster,
            instance_type=instance_type,
            soa_dir=soa_dir,
        )
        if (service, instance) in sis:
            return instance_type
        possibilities.extend(si[1] for si in sis)
    else:
        suggestions = suggest_possibilities(word=instance, possibilities=possibilities)
        raise NoConfigurationForServiceError(
            f"Error: {compose_job_id(service, instance)} doesn't look like it has been configured "
            f"to run on the {cluster} cluster.{suggestions}"
        )


_ComposeRetT = TypeVar("_ComposeRetT")
_ComposeInnerRetT = TypeVar("_ComposeInnerRetT")


def compose(
    func_one: Callable[[_ComposeInnerRetT], _ComposeRetT],
    func_two: Callable[..., _ComposeInnerRetT],
) -> Callable[..., _ComposeRetT]:
    def composed(*args: Any, **kwargs: Any) -> _ComposeRetT:
        return func_one(func_two(*args, **kwargs))

    return composed


class PaastaColors:

    """Collection of static variables and methods to assist in coloring text."""

    # ANSI color codes
    BLUE = "\033[34m"
    BOLD = "\033[1m"
    CYAN = "\033[36m"
    DEFAULT = "\033[0m"
    GREEN = "\033[32m"
    GREY = "\033[38;5;242m"
    MAGENTA = "\033[35m"
    RED = "\033[31m"
    YELLOW = "\033[33m"

    @staticmethod
    def bold(text: str) -> str:
        """Return bolded text.

        :param text: a string
        :return: text color coded with ANSI bold
        """
        return PaastaColors.color_text(PaastaColors.BOLD, text)

    @staticmethod
    def blue(text: str) -> str:
        """Return text that can be printed blue.

        :param text: a string
        :return: text color coded with ANSI blue
        """
        return PaastaColors.color_text(PaastaColors.BLUE, text)

    @staticmethod
    def green(text: str) -> str:
        """Return text that can be printed green.

        :param text: a string
        :return: text color coded with ANSI green"""
        return PaastaColors.color_text(PaastaColors.GREEN, text)

    @staticmethod
    def red(text: str) -> str:
        """Return text that can be printed red.

        :param text: a string
        :return: text color coded with ANSI red"""
        return PaastaColors.color_text(PaastaColors.RED, text)

    @staticmethod
    def magenta(text: str) -> str:
        """Return text that can be printed magenta.

        :param text: a string
        :return: text color coded with ANSI magenta"""
        return PaastaColors.color_text(PaastaColors.MAGENTA, text)

    @staticmethod
    def color_text(color: str, text: str) -> str:
        """Return text that can be printed color.

        :param color: ANSI color code
        :param text: a string
        :return: a string with ANSI color encoding"""

        if os.getenv("NO_COLOR", "0") == "1":
            return text

        # any time text returns to default, we want to insert our color.
        replaced = text.replace(PaastaColors.DEFAULT, PaastaColors.DEFAULT + color)
        # then wrap the beginning and end in our color/default.
        return color + replaced + PaastaColors.DEFAULT

    @staticmethod
    def cyan(text: str) -> str:
        """Return text that can be printed cyan.

        :param text: a string
        :return: text color coded with ANSI cyan"""
        return PaastaColors.color_text(PaastaColors.CYAN, text)

    @staticmethod
    def yellow(text: str) -> str:
        """Return text that can be printed yellow.

        :param text: a string
        :return: text color coded with ANSI yellow"""
        return PaastaColors.color_text(PaastaColors.YELLOW, text)

    @staticmethod
    def grey(text: str) -> str:
        return PaastaColors.color_text(PaastaColors.GREY, text)

    @staticmethod
    def default(text: str) -> str:
        return PaastaColors.color_text(PaastaColors.DEFAULT, text)


LOG_COMPONENTS: Mapping[str, Mapping[str, Any]] = OrderedDict(
    [
        (
            "build",
            {
                "color": PaastaColors.blue,
                "help": (
                    "Logs for pre-deployment steps, such as itests, "
                    "image building, and security checks."
                ),
                "source_env": "devc",
            },
        ),
        (
            "deploy",
            {
                "color": PaastaColors.cyan,
                "help": (
                    "Logs for deployment steps and actions, such as "
                    "bouncing, start/stop/restart, and instance cleanup."
                ),
                "additional_source_envs": ["devc"],
            },
        ),
        (
            "monitoring",
            {
                "color": PaastaColors.green,
                "help": "Logs from Sensu checks for the service",
            },
        ),
        (
            "app_output",
            {
                "color": compose(PaastaColors.yellow, PaastaColors.bold),
                "help": (
                    "Stderr and stdout from a service's running processes. "
                    "Alias for both the stdout and stderr components."
                ),
            },
        ),
        (
            "stdout",
            {
                "color": PaastaColors.yellow,
                "help": "Stdout from a service's running processes.",
            },
        ),
        (
            "stderr",
            {
                "color": PaastaColors.yellow,
                "help": "Stderr from a service's running processes.",
            },
        ),
        (
            "security",
            {
                "color": PaastaColors.red,
                "help": "Logs from security-related services such as firewall monitoring",
            },
        ),
        ("oom", {"color": PaastaColors.red, "help": "Kernel OOM events."}),
        (
            "task_lifecycle",
            {
                "color": PaastaColors.bold,
                "help": "Logs that tell you about task startup, failures, healthchecks, etc.",
            },
        ),
        # I'm leaving these planned components here since they provide some hints
        # about where we want to go. See PAASTA-78.
        #
        # But I'm commenting them out so they don't delude users into believing we
        # can expose logs that we cannot actually expose. See PAASTA-927.
        #
        # ('app_request', {
        #     'color': PaastaColors.bold,
        #     'help': 'The request log for the service. Defaults to "service_NAME_requests"',
        #     'command': 'scribe_reader -e ENV -f service_example_happyhour_requests',
        # }),
        # ('app_errors', {
        #     'color': PaastaColors.red,
        #     'help': 'Application error log, defaults to "stream_service_NAME_errors"',
        #     'command': 'scribe_reader -e ENV -f stream_service_SERVICE_errors',
        # }),
        # ('lb_requests', {
        #     'color': PaastaColors.bold,
        #     'help': 'All requests from Smartstack haproxy',
        #     'command': 'NA - TODO: SRV-1130',
        # }),
        # ('lb_errors', {
        #     'color': PaastaColors.red,
        #     'help': 'Logs from Smartstack haproxy that have 400-500 error codes',
        #     'command': 'scribereader -e ENV -f stream_service_errors | grep SERVICE.instance',
        # }),
    ]
)


class NoSuchLogComponent(Exception):
    pass


def validate_log_component(component: str) -> bool:
    if component in LOG_COMPONENTS.keys():
        return True
    else:
        raise NoSuchLogComponent


def get_git_url(service: str, soa_dir: str = DEFAULT_SOA_DIR) -> str:
    """Get the git url for a service. Assumes that the service's
    repo matches its name, and that it lives in services- i.e.
    if this is called with the string 'test', the returned
    url will be git@github.yelpcorp.com:services/test.

    :param service: The service name to get a URL for
    :returns: A git url to the service's repository"""
    general_config = service_configuration_lib.read_service_configuration(
        service, soa_dir=soa_dir
    )
    # TODO: PAASTA-16927: get this from system config `.git_config`
    default_location = format_git_url(
        "git", "github.yelpcorp.com", f"services/{service}"
    )
    return general_config.get("git_url", default_location)


def format_git_url(git_user: str, git_server: str, repo_name: str) -> str:
    return f"{git_user}@{git_server}:{repo_name}"


def get_service_docker_registry(
    service: str,
    soa_dir: str = DEFAULT_SOA_DIR,
    system_config: Optional["SystemPaastaConfig"] = None,
) -> str:
    if service is None:
        raise NotImplementedError('"None" is not a valid service')
    service_configuration = service_configuration_lib.read_service_configuration(
        service, soa_dir
    )
    try:
        return service_configuration["docker_registry"]
    except KeyError:
        if not system_config:
            system_config = load_system_paasta_config()
        return system_config.get_system_docker_registry()


class NoSuchLogLevel(Exception):
    pass


class LogWriterConfig(TypedDict):
    driver: str
    options: Dict


class LogReaderConfig(TypedDict):
    driver: str
    options: Dict
    components: Optional[List]


# The active log writer.
_log_writer = None
# The map of name -> LogWriter subclasses, used by configure_log.
_log_writer_classes = {}


class LogWriter:
    def __init__(self, **kwargs: Any) -> None:
        pass

    def log(
        self,
        service: str,
        line: str,
        component: str,
        level: str = DEFAULT_LOGLEVEL,
        cluster: str = ANY_CLUSTER,
        instance: str = ANY_INSTANCE,
    ) -> None:
        raise NotImplementedError()

    def log_audit(
        self,
        user: str,
        host: str,
        action: str,
        action_details: dict = None,
        service: str = None,
        cluster: str = ANY_CLUSTER,
        instance: str = ANY_INSTANCE,
    ) -> None:
        raise NotImplementedError()


_LogWriterTypeT = TypeVar("_LogWriterTypeT", bound=Type[LogWriter])


def register_log_writer(name: str) -> Callable[[_LogWriterTypeT], _LogWriterTypeT]:
    """Returns a decorator that registers that log writer class at a given name
    so get_log_writer_class can find it."""

    def outer(log_writer_class: _LogWriterTypeT) -> _LogWriterTypeT:
        _log_writer_classes[name] = log_writer_class
        return log_writer_class

    return outer


def get_log_writer_class(name: str) -> Type[LogWriter]:
    return _log_writer_classes[name]


def list_log_writers() -> Iterable[str]:
    return _log_writer_classes.keys()


def configure_log() -> None:
    """We will log to the yocalhost binded scribe."""
    log_writer_config = load_system_paasta_config().get_log_writer()
    global _log_writer
    LogWriterClass = get_log_writer_class(log_writer_config["driver"])
    _log_writer = LogWriterClass(**log_writer_config.get("options", {}))


def _log(
    service: str,
    line: str,
    component: str,
    level: str = DEFAULT_LOGLEVEL,
    cluster: str = ANY_CLUSTER,
    instance: str = ANY_INSTANCE,
) -> None:
    if _log_writer is None:
        configure_log()
    return _log_writer.log(
        service=service,
        line=line,
        component=component,
        level=level,
        cluster=cluster,
        instance=instance,
    )


def _log_audit(
    action: str,
    action_details: dict = None,
    service: str = None,
    cluster: str = ANY_CLUSTER,
    instance: str = ANY_INSTANCE,
) -> None:
    if _log_writer is None:
        configure_log()

    user = get_username()
    host = get_hostname()

    return _log_writer.log_audit(
        user=user,
        host=host,
        action=action,
        action_details=action_details,
        service=service,
        cluster=cluster,
        instance=instance,
    )


def _now() -> str:
    return datetime.datetime.utcnow().isoformat()


def remove_ansi_escape_sequences(line: str) -> str:
    """Removes ansi escape sequences from the given line."""
    return no_escape.sub("", line)


def format_log_line(
    level: str,
    cluster: str,
    service: str,
    instance: str,
    component: str,
    line: str,
    timestamp: str = None,
) -> str:
    """Accepts a string 'line'.

    Returns an appropriately-formatted dictionary which can be serialized to
    JSON for logging and which contains 'line'.
    """

    validate_log_component(component)
    if not timestamp:
        timestamp = _now()
    line = remove_ansi_escape_sequences(line.strip())
    message = json.dumps(
        {
            "timestamp": timestamp,
            "level": level,
            "cluster": cluster,
            "service": service,
            "instance": instance,
            "component": component,
            "message": line,
        },
        sort_keys=True,
    )
    return message


def format_audit_log_line(
    cluster: str,
    instance: str,
    user: str,
    host: str,
    action: str,
    action_details: dict = None,
    service: str = None,
    timestamp: str = None,
) -> str:
    """Accepts:

        * a string 'user' describing the user that initiated the action
        * a string 'host' describing the server where the user initiated the action
        * a string 'action' describing an action performed by paasta_tools
        * a dict 'action_details' optional information about the action

    Returns an appropriately-formatted dictionary which can be serialized to
    JSON for logging and which contains details about an action performed on
    a service/instance.
    """
    if not timestamp:
        timestamp = _now()
    if not action_details:
        action_details = {}

    message = json.dumps(
        {
            "timestamp": timestamp,
            "cluster": cluster,
            "service": service,
            "instance": instance,
            "user": user,
            "host": host,
            "action": action,
            "action_details": action_details,
        },
        sort_keys=True,
    )
    return message


def get_log_name_for_service(service: str, prefix: str = None) -> str:
    if prefix:
        return f"stream_paasta_{prefix}_{service}"
    return "stream_paasta_%s" % service


try:
    import clog

    # Somehow clog turns on DeprecationWarnings, so we need to disable them
    # again after importing it.
    warnings.filterwarnings("ignore", category=DeprecationWarning)

    class CLogWriter(LogWriter):
        def __init__(self, **kwargs: Any):
            clog.config.configure(**kwargs)

        def log(
            self,
            service: str,
            line: str,
            component: str,
            level: str = DEFAULT_LOGLEVEL,
            cluster: str = ANY_CLUSTER,
            instance: str = ANY_INSTANCE,
        ) -> None:
            """This expects someone (currently the paasta cli main()) to have already
            configured the log object. We'll just write things to it.
            """
            if level == "event":
                print(f"[service {service}] {line}", file=sys.stdout)
            elif level == "debug":
                print(f"[service {service}] {line}", file=sys.stderr)
            else:
                raise NoSuchLogLevel
            log_name = get_log_name_for_service(service)
            formatted_line = format_log_line(
                level, cluster, service, instance, component, line
            )
            clog.log_line(log_name, formatted_line)

        def log_audit(
            self,
            user: str,
            host: str,
            action: str,
            action_details: dict = None,
            service: str = None,
            cluster: str = ANY_CLUSTER,
            instance: str = ANY_INSTANCE,
        ) -> None:
            log_name = AUDIT_LOG_STREAM
            formatted_line = format_audit_log_line(
                user=user,
                host=host,
                action=action,
                action_details=action_details,
                service=service,
                cluster=cluster,
                instance=instance,
            )
            clog.log_line(log_name, formatted_line)

    @register_log_writer("monk")
    class MonkLogWriter(CLogWriter):
        def __init__(
            self,
            monk_host: str = "169.254.255.254",
            monk_port: int = 1473,
            monk_disable: bool = False,
            **kwargs: Any,
        ) -> None:
            super().__init__(
                monk_host=monk_host,
                monk_port=monk_port,
                monk_disable=monk_disable,
            )

    @register_log_writer("scribe")
    class ScribeLogWriter(CLogWriter):
        def __init__(
            self,
            scribe_host: str = "169.254.255.254",
            scribe_port: int = 1463,
            scribe_disable: bool = False,
            **kwargs: Any,
        ) -> None:
            super().__init__(
                scribe_host=scribe_host,
                scribe_port=scribe_port,
                scribe_disable=scribe_disable,
            )

except ImportError:
    warnings.warn("clog is unavailable")


@register_log_writer("null")
class NullLogWriter(LogWriter):
    """A LogWriter class that doesn't do anything. Primarily useful for integration tests where we don't care about
    logs."""

    def __init__(self, **kwargs: Any) -> None:
        pass

    def log(
        self,
        service: str,
        line: str,
        component: str,
        level: str = DEFAULT_LOGLEVEL,
        cluster: str = ANY_CLUSTER,
        instance: str = ANY_INSTANCE,
    ) -> None:
        pass

    def log_audit(
        self,
        user: str,
        host: str,
        action: str,
        action_details: dict = None,
        service: str = None,
        cluster: str = ANY_CLUSTER,
        instance: str = ANY_INSTANCE,
    ) -> None:
        pass


@contextlib.contextmanager
def _empty_context() -> Iterator[None]:
    yield


_AnyIO = Union[io.IOBase, IO]


@register_log_writer("file")
class FileLogWriter(LogWriter):
    def __init__(
        self,
        path_format: str,
        mode: str = "a+",
        line_delimiter: str = "\n",
        flock: bool = False,
    ) -> None:
        self.path_format = path_format
        self.mode = mode
        self.flock = flock
        self.line_delimiter = line_delimiter

    def maybe_flock(self, fd: _AnyIO) -> ContextManager:
        if self.flock:
            # https://github.com/python/typeshed/issues/1548
            return flock(fd)
        else:
            return _empty_context()

    def format_path(
        self, service: str, component: str, level: str, cluster: str, instance: str
    ) -> str:
        return self.path_format.format(
            service=service,
            component=component,
            level=level,
            cluster=cluster,
            instance=instance,
        )

    def _log_message(self, path: str, message: str) -> None:
        # We use io.FileIO here because it guarantees that write() is implemented with a single write syscall,
        # and on Linux, writes to O_APPEND files with a single write syscall are atomic.
        #
        # https://docs.python.org/2/library/io.html#io.FileIO
        # http://article.gmane.org/gmane.linux.kernel/43445

        try:
            with io.FileIO(path, mode=self.mode, closefd=True) as f:
                with self.maybe_flock(f):
                    f.write(message.encode("UTF-8"))
        except IOError as e:
            print(
                "Could not log to {}: {}: {} -- would have logged: {}".format(
                    path, type(e).__name__, str(e), message
                ),
                file=sys.stderr,
            )

    def log(
        self,
        service: str,
        line: str,
        component: str,
        level: str = DEFAULT_LOGLEVEL,
        cluster: str = ANY_CLUSTER,
        instance: str = ANY_INSTANCE,
    ) -> None:
        path = self.format_path(service, component, level, cluster, instance)
        to_write = "{}{}".format(
            format_log_line(level, cluster, service, instance, component, line),
            self.line_delimiter,
        )

        self._log_message(path, to_write)

    def log_audit(
        self,
        user: str,
        host: str,
        action: str,
        action_details: dict = None,
        service: str = None,
        cluster: str = ANY_CLUSTER,
        instance: str = ANY_INSTANCE,
    ) -> None:
        path = self.format_path(AUDIT_LOG_STREAM, "", "", cluster, instance)
        formatted_line = format_audit_log_line(
            user=user,
            host=host,
            action=action,
            action_details=action_details,
            service=service,
            cluster=cluster,
            instance=instance,
        )

        to_write = f"{formatted_line}{self.line_delimiter}"

        self._log_message(path, to_write)


@contextlib.contextmanager
def flock(fd: _AnyIO) -> Iterator[None]:
    try:
        fcntl.flock(fd.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd.fileno(), fcntl.LOCK_UN)


@contextlib.contextmanager
def timed_flock(fd: _AnyIO, seconds: int = 1) -> Iterator[None]:
    """Attempt to grab an exclusive flock with a timeout. Uses Timeout, so will
    raise a TimeoutError if `seconds` elapses before the flock can be obtained
    """
    # We don't want to wrap the user code in the timeout, just the flock grab
    flock_context = flock(fd)
    with Timeout(seconds=seconds):
        flock_context.__enter__()
    try:
        yield
    finally:
        flock_context.__exit__(*sys.exc_info())


def _timeout(process: Popen) -> None:
    """Helper function for _run. It terminates the process.
    Doesn't raise OSError, if we try to terminate a non-existing
    process as there can be a very small window between poll() and kill()
    """
    if process.poll() is None:
        try:
            # sending SIGKILL to the process
            process.kill()
        except OSError as e:
            # No such process error
            # The process could have been terminated meanwhile
            if e.errno != errno.ESRCH:
                raise


class PaastaNotConfiguredError(Exception):
    pass


class NoConfigurationForServiceError(Exception):
    pass


def get_readable_files_in_glob(glob: str, path: str) -> List[str]:
    """
    Returns a sorted list of files that are readable in an input glob by recursively searching a path
    """
    globbed_files = []
    for root, dirs, files in os.walk(path):
        for f in files:
            fn = os.path.join(root, f)
            if os.path.isfile(fn) and os.access(fn, os.R_OK) and fnmatch(fn, glob):
                globbed_files.append(fn)
    return sorted(globbed_files)


class ClusterAutoscalingResource(TypedDict):
    type: str
    id: str
    region: str
    pool: str
    min_capacity: int
    max_capacity: int


IdToClusterAutoscalingResourcesDict = Dict[str, ClusterAutoscalingResource]


class ResourcePoolSettings(TypedDict):
    target_utilization: float
    drain_timeout: int


PoolToResourcePoolSettingsDict = Dict[str, ResourcePoolSettings]


class LocalRunConfig(TypedDict, total=False):
    default_cluster: str


class RemoteRunConfig(TypedDict, total=False):
    default_role: str


class SparkRunConfig(TypedDict, total=False):
    default_cluster: str
    default_pool: str
    default_spark_driver_iam_role: str


class PaastaNativeConfig(TypedDict, total=False):
    principal: str
    secret: str


ExpectedSlaveAttributes = List[Dict[str, Any]]


class KubeKindDict(TypedDict, total=False):
    singular: str
    plural: str


class KubeCustomResourceDict(TypedDict, total=False):
    version: str
    file_prefix: str
    kube_kind: KubeKindDict
    group: str


class KubeStateMetricsCollectorConfigDict(TypedDict, total=False):
    unaggregated_metrics: List[str]
    summed_metric_to_group_keys: Dict[str, List[str]]
    label_metric_to_label_key: Dict[str, List[str]]
    label_renames: Dict[str, str]


class TopologySpreadConstraintDict(TypedDict, total=False):
    topology_key: str
    when_unsatisfiable: Literal["ScheduleAnyway", "DoNotSchedule"]
    max_skew: int


class SystemPaastaConfigDict(TypedDict, total=False):
    allowed_pools: Dict[str, List[str]]
    api_client_timeout: int
    api_endpoints: Dict[str, str]
    api_profiling_config: Dict
    auth_certificate_ttl: str
    auto_config_instance_types_enabled: Dict[str, bool]
    auto_config_instance_type_aliases: Dict[str, str]
    auto_hostname_unique_size: int
    cluster_fqdn_format: str
    clusters: Sequence[str]
    cluster: str
    cr_owners: Dict[str, str]
    dashboard_links: Dict[str, Dict[str, str]]
    datastore_credentials_vault_env_overrides: Dict[str, str]
    default_push_groups: List
    default_should_use_uwsgi_exporter: bool
    deploy_blacklist: UnsafeDeployBlacklist
    deployd_metrics_provider: str
    deploy_whitelist: UnsafeDeployWhitelist
    disabled_watchers: List
    dockercfg_location: str
    docker_registry: str
    enable_client_cert_auth: bool
    enable_nerve_readiness_check: bool
    enable_envoy_readiness_check: bool
    enforce_disk_quota: bool
    envoy_admin_domain_name: str
    envoy_admin_endpoint_format: str
    envoy_nerve_readiness_check_script: List[str]
    envoy_readiness_check_script: List[str]
    expected_slave_attributes: ExpectedSlaveAttributes
    filter_bogus_mesos_cputime_enabled: bool
    fsm_template: str
    git_config: Dict
    hacheck_sidecar_image_url: str
    hacheck_sidecar_volumes: List[DockerVolume]
    kubernetes_add_registration_labels: bool
    kubernetes_custom_resources: List[KubeCustomResourceDict]
    kubernetes_use_hacheck_sidecar: bool
    ldap_host: str
    ldap_reader_password: str
    ldap_reader_username: str
    ldap_search_base: str
    ldap_search_ou: str
    local_run_config: LocalRunConfig
    log_reader: LogReaderConfig
    log_readers: List[LogReaderConfig]
    log_writer: LogWriterConfig
    mark_for_deployment_max_polling_threads: int
    mark_for_deployment_default_polling_interval: float
    mark_for_deployment_default_diagnosis_interval: float
    mark_for_deployment_default_default_time_before_first_diagnosis: float
    mark_for_deployment_should_ping_for_unhealthy_pods: bool
    mesos_config: Dict
    metrics_provider: str
    monitoring_config: Dict
    nerve_readiness_check_script: List[str]
    nerve_register_k8s_terminating: bool
    paasta_native: PaastaNativeConfig
    paasta_status_version: str
    pdb_max_unavailable: Union[str, int]
    pki_backend: str
    pod_defaults: Dict[str, Any]
    pool_node_affinities: Dict[str, Dict[str, List[str]]]
    topology_spread_constraints: List[TopologySpreadConstraintDict]
    readiness_check_prefix_template: List[str]
    register_k8s_pods: bool
    register_native_services: bool
    remote_run_config: RemoteRunConfig
    resource_pool_settings: PoolToResourcePoolSettingsDict
    secret_provider: str
    security_check_command: str
    sensu_host: str
    sensu_port: int
    service_discovery_providers: Dict[str, Any]
    slack: Dict[str, str]
    spark_run_config: SparkRunConfig
    supported_storage_classes: Sequence[str]
    synapse_haproxy_url_format: str
    synapse_host: str
    synapse_port: int
    taskproc: Dict
    tron: Dict
    gunicorn_exporter_sidecar_image_url: str
    vault_cluster_map: Dict
    vault_environment: str
    volumes: List[DockerVolume]
    zookeeper: str
    tron_k8s_cluster_overrides: Dict[str, str]
    skip_cpu_override_validation: List[str]
    spark_k8s_role: str
    cluster_aliases: Dict[str, str]
    hacheck_match_initial_delay: bool
    spark_ui_port: int
    spark_driver_port: int
    spark_blockmanager_port: int
    skip_cpu_burst_validation: List[str]
    tron_default_pool_override: str
    spark_kubeconfig: str
    kube_clusters: Dict
    spark_use_eks_default: bool
    sidecar_requirements_config: Dict[str, KubeContainerResourceRequest]
    eks_cluster_aliases: Dict[str, str]
    secret_sync_delay_seconds: float
    use_multiple_log_readers: Optional[List[str]]
    service_auth_token_settings: ProjectedSAVolume
    always_authenticating_services: List[str]
    mysql_port_mappings: Dict
    vitess_images: Dict
    superregion_to_region_mapping: Dict
    vitess_tablet_types: List[str]
    vitess_tablet_pool_type_mapping: Dict
    vitess_throttling_config: Dict


def load_system_paasta_config(
    path: str = PATH_TO_SYSTEM_PAASTA_CONFIG_DIR,
) -> "SystemPaastaConfig":
    """
    Reads Paasta configs in specified directory in lexicographical order and deep merges
    the dictionaries (last file wins).
    """
    if not os.path.isdir(path):
        raise PaastaNotConfiguredError(
            "Could not find system paasta configuration directory: %s" % path
        )

    if not os.access(path, os.R_OK):
        raise PaastaNotConfiguredError(
            "Could not read from system paasta configuration directory: %s" % path
        )

    try:
        file_stats = frozenset(
            {
                (fn, os.stat(fn))
                for fn in get_readable_files_in_glob(glob="*.json", path=path)
            }
        )
        return parse_system_paasta_config(file_stats, path)
    except IOError as e:
        raise PaastaNotConfiguredError(
            f"Could not load system paasta config file {e.filename}: {e.strerror}"
        )


def optionally_load_system_paasta_config(
    path: str = PATH_TO_SYSTEM_PAASTA_CONFIG_DIR,
) -> "SystemPaastaConfig":
    """
    Tries to load the system paasta config, but will return an empty configuration if not available,
    without raising.
    """
    try:
        return load_system_paasta_config(path=path)
    except PaastaNotConfiguredError:
        return SystemPaastaConfig({}, "")


@lru_cache()
def parse_system_paasta_config(
    file_stats: FrozenSet[Tuple[str, os.stat_result]], path: str
) -> "SystemPaastaConfig":
    """Pass in a dictionary of filename -> os.stat_result, and this returns the merged parsed configs"""
    config: SystemPaastaConfigDict = {}
    for filename, _ in file_stats:
        with open(filename) as f:
            config = deep_merge_dictionaries(
                json.load(f), config, allow_duplicate_keys=False
            )
    return SystemPaastaConfig(config, path)


class PoolsNotConfiguredError(Exception):
    pass


def validate_pool(
    cluster: str, pool: str, system_paasta_config: "SystemPaastaConfig"
) -> bool:
    if pool:
        valid_pools = system_paasta_config.get_pools_for_cluster(cluster)
        if not valid_pools:
            raise PoolsNotConfiguredError
        # at this point, we can be sure that `valid_pools` is populated
        return pool in valid_pools
    return True


class SystemPaastaConfig:
    def __init__(self, config: SystemPaastaConfigDict, directory: str) -> None:
        self.directory = directory
        self.config_dict = config

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, SystemPaastaConfig):
            return (
                self.directory == other.directory
                and self.config_dict == other.config_dict
            )
        return False

    def __repr__(self) -> str:
        return f"SystemPaastaConfig({self.config_dict!r}, {self.directory!r})"

    def get_secret_sync_delay_seconds(self) -> float:
        return self.config_dict.get("secret_sync_delay_seconds", 0)

    def get_spark_use_eks_default(self) -> bool:
        return self.config_dict.get("spark_use_eks_default", False)

    def get_sidecar_requirements_config(
        self,
    ) -> Dict[str, KubeContainerResourceRequest]:
        return self.config_dict.get("sidecar_requirements_config", {})

    def get_tron_default_pool_override(self) -> str:
        """Get the default pool override variable defined in this host's cluster config file.

        :returns: The default_pool_override specified in the paasta configuration
        """
        return self.config_dict.get("tron_default_pool_override", "default")

    def get_zk_hosts(self) -> str:
        """Get the zk_hosts defined in this hosts's cluster config file.
        Strips off the zk:// prefix, if it exists, for use with Kazoo.

        :returns: The zk_hosts specified in the paasta configuration
        """
        try:
            hosts = self.config_dict["zookeeper"]
        except KeyError:
            raise PaastaNotConfiguredError(
                "Could not find zookeeper connection string in configuration directory: %s"
                % self.directory
            )

        # how do python strings not have a method for doing this
        if hosts.startswith("zk://"):
            return hosts[len("zk://") :]
        return hosts

    def get_system_docker_registry(self) -> str:
        """Get the docker_registry defined in this host's cluster config file.

        :returns: The docker_registry specified in the paasta configuration
        """
        try:
            return self.config_dict["docker_registry"]
        except KeyError:
            raise PaastaNotConfiguredError(
                "Could not find docker registry in configuration directory: %s"
                % self.directory
            )

    def get_hacheck_sidecar_volumes(self) -> List[DockerVolume]:
        """Get the hacheck sidecar volumes defined in this host's hacheck_sidecar_volumes config file.

        :returns: The list of volumes specified in the paasta configuration
        """
        try:
            volumes = self.config_dict["hacheck_sidecar_volumes"]
        except KeyError:
            raise PaastaNotConfiguredError(
                "Could not find hacheck_sidecar_volumes in configuration directory: %s"
                % self.directory
            )
        return _reorder_docker_volumes(list(volumes))

    def get_volumes(self) -> Sequence[DockerVolume]:
        """Get the volumes defined in this host's volumes config file.

        :returns: The list of volumes specified in the paasta configuration
        """
        try:
            return self.config_dict["volumes"]
        except KeyError:
            raise PaastaNotConfiguredError(
                "Could not find volumes in configuration directory: %s" % self.directory
            )

    def get_cluster(self) -> str:
        """Get the cluster defined in this host's cluster config file.

        :returns: The name of the cluster defined in the paasta configuration
        """
        try:
            return self.config_dict["cluster"]
        except KeyError:
            raise PaastaNotConfiguredError(
                "Could not find cluster in configuration directory: %s" % self.directory
            )

    def get_dashboard_links(self) -> Mapping[str, Mapping[str, str]]:
        return self.config_dict["dashboard_links"]

    def get_cr_owners(self) -> Dict[str, str]:
        return self.config_dict["cr_owners"]

    def get_auto_hostname_unique_size(self) -> int:
        """
        We automatically add a ["hostname", "UNIQUE"] constraint to "small" services running in production clusters.
        If there are less than or equal to this number of instances, we consider it small.
        We fail safe and return -1 to avoid adding the ['hostname', 'UNIQUE'] constraint if this value is not defined

        :returns: The integer size of a small service
        """
        return self.config_dict.get("auto_hostname_unique_size", -1)

    def get_auto_config_instance_types_enabled(self) -> Dict[str, bool]:
        return self.config_dict.get("auto_config_instance_types_enabled", {})

    def get_auto_config_instance_type_aliases(self) -> Dict[str, str]:
        """
        Allow re-using another instance type's autotuned data. This is useful when an instance can be trivially moved around
        type-wise as it allows us to avoid data races/issues with the autotuned recommendations generator/updater.
        """
        return self.config_dict.get("auto_config_instance_type_aliases", {})

    def get_api_client_timeout(self) -> int:
        """
        We've seen the Paasta API get hung up sometimes and the client not realizing this will sit idle forever.
        This will be used to specify the default timeout
        """
        return self.config_dict.get("api_client_timeout", 120)

    def get_api_endpoints(self) -> Mapping[str, str]:
        return self.config_dict["api_endpoints"]

    def get_enable_client_cert_auth(self) -> bool:
        """
        If enabled present a client certificate from ~/.paasta/pki/<cluster>.crt and ~/.paasta/pki/<cluster>.key
        """
        return self.config_dict.get("enable_client_cert_auth", True)

    def get_enable_nerve_readiness_check(self) -> bool:
        """
        If enabled perform readiness checks on nerve
        """
        return self.config_dict.get("enable_nerve_readiness_check", True)

    def get_enable_envoy_readiness_check(self) -> bool:
        """
        If enabled perform readiness checks on envoy
        """
        return self.config_dict.get("enable_envoy_readiness_check", False)

    def get_nerve_readiness_check_script(self) -> List[str]:
        return self.config_dict.get(
            "nerve_readiness_check_script", ["/check_smartstack_up.sh"]
        )

    def get_envoy_readiness_check_script(self) -> List[str]:
        return self.config_dict.get(
            "envoy_readiness_check_script",
            ["/check_proxy_up.sh", "--enable-envoy", "--envoy-check-mode", "eds-dir"],
        )

    def get_envoy_nerve_readiness_check_script(self) -> List[str]:
        return self.config_dict.get(
            "envoy_nerve_readiness_check_script",
            ["/check_proxy_up.sh", "--enable-smartstack", "--enable-envoy"],
        )

    def get_nerve_register_k8s_terminating(self) -> bool:
        return self.config_dict.get("nerve_register_k8s_terminating", True)

    def get_enforce_disk_quota(self) -> bool:
        """
        If enabled, add `--storage-opt size=SIZE` arg to `docker run` calls,
        enforcing the disk quota as a result.

        Please note that this should be enabled only for a suported environment
        (which at the moment is only `overlay2` driver backed by `XFS`
        filesystem mounted with `prjquota` option) otherwise Docker will fail
        to start.
        """
        return self.config_dict.get("enforce_disk_quota", False)

    def get_auth_certificate_ttl(self) -> str:
        """
        How long to request for ttl on auth certificates. Note that this maybe limited
        by policy in Vault
        """
        return self.config_dict.get("auth_certificate_ttl", "11h")

    def get_fsm_template(self) -> str:
        fsm_path = os.path.dirname(paasta_tools.cli.fsm.__file__)
        template_path = os.path.join(fsm_path, "template")
        return self.config_dict.get("fsm_template", template_path)

    def get_log_writer(self) -> LogWriterConfig:
        """Get the log_writer configuration out of global paasta config

        :returns: The log_writer dictionary.
        """
        try:
            return self.config_dict["log_writer"]
        except KeyError:
            raise PaastaNotConfiguredError(
                "Could not find log_writer in configuration directory: %s"
                % self.directory
            )

    def get_log_reader(self) -> LogReaderConfig:
        """Get the log_reader configuration out of global paasta config

        :returns: the log_reader dictionary.
        """
        try:
            return self.config_dict["log_reader"]
        except KeyError:
            raise PaastaNotConfiguredError(
                "Could not find log_reader in configuration directory: %s"
                % self.directory
            )

    def get_log_readers(self) -> List[LogReaderConfig]:
        """Get the log_readers configuration out of global paasta config

        :returns: the log_readers list of dicts.
        """
        try:
            return self.config_dict["log_readers"]
        except KeyError:
            raise PaastaNotConfiguredError(
                "Could not find log_readers in configuration directory: %s"
                % self.directory
            )

    def use_multiple_log_readers(self) -> Optional[List[str]]:
        """
        Get the list of clusters that are using multiple log readers
        """
        return self.config_dict.get("use_multiple_log_readers")

    def get_metrics_provider(self) -> Optional[str]:
        """Get the metrics_provider configuration out of global paasta config

        :returns: A string identifying the metrics_provider
        """
        deployd_metrics_provider = self.config_dict.get("deployd_metrics_provider")
        if deployd_metrics_provider is not None:
            return deployd_metrics_provider
        return self.config_dict.get("metrics_provider")

    def get_sensu_host(self) -> str:
        """Get the host that we should send sensu events to.

        :returns: the sensu_host string, or localhost if not specified.
        """
        return self.config_dict.get("sensu_host", "localhost")

    def get_sensu_port(self) -> int:
        """Get the port that we should send sensu events to.

        :returns: the sensu_port value as an integer, or 3030 if not specified.
        """
        return int(self.config_dict.get("sensu_port", 3030))

    def get_dockercfg_location(self) -> str:
        """Get the location of the dockerfile, as a URI.

        :returns: the URI specified, or file:///root/.dockercfg if not specified.
        """
        return self.config_dict.get("dockercfg_location", DEFAULT_DOCKERCFG_LOCATION)

    def get_synapse_port(self) -> int:
        """Get the port that haproxy-synapse exposes its status on. Defaults to 3212.

        :returns: the haproxy-synapse status port."""
        return int(self.config_dict.get("synapse_port", 3212))

    def get_default_synapse_host(self) -> str:
        """Get the default host we should interrogate for haproxy-synapse state.

        :returns: A hostname that is running haproxy-synapse."""
        return self.config_dict.get("synapse_host", "localhost")

    def get_synapse_haproxy_url_format(self) -> str:
        """Get a format string for the URL to query for haproxy-synapse state. This format string gets two keyword
        arguments, host and port. Defaults to "http://{host:s}:{port:d}/;csv;norefresh".

        :returns: A format string for constructing the URL of haproxy-synapse's status page."""
        return self.config_dict.get(
            "synapse_haproxy_url_format", DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT
        )

    def get_service_discovery_providers(self) -> Dict[str, Any]:
        return self.config_dict.get("service_discovery_providers", {})

    def get_resource_pool_settings(self) -> PoolToResourcePoolSettingsDict:
        return self.config_dict.get("resource_pool_settings", {})

    def get_cluster_fqdn_format(self) -> str:
        """Get a format string that constructs a DNS name pointing at the paasta masters in a cluster. This format
        string gets one parameter: cluster. Defaults to 'paasta-{cluster:s}.yelp'.

        :returns: A format string for constructing the FQDN of the masters in a given cluster."""
        return self.config_dict.get("cluster_fqdn_format", "paasta-{cluster:s}.yelp")

    def get_paasta_status_version(self) -> str:
        """Get paasta status version string (new | old). Defaults to 'old'.

        :returns: A string with the version desired version of paasta status."""
        return self.config_dict.get("paasta_status_version", "old")

    def get_local_run_config(self) -> LocalRunConfig:
        """Get the local-run config

        :returns: The local-run job config dictionary"""
        return self.config_dict.get("local_run_config", {})

    def get_remote_run_config(self) -> RemoteRunConfig:
        """Get the remote-run config

        :returns: The remote-run system_paasta_config dictionary"""
        return self.config_dict.get("remote_run_config", {})

    def get_spark_run_config(self) -> SparkRunConfig:
        """Get the spark-run config

        :returns: The spark-run system_paasta_config dictionary"""
        return self.config_dict.get("spark_run_config", {})

    def get_paasta_native_config(self) -> PaastaNativeConfig:
        return self.config_dict.get("paasta_native", {})

    def get_mesos_cli_config(self) -> Dict:
        """Get the config for mesos-cli

        :returns: The mesos cli config
        """
        return self.config_dict.get("mesos_config", {})

    def get_monitoring_config(self) -> Dict:
        """Get the monitoring config

        :returns: the monitoring config dictionary"""
        return self.config_dict.get("monitoring_config", {})

    def get_deploy_blacklist(self) -> DeployBlacklist:
        """Get global blacklist. This applies to all services
        in the cluster

        :returns: The blacklist
        """
        return safe_deploy_blacklist(self.config_dict.get("deploy_blacklist", []))

    def get_deploy_whitelist(self) -> DeployWhitelist:
        """Get global whitelist. This applies to all services
        in the cluster

        :returns: The whitelist
        """

        return safe_deploy_whitelist(self.config_dict.get("deploy_whitelist"))

    def get_expected_slave_attributes(self) -> ExpectedSlaveAttributes:
        """Return a list of dictionaries, representing the expected combinations of attributes in this cluster. Used for
        calculating the default routing constraints."""
        return self.config_dict.get("expected_slave_attributes")

    def get_security_check_command(self) -> Optional[str]:
        """Get the script to be executed during the security-check build step

        :return: The name of the file
        """
        return self.config_dict.get("security_check_command", None)

    def get_hacheck_sidecar_image_url(self) -> str:
        """Get the docker image URL for the hacheck sidecar container"""
        return self.config_dict.get("hacheck_sidecar_image_url")

    def get_register_k8s_pods(self) -> bool:
        """Enable registration of k8s services in nerve"""
        return self.config_dict.get("register_k8s_pods", False)

    def get_kubernetes_add_registration_labels(self) -> bool:
        return self.config_dict.get("kubernetes_add_registration_labels", False)

    def get_kubernetes_custom_resources(self) -> Sequence[KubeCustomResourceDict]:
        """List of custom resources that should be synced by setup_kubernetes_cr"""
        return self.config_dict.get("kubernetes_custom_resources", [])

    def get_kubernetes_use_hacheck_sidecar(self) -> bool:
        return self.config_dict.get("kubernetes_use_hacheck_sidecar", True)

    def get_register_native_services(self) -> bool:
        """Enable registration of native paasta services in nerve"""
        return self.config_dict.get("register_native_services", False)

    def get_taskproc(self) -> Dict:
        return self.config_dict.get("taskproc", {})

    def get_disabled_watchers(self) -> List:
        return self.config_dict.get("disabled_watchers", [])

    def get_pool_node_affinities(self) -> Dict[str, Dict[str, List[str]]]:
        """Node selectors that will be applied to all Pods in a pool"""
        return self.config_dict.get("pool_node_affinities", {})

    def get_topology_spread_constraints(self) -> List[TopologySpreadConstraintDict]:
        """List of TopologySpreadConstraints that will be applied to all Pods in the cluster"""
        return self.config_dict.get("topology_spread_constraints", [])

    def get_datastore_credentials_vault_overrides(self) -> Dict[str, str]:
        """In order to use different Vault shards, vault-tools allows you to override
        environment variables (CA, token file, and URL). DB credentials are stored in
        a different shard to minimize the impact on the core Vault shard (which has
        size restrictions derived from Zookeeper limitations)."""
        return self.config_dict.get("datastore_credentials_vault_env_overrides", {})

    def get_vault_environment(self) -> Optional[str]:
        """Get the environment name for the vault cluster
        This must match the environment keys in the secret json files
        used by all services in this cluster"""
        return self.config_dict.get("vault_environment")

    def get_vault_cluster_config(self) -> dict:
        """Get a map from paasta_cluster to vault ecosystem. We need
        this because not every ecosystem will have its own vault cluster"""
        return self.config_dict.get("vault_cluster_map", {})

    def get_secret_provider_name(self) -> str:
        """Get the name for the configured secret_provider, used to
        decrypt secrets"""
        return self.config_dict.get("secret_provider", "paasta_tools.secret_providers")

    def get_slack_token(self) -> str:
        """Get a slack token for slack notifications. Returns None if there is
        none available"""
        return self.config_dict.get("slack", {}).get("token", None)

    def get_tron_config(self) -> dict:
        return self.config_dict.get("tron", {})

    def get_clusters(self) -> Sequence[str]:
        return self.config_dict.get("clusters", [])

    def get_supported_storage_classes(self) -> Sequence[str]:
        return self.config_dict.get("supported_storage_classes", [])

    def get_envoy_admin_endpoint_format(self) -> str:
        """Get the format string for Envoy's admin interface."""
        return self.config_dict.get(
            "envoy_admin_endpoint_format", "http://{host:s}:{port:d}/{endpoint:s}"
        )

    def get_envoy_admin_port(self) -> int:
        """Get the port that Envoy's admin interface is listening on
        from /etc/services."""
        return socket.getservbyname(
            self.config_dict.get("envoy_admin_domain_name", "envoy-admin")
        )

    def get_pdb_max_unavailable(self) -> Union[str, int]:
        return self.config_dict.get("pdb_max_unavailable", 0)

    def get_pod_defaults(self) -> Dict[str, Any]:
        return self.config_dict.get("pod_defaults", {})

    def get_ldap_search_base(self) -> str:
        return self.config_dict.get("ldap_search_base", None)

    def get_ldap_search_ou(self) -> str:
        return self.config_dict.get("ldap_search_ou", None)

    def get_ldap_host(self) -> str:
        return self.config_dict.get("ldap_host", None)

    def get_ldap_reader_username(self) -> str:
        return self.config_dict.get("ldap_reader_username", None)

    def get_ldap_reader_password(self) -> str:
        return self.config_dict.get("ldap_reader_password", None)

    def get_default_push_groups(self) -> List:
        return self.config_dict.get("default_push_groups", None)

    def get_git_config(self) -> Dict:
        """Gets git configuration. Includes repo names and their git servers.

        :returns: the git config dict
        """
        return self.config_dict.get(
            "git_config",
            {
                "git_user": "git",
                "repos": {
                    "yelpsoa-configs": {
                        "repo_name": "yelpsoa-configs",
                        "git_server": DEFAULT_SOA_CONFIGS_GIT_URL,
                        "deploy_server": DEFAULT_SOA_CONFIGS_GIT_URL,
                    },
                },
            },
        )

    def get_git_repo_config(self, repo_name: str) -> Dict:
        """Gets the git configuration for a specific repo.

        :returns: the git config dict for a specific repo.
        """
        return self.get_git_config().get("repos", {}).get(repo_name, {})

    def default_should_use_uwsgi_exporter(self) -> bool:
        return self.config_dict.get("default_should_use_uwsgi_exporter", False)

    def get_gunicorn_exporter_sidecar_image_url(self) -> str:
        """Get the docker image URL for the gunicorn_exporter sidecar container"""
        return self.config_dict.get(
            "gunicorn_exporter_sidecar_image_url",
            "docker-paasta.yelpcorp.com:443/gunicorn_exporter-k8s-sidecar:v0.24.0-yelp0",
        )

    def get_mark_for_deployment_max_polling_threads(self) -> int:
        return self.config_dict.get("mark_for_deployment_max_polling_threads", 4)

    def get_mark_for_deployment_default_polling_interval(self) -> float:
        return self.config_dict.get("mark_for_deployment_default_polling_interval", 60)

    def get_mark_for_deployment_default_diagnosis_interval(self) -> float:
        return self.config_dict.get(
            "mark_for_deployment_default_diagnosis_interval", 60
        )

    def get_mark_for_deployment_default_time_before_first_diagnosis(self) -> float:
        return self.config_dict.get(
            "mark_for_deployment_default_default_time_before_first_diagnosis", 300
        )

    def get_mark_for_deployment_should_ping_for_unhealthy_pods(self) -> bool:
        return self.config_dict.get(
            "mark_for_deployment_should_ping_for_unhealthy_pods", True
        )

    def get_spark_k8s_role(self) -> str:
        return self.config_dict.get("spark_k8s_role", "spark")

    def get_spark_driver_port(self) -> int:
        # default value is an arbitrary value
        return self.config_dict.get("spark_driver_port", 33001)

    def get_spark_blockmanager_port(self) -> int:
        # default value is an arbitrary value
        return self.config_dict.get("spark_blockmanager_port", 33002)

    def get_api_profiling_config(self) -> Dict:
        return self.config_dict.get(
            "api_profiling_config",
            {"cprofile_sampling_enabled": False},
        )

    def get_skip_cpu_override_validation_services(self) -> List[str]:
        return self.config_dict.get("skip_cpu_override_validation", [])

    def get_skip_cpu_burst_validation_services(self) -> List[str]:
        return self.config_dict.get("skip_cpu_burst_validation", [])

    def get_cluster_aliases(self) -> Dict[str, str]:
        return self.config_dict.get("cluster_aliases", {})

    def get_eks_cluster_aliases(self) -> Dict[str, str]:
        return self.config_dict.get("eks_cluster_aliases", {})

    def get_cluster_pools(self) -> Dict[str, List[str]]:
        return self.config_dict.get("allowed_pools", {})

    def get_spark_driver_iam_role(self) -> str:
        return self.get_spark_run_config().get("default_spark_driver_iam_role", "")

    def get_spark_executor_iam_role(self) -> str:
        # use the same IAM role as the Spark driver
        return self.get_spark_run_config().get("default_spark_driver_iam_role", "")

    def get_pools_for_cluster(self, cluster: str) -> List[str]:
        return self.get_cluster_pools().get(cluster, [])

    def get_hacheck_match_initial_delay(self) -> bool:
        return self.config_dict.get("hacheck_match_initial_delay", False)

    def get_readiness_check_prefix_template(self) -> List[str]:
        """A prefix that will be added to the beginning of the readiness check command. Meant for e.g. `flock` and
        `timeout`."""
        # We use flock+timeout here to work around issues discovered in PAASTA-17673:
        # In k8s 1.18, probe timeout wasn't respected at all.
        # When we upgraded to k8s 1.20, the timeout started being partially respected - k8s would stop waiting for a
        # response, but wouldn't kill the command within the container (with the dockershim CRI).
        # Flock prevents multiple readiness probes from running at once, using lots of CPU.
        # The generous timeout allows for a slow readiness probe, but ensures that a truly-stuck readiness probe command
        # will eventually be killed so another process can retry.
        # Once we move off dockershim, we'll likely need to increase the readiness probe timeout, but we can then remove
        # this wrapper.
        return self.config_dict.get(
            "readiness_check_prefix_template",
            ["flock", "-n", "/readiness_check_lock", "timeout", "120"],
        )

    def get_tron_k8s_cluster_overrides(self) -> Dict[str, str]:
        """
        Return a mapping of a tron cluster -> compute cluster. Returns an empty dict if there are no overrides set.

        This exists as we have certain Tron masters that are named differently from the compute cluster that should
        actually be used (e.g., we might have tron-XYZ-test-prod, but instead of scheduling on XYZ-test-prod, we'd
        like to schedule jobs on test-prod).

        To control this, we have an optional config item that we'll puppet onto Tron masters that need this type of
        tron master -> compute cluster override which this function will read.
        """
        return self.config_dict.get("tron_k8s_cluster_overrides", {})

    def get_spark_kubeconfig(self) -> str:
        return self.config_dict.get("spark_kubeconfig", "/etc/kubernetes/spark.conf")

    def get_kube_clusters(self) -> Dict:
        return self.config_dict.get("kube_clusters", {})

    def get_service_auth_token_volume_config(self) -> ProjectedSAVolume:
        return self.config_dict.get("service_auth_token_settings", {})

    def get_always_authenticating_services(self) -> List[str]:
        return self.config_dict.get("always_authenticating_services", [])

    def get_mysql_port_mappings(self) -> Dict:
        return self.config_dict.get("mysql_port_mappings", {})

    def get_vitess_images(self) -> Dict:
        return self.config_dict.get(
            "vitess_images",
            {
                "vtctld_image": "docker-paasta.yelpcorp.com:443/vitess_base:v16.0.3",
                "vtgate_image": "docker-paasta.yelpcorp.com:443/vitess_base:v16.0.3",
                "vttablet_image": "docker-paasta.yelpcorp.com:443/vitess_base:v16.0.3",
                "vtadmin_image": "docker-paasta.yelpcorp.com:443/vtadmin:v16.0.3",
            },
        )

    def get_superregion_to_region_mapping(self) -> Dict:
        return self.config_dict.get("superregion_to_region_mapping", {})

    def get_vitess_tablet_types(self) -> List:
        return self.config_dict.get("vitess_tablet_types", ["primary", "migration"])

    def get_vitess_tablet_pool_type_mapping(self) -> Dict:
        return self.config_dict.get("vitess_tablet_pool_type_mapping", {})

    def get_vitess_throttling_config(self) -> Dict:
        return self.config_dict.get(
            "vitess_throttling_config",
            {
                "migration": {
                    "throttle_query_table": "migration_replication_delay",
                    "throttle_metrics_threshold": "7200",
                },
                "read": {
                    "throttle_query_table": "read_replication_delay",
                    "throttle_metrics_threshold": "3",
                },
                "reporting": {
                    "throttle_query_table": "reporting_replication_delay",
                    "throttle_metrics_threshold": "7200",
                },
                "primary": {
                    "throttle_query_table": "read_replication_delay",
                    "throttle_metrics_threshold": "3",
                },
            },
        )


def _run(
    command: Union[str, List[str]],
    env: Mapping[str, str] = os.environ,
    timeout: float = None,
    log: bool = False,
    stream: bool = False,
    stdin: Any = None,
    stdin_interrupt: bool = False,
    popen_kwargs: Dict = {},
    **kwargs: Any,
) -> Tuple[int, str]:
    """Given a command, run it. Return a tuple of the return code and any
    output.

    :param timeout: If specified, the command will be terminated after timeout
        seconds.
    :param log: If True, the _log will be handled by _run. If set, it is mandatory
        to pass at least a :service: and a :component: parameter. Optionally you
        can pass :cluster:, :instance: and :loglevel: parameters for logging.
    We wanted to use plumbum instead of rolling our own thing with
    subprocess.Popen but were blocked by
    https://github.com/tomerfiliba/plumbum/issues/162 and our local BASH_FUNC
    magic.
    """
    output: List[str] = []
    if log:
        service = kwargs["service"]
        component = kwargs["component"]
        cluster = kwargs.get("cluster", ANY_CLUSTER)
        instance = kwargs.get("instance", ANY_INSTANCE)
        loglevel = kwargs.get("loglevel", DEFAULT_LOGLEVEL)
    try:
        if not isinstance(command, list):
            command = shlex.split(command)
        popen_kwargs["stdout"] = PIPE
        popen_kwargs["stderr"] = STDOUT
        popen_kwargs["stdin"] = stdin
        popen_kwargs["env"] = env
        process = Popen(command, **popen_kwargs)

        if stdin_interrupt:

            def signal_handler(signum: int, frame: FrameType) -> None:
                process.stdin.write("\n".encode("utf-8"))
                process.stdin.flush()
                process.wait()

            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)

        # start the timer if we specified a timeout
        if timeout:
            proctimer = threading.Timer(timeout, _timeout, [process])
            proctimer.start()

        outfn: Any = print if stream else output.append
        for linebytes in iter(process.stdout.readline, b""):
            line = linebytes.decode("utf-8", errors="replace").rstrip("\n")
            outfn(line)

            if log:
                _log(
                    service=service,
                    line=line,
                    component=component,
                    level=loglevel,
                    cluster=cluster,
                    instance=instance,
                )
        # when finished, get the exit code
        process.wait()
        returncode = process.returncode
    except OSError as e:
        if log:
            _log(
                service=service,
                line=e.strerror.rstrip("\n"),
                component=component,
                level=loglevel,
                cluster=cluster,
                instance=instance,
            )
        output.append(e.strerror.rstrip("\n"))
        returncode = e.errno
    except (KeyboardInterrupt, SystemExit):
        # need to clean up the timing thread here
        if timeout:
            proctimer.cancel()
        raise
    else:
        # Stop the timer
        if timeout:
            proctimer.cancel()
    if returncode == -9:
        output.append(f"Command '{command}' timed out (longer than {timeout}s)")
    return returncode, "\n".join(output)


def get_umask() -> int:
    """Get the current umask for this process. NOT THREAD SAFE."""
    old_umask = os.umask(0o0022)
    os.umask(old_umask)
    return old_umask


def get_user_agent() -> str:
    base_name = os.path.basename(sys.argv[0])
    if base_name == "gunicorn":
        return f"{sys.argv[-1]} {paasta_tools.__version__}"
    elif len(sys.argv) >= 1:
        return f"{base_name} {paasta_tools.__version__}"
    else:
        return f"PaaSTA Tools {paasta_tools.__version__}"


@contextlib.contextmanager
def atomic_file_write(target_path: str) -> Iterator[IO]:
    dirname = os.path.dirname(target_path)
    basename = os.path.basename(target_path)

    if target_path == "-":
        yield sys.stdout
    else:
        with tempfile.NamedTemporaryFile(
            dir=dirname, prefix=(".%s-" % basename), delete=False, mode="w"
        ) as f:
            temp_target_path = f.name
            yield f

        mode = 0o0666 & (~get_umask())
        os.chmod(temp_target_path, mode)
        os.rename(temp_target_path, target_path)


class InvalidJobNameError(Exception):
    pass


def compose_job_id(
    name: str,
    instance: str,
    git_hash: Optional[str] = None,
    config_hash: Optional[str] = None,
    spacer: str = SPACER,
) -> str:
    """Compose a job/app id by concatenating its name, instance, git hash, and config hash.

    :param name: The name of the service
    :param instance: The instance of the service
    :param git_hash: The git_hash portion of the job_id. If git_hash is set,
                     config_hash must also be set.
    :param config_hash: The config_hash portion of the job_id. If config_hash
                        is set, git_hash must also be set.
    :returns: <name><SPACER><instance> if no tag, or <name><SPACER><instance><SPACER><hashes>...
              if extra hash inputs are provided.

    """
    composed = f"{name}{spacer}{instance}"
    if git_hash and config_hash:
        composed = f"{composed}{spacer}{git_hash}{spacer}{config_hash}"
    elif git_hash or config_hash:
        raise InvalidJobNameError(
            "invalid job id because git_hash (%s) and config_hash (%s) must "
            "both be defined or neither can be defined" % (git_hash, config_hash)
        )
    return composed


def decompose_job_id(job_id: str, spacer: str = SPACER) -> Tuple[str, str, str, str]:
    """Break a composed job id into its constituent (service name, instance,
    git hash, config hash) by splitting with ``spacer``.

    :param job_id: The composed id of the job/app
    :returns: A tuple (service name, instance, git hash, config hash) that
        comprise the job_id
    """
    decomposed = job_id.split(spacer)
    if len(decomposed) == 2:
        git_hash = None
        config_hash = None
    elif len(decomposed) == 4:
        git_hash = decomposed[2]
        config_hash = decomposed[3]
    else:
        raise InvalidJobNameError("invalid job id %s" % job_id)
    return (decomposed[0], decomposed[1], git_hash, config_hash)


def build_docker_image_name(service: str) -> str:
    """docker-paasta.yelpcorp.com:443 is the URL for the Registry where PaaSTA
    will look for your images.

    :returns: a sanitized-for-Jenkins (s,/,-,g) version of the
    service's path in git. E.g. For github.yelpcorp.com:services/foo the
    docker image name is docker_registry/services-foo.
    """
    docker_registry_url = get_service_docker_registry(service)
    name = f"{docker_registry_url}/services-{service}"
    return name


def build_docker_tag(
    service: str, upstream_git_commit: str, image_version: Optional[str] = None
) -> str:
    """Builds the DOCKER_TAG string

    upstream_git_commit is the SHA that we're building. Usually this is the
    tip of origin/master.
    """
    tag = "{}:paasta-{}".format(build_docker_image_name(service), upstream_git_commit)
    if image_version is not None:
        tag += f"-{image_version}"
    return tag


def check_docker_image(
    service: str,
    commit: str,
    image_version: Optional[str] = None,
) -> bool:
    """Checks whether the given image for :service: with :tag: exists.

    :raises: ValueError if more than one docker image with :tag: found.
    :returns: True if there is exactly one matching image found.
    """
    docker_client = get_docker_client()
    image_name = build_docker_image_name(service)
    docker_tag = build_docker_tag(service, commit, image_version)
    images = docker_client.images(name=image_name)
    # image['RepoTags'] may be None
    # Fixed upstream but only in docker-py 2.
    # https://github.com/docker/docker-py/issues/1401
    result = [image for image in images if docker_tag in (image["RepoTags"] or [])]
    if len(result) > 1:
        raise ValueError(
            f"More than one docker image found with tag {docker_tag}\n{result}"
        )
    return len(result) == 1


def datetime_from_utc_to_local(utc_datetime: datetime.datetime) -> datetime.datetime:
    return datetime_convert_timezone(
        utc_datetime, dateutil.tz.tzutc(), dateutil.tz.tzlocal()
    )


def datetime_convert_timezone(
    dt: datetime.datetime, from_zone: datetime.tzinfo, to_zone: datetime.tzinfo
) -> datetime.datetime:
    dt = dt.replace(tzinfo=from_zone)
    converted_datetime = dt.astimezone(to_zone)
    converted_datetime = converted_datetime.replace(tzinfo=None)
    return converted_datetime


def get_username() -> str:
    """Returns the current username in a portable way. Will use the SUDO_USER
    environment variable if present.
    http://stackoverflow.com/a/2899055
    """
    return os.environ.get("SUDO_USER", pwd.getpwuid(os.getuid())[0])


def get_hostname() -> str:
    """Returns the fully-qualified domain name of the server this code is
    running on.
    """
    return socket.getfqdn()


def get_files_of_type_in_dir(
    file_type: str,
    service: str = None,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> List[str]:
    """Recursively search path if type of file exists.

    :param file_type: a string of a type of a file (kubernetes, slo, etc.)
    :param service: a string of a service
    :param soa_dir: a string of a path to a soa_configs directory
    :return: a list
    """
    # TODO: Only use INSTANCE_TYPES as input by making file_type Literal
    service = "**" if service is None else service
    soa_dir = DEFAULT_SOA_DIR if soa_dir is None else soa_dir
    file_type += "-*.yaml"
    return [
        file_path
        for file_path in glob.glob(
            os.path.join(soa_dir, service, file_type),
            recursive=True,
        )
    ]


def get_soa_cluster_deploy_files(
    service: str = None, soa_dir: str = DEFAULT_SOA_DIR, instance_type: str = None
) -> Iterator[Tuple[str, str]]:
    if service is None:
        service = "*"
    service_path = os.path.join(soa_dir, service)

    valid_clusters = "|".join(load_system_paasta_config().get_clusters())

    if instance_type in INSTANCE_TYPES:
        instance_types = instance_type
    else:
        instance_types = "|".join(INSTANCE_TYPES)

    search_re = r"/.*/(" + instance_types + r")-(" + valid_clusters + r")\.yaml$"

    for yaml_file in glob.glob("%s/*.yaml" % service_path):
        try:
            with open(yaml_file):
                cluster_re_match = re.search(search_re, yaml_file)
                if cluster_re_match is not None:
                    cluster = cluster_re_match.group(2)
                    yield (cluster, yaml_file)
        except IOError as err:
            print(f"Error opening {yaml_file}: {err}")


def list_clusters(
    service: str = None, soa_dir: str = DEFAULT_SOA_DIR, instance_type: str = None
) -> List[str]:
    """Returns a sorted list of clusters a service is configured to deploy to,
    or all clusters if ``service`` is not specified.

    Includes every cluster that has a ``kubernetes-*.yaml`` or ``tron-*.yaml`` file associated with it.

    :param service: The service name. If unspecified, clusters running any service will be included.
    :returns: A sorted list of cluster names
    """
    clusters = set()
    for cluster, _ in get_soa_cluster_deploy_files(
        service=service, soa_dir=soa_dir, instance_type=instance_type
    ):
        clusters.add(cluster)
    return sorted(clusters)


def list_all_instances_for_service(
    service: str,
    clusters: Iterable[str] = None,
    instance_type: str = None,
    soa_dir: str = DEFAULT_SOA_DIR,
    cache: bool = True,
) -> Set[str]:
    instances = set()
    if not clusters:
        clusters = list_clusters(service, soa_dir=soa_dir)
    for cluster in clusters:
        if cache:
            si_list = get_service_instance_list(
                service, cluster, instance_type, soa_dir=soa_dir
            )
        else:
            si_list = get_service_instance_list_no_cache(
                service, cluster, instance_type, soa_dir=soa_dir
            )
        for service_instance in si_list:
            instances.add(service_instance[1])
    return instances


def filter_templates_from_config(config: Dict) -> Dict[str, Any]:
    config = {
        key: value for key, value in config.items() if not key.startswith("_")
    }  # filter templates
    return config or {}


def read_service_instance_names(
    service: str, instance_type: str, cluster: str, soa_dir: str
) -> Collection[Tuple[str, str]]:
    instance_list = []
    conf_file = f"{instance_type}-{cluster}"
    config = service_configuration_lib.read_extra_service_information(
        service,
        conf_file,
        soa_dir=soa_dir,
        deepcopy=False,
    )
    config = filter_templates_from_config(config)
    if instance_type == "tron":
        for job_name, job in config.items():
            action_names = list(job.get("actions", {}).keys())
            for name in action_names:
                instance = f"{job_name}.{name}"
                instance_list.append((service, instance))
    else:
        for instance in config:
            instance_list.append((service, instance))
    return instance_list


def get_production_deploy_group(service: str, soa_dir: str = DEFAULT_SOA_DIR) -> str:
    service_configuration = read_service_configuration(service, soa_dir)
    return service_configuration.get("deploy", {}).get("production_deploy_group", None)


def get_pipeline_config(service: str, soa_dir: str = DEFAULT_SOA_DIR) -> List[Dict]:
    service_configuration = read_service_configuration(service, soa_dir)
    return service_configuration.get("deploy", {}).get("pipeline", [])


def is_secrets_for_teams_enabled(service: str, soa_dir: str = DEFAULT_SOA_DIR) -> bool:
    service_yaml_contents = read_extra_service_information(service, "service", soa_dir)
    return service_yaml_contents.get("secrets_for_owner_team", False)


def get_pipeline_deploy_group_configs(
    service: str, soa_dir: str = DEFAULT_SOA_DIR
) -> List[Dict]:
    pipeline_steps = []
    for step in get_pipeline_config(service, soa_dir):
        # added support for parallel steps in a deploy.yaml
        # parallel steps would break previous functionality as steps arent
        # expected to be nested in a parallel block
        if step.get("parallel"):
            for parallel_step in step.get("parallel"):
                if parallel_step.get("step"):
                    pipeline_steps.append(parallel_step)
        else:
            pipeline_steps.append(step)
    return [step for step in pipeline_steps if is_deploy_step(step["step"])]


def get_pipeline_deploy_groups(
    service: str, soa_dir: str = DEFAULT_SOA_DIR
) -> List[str]:
    deploy_group_configs = get_pipeline_deploy_group_configs(service, soa_dir)
    return [step["step"] for step in deploy_group_configs]


def get_service_instance_list_no_cache(
    service: str,
    cluster: Optional[str] = None,
    instance_type: str = None,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> List[Tuple[str, str]]:
    """Enumerate the instances defined for a service as a list of tuples.

    :param service: The service name
    :param cluster: The cluster to read the configuration for
    :param instance_type: The type of instances to examine: 'kubernetes', 'tron', or None (default) for both
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (name, instance) for each instance defined for the service name
    """

    instance_types: Tuple[str, ...]
    if not cluster:
        cluster = load_system_paasta_config().get_cluster()
    if instance_type in INSTANCE_TYPES:
        instance_types = (instance_type,)
    else:
        instance_types = INSTANCE_TYPES

    instance_list: List[Tuple[str, str]] = []
    for srv_instance_type in instance_types:
        instance_list.extend(
            read_service_instance_names(
                service=service,
                instance_type=srv_instance_type,
                cluster=cluster,
                soa_dir=soa_dir,
            )
        )
    log.debug("Enumerated the following instances: %s", instance_list)
    return instance_list


@time_cache(ttl=5)
def get_service_instance_list(
    service: str,
    cluster: Optional[str] = None,
    instance_type: str = None,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> List[Tuple[str, str]]:
    """Enumerate the instances defined for a service as a list of tuples.

    :param service: The service name
    :param cluster: The cluster to read the configuration for
    :param instance_type: The type of instances to examine: 'kubernetes', 'tron', or None (default) for both
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (name, instance) for each instance defined for the service name
    """
    return get_service_instance_list_no_cache(
        service=service, cluster=cluster, instance_type=instance_type, soa_dir=soa_dir
    )


def get_services_for_cluster(
    cluster: str = None, instance_type: str = None, soa_dir: str = DEFAULT_SOA_DIR
) -> List[Tuple[str, str]]:
    """Retrieve all services and instances defined to run in a cluster.

    :param cluster: The cluster to read the configuration for
    :param instance_type: The type of instances to examine: 'kubernetes', 'tron', or None (default) for both
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (service, instance)
    """

    if not cluster:
        cluster = load_system_paasta_config().get_cluster()
    rootdir = os.path.abspath(soa_dir)
    log.debug(
        "Retrieving all service instance names from %s for cluster %s", rootdir, cluster
    )
    instance_list: List[Tuple[str, str]] = []
    for srv_dir in os.listdir(rootdir):
        instance_list.extend(
            get_service_instance_list(srv_dir, cluster, instance_type, soa_dir)
        )
    return instance_list


def load_service_instance_configs(
    service: str,
    instance_type: str,
    cluster: str,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> Dict[str, InstanceConfigDict]:
    conf_file = f"{instance_type}-{cluster}"
    user_configs = service_configuration_lib.read_extra_service_information(
        service,
        conf_file,
        soa_dir=soa_dir,
        deepcopy=False,
    )
    user_configs = filter_templates_from_config(user_configs)
    auto_configs = load_service_instance_auto_configs(
        service, instance_type, cluster, soa_dir
    )
    merged = {}
    for instance_name, user_config in user_configs.items():
        auto_config = auto_configs.get(instance_name, {})
        merged[instance_name] = deep_merge_dictionaries(
            overrides=user_config,
            defaults=auto_config,
        )
    return merged


def load_service_instance_config(
    service: str,
    instance: str,
    instance_type: str,
    cluster: str,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> InstanceConfigDict:
    if instance.startswith("_"):
        raise InvalidJobNameError(
            f"Unable to load {instance_type} config for {service}.{instance} as instance name starts with '_'"
        )
    conf_file = f"{instance_type}-{cluster}"

    # We pass deepcopy=False here and then do our own deepcopy of the subset of the data we actually care about. Without
    # this optimization, any code that calls load_service_instance_config for every instance in a yaml file is ~O(n^2).
    user_config = copy.deepcopy(
        service_configuration_lib.read_extra_service_information(
            service, conf_file, soa_dir=soa_dir, deepcopy=False
        ).get(instance)
    )
    if user_config is None:
        raise NoConfigurationForServiceError(
            f"{instance} not found in config file {soa_dir}/{service}/{conf_file}.yaml."
        )

    auto_config = load_service_instance_auto_configs(
        service, instance_type, cluster, soa_dir
    ).get(instance, {})
    return deep_merge_dictionaries(
        overrides=user_config,
        defaults=auto_config,
    )


def load_service_instance_auto_configs(
    service: str,
    instance_type: str,
    cluster: str,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> Dict[str, Dict[str, Any]]:
    enabled_types = load_system_paasta_config().get_auto_config_instance_types_enabled()
    # this looks a little funky: but what we're generally trying to do here is ensure that
    # certain types of instances can be moved between instance types without having to worry
    # about any sort of data races (or data weirdness) in autotune.
    # instead, what we do is map certain instance types to whatever we've picked as the "canonical"
    # instance type in autotune and always merge from there.
    realized_type = (
        load_system_paasta_config()
        .get_auto_config_instance_type_aliases()
        .get(instance_type, instance_type)
    )
    conf_file = f"{realized_type}-{cluster}"
    if enabled_types.get(realized_type):
        return service_configuration_lib.read_extra_service_information(
            service,
            f"{AUTO_SOACONFIG_SUBDIR}/{conf_file}",
            soa_dir=soa_dir,
            deepcopy=False,
        )
    else:
        return {}


def get_docker_host() -> str:
    return os.environ.get("DOCKER_HOST", "unix://var/run/docker.sock")


def get_docker_client() -> APIClient:
    client_opts = kwargs_from_env(assert_hostname=False)
    if "base_url" in client_opts:
        return APIClient(**client_opts)
    else:
        return APIClient(base_url=get_docker_host(), **client_opts)


def get_running_mesos_docker_containers() -> List[Dict]:
    client = get_docker_client()
    running_containers = client.containers()
    return [
        container
        for container in running_containers
        if "mesos-" in container["Names"][0]
    ]


class TimeoutError(Exception):
    pass


class Timeout:
    # From http://stackoverflow.com/questions/2281850/timeout-function-if-it-takes-too-long-to-finish

    def __init__(self, seconds: int = 1, error_message: str = "Timeout") -> None:
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum: int, frame: FrameType) -> None:
        raise TimeoutError(self.error_message)

    def __enter__(self) -> None:
        self.old_handler = signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, self.old_handler)


def print_with_indent(line: str, indent: int = 2) -> None:
    """Print a line with a given indent level"""
    print(" " * indent + line)


class NoDeploymentsAvailable(Exception):
    pass


class DeploymentVersion(NamedTuple):
    sha: str
    image_version: Optional[str]

    def __repr__(self) -> str:
        # Represented as commit if no image_version, standard tuple repr otherwise
        return (
            f"DeploymentVersion(sha={self.sha}, image_version={self.image_version})"
            if self.image_version
            else self.sha
        )

    def short_sha_repr(self, sha_len: int = 8) -> str:
        # Same as __repr__ but allows us to print the shortned commit sha.
        short_sha = self.sha[:sha_len]
        return (
            f"DeploymentVersion(sha={short_sha}, image_version={self.image_version})"
            if self.image_version
            else short_sha
        )

    def json(self) -> str:
        return json.dumps(self._asdict())


DeploymentsJsonV1Dict = Dict[str, BranchDictV1]

DeployGroup = str
BranchName = str


class _DeploymentsJsonV2ControlsDict(TypedDict, total=False):
    force_bounce: Optional[str]
    desired_state: str


class _DeploymentsJsonV2DeploymentsDict(TypedDict):
    docker_image: str
    git_sha: str
    image_version: Optional[str]


class DeploymentsJsonV2Dict(TypedDict):
    deployments: Dict[DeployGroup, _DeploymentsJsonV2DeploymentsDict]
    controls: Dict[BranchName, _DeploymentsJsonV2ControlsDict]


class DeploymentsJsonDict(TypedDict):
    v1: DeploymentsJsonV1Dict
    v2: DeploymentsJsonV2Dict


class DeploymentsJsonV1:
    def __init__(self, config_dict: DeploymentsJsonV1Dict) -> None:
        self.config_dict = config_dict

    def get_branch_dict(self, service: str, branch: str) -> BranchDictV1:
        full_branch = f"{service}:paasta-{branch}"
        return self.config_dict.get(full_branch, {})

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, DeploymentsJsonV1)
            and other.config_dict == self.config_dict
        )


class DeploymentsJsonV2:
    def __init__(self, service: str, config_dict: DeploymentsJsonV2Dict) -> None:
        self.config_dict = config_dict
        self.service = service

    def get_branch_dict(
        self, service: str, branch: str, deploy_group: str
    ) -> BranchDictV2:
        full_branch = f"{service}:{branch}"
        branch_dict: BranchDictV2 = {
            "docker_image": self.get_docker_image_for_deploy_group(deploy_group),
            "git_sha": self.get_git_sha_for_deploy_group(deploy_group),
            "image_version": self.get_image_version_for_deploy_group(deploy_group),
            "desired_state": self.get_desired_state_for_branch(full_branch),
            "force_bounce": self.get_force_bounce_for_branch(full_branch),
        }
        return branch_dict

    def get_deploy_groups(self) -> Collection[str]:
        return self.config_dict["deployments"].keys()

    def get_docker_image_for_deploy_group(self, deploy_group: str) -> str:
        try:
            deploy_group_config = self.config_dict["deployments"][deploy_group]
        except KeyError:
            e = f"{self.service} not deployed to {deploy_group}. Has mark-for-deployment been run?"
            raise NoDeploymentsAvailable(e)
        try:
            return deploy_group_config["docker_image"]
        except KeyError:
            e = f"The configuration for service {self.service} in deploy group {deploy_group} does not contain 'docker_image' metadata."
            raise KeyError(e)

    def get_git_sha_for_deploy_group(self, deploy_group: str) -> str:
        try:
            deploy_group_config = self.config_dict["deployments"][deploy_group]
        except KeyError:
            e = f"{self.service} not deployed to {deploy_group}. Has mark-for-deployment been run?"
            raise NoDeploymentsAvailable(e)
        try:
            return deploy_group_config["git_sha"]
        except KeyError:
            e = f"The configuration for service {self.service} in deploy group {deploy_group} does not contain 'git_sha' metadata."
            raise KeyError(e)

    def get_image_version_for_deploy_group(self, deploy_group: str) -> Optional[str]:
        try:
            deploy_group_config = self.config_dict["deployments"][deploy_group]
        except KeyError:
            e = f"{self.service} not deployed to {deploy_group}. Has mark-for-deployment been run?"
            raise NoDeploymentsAvailable(e)
        try:
            # TODO: Once these changes have propagated image_version should
            # always be present in the deployments.json file, so remove the
            # .get() call.
            return deploy_group_config.get("image_version", None)
        except KeyError:
            e = f"The configuration for service {self.service} in deploy group {deploy_group} does not contain 'image_version' metadata."
            raise KeyError(e)

    def get_deployment_version_for_deploy_group(
        self, deploy_group: str
    ) -> DeploymentVersion:
        return DeploymentVersion(
            sha=self.get_git_sha_for_deploy_group(deploy_group),
            image_version=self.get_image_version_for_deploy_group(deploy_group),
        )

    def get_desired_state_for_branch(self, control_branch: str) -> str:
        try:
            return self.config_dict["controls"][control_branch].get(
                "desired_state", "start"
            )
        except KeyError:
            e = f"{self.service} not configured for {control_branch}. Has mark-for-deployment been run?"
            raise NoDeploymentsAvailable(e)

    def get_force_bounce_for_branch(self, control_branch: str) -> str:
        try:
            return self.config_dict["controls"][control_branch].get(
                "force_bounce", None
            )
        except KeyError:
            e = f"{self.service} not configured for {control_branch}. Has mark-for-deployment been run?"
            raise NoDeploymentsAvailable(e)


def load_deployments_json(service: str, soa_dir: str = DEFAULT_SOA_DIR) -> Any:
    deployment_file = os.path.join(soa_dir, service, "deployments.json")
    if os.path.isfile(deployment_file):
        with open(deployment_file) as f:
            config_dict = json.load(f)
            return (
                DeploymentsJsonV1(config_dict["v1"])
                if "v1" in config_dict
                else DeploymentsJsonV2(service=service, config_dict=config_dict["v2"])
            )
    else:
        e = f"{deployment_file} was not found. 'generate_deployments_for_service --service {service}' must be run first"
        raise NoDeploymentsAvailable(e)


def load_v2_deployments_json(
    service: str, soa_dir: str = DEFAULT_SOA_DIR
) -> DeploymentsJsonV2:
    deployment_file = os.path.join(soa_dir, service, "deployments.json")
    if os.path.isfile(deployment_file):
        with open(deployment_file) as f:
            return DeploymentsJsonV2(service=service, config_dict=json.load(f)["v2"])
    else:
        e = f"{deployment_file} was not found. 'generate_deployments_for_service --service {service}' must be run first"
        raise NoDeploymentsAvailable(e)


def get_paasta_branch(cluster: str, instance: str) -> str:
    return SPACER.join((cluster, instance))


def parse_timestamp(tstamp: str) -> datetime.datetime:
    return datetime.datetime.strptime(tstamp, "%Y%m%dT%H%M%S")


def format_timestamp(dt: datetime.datetime = None) -> str:
    if dt is None:
        dt = datetime.datetime.utcnow()
    return dt.strftime("%Y%m%dT%H%M%S")


def get_paasta_tag_from_deploy_group(
    identifier: str, desired_state: str, image_version: Optional[str] = None
) -> str:
    timestamp = format_timestamp(datetime.datetime.utcnow())
    if image_version:
        return f"paasta-{identifier}+{image_version}-{timestamp}-{desired_state}"
    else:
        return f"paasta-{identifier}-{timestamp}-{desired_state}"


def get_paasta_tag(cluster: str, instance: str, desired_state: str) -> str:
    timestamp = format_timestamp(datetime.datetime.utcnow())
    return f"paasta-{cluster}.{instance}-{timestamp}-{desired_state}"


def format_tag(tag: str) -> str:
    return "refs/tags/%s" % tag


def get_latest_deployment_tag(
    refs: Dict[str, str], deploy_group: str
) -> Tuple[str, str, Optional[str]]:
    """Gets the latest deployment tag and sha for the specified deploy_group

    :param refs: A dictionary mapping git refs to shas
    :param deploy_group: The deployment group to return a deploy tag for

    :returns: A tuple of the form (ref, sha, image_version) where ref is the
              actual deployment tag (with the most recent timestamp), sha is
              the sha it points at and image_version provides additional
              version information about the image
    """
    most_recent_dtime = None
    most_recent_ref = None
    most_recent_sha = None
    most_recent_image_version = None
    pattern = re.compile(
        r"^refs/tags/paasta-%s(?:\+(?P<image_version>.*)){0,1}-(?P<dtime>\d{8}T\d{6})-deploy$"
        % deploy_group
    )

    for ref_name, sha in refs.items():
        match = pattern.match(ref_name)
        if match:
            gd = match.groupdict()
            dtime = gd["dtime"]
            if most_recent_dtime is None or dtime > most_recent_dtime:
                most_recent_dtime = dtime
                most_recent_ref = ref_name
                most_recent_sha = sha
                most_recent_image_version = gd["image_version"]
    return most_recent_ref, most_recent_sha, most_recent_image_version


def build_image_identifier(
    git_sha: str, sha_len: Optional[int] = None, image_version: Optional[str] = None
) -> str:
    image = git_sha
    if sha_len is not None:
        image = image[:sha_len]
    if image_version is not None:
        image += f"-{image_version}"

    return image


class NoDockerImageError(Exception):
    pass


def get_config_hash(config: Any, force_bounce: str = None) -> str:
    """Create an MD5 hash of the configuration dictionary to be sent to
    Kubernetes. Or anything really, so long as str(config) works. Returns
    the first 8 characters so things are not really long.

    :param config: The configuration to hash
    :param force_bounce: a timestamp (in the form of a string) that is appended before hashing
                         that can be used to force a hash change
    :returns: A MD5 hash of str(config)
    """
    hasher = hashlib.md5()
    hasher.update(
        json.dumps(config, sort_keys=True).encode("UTF-8")
        + (force_bounce or "").encode("UTF-8")
    )
    return "config%s" % hasher.hexdigest()[:8]


def get_git_sha_from_dockerurl(docker_url: str, long: bool = False) -> str:
    """We encode the sha of the code that built a docker image *in* the docker
    url. This function takes that url as input and outputs the sha.
    """
    if ":paasta-" in docker_url:
        deployment_version = get_deployment_version_from_dockerurl(docker_url)
        git_sha = deployment_version.sha if deployment_version else ""
    # Fall back to the old behavior if the docker_url does not follow the
    # expected pattern
    else:
        parts = docker_url.split("/")
        parts = parts[-1].split("-")
        git_sha = parts[-1]

    return git_sha if long else git_sha[:8]


def get_image_version_from_dockerurl(docker_url: str) -> Optional[str]:
    """We can optionally encode additional metadata about the docker image *in*
    the docker url. This function takes that url as input and outputs the sha.
    """
    deployment_version = get_deployment_version_from_dockerurl(docker_url)
    return deployment_version.image_version if deployment_version else None


def get_deployment_version_from_dockerurl(docker_url: str) -> DeploymentVersion:
    regex_match = re.match(
        r".*:paasta-(?P<git_sha>[A-Za-z0-9]+)(-(?P<image_version>.+))?", docker_url
    )

    return (
        DeploymentVersion(
            sha=regex_match.group("git_sha"),
            image_version=regex_match.group("image_version"),
        )
        if regex_match is not None
        else None
    )


def get_code_sha_from_dockerurl(docker_url: str) -> str:
    """code_sha is hash extracted from docker url prefixed with "git", short
    hash is used because it's embedded in mesos task names and there's length
    limit.
    """
    try:
        git_sha = get_git_sha_from_dockerurl(docker_url, long=False)
        return "git%s" % git_sha
    except Exception:
        return "gitUNKNOWN"


def is_under_replicated(
    num_available: int, expected_count: int, crit_threshold: int
) -> Tuple[bool, float]:
    """Calculates if something is under replicated

    :param num_available: How many things are up
    :param expected_count: How many things you think should be up
    :param crit_threshold: Int from 0-100
    :returns: Tuple of (bool, ratio)
    """
    if expected_count == 0:
        ratio = 100.0
    else:
        ratio = (num_available / float(expected_count)) * 100

    if ratio < int(crit_threshold):
        return (True, ratio)
    else:
        return (False, ratio)


def deploy_blacklist_to_constraints(
    deploy_blacklist: DeployBlacklist,
) -> List[Constraint]:
    """Converts a blacklist of locations into tron appropriate constraints.

    :param blacklist: List of lists of locations to blacklist
    :returns: List of lists of constraints
    """
    constraints: List[Constraint] = []
    for blacklisted_location in deploy_blacklist:
        constraints.append([blacklisted_location[0], "UNLIKE", blacklisted_location[1]])

    return constraints


def deploy_whitelist_to_constraints(
    deploy_whitelist: DeployWhitelist,
) -> List[Constraint]:
    """Converts a whitelist of locations into tron appropriate constraints

    :param deploy_whitelist: List of lists of locations to whitelist
    :returns: List of lists of constraints
    """
    if deploy_whitelist is not None:
        (region_type, regions) = deploy_whitelist
        regionstr = "|".join(regions)

        return [[region_type, "LIKE", regionstr]]
    return []


def terminal_len(text: str) -> int:
    """Return the number of characters that text will take up on a terminal."""
    return len(remove_ansi_escape_sequences(text))


def format_table(
    rows: Iterable[Union[str, Sequence[str]]], min_spacing: int = 2
) -> List[str]:
    """Formats a table for use on the command line.

    :param rows: List of rows, each of which can either be a tuple of strings containing the row's values, or a string
                 to be inserted verbatim. Each row (except literal strings) should be the same number of elements as
                 all the others.
    :returns: A string containing rows formatted as a table.
    """

    list_rows = [r for r in rows if not isinstance(r, str)]

    # If all of the rows are strings, we have nothing to do, so short-circuit.
    if not list_rows:
        return cast(List[str], rows)

    widths = []
    for i in range(len(list_rows[0])):
        widths.append(max(terminal_len(r[i]) for r in list_rows))

    expanded_rows = []
    for row in rows:
        if isinstance(row, str):
            expanded_rows.append([row])
        else:
            expanded_row = []
            for i, cell in enumerate(row):
                if i == len(row) - 1:
                    padding = ""
                else:
                    padding = " " * (widths[i] - terminal_len(cell))
                expanded_row.append(cell + padding)
            expanded_rows.append(expanded_row)

    return [(" " * min_spacing).join(r) for r in expanded_rows]


_DeepMergeT = TypeVar("_DeepMergeT", bound=Any)


class DuplicateKeyError(Exception):
    pass


def deep_merge_dictionaries(
    overrides: _DeepMergeT, defaults: _DeepMergeT, allow_duplicate_keys: bool = True
) -> _DeepMergeT:
    """
    Merges two dictionaries.
    """
    result = copy.deepcopy(defaults)
    stack: List[Tuple[Dict, Dict]] = [(overrides, result)]
    while stack:
        source_dict, result_dict = stack.pop()
        for key, value in source_dict.items():
            try:
                child = result_dict[key]
            except KeyError:
                result_dict[key] = value
            else:
                if isinstance(value, dict) and isinstance(child, dict):
                    stack.append((value, child))
                else:
                    if allow_duplicate_keys:
                        result_dict[key] = value
                    else:
                        raise DuplicateKeyError(
                            f"defaults and overrides both have key {key}"
                        )
    return result


class ZookeeperPool:
    """
    A context manager that shares the same KazooClient with its children. The first nested context manager
    creates and deletes the client and shares it with any of its children. This allows to place a context
    manager over a large number of zookeeper calls without opening and closing a connection each time.
    GIL makes this 'safe'.
    """

    counter: int = 0
    zk: KazooClient = None

    @classmethod
    def __enter__(cls) -> KazooClient:
        if cls.zk is None:
            cls.zk = KazooClient(
                hosts=load_system_paasta_config().get_zk_hosts(), read_only=True
            )
            cls.zk.start()
        cls.counter = cls.counter + 1
        return cls.zk

    @classmethod
    def __exit__(cls, *args: Any, **kwargs: Any) -> None:
        cls.counter = cls.counter - 1
        if cls.counter == 0:
            cls.zk.stop()
            cls.zk.close()
            cls.zk = None


def calculate_tail_lines(verbose_level: int) -> int:
    if verbose_level <= 1:
        return 0
    else:
        return 10 ** (verbose_level - 1)


def is_deploy_step(step: str) -> bool:
    """
    Returns true if the given step deploys to an instancename
    Returns false if the step is a predefined step-type, e.g. itest or command-*
    """
    return not (
        (step in DEPLOY_PIPELINE_NON_DEPLOY_STEPS) or (step.startswith("command-"))
    )


_UseRequestsCacheFuncT = TypeVar("_UseRequestsCacheFuncT", bound=Callable)


def use_requests_cache(
    cache_name: str, backend: str = "memory", **kwargs: Any
) -> Callable[[_UseRequestsCacheFuncT], _UseRequestsCacheFuncT]:
    def wrap(fun: _UseRequestsCacheFuncT) -> _UseRequestsCacheFuncT:
        def fun_with_cache(*args: Any, **kwargs: Any) -> Any:
            requests_cache.install_cache(cache_name, backend=backend, **kwargs)
            result = fun(*args, **kwargs)
            requests_cache.uninstall_cache()
            return result

        return cast(_UseRequestsCacheFuncT, fun_with_cache)

    return wrap


def long_job_id_to_short_job_id(long_job_id: str) -> str:
    service, instance, _, __ = decompose_job_id(long_job_id)
    return compose_job_id(service, instance)


def mean(iterable: Collection[float]) -> float:
    """
    Returns the average value of an iterable
    """
    return sum(iterable) / len(iterable)


def prompt_pick_one(sequence: Collection[str], choosing: str) -> str:
    if not sys.stdin.isatty():
        print(
            "No {choosing} specified and no TTY present to ask."
            "Please specify a {choosing} using the cli.".format(choosing=choosing),
            file=sys.stderr,
        )
        sys.exit(1)

    if not sequence:
        print(
            f"PaaSTA needs to pick a {choosing} but none were found.", file=sys.stderr
        )
        sys.exit(1)

    global_actions = [str("quit")]
    choices = [(item, item) for item in sequence]

    if len(choices) == 1:
        return choices[0][0]

    chooser = choice.Menu(choices=choices, global_actions=global_actions)
    chooser.title = (
        'Please pick a {choosing} from the choices below (or "quit" to quit):'.format(
            choosing=str(choosing)
        )
    )
    try:
        result = chooser.ask()
    except (KeyboardInterrupt, EOFError):
        print("")
        sys.exit(1)

    if isinstance(result, tuple) and result[1] == str("quit"):
        sys.exit(1)
    else:
        return result


def to_bytes(obj: Any) -> bytes:
    if isinstance(obj, bytes):
        return obj
    elif isinstance(obj, str):
        return obj.encode("UTF-8")
    else:
        return str(obj).encode("UTF-8")


_TimeoutFuncRetType = TypeVar("_TimeoutFuncRetType")


def timeout(
    seconds: int = 10,
    error_message: str = os.strerror(errno.ETIME),
    use_signals: bool = True,
) -> Callable[[Callable[..., _TimeoutFuncRetType]], Callable[..., _TimeoutFuncRetType]]:
    if use_signals:

        def decorate(
            func: Callable[..., _TimeoutFuncRetType]
        ) -> Callable[..., _TimeoutFuncRetType]:
            def _handle_timeout(signum: int, frame: FrameType) -> None:
                raise TimeoutError(error_message)

            def wrapper(*args: Any, **kwargs: Any) -> _TimeoutFuncRetType:
                signal.signal(signal.SIGALRM, _handle_timeout)
                signal.alarm(seconds)
                try:
                    result = func(*args, **kwargs)
                finally:
                    signal.alarm(0)
                return result

            return wraps(func)(wrapper)

    else:

        def decorate(
            func: Callable[..., _TimeoutFuncRetType]
        ) -> Callable[..., _TimeoutFuncRetType]:
            # https://github.com/python/mypy/issues/797
            return _Timeout(func, seconds, error_message)  # type: ignore

    return decorate


class _Timeout:
    def __init__(
        self,
        function: Callable[..., _TimeoutFuncRetType],
        seconds: float,
        error_message: str,
    ) -> None:
        self.seconds = seconds
        self.control: queue.Queue[
            Tuple[bool, Union[_TimeoutFuncRetType, Tuple]]
        ] = queue.Queue()
        self.function = function
        self.error_message = error_message

    def run(self, *args: Any, **kwargs: Any) -> None:
        # Try and put the result of the function into the q
        # if an exception occurs then we put the exc_info instead
        # so that it can be raised in the main thread.
        try:
            self.control.put((True, self.function(*args, **kwargs)))
        except Exception:
            self.control.put((False, sys.exc_info()))

    def __call__(self, *args: Any, **kwargs: Any) -> _TimeoutFuncRetType:
        self.func_thread = threading.Thread(target=self.run, args=args, kwargs=kwargs)
        self.func_thread.daemon = True
        self.timeout = self.seconds + time.time()
        self.func_thread.start()
        return self.get_and_raise()

    def get_and_raise(self) -> _TimeoutFuncRetType:
        while not self.timeout < time.time():
            time.sleep(0.01)
            if not self.func_thread.is_alive():
                ret = self.control.get()
                if ret[0]:
                    return cast(_TimeoutFuncRetType, ret[1])
                else:
                    _, e, tb = cast(Tuple, ret[1])
                    raise e.with_traceback(tb)
        raise TimeoutError(self.error_message)


def suggest_possibilities(
    word: str, possibilities: Iterable[str], max_suggestions: int = 3
) -> str:
    suggestions = cast(
        List[str],
        difflib.get_close_matches(
            word=word, possibilities=set(possibilities), n=max_suggestions
        ),
    )
    if len(suggestions) == 1:
        return f"\nDid you mean: {suggestions[0]}?"
    elif len(suggestions) >= 1:
        return f"\nDid you mean one of: {', '.join(suggestions)}?"
    else:
        return ""


def list_services(soa_dir: str = DEFAULT_SOA_DIR) -> Sequence[str]:
    """Returns a sorted list of all services"""
    return sorted(os.listdir(os.path.abspath(soa_dir)))


def get_possible_launched_by_user_variable_from_env() -> str:
    return os.getenv("SUDO_USER") or getpass.getuser()


def load_all_configs(
    cluster: str, file_prefix: str, soa_dir: str
) -> Mapping[str, Mapping[str, Any]]:
    config_dicts = {}
    for service in os.listdir(soa_dir):
        config_dicts[service] = load_service_instance_configs(
            service, file_prefix, cluster, soa_dir
        )
    return config_dicts


def ldap_user_search(
    cn: str,
    search_base: str,
    search_ou: str,
    ldap_host: str,
    username: str,
    password: str,
) -> Set[str]:
    """Connects to LDAP and raises a subclass of LDAPOperationResult when it fails"""
    tls_config = ldap3.Tls(
        validate=ssl.CERT_REQUIRED, ca_certs_file="/etc/ssl/certs/ca-certificates.crt"
    )
    server = ldap3.Server(ldap_host, use_ssl=True, tls=tls_config)
    conn = ldap3.Connection(
        server, user=username, password=password, raise_exceptions=True
    )
    conn.bind()

    search_filter = f"(&(memberOf=CN={cn},{search_ou})(!(userAccountControl=514)))"
    entries = conn.extend.standard.paged_search(
        search_base=search_base,
        search_scope=ldap3.SUBTREE,
        search_filter=search_filter,
        attributes=["sAMAccountName"],
        paged_size=1000,
        time_limit=10,
    )
    return {entry["attributes"]["sAMAccountName"] for entry in entries}


def _reorder_docker_volumes(volumes: List[DockerVolume]) -> List[DockerVolume]:
    deduped = {
        v["containerPath"].rstrip("/") + v["hostPath"].rstrip("/"): v for v in volumes
    }.values()
    return sort_dicts(deduped)


def get_k8s_url_for_cluster(cluster: str) -> Optional[str]:
    """
    Annoyingly, there's two layers of aliases: one to figure out what
    k8s server url to use (this one) and another to figure out what
    soaconfigs filename to use ;_;

    This exists so that we can map something like `--cluster pnw-devc`
    into spark-pnw-devc's k8s apiserver url without needing to update
    any soaconfigs/alter folk's muscle memory.

    Ideally we can get rid of this entirely once spark-run reads soaconfigs
    in a manner more closely aligned to what we do with other paasta workloads
    (i.e., have it automatically determine where to run based on soaconfigs
    filenames - and not rely on explicit config)
    """
    realized_cluster = (
        load_system_paasta_config().get_eks_cluster_aliases().get(cluster, cluster)
    )
    return (
        load_system_paasta_config()
        .get_kube_clusters()
        .get(realized_cluster, {})
        .get("server")
    )


@lru_cache(maxsize=1)
def is_using_unprivileged_containers() -> bool:
    return "podman" in os.getenv("DOCKER_HOST", "")


def maybe_load_previous_config(
    filename: str, config_loader: Callable[[TextIO], dict]
) -> Optional[dict]:
    """Try to load configuration file

    :param str filename: path to load from
    :param Callable[[TextIO], dict] config_loader: parser for the configuration
    :return: configuration data, None if loading fails
    """
    try:
        with open(filename, "r") as fp:
            previous_config = config_loader(fp)
            return previous_config
    except Exception:
        pass
    return None


def write_json_configuration_file(filename: str, configuration: dict) -> None:
    """Atomically write configuration to JSON file

    :param str filename: path to write to
    :param dict configuration: configuration data
    """
    with atomic_file_write(filename) as fp:
        json.dump(
            obj=configuration,
            fp=fp,
            indent=2,
            sort_keys=True,
            separators=(",", ": "),
        )


def write_yaml_configuration_file(
    filename: str, configuration: dict, check_existing: bool = True
) -> None:
    """Atomically write configuration to YAML file

    :param str filename: path to write to
    :param dict configuration: configuration data
    :param bool check_existing: if existing file already matches config, do not overwrite
    """
    if check_existing:
        previous_config = maybe_load_previous_config(filename, yaml.safe_load)
        if previous_config and previous_config == configuration:
            return

    with atomic_file_write(filename) as fp:
        fp.write(
            "# This file is automatically generated by paasta_tools.\n"
            "# It was automatically generated at {now} on {host}.\n".format(
                host=socket.getfqdn(), now=datetime.datetime.now().isoformat()
            )
        )
        yaml.safe_dump(
            configuration,
            fp,
            indent=2,
            explicit_start=True,
            default_flow_style=False,
            allow_unicode=False,
        )
