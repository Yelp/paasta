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
import copy
import math
import os
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import TypeVar
from typing import Union

import service_configuration_lib
from typing_extensions import TypedDict

from paasta_tools.monitoring_tools import MonitoringDict
from paasta_tools.monitoring_tools import read_merged_monitoring_config
from paasta_tools.utils import _reorder_docker_volumes
from paasta_tools.utils import AUTO_SOACONFIG_SUBDIR
from paasta_tools.utils import AwsEbsVolume
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import DeployBlacklist
from paasta_tools.utils import DeployWhitelist
from paasta_tools.utils import DockerVolume
from paasta_tools.utils import filter_templates_from_config
from paasta_tools.utils import get_git_sha_from_dockerurl
from paasta_tools.utils import get_paasta_branch
from paasta_tools.utils import get_pipeline_deploy_groups
from paasta_tools.utils import get_service_docker_registry
from paasta_tools.utils import get_service_instance_list
from paasta_tools.utils import INSTANCE_TYPE_TO_K8S_NAMESPACE
from paasta_tools.utils import INSTANCE_TYPES
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import is_using_unprivileged_containers
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import log
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import PersistentVolume
from paasta_tools.utils import ProjectedSAVolume
from paasta_tools.utils import safe_deploy_blacklist
from paasta_tools.utils import safe_deploy_whitelist
from paasta_tools.utils import SecretVolume
from paasta_tools.utils import suggest_possibilities
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import time_cache
from paasta_tools.utils import UnsafeDeployBlacklist
from paasta_tools.utils import UnsafeDeployWhitelist


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

DEFAULT_CPU_PERIOD = 100000
DEFAULT_CPU_BURST_ADD = 1


class InvalidInstanceConfig(Exception):
    pass


Constraint = Sequence[str]

# e.g. ['GROUP_BY', 'habitat', 2]. Tron doesn't like that so we'll convert to Constraint later.
UnstringifiedConstraint = Sequence[Union[str, int, float]]

SecurityConfigDict = Dict  # Todo: define me.


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
    privileged: bool
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
    uses_bulkdata: bool
    docker_url: str


class DockerParameter(TypedDict):
    key: str
    value: str


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
        self, system_paasta_config: Optional[SystemPaastaConfig] = None
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
        return self.get_monitoring().get("team", None)

    def get_runbook(self) -> str:
        return self.get_monitoring().get("runbook", None)

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
        system_paasta_config: Optional[SystemPaastaConfig] = None,
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
        self, system_paasta_config: Optional[SystemPaastaConfig] = None
    ) -> bool:
        if system_paasta_config is None:
            system_paasta_config = load_system_paasta_config()
        return system_paasta_config.get_enforce_disk_quota()

    def get_docker_init(self) -> Iterable[DockerParameter]:
        return [{"key": "init", "value": "true"}]

    def get_disk(self, default: float = 1024) -> float:
        """Gets the amount of disk space in MiB required from the service's configuration.

        Defaults to 1024 (1GiB) if no value is specified in the config.

        :returns: The amount of disk space specified by the config, 1024 MiB if not specified
        """
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
        self, system_paasta_config: Optional[SystemPaastaConfig] = None
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
        # Our workloads interact with AWS quite a lot, so it comes handy to
        # propagate an "application ID" in the user-agent of API requests
        # for debugging purposes (max length is 50 chars from AWS docs).
        env["AWS_SDK_UA_APP_ID"] = f"{self.service}.{self.instance}"[:50]
        user_env = self.config_dict.get("env", {})
        env.update(user_env)
        return {str(k): str(v) for (k, v) in env.items()}

    def get_env(
        self, system_paasta_config: Optional[SystemPaastaConfig] = None
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
        instance_monitoring_config = self.config_dict.get("monitoring", {})
        return read_merged_monitoring_config(
            self.service, self.soa_dir, instance_overrides=instance_monitoring_config
        )

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
        self, system_paasta_config: Optional[SystemPaastaConfig] = None
    ) -> str:
        """Compose the docker url.
        :returns: '<registry_uri>/<docker_image>'
        """
        # NOTE: we're explicitly only allowing this for adhoc instances to support remote-run toolboxes.
        # If you're looking at this to expand that support for non-remote-run cases, please chat with #paasta first.
        if "docker_url" in self.config_dict:
            return self.config_dict["docker_url"]
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

        :returns: the "pool" attribute in your config dict, or the string "default" if not specified.
        """
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

    def get_volumes(
        self,
        system_volumes: Sequence[DockerVolume],
        uses_bulkdata_default: bool = False,
    ) -> List[DockerVolume]:
        volumes = list(system_volumes) + list(self.get_extra_volumes())
        # we used to add bulkdata as a default mount - but as part of the
        # effort to deprecate the entire system, we're swapping to an opt-in
        # model so that we can shrink the blast radius of any changes
        if self.config_dict.get(
            "uses_bulkdata",
            uses_bulkdata_default,
        ):
            # bulkdata is mounted RO as the data is produced by another
            # system and we want to ensure that there are no inadvertent
            # changes by misbehaved code
            volumes.append(
                {
                    "hostPath": "/nail/bulkdata",
                    "containerPath": "/nail/bulkdata",
                    "mode": "RO",
                }
            )
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

        :returns: A list of dictionaries specified in the dependencies_dict, None if not specified
        """
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


def load_all_configs(
    cluster: str, file_prefix: str, soa_dir: str
) -> Mapping[str, Mapping[str, Any]]:
    config_dicts = {}
    for service in os.listdir(soa_dir):
        config_dicts[service] = load_service_instance_configs(
            service, file_prefix, cluster, soa_dir
        )
    return config_dicts
