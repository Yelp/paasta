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
import errno
import fcntl
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
import sys
import tempfile
import threading
import time
from collections import OrderedDict
from fnmatch import fnmatch
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
from typing import IO
from typing import Iterable
from typing import Iterator
from typing import List  # noqa
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union  # noqa

import choice
import dateutil.tz
import requests_cache
import service_configuration_lib
import yaml
from docker import Client
from docker.utils import kwargs_from_env
from kazoo.client import KazooClient
from mypy_extensions import TypedDict

import paasta_tools.cli.fsm


# DO NOT CHANGE SPACER, UNLESS YOU'RE PREPARED TO CHANGE ALL INSTANCES
# OF IT IN OTHER LIBRARIES (i.e. service_configuration_lib).
# It's used to compose a job's full ID from its name and instance
SPACER = '.'
INFRA_ZK_PATH = '/nail/etc/zookeeper_discovery/infrastructure/'
PATH_TO_SYSTEM_PAASTA_CONFIG_DIR = os.environ.get('PAASTA_SYSTEM_CONFIG_DIR', '/etc/paasta/')
DEFAULT_SOA_DIR = service_configuration_lib.DEFAULT_SOA_DIR
DEFAULT_DOCKERCFG_LOCATION = "file:///root/.dockercfg"
DEPLOY_PIPELINE_NON_DEPLOY_STEPS = (
    'itest',
    'security-check',
    'performance-check',
    'push-to-registry',
)
# Default values for _log
ANY_CLUSTER = 'N/A'
ANY_INSTANCE = 'N/A'
DEFAULT_LOGLEVEL = 'event'
no_escape = re.compile('\x1B\[[0-9;]*[mK]')

DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT = "http://{host:s}:{port:d}/;csv;norefresh"

DEFAULT_CPU_PERIOD = 100000
DEFAULT_CPU_BURST_PCT = 900

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

INSTANCE_TYPES = ('marathon', 'chronos', 'paasta_native', 'adhoc')


TimeCacheEntry = TypedDict(
    'TimeCacheEntry',
    {
        'data': Any,
        'fetch_time': float,
    },
)

_CacheRetT = TypeVar('_CacheRetT')


class time_cache(object):
    def __init__(self, ttl: float=0) -> None:
        self.configs: Dict[Tuple, TimeCacheEntry] = {}
        self.ttl = ttl

    def __call__(self, f: Callable[..., _CacheRetT]) -> Callable[..., _CacheRetT]:
        def cache(*args: Any, **kwargs: Any) -> _CacheRetT:
            if 'ttl' in kwargs:
                ttl = kwargs['ttl']
                del kwargs['ttl']
            else:
                ttl = self.ttl
            key = args
            for item in kwargs.items():
                key += item
            if (not ttl) or (key not in self.configs) or (time.time() - self.configs[key]['fetch_time'] > ttl):
                self.configs[key] = {'data': f(*args, **kwargs), 'fetch_time': time.time()}
            return self.configs[key]['data']
        return cache


_SortDictsT = TypeVar('_SortDictsT', bound=Mapping)


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

# e.g. ['GROUP_BY', 'habitat', 2]. Marathon doesn't like that so we'll convert to Constraint later.
UnstringifiedConstraint = Sequence[Union[str, int, float]]

SecurityConfigDict = Dict  # Todo: define me.

DockerVolume = TypedDict(
    'DockerVolume',
    {
        'hostPath': str,
        'containerPath': str,
        'mode': str,
    },
)

InstanceConfigDict = TypedDict(
    'InstanceConfigDict',
    {
        'deploy_group': str,
        'mem': float,
        'cpus': float,
        'disk': float,
        'cmd': str,
        'args': List[str],
        'cfs_period_us': float,
        'cpu_burst_pct': float,
        'ulimit': Dict[str, Dict[str, Any]],
        'cap_add': List,
        'env': Dict[str, str],
        'monitoring': Dict[str, str],
        'deploy_blacklist': UnsafeDeployBlacklist,
        'deploy_whitelist': UnsafeDeployWhitelist,
        'monitoring_blacklist': UnsafeDeployBlacklist,
        'pool': str,
        'extra_volumes': List[DockerVolume],
        'security': SecurityConfigDict,
        'dependencies_reference': str,
        'dependencies': Dict[str, Dict],
        'constraints': List[UnstringifiedConstraint],
        'extra_constraints': List[UnstringifiedConstraint],
        'net': str,
        'extra_docker_args': Dict[str, str],
        'gpus': float,
    },
    total=False,
)


BranchDict = TypedDict(
    'BranchDict',
    {
        'docker_image': str,
        'git_sha': str,
        'desired_state': str,
        'force_bounce': Optional[str],
    },
    total=False,
)


DockerParameter = TypedDict(
    'DockerParameter',
    {
        'key': str,
        'value': str,
    },
)


def safe_deploy_blacklist(input: UnsafeDeployBlacklist) -> DeployBlacklist:
    return [(t, l) for t, l in input]


def safe_deploy_whitelist(input: UnsafeDeployWhitelist) -> DeployWhitelist:
    try:
        location_type, allowed_values = input
        return cast(str, location_type), cast(List[str], allowed_values)
    except TypeError:
        return None


class InstanceConfig(object):

    def __init__(
        self, cluster: str, instance: str, service: str, config_dict: InstanceConfigDict,
        branch_dict: BranchDict, soa_dir: str=DEFAULT_SOA_DIR,
    ) -> None:
        self.config_dict = config_dict
        self.branch_dict = branch_dict
        self.cluster = cluster
        self.instance = instance
        self.service = service
        self.soa_dir = soa_dir
        self._job_id = compose_job_id(service, instance)
        config_interpolation_keys = ('deploy_group',)
        interpolation_facts = self.__get_interpolation_facts()
        for key in config_interpolation_keys:
            if key in self.config_dict:
                self.config_dict[key] = self.config_dict[key].format(**interpolation_facts)  # type: ignore

    def __get_interpolation_facts(self) -> Dict[str, str]:
        return {
            'cluster': self.cluster,
            'instance': self.instance,
            'service': self.service,
        }

    def get_cluster(self) -> str:
        return self.cluster

    def get_instance(self) -> str:
        return self.instance

    def get_service(self) -> str:
        return self.service

    @property
    def job_id(self) -> str:
        return self._job_id

    def get_docker_registry(self) -> str:
        return get_service_docker_registry(self.service, self.soa_dir)

    def get_branch(self) -> str:
        return get_paasta_branch(cluster=self.get_cluster(), instance=self.get_instance())

    def get_deploy_group(self) -> str:
        return self.config_dict.get('deploy_group', self.get_branch())

    def get_team(self) -> str:
        return self.config_dict.get('monitoring', {}).get('team', None)

    def get_mem(self) -> float:
        """Gets the memory required from the service's configuration.

        Defaults to 1024 (1G) if no value specified in the config.

        :returns: The amount of memory specified by the config, 1024 if not specified"""
        mem = self.config_dict.get('mem', 1024)
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

        Defaults to .25 (1/4 of a cpu) if no value specified in the config.

        :returns: The number of cpus specified in the config, .25 if not specified"""
        cpus = self.config_dict.get('cpus', .25)
        return cpus

    def get_cpu_period(self) -> float:
        """The --cpu-period option to be passed to docker
        Comes from the cfs_period_us configuration option

        :returns: The number to be passed to the --cpu-period docker flag"""
        return self.config_dict.get('cfs_period_us', DEFAULT_CPU_PERIOD)

    def get_cpu_quota(self) -> float:
        """Gets the --cpu-quota option to be passed to docker
        Calculated from the cpu_burst_pct configuration option, which is the percent
        over its declared cpu usage that a container will be allowed to go.

        Calculation: cpus * cfs_period_us * (100 + cpu_burst_pct) / 100

        :returns: The number to be passed to the --cpu-quota docker flag"""
        cpu_burst_pct = self.config_dict.get('cpu_burst_pct', DEFAULT_CPU_BURST_PCT)
        return self.get_cpus() * self.get_cpu_period() * (100 + cpu_burst_pct) / 100

    def get_extra_docker_args(self) -> Dict[str, str]:
        return self.config_dict.get('extra_docker_args', {})

    def get_ulimit(self) -> Iterable[DockerParameter]:
        """Get the --ulimit options to be passed to docker
        Generated from the ulimit configuration option, which is a dictionary
        of ulimit values. Each value is a dictionary itself, with the soft
        limit stored under the 'soft' key and the optional hard limit stored
        under the 'hard' key.

        Example configuration: {'nofile': {soft: 1024, hard: 2048}, 'nice': {soft: 20}}

        :returns: A generator of ulimit options to be passed as --ulimit flags"""
        for key, val in sorted(self.config_dict.get('ulimit', {}).items()):
            soft = val.get('soft')
            hard = val.get('hard')
            if soft is None:
                raise InvalidInstanceConfig(
                    'soft limit missing in ulimit configuration for {}.'.format(key),
                )
            combined_val = '%i' % soft
            if hard is not None:
                combined_val += ':%i' % hard
            yield {"key": "ulimit", "value": "{}={}".format(key, combined_val)}

    def get_cap_add(self) -> Iterable[DockerParameter]:
        """Get the --cap-add options to be passed to docker
        Generated from the cap_add configuration option, which is a list of
        capabilities.

        Example configuration: {'cap_add': ['IPC_LOCK', 'SYS_PTRACE']}

        :returns: A generator of cap_add options to be passed as --cap-add flags"""
        for value in self.config_dict.get('cap_add', []):
            yield {"key": "cap-add", "value": "{}".format(value)}

    def format_docker_parameters(self, with_labels: bool=True) -> List[DockerParameter]:
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
        if with_labels:
            parameters.extend([
                {"key": "label", "value": "paasta_service=%s" % self.service},
                {"key": "label", "value": "paasta_instance=%s" % self.instance},
            ])
        extra_docker_args = self.get_extra_docker_args()
        if extra_docker_args:
            for key, value in extra_docker_args.items():
                parameters.extend([
                    {"key": key, "value": value},
                ])
        parameters.extend(self.get_ulimit())
        parameters.extend(self.get_cap_add())
        return parameters

    def get_disk(self, default: float=1024) -> float:
        """Gets the  amount of disk space required from the service's configuration.

        Defaults to 1024 (1G) if no value is specified in the config.

        :returns: The amount of disk space specified by the config, 1024 if not specified"""
        disk = self.config_dict.get('disk', default)
        return disk

    def get_gpus(self, default: float=0) -> float:
        """Gets the number of gpus required from the service's configuration.

        Default to 0 if no value is specified in the config.

        :returns: The number of gpus specified by the config, 0 if not specified"""
        gpus = self.config_dict.get('gpus', default)
        return gpus

    def get_cmd(self) -> Optional[str]:
        """Get the docker cmd specified in the service's configuration.

        Defaults to None if not specified in the config.

        :returns: A string specified in the config, None if not specified"""
        return self.config_dict.get('cmd', None)

    def get_env_dictionary(self) -> Dict[str, str]:
        """A dictionary of key/value pairs that represent environment variables
        to be injected to the container environment"""
        env = {
            "PAASTA_SERVICE": self.service,
            "PAASTA_INSTANCE": self.instance,
            "PAASTA_CLUSTER": self.cluster,
            "PAASTA_DEPLOY_GROUP": self.get_deploy_group(),
            "PAASTA_DOCKER_IMAGE": self.get_docker_image(),
        }
        user_env = self.config_dict.get('env', {})
        env.update(user_env)
        return env

    def get_env(self) -> Dict[str, str]:
        """Basic get_env that simply returns the basic env, other classes
        might need to override this getter for more implementation-specific
        env getting"""
        return self.get_env_dictionary()

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
            return self.config_dict.get('args', [])
        else:
            args = self.config_dict.get('args', None)
            if args is None:
                return args
            else:
                # TODO validation stuff like this should be moved into a check_* like in chronos tools
                raise InvalidInstanceConfig('Instance configuration can specify cmd or args, but not both.')

    def get_monitoring(self) -> Dict[str, Any]:
        """Get monitoring overrides defined for the given instance"""
        return self.config_dict.get('monitoring', {})

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
            deploy_blacklist_to_constraints(blacklist) +
            deploy_whitelist_to_constraints(whitelist) +
            deploy_blacklist_to_constraints(system_deploy_blacklist) +
            deploy_whitelist_to_constraints(system_deploy_whitelist)
        )

    def get_deploy_blacklist(self) -> DeployBlacklist:
        """The deploy blacklist is a list of lists, where the lists indicate
        which locations the service should not be deployed"""
        return safe_deploy_blacklist(self.config_dict.get('deploy_blacklist', []))

    def get_deploy_whitelist(self) -> DeployWhitelist:
        """The deploy whitelist is a tuple of (location_type, [allowed value, allowed value, ...]).
        To have tasks scheduled on it, a host must be covered by the deploy whitelist (if present) and not excluded by
        the deploy blacklist."""

        return safe_deploy_whitelist(self.config_dict.get('deploy_whitelist'))

    def get_monitoring_blacklist(self, system_deploy_blacklist: DeployBlacklist) -> DeployBlacklist:
        """The monitoring_blacklist is a list of tuples of (location type, location value), where the tuples indicate
        which locations the user doesn't care to be monitored"""
        return (
            safe_deploy_blacklist(self.config_dict.get('monitoring_blacklist', [])) +
            self.get_deploy_blacklist() +
            system_deploy_blacklist
        )

    def get_docker_image(self) -> str:
        """Get the docker image name (with tag) for a given service branch from
        a generated deployments.json file."""
        return self.branch_dict.get('docker_image', '')

    def get_docker_url(self) -> str:
        """Compose the docker url.
        :returns: '<registry_uri>/<docker_image>'
        """
        registry_uri = self.get_docker_registry()
        docker_image = self.get_docker_image()
        if not docker_image:
            raise NoDockerImageError('Docker url not available because there is no docker_image')
        docker_url = '%s/%s' % (registry_uri, docker_image)
        return docker_url

    def get_desired_state(self) -> str:
        """Get the desired state (either 'start' or 'stop') for a given service
        branch from a generated deployments.json file."""
        return self.branch_dict.get('desired_state', 'start')

    def get_force_bounce(self) -> Optional[str]:
        """Get the force_bounce token for a given service branch from a generated
        deployments.json file. This is a token that, when changed, indicates that
        the instance should be recreated and bounced, even if no other
        parameters have changed. This may be None or a string, generally a
        timestamp.
        """
        return self.branch_dict.get('force_bounce', None)

    def check_cpus(self) -> Tuple[bool, str]:
        cpus = self.get_cpus()
        if cpus is not None:
            if not isinstance(cpus, (float, int)):
                return False, 'The specified cpus value "%s" is not a valid float or int.' % cpus
        return True, ''

    def check_mem(self) -> Tuple[bool, str]:
        mem = self.get_mem()
        if mem is not None:
            if not isinstance(mem, (float, int)):
                return False, 'The specified mem value "%s" is not a valid float or int.' % mem
        return True, ''

    def check_disk(self) -> Tuple[bool, str]:
        disk = self.get_disk()
        if disk is not None:
            if not isinstance(disk, (float, int)):
                return False, 'The specified disk value "%s" is not a valid float or int.' % disk
        return True, ''

    def check_gpus(self) -> Tuple[bool, str]:
        gpus = self.get_gpus()
        if gpus is not None and not isinstance(gpus, (float, int)):
            return False, 'The specified gpus value "%s" is not a valid float or int.' % gpus
        return True, ''

    def check_security(self) -> Tuple[bool, str]:
        security = self.config_dict.get('security')
        if security is None:
            return True, ''

        outbound_firewall = security.get('outbound_firewall')
        if outbound_firewall is None:
            return True, ''

        if outbound_firewall not in ('block', 'monitor'):
            return False, 'Unrecognized outbound_firewall value "%s"' % outbound_firewall

        unknown_keys = set(security.keys()) - {'outbound_firewall'}
        if unknown_keys:
            return False, 'Unrecognized items in security dict of service config: "%s"' % ','.join(unknown_keys)

        return True, ''

    def check_dependencies_reference(self) -> Tuple[bool, str]:
        dependencies_reference = self.config_dict.get('dependencies_reference')
        if dependencies_reference is None:
            return True, ''

        dependencies = self.config_dict.get('dependencies')
        if dependencies is None:
            return False, 'dependencies_reference "%s" declared but no dependencies found' % dependencies_reference

        if dependencies_reference not in dependencies:
            return False, 'dependencies_reference "%s" not found in dependencies dictionary' % dependencies_reference

        return True, ''

    def check(self, param: str) -> Tuple[bool, str]:
        check_methods = {
            'cpus': self.check_cpus,
            'mem': self.check_mem,
            'security': self.check_security,
            'dependencies_reference': self.check_dependencies_reference,
        }
        check_method = check_methods.get(param)
        if check_method is not None:
            return check_method()
        else:
            return False, 'Your service config specifies "%s", an unsupported parameter.' % param

    def validate(self) -> List[str]:
        error_msgs = []
        for param in ['cpus', 'mem', 'security', 'dependencies_reference']:
            check_passed, check_msg = self.check(param)
            if not check_passed:
                error_msgs.append(check_msg)
        return error_msgs

    def get_extra_volumes(self) -> List[DockerVolume]:
        """Extra volumes are a specially formatted list of dictionaries that should
        be bind mounted in a container The format of the dictionaries should
        conform to the `Mesos container volumes spec
        <https://mesosphere.github.io/marathon/docs/native-docker.html>`_"""
        return self.config_dict.get('extra_volumes', [])

    def get_pool(self) -> str:
        """Which pool of nodes this job should run on. This can be used to mitigate noisy neighbors, by putting
        particularly noisy or noise-sensitive jobs into different pools.

        This is implemented with an attribute "pool" on each mesos slave and by adding a constraint to Marathon/Chronos
        application defined by this instance config.

        Eventually this may be implemented with Mesos roles, once a framework can register under multiple roles.

        :returns: the "pool" attribute in your config dict, or the string "default" if not specified."""
        return self.config_dict.get('pool', 'default')

    def get_pool_constraints(self) -> List[Constraint]:
        pool = self.get_pool()
        return [["pool", "LIKE", pool]]

    def get_constraints(self) -> Optional[List[Constraint]]:
        return stringify_constraints(self.config_dict.get('constraints', None))

    def get_extra_constraints(self) -> List[Constraint]:
        return stringify_constraints(self.config_dict.get('extra_constraints', []))

    def get_net(self) -> str:
        """
        :returns: the docker networking mode the container should be started with.
        """
        return self.config_dict.get('net', 'bridge')

    def get_volumes(self, system_volumes: Sequence[DockerVolume]) -> List[DockerVolume]:
        volumes = list(system_volumes) + list(self.get_extra_volumes())
        deduped = {v['containerPath'].rstrip('/') + v['hostPath'].rstrip('/'): v for v in volumes}.values()
        return sort_dicts(deduped)

    def get_dependencies_reference(self) -> Optional[str]:
        """Get the reference to an entry in dependencies.yaml

        Defaults to None if not specified in the config.

        :returns: A string specified in the config, None if not specified"""
        return self.config_dict.get('dependencies_reference')

    def get_dependencies(self) -> Optional[Dict]:
        """Get the contents of the dependencies_dict pointed to by the dependency_reference

        Defaults to None if not specified in the config.

        :returns: A list of dictionaries specified in the dependencies_dict, None if not specified"""
        dependencies = self.config_dict.get('dependencies')
        if not dependencies:
            return None
        return dependencies.get(self.get_dependencies_reference())

    def get_outbound_firewall(self) -> Optional[str]:
        """Return 'block', 'monitor', or None as configured in security->outbound_firewall

        Defaults to None if not specified in the config

        :returns: A string specified in the config, None if not specified"""
        security = self.config_dict.get('security')
        if not security:
            return None
        return security.get('outbound_firewall')

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, type(self)):
            return self.config_dict == other.config_dict and \
                self.branch_dict == other.branch_dict and \
                self.cluster == other.cluster and \
                self.instance == other.instance and \
                self.service == other.service
        else:
            return False


def stringify_constraint(usc: UnstringifiedConstraint) -> Constraint:
    return [str(x) for x in usc]


def stringify_constraints(uscs: Optional[List[UnstringifiedConstraint]]) -> List[Constraint]:
    if uscs is None:
        return None
    return [stringify_constraint(usc) for usc in uscs]


def validate_service_instance(service: str, instance: str, cluster: str, soa_dir: str) -> str:
    for instance_type in INSTANCE_TYPES:
        services = get_services_for_cluster(cluster=cluster, instance_type=instance_type, soa_dir=soa_dir)
        if (service, instance) in services:
            return instance_type
    else:
        raise NoConfigurationForServiceError(
            "Error: %s doesn't look like it has been configured to run on the %s cluster." % (
                compose_job_id(service, instance), cluster,
            ),
        )


_ComposeRetT = TypeVar('_ComposeRetT')
_ComposeInnerRetT = TypeVar('_ComposeInnerRetT')


def compose(
    func_one: Callable[[_ComposeInnerRetT], _ComposeRetT],
    func_two: Callable[..., _ComposeInnerRetT],
) -> Callable[..., _ComposeRetT]:
    def composed(*args: Any, **kwargs: Any) -> _ComposeRetT:
        return func_one(func_two(*args, **kwargs))
    return composed


class PaastaColors:

    """Collection of static variables and methods to assist in coloring text."""
    # ANSI colour codes
    BLUE = '\033[34m'
    BOLD = '\033[1m'
    CYAN = '\033[36m'
    DEFAULT = '\033[0m'
    GREEN = '\033[32m'
    GREY = '\033[38;5;242m'
    MAGENTA = '\033[35m'
    RED = '\033[31m'
    YELLOW = '\033[33m'

    @staticmethod
    def bold(text: str) -> str:
        """Return bolded text.

        :param text: a string
        :return: text colour coded with ANSI bold
        """
        return PaastaColors.color_text(PaastaColors.BOLD, text)

    @staticmethod
    def blue(text: str) -> str:
        """Return text that can be printed blue.

        :param text: a string
        :return: text colour coded with ANSI blue
        """
        return PaastaColors.color_text(PaastaColors.BLUE, text)

    @staticmethod
    def green(text: str) -> str:
        """Return text that can be printed green.

        :param text: a string
        :return: text colour coded with ANSI green"""
        return PaastaColors.color_text(PaastaColors.GREEN, text)

    @staticmethod
    def red(text: str) -> str:
        """Return text that can be printed red.

        :param text: a string
        :return: text colour coded with ANSI red"""
        return PaastaColors.color_text(PaastaColors.RED, text)

    @staticmethod
    def magenta(text: str) -> str:
        """Return text that can be printed magenta.

        :param text: a string
        :return: text colour coded with ANSI magenta"""
        return PaastaColors.color_text(PaastaColors.MAGENTA, text)

    @staticmethod
    def color_text(color: str, text: str) -> str:
        """Return text that can be printed color.

        :param color: ANSI colour code
        :param text: a string
        :return: a string with ANSI colour encoding"""
        # any time text returns to default, we want to insert our color.
        replaced = text.replace(PaastaColors.DEFAULT, PaastaColors.DEFAULT + color)
        # then wrap the beginning and end in our color/default.
        return color + replaced + PaastaColors.DEFAULT

    @staticmethod
    def cyan(text: str) -> str:
        """Return text that can be printed cyan.

        :param text: a string
        :return: text colour coded with ANSI cyan"""
        return PaastaColors.color_text(PaastaColors.CYAN, text)

    @staticmethod
    def yellow(text: str) -> str:
        """Return text that can be printed yellow.

        :param text: a string
        :return: text colour coded with ANSI yellow"""
        return PaastaColors.color_text(PaastaColors.YELLOW, text)

    @staticmethod
    def grey(text: str) -> str:
        return PaastaColors.color_text(PaastaColors.GREY, text)

    @staticmethod
    def default(text: str) -> str:
        return PaastaColors.color_text(PaastaColors.DEFAULT, text)


LOG_COMPONENTS = OrderedDict([
    (
        'build', {
            'color': PaastaColors.blue,
            'help': 'Jenkins build jobs output, like the itest, promotion, security checks, etc.',
            'source_env': 'devc',
        },
    ),
    (
        'deploy', {
            'color': PaastaColors.cyan,
            'help': 'Output from the paasta deploy code. (setup_marathon_job, bounces, etc)',
            'additional_source_envs': ['devc'],
        },
    ),
    (
        'monitoring', {
            'color': PaastaColors.green,
            'help': 'Logs from Sensu checks for the service',
        },
    ),
    (
        'marathon', {
            'color': PaastaColors.magenta,
            'help': 'Logs from Marathon for the service',
        },
    ),
    (
        'chronos', {
            'color': PaastaColors.red,
            'help': 'Logs from Chronos for the service',
        },
    ),
    (
        'app_output', {
            'color': compose(PaastaColors.yellow, PaastaColors.bold),
            'help': 'Stderr and stdout of the actual process spawned by Mesos. '
                    'Convenience alias for both the stdout and stderr components',
        },
    ),
    (
        'stdout', {
            'color': PaastaColors.yellow,
            'help': 'Stdout from the process spawned by Mesos.',
        },
    ),
    (
        'stderr', {
            'color': PaastaColors.yellow,
            'help': 'Stderr from the process spawned by Mesos.',
        },
    ),
    (
        'security', {
            'color': PaastaColors.red,
            'help': 'Logs from security-related services such as firewall monitoring',
        },
    ),
    (
        'oom', {
            'color': PaastaColors.red,
            'help': 'Kernel OOM events.',
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
])


class NoSuchLogComponent(Exception):
    pass


def validate_log_component(component: str) -> bool:
    if component in LOG_COMPONENTS.keys():
        return True
    else:
        raise NoSuchLogComponent


def get_git_url(service: str, soa_dir: str=DEFAULT_SOA_DIR) -> str:
    """Get the git url for a service. Assumes that the service's
    repo matches its name, and that it lives in services- i.e.
    if this is called with the string 'test', the returned
    url will be git@git.yelpcorp.com:services/test.git.

    :param service: The service name to get a URL for
    :returns: A git url to the service's repository"""
    general_config = service_configuration_lib.read_service_configuration(
        service,
        soa_dir=soa_dir,
    )
    default_location = 'git@git.yelpcorp.com:services/%s.git' % service
    return general_config.get('git_url', default_location)


def get_service_docker_registry(
    service: str,
    soa_dir: str=DEFAULT_SOA_DIR,
    system_config: Optional['SystemPaastaConfig']=None,
) -> str:
    service_configuration = service_configuration_lib.read_service_configuration(service, soa_dir)
    try:
        return service_configuration['docker_registry']
    except KeyError:
        if not system_config:
            system_config = load_system_paasta_config()
        return system_config.get_system_docker_registry()


class NoSuchLogLevel(Exception):
    pass


LogWriterConfig = TypedDict(
    'LogWriterConfig',
    {
        'driver': str,
        'options': Dict,
    },
)

LogReaderConfig = TypedDict(
    'LogReaderConfig',
    {
        'driver': str,
        'options': Dict,
    },
)


# The active log writer.
_log_writer = None
# The map of name -> LogWriter subclasses, used by configure_log.
_log_writer_classes = {}


class LogWriter(object):
    def __init__(self, **kwargs: Any) -> None:
        pass

    def log(
        self,
        service: str,
        line: str,
        component: str,
        level: str=DEFAULT_LOGLEVEL,
        cluster: str=ANY_CLUSTER,
        instance: str=ANY_INSTANCE,
    ) -> None:
        raise NotImplementedError()


_LogWriterTypeT = TypeVar('_LogWriterTypeT', bound=Type[LogWriter])


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
    LogWriterClass = get_log_writer_class(log_writer_config['driver'])
    _log_writer = LogWriterClass(**log_writer_config.get('options', {}))


def _log(
    service: str,
    line: str,
    component: str,
    level: str=DEFAULT_LOGLEVEL,
    cluster: str=ANY_CLUSTER,
    instance: str=ANY_INSTANCE,
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


def _now() -> str:
    return datetime.datetime.utcnow().isoformat()


def remove_ansi_escape_sequences(line: str) -> str:
    """Removes ansi escape sequences from the given line."""
    return no_escape.sub('', line)


def format_log_line(
    level: str,
    cluster: str,
    service: str,
    instance: str,
    component: str,
    line: str,
    timestamp: str=None,
) -> str:
    """Accepts a string 'line'.

    Returns an appropriately-formatted dictionary which can be serialized to
    JSON for logging and which contains 'line'.
    """
    validate_log_component(component)
    if not timestamp:
        timestamp = _now()
    line = remove_ansi_escape_sequences(line)
    message = json.dumps(
        {
            'timestamp': timestamp,
            'level': level,
            'cluster': cluster,
            'service': service,
            'instance': instance,
            'component': component,
            'message': line,
        }, sort_keys=True,
    )
    return message


def get_log_name_for_service(service: str, prefix: str=None) -> str:
    if prefix:
        return 'stream_paasta_%s_%s' % (prefix, service)
    return 'stream_paasta_%s' % service


@register_log_writer('scribe')
class ScribeLogWriter(LogWriter):
    def __init__(
        self,
        scribe_host: str='169.254.255.254',
        scribe_port: int=1463,
        scribe_disable: bool=False,
        **kwargs: Any,
    ) -> None:
        self.clog = __import__('clog')
        self.clog.config.configure(scribe_host=scribe_host, scribe_port=scribe_port, scribe_disable=scribe_disable)

    def log(
        self,
        service: str,
        line: str,
        component: str,
        level: str=DEFAULT_LOGLEVEL,
        cluster: str=ANY_CLUSTER,
        instance: str=ANY_INSTANCE,
    ) -> None:
        """This expects someone (currently the paasta cli main()) to have already
        configured the log object. We'll just write things to it.
        """
        if level == 'event':
            paasta_print("[service %s] %s" % (service, line), file=sys.stdout)
        elif level == 'debug':
            paasta_print("[service %s] %s" % (service, line), file=sys.stderr)
        else:
            raise NoSuchLogLevel
        log_name = get_log_name_for_service(service)
        formatted_line = format_log_line(level, cluster, service, instance, component, line)
        self.clog.log_line(log_name, formatted_line)


@register_log_writer('null')
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
        level: str=DEFAULT_LOGLEVEL,
        cluster: str=ANY_CLUSTER,
        instance: str=ANY_INSTANCE,
    ) -> None:
        pass


@contextlib.contextmanager
def _empty_context() -> Iterator[None]:
    yield


_AnyIO = Union[io.IOBase, IO]


@register_log_writer('file')
class FileLogWriter(LogWriter):
    def __init__(
        self,
        path_format: str,
        mode: str='a+',
        line_delimeter: str='\n',
        flock: bool=False,
    ) -> None:
        self.path_format = path_format
        self.mode = mode
        self.flock = flock
        self.line_delimeter = line_delimeter

    def maybe_flock(self, fd: _AnyIO) -> ContextManager:
        if self.flock:
            # https://github.com/python/typeshed/issues/1548
            return flock(fd)
        else:
            return _empty_context()

    def format_path(self, service: str, component: str, level: str, cluster: str, instance: str) -> str:
        return self.path_format.format(
            service=service,
            component=component,
            level=level,
            cluster=cluster,
            instance=instance,
        )

    def log(
        self,
        service: str,
        line: str,
        component: str,
        level: str=DEFAULT_LOGLEVEL,
        cluster: str=ANY_CLUSTER,
        instance: str=ANY_INSTANCE,
    ) -> None:
        path = self.format_path(service, component, level, cluster, instance)

        # We use io.FileIO here because it guarantees that write() is implemented with a single write syscall,
        # and on Linux, writes to O_APPEND files with a single write syscall are atomic.
        #
        # https://docs.python.org/2/library/io.html#io.FileIO
        # http://article.gmane.org/gmane.linux.kernel/43445

        to_write = "%s%s" % (format_log_line(level, cluster, service, instance, component, line), self.line_delimeter)

        try:
            with io.FileIO(path, mode=self.mode, closefd=True) as f:
                with self.maybe_flock(f):
                    # remove type ignore comment below once https://github.com/python/typeshed/pull/1541 is merged.
                    f.write(to_write.encode('UTF-8'))  # type: ignore
        except IOError as e:
            paasta_print(
                "Could not log to %s: %s: %s -- would have logged: %s" % (path, type(e).__name__, str(e), to_write),
                file=sys.stderr,
            )


@contextlib.contextmanager
def flock(fd: _AnyIO) -> Iterator[None]:
    try:
        fcntl.flock(fd.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd.fileno(), fcntl.LOCK_UN)


@contextlib.contextmanager
def timed_flock(fd: _AnyIO, seconds: int=1) -> Iterator[None]:
    """ Attempt to grab an exclusive flock with a timeout. Uses Timeout, so will
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


ClusterAutoscalingResources = Dict[str, Dict]

ResourcePoolSettings = Dict[str, Dict]

ChronosConfig = TypedDict(
    'ChronosConfig',
    {
        'user': str,
        'password': str,
        'url': List[str],
    },
    total=False,
)
MarathonConfigDict = TypedDict(
    'MarathonConfigDict',
    {
        'user': str,
        'password': str,
        'url': List[str],
    },
    total=False,
)

LocalRunConfig = TypedDict(
    'LocalRunConfig',
    {
        'default_cluster': str,
    },
    total=False,
)
PaastaNativeConfig = TypedDict(
    'PaastaNativeConfig',
    {
        'principal': str,
        'secret': str,
    },
    total=False,
)

ExpectedSlaveAttributes = List[Dict[str, Any]]

SystemPaastaConfigDict = TypedDict(
    'SystemPaastaConfigDict',
    {
        'zookeeper': str,
        'docker_registry': str,
        'volumes': List[DockerVolume],
        'cluster': str,
        'dashboard_links': Dict[str, Dict[str, str]],
        'api_endpoints': Dict[str, str],
        'fsm_template': str,
        'log_reader': LogReaderConfig,
        'log_writer': LogWriterConfig,
        'deployd_metrics_provider': str,
        'metrics_provider': str,
        'deployd_worker_failure_backoff_factor': int,
        'deployd_maintenance_polling_frequency': int,
        'sensu_host': str,
        'sensu_port': int,
        'dockercfg_location': str,
        'synapse_port': int,
        'synapse_host': str,
        'synapse_haproxy_url_format': str,
        'cluster_autoscaling_resources': ClusterAutoscalingResources,
        'resource_pool_settings': ResourcePoolSettings,
        'cluster_fqdn_format': str,
        'chronos_config': ChronosConfig,
        'marathon_servers': List[MarathonConfigDict],
        'previous_marathon_servers': List[MarathonConfigDict],
        'local_run_config': LocalRunConfig,
        'paasta_native': PaastaNativeConfig,
        'mesos_config': Dict,
        'monitoring_config': Dict,
        'deploy_blacklist': UnsafeDeployBlacklist,
        'deploy_whitelist': UnsafeDeployWhitelist,
        'expected_slave_attributes': ExpectedSlaveAttributes,
        'security_check_command': str,
        'deployd_number_workers': int,
        'deployd_big_bounce_rate': float,
        'deployd_startup_bounce_rate': float,
        'deployd_log_level': str,
        'deployd_startup_oracle_enabled': bool,
        'cluster_autoscaling_draining_enabled': bool,
        'use_mesos_healthchecks': bool,
        'taskproc': Dict,
        'disabled_watchers': List,
        'vault_environment': str,
    },
    total=False,
)


def load_system_paasta_config(path: str=PATH_TO_SYSTEM_PAASTA_CONFIG_DIR) -> 'SystemPaastaConfig':
    """
    Reads Paasta configs in specified directory in lexicographical order and deep merges
    the dictionaries (last file wins).
    """
    config: SystemPaastaConfigDict = {}
    if not os.path.isdir(path):
        raise PaastaNotConfiguredError("Could not find system paasta configuration directory: %s" % path)

    if not os.access(path, os.R_OK):
        raise PaastaNotConfiguredError("Could not read from system paasta configuration directory: %s" % path)

    try:
        for config_file in get_readable_files_in_glob(glob="*.json", path=path):
            with open(config_file) as f:
                config = deep_merge_dictionaries(json.load(f), config)
    except IOError as e:
        raise PaastaNotConfiguredError("Could not load system paasta config file %s: %s" % (e.filename, e.strerror))
    return SystemPaastaConfig(config, path)


class SystemPaastaConfig(object):

    def __init__(self, config: SystemPaastaConfigDict, directory: str) -> None:
        self.directory = directory
        self.config_dict = config

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, SystemPaastaConfig):
            return self.directory == other.directory and self.config_dict == other.config_dict
        return False

    def __repr__(self) -> str:
        return "SystemPaastaConfig(%r, %r)" % (self.config_dict, self.directory)

    def get_zk_hosts(self) -> str:
        """Get the zk_hosts defined in this hosts's cluster config file.
        Strips off the zk:// prefix, if it exists, for use with Kazoo.

        :returns: The zk_hosts specified in the paasta configuration
        """
        try:
            hosts = self.config_dict['zookeeper']
        except KeyError:
            raise PaastaNotConfiguredError(
                'Could not find zookeeper connection string in configuration directory: %s'
                % self.directory,
            )

        # how do python strings not have a method for doing this
        if hosts.startswith('zk://'):
            return hosts[len('zk://'):]
        return hosts

    def get_system_docker_registry(self) -> str:
        """Get the docker_registry defined in this host's cluster config file.

        :returns: The docker_registry specified in the paasta configuration
        """
        try:
            return self.config_dict['docker_registry']
        except KeyError:
            raise PaastaNotConfiguredError(
                'Could not find docker registry in configuration directory: %s'
                % self.directory,
            )

    def get_volumes(self) -> List[DockerVolume]:
        """Get the volumes defined in this host's volumes config file.

        :returns: The list of volumes specified in the paasta configuration
        """
        try:
            return self.config_dict['volumes']
        except KeyError:
            raise PaastaNotConfiguredError('Could not find volumes in configuration directory: %s' % self.directory)

    def get_cluster(self) -> str:
        """Get the cluster defined in this host's cluster config file.

        :returns: The name of the cluster defined in the paasta configuration
        """
        try:
            return self.config_dict['cluster']
        except KeyError:
            raise PaastaNotConfiguredError('Could not find cluster in configuration directory: %s' % self.directory)

    def get_dashboard_links(self) -> Dict[str, Dict[str, str]]:
        return self.config_dict['dashboard_links']

    def get_api_endpoints(self) -> Dict[str, str]:
        return self.config_dict['api_endpoints']

    def get_fsm_template(self) -> str:
        fsm_path = os.path.dirname(paasta_tools.cli.fsm.__file__)
        template_path = os.path.join(fsm_path, "template")
        return self.config_dict.get('fsm_template', template_path)

    def get_log_writer(self) -> LogWriterConfig:
        """Get the log_writer configuration out of global paasta config

        :returns: The log_writer dictionary.
        """
        try:
            return self.config_dict['log_writer']
        except KeyError:
            raise PaastaNotConfiguredError('Could not find log_writer in configuration directory: %s' % self.directory)

    def get_log_reader(self) -> LogReaderConfig:
        """Get the log_reader configuration out of global paasta config

        :returns: the log_reader dictionary.
        """
        try:
            return self.config_dict['log_reader']
        except KeyError:
            raise PaastaNotConfiguredError('Could not find log_reader in configuration directory: %s' % self.directory)

    def get_metrics_provider(self) -> Optional[str]:
        """Get the metrics_provider configuration out of global paasta config

        :returns: A string identifying the metrics_provider
        """
        deployd_metrics_provider = self.config_dict.get('deployd_metrics_provider')
        if deployd_metrics_provider is not None:
            return deployd_metrics_provider
        return self.config_dict.get('metrics_provider')

    def get_deployd_worker_failure_backoff_factor(self) -> int:
        """Get the factor for calculating exponential backoff when a deployd worker
        fails to bounce a service

        :returns: An integer
        """
        return self.config_dict.get('deployd_worker_failure_backoff_factor', 30)

    def get_deployd_maintenance_polling_frequency(self) -> int:
        """Get the frequency in seconds that the deployd maintenance watcher should
        poll mesos's api for new draining hosts

        :returns: An integer
        """
        return self.config_dict.get('deployd_maintenance_polling_frequency', 30)

    def get_deployd_startup_oracle_enabled(self) -> bool:
        """This controls whether deployd will add all services that need a bounce on
        startup. Generally this is desirable behaviour. If you are performing a bounce
        of *all* services you will want to disable this.

        :returns: A boolean
        """
        return self.config_dict.get('deployd_startup_oracle_enabled', True)

    def get_sensu_host(self) -> str:
        """Get the host that we should send sensu events to.

        :returns: the sensu_host string, or localhost if not specified.
        """
        return self.config_dict.get('sensu_host', 'localhost')

    def get_sensu_port(self) -> int:
        """Get the port that we should send sensu events to.

        :returns: the sensu_port value as an integer, or 3030 if not specified.
        """
        return int(self.config_dict.get('sensu_port', 3030))

    def get_dockercfg_location(self) -> str:
        """Get the location of the dockerfile, as a URI.

        :returns: the URI specified, or file:///root/.dockercfg if not specified.
        """
        return self.config_dict.get('dockercfg_location', DEFAULT_DOCKERCFG_LOCATION)

    def get_synapse_port(self) -> int:
        """Get the port that haproxy-synapse exposes its status on. Defaults to 3212.

        :returns: the haproxy-synapse status port."""
        return int(self.config_dict.get('synapse_port', 3212))

    def get_default_synapse_host(self) -> str:
        """Get the default host we should interrogate for haproxy-synapse state.

        :returns: A hostname that is running haproxy-synapse."""
        return self.config_dict.get('synapse_host', 'localhost')

    def get_synapse_haproxy_url_format(self) -> str:
        """Get a format string for the URL to query for haproxy-synapse state. This format string gets two keyword
        arguments, host and port. Defaults to "http://{host:s}:{port:d}/;csv;norefresh".

        :returns: A format string for constructing the URL of haproxy-synapse's status page."""
        return self.config_dict.get('synapse_haproxy_url_format', DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT)

    def get_cluster_autoscaling_resources(self) -> ClusterAutoscalingResources:
        return self.config_dict.get('cluster_autoscaling_resources', {})

    def get_cluster_autoscaling_draining_enabled(self) -> bool:
        """ Enable mesos maintenance mode and trigger draining of instances before the
        autoscaler terminates the instance.

        :returns A bool"""
        return self.config_dict.get('cluster_autoscaling_draining_enabled', True)

    def get_resource_pool_settings(self) -> ResourcePoolSettings:
        return self.config_dict.get('resource_pool_settings', {})

    def get_cluster_fqdn_format(self) -> str:
        """Get a format string that constructs a DNS name pointing at the paasta masters in a cluster. This format
        string gets one parameter: cluster. Defaults to 'paasta-{cluster:s}.yelp'.

        :returns: A format string for constructing the FQDN of the masters in a given cluster."""
        return self.config_dict.get('cluster_fqdn_format', 'paasta-{cluster:s}.yelp')

    def get_chronos_config(self) -> ChronosConfig:
        """Get the chronos config

        :returns: The chronos config dictionary"""
        return self.config_dict.get('chronos_config', {})

    def get_marathon_servers(self) -> List[MarathonConfigDict]:
        return self.config_dict.get('marathon_servers', [])

    def get_previous_marathon_servers(self) -> List[MarathonConfigDict]:
        return self.config_dict.get('previous_marathon_servers', [])

    def get_local_run_config(self) -> LocalRunConfig:
        """Get the local-run config

        :returns: The local-run job config dictionary"""
        return self.config_dict.get('local_run_config', {})

    def get_paasta_native_config(self) -> PaastaNativeConfig:
        return self.config_dict.get('paasta_native', {})

    def get_mesos_cli_config(self) -> Dict:
        """Get the config for mesos-cli

        :returns: The mesos cli config
        """
        return self.config_dict.get("mesos_config", {})

    def get_monitoring_config(self) -> Dict:
        """Get the monitoring config

        :returns: the monitoring config dictionary"""
        return self.config_dict.get('monitoring_config', {})

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

        return safe_deploy_whitelist(self.config_dict.get('deploy_whitelist'))

    def get_expected_slave_attributes(self) -> ExpectedSlaveAttributes:
        """Return a list of dictionaries, representing the expected combinations of attributes in this cluster. Used for
        calculating the default routing constraints."""
        return self.config_dict.get('expected_slave_attributes')

    def get_security_check_command(self) -> Optional[str]:
        """Get the script to be executed during the security-check build step

        :return: The name of the file
        """
        return self.config_dict.get("security_check_command", None)

    def get_deployd_number_workers(self) -> int:
        """Get the number of workers to consume deployment q

        :return: integer
        """
        return self.config_dict.get("deployd_number_workers", 4)

    def get_deployd_big_bounce_rate(self) -> float:
        """Get the number of deploys to do per minute when deployd starts
        or determines it needs to bounce all services

        :return: float
        """

        return float(self.config_dict.get("deployd_big_bounce_rate", .1))

    def get_deployd_startup_bounce_rate(self) -> float:
        """Get the number of deploys to do per minute when deployd starts

        :return: float
        """

        return float(self.config_dict.get("deployd_startup_bounce_rate", .1))

    def get_deployd_log_level(self) -> str:
        """Get the log level for paasta-deployd

        :return: string name of python logging level, e.g. INFO, DEBUG etc.
        """
        return self.config_dict.get("deployd_log_level", 'INFO')

    def get_use_mesos_healthchecks(self) -> bool:
        """Get a boolean indicating whether HTTP(S) healthchecks should
        be driven by Mesos, rather than Marathon

        :return: a bool, indicating whether paasta should use MESOS healthchecks.
        """
        return self.config_dict.get("use_mesos_healthchecks", False)

    def get_taskproc(self) -> Dict:
        return self.config_dict.get('taskproc', {})

    def get_disabled_watchers(self) -> List:
        return self.config_dict.get('disabled_watchers', [])

    def get_vault_environment(self) -> Optional[str]:
        """ Get the environment name for the vault cluster
        This must match the environment keys in the secret json files
        used by all services in this cluster"""
        return self.config_dict.get('vault_environment')


def _run(
    command: Union[str, List[str]],
    env: Mapping[str, str]=os.environ,
    timeout: float=None,
    log: bool=False,
    stream: bool=False,
    stdin: Any=None,
    stdin_interrupt: bool=False,
    popen_kwargs: Dict={},
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
    output = []
    if log:
        service = kwargs['service']
        component = kwargs['component']
        cluster = kwargs.get('cluster', ANY_CLUSTER)
        instance = kwargs.get('instance', ANY_INSTANCE)
        loglevel = kwargs.get('loglevel', DEFAULT_LOGLEVEL)
    try:
        if not isinstance(command, list):
            command = shlex.split(command)
        popen_kwargs['stdout'] = PIPE
        popen_kwargs['stderr'] = STDOUT
        popen_kwargs['stdin'] = stdin
        popen_kwargs['env'] = env
        process = Popen(command, **popen_kwargs)

        if stdin_interrupt:
            def signal_handler(signum: int, frame: FrameType) -> None:
                process.stdin.write("\n")
                process.stdin.flush()
                process.wait()
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)

        # start the timer if we specified a timeout
        if timeout:
            proctimer = threading.Timer(timeout, _timeout, [process])
            proctimer.start()
        for linestr in iter(process.stdout.readline, b''):
            line = linestr.decode('utf-8')
            # additional indentation is for the paasta status command only
            if stream:
                if ('paasta_serviceinit status' in command):
                    if 'instance: ' in line:
                        paasta_print('  ' + line.rstrip('\n'))
                    else:
                        paasta_print('    ' + line.rstrip('\n'))
                else:
                    paasta_print(line.rstrip('\n'))
            else:
                output.append(line.rstrip('\n'))

            if log:
                _log(
                    service=service,
                    line=line.rstrip('\n'),
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
                line=e.strerror.rstrip('\n'),
                component=component,
                level=loglevel,
                cluster=cluster,
                instance=instance,
            )
        output.append(e.strerror.rstrip('\n'))
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
        output.append("Command '%s' timed out (longer than %ss)" % (command, timeout))
    return returncode, '\n'.join(output)


def get_umask() -> int:
    """Get the current umask for this process. NOT THREAD SAFE."""
    old_umask = os.umask(0o0022)
    os.umask(old_umask)
    return old_umask


def get_user_agent() -> str:
    user_agent = "PaaSTA Tools %s" % paasta_tools.__version__
    if len(sys.argv) >= 1:
        return user_agent + " " + os.path.basename(sys.argv[0])
    else:
        return user_agent


@contextlib.contextmanager
def atomic_file_write(target_path: str) -> Iterator[IO]:
    dirname = os.path.dirname(target_path)
    basename = os.path.basename(target_path)

    with tempfile.NamedTemporaryFile(
        dir=dirname,
        prefix=('.%s-' % basename),
        delete=False,
        mode='w',
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
    git_hash: Optional[str]=None,
    config_hash: Optional[str]=None,
    spacer: str=SPACER,
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
    composed = '%s%s%s' % (name, spacer, instance)
    if git_hash and config_hash:
        composed = '%s%s%s%s%s' % (composed, spacer, git_hash, spacer, config_hash)
    elif git_hash or config_hash:
        raise InvalidJobNameError(
            'invalid job id because git_hash (%s) and config_hash (%s) must '
            'both be defined or neither can be defined' % (git_hash, config_hash),
        )
    return composed


def decompose_job_id(job_id: str, spacer: str=SPACER) -> Tuple[str, str, str, str]:
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
        raise InvalidJobNameError('invalid job id %s' % job_id)
    return (decomposed[0], decomposed[1], git_hash, config_hash)


def build_docker_image_name(service: str) -> str:
    """docker-paasta.yelpcorp.com:443 is the URL for the Registry where PaaSTA
    will look for your images.

    :returns: a sanitized-for-Jenkins (s,/,-,g) version of the
    service's path in git. E.g. For git.yelpcorp.com:services/foo the
    docker image name is docker_registry/services-foo.
    """
    docker_registry_url = get_service_docker_registry(service)
    name = '%s/services-%s' % (docker_registry_url, service)
    return name


def build_docker_tag(service: str, upstream_git_commit: str) -> str:
    """Builds the DOCKER_TAG string

    upstream_git_commit is the SHA that we're building. Usually this is the
    tip of origin/master.
    """
    tag = '%s:paasta-%s' % (
        build_docker_image_name(service),
        upstream_git_commit,
    )
    return tag


def check_docker_image(service: str, tag: str) -> bool:
    """Checks whether the given image for :service: with :tag: exists.

    :raises: ValueError if more than one docker image with :tag: found.
    :returns: True if there is exactly one matching image found.
    """
    docker_client = get_docker_client()
    image_name = build_docker_image_name(service)
    docker_tag = build_docker_tag(service, tag)
    images = docker_client.images(name=image_name)
    # image['RepoTags'] may be None
    # Fixed upstream but only in docker-py 2.
    # https://github.com/docker/docker-py/issues/1401
    result = [image for image in images if docker_tag in (image['RepoTags'] or [])]
    if len(result) > 1:
        raise ValueError('More than one docker image found with tag %s\n%s' % (docker_tag, result))
    return len(result) == 1


def datetime_from_utc_to_local(utc_datetime: datetime.datetime) -> datetime.datetime:
    return datetime_convert_timezone(utc_datetime, dateutil.tz.tzutc(), dateutil.tz.tzlocal())


def datetime_convert_timezone(
    dt: datetime.datetime,
    from_zone: datetime.tzinfo,
    to_zone: datetime.tzinfo,
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
    return os.environ.get('SUDO_USER', pwd.getpwuid(os.getuid())[0])


def get_soa_cluster_deploy_files(
    service: str=None,
    soa_dir: str=DEFAULT_SOA_DIR,
    instance_type: str=None,
) -> Iterator[Tuple[str, str]]:
    if service is None:
        service = '*'
    service_path = os.path.join(soa_dir, service)

    if instance_type in INSTANCE_TYPES:
        instance_types = instance_type
    else:
        instance_types = '|'.join(INSTANCE_TYPES)

    search_re = r'/.*/(' + instance_types + r')-([0-9a-z-_]*)\.yaml$'

    for yaml_file in glob.glob('%s/*.yaml' % service_path):
        cluster_re_match = re.search(search_re, yaml_file)
        if cluster_re_match is not None:
            cluster = cluster_re_match.group(2)
            yield (cluster, yaml_file)


def list_clusters(service: str=None, soa_dir: str=DEFAULT_SOA_DIR, instance_type: str=None) -> List[str]:
    """Returns a sorted list of clusters a service is configured to deploy to,
    or all clusters if ``service`` is not specified.

    Includes every cluster that has a ``marathon-*.yaml`` or ``chronos-*.yaml`` file associated with it.

    :param service: The service name. If unspecified, clusters running any service will be included.
    :returns: A sorted list of cluster names
    """
    clusters = set()
    for cluster, _ in get_soa_cluster_deploy_files(
        service=service,
        soa_dir=soa_dir,
        instance_type=instance_type,
    ):
        clusters.add(cluster)
    return sorted(clusters)


def list_all_instances_for_service(
    service: str,
    clusters: Iterable[str]=None,
    instance_type: str=None,
    soa_dir: str=DEFAULT_SOA_DIR,
    cache: bool=True,
) -> Set[str]:
    instances = set()
    if not clusters:
        clusters = list_clusters(service, soa_dir=soa_dir)
    for cluster in clusters:
        if cache:
            si_list = get_service_instance_list(service, cluster, instance_type, soa_dir=soa_dir)
        else:
            si_list = get_service_instance_list_no_cache(service, cluster, instance_type, soa_dir=soa_dir)
        for service_instance in si_list:
            instances.add(service_instance[1])
    return instances


def get_service_instance_list_no_cache(
    service: str,
    cluster: Optional[str]=None,
    instance_type: str=None,
    soa_dir: str=DEFAULT_SOA_DIR,
) -> List[Tuple[str, str]]:
    """Enumerate the instances defined for a service as a list of tuples.

    :param service: The service name
    :param cluster: The cluster to read the configuration for
    :param instance_type: The type of instances to examine: 'marathon', 'chronos', or None (default) for both
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

    instance_list = []
    for srv_instance_type in instance_types:
        conf_file = "%s-%s" % (srv_instance_type, cluster)
        log.info("Enumerating all instances for config file: %s/*/%s.yaml" % (soa_dir, conf_file))
        instances = service_configuration_lib.read_extra_service_information(
            service,
            conf_file,
            soa_dir=soa_dir,
        )
        for instance in instances:
            if instance.startswith('_'):
                log.info("Ignoring %s.%s as instance name begins with '_'." % (service, instance))
            else:
                instance_list.append((service, instance))

    log.debug("Enumerated the following instances: %s", instance_list)
    return instance_list


@time_cache(ttl=5)
def get_service_instance_list(
    service: str,
    cluster: Optional[str]=None,
    instance_type: str=None,
    soa_dir: str=DEFAULT_SOA_DIR,
) -> List[Tuple[str, str]]:
    """Enumerate the instances defined for a service as a list of tuples.

    :param service: The service name
    :param cluster: The cluster to read the configuration for
    :param instance_type: The type of instances to examine: 'marathon', 'chronos', or None (default) for both
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (name, instance) for each instance defined for the service name
    """
    return get_service_instance_list_no_cache(
        service=service,
        cluster=cluster,
        instance_type=instance_type,
        soa_dir=soa_dir,
    )


def get_services_for_cluster(
    cluster: str=None,
    instance_type: str=None,
    soa_dir: str=DEFAULT_SOA_DIR,
) -> List[Tuple[str, str]]:
    """Retrieve all services and instances defined to run in a cluster.

    :param cluster: The cluster to read the configuration for
    :param instance_type: The type of instances to examine: 'marathon', 'chronos', or None (default) for both
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (service, instance)
    """

    if not cluster:
        cluster = load_system_paasta_config().get_cluster()
    rootdir = os.path.abspath(soa_dir)
    log.info("Retrieving all service instance names from %s for cluster %s", rootdir, cluster)
    instance_list: List[Tuple[str, str]] = []
    for srv_dir in os.listdir(rootdir):
        service_instance_list = get_service_instance_list(srv_dir, cluster, instance_type, soa_dir)
        for service_instance in service_instance_list:
            service, instance = service_instance
            if instance.startswith('_'):
                log.info("Ignoring %s.%s as instance name begins with '_'." % (service, instance))
            else:
                instance_list.append(service_instance)
    return instance_list


def parse_yaml_file(yaml_file: str) -> Any:
    return yaml.safe_load(open(yaml_file))


def get_docker_host() -> str:
    return os.environ.get('DOCKER_HOST', 'unix://var/run/docker.sock')


def get_docker_client() -> Client:
    client_opts = kwargs_from_env(assert_hostname=False)
    if 'base_url' in client_opts:
        return Client(**client_opts)
    else:
        return Client(base_url=get_docker_host(), **client_opts)


def get_running_mesos_docker_containers() -> List[Dict]:
    client = get_docker_client()
    running_containers = client.containers()
    return [container for container in running_containers if "mesos-" in container["Names"][0]]


class TimeoutError(Exception):
    pass


class Timeout:
    # From http://stackoverflow.com/questions/2281850/timeout-function-if-it-takes-too-long-to-finish

    def __init__(self, seconds: int=1, error_message: str='Timeout') -> None:
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


def print_with_indent(line: str, indent: int=2) -> None:
    """Print a line with a given indent level"""
    paasta_print(" " * indent + line)


class NoDeploymentsAvailable(Exception):
    pass


def load_deployments_json(service: str, soa_dir: str=DEFAULT_SOA_DIR) -> 'DeploymentsJson':
    deployment_file = os.path.join(soa_dir, service, 'deployments.json')
    if os.path.isfile(deployment_file):
        with open(deployment_file) as f:
            return DeploymentsJson(json.load(f)['v1'])
    else:
        raise NoDeploymentsAvailable


def load_v2_deployments_json(service: str, soa_dir: str=DEFAULT_SOA_DIR) -> 'DeploymentsJson':
    deployment_file = os.path.join(soa_dir, service, 'deployments.json')
    if os.path.isfile(deployment_file):
        with open(deployment_file) as f:
            return DeploymentsJson(json.load(f)['v2'])
    else:
        raise NoDeploymentsAvailable


class DeploymentsJson(dict):
    def get_branch_dict(self, service: str, branch: str) -> BranchDict:
        full_branch = '%s:paasta-%s' % (service, branch)
        return self.get(full_branch, {})

    def get_branch_dict_v2(self, service: str, branch: str, deploy_group: str) -> BranchDict:
        full_branch = '%s:%s' % (service, branch)
        branch_dict: BranchDict = {
            'docker_image': self.get_docker_image_for_deploy_group(deploy_group),
            'git_sha': self.get_git_sha_for_deploy_group(deploy_group),
            'desired_state': self.get_desired_state_for_branch(full_branch),
            'force_bounce': self.get_force_bounce_for_branch(full_branch),
        }
        return branch_dict

    def get_docker_image_for_deploy_group(self, deploy_group: str) -> str:
        try:
            return self['deployments'][deploy_group]['docker_image']
        except KeyError:
            raise NoDeploymentsAvailable

    def get_git_sha_for_deploy_group(self, deploy_group: str) -> str:
        try:
            return self['deployments'][deploy_group]['git_sha']
        except KeyError:
            raise NoDeploymentsAvailable

    def get_desired_state_for_branch(self, control_branch: str) -> str:
        try:
            return self['controls'][control_branch].get('desired_state', 'start')
        except KeyError:
            raise NoDeploymentsAvailable

    def get_force_bounce_for_branch(self, control_branch: str) -> str:
        try:
            return self['controls'][control_branch].get('force_bounce', None)
        except KeyError:
            raise NoDeploymentsAvailable


def get_paasta_branch(cluster: str, instance: str) -> str:
    return SPACER.join((cluster, instance))


def parse_timestamp(tstamp: str) -> datetime.datetime:
    return datetime.datetime.strptime(tstamp, '%Y%m%dT%H%M%S')


def format_timestamp(dt: datetime.datetime=None) -> str:
    if dt is None:
        dt = datetime.datetime.utcnow()
    return dt.strftime('%Y%m%dT%H%M%S')


def get_paasta_tag_from_deploy_group(identifier: str, desired_state: str) -> str:
    timestamp = format_timestamp(datetime.datetime.utcnow())
    return 'paasta-%s-%s-%s' % (identifier, timestamp, desired_state)


def get_paasta_tag(cluster: str, instance: str, desired_state: str) -> str:
    timestamp = format_timestamp(datetime.datetime.utcnow())
    return 'paasta-%s.%s-%s-%s' % (cluster, instance, timestamp, desired_state)


def format_tag(tag: str) -> str:
    return 'refs/tags/%s' % tag


class NoDockerImageError(Exception):
    pass


def get_config_hash(config: Any, force_bounce: str=None) -> str:
    """Create an MD5 hash of the configuration dictionary to be sent to
    Marathon. Or anything really, so long as str(config) works. Returns
    the first 8 characters so things are not really long.

    :param config: The configuration to hash
    :param force_bounce: a timestamp (in the form of a string) that is appended before hashing
                         that can be used to force a hash change
    :returns: A MD5 hash of str(config)
    """
    hasher = hashlib.md5()
    hasher.update(
        json.dumps(config, sort_keys=True).encode('UTF-8') +
        (force_bounce or '').encode('UTF-8'),
    )
    return "config%s" % hasher.hexdigest()[:8]


def get_code_sha_from_dockerurl(docker_url: str) -> str:
    """We encode the sha of the code that built a docker image *in* the docker
    url. This function takes that url as input and outputs the partial sha
    """
    parts = docker_url.split('/')
    parts = parts[-1].split('-')
    return "git%s" % parts[-1][:8]


def is_under_replicated(num_available: int, expected_count: int, crit_threshold: int) -> Tuple[bool, float]:
    """Calculates if something is under replicated

    :param num_available: How many things are up
    :param expected_count: How many things you think should be up
    :param crit_threshold: Int from 0-100
    :returns: Tuple of (bool, ratio)
    """
    if expected_count == 0:
        ratio = 100.
    else:
        ratio = (num_available / float(expected_count)) * 100

    if ratio < int(crit_threshold):
        return (True, ratio)
    else:
        return (False, ratio)


def deploy_blacklist_to_constraints(deploy_blacklist: DeployBlacklist) -> List[Constraint]:
    """Converts a blacklist of locations into marathon appropriate constraints.

    https://mesosphere.github.io/marathon/docs/constraints.html#unlike-operator
    https://github.com/Yelp/chronos/blob/master/docs/docs/api.md#unlike-constraint

    :param blacklist: List of lists of locations to blacklist
    :returns: List of lists of constraints
    """
    constraints: List[Constraint] = []
    for blacklisted_location in deploy_blacklist:
        constraints.append([blacklisted_location[0], "UNLIKE", blacklisted_location[1]])

    return constraints


def deploy_whitelist_to_constraints(deploy_whitelist: DeployWhitelist) -> List[Constraint]:
    """Converts a whitelist of locations into marathon appropriate constraints

    https://mesosphere.github.io/marathon/docs/constraints.html#like-operator
    https://github.com/Yelp/chronos/blob/master/docs/docs/api.md#like-constraint

    :param deploy_whitelist: List of lists of locations to whitelist
    :returns: List of lists of constraints
    """
    if deploy_whitelist is not None:
        (region_type, regions) = deploy_whitelist
        regionstr = '|'.join(regions)

        return [[region_type, 'LIKE', regionstr]]
    return []


def terminal_len(text: str) -> int:
    """Return the number of characters that text will take up on a terminal. """
    return len(remove_ansi_escape_sequences(text))


def format_table(rows: Iterable[Union[str, List[str]]], min_spacing: int=2) -> List[str]:
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
                    padding = ''
                else:
                    padding = ' ' * (widths[i] - terminal_len(cell))
                expanded_row.append(cell + padding)
            expanded_rows.append(expanded_row)

    return [(' ' * min_spacing).join(r) for r in expanded_rows]


_DeepMergeT = TypeVar('_DeepMergeT', bound=Any)


def deep_merge_dictionaries(overrides: _DeepMergeT, defaults: _DeepMergeT) -> _DeepMergeT:
    """
    Merges two dictionaries.
    """
    result = copy.deepcopy(defaults)
    stack: List[Tuple[Dict, Dict]] = [(overrides, result)]
    while stack:
        source_dict, result_dict = stack.pop()
        for key, value in source_dict.items():
            child = result_dict.setdefault(key, {})
            if isinstance(value, dict) and isinstance(child, dict):
                stack.append((value, child))
            else:
                result_dict[key] = value
    return result


class ZookeeperPool(object):
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
            cls.zk = KazooClient(hosts=load_system_paasta_config().get_zk_hosts(), read_only=True)
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
    if verbose_level == 1:
        return 0
    else:
        return 10 ** (verbose_level - 1)


def is_deploy_step(step: str) -> bool:
    """
    Returns true if the given step deploys to an instancename
    Returns false if the step is a predefined step-type, e.g. itest or command-*
    """
    return not ((step in DEPLOY_PIPELINE_NON_DEPLOY_STEPS) or (step.startswith('command-')))


_UseRequestsCacheFuncT = TypeVar('_UseRequestsCacheFuncT', bound=Callable)


def use_requests_cache(
    cache_name: str,
    backend: str='memory',
    **kwargs: Any,
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


def prompt_pick_one(sequence: Sequence[str], choosing: str) -> str:
    if not sys.stdin.isatty():
        paasta_print(
            'No {choosing} specified and no TTY present to ask.'
            'Please specify a {choosing} using the cli.'.format(choosing=choosing),
            file=sys.stderr,
        )
        sys.exit(1)

    if not sequence:
        paasta_print(
            'PaaSTA needs to pick a {choosing} but none were found.'.format(choosing=choosing),
            file=sys.stderr,
        )
        sys.exit(1)

    global_actions = [str('quit')]
    choices = [(str(item), str(item)) for item in sequence]

    chooser = choice.Menu(choices=choices, global_actions=global_actions)
    chooser.title = 'Please pick a {choosing} from the choices below (or "quit" to quit):'.format(
        choosing=str(choosing),
    )
    try:
        result = chooser.ask()
    except (KeyboardInterrupt, EOFError):
        paasta_print('')
        sys.exit(1)

    if isinstance(result, tuple) and result[1] == str('quit'):
        sys.exit(1)
    else:
        return result


def to_bytes(obj: Any) -> bytes:
    if isinstance(obj, bytes):
        return obj
    elif isinstance(obj, str):
        return obj.encode('UTF-8')
    else:
        return str(obj).encode('UTF-8')


def paasta_print(*args: Any, **kwargs: Any) -> None:
    f = kwargs.pop('file', sys.stdout)
    f = getattr(f, 'buffer', f)
    end = to_bytes(kwargs.pop('end', '\n'))
    sep = to_bytes(kwargs.pop('sep', ' '))
    assert not kwargs, kwargs
    to_print = sep.join(to_bytes(x) for x in args) + end
    f.write(to_print)
    f.flush()


_TimeoutFuncRetType = TypeVar('_TimeoutFuncRetType')


def timeout(
    seconds: int=10,
    error_message: str=os.strerror(errno.ETIME),
    use_signals: bool=True,
) -> Callable[[Callable[..., _TimeoutFuncRetType]], Callable[..., _TimeoutFuncRetType]]:
    if use_signals:
        def decorate(func: Callable[..., _TimeoutFuncRetType]) -> Callable[..., _TimeoutFuncRetType]:
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
        def decorate(func: Callable[..., _TimeoutFuncRetType]) -> Callable[..., _TimeoutFuncRetType]:
            # https://github.com/python/mypy/issues/797
            return _Timeout(func, seconds, error_message)  # type: ignore
    return decorate


class _Timeout(object):
    def __init__(self, function: Callable[..., _TimeoutFuncRetType], seconds: float, error_message: str) -> None:
        self.seconds = seconds
        self.control: queue.Queue[Tuple[bool, Union[_TimeoutFuncRetType, Tuple]]] = queue.Queue()
        self.function = function
        self.error_message = error_message

    def run(self, *args: Any, **kwargs: Any) -> None:
        # Try and put the result of the function into the q
        # if an exception occurrs then we put the exc_info instead
        # so that it can be raised in the main thread.
        try:
            self.control.put((True, self.function(*args, **kwargs)))
        except Exception:
            self.control.put((False, sys.exc_info()))

    def __call__(self, *args: Any, **kwargs: Any) -> _TimeoutFuncRetType:
        self.func_thread = threading.Thread(
            target=self.run,
            args=args,
            kwargs=kwargs,
        )
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
