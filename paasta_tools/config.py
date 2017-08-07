# Copyright 2015-2016 Yelp Inc.
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
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import logging
import math
import os
import re

import service_configuration_lib

import paasta_tools.cli.fsm
import paasta_tools.monitoring_tools
import paasta_tools.utils

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


class InstanceConfig(object):

    def __init__(self, cluster, instance, service, config_dict, branch_dict, soa_dir=DEFAULT_SOA_DIR):
        self.config_dict = config_dict
        self.branch_dict = branch_dict
        self.cluster = cluster
        self.instance = instance
        self.service = service
        self.soa_dir = soa_dir
        config_interpolation_keys = ('deploy_group',)
        interpolation_facts = self.__get_interpolation_facts()
        for key in config_interpolation_keys:
            if key in self.config_dict:
                self.config_dict[key] = self.config_dict[key].format(**interpolation_facts)

    def __get_interpolation_facts(self):
        return {
            'cluster': self.cluster,
            'instance': self.instance,
            'service': self.service,
        }

    def get_cluster(self):
        return self.cluster

    def get_instance(self):
        return self.instance

    def get_service(self):
        return self.service

    def get_docker_registry(self):
        return paasta_tools.utils.get_service_docker_registry(self.service, self.soa_dir)

    def get_branch(self):
        return paasta_tools.utils.get_paasta_branch(cluster=self.get_cluster(), instance=self.get_instance())

    def get_deploy_group(self):
        return self.config_dict.get('deploy_group', self.get_branch())

    def get_mem(self):
        """Gets the memory required from the service's configuration.

        Defaults to 1024 (1G) if no value specified in the config.

        :returns: The amount of memory specified by the config, 1024 if not specified"""
        mem = self.config_dict.get('mem', 1024)
        return mem

    def get_mem_swap(self):
        """Gets the memory-swap value. This value is passed to the docker
        container to ensure that the total memory limit (memory + swap) is the
        same value as the 'mem' key in soa-configs. Note - this value *has* to
        be >= to the mem key, so we always round up to the closest MB.
        """
        mem = self.get_mem()
        mem_swap = int(math.ceil(mem))
        return "%sm" % mem_swap

    def get_cpus(self):
        """Gets the number of cpus required from the service's configuration.

        Defaults to .25 (1/4 of a cpu) if no value specified in the config.

        :returns: The number of cpus specified in the config, .25 if not specified"""
        cpus = self.config_dict.get('cpus', .25)
        return cpus

    def get_cpu_period(self):
        """The --cpu-period option to be passed to docker
        Comes from the cfs_period_us configuration option

        :returns: The number to be passed to the --cpu-period docker flag"""
        return self.config_dict.get('cfs_period_us', DEFAULT_CPU_PERIOD)

    def get_cpu_quota(self):
        """Gets the --cpu-quota option to be passed to docker
        Calculated from the cpu_burst_pct configuration option, which is the percent
        over its declared cpu usage that a container will be allowed to go.

        Calculation: cpus * cfs_period_us * (100 + cpu_burst_pct) / 100

        :returns: The number to be passed to the --cpu-quota docker flag"""
        cpu_burst_pct = self.config_dict.get('cpu_burst_pct', DEFAULT_CPU_BURST_PCT)
        return self.get_cpus() * self.get_cpu_period() * (100 + cpu_burst_pct) / 100

    def get_shm_size(self):
        """Get's the shm_size to pass to docker
        See --shm-size in the docker docs"""
        return self.config_dict.get('shm_size', None)

    def get_ulimit(self):
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

    def get_cap_add(self):
        """Get the --cap-add options to be passed to docker
        Generated from the cap_add configuration option, which is a list of
        capabilities.

        Example configuration: {'cap_add': ['IPC_LOCK', 'SYS_PTRACE']}

        :returns: A generator of cap_add options to be passed as --cap-add flags"""
        for value in self.config_dict.get('cap_add', []):
            yield {"key": "cap-add", "value": "{}".format(value)}

    def format_docker_parameters(self, with_labels=True):
        """Formats extra flags for running docker.  Will be added in the format
        `["--%s=%s" % (e['key'], e['value']) for e in list]` to the `docker run` command
        Note: values must be strings

        :param with_labels: Whether to build docker parameters with or without labels
        :returns: A list of parameters to be added to docker run"""
        parameters = [
            {"key": "memory-swap", "value": self.get_mem_swap()},
            {"key": "cpu-period", "value": "%s" % int(self.get_cpu_period())},
            {"key": "cpu-quota", "value": "%s" % int(self.get_cpu_quota())},
        ]
        if with_labels:
            parameters.extend([
                {"key": "label", "value": "paasta_service=%s" % self.service},
                {"key": "label", "value": "paasta_instance=%s" % self.instance},
            ])
        shm = self.get_shm_size()
        if shm:
            parameters.extend([
                {"key": "shm-size", "value": "%s" % shm},
            ])
        parameters.extend(self.get_ulimit())
        parameters.extend(self.get_cap_add())
        return parameters

    def get_disk(self, default=1024):
        """Gets the  amount of disk space required from the service's configuration.

        Defaults to 1024 (1G) if no value is specified in the config.

        :returns: The amount of disk space specified by the config, 1024 if not specified"""
        disk = self.config_dict.get('disk', default)
        return disk

    def get_cmd(self):
        """Get the docker cmd specified in the service's configuration.

        Defaults to None if not specified in the config.

        :returns: A string specified in the config, None if not specified"""
        return self.config_dict.get('cmd', None)

    def get_env_dictionary(self):
        """A dictionary of key/value pairs that represent environment variables
        to be injected to the container environment"""
        env = {
            "PAASTA_SERVICE": self.service,
            "PAASTA_INSTANCE": self.instance,
            "PAASTA_CLUSTER": self.cluster,
            "PAASTA_DEPLOY_GROUP": self.get_deploy_group(),
            "PAASTA_DOCKER_IMAGE": self.get_docker_image(),
            "PAASTA_TEAM": paasta_tools.monitoring_tools.get_team(service=self.service, overrides={}),
        }
        user_env = self.config_dict.get('env', {})
        env.update(user_env)
        return env

    def get_env(self):
        """Basic get_env that simply returns the basic env, other classes
        might need to override this getter for more implementation-specific
        env getting"""
        return self.get_env_dictionary()

    def get_args(self):
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

    def get_monitoring(self):
        """Get monitoring overrides defined for the given instance"""
        return self.config_dict.get('monitoring', {})

    def get_deploy_constraints(self, blacklist, whitelist):
        """Return the combination of deploy_blacklist and deploy_whitelist
        as a list of constraints.
        """
        return (
            paasta_tools.utils.deploy_blacklist_to_constraints(blacklist) +
            paasta_tools.utils.deploy_whitelist_to_constraints(whitelist)
        )

    def get_deploy_blacklist(self, system_deploy_blacklist):
        """The deploy blacklist is a list of lists, where the lists indicate
        which locations the service should not be deployed"""
        return (
            self.config_dict.get('deploy_blacklist', []) +
            system_deploy_blacklist
        )

    def get_deploy_whitelist(self, system_deploy_whitelist):
        """The deploy whitelist is a list of lists, where the lists indicate
        which locations are explicitly allowed.  The blacklist will supersede
        this if a host matches both the white and blacklists."""
        return (
            self.config_dict.get('deploy_whitelist', []) +
            system_deploy_whitelist
        )

    def get_monitoring_blacklist(self, system_deploy_blacklist):
        """The monitoring_blacklist is a list of tuples, where the tuples indicate
        which locations the user doesn't care to be monitored"""
        return (
            self.config_dict.get('monitoring_blacklist', []) +
            self.get_deploy_blacklist(system_deploy_blacklist)
        )

    def get_docker_image(self):
        """Get the docker image name (with tag) for a given service branch from
        a generated deployments.json file."""
        return self.branch_dict.get('docker_image', '')

    def get_docker_url(self):
        """Compose the docker url.
        :returns: '<registry_uri>/<docker_image>'
        """
        registry_uri = self.get_docker_registry()
        docker_image = self.get_docker_image()
        if not docker_image:
            raise paasta_tools.utils.NoDockerImageError(
                'Docker url not available because there is no docker_image',
            )
        docker_url = '%s/%s' % (registry_uri, docker_image)
        return docker_url

    def get_desired_state(self):
        """Get the desired state (either 'start' or 'stop') for a given service
        branch from a generated deployments.json file."""
        return self.branch_dict.get('desired_state', 'start')

    def get_force_bounce(self):
        """Get the force_bounce token for a given service branch from a generated
        deployments.json file. This is a token that, when changed, indicates that
        the instance should be recreated and bounced, even if no other
        parameters have changed. This may be None or a string, generally a
        timestamp.
        """
        return self.branch_dict.get('force_bounce', None)

    def check_cpus(self):
        cpus = self.get_cpus()
        if cpus is not None:
            if not isinstance(cpus, (float, int)):
                return False, 'The specified cpus value "%s" is not a valid float or int.' % cpus
        return True, ''

    def check_mem(self):
        mem = self.get_mem()
        if mem is not None:
            if not isinstance(mem, (float, int)):
                return False, 'The specified mem value "%s" is not a valid float or int.' % mem
        return True, ''

    def check_disk(self):
        disk = self.get_disk()
        if disk is not None:
            if not isinstance(disk, (float, int)):
                return False, 'The specified disk value "%s" is not a valid float or int.' % disk
        return True, ''

    def check_security(self):
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

    def check_dependencies_reference(self):
        dependencies_reference = self.config_dict.get('dependencies_reference')
        if dependencies_reference is None:
            return True, ''

        dependencies = self.config_dict.get('dependencies')
        if dependencies is None:
            return False, 'dependencies_reference "%s" declared but no dependencies found' % dependencies_reference

        if dependencies_reference not in dependencies:
            return False, 'dependencies_reference "%s" not found in dependencies dictionary' % dependencies_reference

        return True, ''

    def check(self, param):
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

    def validate(self):
        error_msgs = []
        for param in ['cpus', 'mem', 'security', 'dependencies_reference']:
            check_passed, check_msg = self.check(param)
            if not check_passed:
                error_msgs.append(check_msg)
        return error_msgs

    def get_extra_volumes(self):
        """Extra volumes are a specially formatted list of dictionaries that should
        be bind mounted in a container The format of the dictionaries should
        conform to the `Mesos container volumes spec
        <https://mesosphere.github.io/marathon/docs/native-docker.html>`_"""
        return self.config_dict.get('extra_volumes', [])

    def get_pool(self):
        """Which pool of nodes this job should run on. This can be used to mitigate noisy neighbors, by putting
        particularly noisy or noise-sensitive jobs into different pools.

        This is implemented with an attribute "pool" on each mesos slave and by adding a constraint to Marathon/Chronos
        application defined by this instance config.

        Eventually this may be implemented with Mesos roles, once a framework can register under multiple roles.

        :returns: the "pool" attribute in your config dict, or the string "default" if not specified."""
        return self.config_dict.get('pool', 'default')

    def get_pool_constraints(self):
        pool = self.get_pool()
        return [["pool", "LIKE", pool]]

    def get_constraints(self):
        return self.config_dict.get('constraints', None)

    def get_extra_constraints(self):
        return self.config_dict.get('extra_constraints', [])

    def get_net(self):
        """
        :returns: the docker networking mode the container should be started with.
        """
        return self.config_dict.get('net', 'bridge')

    def get_volumes(self, system_volumes):
        volumes = system_volumes + self.get_extra_volumes()
        deduped = {v['containerPath'] + v['hostPath']: v for v in volumes}.values()
        return paasta_tools.utils.sort_dicts(deduped)

    def get_dependencies_reference(self):
        """Get the reference to an entry in dependencies.yaml

        Defaults to None if not specified in the config.

        :returns: A string specified in the config, None if not specified"""
        return self.config_dict.get('dependencies_reference')

    def get_dependencies(self):
        """Get the contents of the dependencies_dict pointed to by the dependency_reference

        Defaults to None if not specified in the config.

        :returns: A list of dictionaries specified in the dependencies_dict, None if not specified"""
        dependencies = self.config_dict.get('dependencies')
        if not dependencies:
            return None
        return dependencies.get(self.get_dependencies_reference())

    def get_outbound_firewall(self):
        """Return 'block', 'monitor', or None as configured in security->outbound_firewall

        Defaults to None if not specified in the config

        :returns: A string specified in the config, None if not specified"""
        security = self.config_dict.get('security')
        if not security:
            return None
        return security.get('outbound_firewall')

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.config_dict == other.config_dict and \
                self.branch_dict == other.branch_dict and \
                self.cluster == other.cluster and \
                self.instance == other.instance and \
                self.service == other.service
        else:
            return False


class InvalidInstanceConfig(Exception):
    pass
