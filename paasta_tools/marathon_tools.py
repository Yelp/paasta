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
import json
import logging
import os
import re
import socket
from math import ceil
from time import sleep

import service_configuration_lib
from kazoo.exceptions import NoNodeError
from marathon import MarathonClient
from marathon import MarathonHttpError
from marathon import NotFoundError

from paasta_tools.mesos_tools import get_local_slave_state
from paasta_tools.mesos_tools import get_mesos_network_for_net
from paasta_tools.mesos_tools import get_mesos_slaves_grouped_by_attribute
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import deploy_blacklist_to_constraints
from paasta_tools.utils import deploy_whitelist_to_constraints
from paasta_tools.utils import get_code_sha_from_dockerurl
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import get_docker_url
from paasta_tools.utils import get_paasta_branch
from paasta_tools.utils import get_service_instance_list
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import InvalidInstanceConfig
from paasta_tools.utils import load_deployments_json
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import PATH_TO_SYSTEM_PAASTA_CONFIG_DIR
from paasta_tools.utils import timeout
from paasta_tools.utils import ZookeeperPool

CONTAINER_PORT = 8888
# Marathon creates Mesos tasks with an id composed of the app's full name, a
# spacer, and a UUID. This variable is that spacer. Note that we don't control
# this spacer, i.e. you can't change it here and expect the world to change
# with you. We need to know what it is so we can decompose Mesos task ids.
MESOS_TASK_SPACER = '.'
PATH_TO_MARATHON_CONFIG = os.path.join(PATH_TO_SYSTEM_PAASTA_CONFIG_DIR, 'marathon.json')
PUPPET_SERVICE_DIR = '/etc/nerve/puppet_services.d'

log = logging.getLogger(__name__)
logging.getLogger('marathon').setLevel(logging.WARNING)


def load_marathon_config(path=PATH_TO_MARATHON_CONFIG):
    try:
        with open(path) as f:
            return MarathonConfig(json.load(f), path)
    except IOError as e:
        raise PaastaNotConfiguredError("Could not load marathon config file %s: %s" % (e.filename, e.strerror))


class MarathonNotConfigured(Exception):
    pass


class MarathonConfig(dict):

    def __init__(self, config, path):
        self.path = path
        super(MarathonConfig, self).__init__(config)

    def get_url(self):
        """Get the Marathon API url

        :returns: The Marathon API endpoint"""
        try:
            return self['url']
        except KeyError:
            raise MarathonNotConfigured('Could not find marathon url in system marathon config: %s' % self.path)

    def get_username(self):
        """Get the Marathon API username

        :returns: The Marathon API username"""
        try:
            return self['user']
        except KeyError:
            raise MarathonNotConfigured('Could not find marathon user in system marathon config: %s' % self.path)

    def get_password(self):
        """Get the Marathon API password

        :returns: The Marathon API password"""
        try:
            return self['password']
        except KeyError:
            raise MarathonNotConfigured('Could not find marathon password in system marathon config: %s' % self.path)


def load_marathon_service_config(service, instance, cluster, load_deployments=True, soa_dir=DEFAULT_SOA_DIR):
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
    log.info("Reading service configuration files from dir %s/ in %s" % (service, soa_dir))
    log.info("Reading general configuration file: service.yaml")
    general_config = service_configuration_lib.read_service_configuration(
        service,
        soa_dir=soa_dir
    )
    marathon_conf_file = "marathon-%s" % cluster
    log.info("Reading marathon configuration file: %s.yaml", marathon_conf_file)
    instance_configs = service_configuration_lib.read_extra_service_information(
        service,
        marathon_conf_file,
        soa_dir=soa_dir
    )

    if instance not in instance_configs:
        raise NoConfigurationForServiceError(
            "%s not found in config file %s/%s/%s.yaml." % (instance, soa_dir, service, marathon_conf_file)
        )

    general_config = deep_merge_dictionaries(overrides=instance_configs[instance], defaults=general_config)

    branch_dict = {}
    if load_deployments:
        deployments_json = load_deployments_json(service, soa_dir=soa_dir)
        branch = general_config.get('branch', get_paasta_branch(cluster, instance))
        branch_dict = deployments_json.get_branch_dict(service, branch)

    return MarathonServiceConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=general_config,
        branch_dict=branch_dict,
    )


class InvalidMarathonConfig(Exception):
    pass


class MarathonServiceConfig(InstanceConfig):

    def __init__(self, service, cluster, instance, config_dict, branch_dict):
        super(MarathonServiceConfig, self).__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
        )

    def __repr__(self):
        return "MarathonServiceConfig(%r, %r, %r, %r, %r)" % (
            self.service,
            self.cluster,
            self.instance,
            self.config_dict,
            self.branch_dict
        )

    def copy(self):
        return self.__class__(
            service=self.service,
            instance=self.instance,
            cluster=self.cluster,
            config_dict=dict(self.config_dict),
            branch_dict=dict(self.branch_dict),
        )

    def get_min_instances(self):
        return self.config_dict.get('min_instances', 1)

    def get_max_instances(self):
        return self.config_dict.get('max_instances', None)

    def get_instances(self):
        """Get the number of instances specified in zookeeper or the service's marathon configuration.
        If the number of instances in zookeeper is less than min_instances, returns min_instances.
        If the number of instances in zookeeper is greater than max_instances, returns max_instances.

        Defaults to 0 if not specified in the config.

        :param service_config: The service instance's configuration dictionary
        :returns: The number of instances specified in the config, 0 if not
                  specified or if desired_state is not 'start'."""
        if self.get_desired_state() == 'start':
            if self.get_max_instances() is not None:
                try:
                    zk_instances = get_instances_from_zookeeper(
                        service=self.service,
                        instance=self.instance,
                    )
                    log.debug("Got %d instances out of zookeeper" % zk_instances)
                except NoNodeError:
                    log.debug("No zookeeper data, returning max_instances (%d)" % self.get_max_instances())
                    return self.get_max_instances()
                else:
                    limited_instances = self.limit_instance_count(zk_instances)
                    if limited_instances != zk_instances:
                        log.warning("Returning limited instance count %d. (zk had %d)" % (
                                    limited_instances, zk_instances))
                    return limited_instances
            else:
                instances = self.config_dict.get('instances', 1)
                log.debug("Autoscaling not enabled, returning %d instances" % instances)
                return instances
        else:
            log.debug("Instance is set to stop. Returning '0' instances")
            return 0

    def get_autoscaling_params(self):
        default_params = {
            'metrics_provider': 'mesos_cpu',
            'decision_policy': 'pid',
            'setpoint': 0.8,
        }
        return deep_merge_dictionaries(overrides=self.config_dict.get('autoscaling', {}), defaults=default_params)

    def limit_instance_count(self, instances):
        """
        Returns param instances if it is between min_instances and max_instances.
        Returns max_instances if instances > max_instances
        Returns min_instances if instances < min_instances
        """
        return max(
            self.get_min_instances(),
            min(self.get_max_instances(), instances),
        )

    def get_backoff_seconds(self):
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

    def get_bounce_method(self):
        """Get the bounce method specified in the service's marathon configuration.

        :param service_config: The service instance's configuration dictionary
        :returns: The bounce method specified in the config, or 'crossover' if not specified"""
        return self.config_dict.get('bounce_method', 'crossover')

    def get_drain_method(self, service_namespace_config):
        """Get the drain method specified in the service's marathon configuration.

        :param service_config: The service instance's configuration dictionary
        :returns: The drain method specified in the config, or 'noop' if not specified"""
        default = 'noop'
        # Default to hacheck draining if the service is in smartstack
        if service_namespace_config.is_in_smartstack():
            default = 'hacheck'
        return self.config_dict.get('drain_method', default)

    def get_drain_method_params(self, service_namespace_config):
        """Get the drain method parameters specified in the service's marathon configuration.

        :param service_config: The service instance's configuration dictionary
        :returns: The drain_method_params dictionary specified in the config, or {} if not specified"""
        default = {}
        if service_namespace_config.is_in_smartstack():
            default = {'delay': 30}
        return self.config_dict.get('drain_method_params', default)

    def get_calculated_constraints(self, service_namespace_config):
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
            constraints.extend(self.get_routing_constraints(service_namespace_config))
            constraints.extend(self.get_deploy_constraints())
            constraints.extend(self.get_pool_constraints())
        return [[str(val) for val in constraint] for constraint in constraints]

    def get_routing_constraints(self, service_namespace_config):
        discover_level = service_namespace_config.get_discover()
        locations = get_mesos_slaves_grouped_by_attribute(
            attribute=discover_level, blacklist=self.get_deploy_blacklist(),
            whitelist=self.get_deploy_whitelist())

        routing_constraints = [[discover_level, "GROUP_BY", str(len(locations))]]
        return routing_constraints

    def get_deploy_constraints(self):
        return (deploy_blacklist_to_constraints(self.get_deploy_blacklist()) +
                deploy_whitelist_to_constraints(self.get_deploy_whitelist()))

    def format_marathon_app_dict(self):
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

        # A set of config attributes that don't get included in the hash of the config.
        # These should be things that PaaSTA/Marathon knows how to change without requiring a bounce.
        CONFIG_HASH_BLACKLIST = set(['instances', 'backoff_seconds', 'min_instances', 'max_instances'])

        system_paasta_config = load_system_paasta_config()
        docker_url = get_docker_url(system_paasta_config.get_docker_registry(), self.get_docker_image())
        service_namespace_config = load_service_namespace_config(
            service=self.service,
            namespace=self.get_nerve_namespace(),
        )
        docker_volumes = system_paasta_config.get_volumes() + self.get_extra_volumes()

        net = get_mesos_network_for_net(self.get_net())

        complete_config = {
            'container': {
                'docker': {
                    'image': docker_url,
                    'network': net,
                    "parameters": [
                        {"key": "memory-swap", "value": self.get_mem_swap()},
                    ]
                },
                'type': 'DOCKER',
                'volumes': docker_volumes,
            },
            'uris': [system_paasta_config.get_dockercfg_location(), ],
            'backoff_seconds': self.get_backoff_seconds(),
            'backoff_factor': 2,
            'health_checks': self.get_healthchecks(service_namespace_config),
            'env': self.get_env(),
            'mem': float(self.get_mem()),
            'cpus': float(self.get_cpus()),
            'disk': float(self.get_disk()),
            'constraints': self.get_calculated_constraints(service_namespace_config),
            'instances': self.get_instances(),
            'cmd': self.get_cmd(),
            'args': self.get_args(),
        }

        if net == 'BRIDGE':
            complete_config['container']['docker']['portMappings'] = [
                {
                    'containerPort': CONTAINER_PORT,
                    'hostPort': 0,
                    'protocol': 'tcp',
                },
            ]

        accepted_resource_roles = self.get_accepted_resource_roles()
        if accepted_resource_roles is not None:
            complete_config['accepted_resource_roles'] = accepted_resource_roles

        code_sha = get_code_sha_from_dockerurl(docker_url)

        config_hash = get_config_hash(
            {key: value for key, value in complete_config.items() if key not in CONFIG_HASH_BLACKLIST},
            force_bounce=self.get_force_bounce(),
        )
        complete_config['id'] = format_job_id(self.service, self.instance, code_sha, config_hash)

        log.debug("Complete configuration for instance is: %s", complete_config)
        return complete_config

    def get_healthchecks(self, service_namespace_config):
        """Returns a list of healthchecks per `the Marathon docs`_.

        If you have an http service, it uses the default endpoint that smartstack uses.
        (/status currently)

        Otherwise these do *not* use the same thresholds as smartstack in order to not
        produce a negative feedback loop, where mesos agressivly kills tasks because they
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

        if mode == 'http':
            http_path = self.get_healthcheck_uri(service_namespace_config)
            healthchecks = [
                {
                    "protocol": "HTTP",
                    "path": http_path,
                    "gracePeriodSeconds": graceperiodseconds,
                    "intervalSeconds": intervalseconds,
                    "portIndex": 0,
                    "timeoutSeconds": timeoutseconds,
                    "maxConsecutiveFailures": maxconsecutivefailures
                },
            ]
        elif mode == 'tcp':
            healthchecks = [
                {
                    "protocol": "TCP",
                    "gracePeriodSeconds": graceperiodseconds,
                    "intervalSeconds": intervalseconds,
                    "portIndex": 0,
                    "timeoutSeconds": timeoutseconds,
                    "maxConsecutiveFailures": maxconsecutivefailures
                },
            ]
        elif mode == 'cmd':
            healthchecks = [
                {
                    "protocol": "COMMAND",
                    "command": {"value": self.get_healthcheck_cmd()},
                    "gracePeriodSeconds": graceperiodseconds,
                    "intervalSeconds": intervalseconds,
                    "timeoutSeconds": timeoutseconds,
                    "maxConsecutiveFailures": maxconsecutivefailures
                },
            ]
        elif mode is None:
            healthchecks = []
        else:
            raise InvalidMarathonHealthcheckMode(
                "Unknown mode: %s. Only acceptable healthcheck modes are http/tcp/cmd" % mode)
        return healthchecks

    def get_healthcheck_uri(self, service_namespace_config):
        return self.config_dict.get('healthcheck_uri', service_namespace_config.get_healthcheck_uri())

    def get_healthcheck_cmd(self):
        cmd = self.config_dict.get('healthcheck_cmd', None)
        if cmd is None:
            raise InvalidInstanceConfig("healthcheck mode 'cmd' requires a healthcheck_cmd to run")
        else:
            return cmd

    def get_healthcheck_mode(self, service_namespace_config):
        mode = self.config_dict.get('healthcheck_mode', None)
        if mode is None:
            mode = service_namespace_config.get_mode()
        elif mode not in ['http', 'tcp', 'cmd', None]:
            raise InvalidMarathonHealthcheckMode("Unknown mode: %s" % mode)
        return mode

    def get_healthcheck_grace_period_seconds(self):
        """How long Marathon should give a service to come up before counting failed healthchecks."""
        return self.config_dict.get('healthcheck_grace_period_seconds', 60)

    def get_healthcheck_interval_seconds(self):
        return self.config_dict.get('healthcheck_interval_seconds', 10)

    def get_healthcheck_timeout_seconds(self):
        return self.config_dict.get('healthcheck_timeout_seconds', 10)

    def get_healthcheck_max_consecutive_failures(self):
        return self.config_dict.get('healthcheck_max_consecutive_failures', 30)

    def get_nerve_namespace(self):
        return self.config_dict.get('nerve_ns', self.instance)

    def get_bounce_health_params(self, service_namespace_config):
        default = {}
        if service_namespace_config.is_in_smartstack():
            default = {'check_haproxy': True}
        return self.config_dict.get('bounce_health_params', default)

    def get_accepted_resource_roles(self):
        return self.config_dict.get('accepted_resource_roles', None)

    def get_desired_state_human(self):
        desired_state = self.get_desired_state()
        if desired_state == 'start' and self.get_instances() != 0:
            return PaastaColors.bold('Started')
        elif desired_state == 'start' and self.get_instances() == 0:
            return PaastaColors.bold('Stopped')
        elif desired_state == 'stop':
            return PaastaColors.red('Stopped')
        else:
            return PaastaColors.red('Unknown (desired_state: %s)' % desired_state)

    def get_replication_crit_percentage(self):
        return self.config_dict.get('replication_threshold', 50)


def load_service_namespace_config(service, namespace, soa_dir=DEFAULT_SOA_DIR):
    """Attempt to read the configuration for a service's namespace in a more strict fashion.

    Retrevies the following keys:

    - proxy_port: the proxy port defined for the given namespace
    - healthcheck_mode: the mode for the healthcheck (http or tcp)
    - healthcheck_port: An alternate port to use for health checking
    - healthcheck_uri: URI target for healthchecking
    - healthcheck_timeout_s: healthcheck timeout in seconds
    - updown_timeout_s: updown_service timeout in seconds
    - timeout_connect_ms: proxy frontend timeout in milliseconds
    - timeout_server_ms: proxy server backend timeout in milliseconds
    - timeout_client_ms: proxy server client timeout in milliseconds
    - retries: the number of retries on a proxy backend
    - mode: the mode the service is run in (http or tcp)
    - routes: a list of tuples of (source, destination)
    - discover: the scope at which to discover services e.g. 'habitat'
    - advertise: a list of scopes to advertise services at e.g. ['habitat', 'region']
    - extra_advertise: a list of tuples of (source, destination)
      e.g. [('region:dc6-prod', 'region:useast1-prod')]
    - extra_healthcheck_headers: a dict of HTTP headers that must
      be supplied when health checking. E.g. { 'Host': 'example.com' }

    :param service: The service name
    :param namespace: The namespace to read
    :param soa_dir: The SOA config directory to read from
    :returns: A dict of the above keys, if they were defined
    """

    service_config = service_configuration_lib.read_service_configuration(service, soa_dir)
    smartstack_config = service_config.get('smartstack', {})
    namespace_config_from_file = smartstack_config.get(namespace, {})

    service_namespace_config = ServiceNamespaceConfig()
    # We can't really use .get, as we don't want the key to be in the returned
    # dict at all if it doesn't exist in the config file.
    # We also can't just copy the whole dict, as we only care about some keys
    # and there's other things that appear in the smartstack section in
    # several cases.
    key_whitelist = set([
        'healthcheck_mode',
        'healthcheck_uri',
        'healthcheck_port',
        'healthcheck_timeout_s',
        'updown_timeout_s',
        'proxy_port',
        'timeout_connect_ms',
        'timeout_server_ms',
        'timeout_client_ms',
        'retries',
        'mode',
        'discover',
        'advertise',
        'extra_healthcheck_headers'
    ])

    for key, value in namespace_config_from_file.items():
        if key in key_whitelist:
            service_namespace_config[key] = value

    # Other code in paasta_tools checks 'mode' after the config file
    # is loaded, so this ensures that it is set to the appropriate default
    # if not otherwise specified, even if appropriate default is None.
    service_namespace_config['mode'] = service_namespace_config.get_mode()

    if 'routes' in namespace_config_from_file:
        service_namespace_config['routes'] = [(route['source'], dest)
                                              for route in namespace_config_from_file['routes']
                                              for dest in route['destinations']]

    if 'extra_advertise' in namespace_config_from_file:
        service_namespace_config['extra_advertise'] = [
            (src, dst)
            for src in namespace_config_from_file['extra_advertise']
            for dst in namespace_config_from_file['extra_advertise'][src]
        ]

    return service_namespace_config


class ServiceNamespaceConfig(dict):

    def get_mode(self):
        """Get the mode that the service runs in and check that we support it.
        If the mode is not specified, we check whether the service uses smartstack
        in order to determine the appropriate default value. If proxy_port is specified
        in the config, the service uses smartstack, and we can thus safely assume its mode is http.
        If the mode is not defined and the service does not use smartstack, we set the mode to None.
        """
        mode = self.get('mode', None)
        if mode is None:
            if not self.is_in_smartstack():
                return None
            else:
                return 'http'
        elif mode in ['http', 'tcp']:
            return mode
        else:
            raise InvalidSmartstackMode("Unknown mode: %s" % mode)

    def get_healthcheck_uri(self):
        return self.get('healthcheck_uri', '/status')

    def get_discover(self):
        return self.get('discover', 'region')

    def is_in_smartstack(self):
        if self.get('proxy_port') is not None:
            return True
        else:
            return False


class InvalidSmartstackMode(Exception):
    pass


class InvalidMarathonHealthcheckMode(Exception):
    pass


def get_marathon_client(url, user, passwd):
    """Get a new marathon client connection in the form of a MarathonClient object.

    :param url: The url to connect to marathon at
    :param user: The username to connect with
    :param passwd: The password to connect with
    :returns: A new marathon.MarathonClient object"""
    log.info("Connecting to Marathon server at: %s", url)
    return MarathonClient(url, user, passwd, timeout=30)


def format_job_id(service, instance, git_hash=None, config_hash=None):
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
    service = str(service).replace('_', '--')
    instance = str(instance).replace('_', '--')
    if git_hash:
        git_hash = str(git_hash).replace('_', '--')
    if config_hash:
        config_hash = str(config_hash).replace('_', '--')
    formatted = compose_job_id(service, instance, git_hash, config_hash)
    return formatted


def deformat_job_id(job_id):
    job_id = job_id.replace('--', '_')
    return decompose_job_id(job_id)


def read_namespace_for_service_instance(name, instance, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Retreive a service instance's nerve namespace from its configuration file.
    If one is not defined in the config file, returns instance instead."""
    if not cluster:
        cluster = load_system_paasta_config().get_cluster()
    srv_info = service_configuration_lib.read_extra_service_information(
        name,
        "marathon-%s" % cluster,
        soa_dir
    )[instance]
    return srv_info['nerve_ns'] if 'nerve_ns' in srv_info else instance


def get_proxy_port_for_instance(name, instance, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Get the proxy_port defined in the namespace configuration for a service instance.

    This means that the namespace first has to be loaded from the service instance's
    configuration, and then the proxy_port has to loaded from the smartstack configuration
    for that namespace.

    :param name: The service name
    :param instance: The instance of the service
    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: The proxy_port for the service instance, or None if not defined"""
    namespace = read_namespace_for_service_instance(name, instance, cluster, soa_dir)
    nerve_dict = load_service_namespace_config(name, namespace, soa_dir)
    return nerve_dict.get('proxy_port')


def get_all_namespaces_for_service(service, soa_dir=DEFAULT_SOA_DIR, full_name=True):
    """Get all the smartstack namespaces listed for a given service name.

    :param service: The service name
    :param soa_dir: The SOA config directory to read from
    :param full_name: A boolean indicating if the service name should be prepended to the namespace in the
                      returned tuples as described below (Default: True)
    :returns: A list of tuples of the form (service<SPACER>namespace, namespace_config) if full_name is true,
              otherwise of the form (namespace, namespace_config)
    """
    service_config = service_configuration_lib.read_service_configuration(service, soa_dir)
    smartstack = service_config.get('smartstack', {})
    namespace_list = []
    for namespace in smartstack:
        if full_name:
            name = compose_job_id(service, namespace)
        else:
            name = namespace
        namespace_list.append((name, smartstack[namespace]))
    return namespace_list


def get_all_namespaces(soa_dir=DEFAULT_SOA_DIR):
    """Get all the smartstack namespaces across all services.
    This is mostly so synapse can get everything it needs in one call.

    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of the form (service.namespace, namespace_config)"""
    rootdir = os.path.abspath(soa_dir)
    namespace_list = []
    for srv_dir in os.listdir(rootdir):
        namespace_list.extend(get_all_namespaces_for_service(srv_dir, soa_dir))
    return namespace_list


def get_app_id_and_task_uuid_from_executor_id(executor_id):
    """Parse the marathon executor ID and return the (app id, task uuid)"""
    return executor_id.rsplit('.', 1)


def marathon_services_running_here():
    """See what marathon services are being run by a mesos-slave on this host.
    :returns: A list of triples of (service, instance, port)"""
    slave_state = get_local_slave_state()
    frameworks = [fw for fw in slave_state.get('frameworks', []) if 'marathon' in fw['name']]
    executors = [ex for fw in frameworks for ex in fw.get('executors', [])
                 if u'TASK_RUNNING' in [t[u'state'] for t in ex.get('tasks', [])]]
    srv_list = []
    for executor in executors:
        app_id, task_uuid = get_app_id_and_task_uuid_from_executor_id(executor['id'])
        (srv_name, srv_instance, _, __) = deformat_job_id(app_id)
        srv_port = int(re.findall('[0-9]+', executor['resources']['ports'])[0])
        srv_list.append((srv_name, srv_instance, srv_port))
    return srv_list


def get_marathon_services_running_here_for_nerve(cluster, soa_dir):
    if not cluster:
        try:
            cluster = load_system_paasta_config().get_cluster()
        # In the cases where there is *no* cluster or in the case
        # where there isn't a Paasta configuration file at *all*, then
        # there must be no marathon services running here, so we catch
        # these custom exceptions and return [].
        except (PaastaNotConfiguredError):
            return []
    # When a cluster is defined in mesos, let's iterate through marathon services
    marathon_services = marathon_services_running_here()
    nerve_list = []
    for name, instance, port in marathon_services:
        try:
            namespace = read_namespace_for_service_instance(name, instance, cluster, soa_dir)
            nerve_dict = load_service_namespace_config(name, namespace, soa_dir)
            if not nerve_dict.is_in_smartstack():
                continue
            nerve_dict['port'] = port
            nerve_name = compose_job_id(name, namespace)
            nerve_list.append((nerve_name, nerve_dict))
        except KeyError:
            continue  # SOA configs got deleted for this app, it'll get cleaned up
    return nerve_list


def get_classic_services_that_run_here():
    # find all files in the PUPPET_SERVICE_DIR, but discard broken symlinks
    # this allows us to (de)register services on a machine by
    # breaking/healing a symlink placed by Puppet.
    puppet_service_dir_services = set()
    if os.path.exists(PUPPET_SERVICE_DIR):
        puppet_service_dir_services = {
            i for i in os.listdir(PUPPET_SERVICE_DIR) if
            os.path.exists(os.path.join(PUPPET_SERVICE_DIR, i))
        }

    return sorted(
        service_configuration_lib.services_that_run_here() |
        puppet_service_dir_services
    )


def get_classic_service_information_for_nerve(name, soa_dir):
    return _namespaced_get_classic_service_information_for_nerve(name, 'main', soa_dir)


def _namespaced_get_classic_service_information_for_nerve(name, namespace, soa_dir):
    nerve_dict = load_service_namespace_config(name, namespace, soa_dir)
    port_file = os.path.join(soa_dir, name, 'port')
    nerve_dict['port'] = service_configuration_lib.read_port(port_file)
    nerve_name = compose_job_id(name, namespace)
    return (nerve_name, nerve_dict)


def get_classic_services_running_here_for_nerve(soa_dir):
    classic_services = []
    for name in get_classic_services_that_run_here():
        for namespace in get_all_namespaces_for_service(name, soa_dir, full_name=False):
            classic_services.append(_namespaced_get_classic_service_information_for_nerve(
                name, namespace[0], soa_dir))
    return classic_services


def get_services_running_here_for_nerve(cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Get a list of ALL services running on this box, with returned information
    needed for nerve.

    ALL services means services that have a service.yaml with an entry for this host in
    runs_on, AND services that are currently deployed in a mesos-slave here via marathon.

    conf_dict is a dictionary possibly containing the same keys returned by
    load_service_namespace_config (in fact, that's what this calls).
    Some or none of those keys may not be present on a per-service basis.

    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of the form (service.namespace, service_config)
              AND (service, service_config) for legacy SOA services"""
    # All Legacy yelpsoa services are also announced
    return get_marathon_services_running_here_for_nerve(cluster, soa_dir) + \
        get_classic_services_running_here_for_nerve(soa_dir)


def list_all_marathon_app_ids(client):
    """List all marathon app_ids, regardless of state

    The raw marathon API returns app ids in their URL form, with leading '/'s
    conforming to the Application Group format:
    https://github.com/mesosphere/marathon/blob/master/docs/docs/application-groups.md

    This function wraps the full output of list_apps to return a list
    in the original form, without leading "/"'s.

    returns: List of app ids in the same format they are POSTed."""
    all_app_ids = [app.id for app in client.list_apps()]
    stripped_app_ids = [app_id.lstrip('/') for app_id in all_app_ids]
    return stripped_app_ids


def is_app_id_running(app_id, client):
    """Returns a boolean indicating if the app is in the current list
    of marathon apps

    :param app_id: The app_id to look for
    :param client: A MarathonClient object"""

    all_app_ids = list_all_marathon_app_ids(client)
    return app_id.lstrip('/') in all_app_ids


def app_has_tasks(client, app_id, expected_tasks, exact_matches_only=False):
    """ A predicate function indicating whether an app has launched *at least* expected_tasks
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
        print "no app with id %s found" % app_id
        raise
    print "app %s has %d of %d expected tasks" % (app_id, len(tasks), expected_tasks)
    if exact_matches_only:
        return len(tasks) == expected_tasks
    else:
        return len(tasks) >= expected_tasks


@timeout()
def wait_for_app_to_launch_tasks(client, app_id, expected_tasks, exact_matches_only=False):
    """ Wait for an app to have num_tasks tasks launched. If the app isn't found, then this will swallow the exception
    and retry. Times out after 30 seconds.

    :param client: The marathon client
    :param app_id: The app id to which the tasks belong
    :param expected_tasks: The number of tasks to wait for
    :param exact_matches_only: a boolean indicating whether we require exactly expected_tasks to be running
    """
    found = False
    while not found:
        try:
            found = app_has_tasks(client, app_id, expected_tasks, exact_matches_only)
        except NotFoundError:
            pass
        if found:
            return
        else:
            print "waiting for app %s to have %d tasks. retrying" % (app_id, expected_tasks)
            sleep(0.5)


def create_complete_config(service, instance, soa_dir=DEFAULT_SOA_DIR):
    """Generates a complete dictionary to be POST'ed to create an app on Marathon"""
    return load_marathon_service_config(
        service=service,
        instance=instance,
        cluster=load_system_paasta_config().get_cluster(),
        soa_dir=soa_dir,
    ).format_marathon_app_dict()


def get_expected_instance_count_for_namespace(service, namespace, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Get the number of expected instances for a namespace, based on the number
    of instances set to run on that namespace as specified in Marathon service
    configuration files.

    :param service: The service's name
    :param namespace: The namespace for that service to check
    :param soa_dir: The SOA configuration directory to read from
    :returns: An integer value of the # of expected instances for the namespace"""
    total_expected = 0
    if not cluster:
        cluster = load_system_paasta_config().get_cluster()
    for name, instance in get_service_instance_list(service,
                                                    cluster=cluster,
                                                    instance_type='marathon',
                                                    soa_dir=soa_dir):
        srv_config = load_marathon_service_config(name, instance, cluster, soa_dir=soa_dir)
        instance_ns = srv_config.get_nerve_namespace()
        if namespace == instance_ns:
            total_expected += srv_config.get_instances()
    return total_expected


def get_matching_appids(servicename, instance, client):
    """Returns a list of appids given a service and instance.
    Useful for fuzzy matching if you think there are marathon
    apps running but you don't know the full instance id"""
    return [app.id for app in get_matching_apps(servicename, instance, client)]


def get_matching_apps(servicename, instance, client, embed_failures=False):
    """Returns a list of appids given a service and instance.
    Useful for fuzzy matching if you think there are marathon
    apps running but you don't know the full instance id"""
    jobid = format_job_id(servicename, instance)
    expected_prefix = "/%s%s" % (jobid, MESOS_TASK_SPACER)
    return [app for app in client.list_apps(embed_failures=embed_failures) if app.id.startswith(expected_prefix)]


def get_healthcheck_for_instance(service, instance, service_manifest, random_port, soa_dir=DEFAULT_SOA_DIR):
    """
    Returns healthcheck for a given service instance in the form of a tuple (mode, healthcheck_command)
    or (None, None) if no healthcheck
    """
    smartstack_config = load_service_namespace_config(service, instance, soa_dir)
    mode = service_manifest.get_healthcheck_mode(smartstack_config)
    hostname = socket.getfqdn()

    if mode == "http":
        path = service_manifest.get_healthcheck_uri(smartstack_config)
        healthcheck_command = '%s://%s:%d%s' % (mode, hostname, random_port, path)
    elif mode == "tcp":
        healthcheck_command = '%s://%s:%d' % (mode, hostname, random_port)
    elif mode == 'cmd':
        healthcheck_command = service_manifest.get_healthcheck_cmd()
    else:
        mode = None
        healthcheck_command = None
    return (mode, healthcheck_command)


def kill_task(client, app_id, task_id, scale):
    """Wrapper to the official kill_task method that is tolerant of errors"""
    try:
        return client.kill_task(app_id=app_id, task_id=task_id, scale=True)
    except MarathonHttpError as e:
        # Marathon allows you to kill and scale in one action, but this is not
        # idempotent. If you kill&scale the same task ID twice, the number of instances
        # gets decremented twice. This can lead to a situation where kill&scaling the
        # last task decrements the number of instances below zero, causing a "Bean is not
        # valid" message.
        if e.error_message == 'Bean is not valid' and e.status_code == 422:
            log.debug("Probably tried to kill a task id that didn't exist. Continuing.")
            return []
        elif 'does not exist' in e.error_message and e.status_code == 404:
            log.debug("Probably tried to kill a task id that was already dead. Continuing.")
            return []
        else:
            raise


def kill_given_tasks(client, task_ids, scale):
    """Wrapper to the official kill_given_tasks method that is tolerant of errors"""
    try:
        return client.kill_given_tasks(task_ids=task_ids, scale=scale)
    except MarathonHttpError as e:
        # Marathon's interface is always async, so it is possible for you to see
        # a task in the interface and kill it, yet by the time it tries to kill
        # it, it is already gone. This is not really a failure condition, so we
        # swallow this error.
        if e.error_message == 'Bean is not valid' and e.status_code == 422:
            log.debug("Probably tried to kill a task id that didn't exist. Continuing.")
            return []
        else:
            raise


def compose_autoscaling_zookeeper_root(service, instance):
    return '/autoscaling/%s/%s' % (service, instance)


def set_instances_for_marathon_service(service, instance, instance_count, soa_dir=DEFAULT_SOA_DIR):
    zookeeper_path = '%s/instances' % compose_autoscaling_zookeeper_root(service, instance)
    with ZookeeperPool() as zookeeper_client:
        zookeeper_client.ensure_path(zookeeper_path)
        zookeeper_client.set(zookeeper_path, str(instance_count))


def get_instances_from_zookeeper(service, instance):
    with ZookeeperPool() as zookeeper_client:
        (instances, _) = zookeeper_client.get('%s/instances' % compose_autoscaling_zookeeper_root(service, instance))
        return int(instances)
