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
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import datetime
import json
import logging
import os
from math import ceil

import requests
import service_configuration_lib
from marathon import MarathonClient
from marathon import MarathonHttpError
from marathon import NotFoundError

from paasta_tools.long_running_service_tools import InvalidHealthcheckMode
from paasta_tools.long_running_service_tools import load_service_namespace_config
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.mesos.exceptions import NoSlavesAvailableError
from paasta_tools.mesos_tools import filter_mesos_slaves_by_blacklist
from paasta_tools.mesos_tools import get_mesos_network_for_net
from paasta_tools.mesos_tools import get_mesos_slaves_grouped_by_attribute
from paasta_tools.mesos_tools import mesos_services_running_here
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_code_sha_from_dockerurl
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import get_paasta_branch
from paasta_tools.utils import get_service_instance_list
from paasta_tools.utils import get_user_agent
from paasta_tools.utils import load_deployments_json
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import time_cache

# Marathon creates Mesos tasks with an id composed of the app's full name, a
# spacer, and a UUID. This variable is that spacer. Note that we don't control
# this spacer, i.e. you can't change it here and expect the world to change
# with you. We need to know what it is so we can decompose Mesos task ids.
MESOS_TASK_SPACER = '.'
PUPPET_SERVICE_DIR = '/etc/nerve/puppet_services.d'


# A set of config attributes that don't get included in the hash of the config.
# These should be things that PaaSTA/Marathon knows how to change without requiring a bounce.
CONFIG_HASH_BLACKLIST = {'instances', 'backoff_seconds', 'min_instances', 'max_instances'}

log = logging.getLogger(__name__)
logging.getLogger('marathon').setLevel(logging.WARNING)


def load_marathon_config():
    return MarathonConfig(load_system_paasta_config().get_marathon_config())


class MarathonNotConfigured(Exception):
    pass


class MarathonConfig(dict):

    def __init__(self, config):
        super(MarathonConfig, self).__init__(config)

    def get_url(self):
        """Get the Marathon API url

        :returns: The Marathon API endpoint"""
        try:
            return self['url']
        except KeyError:
            raise MarathonNotConfigured('Could not find marathon url in system marathon config')

    def get_username(self):
        """Get the Marathon API username

        :returns: The Marathon API username"""
        try:
            return self['user']
        except KeyError:
            raise MarathonNotConfigured('Could not find marathon user in system marathon config')

    def get_password(self):
        """Get the Marathon API password

        :returns: The Marathon API password"""
        try:
            return self['password']
        except KeyError:
            raise MarathonNotConfigured('Could not find marathon password in system marathon config')


def load_marathon_service_config_no_cache(service, instance, cluster, load_deployments=True, soa_dir=DEFAULT_SOA_DIR):
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
        soa_dir=soa_dir,
    )
    marathon_conf_file = "marathon-%s" % cluster
    log.info("Reading marathon configuration file: %s.yaml", marathon_conf_file)
    instance_configs = service_configuration_lib.read_extra_service_information(
        service,
        marathon_conf_file,
        soa_dir=soa_dir,
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
        soa_dir=soa_dir,
    )


@time_cache(ttl=5)
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
    return load_marathon_service_config_no_cache(service=service,
                                                 instance=instance,
                                                 cluster=cluster,
                                                 load_deployments=load_deployments,
                                                 soa_dir=soa_dir)


class InvalidMarathonConfig(Exception):
    pass


class MarathonServiceConfig(LongRunningServiceConfig):

    def __init__(self, service, cluster, instance, config_dict, branch_dict, soa_dir=DEFAULT_SOA_DIR):
        super(MarathonServiceConfig, self).__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
            soa_dir=soa_dir,
        )

    def __repr__(self):
        return "MarathonServiceConfig(%r, %r, %r, %r, %r, %r)" % (
            self.service,
            self.cluster,
            self.instance,
            self.config_dict,
            self.branch_dict,
            self.soa_dir
        )

    def copy(self):
        return self.__class__(
            service=self.service,
            instance=self.instance,
            cluster=self.cluster,
            config_dict=dict(self.config_dict),
            branch_dict=dict(self.branch_dict),
            soa_dir=self.soa_dir
        )

    def get_autoscaling_params(self):
        default_params = {
            'metrics_provider': 'mesos_cpu',
            'decision_policy': 'pid',
            'setpoint': 0.8,
        }
        return deep_merge_dictionaries(overrides=self.config_dict.get('autoscaling', {}), defaults=default_params)

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

    def get_backoff_factor(self):
        return self.config_dict.get('backoff_factor', 2)

    def get_max_launch_delay_seconds(self):
        return self.config_dict.get('max_launch_delay_seconds', 300)

    def get_bounce_method(self):
        """Get the bounce method specified in the service's marathon configuration.

        :param service_config: The service instance's configuration dictionary
        :returns: The bounce method specified in the config, or 'crossover' if not specified"""
        return self.config_dict.get('bounce_method', 'crossover')

    def get_calculated_constraints(self, system_paasta_config, service_namespace_config):
        """Gets the calculated constraints for a marathon instance

        If ``constraints`` is specified in the config, it will use that regardless.
        Otherwise it will calculate a good set of constraints from other inputs,
        like ``pool``, blacklist/whitelist, smartstack data, etc.

        :param service_namespace_config: The service instance's configuration dictionary
        :returns: The constraints specified in the config, or defaults described above
        """
        constraints = self.get_constraints()
        blacklist = self.get_deploy_blacklist(
            system_deploy_blacklist=system_paasta_config.get_deploy_blacklist()
        )
        whitelist = self.get_deploy_whitelist(
            system_deploy_whitelist=system_paasta_config.get_deploy_whitelist()
        )
        if constraints is not None:
            return constraints
        else:
            constraints = self.get_extra_constraints()
            constraints.extend(self.get_routing_constraints(
                service_namespace_config=service_namespace_config,
                system_paasta_config=system_paasta_config,
            ))
            constraints.extend(self.get_deploy_constraints(blacklist, whitelist))
            constraints.extend(self.get_pool_constraints())
        return [[str(val) for val in constraint] for constraint in constraints]

    def get_routing_constraints(self, service_namespace_config, system_paasta_config):
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
            blacklist=self.get_deploy_blacklist(
                system_deploy_blacklist=system_paasta_config.get_deploy_blacklist()
            ),
            whitelist=self.get_deploy_whitelist(
                system_deploy_whitelist=system_paasta_config.get_deploy_whitelist()
            ),
        )
        if not filtered_slaves:
            raise NoSlavesAvailableError(
                (
                    "We do not believe any slaves on the cluster will match the constraints for %s.%s. If you believe "
                    "this is incorrect, have your system administrator adjust the value of expected_slave_attributes "
                    "in the system paasta configs."
                ) % (self.service, self.instance)
            )

        value_dict = get_mesos_slaves_grouped_by_attribute(
            filtered_slaves,
            discover_level
        )
        routing_constraints = [[discover_level, "GROUP_BY", str(len(value_dict.keys()))]]
        return routing_constraints

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

        system_paasta_config = load_system_paasta_config()
        docker_url = self.get_docker_url()
        service_namespace_config = load_service_namespace_config(
            service=self.service,
            namespace=self.get_nerve_namespace(),
        )
        docker_volumes = self.get_volumes(system_volumes=system_paasta_config.get_volumes())

        net = get_mesos_network_for_net(self.get_net())

        complete_config = {
            'container': {
                'docker': {
                    'image': docker_url,
                    'network': net,
                    "parameters": self.format_docker_parameters(),
                },
                'type': 'DOCKER',
                'volumes': docker_volumes,
            },
            'uris': [system_paasta_config.get_dockercfg_location(), ],
            'backoff_seconds': self.get_backoff_seconds(),
            'backoff_factor': self.get_backoff_factor(),
            'max_launch_delay_seconds': self.get_max_launch_delay_seconds(),
            'health_checks': self.get_healthchecks(service_namespace_config),
            'env': self.get_env(),
            'mem': float(self.get_mem()),
            'cpus': float(self.get_cpus()),
            'disk': float(self.get_disk()),
            'constraints': self.get_calculated_constraints(
                system_paasta_config=system_paasta_config,
                service_namespace_config=service_namespace_config
            ),
            'instances': self.get_desired_instances(),
            'cmd': self.get_cmd(),
            'args': self.get_args(),
        }

        if net == 'BRIDGE':
            complete_config['container']['docker']['portMappings'] = [
                {
                    'containerPort': self.get_container_port(),
                    'hostPort': self.get_host_port(),
                    'protocol': 'tcp',
                },
            ]
        else:
            complete_config['port_definitions'] = [
                {
                    'port': self.get_host_port(),
                    'protocol': 'tcp',
                },
            ]
            # Without this, we may end up with multiple containers requiring the same port on the same box.
            complete_config['require_ports'] = (self.get_host_port() != 0)

        accepted_resource_roles = self.get_accepted_resource_roles()
        if accepted_resource_roles is not None:
            complete_config['accepted_resource_roles'] = accepted_resource_roles

        code_sha = get_code_sha_from_dockerurl(docker_url)

        config_hash = get_config_hash(
            self.sanitize_for_config_hash(complete_config),
            force_bounce=self.get_force_bounce(),
        )
        complete_config['id'] = format_job_id(self.service, self.instance, code_sha, config_hash)

        log.debug("Complete configuration for instance is: %s", complete_config)
        return complete_config

    def sanitize_for_config_hash(self, config):
        """Removes some data from complete_config to make it suitable for
        calculation of config hash.

        :param config: complete_config hash to sanitize
        :returns: sanitized copy of complete_config hash
        """
        ahash = {key: copy.deepcopy(value) for key, value in config.items() if key not in CONFIG_HASH_BLACKLIST}
        ahash['container']['docker']['parameters'] = self.format_docker_parameters(with_labels=False)
        return ahash

    def get_healthchecks(self, service_namespace_config):
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
            raise InvalidHealthcheckMode(
                "Unknown mode: %s. Only acceptable healthcheck modes are http/tcp/cmd" % mode)
        return healthchecks

    def get_bounce_health_params(self, service_namespace_config):
        default = {}
        if service_namespace_config.is_in_smartstack():
            default = {'check_haproxy': True}
        return self.config_dict.get('bounce_health_params', default)

    def get_bounce_margin_factor(self):
        return self.config_dict.get('bounce_margin_factor', 1.0)

    def get_accepted_resource_roles(self):
        return self.config_dict.get('accepted_resource_roles', None)

    def get_replication_crit_percentage(self):
        return self.config_dict.get('replication_threshold', 50)

    def get_host_port(self):
        '''Map this port on the host to your container's port 8888. Default is 0, which means Marathon picks a port.'''
        return self.config_dict.get('host_port', 0)


class MarathonDeployStatus:
    """ An enum to represent marathon app deploy status.
    Changing name of the keys will affect both the paasta CLI and API.
    """
    Running, Deploying, Stopped, Delayed, Waiting, NotRunning = range(0, 6)

    @classmethod
    def tostring(cls, val):
        for k, v in vars(cls).items():
            if v == val:
                return k

    @classmethod
    def fromstring(cls, str):
        return getattr(cls, str, None)


def get_marathon_app_deploy_status(client, app_id):

    if is_app_id_running(app_id, client):
        app = client.get_app(app_id)
    else:
        return MarathonDeployStatus.NotRunning

    # Check the launch queue to see if an app is blocked
    is_overdue, backoff_seconds = get_app_queue_status(client, app_id)

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
    def list_apps(self, *args, **kwargs):
        return super(CachedMarathonClient, self).list_apps(*args, **kwargs)


def get_marathon_client(url, user, passwd, cached=False):
    """Get a new marathon client connection in the form of a MarathonClient object.

    :param url: The url to connect to marathon at
    :param user: The username to connect with
    :param passwd: The password to connect with
    :param cached: If true, return CachedMarathonClient
    :returns: A new marathon.MarathonClient object"""
    log.info("Connecting to Marathon server at: %s", url)

    session = requests.Session()
    session.headers.update({'User-Agent': get_user_agent()})

    if cached:
        return CachedMarathonClient(url, user, passwd, timeout=30, session=session)
    else:
        return MarathonClient(url, user, passwd, timeout=30, session=session)


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


def read_all_registrations_for_service_instance(service, instance, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Retreive all registrations as fully specified name.instance pairs
    for a particular service instance.

    For example, the 'main' paasta instance of the 'test' service may register
    in the 'test.main' namespace as well as the 'other_svc.main' namespace.

    If one is not defined in the config file, returns a list containing
    name.instance instead.
    """
    if not cluster:
        cluster = load_system_paasta_config().get_cluster()

    marathon_service_config = load_marathon_service_config(
        service, instance, cluster, load_deployments=False, soa_dir=soa_dir
    )
    return marathon_service_config.get_registrations()


def read_registration_for_service_instance(service, instance, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Retreive a service instance's primary registration for a particular
    service instance.

    This is the service and namespace that clients ought talk to, as well as
    paasta ought monitor. In this context "primary" just means the first one
    in the list of registrations.

    If registrations are not defined in the marathon config file, returns
    name.instance instead.

    :returns a fully qualified service.instance registration
    """
    return read_all_registrations_for_service_instance(
        service, instance, cluster, soa_dir
    )[0]


def get_proxy_port_for_instance(name, instance, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Get the proxy_port defined in the first namespace configuration for a
    service instance.

    This means that the namespace first has to be loaded from the service instance's
    configuration, and then the proxy_port has to loaded from the smartstack configuration
    for that namespace.

    :param name: The service name
    :param instance: The instance of the service
    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: The proxy_port for the service instance, or None if not defined"""
    registration = read_registration_for_service_instance(name, instance, cluster, soa_dir)
    service, namespace, _, __ = decompose_job_id(registration)
    nerve_dict = load_service_namespace_config(
        service=service, namespace=namespace, soa_dir=soa_dir)
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


def parse_service_instance_from_executor_id(task_id):
    app_id, task_uuid = get_app_id_and_task_uuid_from_executor_id(task_id)
    (srv_name, srv_instance, _, __) = deformat_job_id(app_id)
    return srv_name, srv_instance


def marathon_services_running_here():
    """See what marathon services are being run by a mesos-slave on this host.
    :returns: A list of triples of (service, instance, port)"""

    return mesos_services_running_here(
        framework_filter=lambda fw: fw['name'].startswith('marathon'),
        parse_service_instance_from_executor_id=parse_service_instance_from_executor_id,
    )


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
            registrations = read_all_registrations_for_service_instance(
                name, instance, cluster, soa_dir
            )
            for registration in registrations:
                reg_service, reg_namespace, _, __ = decompose_job_id(registration)
                nerve_dict = load_service_namespace_config(
                    service=reg_service, namespace=reg_namespace, soa_dir=soa_dir,
                )
                if not nerve_dict.is_in_smartstack():
                    continue
                nerve_dict['port'] = port
                nerve_list.append((registration, nerve_dict))
        except KeyError:
            continue  # SOA configs got deleted for this app, it'll get cleaned up
    return nerve_list


def get_puppet_services_that_run_here():
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
                puppet_service_dir_services[service_name] = puppet_service_data['namespaces']

    return puppet_service_dir_services


def get_puppet_services_running_here_for_nerve(soa_dir):
    puppet_services = []
    for service, namespaces in sorted(get_puppet_services_that_run_here().items()):
        for namespace in namespaces:
            puppet_services.append(
                _namespaced_get_classic_service_information_for_nerve(
                    service, namespace, soa_dir)
            )
    return puppet_services


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
    classic_services_here = service_configuration_lib.services_that_run_here()
    for service in sorted(classic_services_here):
        namespaces = [x[0] for x in get_all_namespaces_for_service(
            service, soa_dir, full_name=False)]
        for namespace in namespaces:
            classic_services.append(
                _namespaced_get_classic_service_information_for_nerve(
                    service, namespace, soa_dir)
            )
    return classic_services


def list_all_marathon_app_ids(client):
    """List all marathon app_ids, regardless of state

    The raw marathon API returns app ids in their URL form, with leading '/'s
    conforming to the Application Group format:
    https://github.com/mesosphere/marathon/blob/master/docs/docs/application-groups.md

    This function wraps the full output of list_apps to return a list
    in the original form, without leading "/"'s.

    returns: List of app ids in the same format they are POSTed."""
    return [app.id.lstrip('/') for app in get_all_marathon_apps(client)]


def is_app_id_running(app_id, client):
    """Returns a boolean indicating if the app is in the current list
    of marathon apps

    :param app_id: The app_id to look for
    :param client: A MarathonClient object"""

    all_app_ids = list_all_marathon_app_ids(client)
    return app_id.lstrip('/') in all_app_ids


def app_has_tasks(client, app_id, expected_tasks, exact_matches_only=False):
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
        paasta_print("no app with id %s found" % app_id)
        raise
    paasta_print("app %s has %d of %d expected tasks" % (app_id, len(tasks), expected_tasks))
    if exact_matches_only:
        return len(tasks) == expected_tasks
    else:
        return len(tasks) >= expected_tasks


def get_app_queue_status(client, app_id):
    """Returns the status of an application if it exists in Marathon's launch queue

    :param client: The marathon client
    :param app_id: The Marathon app id (without the leading /)
    :returns: A tuple of the form (is_overdue, current_backoff_delay) or (None, None)
              if the app cannot be found. If is_overdue is True, then Marathon has
              not received a resource offer that satisfies the requirements for the app
    """
    app_id = "/%s" % app_id
    app_queue = client.list_queue()
    for app_queue_item in app_queue:
        if app_queue_item.app.id == app_id:
            return (app_queue_item.delay.overdue, app_queue_item.delay.time_left_seconds)

    return (None, None)


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
    marathon_apps = get_all_marathon_apps(client)
    return [app.id for app in get_matching_apps(servicename, instance, marathon_apps)]


def get_matching_apps(servicename, instance, marathon_apps):
    """Returns a list of appids given a service and instance.
    Useful for fuzzy matching if you think there are marathon
    apps running but you don't know the full instance id"""
    jobid = format_job_id(servicename, instance)
    expected_prefix = "/%s%s" % (jobid, MESOS_TASK_SPACER)
    return [app for app in marathon_apps if app.id.startswith(expected_prefix)]


def get_all_marathon_apps(client, embed_failures=False):
    return client.list_apps(embed_failures=embed_failures)


def kill_task(client, app_id, task_id, scale):
    """Wrapper to the official kill_task method that is tolerant of errors"""
    try:
        return client.kill_task(app_id=app_id, task_id=task_id, scale=True)
    except MarathonHttpError as e:
        # Marathon allows you to kill and scale in one action, but this is not
        # idempotent. If you kill&scale the same task ID twice, the number of instances
        # gets decremented twice. This can lead to a situation where kill&scaling the
        # last task decrements the number of instances below zero, causing an "Object is not
        # valid" message or a "Bean is not valid" message.
        if 'is not valid' in e.error_message and e.status_code == 422:
            log.warning("Got 'is not valid' when killing task %s. Continuing anyway." % task_id)
            return []
        elif 'does not exist' in e.error_message and e.status_code == 404:
            log.warning("Got 'does not exist' when killing task %s. Continuing anyway." % task_id)
            return []
        else:
            raise


def kill_given_tasks(client, task_ids, scale):
    """Wrapper to the official kill_given_tasks method that is tolerant of errors"""
    if not task_ids:
        log.debug("No task_ids specified, not killing any tasks")
        return []
    try:
        return client.kill_given_tasks(task_ids=task_ids, scale=scale, force=True)
    except MarathonHttpError as e:
        # Marathon's interface is always async, so it is possible for you to see
        # a task in the interface and kill it, yet by the time it tries to kill
        # it, it is already gone. This is not really a failure condition, so we
        # swallow this error.
        if 'is not valid' in e.error_message and e.status_code == 422:
            log.debug("Probably tried to kill a task id that didn't exist. Continuing.")
            return []
        else:
            raise


def is_task_healthy(task, require_all=True, default_healthy=False):
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


def is_old_task_missing_healthchecks(task, marathon_client):
    """We check this because versions of Marathon (at least up to 1.1)
    sometimes stop healthchecking tasks, leaving no results. We can normally
    assume that an "old" task which has no healthcheck results is still up
    and healthy but marathon has simply decided to stop healthchecking it.
    """
    health_checks = marathon_client.get_app(task.app_id).health_checks
    if not task.health_check_results and health_checks and task.started_at:
        healthcheck_startup_time = datetime.timedelta(seconds=health_checks[0].grace_period_seconds) + \
            datetime.timedelta(seconds=health_checks[0].interval_seconds * 5)
        is_task_old = task.started_at + healthcheck_startup_time < datetime.datetime.now()
        return is_task_old
    return False


def get_num_at_risk_tasks(app, draining_hosts):
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
