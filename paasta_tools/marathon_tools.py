"""
This module contains the meat of the logic for most of the scripts
that interact with marathon. There's config parsers, url composers,
and a number of other things used by other components in order to
make the PaaSTA stack work.
"""
import logging
import os
import pipes
import re
import requests
import socket
import glob
from time import sleep

from marathon import MarathonClient
from marathon import NotFoundError
import json
import service_configuration_lib

from paasta_tools.mesos_tools import fetch_local_slave_state
from paasta_tools.mesos_tools import get_mesos_slaves_grouped_by_attribute
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import list_all_clusters
from paasta_tools.utils import load_deployments_json
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import get_code_sha_from_dockerurl
from paasta_tools.utils import get_default_branch
from paasta_tools.utils import get_docker_url
from paasta_tools.utils import NoMarathonClusterFoundException
from paasta_tools.utils import PaastaNotConfigured
from paasta_tools.utils import PATH_TO_SYSTEM_PAASTA_CONFIG_DIR
from paasta_tools.utils import timeout

# DO NOT CHANGE ID_SPACER, UNLESS YOU'RE PREPARED TO CHANGE ALL INSTANCES
# OF IT IN OTHER LIBRARIES (i.e. service_configuration_lib).
# It's used to compose a job's full ID from its name and instance
ID_SPACER = '.'
MY_HOSTNAME = socket.getfqdn()
MESOS_MASTER_PORT = 5050
CONTAINER_PORT = 8888
DEFAULT_SOA_DIR = service_configuration_lib.DEFAULT_SOA_DIR
log = logging.getLogger('__main__')

logging.getLogger('marathon').setLevel(logging.WARNING)

PATH_TO_MARATHON_CONFIG = os.path.join(PATH_TO_SYSTEM_PAASTA_CONFIG_DIR, 'marathon.json')
PUPPET_SERVICE_DIR = '/etc/nerve/puppet_services.d'


def load_marathon_config(path=PATH_TO_MARATHON_CONFIG):
    try:
        with open(path) as f:
            return MarathonConfig(json.load(f), path)
    except IOError as e:
        raise PaastaNotConfigured("Could not load marathon config file %s: %s" % (e.filename, e.strerror))


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


def load_marathon_service_config(service_name, instance, cluster, load_deployments=True, soa_dir=DEFAULT_SOA_DIR):
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
    log.info("Reading service configuration files from dir %s/ in %s" % (service_name, soa_dir))
    log.info("Reading general configuration file: service.yaml")
    general_config = service_configuration_lib.read_service_configuration(
        service_name,
        soa_dir=soa_dir
    )
    marathon_conf_file = "marathon-%s" % cluster
    log.info("Reading marathon configuration file: %s.yaml", marathon_conf_file)
    instance_configs = service_configuration_lib.read_extra_service_information(
        service_name,
        marathon_conf_file,
        soa_dir=soa_dir
    )

    if instance not in instance_configs:
        raise NoMarathonConfigurationForService(
            "%s not found in config file %s/%s/%s.yaml." % (instance, soa_dir, service_name, marathon_conf_file)
        )

    general_config.update(instance_configs[instance])

    branch_dict = {}
    if load_deployments:
        deployments_json = load_deployments_json(service_name, soa_dir=soa_dir)
        branch = general_config.get('branch', get_default_branch(cluster, instance))
        branch_dict = deployments_json.get_branch_dict(service_name, branch)

    return MarathonServiceConfig(
        service_name,
        instance,
        general_config,
        branch_dict,
    )


class InvalidMarathonConfig(Exception):
    pass


class MarathonServiceConfig(InstanceConfig):

    def __init__(self, service_name, instance, config_dict, branch_dict):
        super(MarathonServiceConfig, self).__init__(config_dict, branch_dict)
        self.service_name = service_name
        self.instance = instance
        self.config_dict = config_dict
        self.branch_dict = branch_dict

    def __repr__(self):
        return "MarathonServiceConfig(%r, %r, %r, %r)" % (
            self.service_name,
            self.instance,
            self.config_dict,
            self.branch_dict
        )

    def copy(self):
        return self.__class__(self.service_name, self.instance, dict(self.config_dict), dict(self.branch_dict))

    def get_instances(self):
        """Get the number of instances specified in the service's marathon configuration.

        Defaults to 0 if not specified in the config.

        :param service_config: The service instance's configuration dictionary
        :returns: The number of instances specified in the config, 0 if not
                  specified or if desired_state is not 'start'."""
        if self.get_desired_state() == 'start':
            instances = self.config_dict.get('instances', 1)
            return int(instances)
        else:
            return 0

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

    def get_constraints(self, service_namespace_config):
        """Gets the constraints specified in the service's marathon configuration.

        These are Marathon job constraints. See
        https://github.com/mesosphere/marathon/wiki/Constraints

        Defaults to `GROUP_BY region`. If the service's smartstack configuration
        specifies a `discover` key, then defaults to `GROUP_BY <value of discover>` instead.

        :param service_namespace_config: The service instance's configuration dictionary
        :returns: The constraints specified in the config, or defaults described above
        """
        if 'constraints' in self.config_dict:
            return self.config_dict.get('constraints')
        else:
            discover_level = service_namespace_config.get_discover()
            locations = get_mesos_slaves_grouped_by_attribute(discover_level)
            return [[discover_level, "GROUP_BY", str(len(locations))]]

    def format_marathon_app_dict(self, job_id, docker_url, docker_volumes, service_namespace_config):
        """Create the configuration that will be passed to the Marathon REST API.

        Currently compiles the following keys into one nice dict:

        - id: the ID of the image in Marathon
        - container: a dict containing the docker url and docker launch options. Needed by deimos.
        - uris: blank.
        - ports: an array containing the port.
        - env: environment variables for the container.
        - mem: the amount of memory required.
        - cpus: the number of cpus required.
        - constraints: the constraints on the Marathon job.
        - instances: the number of instances required.
        - cmd: the command to be executed.
        - args: an alternative to cmd that requires the docker container to have an entrypoint.

        The last 7 keys are retrieved using the get_<key> functions defined above.

        :param job_id: The job/app id name
        :param docker_url: The url to the docker image the job will actually execute
        :param docker_volumes: The docker volumes to run the image with, via the
                               marathon configuration file
        :param service_namespace_config: The service instance's configuration dict
        :returns: A dict containing all of the keys listed above"""
        complete_config = {
            'id': job_id,
            'container': {
                'docker': {
                    'image': docker_url,
                    'network': 'BRIDGE',
                    'portMappings': [
                        {
                            'containerPort': CONTAINER_PORT,
                            'hostPort': 0,
                            'protocol': 'tcp',
                        },
                    ],
                },
                'type': 'DOCKER',
                'volumes': docker_volumes,
            },
            'uris': ['file:///root/.dockercfg', ],
            'backoff_seconds': 1,
            'backoff_factor': 2,
            'health_checks': self.get_healthchecks(service_namespace_config),
            'env': self.get_env(),
            'mem': float(self.get_mem()),
            'cpus': float(self.get_cpus()),
            'constraints': self.get_constraints(service_namespace_config),
            'instances': self.get_instances(),
            'cmd': self.get_cmd(),
            'args': self.get_args(),
        }
        log.info("Complete configuration for instance is: %s", complete_config)
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
            command = pipes.quote(self.get_healthcheck_cmd())
            hc_command = "paasta_execute_docker_command " \
                "--mesos-id \"$MESOS_TASK_ID\" --cmd %s --timeout '%s'" % (command, timeoutseconds)

            healthchecks = [
                {
                    "protocol": "COMMAND",
                    "command": {"value": hc_command},
                    "gracePeriodSeconds": graceperiodseconds,
                    "intervalSeconds": intervalseconds,
                    "timeoutSeconds": timeoutseconds,
                    "maxConsecutiveFailures": maxconsecutivefailures
                },
            ]
        elif mode is None:
            healthchecks = []
        else:
            raise InvalidSmartstackMode("Unknown mode: %s" % mode)
        return healthchecks

    def get_healthcheck_uri(self, service_namespace_config):
        return self.config_dict.get('healthcheck_uri', service_namespace_config.get_healthcheck_uri())

    def get_healthcheck_cmd(self):
        return self.config_dict.get('healthcheck_cmd', '/bin/true')

    def get_healthcheck_mode(self, service_namespace_config):
        mode = self.config_dict.get('healthcheck_mode', None)
        if mode is None:
            mode = service_namespace_config.get_mode()
        elif mode not in ['http', 'tcp', 'cmd']:
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
        return self.config_dict.get('healthcheck_max_consecutive_failures', 6)

    def get_nerve_namespace(self):
        return self.config_dict.get('nerve_ns', self.instance)

    def get_bounce_health_params(self, service_namespace_config):
        default = {}
        if service_namespace_config.is_in_smartstack():
            default = {'check_haproxy': True}
        return self.config_dict.get('bounce_health_params', default)


def load_service_namespace_config(srv_name, namespace, soa_dir=DEFAULT_SOA_DIR):
    """Attempt to read the configuration for a service's namespace in a more strict fashion.

    Retrevies the following keys:

    - proxy_port: the proxy port defined for the given namespace
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
    - advertise_extra: a list of tuples of (source, destination)
      e.g. [('region:dc6-prod', 'region:useast1-prod')]

    :param srv_name: The service name
    :param namespace: The namespace to read
    :param soa_dir: The SOA config directory to read from
    :returns: A dict of the above keys, if they were defined
    """

    service_config = service_configuration_lib.read_service_configuration(srv_name, soa_dir)
    smartstack_config = service_config.get('smartstack', {})
    namespace_config_from_file = smartstack_config.get(namespace, {})

    service_namespace_config = ServiceNamespaceConfig()
    # We can't really use .get, as we don't want the key to be in the returned
    # dict at all if it doesn't exist in the config file.
    # We also can't just copy the whole dict, as we only care about some keys
    # and there's other things that appear in the smartstack section in
    # several cases.
    key_whitelist = set([
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


class NoMarathonConfigurationForService(Exception):
    pass


def get_cluster():
    return load_system_paasta_config().get_cluster()


def get_marathon_client(url, user, passwd):
    """Get a new marathon client connection in the form of a MarathonClient object.

    :param url: The url to connect to marathon at
    :param user: The username to connect with
    :param passwd: The password to connect with
    :returns: A new marathon.MarathonClient object"""
    log.info("Connecting to Marathon server at: %s", url)
    return MarathonClient(url, user, passwd, timeout=30)


def compose_job_id(name, instance, tag=None):
    """Compose a marathon job/app id.

    :param name: The name of the service
    :param instance: The instance of the service
    :param tag: A hash or tag to append to the end of the id to make it unique
    :returns: <name>.<instance> if no tag, or <name>.<instance>.<tag> if tag given"""
    name = str(name).replace('_', '--')
    instance = str(instance).replace('_', '--')
    composed = '%s%s%s' % (name, ID_SPACER, instance)
    if tag:
        tag = str(tag).replace('_', '--')
        composed = '%s%s%s' % (composed, ID_SPACER, tag)
    return composed


def remove_tag_from_job_id(job_id):
    """Remove the tag from a job id, if there is one.

    :param job_id: The job_id.
    :returns: The job_id with the tag removed, if there was one."""
    return '%s%s%s' % (job_id.split(ID_SPACER)[0], ID_SPACER, job_id.split(ID_SPACER)[1])


def read_namespace_for_service_instance(name, instance, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Retreive a service instance's nerve namespace from its configuration file.
    If one is not defined in the config file, returns instance instead."""
    if not cluster:
        cluster = get_cluster()
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
    if not cluster:
        cluster = get_cluster()
    namespace = read_namespace_for_service_instance(name, instance, cluster, soa_dir)
    nerve_dict = load_service_namespace_config(name, namespace, soa_dir)
    return nerve_dict.get('proxy_port')


def list_clusters(service=None, soa_dir=DEFAULT_SOA_DIR):
    """Returns a sorted list of all clusters that appear to be in use. This
    is useful for cli tools.

    :param service: Optional. If provided will only list clusters that
                    the particular service is using
    """
    clusters = set()
    if service is None:
        clusters = list_all_clusters()
    else:
        clusters = set(get_clusters_deployed_to(service))
    return sorted(clusters)


def get_clusters_deployed_to(service, soa_dir=DEFAULT_SOA_DIR):
    """Looks at the clusters that a service is probably deployed to
    by looking at ``marathon-*.yaml``'s and returns a sorted list of clusters.
    """
    clusters = set()
    srv_path = os.path.join(soa_dir, service)
    if os.path.isdir(srv_path):
        marathon_files = "%s/marathon-*.yaml" % srv_path
        for marathon_file in glob.glob(marathon_files):
            basename = os.path.basename(marathon_file)
            cluster_re_match = re.search('marathon-([0-9a-z-]*).yaml', basename)
            if cluster_re_match is not None:
                clusters.add(cluster_re_match.group(1))
    return sorted(clusters)


def get_default_cluster_for_service(service_name):
    cluster = None
    try:
        cluster = load_system_paasta_config().get_cluster()
    except NoMarathonClusterFoundException:
        clusters_deployed_to = get_clusters_deployed_to(service_name)
        if len(clusters_deployed_to) > 0:
            cluster = clusters_deployed_to[0]
        else:
            raise NoMarathonConfigurationForService("No cluster configuration found for service %s" % service_name)
    return cluster


def list_all_marathon_instances_for_service(service):
    instances = set()
    for cluster in list_clusters(service):
        for service_instance in get_service_instance_list(service, cluster):
            instances.add(service_instance[1])
    return instances


def get_service_instance_list(name, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Enumerate the marathon instances defined for a service as a list of tuples.

    :param name: The service name
    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (name, instance) for each instance defined for the service name"""
    if not cluster:
        cluster = get_cluster()
    marathon_conf_file = "marathon-%s" % cluster
    log.info("Enumerating all instances for config file: %s/*/%s.yaml", soa_dir, marathon_conf_file)
    instances = service_configuration_lib.read_extra_service_information(
        name,
        marathon_conf_file,
        soa_dir=soa_dir
    )
    instance_list = []
    for instance in instances:
        instance_list.append((name, instance))
    log.debug("Enumerated the following instances: %s", instance_list)
    return instance_list


def get_marathon_services_for_cluster(cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Retrieve all marathon services and instances defined to run in a cluster.

    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (service_name, instance_name)"""
    if not cluster:
        cluster = get_cluster()
    rootdir = os.path.abspath(soa_dir)
    log.info("Retrieving all service instance names from %s for cluster %s", rootdir, cluster)
    instance_list = []
    for srv_dir in os.listdir(rootdir):
        instance_list += get_service_instance_list(srv_dir, cluster, soa_dir)
    return instance_list


def get_all_namespaces_for_service(service_name, soa_dir=DEFAULT_SOA_DIR, full_name=True):
    """Get all the smartstack namespaces listed for a given service name.

    :param service_name: The service name
    :param soa_dir: The SOA config directory to read from
    :param full_name: A boolean indicating if the service name should be prepended to the namespace in the
                      returned tuples as described below (Default: True)
    :returns: A list of tuples of the form (service_name.namespace, namespace_config) if full_name is true,
              otherwise of the form (namespace, namespace_config)
    """
    service_config = service_configuration_lib.read_service_configuration(service_name, soa_dir)
    smartstack = service_config.get('smartstack', {})
    namespace_list = []
    for namespace in smartstack:
        if full_name:
            name = '%s%s%s' % (service_name, ID_SPACER, namespace)
        else:
            name = namespace
        namespace_list.append((name, smartstack[namespace]))
    return namespace_list


def get_all_namespaces(soa_dir=DEFAULT_SOA_DIR):
    """Get all the smartstack namespaces across all services.
    This is mostly so synapse can get everything it needs in one call.

    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of the form (service_name.namespace, namespace_config)"""
    rootdir = os.path.abspath(soa_dir)
    namespace_list = []
    for srv_dir in os.listdir(rootdir):
        namespace_list += get_all_namespaces_for_service(srv_dir, soa_dir)
    return namespace_list


def marathon_services_running_here():
    """See what marathon services are being run by a mesos-slave on this host.
    :returns: A list of triples of (service_name, instance_name, port)"""
    slave_state = fetch_local_slave_state()
    frameworks = [fw for fw in slave_state.get('frameworks', []) if 'marathon' in fw['name']]
    executors = [ex for fw in frameworks for ex in fw.get('executors', [])
                 if u'TASK_RUNNING' in [t[u'state'] for t in ex.get('tasks', [])]]
    srv_list = []
    for executor in executors:
        srv_name = executor['id'].split(ID_SPACER)[0].replace('--', '_')
        srv_instance = executor['id'].split(ID_SPACER)[1].replace('--', '_')
        srv_port = int(re.findall('[0-9]+', executor['resources']['ports'])[0])
        srv_list.append((srv_name, srv_instance, srv_port))
    return srv_list


def get_marathon_services_running_here_for_nerve(cluster, soa_dir):
    if not cluster:
        try:
            cluster = get_cluster()
        # In the cases where there is *no* cluster or in the case
        # where there isn't a Paasta configuration file at *all*, then
        # there must be no marathon services running here, so we catch
        # these custom exceptions and return [].
        except (NoMarathonClusterFoundException, PaastaNotConfigured):
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
            nerve_name = '%s%s%s' % (name, ID_SPACER, namespace)
            nerve_list.append((nerve_name, nerve_dict))
        except KeyError:
            continue  # SOA configs got deleted for this job, it'll get cleaned up
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
    nerve_name = '%s%s%s' % (name, ID_SPACER, namespace)
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
    :returns: A list of tuples of the form (service_name.namespace, service_config)
              AND (service_name, service_config) for legacy SOA services"""
    # All Legacy yelpsoa services are also announced
    return get_marathon_services_running_here_for_nerve(cluster, soa_dir) + \
        get_classic_services_running_here_for_nerve(soa_dir)


class MesosMasterConnectionException(Exception):
    pass


def get_mesos_leader(hostname=MY_HOSTNAME):
    """Get the current mesos-master leader's hostname. Raise
    MesosMasterConnectionException if we can't connect.

    :param hostname: The hostname to query mesos-master on
    :returns: The current mesos-master hostname"""
    redirect_url = 'http://%s:%s/redirect' % (hostname, MESOS_MASTER_PORT)
    try:
        r = requests.get(redirect_url, timeout=10)
    except requests.exceptions.ConnectionError as e:
        # Repackage the exception so upstream code can handle this case without
        # knowing our implementation details.
        raise MesosMasterConnectionException(repr(e))
    r.raise_for_status()
    return re.search('(?<=http://)[0-9a-zA-Z\.\-]+', r.url).group(0)


def is_mesos_leader(hostname=MY_HOSTNAME):
    """Check if a hostname is the current mesos leader.

    :param hostname: The hostname to query mesos-master on
    :returns: True if hostname is the mesos-master leader, False otherwise"""
    return hostname in get_mesos_leader(hostname)


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
    return app_id in all_app_ids


def app_has_tasks(client, app_id, expected_tasks):
    """ A predicate function indicating whether an app has launched *at least* expected_tasks
    tasks.

    Raises a marathon.NotFoundError when no app with matching id is found.

    :param client: the marathon client
    :param app_id: the app_id to which the tasks should belong
    :param minimum_tasks: the minimum number of tasks to check for
    :returns a boolean indicating whether there are atleast expected_tasks tasks with
    an app id matching app_id:
    """
    try:
        tasks = client.list_tasks(app_id=app_id)
    except NotFoundError:
        print "no app with id %s found" % app_id
        raise
    print "app %s has %d of %d expected tasks" % (app_id, len(tasks), expected_tasks)
    return len(tasks) >= expected_tasks


@timeout()
def wait_for_app_to_launch_tasks(client, app_id, expected_tasks):
    """ Wait for an app to have num_tasks tasks launched. If the app isn't found, then this will swallow the exception
        and retry. Times out after 30 seconds.

       :param client: The marathon client
       :param app_id: The app id to which the tasks belong
       :param num_tasks: The number of tasks to wait for
    """
    found = False
    while not found:
        try:
            found = app_has_tasks(client, app_id, expected_tasks)
        except NotFoundError:
            pass
        if found:
            return
        else:
            print "waiting for app %s to have %d tasks. retrying" % (app_id, expected_tasks)
            sleep(0.5)


def create_complete_config(name, instance, marathon_config, soa_dir=DEFAULT_SOA_DIR):
    system_paasta_config = load_system_paasta_config()
    partial_id = compose_job_id(name, instance)
    srv_config = load_marathon_service_config(name, instance, get_cluster(), soa_dir=soa_dir)
    docker_url = get_docker_url(system_paasta_config.get_docker_registry(), srv_config.get_docker_image())
    service_namespace_config = load_service_namespace_config(name, srv_config.get_nerve_namespace())

    complete_config = srv_config.format_marathon_app_dict(
        partial_id,
        docker_url,
        system_paasta_config.get_volumes(),
        service_namespace_config,
    )
    code_sha = get_code_sha_from_dockerurl(docker_url)
    config_hash = get_config_hash(
        complete_config,
        force_bounce=srv_config.get_force_bounce(),
    )
    tag = "%s.%s" % (code_sha, config_hash)
    full_id = compose_job_id(name, instance, tag)
    complete_config['id'] = full_id
    return complete_config


def get_app_id(name, instance, marathon_config, soa_dir=DEFAULT_SOA_DIR):
    """Composes a predictable marathon app_id from the service's docker image and
    marathon configuration. Editing this function *will* cause a bounce of all
    services because they will see an "old" version of the marathon app deployed,
    and a new one with the new hash will try to be deployed"""
    return create_complete_config(name, instance, marathon_config, soa_dir=soa_dir)['id']


def get_expected_instance_count_for_namespace(service_name, namespace, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Get the number of expected instances for a namespace, based on the number
    of instances set to run on that namespace as specified in Marathon service
    configuration files.

    :param service_name: The service's name
    :param namespace: The namespace for that service to check
    :param soa_dir: The SOA configuration directory to read from
    :returns: An integer value of the # of expected instances for the namespace"""
    total_expected = 0
    if not cluster:
        cluster = get_cluster()
    for name, instance in get_service_instance_list(service_name, cluster=cluster, soa_dir=soa_dir):
        srv_config = load_marathon_service_config(name, instance, cluster, soa_dir=soa_dir)
        instance_ns = srv_config.get_nerve_namespace()
        if namespace == instance_ns:
            total_expected += srv_config.get_instances()
    return total_expected


def get_matching_appids(servicename, instance, client):
    """Returns a list of appids given a service and instance.
    Useful for fuzzy matching if you think there are marathon
    apps running but you don't know the full instance id"""
    jobid = compose_job_id(servicename, instance)
    return [app.id for app in client.list_apps() if app.id.startswith("/%s" % jobid)]


def get_healthcheck_for_instance(service_name, instance, service_manifest, random_port):
    """
    Returns healthcheck for a given service instance in the form of a tuple (mode, healthcheck_command)
    or (None, None) if no healthcheck
    """
    smartstack_config = load_service_namespace_config(service_name, instance)
    mode = service_manifest.get_healthcheck_mode(smartstack_config)
    path = service_manifest.get_healthcheck_uri(smartstack_config)
    hostname = socket.getfqdn()

    if mode == "http":
        healthcheck_command = '%s://%s:%d%s' % (mode, hostname, random_port, path)
    elif mode == "tcp":
        healthcheck_command = '%s://%s:%d' % (mode, hostname, random_port)
    elif mode == 'cmd':
        healthcheck_command = service_manifest.get_healthcheck_cmd()
    else:
        mode = None
        healthcheck_command = None
    return (mode, healthcheck_command)
