"""
This module contains the meat of the logic for most of the scripts
that interact with marathon. There's config parsers, url composers,
and a number of other things used by other components in order to
make the PaaSTA stack work.
"""
import hashlib
import logging
import os
import re
import requests
import socket
import sys
import glob

from marathon import MarathonClient
import json
import service_configuration_lib

# DO NOT CHANGE ID_SPACER, UNLESS YOU'RE PREPARED TO CHANGE ALL INSTANCES
# OF IT IN OTHER LIBRARIES (i.e. service_configuration_lib).
# It's used to compose a job's full ID from its name and instance
ID_SPACER = '.'
MY_HOSTNAME = socket.getfqdn()
MESOS_MASTER_PORT = 5050
MESOS_SLAVE_PORT = 5051
CONTAINER_PORT = 8888
DEFAULT_SOA_DIR = service_configuration_lib.DEFAULT_SOA_DIR
log = logging.getLogger('__main__')

PATH_TO_MARATHON_CONFIG = '/etc/paasta_tools/marathon_config.json'
PUPPET_SERVICE_DIR = '/etc/nerve/puppet_services.d'


class MarathonConfig(dict):
    @classmethod
    def load(cls, path=PATH_TO_MARATHON_CONFIG):
        try:
            with open(path) as f:
                return cls(json.load(f))
        except IOError as e:
            raise PaastaNotConfigured("Could not load marathon config file %s: %s" % (e.filename, e.strerror))

    def get_cluster(self):
        """Get the cluster defined in this host's marathon config file.

        :returns: The name of the cluster defined in the marathon configuration"""
        try:
            return self['cluster']
        except KeyError:
            log.warning('Could not find marathon cluster in marathon config at %s' % PATH_TO_MARATHON_CONFIG)
            raise NoMarathonClusterFoundException

    def get_docker_registry(self):
        """Get the docker_registry defined in this host's marathon config file.

        :returns: The docker_registry specified in the marathon configuration"""
        return self['docker_registry']

    def get_zk_hosts(self):
        """Get the zk_hosts defined in this hosts's marathon config file.
        Strips off the zk:// prefix, if it exists, for use with Kazoo.

        :returns: The zk_hosts specified in the marathon configuration"""
        hosts = self['zk_hosts']
        # how do python strings not have a method for doing this
        if hosts.startswith('zk://'):
            return hosts[len('zk://'):]
        return hosts

    def get_docker_volumes(self):
        return self['docker_volumes']


class DeploymentsJson(dict):
    @classmethod
    def load(cls, soa_dir=DEFAULT_SOA_DIR):
        deployment_file = os.path.join(soa_dir, 'deployments.json')
        with open(deployment_file) as f:
            return cls(json.load(f)['v1'])

    def get_branch_dict(self, service_name, instance_name):
        full_branch = '%s:%s' % (service_name, instance_name)
        return self.get(full_branch, {})

    def get_deployed_images(self):
        """Get the docker images that are supposed/allowed to be deployed here
        according to deployments.json.

        :param soa_dir: The SOA Configuration directory with deployments.json
        :returns: A set of images (as strings), or empty set if deployments.json
        doesn't exist in soa_dir
        """

        images = set()
        for branch_dict in self.values():
            if 'docker_image' in branch_dict and branch_dict['desired_state'] == 'start':
                images.add(branch_dict['docker_image'])
        return images


class MarathonServiceConfig(object):
    @classmethod
    def load(cls, service_name, instance, cluster, deployments_json=None, soa_dir=DEFAULT_SOA_DIR):
        """Read a service instance's configuration for marathon.

        If a branch isn't specified for a config, the 'branch' key defaults to
        paasta-${cluster}.${instance}.

        If cluster isn't given, it's loaded using get_cluster.

        :param name: The service name
        :param instance: The instance of the service to retrieve
        :param cluster: The cluster to read the configuration for
        :param soa_dir: The SOA configuration directory to read from
        :returns: A dictionary of whatever was in the config for the service instance"""
        log.info("Reading service configuration files from dir %s/ in %s", service_name, soa_dir)
        log.info("Reading general configuration file: service.yaml")
        general_config = service_configuration_lib.read_extra_service_information(
            service_name,
            "service",
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
            log.error("%s not found in config file %s.yaml.", instance, marathon_conf_file)
            return {}

        general_config.update(instance_configs[instance])

        if deployments_json is None:
            deployments_json = DeploymentsJson.load(soa_dir=soa_dir)

        # Noisy debugging output for PAASTA-322
        general_config['deployments_json'] = deployments_json

        return cls(
            service_name,
            instance,
            general_config,
            deployments_json.get_branch_dict(service_name, instance),
        )

    def __init__(self, service_name, instance, config_dict, branch_dict):
        self.service_name = service_name
        self.instance = instance
        self.config_dict = config_dict
        self.branch_dict = branch_dict

    def copy(self):
        return self.__class__(self.service_name, self.instance, dict(self.config_dict), dict(self.branch_dict))

    def get_docker_image(self):
        """Get the docker image name (with tag) for a given service branch from
        a generated deployments.json file."""
        return self.branch_dict.get('docker_image', '')

    def get_desired_state(self):
        """Get the desired state (either 'start' or 'stop') for a given service
        branch from a generated deployments.json file."""
        return self.branch_dict.get('desired_state', 'start')

    def get_force_bounce(self):
        """Get the force_bounce token for a given service branch from a generated
        deployments.json file. This is a token that, when changed, indicates that
        the marathon job should be recreated and bounced, even if no other
        parameters have changed. This may be None or a string, generally a
        timestamp.
        """
        return self.branch_dict.get('force_bounce', None)

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

    def get_mem(self):
        """Gets the memory required from the service's marathon configuration.

        Defaults to 1000 (1G) if no value specified in the config.

        :param service_config: The service instance's configuration dictionary
        :returns: The amount of memory specified by the config, 1000 if not specified"""
        mem = self.config_dict.get('mem')
        return int(mem) if mem else 1000

    def get_env(self):
        """Gets the environment required from the service's marathon configuration.

        :param service_config: The service instance's configuration dictionary
        :returns: A dictionary with the requested env."""
        env = self.config_dict.get('env', {})
        return env

    def get_cpus(self):
        """Gets the number of cpus required from the service's marathon configuration.

        Defaults to .25 (1/4 of a cpu) if no value specified in the config.

        :param service_config: The service instance's configuration dictionary
        :returns: The number of cpus specified in the config, .25 if not specified"""
        cpus = self.config_dict.get('cpus')
        return float(cpus) if cpus else .25

    def get_args(self):
        """Get the docker args specified in the service's marathon configuration.

        Defaults to an empty array if not specified in the config.

        :param service_config: The service instance's configuration dictionary
        :returns: An array of args specified in the config, [] if not specified"""
        return self.config_dict.get('args', [])

    def get_bounce_method(self):
        """Get the bounce method specified in the service's marathon configuration.

        :param service_config: The service instance's configuration dictionary
        :returns: The bounce method specified in the config, or 'upthendown' if not specified"""
        bounce_method = self.config_dict.get('bounce_method')
        return bounce_method if bounce_method else 'upthendown'

    def get_constraints(self):
        """Gets the constraints specified in the service's marathon configuration.

        These are Marathon job constraints. See
        https://github.com/mesosphere/marathon/wiki/Constraints

        Defaults to no constraints if none given.

        :param service_config: The service instance's configuration dictionary
        :returns: The constraints specified in the config, an empty array if not specified"""
        return self.config_dict.get('constraints')

    def format_marathon_app_dict(self, job_id, docker_url, docker_volumes, healthchecks):
        """Create the configuration that will be passed to the Marathon REST API.

        Currently compiles the following keys into one nice dict:

        - id: the ID of the image in Marathon
        - cmd: currently the docker_url, seemingly needed by Marathon to keep the container field
        - container: a dict containing the docker url and docker launch options. Needed by deimos.
        - uris: blank.
        - ports: an array containing the port.
        - mem: the amount of memory required.
        - cpus: the number of cpus required.
        - constraints: the constraints on the Marathon job.
        - instances: the number of instances required.

        The last 5 keys are retrieved using the get_<key> functions defined above.

        :param job_id: The job/app id name
        :param docker_url: The url to the docker image the job will actually execute
        :param docker_volumes: The docker volumes to run the image with, via the
                               marathon configuration file
        :param service_marathon_config: The service instance's configuration dict
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
            'health_checks': healthchecks,
        }
        complete_config['env'] = self.get_env()
        complete_config['mem'] = self.get_mem()
        complete_config['cpus'] = self.get_cpus()
        complete_config['constraints'] = self.get_constraints()
        complete_config['instances'] = self.get_instances()
        complete_config['args'] = self.get_args()
        log.info("Complete configuration for instance is: %s", complete_config)
        return complete_config

    def get_nerve_namespace(self):
        return self.config_dict.get('nerve_ns', self.instance)

    def get_bounce_health_params(self):
        return self.config_dict.get('bounce_health_params', {})


class ServiceNamespaceConfig(dict):
    @classmethod
    def load(cls, srv_name, namespace, soa_dir=DEFAULT_SOA_DIR):
        """Attempt to read the configuration for a service's namespace in a more strict fashion.

        Retrevies the following keys:

        - proxy_port: the proxy port defined for the given namespace
        - healthcheck_uri: URI target for healthchecking
        - healthcheck_timeout_s: healthcheck timeout in seconds
        - timeout_connect_ms: proxy frontend timeout in milliseconds
        - timeout_server_ms: proxy server backend timeout in milliseconds
        - timeout_client_ms: proxy server client timeout in milliseconds
        - retries: the number of retries on a proxy backend
        - mode: the mode the service is run in (http or tcp)
        - routes: a list of tuples of (source, destination)

        :param srv_name: The service name
        :param namespace: The namespace to read
        :param soa_dir: The SOA config directory to read from
        :returns: A dict of the above keys, if they were defined
        """

        smartstack = service_configuration_lib.read_extra_service_information(srv_name, 'smartstack', soa_dir)
        config_from_file = smartstack[namespace]
        service_namespace_config = cls()
        # We can't really use .get, as we don't want the key to be in the returned
        # dict at all if it doesn't exist in the config file.
        # We also can't just copy the whole dict, as we only care about some keys
        # and there's other things that appear in the smartstack section in
        # several cases.
        key_whitelist = set([
            'healthcheck_uri',
            'healthcheck_timeout_s',
            'proxy_port',
            'timeout_connect_ms',
            'timeout_server_ms',
            'timeout_client_ms',
            'retries',
            'mode',
        ])

        for key, value in config_from_file.items():
            if key in key_whitelist:
                service_namespace_config[key] = value

        if 'routes' in config_from_file:
            service_namespace_config['routes'] = [(route['source'], dest)
                                                  for route in config_from_file['routes']
                                                  for dest in route['destinations']]

        return service_namespace_config

    def get_healthchecks(self):
        """Returns a list of healthchecks per the spec:
        https://mesosphere.github.io/marathon/docs/health-checks.html
        Tries to be very conservative. Currently uses the same configuration
        that smartstack uses, regarding mode (tcp/http) and http status uri.

        If you have an http service, it uses the default endpoint that smartstack uses.
        (/status currently)

        Otherwise these do *not* use the same thresholds as smarstack in order to not
        produce a negative feedback loop, where mesos agressivly kills tasks because they
        are slow, which causes other things to be slow, etc.

        :param service_config: service config hash
        :returns: list of healthcheck defines for marathon"""

        mode = self.get('mode', 'http')
        # We wait for a minute for a service to come up
        graceperiodseconds = 60
        intervalseconds = 10
        timeoutseconds = 10
        # And kill it after it has been failing for a minute
        maxconsecutivefailures = 6

        if mode == 'http':
            http_path = self.get('healthcheck_uri', '/status')
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
        else:
            raise InvalidSmartstackMode("Unknown mode: %s" % mode)
        return healthchecks


class PaastaNotConfigured(Exception):
    pass


class NoMarathonClusterFoundException(Exception):
    pass


class NoDockerImageError(Exception):
    pass


class InvalidSmartstackMode(Exception):
    pass


def get_cluster():
    return MarathonConfig.load().get_cluster()


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


def get_default_branch(cluster, instance):
    return 'paasta-%s.%s' % (cluster, instance)


def get_docker_url(registry_uri, docker_image):
    """Compose the docker url.

    If verify is true, checks if the URL will point to a
    valid image first, returning an empty string if it doesn't.

    :param registry_uri: The URI of the docker registry
    :param docker_image: The docker image name, with tag if desired
    :param verify: Set to False to not verify the composed docker url
    :returns: '<registry_uri>/<docker_image>', or '' if URL didn't verify"""
    if not docker_image:
        raise NoDockerImageError('Docker url not available because there is no docker_image')
    docker_url = '%s/%s' % (registry_uri, docker_image)
    log.info("Docker URL: %s", docker_url)
    return docker_url


def get_config_hash(config, force_bounce=None):
    """Create an MD5 hash of the configuration dictionary to be sent to
    Marathon. Or anything really, so long as str(config) works. Returns
    the first 8 characters so things are not really long.

    :param config: The configuration to hash
    :returns: A MD5 hash of str(config)"""
    hasher = hashlib.md5()
    hasher.update(str(config) + (force_bounce or ''))
    return "config%s" % hasher.hexdigest()[:8]


def read_monitoring_config(name, soa_dir=DEFAULT_SOA_DIR):
    """Read a service's monitoring.yaml file.

    :param name: The service name
    :param soa_dir: THe SOA configuration directory to read from
    :returns: A dictionary of whatever was in soa_dir/name/monitoring.yaml"""
    rootdir = os.path.abspath(soa_dir)
    monitoring_file = os.path.join(rootdir, name, "monitoring.yaml")
    monitor_conf = service_configuration_lib.read_monitoring(monitoring_file)
    return monitor_conf


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
    nerve_dict = ServiceNamespaceConfig.load(name, namespace, soa_dir)
    return nerve_dict.get('proxy_port')


def get_mode_for_instance(name, instance, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Get the mode defined in the namespace configuration for a service instances.
    Defaults to http if one isn't defined.

    This means that the namespace first has to be loaded from the service instance's
    configuration, and then the mode has to loaded from the smartstack configuration
    for that namespace.

    :param name: The service name
    :param instance: The instance of the service
    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: The mode for the service instance, or 'http' if not defined
    """
    if not cluster:
        cluster = get_cluster()
    namespace = read_namespace_for_service_instance(name, instance, cluster, soa_dir)
    nerve_dict = ServiceNamespaceConfig.load(name, namespace, soa_dir)
    return nerve_dict.get('mode', 'http')


def list_clusters(service=None, soa_dir=DEFAULT_SOA_DIR):
    """Returns a sorted list of all clusters that appear to be in use. This
    is useful for cli tools.

    :param service: Optional. If provided will only list clusters that
    the particular service is using
    """
    clusters = set()
    if service is None:
        services = service_configuration_lib.read_services_configuration().keys()
    else:
        services = [service]

    for service in services:
        clusters = clusters.union(set(get_clusters_deployed_to(service)))
    return sorted(clusters)


def get_clusters_deployed_to(service, soa_dir=DEFAULT_SOA_DIR):
    """Looks at the clusters that a service is probably deployed to
    by looking at marathon-*.yaml's and returns a sorted list of clusters.
    """
    clusters = set()
    srv_path = os.path.join(soa_dir, service)
    if os.path.isdir(srv_path):
        marathon_files = "%s/marathon-*.yaml" % srv_path
        for marathon_file in glob.glob(marathon_files):
            basename = os.path.basename(marathon_file)
            cluster = re.search('marathon-(.*).yaml', basename).group(1)
            clusters.add(cluster)
    clusters.discard('SHARED')
    return sorted(clusters)


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


def get_all_namespaces_for_service(name, soa_dir=DEFAULT_SOA_DIR):
    """Get all the smartstack namespaces listed for a given service name.

    :param name: The service name
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of the form (service_name.namespace, namespace_config)"""
    namespace_list = []
    smartstack = service_configuration_lib.read_extra_service_information(name, 'smartstack', soa_dir)
    for namespace in smartstack:
        full_name = '%s%s%s' % (name, ID_SPACER, namespace)
        namespace_list.append((full_name, smartstack[namespace]))
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


def marathon_services_running_on(hostname=MY_HOSTNAME, port=MESOS_SLAVE_PORT, timeout_s=30):
    """See what services are being run by a mesos-slave via marathon on
    the given host and port.

    :param hostname: The hostname to query mesos-slave on
    :param port: The port to query mesos-slave on
    :timeout_s: The timeout, in seconds, for the mesos-slave state request
    :returns: A list of triples of (service_name, instance_name, port)"""
    state_url = 'http://%s:%s/state.json' % (hostname, port)
    try:
        r = requests.get(state_url, timeout=10)
        r.raise_for_status()
    except requests.ConnectionError as e:
        sys.stderr.write('Could not connect to the mesos slave to see which services are running\n')
        sys.stderr.write('on %s:%s. Is the mesos-slave running?\n' % (hostname, port))
        sys.stderr.write('Error was: %s\n' % e.message)
        sys.exit(1)
    slave_state = r.json()
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


def marathon_services_running_here(port=MESOS_SLAVE_PORT, timeout_s=30):
    """See what marathon services are being run by a mesos-slave on this host.
    This is just marathon_services_running_on for localhost.

    :param port: The port to query mesos-slave on
    :timeout_s: The timeout, in seconds, for the mesos-slave state request
    :returns: A list of triples of (service_name, instance_name, port)"""
    return marathon_services_running_on(port=port, timeout_s=timeout_s)


def get_marathon_services_running_here_for_nerve(cluster, soa_dir):
    if not cluster:
        try:
            cluster = get_cluster()
        # In the cases where there is *no* cluster or in the case
        # where there isn't a Paasta configuration file at *all*, then
        # there must be no marathon_services running here, so we catch
        # these custom exceptions and return [].
        except (NoMarathonClusterFoundException, PaastaNotConfigured):
            return []
    # When a cluster is defined in mesos, lets iterate through marathon services
    marathon_services = marathon_services_running_here()
    nerve_list = []
    for name, instance, port in marathon_services:
        try:
            namespace = read_namespace_for_service_instance(name, instance, cluster, soa_dir)
            nerve_dict = ServiceNamespaceConfig.load(name, namespace, soa_dir)
            nerve_dict['port'] = port
            nerve_name = '%s%s%s' % (name, ID_SPACER, namespace)
            nerve_list.append((nerve_name, nerve_dict))
        except KeyError:
            continue  # SOA configs got deleted for this job, it'll get cleaned up
    return nerve_list


def get_classic_services_that_run_here():
    return sorted(
        service_configuration_lib.services_that_run_here() +
        # find all files in the PUPPET_SERVICE_DIR, but discard broken symlinks
        # this allows us to (de)register services on a machine by
        # breaking/healing a symlink placed by Puppet.
        [i for i in os.listdir(PUPPET_SERVICE_DIR) if os.path.exists(i)]
    )


def get_classic_service_information_for_nerve(name, soa_dir):
    nerve_dict = ServiceNamespaceConfig.load(name, 'main', soa_dir)
    port_file = os.path.join(soa_dir, name, 'port')
    nerve_dict['port'] = service_configuration_lib.read_port(port_file)
    nerve_name = '%s%s%s' % (name, ID_SPACER, 'main')
    return (nerve_name, nerve_dict)


def get_classic_services_running_here_for_nerve(soa_dir):
    return [
        get_classic_service_information_for_nerve(name, soa_dir)
        for name in get_classic_services_that_run_here()
    ]


def get_services_running_here_for_nerve(cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Get a list of ALL services running on this box, with returned information
    needed for nerve.

    ALL services means services that have a service.yaml with an entry for this host in
    runs_on, AND services that are currently deployed in a mesos-slave here via marathon.

    conf_dict is a dictionary possibly containing the same keys returned by
    ServiceNamespaceConfig.load (in fact, that's what this calls).
    Some or none of those keys may not be present on a per-service basis.

    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of the form (service_name.namespace, service_config)
              AND (service_name, service_config) for legacy SOA services"""
    # All Legacy yelpsoa services are also announced
    return get_marathon_services_running_here_for_nerve(cluster, soa_dir) + \
        get_classic_services_running_here_for_nerve(soa_dir)


def get_mesos_leader(hostname=MY_HOSTNAME):
    """Get the current mesos-master leader's hostname.

    :param hostname: The hostname to query mesos-master on
    :returns: The current mesos-master hostname"""
    redirect_url = 'http://%s:%s/redirect' % (hostname, MESOS_MASTER_PORT)
    r = requests.get(redirect_url, timeout=10)
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


def create_complete_config(name, instance, marathon_config, soa_dir=DEFAULT_SOA_DIR):
    partial_id = compose_job_id(name, instance)
    srv_config = MarathonServiceConfig.load(name, instance, soa_dir=soa_dir)
    try:
        docker_url = get_docker_url(marathon_config.get_docker_registry(), srv_config.get_docker_image())
    # Noisy debugging output for PAASTA-322
    except NoDockerImageError as err:
        err.srv_config = srv_config
        raise err
    healthchecks = ServiceNamespaceConfig.load(name, instance).get_healthchecks()
    complete_config = srv_config.format_marathon_app_dict(
        partial_id,
        docker_url,
        marathon_config.get_docker_volumes(),
        healthchecks,
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


def get_code_sha_from_dockerurl(docker_url):
    """We encode the sha of the code that built a docker image *in* the docker
    url. This function takes that url as input and outputs the partial sha"""
    parts = docker_url.split('-')
    return "git%s" % parts[-1][:8]


def get_expected_instance_count_for_namespace(service_name, namespace, soa_dir=DEFAULT_SOA_DIR):
    """Get the number of expected instances for a namespace, based on the number
    of instances set to run on that namespace as specified in Marathon service
    configuration files.

    :param service_name: The service's name
    :param namespace: The namespace for that service to check
    :param soa_dir: The SOA configuration directory to read from
    :returns: An integer value of the # of expected instances for the namespace"""
    total_expected = 0
    for name, instance in get_service_instance_list(service_name, soa_dir=soa_dir):
        srv_config = MarathonServiceConfig.load(name, instance, soa_dir=soa_dir)
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
