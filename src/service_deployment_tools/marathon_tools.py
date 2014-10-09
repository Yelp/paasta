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
import socket
from StringIO import StringIO

import json
import pycurl
import service_configuration_lib

# DO NOT CHANGE ID_SPACER, UNLESS YOU'RE PREPARED TO CHANGE ALL INSTANCES
# OF IT IN OTHER LIBRARIES (i.e. service_configuration_lib).
# It's used to compose a job's full ID from its name, instance, and iteration.
ID_SPACER = '.'
MY_HOSTNAME = socket.getfqdn()
MESOS_MASTER_PORT = 5050
MESOS_SLAVE_PORT = 5051
DEFAULT_SOA_DIR = service_configuration_lib.DEFAULT_SOA_DIR
log = logging.getLogger(__name__)


class MarathonConfig:
    # A simple borg DP class to keep the config from being loaded tons of times.
    # http://code.activestate.com/recipes/66531/
    __shared_state = {'config': None}

    def __init__(self):
        self.__dict__ = self.__shared_state
        if not self.config:
            self.config = json.loads(open('/etc/service_deployment_tools/marathon_config.json').read())

    def get(self):
        return self.config


def get_config():
    """Get the general marathon configuration information, or load
    a default configuration if the config file isn't deployed here.

    The configuration file is managed by puppet, and is called
    /etc/service_deployment_tools/marathon_config.json.

    :returns: A dict of the marathon configuration"""
    return MarathonConfig().get()


def get_cluster():
    """Get the cluster defined in this host's marathon config file.

    :returns: The name of the cluster defined in the marathon configuration"""
    return get_config()['cluster']


def get_docker_registry():
    """Get the docker_registry defined in this host's marathon config file.

    :returns: The docker_registry specified in the marathon configuration"""
    return get_config()['docker_registry']


def get_zk_hosts():
    """Get the zk_hosts defined in this hosts's marathon config file.
    Strips off the zk:// prefix, if it exists, for use with Kazoo.

    :returns: The zk_hosts specified in the marathon configuration"""
    return get_config()['zk_hosts'].lstrip('zk://')


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


def get_docker_url(registry_uri, docker_image, verify=True):
    """Compose the docker url.

    If verify is true, checks if the URL will point to a
    valid image first, returning an empty string if it doesn't.

    :param registry_uri: The URI of the docker registry
    :param docker_image: The docker image name, with tag if desired
    :param verify: Set to False to not verify the composed docker url
    :returns: '<registry_uri>/<docker_image>', or '' if URL didn't verify"""
    if not docker_image:
        return ''
    if verify:
        s = StringIO()
        c = pycurl.Curl()
        c.setopt(pycurl.URL, str('http://%s/v1/repositories/%s/tags/%s' % (registry_uri,
                                                                           docker_image.split(':')[0],
                                                                           docker_image.split(':')[1])))
        c.setopt(pycurl.WRITEFUNCTION, s.write)
        c.perform()
        if 'error' in s.getvalue():
            log.error("Docker image not found: %s/%s", registry_uri, docker_image)
            return ''
    docker_url = '%s/%s' % (registry_uri, docker_image)
    log.info("Docker URL: %s", docker_url)
    return docker_url


def get_mem(service_config):
    """Gets the memory required from the service's marathon configuration.

    Defaults to 100 if no value specified in the config.

    :param service_config: The service instance's configuration dictionary
    :returns: The amount of memory specified by the config, 100 if not specified"""
    mem = service_config.get('mem')
    return int(mem) if mem else 100


def get_cpus(service_config):
    """Gets the number of cpus required from the service's marathon configuration.

    Defaults to 1 if no value specified in the config.

    :param service_config: The service instance's configuration dictionary
    :returns: The number of cpus specified in the config, 1 if not specified"""
    cpus = service_config.get('cpus')
    return int(cpus) if cpus else 1


def get_constraints(service_config):
    """Gets the constraints specified in the service's marathon configuration.

    These are Marathon job constraints. See
    https://github.com/mesosphere/marathon/wiki/Constraints

    Defaults to no constraints if none given.

    :param service_config: The service instance's configuration dictionary
    :returns: The constraints specified in the config, an empty array if not specified"""
    return service_config.get('constraints')


def get_instances(service_config):
    """Get the number of instances specified in the service's marathon configuration.

    Defaults to 1 if not specified in the config.

    :param service_config: The service instance's configuration dictionary
    :returns: The number of instances specified in the config, 1 if not specified"""
    instances = service_config.get('instances')
    return int(instances) if instances else 1


def get_bounce_method(service_config):
    """Get the bounce method specified in the service's marathon configuration.

    Defaults to crossover if no method specified in the config.

    :param service_config: The service instance's configuration dictionary
    :returns: The bounce method specified in the config, or 'crossover' if not specified"""
    bounce_method = service_config.get('bounce_method')
    return bounce_method if bounce_method else 'crossover'


def get_config_hash(config):
    """Create an MD5 hash of the configuration dictionary to be sent to
    Marathon. Or anything really, so long as str(config) works.

    :param config: The configuration to hash
    :returns: A MD5 hash of str(config)"""
    hasher = hashlib.md5()
    hasher.update(str(config))
    return hasher.hexdigest()


def create_complete_config(job_id, docker_url, docker_volumes, service_marathon_config):
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
                        'containerPort': 8888,
                        'hostPort': 0,
                        'protocol': 'tcp',
                    },
                ],
            },
            'type': 'DOCKER',
            'volumes': docker_volumes,
        },
        'uris': ['file:///root/.dockercfg', ],
    }
    complete_config['mem'] = get_mem(service_marathon_config)
    complete_config['cpus'] = get_cpus(service_marathon_config)
    complete_config['constraints'] = get_constraints(service_marathon_config)
    complete_config['instances'] = get_instances(service_marathon_config)
    log.info("Complete configuration for instance is: %s", complete_config)
    return complete_config


def get_docker_from_branch(service_name, branch_name, soa_dir=DEFAULT_SOA_DIR):
    """Get the docker image name (with tag) for a given service branch from
    a generated deployments.json file.

    :param service_name: The name of the service
    :param branch_name: The name of the remote branch to get an image for
    :param soa_dir: The SOA Configuration directory with deployments.json
    :returns: The name and tag of the docker image for the branch, or '' if
              deployments.json doesn't exist in soa_dir"""
    deployment_file = os.path.join(soa_dir, 'deployments.json')
    if os.path.exists(deployment_file):
        dockers = json.loads(open(deployment_file).read())
        full_branch = '%s:%s' % (service_name, branch_name)
        return dockers.get(full_branch, '')
    else:
        return ''


def read_monitoring_config(name, soa_dir=DEFAULT_SOA_DIR):
    """Read a service's monitoring.yaml file.

    :param name: The service name
    :param soa_dir: THe SOA configuration directory to read from
    :returns: A dictionary of whatever was in soa_dir/name/monitoring.yaml"""
    rootdir = os.path.abspath(soa_dir)
    monitoring_file = os.path.join(rootdir, name, "monitoring.yaml")
    monitor_conf = service_configuration_lib.read_monitoring(monitoring_file)
    return monitor_conf


def read_service_config(name, instance, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Read a service instance's configuration for marathon.

    If a branch or docker_image aren't specified for a config, the
    'branch' key defaults to paasta-${cluster}.${instance}.

    If cluster isn't given, it's loaded using get_cluster.

    :param name: The service name
    :param instance: The instance of the service to retrieve
    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA configuration directory to read from
    :returns: A dictionary of whatever was in the config for the service instance"""
    if not cluster:
        cluster = get_cluster()
    log.info("Reading service configuration files from dir %s/ in %s", name, soa_dir)
    log.info("Reading general configuration file: service.yaml")
    general_config = service_configuration_lib.read_extra_service_information(
                            name,
                            "service",
                            soa_dir=soa_dir)
    marathon_conf_file = "marathon-%s" % cluster
    log.info("Reading marathon configuration file: %s.yaml", marathon_conf_file)
    instance_configs = service_configuration_lib.read_extra_service_information(
                            name,
                            marathon_conf_file,
                            soa_dir=soa_dir)
    if instance in instance_configs:
        general_config.update(instance_configs[instance])
        # Once we don't allow docker_image anymore, remove this if and everything will work
        if 'docker_image' not in general_config:
            if 'branch' not in general_config:
                branch = get_default_branch(cluster, instance)
            else:
                branch = general_config['branch']
            general_config['docker_image'] = get_docker_from_branch(name, branch, soa_dir)
        return general_config
    else:
        log.error("%s not found in config file %s.yaml.", instance, marathon_conf_file)
        return {}


def read_namespace_for_service_instance(name, instance, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Retreive a service instance's nerve namespace from its configuration file.
    If one is not defined in the config file, returns instance instead."""
    if not cluster:
        cluster = get_cluster()
    srv_info = service_configuration_lib.read_extra_service_information(
                    name, "marathon-%s" % cluster, soa_dir)[instance]
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
    nerve_dict = read_service_namespace_config(name, namespace, soa_dir)
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
    nerve_dict = read_service_namespace_config(name, namespace, soa_dir)
    return nerve_dict.get('mode', 'http')


def get_service_instance_list(name, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Enumerate the marathon instances defined for a service as a list of tuples.

    :param name: The service name
    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (name, instance) for each instance defined for the service name"""
    if not cluster:
        cluster = get_cluster()
    marathon_conf_file = "marathon-%s" % cluster
    log.info("Enumerating all instances for config file: %s/%s.yaml", soa_dir, marathon_conf_file)
    instances = service_configuration_lib.read_extra_service_information(
                    name,
                    marathon_conf_file,
                    soa_dir=soa_dir)
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
    smartstack = service_configuration_lib.read_extra_service_information(
                    name, 'smartstack', soa_dir)
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


def read_service_namespace_config(srv_name, namespace, soa_dir=DEFAULT_SOA_DIR):
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
    try:
        smartstack = service_configuration_lib.read_extra_service_information(
                            srv_name, 'smartstack', soa_dir)
        ns_config = smartstack[namespace]
        ns_dict = {}
        # We can't really use .get, as we don't want the key to be in the returned
        # dict at all if it doesn't exist in the config file.
        # We also can't just copy the whole dict, as we only care about some keys
        # and there's other things that appear in the smartstack section in
        # several cases.
        if 'healthcheck_uri' in ns_config:
            ns_dict['healthcheck_uri'] = ns_config['healthcheck_uri']
        if 'healthcheck_timeout_s' in ns_config:
            ns_dict['healthcheck_timeout_s'] = ns_config['healthcheck_timeout_s']
        if 'proxy_port' in ns_config:
            ns_dict['proxy_port'] = ns_config['proxy_port']
        if 'timeout_connect_ms' in ns_config:
            ns_dict['timeout_connect_ms'] = ns_config['timeout_connect_ms']
        if 'timeout_server_ms' in ns_config:
            ns_dict['timeout_server_ms'] = ns_config['timeout_server_ms']
        if 'timeout_client_ms' in ns_config:
            ns_dict['timeout_client_ms'] = ns_config['timeout_client_ms']
        if 'retries' in ns_config:
            ns_dict['retries'] = ns_config['retries']
        if 'mode' in ns_config:
            ns_dict['mode'] = ns_config['mode']
        if 'routes' in ns_config:
            ns_dict['routes'] = [(route['source'], dest)
                                 for route in ns_config['routes']
                                 for dest in route['destinations']]
        return ns_dict
    except:  # The file couldn't be loaded, didn't exist, or otherwise was broken.
        return {}


def marathon_services_running_on(hostname=MY_HOSTNAME, port=MESOS_SLAVE_PORT, timeout_s=30):
    """See what services are being run by a mesos-slave via marathon on
    the given host and port.

    :param hostname: The hostname to query mesos-slave on
    :param port: The port to query mesos-slave on
    :timeout_s: The timeout, in seconds, for the mesos-slave state request
    :returns: A list of triples of (service_name, instance_name, port)"""
    s = StringIO()
    req = pycurl.Curl()
    req.setopt(pycurl.TIMEOUT, timeout_s)
    req.setopt(pycurl.URL, 'http://%s:%s/state.json' % (hostname, port))
    req.setopt(pycurl.WRITEFUNCTION, s.write)
    req.perform()
    # If there's an I/O error here, we should fail and know about it, as
    # we should be running is_mesos_slave(localhost) before hitting this
    slave_state = json.loads(s.getvalue())
    frameworks = [fw for fw in slave_state.get('frameworks', []) if 'marathon' in fw['name']]
    executors = [ex for fw in frameworks for ex in fw.get('executors', [])
                 if ex.get('tasks')]
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
        except:
            return []
    # When a cluster is defined in mesos, lets iterate through marathon services
    marathon_services = marathon_services_running_here()
    nerve_list = []
    for name, instance, port in marathon_services:
        try:
            namespace = read_namespace_for_service_instance(name, instance, cluster, soa_dir)
            nerve_dict = read_service_namespace_config(name, namespace, soa_dir)
            nerve_dict['port'] = port
            nerve_name = '%s%s%s' % (name, ID_SPACER, namespace)
            nerve_list.append((nerve_name, nerve_dict))
        except KeyError:
            continue  # SOA configs got deleted for this job, it'll get cleaned up
    return nerve_list


def get_classic_services_running_here_for_nerve(soa_dir):
    regular_services = service_configuration_lib.services_that_run_here()
    nerve_list = []

    for name in regular_services:
        nerve_dict = read_service_namespace_config(name, 'main', soa_dir)
        port_file = os.path.join(soa_dir, name, 'port')
        nerve_dict['port'] = service_configuration_lib.read_port(port_file)
        nerve_name = '%s%s%s' % (name, ID_SPACER, 'main')
        nerve_list.append((nerve_name, nerve_dict))
        # Kill this line when we've migrated over- this is the 'old'
        # way of naming services. We have namespaces now, which we should
        # be using in the future, but synapse isn't really namespace
        # compatible at the moment. Once it is, we won't need the old way.
        nerve_list.append((name, nerve_dict))
    return nerve_list


def get_services_running_here_for_nerve(cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Get a list of ALL services running on this box, with returned information
    needed for nerve.

    ALL services means services that have a service.yaml with an entry for this host in
    runs_on, AND services that are currently deployed in a mesos-slave here via marathon.

    conf_dict is a dictionary possibly containing the same keys returned by
    read_service_namespace_config (in fact, that's what this calls).
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
    curl = pycurl.Curl()
    curl.setopt(pycurl.URL, 'http://%s:%s/redirect' % (hostname, MESOS_MASTER_PORT))
    curl.setopt(pycurl.HEADER, True)
    curl.setopt(pycurl.WRITEFUNCTION, lambda a: None)
    curl.perform()
    return re.search('(?<=http://)[0-9a-zA-Z\.\-]+', curl.getinfo(pycurl.REDIRECT_URL)).group(0)


def is_mesos_leader(hostname=MY_HOSTNAME):
    """Check if a hostname is the current mesos leader.

    :param hostname: The hostname to query mesos-master on
    :returns: True if hostname is the mesos-master leader, False otherwise"""
    return hostname in get_mesos_leader(hostname)
