import hashlib
import logging
import os
import re
import socket
from StringIO import StringIO

import json
import pycurl
import service_configuration_lib


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
    /etc/service_deployment_tools/marathon_config.json."""
    return MarathonConfig().get()


def get_cluster():
    """Get the cluster defined in this host's marathon config file."""
    return get_config()['cluster']


def get_docker_from_branch(service_name, branch_name, soa_dir=DEFAULT_SOA_DIR):
    deployment_file = os.path.join(soa_dir, 'deployments.json')
    if os.path.exists(deployment_file):
        dockers = json.loads(open(deployment_file).read())
        full_branch = '%s:%s' % (service_name, branch_name)
        return dockers.get(full_branch, '')
    else:
        return ''


def get_ports(service_config):
    """Gets the number of ports required from the service's marathon configuration.

    Defaults to one port if unspecified.
    Ports are randomly assigned by Mesos.
    This must return an array, as the Marathon REST API takes an
    array of ports, not a single value."""
    num_ports = service_config.get('num_ports')
    if num_ports:
        return [0 for i in range(int(num_ports))]
    else:
        log.warning("'num_ports' not specified in config. One port will be used.")
        return [0]


def get_mem(service_config):
    """Gets the memory required from the service's marathon configuration.

    Defaults to 100 if no value specified in the config."""
    mem = service_config.get('mem')
    if not mem:
        log.warning("'mem' not specified in config. Using default: 100")
    return int(mem) if mem else 100


def get_cpus(service_config):
    """Gets the number of cpus required from the service's marathon configuration.

    Defaults to 1 if no value specified in the config."""
    cpus = service_config.get('cpus')
    if not cpus:
        log.warning("'cpus' not specified in config. Using default: 1")
    return int(cpus) if cpus else 1


def get_constraints(service_config):
    """Gets the constraints specified in the service's marathon configuration.

    Defaults to no constraints if none given."""
    return service_config.get('constraints')


def get_instances(service_config):
    """Get the number of instances specified in the service's marathon configuration.

    Defaults to 1 if not specified in the config."""
    instances = service_config.get('instances')
    if not instances:
        log.warning("'instances' not specified in config. Using default: 1")
    return int(instances) if instances else 1


def get_bounce_method(service_config):
    """Get the bounce method specified in the service's marathon configuration.

    Defaults to brutal if no method specified in the config."""
    bounce_method = service_config.get('bounce_method')
    if not bounce_method:
        log.warning("'bounce_method' not specified in config. Using default: brutal")
    return bounce_method if bounce_method else 'brutal'


def get_config_hash(config):
    """Create an MD5 hash of the configuration dictionary to be sent to
    Marathon. Or anything, really, so long as str(object) works."""
    hasher = hashlib.md5()
    hasher.update(str(config))
    return hasher.hexdigest()


def get_docker_url(registry_uri, docker_image, verify=True):
    """Compose the docker url.

    Uses the registry_uri (docker_registry) value from marathon_config
    and the docker_image value from a service config to make a Docker URL.
    Checks if the URL will point to a valid image, first, returning a null
    string if it doesn't.

    The URL is prepended with docker:/// per the deimos docs, at
    https://github.com/mesosphere/deimos"""
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
    docker_url = 'docker:///%s/%s' % (registry_uri, docker_image)
    log.info("Docker URL: %s", docker_url)
    return docker_url


def create_complete_config(name, url, docker_options, service_marathon_config):
    """Create the configuration that will be passed to the Marathon REST API.

    Currently compiles the following keys into one nice dict:
      id: the ID of the image in Marathon
      cmd: currently the docker_url, seemingly needed by Marathon to keep the container field
      container: a dict containing the docker url and docker launch options. Needed by deimos.
      uris: blank.
    The following keys are retrieved with the get_* functions defined above:
      ports: an array containing the port.
      mem: the amount of memory required.
      cpus: the number of cpus required.
      constraints: the constraints on the Marathon job.
      instances: the number of instances required."""
    complete_config = {'id': name,
                       'container': {'image': url, 'options': docker_options},
                       'uris': []}
    complete_config['ports'] = get_ports(service_marathon_config)
    complete_config['mem'] = get_mem(service_marathon_config)
    complete_config['cpus'] = get_cpus(service_marathon_config)
    complete_config['constraints'] = get_constraints(service_marathon_config)
    complete_config['instances'] = get_instances(service_marathon_config)
    log.info("Complete configuration for instance is: %s", complete_config)
    return complete_config


def read_service_config(name, instance, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Read a service instance's marathon configuration."""
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
                branch = cluster
            else:
                branch = general_config['branch']
            general_config['docker_image'] = get_docker_from_branch(name, branch, soa_dir)
        return general_config
    else:
        log.error("%s not found in config file %s.yaml.", instance, marathon_conf_file)
        return {}


def compose_job_id(name, instance, tag=None):
    name = str(name).replace('_', '--')
    instance = str(instance).replace('_', '--')
    composed = '%s%s%s' % (name, ID_SPACER, instance)
    if tag:
        tag = str(tag).replace('_', '--')
        composed = '%s%s%s' % (composed, ID_SPACER, tag)
    return composed


def remove_tag_from_job_id(name):
    return '%s%s%s' % (name.split(ID_SPACER)[0], ID_SPACER, name.split(ID_SPACER)[1])


def get_service_instance_list(name, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Enumerate the marathon instances defined for a service as a list of tuples."""
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

    Returns a list of tuples of (service_name, instance_name)."""
    if not cluster:
        cluster = get_cluster()
    rootdir = os.path.abspath(soa_dir)
    log.info("Retrieving all service instance names from %s for cluster %s", rootdir, cluster)
    instance_list = []
    for srv_dir in os.listdir(rootdir):
        instance_list += get_service_instance_list(srv_dir, cluster, soa_dir)
    return instance_list


def get_all_namespaces_for_service(name, soa_dir=DEFAULT_SOA_DIR):
    """Get all the nerve namespaces listed for a given service name.

    Returns a list of tuples of the form (service_name.namespace, config)."""
    namespace_list = []
    smartstack = service_configuration_lib.read_extra_service_information(
                    name, 'smartstack', soa_dir)
    for namespace in smartstack:
        full_name = '%s%s%s' % (name, ID_SPACER, namespace)
        namespace_list.append((full_name, smartstack[namespace]))
    return namespace_list


def get_all_namespaces(soa_dir=DEFAULT_SOA_DIR):
    """Get all the nerve namespaces across all services.
    This is mostly so synapse can get everything it needs in one call.

    Returns a list of triples of the form (service_name.namespace, config)
    where config is a dict of the config vars defined in that namespace."""
    rootdir = os.path.abspath(soa_dir)
    namespace_list = []
    for srv_dir in os.listdir(rootdir):
        namespace_list += get_all_namespaces_for_service(srv_dir, soa_dir)
    return namespace_list


def get_proxy_port_for_instance(name, instance, cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Get the proxy_port defined in the namespace configuration for a service instance.

    Attempts to load two configuration files- marathon-%s.yaml % (cluster)
    and smartstack.yaml, both from the soa_dir/name/ directory."""
    if not cluster:
        cluster = get_cluster()
    namespace = read_namespace_for_service_instance(name, instance, cluster, soa_dir)
    nerve_dict = read_service_namespace_config(name, namespace, soa_dir)
    return nerve_dict.get('proxy_port')


def read_namespace_for_service_instance(name, instance, cluster, soa_dir=DEFAULT_SOA_DIR):
    """Retreive a service instance's nerve namespace from its configuration file.
    If one is not defined in the config file, returns instance instead."""
    srv_info = service_configuration_lib.read_extra_service_information(
                    name, "marathon-%s" % cluster, soa_dir)[instance]
    return srv_info['nerve_ns'] if 'nerve_ns' in srv_info else instance


def read_service_namespace_config(srv_name, namespace, soa_dir=DEFAULT_SOA_DIR):
    """Attempt to read the configuration for a service's namespace.

    Retrevies the following keys:
      proxy_port: the proxy port defined for the given namespace
      healthcheck_uri: URI target for healthchecking
      healthcheck_timeout_s: healthcheck timeout in seconds
      routes: a list of tuples of (source, destination)
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
        if 'routes' in ns_config:
            ns_dict['routes'] = [(route['source'], dest)
                                 for route in ns_config['routes']
                                 for dest in route['destinations']]
        return ns_dict
    except:  # The file couldn't be loaded, didn't exist, or otherwise was broken.
        return {}


def marathon_services_running_on(hostname=MY_HOSTNAME, port=MESOS_SLAVE_PORT, timeout_s=30):
    """See what services are being run by a mesos-slave via marathon on
    the host hostname, where port is the port the mesos-slave is running on.

    Returns a list of tuples, where the tuples are (service_name, instance_name, port)."""
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
    executors = [ex for fw in frameworks for ex in fw.get('executors', [])]
    srv_list = []
    for executor in executors:
        srv_name = executor['id'].split(ID_SPACER)[0].replace('--', '_')
        srv_instance = executor['id'].split(ID_SPACER)[1].replace('--', '_')
        srv_port = int(re.findall('[0-9]+', executor['resources']['ports'])[0])
        srv_list.append((srv_name, srv_instance, srv_port))
    return srv_list


def marathon_services_running_here(port=MESOS_SLAVE_PORT, timeout_s=30):
    """See what marathon services are being run by a mesos-slave on this host."""
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
        namespace = read_namespace_for_service_instance(name, instance, cluster, soa_dir)
        nerve_dict = read_service_namespace_config(name, namespace, soa_dir)
        nerve_dict['port'] = port
        nerve_name = '%s%s%s' % (name, ID_SPACER, namespace)
        nerve_list.append((nerve_name, nerve_dict))
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
    needed for nerve. Returns a list of tuples of the form (service_name, conf_dict).

    ALL services means services that have a service.yaml with an entry for this host in
    runs_on, AND services that are currently deployed in a mesos-slave here via marathon.

    service_name is NAME.NAMESPACE, where NAME is the service/dir name and NAMESPACE
    is the nerve_ns associated with a service instance.

    conf_dict is a dictionary possibly containing the following keys:
      port: the external port that mesos has assigned the service
      proxy_port: the proxy port defined for NAME.NAMESPACE
      healthcheck_uri: URI target for healthchecking
      healthcheck_timeout_s: healthcheck timeout in seconds
      routes: a list of tuples of (source, destination)
    Some or none of these keys may not be present on a per-service basis."""
    # All Legacy yelpsoa services are also announced
    return get_marathon_services_running_here_for_nerve(cluster, soa_dir) + \
        get_classic_services_running_here_for_nerve(soa_dir)


def get_mesos_leader(hostname=MY_HOSTNAME):
    """Get the current mesos-master leader's hostname.

    Requires a hostname to actually query mesos-master on,
    but defaults to the local hostname if none is given."""
    curl = pycurl.Curl()
    curl.setopt(pycurl.URL, 'http://%s:%s/redirect' % (hostname, MESOS_MASTER_PORT))
    curl.setopt(pycurl.HEADER, True)
    curl.perform()
    return re.search('(?<=http://)[0-9a-zA-Z\.\-]+', curl.getinfo(pycurl.REDIRECT_URL)).group(0)


def is_mesos_leader(hostname=MY_HOSTNAME):
    """Check if a hostname is the current mesos leader.

    Defaults to the local hostname."""
    return hostname in get_mesos_leader(hostname)
