import logging
import os
import re
import socket

import json
import pycurl
import service_configuration_lib


ID_SPACER = '.'
MY_HOSTNAME = socket.getfqdn()
MY_CLUSTER = None
MESOS_MASTER_PORT = 5050
MESOS_SLAVE_PORT = 5051
DEFAULT_SOA_DIR = service_configuration_lib.DEFAULT_SOA_DIR
log = logging.getLogger(__name__)


def get_config():
    try:
        return json.loads(open('/etc/service_deployment_tools.json').read())
    except:  # Couldn't load config, fall back to default
        return {
            'cluster': 'devc',
            'url': 'http://localhost:5052',
            'user': 'admin',
            'pass': '***REMOVED***',
            'docker_registry': 'docker-dev.yelpcorp.com',
            'docker_options': ["-v", "/nail/etc/:/nail/etc/:ro"]
        }


def get_cluster():
    # A simplistic singleton pattern to not load the config over and over and over
    global MY_CLUSTER
    if not MY_CLUSTER:
        MY_CLUSTER = get_config()['cluster']
    return MY_CLUSTER


def read_service_config(name, instance, cluster=get_cluster(), soa_dir=DEFAULT_SOA_DIR):
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
        return general_config
    else:
        log.error("%s not found in config file %s.yaml.", instance, marathon_conf_file)
        return {}


def get_service_instance_list(name, cluster=get_cluster(), soa_dir=DEFAULT_SOA_DIR):
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


def get_marathon_services_for_cluster(cluster=get_cluster(), soa_dir=DEFAULT_SOA_DIR):
    rootdir = os.path.abspath(soa_dir)
    log.info("Retrieving all service instance names from soa_dir %s", rootdir)
    instance_list = []
    for srv_dir in os.listdir(rootdir):
        instance_list += get_service_instance_list(srv_dir, cluster, soa_dir)
    return instance_list


def brutal_bounce(old_ids, new_config, client):
    """Brutally bounce the service by killing all old instances first.

    Kills all old_ids then spawns a new app with the new_config via a
    Marathon client."""
    for app in old_ids:
        log.info("Killing %s", app)
        client.delete_app(app)
    log.info("Creating %s", new_config['id'])
    client.create_app(**new_config)


def read_service_instance_namespace(name, instance, cluster, soa_dir=DEFAULT_SOA_DIR):
    """Retreive a service instance's namespace from its configuration file.
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
        # We also can't just copy the whole dict, as we only care about 2 keys
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

    Returns a list of tuples, where the tuples are (service_name, instance_name, port).

    """
    req = pycurl.Curl()
    req.setopt(pycurl.TIMEOUT, timeout_s)
    req.setopt(pycurl.URL, 'http://%s:%s/state.json' % (hostname, port))
    # If there's an I/O error here, we should fail and know about it, as
    # we should be running is_mesos_slave(localhost) before hitting this
    slave_state = json.loads(req.perform())
    frameworks = [fw for fw in slave_state.get('frameworks', []) if 'marathon' in fw['name']]
    executors = [ex for fw in frameworks for ex in fw.get('executors', [])]
    srv_list = []
    for executor in executors:
        srv_name = executor['id'].split(ID_SPACER)[0]
        srv_instance = executor['id'].split(ID_SPACER)[1]
        srv_port = int(re.findall('[0-9]+', executor['resources']['ports'])[0])
        srv_list.append((srv_name, srv_instance, srv_port))
    return srv_list


def marathon_services_running_here(port=MESOS_SLAVE_PORT, timeout_s=30):
    return marathon_services_running_on(port=port, timeout_s=timeout_s)


def get_services_running_here_for_nerve(cluster=get_cluster(), soa_dir=DEFAULT_SOA_DIR):
    """Get a list of services running in marathon on this box, with returned information
    needed for nerve. Returns a list of tuples of the form (service_name, conf_dict).

    service_name is NAME.NAMESPACE, where NAME is the service/dir name and NAMESPACE
    is the nerve_ns associated with a service instance.

    conf_dict is a dictionary possibly containing the following keys:
      port: the external port that mesos has assigned the service
      proxy_port: the proxy port defined for NAME.NAMESPACE
      healthcheck_uri: URI target for healthchecking
      healthcheck_timeout_s: healthcheck timeout in seconds
      routes: a list of tuples of (source, destination)
    Some or none of these keys may not be present on a per-service basis."""
    services = marathon_services_running_here()
    nerve_list = []
    for name, instance, port in services:
        namespace = read_service_instance_namespace(name, instance, cluster, soa_dir)
        nerve_dict = read_service_namespace_config(name, namespace, soa_dir)
        nerve_dict['port'] = port
        nerve_name = '%s%s%s' % (name, ID_SPACER, namespace)
        nerve_list.append((nerve_name, nerve_dict))
    return nerve_list


def get_mesos_leader(hostname=MY_HOSTNAME):
    """Get the current mesos-master leader's hostname.

    Requires a hostname to actually query mesos-master on,
    but defaults to localhost if none is given."""
    curl = pycurl.Curl()
    curl.setopt(pycurl.URL, 'http://%s:%s/redirect' % (hostname, MESOS_MASTER_PORT))
    curl.setopt(pycurl.HEADER, True)
    curl.perform()
    return re.search('(?<=http://)[0-9a-zA-Z\.\-]+', curl.getinfo(pycurl.REDIRECT_URL)).group(0)


def is_mesos_leader(hostname=MY_HOSTNAME):
    """Check if a hostname is the current mesos leader.

    Defaults to localhost."""
    return hostname in get_mesos_leader(hostname)
