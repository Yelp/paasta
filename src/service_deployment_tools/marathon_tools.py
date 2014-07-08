import logging
import re

import json
import pycurl
import service_configuration_lib

log = logging.getLogger(__name__)


def read_srv_config(name, instance, cluster, soa_dir):
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


def get_srv_instance_list(name, cluster, soa_dir):
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


def brutal_bounce(old_ids, new_config, client):
    """Brutally bounce the service by killing all old instances first.

    Kills all old_ids then spawns a new app with the new_config via a
    Marathon client."""
    log.debug("Brutally bouncing...")
    for app in old_ids:
        log.info("Killing %s", app)
        client.delete_app(app)
    log.info("Creating %s", new_config['id'])
    client.create_app(**new_config)


def get_config():
    return json.loads(open('/etc/service_deployment_tools.json').read())


def get_mesos_leader(hostname='localhost'):
    """Get the current mesos-master leader's hostname.

    Requires a hostname to actually query mesos-master on.
    The returned URL is http://<leader_hostname>:5050"""
    curl = pycurl.Curl()
    curl.setopt(pycurl.URL, 'http://%s:5050/redirect' % hostname)
    curl.setopt(pycurl.HEADER, True)
    try:
        curl.perform()
        return re.search('(?<=http://)[0-9a-zA-Z\.\-]+', curl.getinfo(pycurl.REDIRECT_URL)).group(0)
    except pycurl.error as e:
        if e[0] == 7:  # Error 7: couldn't connect to host, but it did resolve/was valid
            return None
        else:
            raise e


def is_leader(hostname='localhost'):
    return hostname in get_mesos_leader(hostname)
