import logging
import json
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

# Not actually needed; marathon is smart enough now that any instance can
# deploy something into zookeeper/mesos
# def is_leader(marathon_config):
#     return True
    # http://dev15-devc:5052/v1/debug/leaderUrl
