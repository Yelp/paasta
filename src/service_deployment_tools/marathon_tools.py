import logging
import service_configuration_lib

log = logging.getLogger("__main__")

def read_srv_config(name, instance, cluster, soa_dir):
    log.info("Reading service configuration files from dir %s/ in %s", name, soa_dir)
    log.info("Reading general configuration file: service.yaml")
    general_config = service_configuration_lib.read_extra_service_information(
                            name,
                            "service",
                            soa_dir=soa_dir)
    marathon_conf_file = "marathon-" + cluster
    log.info("Reading marathon configuration file: %s.yaml", marathon_conf_file)
    instance_configs = service_configuration_lib.read_extra_service_information(
                            name,
                            marathon_conf_file,
                            soa_dir=soa_dir)
    if instance in instance_configs:
        return dict(general_config.items() + instance_configs[instance].items())
    else:
        log.error("%s not found in config file %s.yaml.", instance, marathon_conf_file)
        return {}

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
    # Required keys (need defaults for some):
    #   docker_registry
    #   docker_image
    #   url
    #   user
    #   pass
    #   cluster
    #   executor
    # TODO read from a config file
    config = {
        'cluster': 'devc',
        'url': 'http://dev5-devc.dev.yelpcorp.com:5052',
        'user': 'admin',
        'pass': '***REMOVED***',
        'docker_registry': 'docker-dev.yelpcorp.com',
        'docker_options': ['-v', '/nail/etc/:/nail/etc/:ro'],
        'executor': '/usr/bin/deimos',
    }
    return config



def is_leader(marathon_config):
    return true
    #http://dev15-devc:5052/v1/debug/leaderUrl




