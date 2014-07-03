#!/usr/bin/env python

import sys
import logging
import argparse
import service_configuration_lib
import marathon_tools
from marathon import MarathonClient

# Marathon REST API:
# https://github.com/mesosphere/marathon/blob/master/REST.md#post-v2apps

# DO NOT CHANGE ID_SPACER, UNLESS YOU'RE PREPARED TO CHANGE ALL INSTANCES
# OF IT IN OTHER LIBRARIES (i.e. service_configuration_lib).
# It's used to compose a job's full ID from its name, instance, and iteration.
ID_SPACER = '.'
log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='Creates marathon jobs.')
    parser.add_argument('service_name',
                        help="The service name to create or update")
    parser.add_argument('instance_name',
                        help="The marathon instance of the service to create or update")
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest="verbose", default=False)
    args = parser.parse_args()
    return args


def compose_job_id(name, instance, iteration=None):
    composed = '%s%s%s' % (name, ID_SPACER, instance)
    if iteration:
        composed = '%s%s%s' % (composed, ID_SPACER, iteration)
    return composed


def remove_iteration_from_job_id(name):
    return '%s%s%s' % (name.split(ID_SPACER)[0], ID_SPACER, name.split(ID_SPACER)[1])


def get_main_marathon_config():
    log.debug("Reading marathon configuration")
    marathon_config = marathon_tools.get_config()
    log.info("Marathon config is: %s", marathon_config)
    return marathon_config


def get_docker_url(registry_uri, docker_image):
    """Compose the docker url.

    Uses the registry_uri (docker_registry) value from marathon_config
    and the docker_image value from a service config to make a Docker URL.
    The URL is prepended with docker:/// per the deimos docs, at
    https://github.com/mesosphere/deimos"""
    docker_url = 'docker:///%s/%s' % (registry_uri, docker_image)
    log.info("Docker URL: %s", docker_url)
    return docker_url


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


def get_marathon_client(url, user, passwd):
    """Get a new marathon client connection in the form of a MarathonClient object.

    Connects to the Marathon server at 'url' with login specified
    by 'user' and 'pass', all from the marathon config."""
    log.info("Connecting to Marathon server at: %s", url)
    return MarathonClient(url, user, passwd)


def create_complete_config(name, url, docker_options, executor, service_marathon_config):
    """Create the configuration that will be passed to the Marathon REST API.

    Currently compiles the following keys into one nice dict:
      id: the ID of the image in Marathon
      cmd: currently the docker_url, seemingly needed by Marathon to keep the container field
      container: a dict containing the docker url and docker launch options. Needed by deimos.
      executor: Should generally be deimos, but comes from the marathon_config.
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


def deploy_service(name, config, client, bounce_method):
    """Deploy the service with the given name, config, and bounce_method."""
    log.info("Deploying service instance %s with bounce_method %s", name, bounce_method)
    log.debug("Searching for old service instance iterations")
    filter_name = remove_iteration_from_job_id(name)
    app_list = client.list_apps()
    old_app_ids = [app.id for app in app_list if filter_name in app.id]
    if old_app_ids:  # there's a previous iteration; bounce
        log.info("Old service instance iterations found: %s", old_app_ids)
        if bounce_method == "brutal":
            marathon_tools.brutal_bounce(old_app_ids, config, client)
        else:
            log.error("bounce_method not recognized: %s. Exiting", bounce_method)
            return False
    else:  # there wasn't actually a previous iteration; just deploy it
        log.info("No old instances found. Deploying instance %s", name)
        client.create_app(**config)
    log.info("%s deployed. Exiting", name)
    return True


def setup_service(service_name, instance_name, client, marathon_config,
                  service_marathon_config):
    """Setup the service instance given and attempt to deploy it, if possible.

    The full id of the service instance is service_name__instance_name__iteration.
    Doesn't do anything if the full id is already in Marathon.
    If it's not, attempt to find old instances of the service and bounce them."""
    full_id = compose_job_id(service_name, instance_name, service_marathon_config['iteration'])
    log.info("Setting up service instance for: %s", remove_iteration_from_job_id(full_id))
    log.info("Desired Marathon instance id: %s", full_id)
    docker_url = get_docker_url(marathon_config['docker_registry'],
                                service_marathon_config['docker_image'])
    complete_config = create_complete_config(full_id, docker_url, marathon_config['docker_options'],
                                             marathon_config['executor'], service_marathon_config)
    try:
        log.info("Checking if instance with iteration %s already exists",
                 service_marathon_config['iteration'])
        client.get_app(full_id)
        log.warning("App id %s already exists. Skipping configuration and exiting.", full_id)
        # I wanted to compare the configs with a call to get_app, but Marathon doesn't
        # return the 'container' key via its REST API, which makes comparing not
        # really viable (especially since the container keys are extremely likely to
        # get bumped when a service is updated) -jrm
        return True
    except KeyError:
        return deploy_service(full_id, complete_config, client,
                              bounce_method=get_bounce_method(service_marathon_config))


def main():
    """Deploy a service instance to Marathon from a configuration file.

    Usage: python setup_marathon_job.py <service_name> <instance_name> [options]
    Valid options:
      -d, --soa-dir: A soa config directory to read config files from, otherwise uses
                     service_configuration_lib.DEFAULT_SOA_DIR
      -v, --verbose: Verbose output"""
    args = parse_args()
    service_name = args.service_name
    instance_name = args.instance_name
    soa_dir = args.soa_dir
    if args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)

    marathon_config = get_main_marathon_config()
    client = get_marathon_client(marathon_config['url'], marathon_config['user'],
                                 marathon_config['pass'])

    service_instance_config = marathon_tools.read_srv_config(service_name, instance_name,
                                                             marathon_config['cluster'], soa_dir)

    if service_instance_config:
        if setup_service(service_name, instance_name, client, marathon_config,
                         service_instance_config):
            sys.exit(0)
        else:
            sys.exit(1)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
