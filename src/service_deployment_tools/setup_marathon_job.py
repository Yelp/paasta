#!/usr/bin/env python

import sys
import argparse
import datetime
import service_configuration_lib
import marathon_tools
from marathon import MarathonClient

# Marathon REST API:
# https://github.com/mesosphere/marathon/blob/master/REST.md#post-v2apps

ID_TKN = '__'

def parse_args():
    """Parse arguments for the script."""
    parser = argparse.ArgumentParser(description='Creates marathon jobs.')
    parser.add_argument('service_name',
                        help="The service name to create or update")
    parser.add_argument('instance_name',
                        help="The service instance to create or update")
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest="verbose", default=False)
    args = parser.parse_args()
    return args

def get_main_marathon_config():
    """Get the main marathon configuration via marathon_tools.get_config()."""
    if verbose:
        print "Reading general marathon configuration"
    marathon_config = marathon_tools.get_config()
    if verbose:
        print "Marathon config is:"
        print marathon_config
        print ""
    return marathon_config

def get_docker_url(marathon_config, service_config):
    """Compose the docker url.

    Uses the docker_registry value from marathon_config and the
    docker_image value from service_config.
    The URL is prepended with docker:/// per the deimos docs, at
    https://github.com/mesosphere/deimos"""
    if verbose:
        print "Docker URL: " + str('docker:///' + \
            marathon_config['docker_registry'] + '/' + service_config['docker_image'])
    return 'docker:///' + marathon_config['docker_registry'] + '/' + service_config['docker_image']

def get_time_tag():
    """Gets the current time in Marathon's expected format for versioning."""
    now = datetime.datetime.now()
    return str(now.date()) + "T" + str(now.time()) + "Z"

def get_ports(general_config):
    """Gets the port from the general service configuration.

    Defaults to no ports at all if unspecified.
    This must return an array, as the Marathon REST API takes an
    array of ports, not a single value."""
    if 'port' in general_config:
        return [general_config['port']]
    else:
        if verbose:
            print "WARNING: 'port' not specified in config. No port will be used"
        return []

def get_mem(service_config):
    """Gets the memory required from the service's marathon configuration.

    Defaults to 100 if no value specified in the config."""
    if 'mem' in service_config:
        return service_config['mem']
    else:
        if verbose:
            print "WARNING: 'mem' not specified in config. Using default: 100"
        return '100'

def get_cpus(service_config):
    """Gets the number of cpus required from the service's marathon configuration.

    Defaults to 1 if no value specified in the config."""
    if 'cpus' in service_config:
        return service_config['cpus']
    else:
        if verbose:
            print "WARNING: 'cpus' not specified in config. Using default: 1"
        return 1

def get_constraints(service_config):
    """Gets the constraints specified in the service's marathon configuration.

    Defaults to no constraints if none specified."""
    if 'constraints' in service_config:
        return service_config['constraints']
    else:
        return []

def get_instances(service_config):
    """Get the number of instances specified in the service's marathon configuration.

    Defaults to 1 if not specified in the config."""
    if 'instances' in service_config:
        return service_config['instances']
    else:
        if verbose:
            print "WARNING: 'instances' not specified in config. Using default: 1"
        return 1

def get_bounce_method(service_config):
    """Get the bounce method specified in the service's marathon configuration.

    Defaults to brutal if no method specified in the config."""
    if 'bounce_method' in service_config:
        return service_config['bounce_method']
    else:
        if verbose:
            print "WARNING: 'bounce_method' not specified in config. Using default: brutal"
        return 'brutal'

def get_marathon_client(marathon_config):
    """Get a new marathon client connection in the form of a MarathonClient object.

    Connects to the Marathon server at 'url' with login specified
    by 'user' and 'pass', all from the general marathon_config."""
    if verbose:
        print "Connecting to Marathon server at: " + str(marathon_config['url'])
    return MarathonClient(marathon_config['url'], marathon_config['user'], marathon_config['pass'])

def create_complete_config(name, url, marathon_config, service_general_config, service_marathon_config):
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
                       'cmd': url,
                       'container': {'image': url, 'options': marathon_config['docker_options']},
                       'executor': marathon_config['executor'],
                       'uris': [] }
    complete_config['ports'] = get_ports(service_general_config)
    complete_config['mem'] = get_mem(service_marathon_config)
    complete_config['cpus'] = get_cpus(service_marathon_config)
    complete_config['constraints'] = get_constraints(service_marathon_config)
    complete_config['instances'] = get_instances(service_marathon_config)
    app_kwargs = {}
    for key in complete_config:
        if complete_config[key]:
            app_kwargs[key] = complete_config[key]
    if verbose:
        print "Complete configuration for instance is:"
        print app_kwargs
        print ""
    return app_kwargs

def brutal_bounce(old_ids, new_config, client):
    """Brutally bounce the service by killing all old instances first.

    Kills all old_ids then spawns a new app with the new_config via a
    Marathon client."""
    if verbose:
        print "Brutally bouncing..."
    for app in old_ids:
        if verbose:
            print "Killing " + app
        client.delete_app(app)
    if verbose:
        print "Creating " + new_config['id']
    client.create_app(**new_config)

def deploy_service(name, config, client, bounce_method):
    """Deploy the service with the given name, config, and bounce_method.

    Currently, the only bounce method recognized is brutal."""
    if verbose:
        print "Deploying service instance " + name + " with bounce_method " + bounce_method
        print "Searching for old service iterations"
    filter_name = name.split(ID_TKN)[0] + ID_TKN + name.split(ID_TKN)[1]
    app_list = client.list_apps()
    matching = [app.id for app in filter(lambda app: filter_name in app.id, app_list)]
    if matching: # there's a previous iteration; bounce
        if verbose:
            print "Matching instances found: " + str(matching)
        if bounce_method == "brutal":
            brutal_bounce(matching, config, client)
        else:
            print "ERROR: bounce_method not recognized: " + bounce_method + ". Exiting"
            return False
    else: # there wasn't actually a previous iteration; just deploy it
        if verbose:
            print "No old instances found. Deploying instance " + name
        client.create_app(**config)
    if verbose:
        print name + " deployed. Exiting"
    return True

def setup_service(service_name, instance_name, client, marathon_config,
                  service_general_config, service_marathon_config):
    """Setup the service instance given and attempt to deploy it, if possible.

    The full id of the service instance is service_name__instance_name__iteration.
    Doesn't do anything if the full id is already in Marathon.
    If it's not, attempt to find old instances of the service and bounce them."""
    full_id = service_name + ID_TKN + instance_name + ID_TKN + service_marathon_config['iteration']
    if verbose:
        print "Setting up service instance for: " + service_name + ID_TKN + instance_name
        print "Desired Marathon instance id: " + full_id
    docker_url = get_docker_url(marathon_config, service_marathon_config)
    complete_config = create_complete_config(full_id, docker_url, marathon_config,
                                             service_general_config, service_marathon_config)
    try:
        if verbose:
            print "Checking if instance with iteration " + \
                service_marathon_config['iteration'] + " already exists"
        client.get_app(full_id)
        if verbose:
            print "Iteration already exists. Exiting"
        # I wanted to compare the configs with a call to get_app, but Marathon doesn't
        # return the 'container' key via its REST API, which makes comparing not
        # really viable (especially since the container keys extremely likely to
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
    global verbose
    args = parse_args()
    service_name = args.service_name
    instance_name = args.instance_name
    soa_dir = args.soa_dir
    verbose = args.verbose
    #time_tag = get_time_tag()

    marathon_config = get_main_marathon_config()
    client = get_marathon_client(marathon_config)

    if verbose:
        print "Reading service configuration dir " + service_name + "/ in " + soa_dir
    service_general_config = service_configuration_lib.read_service_configuration(service_name,
                                                                                  soa_dir=soa_dir)
    service_instance_configs = service_configuration_lib.read_extra_service_information(
                                    service_name,
                                    "marathon-" + marathon_config['cluster'],
                                    soa_dir=soa_dir)
    if instance_name in service_instance_configs:
        if setup_service(service_name, instance_name, client, marathon_config,
                         service_general_config, service_instance_configs[instance_name]):
            sys.exit(0)
        else:
            sys.exit(1)
    else:
        print "ERROR: " + instance_name + " not found in config file marathon-" + \
                marathon_config['cluster'] + ".yaml. Exiting"
        sys.exit(1)



if __name__ == "__main__":
    main()
