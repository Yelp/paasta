#!/usr/bin/env python

import os
import sys
import logging
import argparse
import service_configuration_lib
from service_deployment_tools import marathon_tools
from service_deployment_tools import bounce_lib
from marathon import MarathonClient
import pysensu_yelp

# Marathon REST API:
# https://github.com/mesosphere/marathon/blob/master/REST.md#post-v2apps

# DO NOT CHANGE ID_SPACER, UNLESS YOU'RE PREPARED TO CHANGE ALL INSTANCES
# OF IT IN OTHER LIBRARIES (i.e. service_configuration_lib).
# It's used to compose a job's full ID from its name, instance, and iteration.
ID_SPACER = marathon_tools.ID_SPACER
log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='Creates marathon jobs.')
    parser.add_argument('service_instance',
                        help="The marathon instance of the service to create or update",
                        metavar="SERVICE.INSTANCE")
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest="verbose", default=False)
    args = parser.parse_args()
    return args


def send_sensu_event(name, instance, soa_dir, status, output):
    rootdir = os.path.abspath(soa_dir)
    monitoring_file = os.path.join(rootdir, name, "monitoring.yaml")
    monitor_conf = service_configuration_lib.read_monitoring(monitoring_file)
    # We don't use compose_job_id here because we don't want to change _ to -
    full_name = 'setup_marathon_job.%s%s%s' % (name, ID_SPACER, instance)
    # runbook = monitor_conf.get('runbook')
    runbook = 'y/rb-marathon'
    team = monitor_conf.get('team')

    if team:
        # We need to remove things that aren't kwargs to send_event
        # so that we can just pass everything else as a kwarg.
        # This means that if monitoring.yaml has an erroneous key,
        # the event won't get emitted at all!
        # We'll need a strict spec in yelpsoa_configs to make sure
        # that doesn't happen.
        valid_kwargs = ['page', 'tip', 'notification_email', 'check_every', 'realert_every',
                        'alert_after', 'dependencies', 'irc_channels']
        sensu_kwargs = {}
        for kwarg in valid_kwargs:
            if kwarg in monitor_conf:
                sensu_kwargs[kwarg] = monitor_conf[kwarg]
        sensu_kwargs['realert_every'] = -1
        pysensu_yelp.send_event(full_name, runbook, status, output, team, **sensu_kwargs)


def get_main_marathon_config():
    log.debug("Reading marathon configuration")
    marathon_config = marathon_tools.get_config()
    log.info("Marathon config is: %s", marathon_config)
    return marathon_config


def get_marathon_client(url, user, passwd):
    """Get a new marathon client connection in the form of a MarathonClient object.

    Connects to the Marathon server at 'url' with login specified
    by 'user' and 'pass', all from the marathon config."""
    log.info("Connecting to Marathon server at: %s", url)
    return MarathonClient(url, user, passwd)


def deploy_service(name, config, client, bounce_method):
    """Deploy the service with the given name, config, and bounce_method."""
    log.info("Deploying service instance %s with bounce_method %s", name, bounce_method)
    log.debug("Searching for old service instance iterations")
    filter_name = marathon_tools.remove_tag_from_job_id(name)
    app_list = client.list_apps()
    old_app_ids = [app.id for app in app_list if filter_name in app.id]
    if old_app_ids:  # there's a previous iteration; bounce
        log.info("Old service instance iterations found: %s", old_app_ids)
        try:
            if bounce_method == "brutal":
                bounce_lib.brutal_bounce(old_app_ids, config, client)
            else:
                log.error("bounce_method not recognized: %s. Exiting", bounce_method)
                return (1, "bounce_method not recognized: %s" % bounce_method)
        except IOError:
            log.error("service %s already being bounced. Exiting", filter_name)
            return (1, "Service is taking a while to bounce")
    else:  # there wasn't actually a previous iteration; just deploy it
        log.info("No old instances found. Deploying instance %s", name)
        client.create_app(**config)
    log.info("%s deployed. Exiting", name)
    return (0, 'Service deployed.')


def setup_service(service_name, instance_name, client, marathon_config,
                  service_marathon_config):
    """Setup the service instance given and attempt to deploy it, if possible.

    The full id of the service instance is service_name__instance_name__iteration.
    Doesn't do anything if the full id is already in Marathon.
    If it's not, attempt to find old instances of the service and bounce them."""
    partial_id = marathon_tools.compose_job_id(service_name, instance_name)
    log.info("Setting up instance %s for service %s", instance_name, service_name)
    docker_url = marathon_tools.get_docker_url(marathon_config['docker_registry'],
                                               service_marathon_config['docker_image'])
    if not docker_url:
        log.error("Docker image %s not found. Exiting", service_marathon_config['docker_image'])
        return (1, "Docker image not found in deployments.json: %s"
                   % service_marathon_config['docker_image'])
    complete_config = marathon_tools.create_complete_config(partial_id, docker_url,
                                                            marathon_config['docker_options'],
                                                            service_marathon_config)
    config_hash = marathon_tools.get_config_hash(complete_config)
    full_id = marathon_tools.compose_job_id(service_name, instance_name, config_hash)
    complete_config['id'] = full_id
    log.info("Desired Marathon instance id: %s", full_id)
    try:
        log.info("Checking if instance with hash %s already exists", config_hash)
        client.get_app(full_id)
        log.warning("App id %s already exists. Skipping configuration and exiting.", full_id)
        return (0, 'Service was already deployed.')
    except KeyError:
        return deploy_service(full_id, complete_config, client,
                              bounce_method=marathon_tools.get_bounce_method(service_marathon_config))


def main():
    """Deploy a service instance to Marathon from a configuration file.

    Usage: python setup_marathon_job.py <service_name> <instance_name> [options]
    Valid options:
      -d, --soa-dir: A soa config directory to read config files from, otherwise uses
                     service_configuration_lib.DEFAULT_SOA_DIR
      -v, --verbose: Verbose output"""
    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)
    try:
        service_name = args.service_instance.split(ID_SPACER)[0]
        instance_name = args.service_instance.split(ID_SPACER)[1]
    except IndexError:
        log.error("Invalid service instance specified. Format is service_name.instance_name.")
        sys.exit(1)

    marathon_config = get_main_marathon_config()
    client = get_marathon_client(marathon_config['url'], marathon_config['user'],
                                 marathon_config['pass'])

    service_instance_config = marathon_tools.read_service_config(service_name, instance_name,
                                                                 marathon_config['cluster'], soa_dir)

    if service_instance_config:
        try:
            status, output = setup_service(service_name, instance_name, client, marathon_config,
                                           service_instance_config)
            sensu_status = pysensu_yelp.Status.CRITICAL if status else pysensu_yelp.Status.OK
            send_sensu_event(service_name, instance_name, soa_dir, sensu_status, output)
            sys.exit(status)
        except (KeyError, TypeError, ValueError) as e:
            log.error(str(e))
            send_sensu_event(service_name, instance_name, soa_dir, pysensu_yelp.Status.CRITICAL, str(e))
            sys.exit(1)
    else:
        error_msg = "Could not read marathon configuration file for %s in cluster %s" % \
                    (args.service_instance, marathon_config['cluster'])
        log.error(error_msg)
        send_sensu_event(service_name, instance_name, soa_dir, pysensu_yelp.Status.CRITICAL, error_msg)
        sys.exit(1)


if __name__ == "__main__":
    main()
