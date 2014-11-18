#!/usr/bin/env python
"""
Usage: ./setup_marathon_job.py <service_name.instance_name> [options]

Deploy a service instance to Marathon from a configuration file.
Attempts to load the marathon configuration at
/etc/service_deployment_tools/marathon_config.json, and read
from the soa_dir /nail/etc/services by default.

This script will attempt to load a service's configuration
from the soa_dir and generate a marathon job configuration for it,
as well as handle deploying that configuration with a bounce strategy
if there's an old version of the service. To determine whether or not
a deployment is 'old', each marathon job has a complete id of
service_name.instance_name.configuration_hash, where configuration_hash
is an MD5 hash of the configuration dict to be sent to marathon (without
the configuration_hash in the id field, of course- we change that after
the hash is calculated).

The script will emit a sensu event based on how the deployment went-
if something went wrong, it'll alert the team responsible for the service
(as defined in that service's monitoring.yaml), and it'll send resolves
when the deployment goes alright.

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -v, --verbose: Verbose output
"""
from marathon.exceptions import NotFoundError, InternalServerError
from marathon import MarathonClient
import argparse
import logging
import pysensu_yelp
import service_configuration_lib
import sys

from service_deployment_tools import bounce_lib
from service_deployment_tools import marathon_tools
from service_deployment_tools import monitoring_tools

# Marathon REST API:
# https://github.com/mesosphere/marathon/blob/master/REST.md#post-v2apps

ID_SPACER = marathon_tools.ID_SPACER
log = logging.getLogger('__main__')
log.addHandler(logging.StreamHandler(sys.stdout))


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


def send_event(name, instance, soa_dir, status, output):
    """Send an event to sensu via pysensu_yelp with the given information.

    :param name: The service name the event is about
    :param instance: The instance of the service the event is about
    :param soa_dir: The service directory to read monitoring information from
    :param status: The status to emit for this event
    :param output: The output to emit for this event"""
    # This function assumes the input is a string like "mumble.main"
    framework = 'marathon'
    team = monitoring_tools.get_team(framework, name, instance, soa_dir)
    if not team:
        return
    check_name = 'setup_marathon_job.%s%s%s' % (name, ID_SPACER, instance)
    runbook = 'y/rb-marathon'
    result_dict = {
        'tip': monitoring_tools.get_tip(framework, name, instance, soa_dir),
        'notification_email': monitoring_tools.get_notification_email(framework, name, instance, soa_dir),
        'irc_channels': monitoring_tools.get_irc_channels(framework, name, instance, soa_dir),
        'alert_after': '6m',
        'check_every': '2m',
        'realert_every': -1,
        'source': 'mesos-%s' % marathon_tools.get_cluster()
    }
    pysensu_yelp.send_event(check_name, runbook, status, output, team, **result_dict)


def get_main_marathon_config():
    log.debug("Reading marathon configuration")
    marathon_config = marathon_tools.get_config()
    log.info("Marathon config is: %s", marathon_config)
    return marathon_config


def get_marathon_client(url, user, passwd):
    """Get a new marathon client connection in the form of a MarathonClient object.

    :param url: The url to connect to marathon at
    :param user: The username to connect with
    :param passwd: The password to connect with
    :returns: A new marathon.MarathonClient object"""
    log.info("Connecting to Marathon server at: %s", url)
    return MarathonClient(url, user, passwd, timeout=35)


def deploy_service(name, config, client, namespace, bounce_method):
    """Deploy the service to marathon, either directly or via a bounce if needed.
    Called by setup_service when it's time to actually deploy.

    :param name: The full marathon job name to deploy
    :param config: The complete configuration dict to send to marathon
    :param client: A MarathonClient object
    :param namespace: The service's Smartstack namespace
    :param bounce_method: The bounce method to use, if needed
    :returns: A tuple of (status, output) to be used with send_sensu_event"""
    log.info("Deploying service instance %s with bounce_method %s", name, bounce_method)
    log.debug("Searching for old service instance iterations")
    filter_name = marathon_tools.remove_tag_from_job_id(name)
    app_list = client.list_apps()
    old_app_ids = [app.id for app in app_list if filter_name in app.id]
    if old_app_ids:  # there's a previous version; bounce
        log.info("Old service instance iterations found: %s", old_app_ids)
        try:
            if bounce_method == "brutal":
                bounce_lib.brutal_bounce(old_app_ids, config, client, namespace)
            elif bounce_method == "crossover":
                bounce_lib.crossover_bounce(old_app_ids, config, client, namespace)
            else:
                log.error("bounce_method not recognized: %s. Exiting", bounce_method)
                return (1, "bounce_method not recognized: %s" % bounce_method)
        except IOError:
            log.error("Namespace %s already being bounced. Exiting", filter_name)
            return (1, "Service is taking a while to bounce")
    else:  # there wasn't a previous version; just deploy it
        log.info("No old instances found. Deploying instance %s", name)
        bounce_lib.create_marathon_app(config, client)
    log.info("%s deployed. Exiting", name)
    return (0, 'Service deployed.')


def setup_service(service_name, instance_name, client, marathon_config,
                  service_marathon_config):
    """Setup the service instance given and attempt to deploy it, if possible.
    Doesn't do anything if the service is already in Marathon and hasn't changed.
    If it's not, attempt to find old instances of the service and bounce them.

    :param service_name: The service name to setup
    :param instance_name: The instance of the service to setup
    :param client: A MarathonClient object
    :param marathon_config: The marathon configuration dict
    :param service_marathon_config: The service instance's configuration dict
    :returns: A tuple of (status, output) to be used with send_sensu_event"""
    partial_id = marathon_tools.compose_job_id(service_name, instance_name)
    log.info("Setting up instance %s for service %s", instance_name, service_name)
    docker_url = marathon_tools.get_docker_url(marathon_config['docker_registry'],
                                               service_marathon_config['docker_image'])
    if not docker_url:
        error_msg = "Docker image for {0}.{1} not in deployments.json (or marathon config) Exiting.".format(
                  service_name, instance_name)
        log.error(error_msg)
        return (1, error_msg)
    complete_config = marathon_tools.create_complete_config(partial_id, docker_url,
                                                            marathon_config['docker_volumes'],
                                                            service_marathon_config)
    config_hash = marathon_tools.get_config_hash(complete_config)
    full_id = marathon_tools.compose_job_id(service_name, instance_name, config_hash)
    namespace = service_marathon_config.get('nerve_ns', instance_name)
    complete_config['id'] = full_id
    log.info("Desired Marathon instance id: %s", full_id)
    try:
        log.info("Checking if instance with hash %s already exists", config_hash)
        client.get_app(full_id)
        log.warning("App id %s already exists. Skipping configuration and exiting.", full_id)
        return (0, 'Service was already deployed.')
    except NotFoundError:
        return deploy_service(full_id, complete_config, client, namespace,
                              marathon_tools.get_bounce_method(service_marathon_config))
    except InternalServerError as e:
        log.error('Marathon had an internal server error: %s' % str(e))
        return(1, str(e))


def main():
    """Attempt to set up the marathon service instance given.
    Exits 1 if the deployment failed.
    This is done in the following order:

    - Load the marathon configuration
    - Connect to marathon
    - Load the service instance's configuration
    - Create the complete marathon job configuration
    - Deploy/bounce the service
    - Emit an event about the deployment to sensu"""
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
            send_event(service_name, instance_name, soa_dir, sensu_status, output)
            sys.exit(status)
        except (KeyError, TypeError, AttributeError):
            import traceback
            error_str = traceback.format_exc()
            log.error(error_str)
            send_event(service_name, instance_name, soa_dir, pysensu_yelp.Status.CRITICAL, error_str)
            sys.exit(1)
    else:
        error_msg = "Could not read marathon configuration file for %s in cluster %s" % \
                    (args.service_instance, marathon_config['cluster'])
        log.error(error_msg)
        send_event(service_name, instance_name, soa_dir, pysensu_yelp.Status.CRITICAL, error_msg)
        sys.exit(1)


if __name__ == "__main__":
    main()
