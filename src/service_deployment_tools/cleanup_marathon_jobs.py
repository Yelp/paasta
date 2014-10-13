#!/usr/bin/env python
"""
Usage: ./cleanup_marathon_jobs.py [options]

Clean up marathon jobs/apps that aren't current (i.e. some part of the
final marathon job configuration has changed) by deleting them.

Gets the current job/app list from marathon, and then a 'valid_app_list'
via marathon_tools.get_marathon_services_for_cluster and then
compiling the complete marathon job configuration for that service and
calculating the MD5 hash that would've been appended to that service.

If a job/app in the marathon app list isn't in the valid_app_list, it's
deleted.

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -v, --verbose: Verbose output
"""
import argparse
import logging

import service_configuration_lib
from service_deployment_tools import marathon_tools
from service_deployment_tools import bounce_lib
from marathon import MarathonClient


ID_SPACER = marathon_tools.ID_SPACER
log = logging.getLogger('__main__')


def parse_args():
    parser = argparse.ArgumentParser(description='Cleans up stale marathon jobs.')
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest="verbose", default=False)
    args = parser.parse_args()
    return args


def get_marathon_client(url, user, passwd):
    """Get a new marathon client connection in the form of a MarathonClient object.

    :param url: The url to connect to marathon at
    :param user: The username to connect with
    :param passwd: The password to connect with
    :returns: A new marathon.MarathonClient object"""
    log.info("Connecting to Marathon server at: %s", url)
    return MarathonClient(url, user, passwd, timeout=35)


def get_valid_app_list(marathon_config, soa_dir):
    """Get the 'valid_app_list' by calling marathon_tools.get_marathon_services_for_cluster
    and then calculating each service's current configuration via its soa_dir configs in
    order to get the current MD5 hash that should be on an app's id.

    :param marathon_config: A dict of the marathon_configuration
    :param soa_dir: The SOA config directory to read from
    :returns: A list of valid app ids"""
    log.info("Getting desired app list from configs")
    cluster_app_list = marathon_tools.get_marathon_services_for_cluster(soa_dir=soa_dir)
    valid_app_list = []
    for name, instance in cluster_app_list:
        partial_id = marathon_tools.compose_job_id(name, instance)
        config = marathon_tools.read_service_config(name, instance, soa_dir=soa_dir)
        docker_url = marathon_tools.get_docker_url(marathon_config['docker_registry'],
                                                   config['docker_image'],
                                                   verify=False)
        complete_config = marathon_tools.create_complete_config(partial_id, docker_url,
                                                                marathon_config['docker_volumes'],
                                                                config)
        config_hash = marathon_tools.get_config_hash(complete_config)
        full_id = marathon_tools.compose_job_id(name, instance, config_hash)
        valid_app_list.append(full_id)
    return valid_app_list


def cleanup_apps(soa_dir):
    """Clean up old or invalid jobs/apps from marathon. Retrieves
    both a list of apps currently in marathon and a list of valid
    app ids in order to determine what to kill.

    :param soa_dir: The SOA config directory to read from"""
    log.info("Loading marathon configuration")
    marathon_config = marathon_tools.get_config()
    log.info("Connecting to marathon")
    client = get_marathon_client(marathon_config['url'], marathon_config['user'],
                                 marathon_config['pass'])
    valid_app_list = get_valid_app_list(marathon_config, soa_dir)
    app_ids = marathon_tools.list_all_marathon_app_ids(client)
    for app_id in app_ids:
        log.info("Checking app id %s", app_id)
        if not any([app_id == deployed_id for deployed_id in valid_app_list]):
            try:
                log.warn("%s appears to be old; attempting to delete", app_id)
                srv_instance = marathon_tools.remove_tag_from_job_id(app_id).replace('--', '_')
                with bounce_lib.bounce_lock_zookeeper(srv_instance):
                    bounce_lib.delete_marathon_app(app_id, client)
            except IOError:
                log.info("%s is being bounced, skipping", app_id)
                continue  # It's being bounced, don't touch it!


def main():
    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)
    cleanup_apps(soa_dir)


if __name__ == "__main__" and marathon_tools.is_mesos_leader():
    main()
