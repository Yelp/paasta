#!/usr/bin/env python
"""
Usage: ./cleanup_marathon_jobs.py [options]

Clean up marathon apps that aren't supposed to run on this cluster by deleting them.

Gets the current app list from marathon, and then a 'valid_app_list'
via marathon_tools.get_marathon_services_for_cluster

If an app in the marathon app list isn't in the valid_app_list, it's
deleted.

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -v, --verbose: Verbose output
"""
import argparse
import logging
import sys

import service_configuration_lib
from paasta_tools import marathon_tools
from paasta_tools import bounce_lib
from paasta_tools.utils import _log


ID_SPACER = marathon_tools.ID_SPACER
log = logging.getLogger('__main__')
log.addHandler(logging.StreamHandler(sys.stdout))


def parse_args():
    parser = argparse.ArgumentParser(description='Cleans up stale marathon jobs.')
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest="verbose", default=False)
    args = parser.parse_args()
    return args


def delete_app(app_id, client):
    """Deletes a marathon app safely and logs to notify the user that it
    happened"""
    short_app_id = marathon_tools.remove_tag_from_job_id(app_id)
    log.warn("%s appears to be old; attempting to delete" % app_id)
    srv_instance = short_app_id.replace('--', '_')
    service_name = srv_instance.split('.')[0]
    instance = srv_instance.split('.')[1]
    try:
        with bounce_lib.bounce_lock_zookeeper(srv_instance):
            bounce_lib.delete_marathon_app(app_id, client)
            log_line = "Deleted stale marathon job that looks lost: %s" % app_id
            _log(service_name=service_name,
                 component='deploy',
                 level='event',
                 cluster=marathon_tools.get_cluster(),
                 instance=instance,
                 line=log_line)
    except IOError:
        log.debug("%s is being bounced, skipping" % app_id)


def cleanup_apps(soa_dir):
    """Clean up old or invalid jobs/apps from marathon. Retrieves
    both a list of apps currently in marathon and a list of valid
    app ids in order to determine what to kill.

    :param soa_dir: The SOA config directory to read from"""
    log.info("Loading marathon configuration")
    marathon_config = marathon_tools.get_config()
    log.info("Connecting to marathon")
    client = marathon_tools.get_marathon_client(marathon_config['url'], marathon_config['user'],
                                                marathon_config['pass'])

    valid_services = marathon_tools.get_marathon_services_for_cluster(soa_dir=soa_dir)
    valid_short_app_ids = [marathon_tools.compose_job_id(name, instance) for (name, instance) in valid_services]
    running_app_ids = marathon_tools.list_all_marathon_app_ids(client)

    for app_id in running_app_ids:
        log.debug("Checking app id %s", app_id)
        try:
            short_app_id = marathon_tools.remove_tag_from_job_id(app_id)
        except IndexError:
            log.warn("%s doesn't conform to paasta naming conventions? Skipping." % app_id)
            continue
        if short_app_id not in valid_short_app_ids:
            delete_app(app_id, client)


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
