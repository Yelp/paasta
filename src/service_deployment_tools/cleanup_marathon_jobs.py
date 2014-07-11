import argparse
import logging
# import os

import service_configuration_lib
import marathon_tools
from marathon import MarathonClient


ID_SPACER = marathon_tools.ID_SPACER
log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='Creates marathon jobs.')
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest="verbose", default=False)
    args = parser.parse_args()
    return args


def get_marathon_client(url, user, passwd):
    """Get a new marathon client connection in the form of a MarathonClient object.

    Connects to the Marathon server at 'url' with login specified
    by 'user' and 'pass', all from the marathon config."""
    log.info("Connecting to Marathon server at: %s", url)
    return MarathonClient(url, user, passwd)


def cleanup_apps(client, soa_dir):
    valid_app_list = marathon_tools.get_marathon_services_for_cluster(soa_dir=soa_dir,
                                                                      include_iteration=True)
    app_ids = [app.id for app in client.list_apps()]
    for app_id in app_ids:
        if not any([app_id == marathon_tools.compose_job_id(service, instance, iteration)
                    for service, instance, iteration in valid_app_list]):
            try:
                srv_instance = marathon_tools.remove_iteration_from_job_id(app_id)
                with marathon_tools.bounce_lock(srv_instance):
                    client.delete_app(app_id)
            except IOError:
                continue  # It's being bounced, don't touch it!


def main():
    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)
    marathon_config = marathon_tools.get_config()
    client = get_marathon_client(marathon_config['url'], marathon_config['user'],
                                 marathon_config['pass'])
    cleanup_apps(client, soa_dir)


if __name__ == "__main__":
    main()