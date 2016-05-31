#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Usage: ./cleanup_marathon_jobs.py [options]

Clean up marathon apps that aren't supposed to run on this cluster by deleting them.

Gets the current app list from marathon, and then a 'valid_app_list'
via utils.get_services_for_cluster

If an app in the marathon app list isn't in the valid_app_list, it's
deleted.

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -v, --verbose: Verbose output
"""
import argparse
import logging
import traceback

import pysensu_yelp

from paasta_tools import bounce_lib
from paasta_tools import marathon_tools
from paasta_tools.monitoring_tools import send_event
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import load_system_paasta_config


log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='Cleans up stale marathon jobs.')
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest="verbose", default=False)
    args = parser.parse_args()
    return args


def delete_app(app_id, client, soa_dir):
    """Deletes a marathon app safely and logs to notify the user that it
    happened"""
    log.warn("%s appears to be old; attempting to delete" % app_id)
    service, instance, _, __ = marathon_tools.deformat_job_id(app_id)
    cluster = load_system_paasta_config().get_cluster()
    try:
        short_app_id = marathon_tools.compose_job_id(service, instance)
        with bounce_lib.bounce_lock_zookeeper(short_app_id):
            bounce_lib.delete_marathon_app(app_id, client)
        send_event(
            service=service,
            check_name='check_marathon_services_replication.%s' % short_app_id,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.OK,
            overrides={},
            output="This instance was removed and is no longer running",
        )
        send_event(
            service=service,
            check_name='setup_marathon_job.%s' % short_app_id,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.OK,
            overrides={},
            output="This instance was removed and is no longer running",
        )
        send_event(
            service=service,
            check_name='paasta_bounce_progress.%s' % short_app_id,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.OK,
            overrides={},
            output="This instance was removed and is no longer running",
        )
        log_line = "Deleted stale marathon job that looks lost: %s" % app_id
        _log(
            service=service,
            component='deploy',
            level='event',
            cluster=cluster,
            instance=instance,
            line=log_line,
        )
    except IOError:
        log.debug("%s is being bounced, skipping" % app_id)
    except Exception:
        loglines = ['Exception raised during cleanup of service %s:' % service]
        loglines.extend(traceback.format_exc().rstrip().split("\n"))
        for logline in loglines:
            _log(
                service=service,
                component='deploy',
                level='debug',
                cluster=load_system_paasta_config().get_cluster(),
                instance=instance,
                line=logline,
            )
        raise


def cleanup_apps(soa_dir):
    """Clean up old or invalid jobs/apps from marathon. Retrieves
    both a list of apps currently in marathon and a list of valid
    app ids in order to determine what to kill.

    :param soa_dir: The SOA config directory to read from"""
    log.info("Loading marathon configuration")
    marathon_config = marathon_tools.load_marathon_config()
    log.info("Connecting to marathon")
    client = marathon_tools.get_marathon_client(marathon_config.get_url(), marathon_config.get_username(),
                                                marathon_config.get_password())

    valid_services = get_services_for_cluster(instance_type='marathon', soa_dir=soa_dir)
    running_app_ids = marathon_tools.list_all_marathon_app_ids(client)

    for app_id in running_app_ids:
        log.debug("Checking app id %s", app_id)
        try:
            service, instance, _, __ = marathon_tools.deformat_job_id(app_id)
        except InvalidJobNameError:
            log.warn("%s doesn't conform to paasta naming conventions? Skipping." % app_id)
            continue
        if (service, instance) not in valid_services:
            delete_app(
                app_id=app_id,
                client=client,
                soa_dir=soa_dir,
            )


def main():
    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    cleanup_apps(soa_dir)


if __name__ == "__main__":
    main()
