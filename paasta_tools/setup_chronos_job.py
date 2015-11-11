#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
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
Usage: ./setup_chronos_job.py <service.instance> [options]

Deploy a service instance to Chronos from a configuration file.
Reads from the soa_dir /nail/etc/services by default.

This script will attempt to load a service's configuration
from the soa_dir and generate a chronos job configuration for it,
as well as handle deploying that configuration if there's an old version of the service.

To determine whether or not a deployment is 'old', each chronos job has a complete id of
service.instance.configuration_hash, where configuration_hash
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
import argparse
import logging
import pysensu_yelp
import sys

import service_configuration_lib

from paasta_tools import chronos_tools
from paasta_tools import monitoring_tools
from paasta_tools.utils import _log
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import configure_log
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import SPACER


log = logging.getLogger('__main__')
logging.basicConfig()


def parse_args():
    parser = argparse.ArgumentParser(description='Creates chronos jobs.')
    parser.add_argument('service_instance',
                        help="The chronos instance of the service to create or update",
                        metavar=compose_job_id("SERVICE", "INSTANCE"))
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest="verbose", default=False)
    args = parser.parse_args()
    return args


def send_event(service, instance, soa_dir, status, output):
    """Send an event to sensu via pysensu_yelp with the given information.

    :param service: The service name the event is about
    :param instance: The instance of the service the event is about
    :param soa_dir: The service directory to read monitoring information from
    :param status: The status to emit for this event
    :param output: The output to emit for this event
    """
    cluster = load_system_paasta_config().get_cluster()
    monitoring_overrides = chronos_tools.load_chronos_job_config(
        service=service,
        instance=instance,
        cluster=cluster,
        soa_dir=soa_dir,
    ).get_monitoring()
    # In order to let sensu know how often to expect this check to fire,
    # we need to set the ``check_every`` to the frequency of our cron job, which
    # is 10s.
    monitoring_overrides['check_every'] = '10s'
    # Most deploy_chrono_jobs failures are transient and represent issues
    # that will probably be fixed eventually, so we set an alert_after
    # to suppress extra noise
    monitoring_overrides['alert_after'] = '10m'
    check_name = 'setup_chronos_job.%s' % compose_job_id(service, instance)
    monitoring_tools.send_event(
        service=service,
        check_name=check_name,
        overrides=monitoring_overrides,
        status=status,
        output=output,
        soa_dir=soa_dir,
    )


def bounce_chronos_job(
    service,
    instance,
    cluster,
    jobs_to_disable,
    jobs_to_delete,
    job_to_create,
    client
):
    if any([jobs_to_disable, jobs_to_delete, job_to_create]):
        log_line = "Chronos bouncing. Jobs to disable: %s, jobs to delete: %s, job_to_create: %s" % (
            jobs_to_disable, jobs_to_delete, job_to_create)
        _log(service=service, instance=instance, component='deploy',
             cluster=cluster, level='debug', line=log_line)
    else:
        log.debug("Not doing any chronos bounce action for %s" % chronos_tools.compose_job_id(
            service, instance))
    for job in jobs_to_disable:
        chronos_tools.disable_job(client=client, job=job)
    for job in jobs_to_delete:
        chronos_tools.delete_job(client=client, job=job)
    if job_to_create:
        chronos_tools.create_job(client=client, job=job_to_create)
        log_line = 'Created new Chronos job: %s' % job_to_create['name']
        _log(service=service, instance=instance, component='deploy',
             cluster=cluster, level='event', line=log_line)
    return (0, "All chronos bouncing tasks finished.")


def setup_job(service, instance, chronos_job_config, complete_job_config, client, cluster):
    job_id = complete_job_config['name']
    # Sort this initial list since other lists of jobs will come from it (with
    # their orders preserved by list comprehensions) and since we'll care about
    # ordering by recency when we go calculate jobs_to_delete.
    all_existing_jobs = chronos_tools.sort_jobs(chronos_tools.lookup_chronos_jobs(
        service=service,
        instance=instance,
        client=client,
    ))
    old_jobs = [job for job in all_existing_jobs if job["name"] != job_id]
    enabled_old_jobs = [job for job in old_jobs if not job["disabled"]]

    if all_existing_jobs == old_jobs:
        job_to_create = complete_job_config
    else:
        job_to_create = None

    number_of_old_jobs_to_keep = 5
    return bounce_chronos_job(
        service=service,
        instance=instance,
        cluster=cluster,
        jobs_to_disable=enabled_old_jobs,
        jobs_to_delete=old_jobs[number_of_old_jobs_to_keep:],
        job_to_create=job_to_create,
        client=client,
    )


def main():
    configure_log()
    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.WARNING)
    try:
        service, instance, _, __ = decompose_job_id(args.service_instance)
    except InvalidJobNameError:
        log.error("Invalid service instance '%s' specified. Format is service%sinstance."
                  % (args.service_instance, SPACER))
        sys.exit(1)

    client = chronos_tools.get_chronos_client(chronos_tools.load_chronos_config())
    cluster = load_system_paasta_config().get_cluster()

    try:
        chronos_job_config = chronos_tools.load_chronos_job_config(
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=soa_dir,
        )
    except (NoDeploymentsAvailable, NoDockerImageError):
        error_msg = "No deployment found for %s in cluster %s. Has Jenkins run for it?" % (
            args.service_instance, cluster)
        send_event(
            service=service,
            instance=None,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.CRITICAL,
            output=error_msg,
        )
        log.error(error_msg)
        # exit 0 because the event was sent to the right team and this is not an issue with Paasta itself
        sys.exit(0)
    except chronos_tools.InvalidChronosConfigError as e:
        error_msg = (
            "Could not read chronos configuration file for %s in cluster %s\n" % (args.service_instance, cluster) +
            "Error was: %s" % str(e))
        log.error(error_msg)
        send_event(
            service=service,
            instance=instance,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.CRITICAL,
            output=error_msg,
        )
        # exit 0 because the event was sent to the right team and this is not an issue with Paasta itself
        sys.exit(0)

    complete_job_config = chronos_tools.create_complete_config(
        service=service,
        job_name=instance,
        soa_dir=soa_dir,
    )
    status, output = setup_job(
        service=service,
        instance=instance,
        cluster=cluster,
        chronos_job_config=chronos_job_config,
        complete_job_config=complete_job_config,
        client=client,
    )
    sensu_status = pysensu_yelp.Status.CRITICAL if status else pysensu_yelp.Status.OK
    send_event(
        service=service,
        instance=instance,
        soa_dir=soa_dir,
        status=sensu_status,
        output=output,
    )
    # We exit 0 because the script finished ok and the event was sent to the right team.
    sys.exit(0)


if __name__ == "__main__":
    main()
