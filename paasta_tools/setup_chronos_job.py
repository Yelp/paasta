#!/usr/bin/env python
"""
Usage: ./setup_chronos_job.py <service_name.instance_name> [options]

Deploy a service instance to Chronos from a configuration file.
Reads from the soa_dir /nail/etc/services by default.

This script will attempt to load a service's configuration
from the soa_dir and generate a chronos job configuration for it,
as well as handle deploying that configuration if there's an old version of the service.

To determine whether or not a deployment is 'old', each chronos job has a complete id of
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
import argparse
import logging
import pysensu_yelp
import sys

import service_configuration_lib

from paasta_tools import chronos_tools
from paasta_tools import monitoring_tools
from paasta_tools.chronos_serviceinit import restart_chronos_job
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import configure_log
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import SPACER


log = logging.getLogger('__main__')
log.addHandler(logging.StreamHandler(sys.stdout))


def parse_args():
    parser = argparse.ArgumentParser(description='Creates chronos jobs.')
    parser.add_argument('service_instance',
                        help="The chronos instance of the service to create or update",
                        metavar="SERVICE%sINSTANCE" % SPACER)
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
    :param output: The output to emit for this event
    """
    cluster = load_system_paasta_config().get_cluster()
    monitoring_overrides = chronos_tools.load_chronos_job_config(name, instance, cluster).get_monitoring()
    check_name = 'setup_chronos_job.%s' % compose_job_id(name, instance, spacer=chronos_tools.INTERNAL_SPACER)
    monitoring_tools.send_event(name, check_name, monitoring_overrides, status, output, soa_dir)


def _setup_existing_job(job_id, existing_job, complete_job_config, client):
    desired_state = 'stop' if complete_job_config['disabled'] else 'start'
    # Do nothing if job state doesn't need to change, otherwise update job with new state
    if complete_job_config['disabled'] == existing_job['disabled']:
        output = "Job '%s' state is already set to '%s'" % (job_id, desired_state)
    else:
        state_change = 'Disabled' if complete_job_config['disabled'] else 'Enabled'
        client.update(complete_job_config)
        output = "%s job '%s'" % (state_change, job_id)
    return (0, output)


def _setup_new_job(job_id, previous_jobs, complete_job_config, client):
    # The job hash has changed so we disable the old jobs and start a new one
    for previous_job in previous_jobs:
        previous_job['disabled'] = True
        client.update(previous_job)

    client.add(complete_job_config)
    return (0, "Deployed job '%s'" % job_id)


def setup_job(service_name, instance_name, chronos_job_config, complete_job_config, client, cluster):
    job_prefix = compose_job_id(service_name, instance_name, spacer=chronos_tools.SPACER)
    job_id = complete_job_config['name']
    existing_jobs = chronos_tools.lookup_chronos_jobs(
        r'^%s$' % job_id,
        client,
        max_expected=1,
    )
    matching_jobs = chronos_tools.lookup_chronos_jobs(
        r'^%s%s' % (job_prefix, chronos_tools.SPACER),
        client,
        include_disabled=True,
    )

    bounce_method = chronos_job_config.get_bounce_method()
    if bounce_method == 'graceful':
        if len(existing_jobs) > 0:
            return _setup_existing_job(job_id, existing_jobs[0], complete_job_config, client)
        else:
            return _setup_new_job(job_id, matching_jobs, complete_job_config, client)
    elif bounce_method == 'brutal':
        restart_chronos_job(service_name, instance_name, job_id, client, cluster, matching_jobs, complete_job_config)
        return (0, "Job '%s' bounced using the 'brutal' method" % job_id)
    else:
        return (1, ("ERROR: bounce_method '%s' not recognized. Must be one of (%s)."
                    % (bounce_method, ', '.join(chronos_tools.VALID_BOUNCE_METHODS))))


def main():
    configure_log()
    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)
    try:
        service_name, instance_name, _ = decompose_job_id(args.service_instance)
    except InvalidJobNameError:
        log.error("Invalid service instance '%s' specified. Format is service_name%sinstance_name."
                  % (args.service_instance, SPACER))
        sys.exit(1)

    client = chronos_tools.get_chronos_client(chronos_tools.load_chronos_config())
    cluster = load_system_paasta_config().get_cluster()

    try:
        chronos_job_config = chronos_tools.load_chronos_job_config(
            service_name,
            instance_name,
            cluster,
            soa_dir=soa_dir,
        )
    except NoDeploymentsAvailable:
        error_msg = "No deployments found for %s in cluster %s" % (args.service_instance, cluster)
        send_event(service_name, None, soa_dir, pysensu_yelp.Status.CRITICAL, error_msg)
        log.error(error_msg)
        # exit 0 because the event was sent to the right team and this is not an issue with Paasta itself
        sys.exit(0)
    except chronos_tools.InvalidChronosConfigError as e:
        error_msg = (
            "Could not read chronos configuration file for %s in cluster %s\n" % (args.service_instance, cluster) +
            "Error was: %s" % str(e))
        log.error(error_msg)
        send_event(service_name, instance_name, soa_dir, pysensu_yelp.Status.CRITICAL, error_msg)
        # exit 0 because the event was sent to the right team and this is not an issue with Paasta itself
        sys.exit(0)

    complete_job_config = chronos_tools.create_complete_config(service_name, instance_name, soa_dir=soa_dir)
    status, output = setup_job(service_name, instance_name, chronos_job_config, complete_job_config, client, cluster)
    sensu_status = pysensu_yelp.Status.CRITICAL if status else pysensu_yelp.Status.OK
    send_event(service_name, instance_name, soa_dir, sensu_status, output)
    print status, output
    # We exit 0 because the script finished ok and the event was sent to the right team.
    sys.exit(0)


if __name__ == "__main__":
    main()
