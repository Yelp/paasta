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
from paasta_tools.chronos_serviceinit import restart_chronos_job
from paasta_tools.utils import _log
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import configure_log
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoDeploymentsAvailable
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


def _setup_existing_job(service, instance, cluster, job_id, existing_job, complete_job_config, client):
    desired_state = 'stop' if complete_job_config['disabled'] else 'start'
    # Do nothing if job state doesn't need to change, otherwise update job with new state
    if complete_job_config['disabled'] == existing_job['disabled']:
        output = "Job '%s' state is already setup and set to '%s'" % (job_id, desired_state)
    else:
        state_change = 'Disabled' if complete_job_config['disabled'] else 'Enabled'
        client.update(complete_job_config)
        output = "%s job '%s'" % (state_change, job_id)
        _log(service=service, instance=instance, component='deploy',
             cluster=cluster, level='event', line=output)
    log.info(output)
    return (0, output)


def _setup_new_job(service, instance, cluster, job_id, previous_jobs, complete_job_config, client):
    # The job hash has changed so we disable the old jobs and start a new one
    for previous_job in previous_jobs:
        previous_job['disabled'] = True
        client.update(previous_job)
        log_line = "Disabling old job %s to make way for a new chronos job." % previous_job['name']
        _log(service=service, instance=instance, component='deploy',
             cluster=cluster, level='event', line=log_line)

    client.add(complete_job_config)
    output = "Deployed new chronos job: '%s'" % job_id
    _log(service=service, instance=instance, component='deploy',
         cluster=cluster, level='event', line=output)
    return (0, "Deployed job '%s'" % job_id)


def setup_job(service, instance, chronos_job_config, complete_job_config, client, cluster):
    job_prefix = chronos_tools.compose_job_id(service, instance)
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
            log.debug("Gracefully bouncing %s because it has existing jobs" % compose_job_id(service, instance))
            return _setup_existing_job(
                service=service,
                instance=instance,
                cluster=cluster,
                job_id=job_id,
                existing_job=existing_jobs[0],
                complete_job_config=complete_job_config,
                client=client,
            )
        else:
            log.debug("Setting up %s as a new job because it has no existing jobs" % compose_job_id(service, instance))
            return _setup_new_job(
                service=service,
                instance=instance,
                cluster=cluster,
                job_id=job_id,
                previous_jobs=matching_jobs,
                complete_job_config=complete_job_config,
                client=client,
            )
    elif bounce_method == 'brutal':
        restart_chronos_job(
            service=service,
            instance=instance,
            job_id=job_id,
            client=client,
            cluster=cluster,
            matching_jobs=matching_jobs,
            job_config=complete_job_config,
        )
        return (0, "Job '%s' bounced using the 'brutal' method" % job_id)
    else:
        return (1, ("ERROR: bounce_method '%s' not recognized. Must be one of (%s)."
                    % (bounce_method, ', '.join(chronos_tools.VALID_BOUNCE_METHODS))))


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
    except NoDeploymentsAvailable:
        error_msg = "No deployments found for %s in cluster %s" % (args.service_instance, cluster)
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
