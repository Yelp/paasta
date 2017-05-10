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
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import copy
import logging
import sys

import pysensu_yelp

from paasta_tools import chronos_tools
from paasta_tools import monitoring_tools
from paasta_tools.mesos.exceptions import NoSlavesAvailableError
from paasta_tools.utils import _log
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import SPACER


log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='Creates chronos jobs.')
    parser.add_argument('service_instance',
                        help="The chronos instance of the service to create or update",
                        metavar=compose_job_id("SERVICE", "INSTANCE"))
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=chronos_tools.DEFAULT_SOA_DIR,
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
    try:
        monitoring_overrides = chronos_tools.load_chronos_job_config(
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=soa_dir,
            load_deployments=False,
        ).get_monitoring()
    except NoConfigurationForServiceError:
        monitoring_overrides = {}
    # In order to let sensu know how often to expect this check to fire,
    # we need to set the ``check_every`` to the frequency of our cron job, which
    # is 10s.
    monitoring_overrides['check_every'] = '10s'
    # Most deploy_chronos_jobs failures are transient and represent issues
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
    job_to_update,
    client
):
    if job_to_update:
        log_line = 'Job to update: %s' % job_to_update
        _log(service=service, instance=instance, component='deploy',
             cluster=cluster, level='debug', line=log_line)
        chronos_tools.update_job(client=client, job=job_to_update)
        log_line = 'Updated Chronos job: %s' % job_to_update['name']
        _log(service=service, instance=instance, component='deploy',
             cluster=cluster, level='event', line=log_line)

    return (0, "All chronos bouncing tasks finished.")


def setup_job(service, instance, complete_job_config, client, cluster):
    # There should only ever be *one* job for a given service_instance
    all_existing_jobs = chronos_tools.lookup_chronos_jobs(
        service=service,
        instance=instance,
        client=client,
        include_disabled=True,
    )

    job_to_update = None
    if len(all_existing_jobs) > 0:
        # we store the md5 sum of the config in the description field.
        if all_existing_jobs[0]['description'] != complete_job_config['description']:
            job_to_update = complete_job_config
    else:
        job_to_update = complete_job_config

    return bounce_chronos_job(
        service=service,
        instance=instance,
        cluster=cluster,
        job_to_update=job_to_update,
        client=client,
    )


def config_with_historical_stats(chronos_client, service, instance, job_config):
    """
    Given a complete_job_config, return a new chronos_job_config
    to be posted with the historical 'stats' for the job are merged
    into the new config to be posted. If the job is 'new', that is,
    there is no job currently deployed to Chronos for the service+instance
    in soa-configs, these values are left empty.

    :param chrons_client: a chronos-python client object
    :param service: the service for the job
    :param instance: the instance for the job
    :param job_config: the job config to be modified
    :returns: a modified job config, with the historcal stats set
    """
    existing_matching_jobs = chronos_tools.lookup_chronos_jobs(
        client=chronos_client,
        service=service,
        instance=instance,
        include_disabled=True,
        include_temporary=False,
    )
    if existing_matching_jobs:
        # assume there is only one of them
        existing_job = existing_matching_jobs[0]
        # these keys should always exist, but I guess it's good to be
        # defensive
        historical_state = {
            'lastSuccess': existing_job.get('lastSuccess', ''),
            'lastError': existing_job.get('lastError', ''),
            'successCount': existing_job.get('successCount', 0),
            'errorCount': existing_job.get('errorCount', 0),
        }
        copied = copy.deepcopy(job_config)
        copied.update(historical_state)
        return copied
    return job_config


def main():
    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    try:
        service, instance, _, __ = decompose_job_id(args.service_instance, spacer=chronos_tools.INTERNAL_SPACER)
    except InvalidJobNameError:
        log.error("Invalid service instance '%s' specified. Format is service%sinstance."
                  % (args.service_instance, SPACER))
        sys.exit(1)

    client = chronos_tools.get_chronos_client(chronos_tools.load_chronos_config())
    cluster = load_system_paasta_config().get_cluster()

    try:
        complete_job_config = chronos_tools.create_complete_config(
            service=service,
            job_name=instance,
            soa_dir=soa_dir,
        )
    except (NoDeploymentsAvailable, NoDockerImageError):
        error_msg = "No deployment found for %s in cluster %s. Has Jenkins run for it?" % (
            args.service_instance, cluster)
        send_event(
            service=service,
            instance=instance,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.CRITICAL,
            output=error_msg,
        )
        log.error(error_msg)
        sys.exit(0)
    except NoConfigurationForServiceError as e:
        error_msg = (
            "Could not read chronos configuration file for %s in cluster %s\n" % (args.service_instance, cluster) +
            "Error was: %s" % str(e))
        send_event(
            service=service,
            instance=instance,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.CRITICAL,
            output=error_msg,
        )
        log.error(error_msg)
        sys.exit(0)
    except NoSlavesAvailableError as e:
        error_msg = (
            "There are no PaaSTA slaves that can run %s in cluster %s\n" % (args.service_instance, cluster) +
            "Double check the cluster and the configured constratints/pool/whitelist.\n"
            "Error was: %s" % str(e))
        send_event(
            service=service,
            instance=instance,
            soa_dir=soa_dir,
            status=pysensu_yelp.Status.CRITICAL,
            output=error_msg,
        )
        log.error(error_msg)
        sys.exit(0)
    except chronos_tools.InvalidParentError:
        log.warn("Skipping %s.%s: Parent job could not be found" % (service, instance))
        sys.exit(0)

    modified_config = config_with_historical_stats(
        chronos_client=client,
        service=service,
        instance=instance,
        job_config=complete_job_config
    )

    status, output = setup_job(
        service=service,
        instance=instance,
        cluster=cluster,
        complete_job_config=modified_config,
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
