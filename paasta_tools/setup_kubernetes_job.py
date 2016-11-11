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
Usage: ./setup_kubernetes_job.py <service.instance> [options]

Deploy a service instance to Kubernetes from a configuration file.  Attempts to
load the kubernetes configuration at /etc/paasta/kubernetes.json, and read from
the soa_dir /nail/etc/services by default.

This script will attempt to load a service's configuration from the soa_dir and
generate a kubernetes job configuration for it, as well as handle deploying
that configuration with a bounce strategy if there's an old version of the
service. To determine whether or not a deployment is 'old', each kubernetes job
has a complete id of service.instance.configuration_hash, where
configuration_hash is an MD5 hash of the configuration dict to be sent to
kubernetes (without the configuration_hash in the id field, of course - we
change that after the hash is calculated).

The script will emit a sensu event based on how the deployment went- if
something went wrong, it'll alert the team responsible for the service (as
defined in that service's monitoring.yaml), and it'll send resolves when the
deployment goes alright.

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -v, --verbose: Verbose output
"""
import argparse
import logging
import sys
import traceback

import pysensu_yelp
import requests_cache

from paasta_tools import monitoring_tools
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import load_kubernetes_pod_config
from paasta_tools.utils import _log
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import InvalidInstanceConfig
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import SPACER

log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='Creates kubernetes jobs.')
    parser.add_argument(
        'service_instance_list',
        nargs='+',
        help="The list of kubernetes pods to create or update",
        metavar="SERVICE%sINSTANCE" % SPACER,
    )
    parser.add_argument(
        '-d',
        '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        dest="verbose",
        default=False,
    )
    args = parser.parse_args()
    return args


def send_event(
    name,
    instance,
    cluster,
    soa_dir,
    status,
    output,
):
    """
    Send an event to sensu via pysensu_yelp with the given information.

    :param name: The service name the event is about
    :param instance: The instance of the service the event is about
    :param soa_dir: The service directory to read monitoring information from
    :param status: The status to emit for this event
    :param output: The output to emit for this event
    """
    monitoring_overrides = load_kubernetes_pod_config(
        service=name,
        instance=instance,
        cluster=cluster,
        soa_dir=soa_dir,
        load_deployments=False,
    ).get_monitoring()
    monitoring_overrides['check_every'] = '10s'
    monitoring_overrides['alert_after'] = '10m'
    check_name = 'setup_kubernetes_job.%s' % compose_job_id(name, instance)
    monitoring_tools.send_event(name, check_name, monitoring_overrides, status, output, soa_dir)


def deploy_service(
    service,
    instance,
    config,
    client,
):
    """
    Deploy the service to kubernetes, either directly or via a bounce if needed.
    Called by setup_service when it's time to actually deploy.

    :param service: The name of the service to deploy
    :param instance: The instance of the service to deploy
    :param config: The complete configuration dict to send to kubernetes
    :param client: A KubeClient object
    :returns: A tuple of (status, output) to be used with send_sensu_event
    """

    def log_deploy_error(errormsg, level='event'):
        return _log(
            service=service,
            line=errormsg,
            component='deploy',
            level='event',
            cluster=cluster,
            instance=instance
        )

    system_paasta_config = load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()
    existing_deployments = client.get_matching_deployments(service, instance)

    already_deployed = False

    for deployment in existing_deployments:
        if deployment.obj['metadata']['name'] == config['metadata']['name']:
            already_deployed = True
        else:
            deployment.delete()

    if not already_deployed:
        client.create_deployment(config)

    return (0, 'Service deployed.')


def setup_service(
    service,
    instance,
    client,
    service_kubernetes_config,
):
    """
    Setup the service instance given and attempt to deploy it, if possible.
    Doesn't do anything if the service is already in Kubernetes and hasn't
    changed.  If it's not, attempt to find old instances of the service and
    bounce them.

    :param service: The service name to setup
    :param instance: The instance of the service to setup
    :param client: A KubeClient object
    :param service_kubernetes_config: The service instance's configuration dict
    :returns: A tuple of (status, output) to be used with send_sensu_event
    """

    log.info("Setting up instance %s for service %s", instance, service)
    try:
        kubernetes_pod_dict = service_kubernetes_config.format_kubernetes_pod_dict()
    except NoDockerImageError:
        error_msg = (
            "Docker image for {0}.{1} not in deployments.json. Exiting. Has Jenkins deployed it?\n"
        ).format(
            service,
            instance,
        )
        log.error(error_msg)
        return (1, error_msg)

    full_id = kubernetes_pod_dict['metadata']['labels']['id']

    log.info("Desired Kubernetes instance id: %s", full_id)
    return deploy_service(
        service=service,
        instance=instance,
        config=kubernetes_pod_dict,
        client=client,
    )


def main():
    """
    Attempt to set up a list of kubernetes service instances given.
    Exits 1 if any service.instance deployment failed.
    This is done in the following order:

    - Load the kubernetes configuration
    - Connect to kubernetes
    - Do the following for each service.instance:
        - Load the service instance's configuration
        - Create the complete kubernetes job configuration
        - Deploy/bounce the service
        - Emit an event about the deployment to sensu
    """

    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    cluster = load_system_paasta_config().get_cluster()

    # Setting up transparent cache for http API calls
    requests_cache.install_cache("setup_kubernetes_jobs", backend="memory")

    client = KubeClient()

    num_failed_deployments = 0
    for service_instance in args.service_instance_list:
        try:
            service, instance, _, __ = decompose_job_id(service_instance)
        except InvalidJobNameError:
            log.error("Invalid service instance specified. Format is service%sinstance." % SPACER)
            num_failed_deployments = num_failed_deployments + 1
        else:
            if deploy_kubernetes_pod(
                service=service,
                instance=instance,
                cluster=cluster,
                client=client,
                soa_dir=soa_dir,
            ):
                num_failed_deployments = num_failed_deployments + 1

    requests_cache.uninstall_cache()

    log.debug("%d out of %d service.instances failed to deploy." %
              (num_failed_deployments, len(args.service_instance_list)))

    sys.exit(1 if num_failed_deployments else 0)


def deploy_kubernetes_pod(service, instance, cluster, client, soa_dir):
    try:
        service_instance_config = load_kubernetes_pod_config(
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=soa_dir,
        )
    except NoDeploymentsAvailable:
        log.debug("No deployments found for %s.%s in cluster %s. Skipping." %
                  (service, instance, cluster))
        return 0
    except NoConfigurationForServiceError:
        error_msg = "Could not read kubernetes configuration file for %s.%s in cluster %s" % \
                    (service, instance, cluster)
        log.error(error_msg)
        return 1

    try:
        status, output = setup_service(
            service,
            instance,
            client,
            service_instance_config,
        )
        sensu_status = pysensu_yelp.Status.CRITICAL if status else pysensu_yelp.Status.OK
        send_event(service, instance, cluster, soa_dir, sensu_status, output)
        return 0
    except (KeyError, TypeError, AttributeError, InvalidInstanceConfig):
        error_str = traceback.format_exc()
        log.error(error_str)
        send_event(service, instance, cluster, soa_dir, pysensu_yelp.Status.CRITICAL, error_str)
        return 1


if __name__ == "__main__":
    main()
