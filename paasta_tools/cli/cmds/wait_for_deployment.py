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
"""Contains methods used by the paasta client to wait for deployment
of a docker image to a cluster.instance.
"""
import logging
import time

import progressbar
from bravado.exception import HTTPError
from requests.exceptions import ConnectionError

from paasta_tools.api import client
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import list_services
from paasta_tools.cli.utils import NoSuchService
from paasta_tools.cli.utils import validate_full_git_sha
from paasta_tools.cli.utils import validate_given_deploy_groups
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.generate_deployments_for_service \
    import get_cluster_instance_map_for_service
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import Timeout
from paasta_tools.utils import TimeoutError


DEFAULT_DEPLOYMENT_TIMEOUT = 3600  # seconds


log = logging.getLogger(__name__)


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'wait-for-deployment',
        help='Wait a service to be deployed to deploy_group',
        description=(
            "'paasta wait-for-deployment' waits for a previously marked for "
            "deployment service to be deployed to deploy_group."
        ),
        epilog=(
            "Note: Access and credentials to the Git repo of a service "
            "are required for this command to work."
        )
    )
    list_parser.add_argument(
        '-c', '-k', '--commit',
        help='Git sha to wait for deployment',
        required=True,
        type=validate_full_git_sha,
    )
    list_parser.add_argument(
        '-l', '--deploy-group',
        help='deploy group (e.g. cluster1.canary, cluster2.main).',
        required=True,
    ).completer = lazy_choices_completer(list_deploy_groups)
    list_parser.add_argument(
        '-s', '--service',
        help='Name of the service which you wish to wait for deployment. '
        'Leading "services-" will be stripped.',
        required=True,
    ).completer = lazy_choices_completer(list_services)
    list_parser.add_argument(
        '-t', '--timeout',
        dest="timeout",
        type=int,
        default=DEFAULT_DEPLOYMENT_TIMEOUT,
        help=(
            "Time in seconds to wait for paasta to deploy the service. "
            "If the timeout is exceeded we return 1. "
            "Default is %(default)s seconds."
        ),
    )
    list_parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    list_parser.add_argument(
        '-v', '--verbose',
        action='count',
        dest="verbose",
        default=0,
        help="Print out more output."
    )

    list_parser.set_defaults(command=paasta_wait_for_deployment)


def paasta_wait_for_deployment(args):
    """Wrapping wait_for_deployment"""
    if args.verbose:
        log.setLevel(level=logging.DEBUG)
    else:
        log.setLevel(level=logging.INFO)

    service = args.service
    if service and service.startswith('services-'):
        service = service.split('services-', 1)[1]
    try:
        validate_service_name(service, soa_dir=args.soa_dir)
    except NoSuchService as e:
        print(PaastaColors.red('%s' % e))
        return 1

    in_use_deploy_groups = list_deploy_groups(service=service,
                                              soa_dir=args.soa_dir)
    _, invalid_deploy_groups = \
        validate_given_deploy_groups(in_use_deploy_groups, [args.deploy_group])

    if len(invalid_deploy_groups) == 1:
        print(PaastaColors.red("ERROR: These deploy groups are not currently "
                               "used anywhere: %s.\n" %
                               (",").join(invalid_deploy_groups)))
        print(PaastaColors.red("You probably need one of these in-use deploy "
                               "groups?:\n   %s" %
                               (",").join(in_use_deploy_groups)))
        return 1

    try:
        wait_for_deployment(service=service,
                            deploy_group=args.deploy_group,
                            git_sha=args.commit,
                            soa_dir=args.soa_dir,
                            timeout=args.timeout)
        _log(service=service,
             component='deploy',
             line=("Deployment of {0} for {1} complete".
                   format(args.commit, args.deploy_group)),
             level='event')

    except (KeyboardInterrupt, TimeoutError):
        print("Waiting for deployment aborted.")
        return 1
    except NoInstancesFound:
        return 1

    return 0


def _get_service_statuses(cluster, service, instances):
    """Return the list of statuses of each instance of the service.

    Used by instances_deployed().
    """
    statuses = []

    api = client.get_paasta_api_client(cluster=cluster)
    if not api:
        log.warning("Couldn't reach the PaaSTA api for {}! Assuming it is not "
                    "deployed there yet.".format(cluster))
        return statuses

    for instance in instances:
        log.info("Inspecting the deployment status of {}.{} on {}"
                 .format(service, instance, cluster))
        try:
            status = api.service.status_instance(service=service,
                                                 instance=instance).result()
            statuses.append(status)
        except HTTPError as e:
            if e.response.status_code == 404:
                log.warning("Can't get status for instance {0}, service {1} in "
                            "cluster {2}. This is normally because it is a new "
                            "service that hasn't been deployed by PaaSTA yet"
                            .format(instance, service, cluster))
            else:
                log.warning("Error getting service status from PaaSTA API: {0}:"
                            " {1}".format(e.response.status_code,
                                          e.response.text))
            statuses.append(None)
        except ConnectionError as e:
            log.warning("Error getting service status from PaaSTA API for {0}: "
                        "{1}".format(cluster, e))
            statuses.append(None)
    return statuses


def instances_deployed(cluster, service, instances, git_sha):
    """Return the number of instances deployed"""
    num_deployed_instances = 0
    for status in _get_service_statuses(cluster, service, instances):
        if not status:
            log.info("No status for an unknown instance in {}. "
                     "Not deployed yet.".format(cluster))
        elif not status.marathon:
            log.info("{}.{} in {} is not a Marathon job. Marked as deployed."
                     .format(service, status.instance, cluster))
            num_deployed_instances += 1
        elif (status.marathon.expected_instance_count == 0 or
              status.marathon.desired_state == 'stop'):
            log.info("{}.{} in {} is marked as stopped. Marked as deployed."
                     .format(service, status.instance, cluster))
            num_deployed_instances += 1
        else:
            if status.marathon.app_count != 1:
                log.info("{}.{} on {} is still bouncing, {} versions running"
                         .format(service, status.instance, cluster,
                                 status.marathon.app_count))
                continue
            if not git_sha.startswith(status.git_sha):
                log.info("{}.{} on {} doesn't have the right sha yet: {}"
                         .format(service, status.instance, cluster,
                                 status.git_sha))
                continue
            if status.marathon.deploy_status != 'Running':
                log.info("{}.{} on {} isn't running yet: {}"
                         .format(service, status.instance, cluster,
                                 status.marathon.deploy_status))
                continue
            if (status.marathon.expected_instance_count !=
                    status.marathon.running_instance_count):
                log.info("{}.{} on {} isn't scaled up yet, has {} out of {}"
                         .format(service, status.instance, cluster,
                                 status.marathon.running_instance_count,
                                 status.marathon.expected_instance_count))
                continue
            log.info("{}.{} on {} looks 100% deployed at {} instances on {}"
                     .format(service, status.instance, cluster,
                             status.marathon.running_instance_count,
                             status.git_sha))
            num_deployed_instances += 1

    return num_deployed_instances


def wait_for_deployment(service, deploy_group, git_sha, soa_dir, timeout):
    cluster_map = get_cluster_instance_map_for_service(soa_dir,
                                                       service,
                                                       deploy_group)
    if not cluster_map:
        _log(service=service,
             component='deploy',
             line=("Couldn't find any instances for service {0} in"
                   " deploy group {1}").format(service, deploy_group),
             level='event')
        raise NoInstancesFound

    print("Waiting for deployment of {0} for '{1}' complete..."
          .format(git_sha, deploy_group))

    for cluster in cluster_map.values():
        cluster['deployed'] = 0
    try:
        with Timeout(seconds=timeout):
            total_instances = sum([len(v["instances"])
                                   for v in cluster_map.values()])
            with progressbar.ProgressBar(maxval=total_instances) as bar:
                while True:
                    for cluster, instances in cluster_map.items():
                        if cluster_map[cluster]['deployed'] != len(cluster_map[cluster]['instances']):
                            cluster_map[cluster]['deployed'] = instances_deployed(
                                cluster=cluster,
                                service=service,
                                instances=instances['instances'],
                                git_sha=git_sha)
                            if cluster_map[cluster]['deployed'] == len(cluster_map[cluster]['instances']):
                                instance_csv = ", ".join(cluster_map[cluster]['instances'])
                                print("Deploy to %s complete! (instances: %s)" % (cluster, instance_csv))
                        bar.update(sum([v["deployed"] for v in cluster_map.values()]))
                    if all([cluster['deployed'] == len(cluster["instances"]) for cluster in cluster_map.values()]):
                        break
                    else:
                        time.sleep(10)
    except TimeoutError:
        human_status = ["{0}: {1}".format(cluster, data['deployed'])
                        for cluster, data in cluster_map.items()]
        _log(service=service, component='deploy',
             line=("\nCurrent deployment status of {0} per cluster:\n"
                   .format(deploy_group) + "\n".join(human_status)),
             level='event')

        line = ("\n\nTimed out after {0} seconds, waiting for {2} in {1} to be "
                "deployed by PaaSTA.\n\n"
                "If you are sure your git sha is correct, this probably means "
                "the deploy hasn't suceeded. The new service might not be "
                "healthy or one or more clusters could be having issues.\n\n"
                "To debug: try running 'paasta status -s {2} -vv' or 'paasta "
                "logs -s {2}' to determine the cause.\n\n"
                .format(timeout, deploy_group, service, git_sha))
        _log(service=service, component='deploy', line=line, level='event')
        raise


class NoInstancesFound(Exception):
    pass
