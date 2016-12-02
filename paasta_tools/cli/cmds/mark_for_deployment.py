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
"""Contains methods used by the paasta client to mark a docker image for
deployment to a cluster.instance.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import time

import progressbar
from bravado.exception import HTTPError
from requests.exceptions import ConnectionError

from paasta_tools import remote_git
from paasta_tools.api import client
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.cli.utils import list_services
from paasta_tools.cli.utils import validate_full_git_sha
from paasta_tools.cli.utils import validate_given_deploy_groups
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.deployment_utils import get_currently_deployed_sha
from paasta_tools.generate_deployments_for_service import get_cluster_instance_map_for_service
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import format_tag
from paasta_tools.utils import get_git_url
from paasta_tools.utils import get_paasta_tag_from_deploy_group
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import Timeout
from paasta_tools.utils import TimeoutError


DEFAULT_DEPLOYMENT_TIMEOUT = 3600  # seconds


log = logging.getLogger(__name__)


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'mark-for-deployment',
        help='Mark a docker image for deployment in git',
        description=(
            "'paasta mark-for-deployment' uses Git as the control-plane, to "
            "signal to other PaaSTA components that a particular docker image "
            "is ready to be deployed."
        ),
        epilog=(
            "Note: Access and credentials to the Git repo of a service are required "
            "for this command to work."
        )
    )
    list_parser.add_argument(
        '-u', '--git-url',
        help=(
            'Git url for service -- where magic mark-for-deployment tags are pushed. '
            'Defaults to the normal git URL for the service.'),
        default=None
    )
    list_parser.add_argument(
        '-c', '-k', '--commit',
        help='Git sha to mark for deployment',
        required=True,
        type=validate_full_git_sha,
    )
    list_parser.add_argument(
        '-l', '--deploy-group', '--clusterinstance',
        help='Mark the service ready for deployment in this deploy group (e.g. '
             'cluster1.canary, cluster2.main). --clusterinstance is deprecated and '
             'should be replaced with --deploy-group',
        required=True,
    ).completer = lazy_choices_completer(list_deploy_groups)
    list_parser.add_argument(
        '-s', '--service',
        help='Name of the service which you wish to mark for deployment. Leading '
        '"services-" will be stripped.',
        required=True,
    ).completer = lazy_choices_completer(list_services)
    list_parser.add_argument(
        '--wait-for-deployment',
        help='Set to poll paasta and wait for the deployment to finish, '
             'the default strategy is to mark for deployment and exit straightaway',
        dest='block',
        action='store_true',
        default=False
    )
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
        '--auto-rollback',
        help='Automatically roll back to the previously deployed sha if the deployment '
             'times out or is canceled (ctrl-c). Only applicable with --wait-for-deployment. '
             'Defaults to false.',
        dest='auto_rollback',
        action='store_true',
        default=False
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

    list_parser.set_defaults(command=paasta_mark_for_deployment)


def mark_for_deployment(git_url, deploy_group, service, commit):
    """Mark a docker image for deployment"""
    tag = get_paasta_tag_from_deploy_group(identifier=deploy_group, desired_state='deploy')
    remote_tag = format_tag(tag)
    ref_mutator = remote_git.make_force_push_mutate_refs_func(
        targets=[remote_tag],
        sha=commit,
    )
    try:
        remote_git.create_remote_refs(git_url=git_url, ref_mutator=ref_mutator, force=True)
    except Exception as e:
        loglines = ["Failed to mark %s for deployment in deploy group %s!" % (commit, deploy_group)]
        for line in str(e).split('\n'):
            loglines.append(line)
        return_code = 1
    else:
        loglines = ["Marked %s for deployment in deploy group %s" % (commit, deploy_group)]
        return_code = 0

    for logline in loglines:
        _log(
            service=service,
            line=logline,
            component='deploy',
            level='event',
        )
    return return_code


def paasta_mark_for_deployment(args):
    """Wrapping mark_for_deployment"""
    if args.verbose:
        log.setLevel(level=logging.DEBUG)
    else:
        log.setLevel(level=logging.INFO)

    service = args.service
    if service and service.startswith('services-'):
        service = service.split('services-', 1)[1]
    validate_service_name(service, soa_dir=args.soa_dir)

    in_use_deploy_groups = list_deploy_groups(
        service=service,
        soa_dir=args.soa_dir,
    )
    _, invalid_deploy_groups = validate_given_deploy_groups(in_use_deploy_groups, [args.deploy_group])

    if len(invalid_deploy_groups) == 1:
        paasta_print(PaastaColors.red(
            "ERROR: These deploy groups are not currently used anywhere: %s.\n" % (",").join(invalid_deploy_groups)))
        paasta_print(PaastaColors.red(
            "This isn't technically wrong because you can mark-for-deployment before deploying there"))
        paasta_print(PaastaColors.red("but this is probably a typo. Did you mean one of these in-use deploy groups?:"))
        paasta_print(PaastaColors.red("   %s" % (",").join(in_use_deploy_groups)))
        paasta_print()
        paasta_print(PaastaColors.red("Continuing regardless..."))

    if args.git_url is None:
        args.git_url = get_git_url(service=service, soa_dir=args.soa_dir)

    old_git_sha = get_currently_deployed_sha(service=service, deploy_group=args.deploy_group)
    if old_git_sha == args.commit:
        paasta_print("Warning: The sha asked to be deployed already matches what is set to be deployed:")
        paasta_print(old_git_sha)
        paasta_print("Continuing anyway.")

    ret = mark_for_deployment(
        git_url=args.git_url,
        deploy_group=args.deploy_group,
        service=service,
        commit=args.commit,
    )
    if args.block:
        try:
            paasta_print("Waiting for deployment of {0} for '{1}' complete...".format(args.commit, args.deploy_group))
            wait_for_deployment(service=service,
                                deploy_group=args.deploy_group,
                                git_sha=args.commit,
                                soa_dir=args.soa_dir,
                                timeout=args.timeout)
            line = "Deployment of {0} for {1} complete".format(args.commit, args.deploy_group)
            _log(
                service=service,
                component='deploy',
                line=line,
                level='event'
            )
        except (KeyboardInterrupt, TimeoutError):
            if args.auto_rollback is True:
                if old_git_sha == args.commit:
                    paasta_print("Error: --auto-rollback was requested, but the previous sha")
                    paasta_print("is the same that was requested with --commit. Can't rollback")
                    paasta_print("automatically.")
                else:
                    paasta_print("Auto-Rollback requested. Marking the previous sha")
                    paasta_print("(%s) for %s as desired." % (args.deploy_group, old_git_sha))
                    mark_for_deployment(
                        git_url=args.git_url,
                        deploy_group=args.deploy_group,
                        service=service,
                        commit=old_git_sha,
                    )
            else:
                paasta_print("Waiting for deployment aborted. PaaSTA will continue to try to deploy this code.")
                paasta_print("If you wish to see the status, run:")
                paasta_print()
                paasta_print("    paasta status -s %s -v" % service)
                paasta_print()
            ret = 1
    if old_git_sha is not None and old_git_sha != args.commit:
        paasta_print()
        paasta_print("If you wish to roll back, you can run:")
        paasta_print()
        paasta_print(PaastaColors.bold("    paasta rollback --service %s --deploy-group %s --commit %s " % (
            service, args.deploy_group, old_git_sha))
        )
    return ret


def instances_deployed(cluster, service, instances, git_sha):
    api = client.get_paasta_api_client(cluster=cluster)
    if not api:
        log.warning("Couldn't reach the PaaSTA api for {}! Assuming it is not deployed there yet.".format(cluster))
        return False
    statuses = []
    for instance in instances:
        log.info("Inspecting the deployment status of {}.{} on {}".format(service, instance, cluster))
        try:
            status = api.service.status_instance(service=service, instance=instance).result()
            statuses.append(status)
        except HTTPError as e:
            if e.response.status_code == 404:
                log.warning("Can't get status for instance {0}, service {1} in cluster {2}. "
                            "This is normally because it is a new service that hasn't been "
                            "deployed by PaaSTA yet".format(instance, service, cluster))
            else:
                log.warning("Error getting service status from PaaSTA API: {0}: {1}".format(e.response.status_code,
                                                                                            e.response.text))
            statuses.append(None)
        except ConnectionError as e:
            log.warning("Error getting service status from PaaSTA API for {0}: {1}".format(cluster, e))
            statuses.append(None)
    results = []
    for status in statuses:
        if not status:
            log.info("No status for an unknown instance in {}. Not deployed yet.".format(cluster))
            results.append(False)
        elif not status.marathon:
            log.info("{}.{} in {} is not a Marathon job. Marked as deployed.".format(
                service, status.instance, cluster))
            results.append(True)
        elif status.marathon.expected_instance_count == 0 or status.marathon.desired_state == 'stop':
            log.info("{}.{} in {} is marked as stopped. Marked as deployed.".format(
                service, status.instance, cluster))
            results.append(True)
        else:
            if status.marathon.app_count != 1:
                log.info("{}.{} on {} is still bouncing, {} versions running".format(
                    service, status.instance, cluster, status.marathon.app_count))
                results.append(False)
                continue
            if not git_sha.startswith(status.git_sha):
                log.info("{}.{} on {} doesn't have the right sha yet: {}".format(
                    service, status.instance, cluster, status.git_sha))
                results.append(False)
                continue
            if status.marathon.deploy_status != 'Running':
                log.info("{}.{} on {} isn't running yet: {}".format(
                    service, status.instance, cluster, status.marathon.deploy_status))
                results.append(False)
                continue
            if status.marathon.expected_instance_count != status.marathon.running_instance_count:
                log.info("{}.{} on {} isn't scaled up yet, has {} out of {}".format(
                    service, status.instance, cluster, status.marathon.running_instance_count,
                    status.marathon.expected_instance_count))
                results.append(False)
                continue
            log.info("{}.{} on {} looks 100% deployed at {} instances on {}".format(
                service, status.instance, cluster, status.marathon.running_instance_count, status.git_sha))
            results.append(True)

    return sum(results)


def wait_for_deployment(service, deploy_group, git_sha, soa_dir, timeout):
    cluster_map = get_cluster_instance_map_for_service(soa_dir, service, deploy_group)
    if not cluster_map:
        line = "Couldn't find any instances for service {0} in deploy group {1}".format(service, deploy_group)
        _log(
            service=service,
            component='deploy',
            line=line,
            level='event'
        )
        raise NoInstancesFound
    for cluster in cluster_map.values():
        cluster['deployed'] = 0
    try:
        with Timeout(seconds=timeout):
            total_instances = sum([len(v["instances"]) for v in cluster_map.values()])
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
                                paasta_print("Deploy to %s complete! (instances: %s)" % (cluster, instance_csv))
                        bar.update(sum([v["deployed"] for v in cluster_map.values()]))
                    if all([cluster['deployed'] == len(cluster["instances"]) for cluster in cluster_map.values()]):
                        break
                    else:
                        time.sleep(10)
    except TimeoutError:
        human_status = ["{0}: {1}".format(cluster, data['deployed']) for cluster, data in cluster_map.items()]
        line = "\nCurrent deployment status of {0} per cluster:\n".format(deploy_group) + "\n".join(human_status)
        _log(
            service=service,
            component='deploy',
            line=line,
            level='event'
        )
        line = "\n\nTimed out after {0} seconds, waiting for {1} in {2} to be deployed by PaaSTA. \n\n"\
               "This probably means the deploy hasn't suceeded. The new service might not be healthy or one "\
               "or more clusters could be having issues.\n\n"\
               "To debug: try running 'paasta status -s {2} -vv' or 'paasta logs -s {2}' to determine the cause.\n\n"\
               "{3} is still *marked* for deployment. To rollback, you can run: 'paasta rollback --service "\
               "{2} --deploy-group {1}'\n\n"\
               "If the service is known to be slow to start you may wish to increase "\
               "the timeout on this step.".format(timeout, deploy_group, service, git_sha)
        _log(
            service=service,
            component='deploy',
            line=line,
            level='event'
        )
        raise
    return True


class NoInstancesFound(Exception):
    pass
