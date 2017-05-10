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
import sys
import time
from threading import Event
from threading import Thread

import progressbar
from bravado.exception import HTTPError
from requests.exceptions import ConnectionError
from six.moves.queue import Empty
from six.moves.queue import Queue

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
            wait_for_deployment(service=service,
                                deploy_group=args.deploy_group,
                                git_sha=args.commit,
                                soa_dir=args.soa_dir,
                                timeout=args.timeout)
            line = "Deployment of {} for {} complete".format(args.commit, args.deploy_group)
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
        except NoInstancesFound:
            return 1
    if old_git_sha is not None and old_git_sha != args.commit and not args.auto_rollback:
        paasta_print()
        paasta_print("If you wish to roll back, you can run:")
        paasta_print()
        paasta_print(PaastaColors.bold("    paasta rollback --service %s --deploy-group %s --commit %s " % (
            service, args.deploy_group, old_git_sha))
        )
    return ret


class ClusterData:
    """An auxiliary data transfer class.

    Used by _query_clusters(), instances_deployed(),
    _run_cluster_worker(), _run_instance_worker().

    :param cluster: the name of the cluster.
    :param service: the name of the service.
    :param git_sha: git sha marked for deployment.
    :param instances_queue: a thread-safe queue. Should contain all cluster
                            instances that need to be checked.
    :type instances_queue: Queue
    """

    def __init__(self, cluster, service, git_sha, instances_queue):
        self.cluster = cluster
        self.service = service
        self.git_sha = git_sha
        self.instances_queue = instances_queue


def instances_deployed(cluster_data, instances_out, green_light):
    """Create a thread pool to run _run_instance_worker()

    :param cluster_data: an instance of ClusterData.
    :param instances_out: a empty thread-safe queue. I will contain
                          instances that are not deployed yet.
    :type instances_out: Queue
    :param green_light: See the docstring for _query_clusters().
    """
    num_threads = min(5, cluster_data.instances_queue.qsize())

    workers_launched = []
    for _ in range(num_threads):
        worker = Thread(target=_run_instance_worker,
                        args=(cluster_data, instances_out, green_light))
        worker.start()
        workers_launched.append(worker)

    for worker in workers_launched:
        worker.join()


def _run_instance_worker(cluster_data, instances_out, green_light):
    """Get instances from the instances_in queue and check them one by one.

    If an instance isn't deployed, add it to the instances_out queue
    to re-check it later.

    :param cluster_data: an instance of ClusterData.
    :param instances_out: See the docstring for instances_deployed().
    :param green_light: See the docstring for _query_clusters().
    """

    api = client.get_paasta_api_client(cluster=cluster_data.cluster)
    if not api:
        log.warning("Couldn't reach the PaaSTA api for {}! Assuming it is not "
                    "deployed there yet.".format(cluster_data.cluster))
        while not cluster_data.instances_queue.empty():
            try:
                instance = cluster_data.instances_queue.get(block=False)
            except Empty:
                return
            cluster_data.instances_queue.task_done()
            instances_out.put(instance)

    while not cluster_data.instances_queue.empty() and green_light.is_set():
        try:
            instance = cluster_data.instances_queue.get(block=False)
        except Empty:
            return
        log.debug("Inspecting the deployment status of {}.{} on {}"
                  .format(cluster_data.service, instance, cluster_data.cluster))
        try:
            status = None
            status = api.service.status_instance(service=cluster_data.service,
                                                 instance=instance).result()
        except HTTPError as e:
            if e.response.status_code == 404:
                log.warning("Can't get status for instance {}, service {} in "
                            "cluster {}. This is normally because it is a new "
                            "service that hasn't been deployed by PaaSTA yet"
                            .format(instance, cluster_data.service,
                                    cluster_data.cluster))
            else:
                log.warning("Error getting service status from PaaSTA API: {}:"
                            "{}".format(e.response.status_code,
                                        e.response.text))
        except ConnectionError as e:
            log.warning("Error getting service status from PaaSTA API for {}:"
                        "{}".format(cluster_data.cluster, e))

        if not status:
            log.debug("No status for {}.{}, in {}. Not deployed yet."
                      .format(cluster_data.service, instance,
                              cluster_data.cluster))
            cluster_data.instances_queue.task_done()
            instances_out.put(instance)
        elif not status.marathon:
            log.debug("{}.{} in {} is not a Marathon job. Marked as deployed."
                      .format(cluster_data.service, instance,
                              cluster_data.cluster))
        elif (status.marathon.expected_instance_count == 0 or
                status.marathon.desired_state == 'stop'):
            log.debug("{}.{} in {} is marked as stopped. Marked as deployed."
                      .format(cluster_data.service, status.instance,
                              cluster_data.cluster))
        else:
            if status.marathon.app_count != 1:
                paasta_print("  {}.{} on {} is still bouncing, {} versions "
                             "running"
                             .format(cluster_data.service, status.instance,
                                     cluster_data.cluster,
                                     status.marathon.app_count))
                cluster_data.instances_queue.task_done()
                instances_out.put(instance)
                continue
            if not cluster_data.git_sha.startswith(status.git_sha):
                paasta_print("  {}.{} on {} doesn't have the right sha yet: {}"
                             .format(cluster_data.service, instance,
                                     cluster_data.cluster, status.git_sha))
                cluster_data.instances_queue.task_done()
                instances_out.put(instance)
                continue
            if status.marathon.deploy_status not in ['Running', 'Deploying', 'Waiting']:
                paasta_print("  {}.{} on {} isn't running yet: {}"
                             .format(cluster_data.service, instance,
                                     cluster_data.cluster,
                                     status.marathon.deploy_status))
                cluster_data.instances_queue.task_done()
                instances_out.put(instance)
                continue
            if (status.marathon.expected_instance_count >
                    status.marathon.running_instance_count):
                paasta_print("  {}.{} on {} isn't scaled up yet, "
                             "has {} out of {}"
                             .format(cluster_data.service, instance,
                                     cluster_data.cluster,
                                     status.marathon.running_instance_count,
                                     status.marathon.expected_instance_count))
                cluster_data.instances_queue.task_done()
                instances_out.put(instance)
                continue
            paasta_print("Complete: {}.{} on {} looks 100% deployed at {} "
                         "instances on {}"
                         .format(cluster_data.service, instance,
                                 cluster_data.cluster,
                                 status.marathon.running_instance_count,
                                 status.git_sha))
            cluster_data.instances_queue.task_done()


def _query_clusters(clusters_data, green_light):
    """Run _run_cluster_worker() in a separate thread for each paasta cluster

    :param clusters_data: a list of ClusterData instances.
    :param green_light: an instance of threading.Event().
                        It is supposed to be cleared when KeyboardInterrupt is
                        received. All running threads should check it
                        periodically and exit when it is cleared.
    """
    workers_launched = []

    for cluster_data in clusters_data:
        if not cluster_data.instances_queue.empty():
            worker = Thread(target=_run_cluster_worker,
                            args=(cluster_data, green_light))
            worker.start()
            workers_launched.append(worker)

    for worker in workers_launched:
        try:
            while green_light.is_set() and worker.isAlive():
                time.sleep(.2)
        except (KeyboardInterrupt, SystemExit):
            green_light.clear()
            paasta_print('KeyboardInterrupt received. Terminating..')
        worker.join()


def _run_cluster_worker(cluster_data, green_light):
    """Run instances_deployed() for a cluster

    :param cluster_data: an instance of ClusterData.
    :param green_light: See the docstring for _query_clusters().
    """
    instances_out = Queue()
    instances_deployed(cluster_data=cluster_data,
                       instances_out=instances_out,
                       green_light=green_light)
    cluster_data.instances_queue = instances_out
    if cluster_data.instances_queue.empty():
        paasta_print("Deploy to {} complete!".format(cluster_data.cluster))


def wait_for_deployment(service, deploy_group, git_sha, soa_dir, timeout):
    cluster_map = get_cluster_instance_map_for_service(soa_dir=soa_dir, service=service, deploy_group=deploy_group)
    if not cluster_map:
        _log(
            service=service,
            component='deploy',
            line=("Couldn't find any instances for service {} in deploy "
                  "group {}".format(service, deploy_group)),
            level='event'
        )
        raise NoInstancesFound
    paasta_print("Waiting for deployment of {} for '{}' complete..."
                 .format(git_sha, deploy_group))

    total_instances = 0
    clusters_data = []
    for cluster in cluster_map:
        clusters_data.append(ClusterData(cluster=cluster, service=service,
                                         git_sha=git_sha,
                                         instances_queue=Queue()))
        for i in cluster_map[cluster]['instances']:
            clusters_data[-1].instances_queue.put(i)
        total_instances += len(cluster_map[cluster]['instances'])
    deadline = time.time() + timeout
    green_light = Event()
    green_light.set()

    with progressbar.ProgressBar(maxval=total_instances) as bar:
        while time.time() < deadline:
            _query_clusters(clusters_data, green_light)
            if not green_light.is_set():
                raise KeyboardInterrupt

            bar.update(total_instances - sum((c.instances_queue.qsize()
                                              for c in clusters_data)))

            if all((cluster.instances_queue.empty()
                    for cluster in clusters_data)):
                sys.stdout.flush()
                return 0
            else:
                time.sleep(min(60, timeout))
            sys.stdout.flush()

    _log(
        service=service,
        component='deploy',
        line=compose_timeout_message(clusters_data, timeout, deploy_group, service, git_sha),
        level='event'
    )
    raise TimeoutError


def compose_timeout_message(clusters_data, timeout, deploy_group, service, git_sha):
    cluster_instances = {}
    for c_d in clusters_data:
        while c_d.instances_queue.qsize() > 0:
            cluster_instances.setdefault(c_d.cluster, []).append(c_d.instances_queue.get(block=False))
            c_d.instances_queue.task_done()

    paasta_status = []
    paasta_logs = []
    for cluster, instances in sorted(cluster_instances.items()):
        if instances:
            joined_instances = ','.join(instances)
            paasta_status.append('paasta status -c {cluster} -s {service} -i {instances}'
                                 .format(cluster=cluster, service=service,
                                         instances=joined_instances))
            paasta_logs.append('paasta logs -c {cluster} -s {service} -i {instances} -C deploy -l 1000'
                               .format(cluster=cluster, service=service,
                                       instances=joined_instances))

    return ("\n\nTimed out after {timeout} seconds, waiting for {service} "
            "in {deploy_group} to be deployed by PaaSTA.\n"
            "This probably means the deploy hasn't succeeded. The new service "
            "might not be healthy or one or more clusters could be having issues.\n\n"
            "To debug try running:\n\n"
            "  {status_commands}\n\n  {logs_commands}"
            "\n\nIf the service is known to be slow to start you may wish to "
            "increase the timeout on this step.\n"
            "To wait a little longer run:\n\n"
            "  paasta wait-for-deployment -s {service} -l {deploy_group} -c {git_sha}"
            .format(timeout=timeout,
                    deploy_group=deploy_group,
                    service=service,
                    git_sha=git_sha,
                    status_commands='\n  '.join(paasta_status),
                    logs_commands='\n  '.join(paasta_logs)))


class NoInstancesFound(Exception):
    pass
