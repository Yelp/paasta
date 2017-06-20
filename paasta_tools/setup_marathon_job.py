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
Usage: ./setup_marathon_job.py <service.instance> [options]

Deploy a service instance to Marathon from a configuration file.
Attempts to load the marathon configuration at
/etc/paasta/marathon.json, and read
from the soa_dir /nail/etc/services by default.

This script will attempt to load a service's configuration
from the soa_dir and generate a marathon job configuration for it,
as well as handle deploying that configuration with a bounce strategy
if there's an old version of the service. To determine whether or not
a deployment is 'old', each marathon job has a complete id of
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
import logging
import sys
import traceback
from collections import defaultdict

import pysensu_yelp
import requests_cache
from marathon.exceptions import MarathonHttpError
from requests.exceptions import HTTPError
from requests.exceptions import ReadTimeout

from paasta_tools import bounce_lib
from paasta_tools import drain_lib
from paasta_tools import marathon_tools
from paasta_tools import monitoring_tools
from paasta_tools.marathon_tools import get_num_at_risk_tasks
from paasta_tools.marathon_tools import kill_given_tasks
from paasta_tools.mesos.exceptions import NoSlavesAvailableError
from paasta_tools.mesos_maintenance import get_draining_hosts
from paasta_tools.mesos_maintenance import reserve_all_resources
from paasta_tools.utils import _log
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import InvalidInstanceConfig
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import SPACER

# Marathon REST API:
# https://github.com/mesosphere/marathon/blob/master/REST.md#post-v2apps

log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='Creates marathon jobs.')
    parser.add_argument('service_instance_list', nargs='+',
                        help="The list of marathon service instances to create or update",
                        metavar="SERVICE%sINSTANCE" % SPACER)
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=marathon_tools.DEFAULT_SOA_DIR,
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
    monitoring_overrides = marathon_tools.load_marathon_service_config(
        name,
        instance,
        cluster,
        soa_dir=soa_dir,
        load_deployments=False,
    ).get_monitoring()
    # In order to let sensu know how often to expect this check to fire,
    # we need to set the ``check_every`` to the frequency of our cron job, which
    # is 10s.
    monitoring_overrides['check_every'] = '10s'
    # Most setup_marathon_job failures are transient and represent issues
    # that will probably be fixed eventually, so we set an alert_after
    # to suppress extra noise
    monitoring_overrides['alert_after'] = '10m'
    check_name = 'setup_marathon_job.%s' % compose_job_id(name, instance)
    monitoring_tools.send_event(name, check_name, monitoring_overrides, status, output, soa_dir)


def get_main_marathon_config():
    log.debug("Reading marathon configuration")
    marathon_config = marathon_tools.load_marathon_config()
    log.info("Marathon config is: %s", marathon_config)
    return marathon_config


def drain_tasks_and_find_tasks_to_kill(tasks_to_drain, already_draining_tasks, drain_method, log_bounce_action,
                                       bounce_method, at_risk_tasks):
    """Drain the tasks_to_drain, and return the set of tasks that are safe to kill."""
    all_draining_tasks = set(already_draining_tasks) | set(at_risk_tasks)
    tasks_to_kill = set()

    if len(tasks_to_drain) > 0:
        tasks_to_drain_by_app_id = defaultdict(set)
        for task in tasks_to_drain:
            tasks_to_drain_by_app_id[task.app_id].add(task)
        for app_id, tasks in tasks_to_drain_by_app_id.items():
            log_bounce_action(
                line='%s bounce draining %d old tasks with app_id %s' %
                (bounce_method, len(tasks), app_id),
            )
        for task in tasks_to_drain:
            all_draining_tasks.add(task)

    for task in all_draining_tasks:
        try:
            drain_method.drain(task)
        except Exception as e:
            log_bounce_action(
                line=("%s bounce killing task %s due to exception when draining: %s" % (bounce_method, task.id, e)),
            )
            tasks_to_kill.add(task)

    for task in all_draining_tasks:
        try:
            if drain_method.is_safe_to_kill(task):
                tasks_to_kill.add(task)
                log_bounce_action(line='%s bounce killing drained task %s' % (bounce_method, task.id))
        except Exception as e:
            tasks_to_kill.add(task)
            log_bounce_action(
                line='%s bounce killing task %s due to exception in is_safe_to_kill: %s' % (bounce_method, task.id, e),
            )

    return tasks_to_kill


def do_bounce(
    bounce_func,
    drain_method,
    config,
    new_app_running,
    happy_new_tasks,
    old_app_live_happy_tasks,
    old_app_live_unhappy_tasks,
    old_app_draining_tasks,
    old_app_at_risk_tasks,
    service,
    bounce_method,
    serviceinstance,
    cluster,
    instance,
    marathon_jobid,
    client,
    soa_dir,
    bounce_margin_factor=1.0,
):
    def log_bounce_action(line, level='debug'):
        return _log(
            service=service,
            line=line,
            component='deploy',
            level=level,
            cluster=cluster,
            instance=instance
        )

    # log if we're not in a steady state.
    if any([
        (not new_app_running),
        old_app_live_happy_tasks.keys()
    ]):
        log_bounce_action(
            line=' '.join([
                '%s bounce in progress on %s.' % (bounce_method, serviceinstance),
                'New marathon app %s %s.' % (marathon_jobid, ('exists' if new_app_running else 'not created yet')),
                '%d new tasks to bring up.' % (config['instances'] - len(happy_new_tasks)),
                '%d old tasks receiving traffic and happy.' % len(bounce_lib.flatten_tasks(old_app_live_happy_tasks)),
                '%d old tasks unhappy.' % len(bounce_lib.flatten_tasks(old_app_live_unhappy_tasks)),
                '%d old tasks draining.' % len(bounce_lib.flatten_tasks(old_app_draining_tasks)),
                '%d old tasks at risk.' % len(bounce_lib.flatten_tasks(old_app_at_risk_tasks)),
                '%d old apps.' % len(old_app_live_happy_tasks.keys()),
            ]),
            level='event',
        )
    else:
        log.debug("Nothing to do, bounce is in a steady state")

    actions = bounce_func(
        new_config=config,
        new_app_running=new_app_running,
        happy_new_tasks=happy_new_tasks,
        old_app_live_happy_tasks=old_app_live_happy_tasks,
        old_app_live_unhappy_tasks=old_app_live_unhappy_tasks,
        margin_factor=bounce_margin_factor,
    )

    if actions['create_app'] and not new_app_running:
        log_bounce_action(
            line='%s bounce creating new app with app_id %s' % (bounce_method, marathon_jobid),
        )
        with requests_cache.disabled():
            try:
                bounce_lib.create_marathon_app(marathon_jobid, config, client)
            except MarathonHttpError as e:
                if e.status_code == 409:
                    log.warning("Failed to create, app %s already exists. This means another bounce beat us to it."
                                " Skipping the rest of the bounce for this run" % marathon_jobid)
                    return 60
                raise

    tasks_to_kill = drain_tasks_and_find_tasks_to_kill(
        tasks_to_drain=actions['tasks_to_drain'],
        already_draining_tasks=bounce_lib.flatten_tasks(old_app_draining_tasks),
        drain_method=drain_method,
        log_bounce_action=log_bounce_action,
        bounce_method=bounce_method,
        at_risk_tasks=bounce_lib.flatten_tasks(old_app_at_risk_tasks),
    )

    kill_given_tasks(client=client, task_ids=[task.id for task in tasks_to_kill], scale=True)

    for task in bounce_lib.flatten_tasks(old_app_at_risk_tasks):
        if task in tasks_to_kill:
            hostname = task.host
            try:
                reserve_all_resources([hostname])
            except HTTPError:
                log.warning("Failed to reserve resources on %s" % hostname)

    apps_to_kill = []
    for app in old_app_live_happy_tasks.keys():
        if app != '/%s' % marathon_jobid:
            live_happy_tasks = old_app_live_happy_tasks[app]
            live_unhappy_tasks = old_app_live_unhappy_tasks[app]
            draining_tasks = old_app_draining_tasks[app]
            at_risk_tasks = old_app_at_risk_tasks[app]

            if 0 == len((live_happy_tasks | live_unhappy_tasks | draining_tasks | at_risk_tasks) - tasks_to_kill):
                apps_to_kill.append(app)

    if apps_to_kill:
        log_bounce_action(
            line='%s bounce removing old unused apps with app_ids: %s' %
            (
                bounce_method,
                ', '.join(apps_to_kill)
            ),
        )
        with requests_cache.disabled():
            bounce_lib.kill_old_ids(apps_to_kill, client)

    all_old_tasks = set.union(set(), *old_app_live_happy_tasks.values())
    all_old_tasks = set.union(all_old_tasks, *old_app_live_unhappy_tasks.values())
    all_old_tasks = set.union(all_old_tasks, *old_app_draining_tasks.values())
    all_old_tasks = set.union(all_old_tasks, *old_app_at_risk_tasks.values())

    if all_old_tasks or (not new_app_running):
        # Still have work more work to do, try again in 60 seconds
        return 60
    else:
        # log if we appear to be finished
        if all([
            (apps_to_kill or tasks_to_kill),
            apps_to_kill == list(old_app_live_happy_tasks),
            tasks_to_kill == all_old_tasks,
        ]):
            log_bounce_action(
                line='%s bounce on %s finishing. Now running %s' %
                (
                    bounce_method,
                    serviceinstance,
                    marathon_jobid
                ),
                level='event',
            )
        return None


def get_tasks_by_state_for_app(app, drain_method, service, nerve_ns, bounce_health_params,
                               system_paasta_config, log_deploy_error, draining_hosts):
    tasks_by_state = {
        'happy': set(),
        'unhappy': set(),
        'draining': set(),
        'at_risk': set(),
    }

    happy_tasks = bounce_lib.get_happy_tasks(app, service, nerve_ns, system_paasta_config, **bounce_health_params)
    for task in app.tasks:
        try:
            is_draining = drain_method.is_draining(task)
        except Exception as e:
            log_deploy_error(
                "Ignoring exception during is_draining of task %s:"
                " %s. Treating task as 'unhappy'." % (task, e)
            )
            state = 'unhappy'
        else:
            if is_draining is True:
                state = 'draining'
            elif task in happy_tasks:
                if task.host in draining_hosts:
                    state = 'at_risk'
                else:
                    state = 'happy'
            else:
                state = 'unhappy'
        tasks_by_state[state].add(task)

    return tasks_by_state


def get_tasks_by_state(other_apps, drain_method, service, nerve_ns, bounce_health_params,
                       system_paasta_config, log_deploy_error, draining_hosts):
    """Split tasks from old apps into 4 categories:
      - live (not draining) and happy (according to get_happy_tasks)
      - live (not draining) and unhappy
      - draining
      - at-risk (running on a host marked draining in Mesos in preparation for maintenance)
    """

    old_app_live_happy_tasks = {}
    old_app_live_unhappy_tasks = {}
    old_app_draining_tasks = {}
    old_app_at_risk_tasks = {}

    for app in other_apps:

        tasks_by_state = get_tasks_by_state_for_app(
            app=app,
            drain_method=drain_method,
            service=service,
            nerve_ns=nerve_ns,
            bounce_health_params=bounce_health_params,
            system_paasta_config=system_paasta_config,
            log_deploy_error=log_deploy_error,
            draining_hosts=draining_hosts,
        )

        old_app_live_happy_tasks[app.id] = tasks_by_state['happy']
        old_app_live_unhappy_tasks[app.id] = tasks_by_state['unhappy']
        old_app_draining_tasks[app.id] = tasks_by_state['draining']
        old_app_at_risk_tasks[app.id] = tasks_by_state['at_risk']

    return old_app_live_happy_tasks, old_app_live_unhappy_tasks, old_app_draining_tasks, old_app_at_risk_tasks


def undrain_tasks(to_undrain, leave_draining, drain_method, log_deploy_error):
    # If any tasks on the new app happen to be draining (e.g. someone reverts to an older version with
    # `paasta mark-for-deployment`), then we should undrain them.
    for task in to_undrain:
        if task not in leave_draining:
            try:
                drain_method.stop_draining(task)
            except Exception as e:
                log_deploy_error("Ignoring exception during stop_draining of task %s: %s." % (task, e))


def deploy_service(
    service,
    instance,
    marathon_jobid,
    config,
    client,
    marathon_apps,
    bounce_method,
    drain_method_name,
    drain_method_params,
    nerve_ns,
    bounce_health_params,
    soa_dir,
    bounce_margin_factor=1.0,
):
    """Deploy the service to marathon, either directly or via a bounce if needed.
    Called by setup_service when it's time to actually deploy.

    :param service: The name of the service to deploy
    :param instance: The instance of the service to deploy
    :param marathon_jobid: Full id of the marathon job
    :param config: The complete configuration dict to send to marathon
    :param client: A MarathonClient object
    :param bounce_method: The bounce method to use, if needed
    :param drain_method_name: The name of the traffic draining method to use.
    :param nerve_ns: The nerve namespace to look in.
    :param bounce_health_params: A dictionary of options for bounce_lib.get_happy_tasks.
    :param bounce_margin_factor: the multiplication factor used to calculate the number of instances to be drained
    :returns: A tuple of (status, output, bounce_in_seconds) to be used with send_sensu_event"""

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
    existing_apps = marathon_tools.get_matching_apps(service, instance, marathon_apps)
    new_app_list = [a for a in existing_apps if a.id == '/%s' % config['id']]
    other_apps = [a for a in existing_apps if a.id != '/%s' % config['id']]
    serviceinstance = "%s.%s" % (service, instance)

    if new_app_list:
        new_app = new_app_list[0]
        if len(new_app_list) != 1:
            raise ValueError("Only expected one app per ID; found %d" % len(new_app_list))
        new_app_running = True
        happy_new_tasks = bounce_lib.get_happy_tasks(new_app, service, nerve_ns, system_paasta_config,
                                                     **bounce_health_params)
    else:
        new_app_running = False
        happy_new_tasks = []

    try:
        drain_method = drain_lib.get_drain_method(
            drain_method_name,
            service=service,
            instance=instance,
            nerve_ns=nerve_ns,
            drain_method_params=drain_method_params,
        )
    except KeyError:
        errormsg = 'ERROR: drain_method not recognized: %s. Must be one of (%s)' % \
            (drain_method_name, ', '.join(drain_lib.list_drain_methods()))
        log_deploy_error(errormsg)
        return (1, errormsg, None)

    try:
        draining_hosts = get_draining_hosts()
    except ReadTimeout as e:
        errormsg = "ReadTimeout encountered trying to get draining hosts: %s" % e
        return (1, errormsg, 60)

    (old_app_live_happy_tasks,
     old_app_live_unhappy_tasks,
     old_app_draining_tasks,
     old_app_at_risk_tasks,
     ) = get_tasks_by_state(
        other_apps=other_apps,
        drain_method=drain_method,
        service=service,
        nerve_ns=nerve_ns,
        bounce_health_params=bounce_health_params,
        system_paasta_config=system_paasta_config,
        log_deploy_error=log_deploy_error,
        draining_hosts=draining_hosts,
    )

    num_at_risk_tasks = 0
    if new_app_running:
        num_at_risk_tasks = get_num_at_risk_tasks(new_app, draining_hosts=draining_hosts)
        if new_app.instances < config['instances'] + num_at_risk_tasks:
            log.info("Scaling %s up from %d to %d instances." %
                     (new_app.id, new_app.instances, config['instances'] + num_at_risk_tasks))
            client.scale_app(app_id=new_app.id, instances=config['instances'] + num_at_risk_tasks, force=True)
        # If we have more than the specified number of instances running, we will want to drain some of them.
        # We will start by draining any tasks running on at-risk hosts.
        elif new_app.instances > config['instances'] + num_at_risk_tasks:
            num_tasks_to_scale = max(min(len(new_app.tasks), new_app.instances) - config['instances'], 0)
            task_dict = get_tasks_by_state_for_app(
                app=new_app,
                drain_method=drain_method,
                service=service,
                nerve_ns=nerve_ns,
                bounce_health_params=bounce_health_params,
                system_paasta_config=system_paasta_config,
                log_deploy_error=log_deploy_error,
                draining_hosts=draining_hosts,
            )
            scaling_app_happy_tasks = list(task_dict['happy'])
            scaling_app_unhappy_tasks = list(task_dict['unhappy'])
            scaling_app_draining_tasks = list(task_dict['draining'])
            scaling_app_at_risk_tasks = list(task_dict['at_risk'])

            tasks_to_move_draining = min(len(scaling_app_draining_tasks), num_tasks_to_scale)
            old_app_draining_tasks[new_app.id] = set(scaling_app_draining_tasks[:tasks_to_move_draining])
            num_tasks_to_scale = num_tasks_to_scale - tasks_to_move_draining

            tasks_to_move_unhappy = min(len(scaling_app_unhappy_tasks), num_tasks_to_scale)
            old_app_live_unhappy_tasks[new_app.id] = set(scaling_app_unhappy_tasks[:tasks_to_move_unhappy])
            num_tasks_to_scale = num_tasks_to_scale - tasks_to_move_unhappy

            tasks_to_move_at_risk = min(len(scaling_app_at_risk_tasks), num_tasks_to_scale)
            old_app_at_risk_tasks[new_app.id] = set(scaling_app_at_risk_tasks[:tasks_to_move_at_risk])
            num_tasks_to_scale = num_tasks_to_scale - tasks_to_move_at_risk

            tasks_to_move_happy = min(len(scaling_app_happy_tasks), num_tasks_to_scale)
            old_app_live_happy_tasks[new_app.id] = set(scaling_app_happy_tasks[:tasks_to_move_happy])
            happy_new_tasks = scaling_app_happy_tasks[tasks_to_move_happy:]

            # slack represents remaining the extra remaining instances that are configured
            # in marathon that don't have a launched task yet. When scaling down we want to
            # reduce this slack so marathon doesn't get a chance to launch a new task in
            # that space that we will then have to drain and kill again.
            slack = max(new_app.instances - len(new_app.tasks), 0)
            if slack > 0:
                print("Scaling %s down from %d to %d instances to remove slack." %
                      (new_app.id, new_app.instances, new_app.instances - slack))
                client.scale_app(app_id=new_app.id, instances=(new_app.instances - slack), force=True)

        # TODO: don't take actions in deploy_service.
        undrain_tasks(
            to_undrain=new_app.tasks,
            leave_draining=old_app_draining_tasks.get(new_app.id, []),
            drain_method=drain_method,
            log_deploy_error=log_deploy_error,
        )

    # log all uncaught exceptions and raise them again
    try:
        try:
            bounce_func = bounce_lib.get_bounce_method_func(bounce_method)
        except KeyError:
            errormsg = 'ERROR: bounce_method not recognized: %s. Must be one of (%s)' % \
                (bounce_method, ', '.join(bounce_lib.list_bounce_methods()))
            log_deploy_error(errormsg)
            return (1, errormsg, None)

        bounce_again_in_seconds = do_bounce(
            bounce_func=bounce_func,
            drain_method=drain_method,
            config=config,
            new_app_running=new_app_running,
            happy_new_tasks=happy_new_tasks,
            old_app_live_happy_tasks=old_app_live_happy_tasks,
            old_app_live_unhappy_tasks=old_app_live_unhappy_tasks,
            old_app_draining_tasks=old_app_draining_tasks,
            old_app_at_risk_tasks=old_app_at_risk_tasks,
            service=service,
            bounce_method=bounce_method,
            serviceinstance=serviceinstance,
            cluster=cluster,
            instance=instance,
            marathon_jobid=marathon_jobid,
            client=client,
            soa_dir=soa_dir,
            bounce_margin_factor=bounce_margin_factor,
        )
    except bounce_lib.LockHeldException:
        logline = 'Failed to get lock to create marathon app for %s.%s' % (service, instance)
        log_deploy_error(logline, level='debug')
        return (0, "Couldn't get marathon lock, skipping until next time", None)
    except Exception:
        logline = 'Exception raised during deploy of service %s:\n%s' % (service, traceback.format_exc())
        log_deploy_error(logline, level='debug')
        raise
    if num_at_risk_tasks:
        bounce_again_in_seconds = 60
    elif new_app_running:
        if new_app.instances > config['instances']:
            bounce_again_in_seconds = 60
    return (0, 'Service deployed.', bounce_again_in_seconds)


def setup_service(service, instance, client, service_marathon_config, marathon_apps, soa_dir):
    """Setup the service instance given and attempt to deploy it, if possible.
    Doesn't do anything if the service is already in Marathon and hasn't changed.
    If it's not, attempt to find old instances of the service and bounce them.

    :param service: The service name to setup
    :param instance: The instance of the service to setup
    :param client: A MarathonClient object
    :param service_marathon_config: The service instance's configuration dict
    :returns: A tuple of (status, output, bounce_in_seconds) to be used with send_sensu_event"""

    log.info("Setting up instance %s for service %s", instance, service)
    try:
        marathon_app_dict = service_marathon_config.format_marathon_app_dict()
    except NoDockerImageError:
        error_msg = (
            "Docker image for {0}.{1} not in deployments.json. Exiting. Has Jenkins deployed it?\n"
        ).format(
            service,
            instance,
        )
        log.error(error_msg)
        return (1, error_msg, None)

    full_id = marathon_app_dict['id']
    service_namespace_config = marathon_tools.load_service_namespace_config(
        service=service, namespace=service_marathon_config.get_nerve_namespace(), soa_dir=soa_dir)

    log.info("Desired Marathon instance id: %s", full_id)
    return deploy_service(
        service=service,
        instance=instance,
        marathon_jobid=full_id,
        config=marathon_app_dict,
        client=client,
        marathon_apps=marathon_apps,
        bounce_method=service_marathon_config.get_bounce_method(),
        drain_method_name=service_marathon_config.get_drain_method(service_namespace_config),
        drain_method_params=service_marathon_config.get_drain_method_params(service_namespace_config),
        nerve_ns=service_marathon_config.get_nerve_namespace(),
        bounce_health_params=service_marathon_config.get_bounce_health_params(service_namespace_config),
        soa_dir=soa_dir,
        bounce_margin_factor=service_marathon_config.get_bounce_margin_factor(),
    )


def main():
    """Attempt to set up a list of marathon service instances given.
    Exits 1 if any service.instance deployment failed.
    This is done in the following order:

    - Load the marathon configuration
    - Connect to marathon
    - Do the following for each service.instance:
        - Load the service instance's configuration
        - Create the complete marathon job configuration
        - Deploy/bounce the service
        - Emit an event about the deployment to sensu"""

    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    # Setting up transparent cache for http API calls
    requests_cache.install_cache("setup_marathon_jobs", backend="memory")

    marathon_config = get_main_marathon_config()
    client = marathon_tools.get_marathon_client(marathon_config.get_url(), marathon_config.get_username(),
                                                marathon_config.get_password())
    marathon_apps = marathon_tools.get_all_marathon_apps(client, embed_failures=True)

    num_failed_deployments = 0
    for service_instance in args.service_instance_list:
        try:
            service, instance, _, __ = decompose_job_id(service_instance)
        except InvalidJobNameError:
            log.error("Invalid service instance specified. Format is service%sinstance." % SPACER)
            num_failed_deployments = num_failed_deployments + 1
        else:
            if deploy_marathon_service(service, instance, client, soa_dir, marathon_config, marathon_apps)[0]:
                num_failed_deployments = num_failed_deployments + 1

    requests_cache.uninstall_cache()

    log.debug("%d out of %d service.instances failed to deploy." %
              (num_failed_deployments, len(args.service_instance_list)))

    sys.exit(1 if num_failed_deployments else 0)


def deploy_marathon_service(service, instance, client, soa_dir, marathon_config, marathon_apps):
    """deploy the service instance given and proccess return code
    if there was an error we send a sensu alert.

    :param service: The service name to setup
    :param instance: The instance of the service to setup
    :param client: A MarathonClient object
    :param soa_dir: Path to yelpsoa configs
    :param marathon_config: The service instance's configuration dict
    :param marathon_apps: A list of all marathon app objects
    :returns: A tuple of (status, bounce_in_seconds) to be used by paasta-deployd
        bounce_in_seconds instructs how long until the deployd should try another bounce
        None means that it is in a steady state and doesn't need to bounce again
    """
    short_id = marathon_tools.format_job_id(service, instance)
    try:
        with bounce_lib.bounce_lock_zookeeper(short_id):
            try:
                service_instance_config = marathon_tools.load_marathon_service_config_no_cache(
                    service,
                    instance,
                    load_system_paasta_config().get_cluster(),
                    soa_dir=soa_dir,
                )
            except NoDeploymentsAvailable:
                log.debug("No deployments found for %s.%s in cluster %s. Skipping." %
                          (service, instance, load_system_paasta_config().get_cluster()))
                return 0, None
            except NoConfigurationForServiceError:
                error_msg = "Could not read marathon configuration file for %s.%s in cluster %s" % \
                            (service, instance, load_system_paasta_config().get_cluster())
                log.error(error_msg)
                return 1, None

            try:
                status, output, bounce_again_in_seconds = setup_service(service,
                                                                        instance,
                                                                        client,
                                                                        service_instance_config,
                                                                        marathon_apps,
                                                                        soa_dir)
                sensu_status = pysensu_yelp.Status.CRITICAL if status else pysensu_yelp.Status.OK
                send_event(service, instance, soa_dir, sensu_status, output)
                return 0, bounce_again_in_seconds
            except (KeyError, TypeError, AttributeError, InvalidInstanceConfig, NoSlavesAvailableError):
                error_str = traceback.format_exc()
                log.error(error_str)
                send_event(service, instance, soa_dir, pysensu_yelp.Status.CRITICAL, error_str)
                return 1, None
    except bounce_lib.LockHeldException:
        log.error("Instance %s already being bounced. Exiting", short_id)
        return 0, None


if __name__ == "__main__":
    main()
