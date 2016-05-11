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
import argparse
import logging
import sys
import traceback
from collections import defaultdict

import pysensu_yelp

from paasta_tools import bounce_lib
from paasta_tools import drain_lib
from paasta_tools import marathon_tools
from paasta_tools import monitoring_tools
from paasta_tools.marathon_tools import kill_given_tasks
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


def send_sensu_bounce_keepalive(service, instance, cluster, soa_dir):
    """Send a Sensu event with a special ``ttl``, to let Sensu know that
    the everything is fine. This event is **not** fired when the bounce is in
    progress.

    If the bounce goes on for too long, this the ``ttl`` will expire and Sensu
    will emit a new event saying that this one didn't check in within the expected
    time-to-live."""
    ttl = '1h'
    monitoring_overrides = marathon_tools.load_marathon_service_config(
        service=service,
        instance=instance,
        cluster=cluster,
        soa_dir=soa_dir,
        load_deployments=False,
    ).get_monitoring()
    # Sensu currently emits events for expired ttl checks every 30s
    monitoring_overrides['check_every'] = '30s'
    monitoring_overrides['alert_after'] = '2m'
    monitoring_overrides['runbook'] = 'http://y/paasta-troubleshooting'
    monitoring_overrides['tip'] = ("Check out `paasta logs`. If the bounce hasn't made progress, "
                                   "it may mean that the new version isn't healthy.")
    # Dogfooding this alert till I'm comfortable it doesn't spam people
    monitoring_overrides['team'] = 'noop'
    monitoring_overrides['notification_email'] = 'kwa@yelp.com'

    monitoring_tools.send_event(
        service=service,
        check_name='paasta_bounce_progress.%s' % compose_job_id(service, instance),
        overrides=monitoring_overrides,
        status=pysensu_yelp.Status.OK,
        output="The bounce is in a steady state",
        soa_dir=soa_dir,
        ttl=ttl,
    )


def get_main_marathon_config():
    log.debug("Reading marathon configuration")
    marathon_config = marathon_tools.load_marathon_config()
    log.info("Marathon config is: %s", marathon_config)
    return marathon_config


def do_bounce(
    bounce_func,
    drain_method,
    config,
    new_app_running,
    happy_new_tasks,
    old_app_live_happy_tasks,
    old_app_live_unhappy_tasks,
    old_app_draining_tasks,
    service,
    bounce_method,
    serviceinstance,
    cluster,
    instance,
    marathon_jobid,
    client,
    soa_dir,
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
                '%d old apps.' % len(old_app_live_happy_tasks.keys()),
            ]),
            level='event',
        )
    else:
        # In a steady state. Let's let Sensu know everything is fine.
        send_sensu_bounce_keepalive(
            service=service,
            instance=instance,
            cluster=cluster,
            soa_dir=soa_dir,
        )

    all_draining_tasks = set()
    actions = bounce_func(
        new_config=config,
        new_app_running=new_app_running,
        happy_new_tasks=happy_new_tasks,
        old_app_live_happy_tasks=old_app_live_happy_tasks,
        old_app_live_unhappy_tasks=old_app_live_unhappy_tasks,
    )

    if actions['create_app'] and not new_app_running:
        log_bounce_action(
            line='%s bounce creating new app with app_id %s' % (bounce_method, marathon_jobid),
        )
        bounce_lib.create_marathon_app(marathon_jobid, config, client)
    if len(actions['tasks_to_drain']) > 0:
        tasks_to_drain_by_app_id = defaultdict(set)
        for task in actions['tasks_to_drain']:
            tasks_to_drain_by_app_id[task.app_id].add(task)
        for app_id, tasks in tasks_to_drain_by_app_id.items():
            log_bounce_action(
                line='%s bounce draining %d old tasks with app_id %s' %
                (bounce_method, len(tasks), app_id),
            )
        for task in actions['tasks_to_drain']:
            all_draining_tasks.add(task)
            drain_method.drain(task)
    for app, tasks in old_app_draining_tasks.items():
        for task in tasks:
            all_draining_tasks.add(task)

    tasks_to_kill = set()

    for task in all_draining_tasks:
        if drain_method.is_safe_to_kill(task):
            tasks_to_kill.add(task)
            log_bounce_action(line='%s bounce killing drained task %s' % (bounce_method, task.id))

    kill_given_tasks(client=client, task_ids=[task.id for task in tasks_to_kill], scale=True)

    apps_to_kill = []
    for app in old_app_live_happy_tasks.keys():
        if app != '/%s' % marathon_jobid:
            live_happy_tasks = old_app_live_happy_tasks[app]
            live_unhappy_tasks = old_app_live_unhappy_tasks[app]
            draining_tasks = old_app_draining_tasks[app]

            if 0 == len((live_happy_tasks | live_unhappy_tasks | draining_tasks) - tasks_to_kill):
                apps_to_kill.append(app)

    if apps_to_kill:
        log_bounce_action(
            line='%s bounce removing old unused apps with app_ids: %s' %
            (
                bounce_method,
                ', '.join(apps_to_kill)
            ),
        )
        bounce_lib.kill_old_ids(apps_to_kill, client)

    all_old_tasks = set.union(set(), *old_app_live_happy_tasks.values())
    all_old_tasks = set.union(all_old_tasks, *old_app_live_unhappy_tasks.values())
    all_old_tasks = set.union(all_old_tasks, *old_app_draining_tasks.values())

    # log if we appear to be finished
    if all([
        (apps_to_kill or tasks_to_kill),
        apps_to_kill == old_app_live_happy_tasks.keys(),
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


def get_old_happy_unhappy_draining_tasks_for_app(app, drain_method, service, nerve_ns, bounce_health_params,
                                                 system_paasta_config):
    tasks_by_state = {
        'happy': set(),
        'unhappy': set(),
        'draining': set(),
    }

    happy_tasks = bounce_lib.get_happy_tasks(app, service, nerve_ns, system_paasta_config, **bounce_health_params)
    for task in app.tasks:
        if drain_method.is_draining(task):
            state = 'draining'
        elif task in happy_tasks:
            state = 'happy'
        else:
            state = 'unhappy'
        tasks_by_state[state].add(task)

    return tasks_by_state


def get_old_happy_unhappy_draining_tasks(other_apps, drain_method, service, nerve_ns, bounce_health_params,
                                         system_paasta_config):
    """Split tasks from old apps into 3 categories:
      - live (not draining) and happy (according to get_happy_tasks)
      - live (not draining) and unhappy
      - draining
    """

    old_app_live_happy_tasks = {}
    old_app_live_unhappy_tasks = {}
    old_app_draining_tasks = {}

    for app in other_apps:

        tasks_by_state = get_old_happy_unhappy_draining_tasks_for_app(
            app, drain_method, service, nerve_ns, bounce_health_params, system_paasta_config)

        old_app_live_happy_tasks[app.id] = tasks_by_state['happy']
        old_app_live_unhappy_tasks[app.id] = tasks_by_state['unhappy']
        old_app_draining_tasks[app.id] = tasks_by_state['draining']

    return old_app_live_happy_tasks, old_app_live_unhappy_tasks, old_app_draining_tasks


def deploy_service(
    service,
    instance,
    marathon_jobid,
    config,
    client,
    bounce_method,
    drain_method_name,
    drain_method_params,
    nerve_ns,
    bounce_health_params,
    soa_dir,
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
    :returns: A tuple of (status, output) to be used with send_sensu_event"""

    def log_deploy_error(errormsg, level='event'):
        return _log(
            service=service,
            line=errormsg,
            component='deploy',
            level='event',
            cluster=cluster,
            instance=instance
        )

    short_id = marathon_tools.format_job_id(service, instance)

    system_paasta_config = load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()
    existing_apps = marathon_tools.get_matching_apps(service, instance, client, embed_failures=True)
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
        return (1, errormsg)

    old_app_live_happy_tasks, old_app_live_unhappy_tasks, old_app_draining_tasks = get_old_happy_unhappy_draining_tasks(
        other_apps,
        drain_method,
        service,
        nerve_ns,
        bounce_health_params,
        system_paasta_config,
    )

    if new_app_running:
        protected_draining_tasks = set()
        if new_app.instances < config['instances']:
            client.scale_app(app_id=new_app.id, instances=config['instances'], force=True)
        elif new_app.instances > config['instances']:
            num_tasks_to_scale = max(min(len(new_app.tasks), new_app.instances) - config['instances'], 0)
            task_dict = get_old_happy_unhappy_draining_tasks_for_app(
                new_app,
                drain_method,
                service,
                nerve_ns,
                bounce_health_params,
                system_paasta_config,
            )
            scaling_app_happy_tasks = list(task_dict['happy'])
            scaling_app_unhappy_tasks = list(task_dict['unhappy'])
            scaling_app_draining_tasks = list(task_dict['draining'])

            tasks_to_move_draining = min(len(scaling_app_draining_tasks), num_tasks_to_scale)
            old_app_draining_tasks[new_app.id] = set(scaling_app_draining_tasks[:tasks_to_move_draining])
            protected_draining_tasks.update(scaling_app_draining_tasks[:tasks_to_move_draining])
            num_tasks_to_scale = num_tasks_to_scale - tasks_to_move_draining

            tasks_to_move_unhappy = min(len(scaling_app_unhappy_tasks), num_tasks_to_scale)
            old_app_live_unhappy_tasks[new_app.id] = set(scaling_app_unhappy_tasks[:tasks_to_move_unhappy])
            num_tasks_to_scale = num_tasks_to_scale - tasks_to_move_unhappy

            tasks_to_move_happy = min(len(scaling_app_happy_tasks), num_tasks_to_scale)
            old_app_live_happy_tasks[new_app.id] = set(scaling_app_happy_tasks[:tasks_to_move_happy])
            happy_new_tasks = scaling_app_happy_tasks[tasks_to_move_happy:]
        # If any tasks on the new app happen to be draining (e.g. someone reverts to an older version with
        # `paasta mark-for-deployment`), then we should undrain them.
        for task in new_app.tasks:
            if task not in protected_draining_tasks:
                drain_method.stop_draining(task)

    # Re-drain any already draining tasks on old apps
    for tasks in old_app_draining_tasks.values():
        for task in tasks:
            drain_method.drain(task)

    # log all uncaught exceptions and raise them again
    try:
        try:
            bounce_func = bounce_lib.get_bounce_method_func(bounce_method)
        except KeyError:
            errormsg = 'ERROR: bounce_method not recognized: %s. Must be one of (%s)' % \
                (bounce_method, ', '.join(bounce_lib.list_bounce_methods()))
            log_deploy_error(errormsg)
            return (1, errormsg)

        try:
            with bounce_lib.bounce_lock_zookeeper(short_id):
                do_bounce(
                    bounce_func=bounce_func,
                    drain_method=drain_method,
                    config=config,
                    new_app_running=new_app_running,
                    happy_new_tasks=happy_new_tasks,
                    old_app_live_happy_tasks=old_app_live_happy_tasks,
                    old_app_live_unhappy_tasks=old_app_live_unhappy_tasks,
                    old_app_draining_tasks=old_app_draining_tasks,
                    service=service,
                    bounce_method=bounce_method,
                    serviceinstance=serviceinstance,
                    cluster=cluster,
                    instance=instance,
                    marathon_jobid=marathon_jobid,
                    client=client,
                    soa_dir=soa_dir,
                )

        except bounce_lib.LockHeldException:
            log.error("Instance %s already being bounced. Exiting", short_id)
            return (1, "Instance %s is already being bounced." % short_id)
    except Exception:
        loglines = ['Exception raised during deploy of service %s:' % service]
        loglines.extend(traceback.format_exc().rstrip().split("\n"))
        for logline in loglines:
            log_deploy_error(logline, level='debug')
        raise

    return (0, 'Service deployed.')


def setup_service(service, instance, client, marathon_config,
                  service_marathon_config, soa_dir):
    """Setup the service instance given and attempt to deploy it, if possible.
    Doesn't do anything if the service is already in Marathon and hasn't changed.
    If it's not, attempt to find old instances of the service and bounce them.

    :param service: The service name to setup
    :param instance: The instance of the service to setup
    :param client: A MarathonClient object
    :param marathon_config: The marathon configuration dict
    :param service_marathon_config: The service instance's configuration dict
    :returns: A tuple of (status, output) to be used with send_sensu_event"""

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
        return (1, error_msg)

    full_id = marathon_app_dict['id']
    service_namespace_config = marathon_tools.load_service_namespace_config(service, instance)

    log.info("Desired Marathon instance id: %s", full_id)
    return deploy_service(
        service=service,
        instance=instance,
        marathon_jobid=full_id,
        config=marathon_app_dict,
        client=client,
        bounce_method=service_marathon_config.get_bounce_method(),
        drain_method_name=service_marathon_config.get_drain_method(service_namespace_config),
        drain_method_params=service_marathon_config.get_drain_method_params(service_namespace_config),
        nerve_ns=service_marathon_config.get_nerve_namespace(),
        bounce_health_params=service_marathon_config.get_bounce_health_params(service_namespace_config),
        soa_dir=soa_dir,
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

    marathon_config = get_main_marathon_config()
    client = marathon_tools.get_marathon_client(marathon_config.get_url(), marathon_config.get_username(),
                                                marathon_config.get_password())

    num_failed_deployments = 0
    for service_instance in args.service_instance_list:
        try:
            service, instance, _, __ = decompose_job_id(service_instance)
        except InvalidJobNameError:
            log.error("Invalid service instance specified. Format is service%sinstance." % SPACER)
            num_failed_deployments = num_failed_deployments + 1
        else:
            if deploy_marathon_service(service, instance, client, soa_dir, marathon_config):
                num_failed_deployments = num_failed_deployments + 1

    log.debug("%d out of %d service.instances failed to deploy." %
              (num_failed_deployments, len(args.service_instance_list)))

    sys.exit(1 if num_failed_deployments else 0)


def deploy_marathon_service(service, instance, client, soa_dir, marathon_config):
    try:
        service_instance_config = marathon_tools.load_marathon_service_config(
            service,
            instance,
            load_system_paasta_config().get_cluster(),
            soa_dir=soa_dir,
        )
    except NoDeploymentsAvailable:
        log.debug("No deployments found for %s.%s in cluster %s. Skipping." %
                  (service, instance, load_system_paasta_config().get_cluster()))
        return 0
    except NoConfigurationForServiceError:
        error_msg = "Could not read marathon configuration file for %s.%s in cluster %s" % \
                    (service, instance, load_system_paasta_config().get_cluster())
        log.error(error_msg)
        return 1

    try:
        status, output = setup_service(service, instance, client, marathon_config,
                                       service_instance_config, soa_dir)
        sensu_status = pysensu_yelp.Status.CRITICAL if status else pysensu_yelp.Status.OK
        send_event(service, instance, soa_dir, sensu_status, output)
        return 0
    except (KeyError, TypeError, AttributeError, InvalidInstanceConfig):
        import traceback
        error_str = traceback.format_exc()
        log.error(error_str)
        send_event(service, instance, soa_dir, pysensu_yelp.Status.CRITICAL, error_str)
        return 1


if __name__ == "__main__":
    main()
