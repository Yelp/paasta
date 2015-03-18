#!/usr/bin/env python
"""
Usage: ./setup_marathon_job.py <service_name.instance_name> [options]

Deploy a service instance to Marathon from a configuration file.
Attempts to load the marathon configuration at
/etc/paasta_tools/marathon_config.json, and read
from the soa_dir /nail/etc/services by default.

This script will attempt to load a service's configuration
from the soa_dir and generate a marathon job configuration for it,
as well as handle deploying that configuration with a bounce strategy
if there's an old version of the service. To determine whether or not
a deployment is 'old', each marathon job has a complete id of
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
import service_configuration_lib
import sys

from paasta_tools import bounce_lib
from paasta_tools import marathon_tools
from paasta_tools import monitoring_tools

# Marathon REST API:
# https://github.com/mesosphere/marathon/blob/master/REST.md#post-v2apps

ID_SPACER = marathon_tools.ID_SPACER
log = logging.getLogger('__main__')
log.addHandler(logging.StreamHandler(sys.stdout))


def parse_args():
    parser = argparse.ArgumentParser(description='Creates marathon jobs.')
    parser.add_argument('service_instance',
                        help="The marathon instance of the service to create or update",
                        metavar="SERVICE.INSTANCE")
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
    :param output: The output to emit for this event"""
    # This function assumes the input is a string like "mumble.main"
    framework = 'marathon'
    team = monitoring_tools.get_team(framework, name, instance, soa_dir)
    if not team:
        return
    check_name = 'setup_marathon_job.%s%s%s' % (name, ID_SPACER, instance)
    runbook = 'y/rb-marathon'
    result_dict = {
        'tip': monitoring_tools.get_tip(framework, name, instance, soa_dir),
        'notification_email': monitoring_tools.get_notification_email(framework, name, instance, soa_dir),
        'irc_channels': monitoring_tools.get_irc_channels(framework, name, instance, soa_dir),
        'alert_after': '6m',
        'check_every': '2m',
        'realert_every': -1,
        'source': 'mesos-%s' % marathon_tools.get_cluster(),
    }
    pysensu_yelp.send_event(check_name, runbook, status, output, team, **result_dict)


def get_main_marathon_config():
    log.debug("Reading marathon configuration")
    marathon_config = marathon_tools.MarathonConfig.read()
    log.info("Marathon config is: %s", marathon_config)
    return marathon_config


def deploy_service(service_name, instance_name, marathon_jobid, config, client,
                   bounce_method, nerve_ns, bounce_health_params):
    """Deploy the service to marathon, either directly or via a bounce if needed.
    Called by setup_service when it's time to actually deploy.

    :param name: The full marathon job name to deploy
    :param config: The complete configuration dict to send to marathon
    :param client: A MarathonClient object
    :param namespace: The service's Smartstack namespace
    :param bounce_method: The bounce method to use, if needed
    :param nerve_ns: The nerve namespace to look in.
    :param bounce_health_params: A dictionary of options for bounce_lib.get_happy_tasks.
    :returns: A tuple of (status, output) to be used with send_sensu_event"""
    log.info("Deploying service instance %s with bounce_method %s",
             service_name, bounce_method)
    log.debug("Searching for old service instance iterations")
    short_id = marathon_tools.remove_tag_from_job_id(marathon_jobid)

    # would do embed_failures but we support versions of marathon where https://github.com/mesosphere/marathon/pull/1105
    # isn't fixed.
    app_list = client.list_apps(embed_failures=True)
    existing_apps = [app for app in app_list if short_id in app.id]
    new_app_list = [a for a in existing_apps if a.id == '/%s' % config['id']]
    other_apps = [a for a in existing_apps if a.id != '/%s' % config['id']]

    if new_app_list:
        new_app = new_app_list[0]
        if len(new_app_list) != 1:
            raise ValueError("Only expected one app per ID; found %d" % len(new_app_list))
        new_app_running = True
        happy_new_tasks = bounce_lib.get_happy_tasks(new_app.tasks, service_name, nerve_ns, **bounce_health_params)
    else:
        new_app_running = False
        happy_new_tasks = []

    old_app_tasks = dict([(a.id, set(a.tasks)) for a in other_apps])

    try:
        bounce_func = bounce_lib.get_bounce_method_func(bounce_method)
    except KeyError:
        log.error("bounce_method not recognized: %s. Exiting", bounce_method)
        return (1, "bounce_method not recognized: %s" % bounce_method)

    try:
        with bounce_lib.bounce_lock_zookeeper(short_id):
            log.info("Initiating %s bounce on %s.", bounce_method, short_id)
            actions = bounce_func(
                new_config=config,
                new_app_running=new_app_running,
                happy_new_tasks=happy_new_tasks,
                old_app_tasks=old_app_tasks,
            )

            if actions['create_app'] and not new_app_running:
                log.info('Bounce method %s requested new app to be created (id %s)', bounce_method, marathon_jobid)
                bounce_lib.create_marathon_app(marathon_jobid, config, client)
            for task in actions['tasks_to_kill']:
                log.info('Bounce method %s requested task %s to be killed (app id %s)', bounce_method, task.id,
                         task.app_id)
                client.kill_task(task.app_id, task.id, scale=True)
            if actions['apps_to_kill']:
                log.info('Bounce method %s requested apps %r to be killed', bounce_method, actions['apps_to_kill'])
            bounce_lib.kill_old_ids(actions['apps_to_kill'], client)

    except bounce_lib.LockHeldException:
        log.error("Instance %s already being bounced. Exiting", short_id)
        return (1, "Instance %s is already being bounced.", short_id)

    log.info("%s deployed. Exiting", marathon_jobid)
    return (0, 'Service deployed.')


def setup_service(service_name, instance_name, client, marathon_config,
                  service_marathon_config):
    """Setup the service instance given and attempt to deploy it, if possible.
    Doesn't do anything if the service is already in Marathon and hasn't changed.
    If it's not, attempt to find old instances of the service and bounce them.

    :param service_name: The service name to setup
    :param instance_name: The instance of the service to setup
    :param client: A MarathonClient object
    :param marathon_config: The marathon configuration dict
    :param service_marathon_config: The service instance's configuration dict
    :returns: A tuple of (status, output) to be used with send_sensu_event"""

    log.info("Setting up instance %s for service %s", instance_name, service_name)
    try:
        complete_config = marathon_tools.create_complete_config(service_name, instance_name, marathon_config)
    except marathon_tools.NoDockerImageError:
        error_msg = "Docker image for {0}.{1} not in deployments.json. Exiting. Has Jenkins deployed it?".format(
            service_name,
            instance_name
        )
        log.error(error_msg)
        return (1, error_msg)

    full_id = complete_config['id']

    log.info("Desired Marathon instance id: %s", full_id)
    return deploy_service(
        service_name,
        instance_name,
        full_id,
        complete_config,
        client,
        service_marathon_config.get_bounce_method(),
        service_marathon_config.get_nerve_namespace(),
        service_marathon_config.get_bounce_health_params(),
    )


def main():
    """Attempt to set up the marathon service instance given.
    Exits 1 if the deployment failed.
    This is done in the following order:

    - Load the marathon configuration
    - Connect to marathon
    - Load the service instance's configuration
    - Create the complete marathon job configuration
    - Deploy/bounce the service
    - Emit an event about the deployment to sensu"""
    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)
    try:
        service_name, instance_name = args.service_instance.split(ID_SPACER)
    except ValueError:
        log.error("Invalid service instance specified. Format is service_name.instance_name.")
        sys.exit(1)

    marathon_config = get_main_marathon_config()
    client = marathon_tools.get_marathon_client(marathon_config['url'], marathon_config['user'],
                                                marathon_config['pass'])

    service_instance_config = marathon_tools.MarathonServiceConfig.read(
        service_name,
        instance_name,
        marathon_tools.get_cluster(),
        soa_dir,
    )

    if service_instance_config:
        try:
            status, output = setup_service(service_name, instance_name, client, marathon_config,
                                           service_instance_config)
            sensu_status = pysensu_yelp.Status.CRITICAL if status else pysensu_yelp.Status.OK
            send_event(service_name, instance_name, soa_dir, sensu_status, output)
            # We exit 0 because the script finished ok and the event was sent to the right team.
            sys.exit(0)
        except (KeyError, TypeError, AttributeError):
            import traceback
            error_str = traceback.format_exc()
            log.error(error_str)
            send_event(service_name, instance_name, soa_dir, pysensu_yelp.Status.CRITICAL, error_str)
            # We exit 0 because the script finished ok and the event was sent to the right team.
            sys.exit(0)
    else:
        error_msg = "Could not read marathon configuration file for %s in cluster %s" % \
                    (args.service_instance, marathon_tools.get_cluster())
        log.error(error_msg)
        send_event(service_name, instance_name, soa_dir, pysensu_yelp.Status.CRITICAL, error_msg)
        sys.exit(1)


if __name__ == "__main__":
    main()
