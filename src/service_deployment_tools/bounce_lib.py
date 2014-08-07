#!/usr/bin/env python
from contextlib import contextmanager, nested
import fcntl
import logging
import os
import signal
import time
import marathon_tools
from service_deployment_tools.monitoring.replication_utils import get_replication_for_services


log = logging.getLogger(__name__)
DEFAULT_SYNAPSE_HOST = 'localhost:3212'
CROSSOVER_MAX_TIME_M = 30  # Max time in minutes to bounce
CROSSOVER_FRACTION_REQUIRED = 0.9  # % of new instances needed in HAProxy for a successful bounce
CROSSOVER_SLEEP_INTERVAL_S = 10  # seconds to sleep between scaling an app and checking HAProxy


class TimeoutException(Exception):
    """An exception type used by time_limit."""
    pass


@contextmanager
def bounce_lock(name):
    """Acquire a bounce lockfile for the name given. The name should generally
    be the service namespace being bounced.

    This is a contextmanager. Please use it via 'with bounce_lock(name):'.

    :param name: The lock name to acquire"""
    lockfile = '/var/lock/%s.lock' % name
    fd = open(lockfile, 'w')
    try:
        fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        raise IOError("Service %s is already being bounced!" % name)
    try:
        yield
    finally:
        fd.close()
        os.remove(lockfile)


@contextmanager
def time_limit(minutes):
    """A contextmanager to raise a TimeoutException whenever a specified
    number of minutes has passed.

    :param minutes: The number of minutes until an exception is raised"""
    def signal_handler(signum, frame):
        raise TimeoutException("Timeout")
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(minutes * 60)
    try:
        yield
    finally:
        signal.alarm(0)


def kill_old_ids(old_ids, client):
    """Kill old marathon job ids. Skips anything that doesn't exist.

    :param old_ids: A list of old job/app ids to kill
    :param client: A marathon.MarathonClient object"""
    for app in old_ids:
        try:
            log.info("Killing %s", app)
            client.delete_app(app)
        except KeyError:
            continue


def brutal_bounce(old_ids, new_config, client, namespace):
    """Brutally bounce the service by killing all old instances first.

    Kills all old_ids then spawns the new job/app.

    :param old_ids: Old job ids to kill off
    :param new_config: The complete marathon job configuration for the new job
    :param client: A marathon.MarathonClient object
    :param namespace: The smartstack namespace of the service"""
    target_service = marathon_tools.remove_tag_from_job_id(new_config['id'])
    service_namespace = '%s%s%s' % (target_service.split(marathon_tools.ID_SPACER)[0],
                                    marathon_tools.ID_SPACER, namespace)
    with bounce_lock(service_namespace):
        kill_old_ids(old_ids, client)
        log.info("Creating %s", new_config['id'])
        client.create_app(**new_config)


def scale_apps(scalable_apps, remove_count, client):
    """Kill off a number of apps from the scalable_apps list, composed of
    tuples of (old_job_id, instance_count), equal to remove_count.

    If an app is deleted because it was scaled, it is removed from the
    scalable_apps list. If an app is scaled down, its instance_count
    changes to reflect this.

    If remove_count is <= 0, returns 0 immediately.

    :param scalable_apps: A list of tuples of (old_job_id, instance_count)
    :param remove_count: The number of instances to kill off
    :param client: A marathon.MarathonClient object
    :returns: The total number of removed instances"""
    total_remove_count = remove_count
    # We're scaling down- if there's a shortage of instances (the count
    # is negative or 0), then we shouldn't scale back any more yet!
    if remove_count <= 0:
        return 0
    # Okay, now scale down instances from old jobs.
    while remove_count > 0:
        app_id, remaining = scalable_apps.pop()
        # If this app can be removed completely, do it!
        if remove_count >= remaining:
            client.delete_app(app_id)
            remove_count -= remaining
        # Otherwise, scale it down and add the app back into the list.
        else:
            client.scale_app(app_id, delta=(-1 * remove_count))
            scalable_apps.append((app_id, remaining - remove_count))
            remove_count = 0
    return total_remove_count


def crossover_bounce(old_ids, new_config, client, namespace):
    """Bounce the service via crossover: spin up the new instances
    first, and then kill old ones as they get registered in nerve.

    :param old_ids: Old job ids to kill off
    :param new_config: The complete marathon job configuration for the new job
    :param client: A marathon.MarathonClient object
    :param namespace: The smartstack namespace of the service"""
    target_service = marathon_tools.remove_tag_from_job_id(new_config['id'])
    service_namespace = '%s%s%s' % (target_service.split(marathon_tools.ID_SPACER)[0],
                                    marathon_tools.ID_SPACER, namespace)
    scalable_apps = [(old_id, client.get_app(old_id).instances) for old_id in old_ids]
    # First, how many instances are currently UP in HAProxy?
    initial_instances = get_replication_for_services(DEFAULT_SYNAPSE_HOST,
                                                     [service_namespace])[service_namespace]
    # Alright, we want to get a fraction of the way there before
    # we just cut off the rest of the instances.
    target_delta = max(int(new_config['instances'] * CROSSOVER_FRACTION_REQUIRED), 1)
    total_delta = 0
    try:
        with nested(bounce_lock(service_namespace), time_limit(CROSSOVER_MAX_TIME_M)):
            # Okay, deploy the new job!
            client.create_app(**new_config)
            # Sleep once to give the stack some time to spin up.
            time.sleep(CROSSOVER_SLEEP_INTERVAL_S)
            # If we run of out stuff to kill, we're just as completed
            # as if we had reached the target_delta.
            # This can happen if the # of instances is increased in a job.
            while total_delta < target_delta and scalable_apps:
                # How many instances are there now?
                available_instances = get_replication_for_services(DEFAULT_SYNAPSE_HOST,
                                                                   [service_namespace])[service_namespace]
                total_delta += scale_apps(scalable_apps, available_instances - initial_instances, client)
                # Wait for nerve/synapse to reconfigure.
                time.sleep(CROSSOVER_SLEEP_INTERVAL_S)
    except TimeoutException:
        pass
    # The reason we don't delete from scalable_apps is because it's possible to
    # time out a bounce while a scalable app was being scaled down, and
    # wasn't added back into the list yet. This'll make sure everything
    # is gone and only the new job remains.
    kill_old_ids(old_ids, client)
