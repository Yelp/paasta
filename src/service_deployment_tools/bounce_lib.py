#!/usr/bin/env python
from contextlib import contextmanager, nested
import fcntl
import logging
import os
import signal
import time
from kazoo.client import KazooClient
from kazoo.exceptions import LockTimeout
import marathon_tools
from service_deployment_tools.monitoring.replication_utils import get_replication_for_services


log = logging.getLogger(__name__)
DEFAULT_SYNAPSE_HOST = 'localhost:3212'
CROSSOVER_MAX_TIME_M = 30  # Max time in minutes to bounce
CROSSOVER_FRACTION_REQUIRED = 0.9  # % of new instances needed in HAProxy for a successful bounce
CROSSOVER_SLEEP_INTERVAL_S = 10  # seconds to sleep between scaling an app and checking HAProxy
ZK_LOCK_CONNECT_TIMEOUT_S = 10.0  # seconds to wait to connect to zookeeper
ZK_LOCK_PATH = '/bounce'
WAIT_CREATE_S = 3
WAIT_DELETE_S = 5


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
    remove = False
    try:
        fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        remove = True
        yield
    except IOError:
        raise IOError("Service %s is already being bounced!" % name)
    finally:
        fd.close()
        if remove:
            os.remove(lockfile)


@contextmanager
def bounce_lock_zookeeper(name):
    """Acquire a bounce lock in zookeeper for the name given. The name should
    generally be the service namespace being bounced.

    This is a contextmanager. Please use it via 'with bounce_lock(name):'.

    :param name: The lock name to acquire"""
    zk = KazooClient(hosts=marathon_tools.get_zk_hosts(), timeout=ZK_LOCK_CONNECT_TIMEOUT_S)
    zk.start()
    lock = zk.Lock('%s/%s' % (ZK_LOCK_PATH, name))
    acquired = False
    try:
        lock.acquire(timeout=1)  # timeout=0 throws some other strange exception
        acquired = True
        yield
    except LockTimeout:
        raise IOError("Service %s is already being bounced!" % name)
    finally:
        if acquired:
            lock.release()
        zk.stop()


@contextmanager
def create_app_lock():
    """Acquire a lock in zookeeper for creating a marathon app. This is
    due to marathon's extreme lack of resilience with creating multiple
    apps at once, so we use this to not do that and only deploy
    one app at a time."""
    zk = KazooClient(hosts=marathon_tools.get_zk_hosts(), timeout=ZK_LOCK_CONNECT_TIMEOUT_S)
    zk.start()
    lock = zk.Lock('%s/%s' % (ZK_LOCK_PATH, 'create_marathon_app_lock'))
    try:
        lock.acquire(timeout=30)  # timeout=0 throws some other strange exception
        yield
    except LockTimeout:
        raise IOError("Failed to acquire lock for creating marathon app!")
    finally:
        lock.release()
        zk.stop()


@contextmanager
def time_limit(minutes):
    """A contextmanager to raise a TimeoutException whenever a specified
    number of minutes has passed.

    :param minutes: The number of minutes until an exception is raised"""
    def signal_handler(signum, frame):
        raise TimeoutException("Service bounce timed out")
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(minutes * 60)
    try:
        yield
    finally:
        signal.alarm(0)


def wait_for_create(app_id, client):
    """Wait for the specified app_id to be listed in marathon.
    Waits WAIT_CREATE_S seconds between calls to list_apps, with a miniumum of
    one wait (occuring when the function is called).

    :param app_id: The app_id to ensure creation for
    :param client: A MarathonClient object"""
    app_ids = []
    while app_id not in app_ids:
        time.sleep(WAIT_CREATE_S)
        try:
            app_ids = [app.id for app in client.list_apps()]
        except:
            return


def create_marathon_app(config, client):
    """Create a new marathon application with a given
    config and marathon client object.

    :param config: The marathon configuration to be deployed
    :param client: A MarathonClient object"""
    with create_app_lock():
        client.create_app(**config)
        wait_for_create(config['id'], client)


def wait_for_delete(app_id, client):
    """Wait for the specified app_id to not be listed in marathon
    anymore. Waits WAIT_DELETE_S seconds inbetween checks, starting with a
    sleep when the function is called.

    :param app_id: The app_id to check for deletion
    :param client: A MarathonClient object"""
    app_ids = [app_id]
    while app_id in app_ids:
        time.sleep(WAIT_DELETE_S)
        try:
            app_ids = [app.id for app in client.list_apps()]
        except:
            return


def delete_marathon_app(app_id, client):
    """Delete a new marathon application with a given
    app_id and marathon client object.

    :param app_id: The marathon app id to be deleted
    :param client: A MarathonClient object"""
    client.delete_app(app_id)
    wait_for_delete(app_id, client)


def kill_old_ids(old_ids, client):
    """Kill old marathon job ids. Skips anything that doesn't exist or
    otherwise raises an exception. If this doesn't kill something due
    to an exception, that's okay- it'll get cleaned up later.

    :param old_ids: A list of old job/app ids to kill
    :param client: A marathon.MarathonClient object"""
    for app in old_ids:
        try:
            log.info("Killing %s", app)
            delete_marathon_app(app, client)
        except:
            continue


def brutal_bounce(old_ids, new_config, client, namespace):
    """Brutally bounce the service by killing all old instances first.

    Kills all old_ids then spawns the new job/app.

    :param old_ids: Old job ids to kill off
    :param new_config: The complete marathon job configuration for the new job
    :param client: A marathon.MarathonClient object
    :param namespace: The smartstack namespace of the service"""
    target_service = marathon_tools.remove_tag_from_job_id(new_config['id']).replace('--', '_')
    service_namespace = '%s%s%s' % (target_service.split(marathon_tools.ID_SPACER)[0],
                                    marathon_tools.ID_SPACER, namespace)
    with bounce_lock_zookeeper(service_namespace):
        kill_old_ids(old_ids, client)
        log.info("Creating %s", new_config['id'])
        create_marathon_app(new_config, client)


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
    target_service = marathon_tools.remove_tag_from_job_id(new_config['id']).replace('--', '_')
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
        with nested(bounce_lock_zookeeper(service_namespace), time_limit(CROSSOVER_MAX_TIME_M)):
            # Okay, deploy the new job!
            create_marathon_app(new_config, client)
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
    finally:
        # The reason we don't delete from scalable_apps is because it's possible to
        # time out a bounce while a scalable app was being scaled down, and
        # wasn't added back into the list yet. This'll make sure everything
        # is gone and only the new job remains.
        kill_old_ids(old_ids, client)
