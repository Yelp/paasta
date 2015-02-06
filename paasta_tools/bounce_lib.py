#!/usr/bin/env python
from contextlib import contextmanager, nested
import fcntl
import logging
import os
import signal
import time

from functools import wraps
from kazoo.client import KazooClient
from kazoo.exceptions import LockTimeout
from marathon.models import MarathonApp

import marathon_tools

log = logging.getLogger('__main__')
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


_bounce_method_funcs = {}


def bounce_method(name):
    """Returns a decorator that wraps a bounce function in a lock, and
    registers that bounce function so get_bounce_method_func can find it."""
    def outer(bounce_func):
        """This decorator wraps a bounce function in a lock (so that we don't
            try to bounce the same service twice at the same time."""
        @wraps(bounce_func)
        def bounce_wrapper(service_name, instance_name, existing_apps,
                           new_config, client):
            bounce_func(service_name, instance_name, existing_apps,
                        new_config, client)
        _bounce_method_funcs[name] = bounce_wrapper
        return bounce_wrapper
    return outer


def get_bounce_method_func(name):
    return _bounce_method_funcs[name]


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
        raise TimeoutException("Time limit exired")
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(minutes * 60)
    try:
        yield
    finally:
        signal.alarm(0)


def wait_for_create(app_id, client):
    """Wait for the specified app_id to be listed in marathon.
    Waits WAIT_CREATE_S seconds between calls to list_apps.

    :param app_id: The app_id to ensure creation for
    :param client: A MarathonClient object"""
    while marathon_tools.is_app_id_running(app_id, client) is False:
        log.info("Waiting for %s to be created in marathon..", app_id)
        time.sleep(WAIT_CREATE_S)


def create_marathon_app(app_id, config, client):
    """Create a new marathon application with a given
    config and marathon client object.

    :param config: The marathon configuration to be deployed
    :param client: A MarathonClient object"""
    with nested(create_app_lock(), time_limit(1)):
        client.create_app(app_id, MarathonApp(**config))
        wait_for_create(app_id, client)


def wait_for_delete(app_id, client):
    """Wait for the specified app_id to not be listed in marathon
    anymore. Waits WAIT_DELETE_S seconds inbetween checks.

    :param app_id: The app_id to check for deletion
    :param client: A MarathonClient object"""
    while marathon_tools.is_app_id_running(app_id, client) is True:
        log.info("Waiting for %s to be deleted from marathon...", app_id)
        time.sleep(WAIT_DELETE_S)


def delete_marathon_app(app_id, client):
    """Delete a new marathon application with a given
    app_id and marathon client object.

    :param app_id: The marathon app id to be deleted
    :param client: A MarathonClient object"""
    with nested(create_app_lock(), time_limit(1)):
        # Scale app to 0 first to work around
        # https://github.com/mesosphere/marathon/issues/725
        client.scale_app(app_id, instances=0, force=True)
        time.sleep(1)
        client.delete_app(app_id, force=True)
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


@bounce_method('brutal')
def brutal_bounce(
    service_name,
    instance_name,
    existing_apps,
    new_config,
    client,
):
    """Pays no regard to safety. Starts the new app if necessary, and kills any
    old ones. Mostly meant as an example of the simplest working bounce method,
    but might be tolerable for some services.

    :param service_name: service name
    :param instance_name: instance name
    :param existing_apps: Apps that marathon is already aware of.
    :param new_config: The complete marathon job configuration for the new job
    :param client: A marathon.MarathonClient object
    """
    new_id = new_config['id']
    existing_ids = set(a.id for a in existing_apps)

    # Start the app if it's not there
    if new_id not in existing_ids:
        create_marathon_app(new_id, new_config, client)

    # Kill any old instances.
    kill_old_ids(existing_ids - set([new_id]), client)
