#!/usr/bin/env python
from contextlib import contextmanager, nested
import datetime
import fcntl
import logging
import os
import signal
import time

from kazoo.client import KazooClient
from kazoo.exceptions import LockTimeout
from marathon.models import MarathonApp
from paasta_tools.monitoring.replication_utils import \
    get_registered_marathon_tasks

import marathon_tools

log = logging.getLogger('__main__')
DEFAULT_SYNAPSE_HOST = 'localhost:3212'
ZK_LOCK_CONNECT_TIMEOUT_S = 10.0  # seconds to wait to connect to zookeeper
ZK_LOCK_PATH = '/bounce'
WAIT_CREATE_S = 3
WAIT_DELETE_S = 5


class TimeoutException(Exception):
    """An exception type used by time_limit."""
    pass


_bounce_method_funcs = {}


def register_bounce_method(name):
    """Returns a decorator that registers that bounce function at a given name
    so get_bounce_method_func can find it."""
    def outer(bounce_func):
        _bounce_method_funcs[name] = bounce_func
        return bounce_func
    return outer


def get_bounce_method_func(name):
    return _bounce_method_funcs[name]


class LockHeldException(Exception):
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
        raise LockHeldException("Service %s is already being bounced!" % name)
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
        raise LockHeldException("Service %s is already being bounced!" % name)
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
        raise LockHeldException("Failed to acquire lock for creating marathon app!")
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


def get_happy_tasks(tasks, service_name, nerve_ns, min_task_uptime=None, check_haproxy=False):
    """Given a list of MarathonTask objects, return the subset which are considered healthy. With the default options,
    this is a noop - it just returns tasks. For it to do anything interesting, set min_task_uptime or check_haproxy.

    :param tasks: A list of MarathonTask objects.
    :param service_name: The name of the service.
    :param nerve_ns: The nerve namespace
    :param min_task_uptime: Minimum number of seconds that a task must be running before we consider it healthy. Useful
                            if tasks take a while to start up.
    :param check_haproxy: Whether to check the local haproxy to make sure this task has been registered and discovered.
    """
    happy = []
    now = datetime.datetime.now()

    if check_haproxy:
        service_namespace = '%s%s%s' % (
            service_name,
            marathon_tools.ID_SPACER,
            nerve_ns
        )

        tasks = get_registered_marathon_tasks(
            DEFAULT_SYNAPSE_HOST,
            service_namespace,
            tasks,
        )

    for task in tasks:
        if min_task_uptime is not None:
            if (now - task.started_at).total_seconds() < min_task_uptime:
                continue

        happy.append(task)

    return happy


@register_bounce_method('brutal')
def brutal_bounce(
    new_config,
    new_app_running,
    happy_new_tasks,
    old_app_tasks,
):
    """Pays no regard to safety. Starts the new app if necessary, and kills any
    old ones. Mostly meant as an example of the simplest working bounce method,
    but might be tolerable for some services.

    :param new_config: The configuration dictionary representing the desired new app.
    :param new_app_running: Whether there is an app in Marathon with the same ID as the new config.
    :param happy_new_tasks: Set of MarathonTasks belonging to the new application that are considered healthy and up.
    :param old_app_tasks: Dictionary of app_id -> set(Tasks) belonging to apps for old apps for this service.
    :return: A dictionary with keys create_app, tasks_to_kill, apps_to_kill, representing the desired bounce actions.
    """
    return {
        "create_app": not new_app_running,
        "tasks_to_kill": set.union(set(), *old_app_tasks.values()),  # set.union doesn't like getting zero arguments
        "apps_to_kill": set(old_app_tasks.keys()),
    }


@register_bounce_method('upthendown')
def upthendown_bounce(
    new_config,
    new_app_running,
    happy_new_tasks,
    old_app_tasks,
):
    """Starts a new app if necessary; only kills old apps once all the requested tasks for the new version are running.

    :param new_config: The configuration dictionary representing the desired new app.
    :param new_app_running: Whether there is an app in Marathon with the same ID as the new config.
    :param happy_new_tasks: Set of MarathonTasks belonging to the new application that are considered healthy and up.
    :param old_app_tasks: Dictionary of app_id -> set(Tasks) belonging to apps for old apps for this service.
    :return: A dictionary with keys create_app, tasks_to_kill, apps_to_kill, representing the desired bounce actions.
    """
    if new_app_running and len(happy_new_tasks) == new_config['instances']:
        return {
            "create_app": False,
            "tasks_to_kill": set.union(set(), *old_app_tasks.values()),  # set.union doesn't like getting zero arguments
            "apps_to_kill": set(old_app_tasks.keys()),
        }
    else:
        return {
            "create_app": not new_app_running,
            "tasks_to_kill": set(),
            "apps_to_kill": set(),
        }


@register_bounce_method('crossover')
def crossover_bounce(
    new_config,
    new_app_running,
    happy_new_tasks,
    old_app_tasks,
):
    """Starts a new app if necessary; slowly kills old apps as instances of the new app become happy.

    :param new_config: The configuration dictionary representing the desired new app.
    :param new_app_running: Whether there is an app in Marathon with the same ID as the new config.
    :param happy_new_tasks: Set of MarathonTasks belonging to the new application that are considered healthy and up.
    :param old_app_tasks: Dictionary of app_id -> set(Tasks) belonging to apps for old apps for this service.
    :return: A dictionary with keys create_app, tasks_to_kill, apps_to_kill, representing the desired bounce actions.

    """

    if not new_app_running:
        return {
            "create_app": True,
            "tasks_to_kill": set(),
            "apps_to_kill": set(),
        }
    else:
        happy_count = len(happy_new_tasks)
        needed_count = max(new_config['instances'] - happy_count, 0)

        old_tasks = []
        for app, tasks in old_app_tasks.items():
            for task in tasks:
                old_tasks.append(task)

        return {
            "create_app": False,
            "tasks_to_kill": set(set(old_tasks[needed_count:])),
            "apps_to_kill": set(app_id for (app_id, tasks) in old_app_tasks.items() if len(tasks) == 0),
        }


@register_bounce_method('downthenup')
def downthenup_bounce(
    new_config,
    new_app_running,
    happy_new_tasks,
    old_app_tasks,
):
    """Stops any old apps and waits for them to die before starting a new one.

    :param new_config: The configuration dictionary representing the desired new app.
    :param new_app_running: Whether there is an app in Marathon with the same ID as the new config.
    :param happy_new_tasks: Set of MarathonTasks belonging to the new application that are considered healthy and up.
    :param old_app_tasks: Dictionary of app_id -> set(Tasks) belonging to apps for old apps for this service.
    :return: A dictionary with keys create_app, tasks_to_kill, apps_to_kill, representing the desired bounce actions.
    """
    return {
        "create_app": not old_app_tasks and not new_app_running,
        "tasks_to_kill": set.union(set(), *old_app_tasks.values()),  # set.union doesn't like getting zero arguments,
        "apps_to_kill": set(old_app_tasks.keys()),
    }

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
