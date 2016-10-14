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
import datetime
import fcntl
import logging
import math
import os
import signal
import time
from contextlib import contextmanager
from contextlib import nested

import marathon_tools
import mesos_tools
from kazoo.client import KazooClient
from kazoo.exceptions import LockTimeout
from marathon.models import MarathonApp

from paasta_tools.smartstack_tools import get_registered_marathon_tasks
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import load_system_paasta_config


log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())
logging.getLogger("requests").setLevel(logging.WARNING)

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


def list_bounce_methods():
    return _bounce_method_funcs.keys()


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
    zk = KazooClient(hosts=load_system_paasta_config().get_zk_hosts(), timeout=ZK_LOCK_CONNECT_TIMEOUT_S)
    zk.start()
    lock = zk.Lock('%s/%s' % (ZK_LOCK_PATH, name))
    try:
        lock.acquire(timeout=1)  # timeout=0 throws some other strange exception
        yield
    except LockTimeout:
        raise LockHeldException("Service %s is already being bounced!" % name)
    else:
        lock.release()
    finally:
        zk.stop()


@contextmanager
def create_app_lock():
    """Acquire a lock in zookeeper for creating a marathon app. This is
    due to marathon's extreme lack of resilience with creating multiple
    apps at once, so we use this to not do that and only deploy
    one app at a time."""
    zk = KazooClient(hosts=load_system_paasta_config().get_zk_hosts(), timeout=ZK_LOCK_CONNECT_TIMEOUT_S)
    zk.start()
    lock = zk.Lock('%s/%s' % (ZK_LOCK_PATH, 'create_marathon_app_lock'))
    try:
        lock.acquire(timeout=30)  # timeout=0 throws some other strange exception
        yield
    except LockTimeout:
        raise LockHeldException("Failed to acquire lock for creating marathon app!")
    else:
        lock.release()
    finally:
        zk.stop()


@contextmanager
def time_limit(minutes):
    """A contextmanager to raise a TimeoutException whenever a specified
    number of minutes has passed.

    :param minutes: The number of minutes until an exception is raised"""
    def signal_handler(signum, frame):
        raise TimeoutException("Time limit expired")
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
        except Exception:
            continue


def get_happy_tasks(app, service, nerve_ns, system_paasta_config, min_task_uptime=None, check_haproxy=False):
    """Given a MarathonApp object, return the subset of tasks which are considered healthy.
    With the default options, this returns tasks where at least one of the defined Marathon healthchecks passes.
    For it to do anything interesting, set min_task_uptime or check_haproxy.

    :param app: A MarathonApp object.
    :param service: The name of the service.
    :param nerve_ns: The nerve namespace
    :param min_task_uptime: Minimum number of seconds that a task must be running before we consider it healthy. Useful
                            if tasks take a while to start up.
    :param check_haproxy: Whether to check the local haproxy to make sure this task has been registered and discovered.
    """
    tasks = app.tasks
    happy = []
    now = datetime.datetime.utcnow()

    if check_haproxy:
        tasks_in_smartstack = []
        service_namespace = compose_job_id(service, nerve_ns)

        service_namespace_config = marathon_tools.load_service_namespace_config(service, nerve_ns)
        discover_location_type = service_namespace_config.get_discover()
        unique_values = mesos_tools.get_mesos_slaves_grouped_by_attribute(
            slaves=mesos_tools.get_slaves(),
            attribute=discover_location_type
        )

        for value, hosts in unique_values.iteritems():
            synapse_hostname = hosts[0]['hostname']
            tasks_in_smartstack.extend(get_registered_marathon_tasks(
                synapse_hostname,
                system_paasta_config.get_synapse_port(),
                system_paasta_config.get_synapse_haproxy_url_format(),
                service_namespace,
                tasks,
            ))
        tasks = tasks_in_smartstack

    for task in tasks:
        if task.started_at is None:
            # Can't be healthy if it hasn't started
            continue

        if min_task_uptime is not None:
            if (now - task.started_at).total_seconds() < min_task_uptime:
                continue

        # if there are healthchecks defined for the app but none have executed yet, then task is unhappy
        if len(app.health_checks) > 0 and len(task.health_check_results) == 0:
            continue

        # if there are health check results, check if at least one healthcheck is passing
        if not marathon_tools.is_task_healthy(task, require_all=False, default_healthy=True):
            continue
        happy.append(task)

    return happy


def flatten_tasks(tasks_by_app_id):
    """Takes a dictionary of app_id -> set([task, task, ...]) and returns the union of all the task sets.

    :param tasks_by_app_id: A dictionary of app_id -> set(Tasks), such as the old_app_live_happy_tasks or
                            old_app_live_unhappy_tasks parameters passed to bounce methods.
    :return: A set of Tasks which is the union of all the values of the dictionary.
    """
    return set.union(set(), *(tasks_by_app_id.values()))


@register_bounce_method('brutal')
def brutal_bounce(
    new_config,
    new_app_running,
    happy_new_tasks,
    old_app_live_happy_tasks,
    old_app_live_unhappy_tasks,
    margin_factor=1.0,
):
    """Pays no regard to safety. Starts the new app if necessary, and kills any
    old ones. Mostly meant as an example of the simplest working bounce method,
    but might be tolerable for some services.

    :param new_config: The configuration dictionary representing the desired new app.
    :param new_app_running: Whether there is an app in Marathon with the same ID as the new config.
    :param happy_new_tasks: Set of MarathonTasks belonging to the new application that are considered healthy and up.
    :param old_app_live_tasks: Dictionary of app_id -> set(Tasks) belonging to apps for old apps for this service. Tasks
                               that are being drained are not included in this dictionary.
    :param margin_factor: the multiplication factor used to calculate the number of instances to be drained
                          when the crossover method is used.
    :return: A dictionary representing the desired bounce actions and containing the following keys:
              - create_app: True if we should start the new Marathon app, False otherwise.
              - tasks_to_drain: a set of task objects which should be drained and killed. May be empty.
    """
    return {
        "create_app": not new_app_running,
        # set.union doesn't like getting zero arguments
        "tasks_to_drain": flatten_tasks(old_app_live_happy_tasks) | flatten_tasks(old_app_live_unhappy_tasks),
    }


@register_bounce_method('upthendown')
def upthendown_bounce(
    new_config,
    new_app_running,
    happy_new_tasks,
    old_app_live_happy_tasks,
    old_app_live_unhappy_tasks,
    margin_factor=1.0,
):
    """Starts a new app if necessary; only kills old apps once all the requested tasks for the new version are running.

    See the docstring for brutal_bounce() for parameters and return value.
    """
    if new_app_running and len(happy_new_tasks) == new_config['instances']:
        return {
            "create_app": False,
            # set.union doesn't like getting zero arguments
            "tasks_to_drain": flatten_tasks(old_app_live_happy_tasks) | flatten_tasks(old_app_live_unhappy_tasks),
        }
    else:
        return {
            "create_app": not new_app_running,
            "tasks_to_drain": set(),
        }


@register_bounce_method('crossover')
def crossover_bounce(
    new_config,
    new_app_running,
    happy_new_tasks,
    old_app_live_happy_tasks,
    old_app_live_unhappy_tasks,
    margin_factor=1.0,
):
    """Starts a new app if necessary; slowly kills old apps as instances of the new app become happy.

    See the docstring for brutal_bounce() for parameters and return value.
    """

    assert margin_factor > 0
    assert margin_factor <= 1

    needed_count = max(int(math.ceil(new_config['instances'] * margin_factor)) -
                       len(happy_new_tasks), 0)

    old_tasks = []
    for app, tasks in old_app_live_happy_tasks.items():
        for task in tasks:
            old_tasks.append(task)

    for app, tasks in old_app_live_unhappy_tasks.items():
        for task in tasks:
            old_tasks.append(task)

    return {
        "create_app": not new_app_running,
        "tasks_to_drain": set(old_tasks[needed_count:]),
    }


@register_bounce_method('downthenup')
def downthenup_bounce(
    new_config,
    new_app_running,
    happy_new_tasks,
    old_app_live_happy_tasks,
    old_app_live_unhappy_tasks,
    margin_factor=1.0,
):
    """Stops any old apps and waits for them to die before starting a new one.

    See the docstring for brutal_bounce() for parameters and return value.
    """
    return {
        "create_app": not old_app_live_happy_tasks and not new_app_running,
        # set.union doesn't like getting zero arguments,
        "tasks_to_drain": flatten_tasks(old_app_live_happy_tasks) | flatten_tasks(old_app_live_unhappy_tasks),
    }


@register_bounce_method('down')
def down_bounce(
    new_config,
    new_app_running,
    happy_new_tasks,
    old_app_live_happy_tasks,
    old_app_live_unhappy_tasks,
    margin_factor=1.0,
):
    """
    Stops old apps, doesn't start any new apps.
    Used for the graceful_app_drain script.
    """
    return {
        "create_app": False,
        "tasks_to_drain": flatten_tasks(old_app_live_happy_tasks) | flatten_tasks(old_app_live_unhappy_tasks),
    }

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
