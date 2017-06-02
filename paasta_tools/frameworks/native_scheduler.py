#!/usr/bin/env python
# Without this, the import of mesos.interface breaks because paasta_tools.mesos exists
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import getpass
import logging
import random
import threading
import time
import uuid
from threading import Timer

import service_configuration_lib
from addict import Dict
from pymesos import MesosSchedulerDriver
from pymesos.interface import Scheduler

from paasta_tools import bounce_lib
from paasta_tools import drain_lib
from paasta_tools import mesos_tools
from paasta_tools.frameworks.constraints import check_offer_constraints
from paasta_tools.frameworks.constraints import update_constraint_state
from paasta_tools.frameworks.native_service_config import load_paasta_native_job_config
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import paasta_print

log = logging.getLogger(__name__)

MESOS_TASK_SPACER = '.'

# Bring these into local scope for shorter lines of code.
TASK_STAGING = 'TASK_STAGING'
TASK_STARTING = 'TASK_STARTING'
TASK_RUNNING = 'TASK_RUNNING'

TASK_KILLING = 'TASK_KILLING'
TASK_FINISHED = 'TASK_FINISHED'
TASK_FAILED = 'TASK_FAILED'
TASK_KILLED = 'TASK_KILLED'
TASK_LOST = 'TASK_LOST'
TASK_ERROR = 'TASK_ERROR'

LIVE_TASK_STATES = (TASK_STAGING, TASK_STARTING, TASK_RUNNING)


class ConstraintFailAllTasksError(Exception):
    pass


class MesosTaskParameters(object):
    def __init__(
        self,
        health=None,
        mesos_task_state=None,
        is_draining=False,
        is_healthy=False,
        staging_timer=None,
        offer=None
    ):
        self.health = health
        self.mesos_task_state = mesos_task_state

        self.is_draining = is_draining
        self.is_healthy = is_healthy
        self.offer = offer
        self.marked_for_gc = False
        self.staging_timer = staging_timer


class NativeScheduler(Scheduler):
    def __init__(self, service_name, instance_name, cluster,
                 system_paasta_config, staging_timeout,
                 soa_dir=DEFAULT_SOA_DIR, service_config=None, reconcile_backoff=30,
                 instance_type='paasta_native', service_config_overrides=None,
                 reconcile_start_time=float('inf')):
        self.service_name = service_name
        self.instance_name = instance_name
        self.instance_type = instance_type
        self.cluster = cluster
        self.system_paasta_config = system_paasta_config
        self.soa_dir = soa_dir
        self.tasks_with_flags = {}
        self.service_config_overrides = service_config_overrides or {}
        self.constraint_state = {}
        self.constraint_state_lock = threading.Lock()
        self.frozen = False

        # don't accept resources until we reconcile.
        self.reconcile_start_time = reconcile_start_time

        # wait this long after starting a reconcile before accepting offers.
        self.reconcile_backoff = reconcile_backoff

        # wait this long for a task to launch.
        self.staging_timeout = staging_timeout

        # Gets set when registered() is called
        self.framework_id = None

        self.blacklisted_slaves = set()
        self.blacklisted_slaves_lock = threading.Lock()
        self.blacklist_timeout = 3600

        if service_config is not None:
            self.service_config = service_config
            self.service_config.config_dict.update(self.service_config_overrides)
            self.recreate_drain_method()
            self.reload_constraints()
            self.validate_config()
        else:
            self.load_config()

    def shutdown(self, driver):
        # TODO: this is naive, as it does nothing to stop on-going calls
        #       to statusUpdate or resourceOffers.
        paasta_print("Freezing the scheduler. Further status updates and resource offers are ignored.")
        self.frozen = True
        paasta_print("Killing any remaining live tasks.")
        for task, parameters in self.tasks_with_flags.items():
            if parameters.mesos_task_state in LIVE_TASK_STATES:
                self.kill_task(driver, task)

    def registered(self, driver, frameworkId, masterInfo):
        self.framework_id = frameworkId.value
        paasta_print("Registered with framework ID %s" % frameworkId.value)

        self.reconcile_start_time = time.time()
        driver.reconcileTasks([])

    def reregistered(self, driver, masterInfo):
        self.registered(driver, Dict(value=driver.framework_id), masterInfo)

    def resourceOffers(self, driver, offers):
        if self.frozen:
            return

        if self.within_reconcile_backoff():
            paasta_print("Declining all offers since we started reconciliation too recently")
            for offer in offers:
                driver.declineOffer(offer.id)
        else:
            for idx, offer in enumerate(offers):
                if offer.agent_id.value in self.blacklisted_slaves:
                    log.critical("Ignoring offer %s from blacklisted slave %s" %
                                 (offer.id.value, offer.agent_id.value))
                    filters = Dict(refuse_seconds=self.blacklist_timeout)
                    driver.declineOffer(offer.id, filters)
                    del offers[idx]

            self.launch_tasks_for_offers(driver, offers)

    def launch_tasks_for_offers(self, driver, offers):
        """For each offer tries to launch all tasks that can fit in there.
        Declines offer if no fitting tasks found."""
        launched_tasks = []

        for offer in offers:
            with self.constraint_state_lock:
                try:
                    tasks, new_state = self.tasks_and_state_for_offer(
                        driver, offer, self.constraint_state)

                    if tasks is not None and len(tasks) > 0:
                        driver.launchTasks([offer.id], tasks)

                        for task in tasks:
                            staging_timer = self.staging_timer_for_task(
                                self.staging_timeout,
                                driver,
                                task.task_id.value
                            )
                            self.tasks_with_flags.setdefault(
                                task.task_id.value,
                                MesosTaskParameters(
                                    health=None,
                                    mesos_task_state=TASK_STAGING,
                                    offer=offer,
                                    staging_timer=staging_timer,
                                ))
                            staging_timer.start()
                        launched_tasks.extend(tasks)
                        self.constraint_state = new_state
                    else:
                        driver.declineOffer(offer.id)
                except ConstraintFailAllTasksError:
                    paasta_print("Offer failed constraints for every task, rejecting 60s")
                    filters = Dict(refuse_seconds=60)
                    driver.declineOffer(offer.id, filters)
        return launched_tasks

    def task_fits(self, offer):
        """Checks whether the offer is big enough to fit the tasks"""
        needed_resources = {
            "cpus": self.service_config.get_cpus(),
            "mem": self.service_config.get_mem(),
            "disk": self.service_config.get_disk(),
        }
        for resource in offer.resources:
            try:
                if resource.scalar.value < needed_resources[resource.name]:
                    return False
            except KeyError:
                pass

        return True

    def need_more_tasks(self, name, existingTasks, scheduledTasks):
        """Returns whether we need to start more tasks."""
        num_have = 0
        for task, parameters in existingTasks.items():
            if self.is_task_new(name, task) and (parameters.mesos_task_state in LIVE_TASK_STATES):
                num_have += 1

        for task in scheduledTasks:
            if task.name == name:
                num_have += 1

        return num_have < self.service_config.get_desired_instances()

    def get_new_tasks(self, name, tasks):
        return set(filter(
            lambda tid:
                self.is_task_new(name, tid) and
                self.tasks_with_flags[tid].mesos_task_state in LIVE_TASK_STATES,
            tasks))

    def get_old_tasks(self, name, tasks):
        return set(filter(
            lambda tid:
                not(self.is_task_new(name, tid)) and
                self.tasks_with_flags[tid].mesos_task_state in LIVE_TASK_STATES,
            tasks))

    def is_task_new(self, name, tid):
        return tid.startswith("%s." % name)

    def log_and_kill(self, driver, task_id):
        log.critical('Task stuck launching for %ss, assuming to have failed. Killing task.' % self.staging_timeout)
        self.blacklist_slave(self.tasks_with_flags[task_id].offer.agent_id.value)
        self.kill_task(driver, task_id)

    def staging_timer_for_task(self, timeout_value, driver, task_id):
        timer = Timer(timeout_value, lambda: self.log_and_kill(driver, task_id))
        timer.daemon = True
        return timer

    def tasks_and_state_for_offer(self, driver, offer, state):
        """Returns collection of tasks that can fit inside an offer."""
        tasks = []
        offerCpus = 0
        offerMem = 0
        offerPorts = []
        for resource in offer.resources:
            if resource.name == "cpus":
                offerCpus += resource.scalar.value
            elif resource.name == "mem":
                offerMem += resource.scalar.value
            elif resource.name == "ports":
                for rg in resource.ranges.range:
                    # I believe mesos protobuf ranges are inclusive, but range() is exclusive
                    offerPorts += range(rg.begin, rg.end + 1)
        remainingCpus = offerCpus
        remainingMem = offerMem
        remainingPorts = set(offerPorts)

        base_task = self.service_config.base_task(self.system_paasta_config)
        base_task.agent_id.value = offer.agent_id.value

        task_mem = self.service_config.get_mem()
        task_cpus = self.service_config.get_cpus()

        # don't mutate existing state
        new_constraint_state = copy.deepcopy(state)
        total = 0
        failed_constraints = 0
        while self.need_more_tasks(base_task.name, self.tasks_with_flags, tasks):
            total += 1

            if not(remainingCpus >= task_cpus and
                   remainingMem >= task_mem and
                   self.offer_matches_pool(offer) and
                   len(remainingPorts) >= 1):
                break

            if not(check_offer_constraints(offer, self.constraints,
                                           new_constraint_state)):
                failed_constraints += 1
                break

            task_port = random.choice(list(remainingPorts))

            task = copy.deepcopy(base_task)
            task.task_id = Dict(
                value='{}.{}'.format(task.name, uuid.uuid4().hex)
            )

            task.container.docker.port_mappings[0].host_port = task_port
            for resource in task.resources:
                if resource.name == 'ports':
                    resource.ranges.range[0].begin = task_port
                    resource.ranges.range[0].end = task_port

            tasks.append(task)

            remainingCpus -= task_cpus
            remainingMem -= task_mem
            remainingPorts -= {task_port}

            update_constraint_state(offer, self.constraints, new_constraint_state)

        # raise constraint error but only if no other tasks fit/fail the offer
        if total > 0 and failed_constraints == total:
            raise ConstraintFailAllTasksError

        return tasks, new_constraint_state

    def offer_matches_pool(self, offer):
        for attribute in offer.attributes:
            if attribute.name == "pool":
                return attribute.text.value == self.service_config.get_pool()
        # we didn't find a pool attribute on this slave, so assume it's not in our pool.
        return False

    def within_reconcile_backoff(self):
        return time.time() - self.reconcile_backoff < self.reconcile_start_time

    def periodic(self, driver):
        if self.frozen:
            return

        self.periodic_was_called = True  # Used for testing.
        if not self.within_reconcile_backoff():
            driver.reviveOffers()

        self.load_config()
        self.kill_tasks_if_necessary(driver)

    def statusUpdate(self, driver, update):
        if self.frozen:
            return

        # update tasks
        task_id = update.task_id.value
        paasta_print('Task {} is in state {}'.format(
            task_id, update.state
        ))

        task_params = self.tasks_with_flags.setdefault(task_id, MesosTaskParameters(health=None))
        task_params.mesos_task_state = update.state

        for task, params in list(self.tasks_with_flags.items()):
            if params.marked_for_gc:
                self.tasks_with_flags.pop(task)

        if task_params.mesos_task_state is not TASK_STAGING:
            if self.tasks_with_flags[task_id].staging_timer:
                self.tasks_with_flags[task_id].staging_timer.cancel()
                self.tasks_with_flags[task_id].staging_timer = None

        if task_params.mesos_task_state not in LIVE_TASK_STATES:
            task_params.marked_for_gc = True
            with self.constraint_state_lock:
                update_constraint_state(task_params.offer, self.constraints,
                                        self.constraint_state, step=-1)

        driver.acknowledgeStatusUpdate(update)
        self.kill_tasks_if_necessary(driver)

    def make_healthiness_sorter(self, base_task_name):
        def healthiness_score(task_id):
            """Return a tuple that can be used as a key for sorting, that expresses our desire to keep this task around.
            Higher values (things that sort later) are more desirable."""
            params = self.tasks_with_flags[task_id]

            state_score = {
                TASK_KILLING: 0,
                TASK_FINISHED: 0,
                TASK_FAILED: 0,
                TASK_KILLED: 0,
                TASK_LOST: 0,
                TASK_ERROR: 0,
                TASK_STAGING: 1,
                TASK_STARTING: 2,
                TASK_RUNNING: 3,
            }[params.mesos_task_state]

            # unhealthy tasks < healthy
            # staging < starting < running
            # old < new
            return (params.is_healthy, state_score, self.is_task_new(base_task_name, task_id))
        return healthiness_score

    def kill_tasks_if_necessary(self, driver):
        base_task = self.service_config.base_task(self.system_paasta_config)

        new_tasks = self.get_new_tasks(base_task.name, self.tasks_with_flags.keys())
        happy_new_tasks = self.get_happy_tasks(new_tasks)

        desired_instances = self.service_config.get_desired_instances()
        # this puts the most-desired tasks first. I would have left them in order of bad->good and used
        # new_tasks_by_desirability[:-desired_instances] instead, but list[:-0] is an empty list, rather than the full
        # list.
        new_tasks_by_desirability = sorted(
            list(new_tasks),
            key=self.make_healthiness_sorter(base_task.name),
            reverse=True)
        new_tasks_to_kill = new_tasks_by_desirability[desired_instances:]

        old_tasks = self.get_old_tasks(base_task.name, self.tasks_with_flags.keys())
        old_happy_tasks = self.get_happy_tasks(old_tasks)
        old_draining_tasks = self.get_draining_tasks(old_tasks)
        old_unhappy_tasks = set(old_tasks) - set(old_happy_tasks) - set(old_draining_tasks)

        actions = bounce_lib.crossover_bounce(
            new_config={"instances": desired_instances},
            new_app_running=True,
            happy_new_tasks=happy_new_tasks,
            old_app_live_happy_tasks=self.group_tasks_by_version(old_happy_tasks + new_tasks_to_kill),
            old_app_live_unhappy_tasks=self.group_tasks_by_version(old_unhappy_tasks),
        )

        for task in set(new_tasks) - set(actions['tasks_to_drain']):
            self.undrain_task(task)
        for task in actions['tasks_to_drain']:
            self.drain_task(task)

        for task, parameters in self.tasks_with_flags.items():
            if parameters.is_draining and \
                    self.drain_method.is_safe_to_kill(DrainTask(id=task)) and \
                    parameters.mesos_task_state in LIVE_TASK_STATES:
                self.kill_task(driver, task)

    def get_happy_tasks(self, tasks):
        """Filter a list of tasks to those that are happy."""
        happy_tasks = []
        for task in tasks:
            params = self.tasks_with_flags[task]
            if params.mesos_task_state == TASK_RUNNING and not params.is_draining:
                happy_tasks.append(task)
        return happy_tasks

    def get_draining_tasks(self, tasks):
        """Filter a list of tasks to those that are draining."""
        return [t for t, p in self.tasks_with_flags.items() if p.is_draining]

    def undrain_task(self, task):
        self.drain_method.stop_draining(DrainTask(id=task))
        self.tasks_with_flags[task].is_draining = False

    def drain_task(self, task):
        self.drain_method.drain(DrainTask(id=task))
        self.tasks_with_flags[task].is_draining = True

    def kill_task(self, driver, task):
        driver.killTask(Dict(value=task))
        self.tasks_with_flags[task].mesos_task_state = TASK_KILLING

    def group_tasks_by_version(self, task_ids):
        d = {}
        for task_id in task_ids:
            version = task_id.rsplit('.', 1)[0]
            d.setdefault(version, []).append(task_id)
        return d

    def load_config(self):
        service_configuration_lib._yaml_cache = {}
        self.service_config = load_paasta_native_job_config(
            service=self.service_name,
            instance=self.instance_name,
            instance_type=self.instance_type,
            cluster=self.cluster,
            soa_dir=self.soa_dir,
            config_overrides=self.service_config_overrides
        )
        self.recreate_drain_method()
        self.reload_constraints()
        self.validate_config()

    def validate_config(self):
        pass

    def recreate_drain_method(self):
        """Re-instantiate self.drain_method. Should be called after self.service_config changes."""
        self.drain_method = drain_lib.get_drain_method(
            name=self.service_config.get_drain_method(self.service_config.service_namespace_config),
            service=self.service_name,
            instance=self.instance_name,
            nerve_ns=self.service_config.get_nerve_namespace(),
            **self.service_config.get_drain_method_params(self.service_config.service_namespace_config)
        )

    def reload_constraints(self):
        self.constraints = self.service_config.get_constraints() or []

    def blacklist_slave(self, agent_id):
        if agent_id in self.blacklisted_slaves:
            return

        log.debug("Blacklisting slave: %s" % agent_id)
        with self.blacklisted_slaves_lock:
            self.blacklisted_slaves.add(agent_id)
            t = Timer(self.blacklist_timeout, lambda: self.unblacklist_slave(agent_id))
            t.daemon = True
            t.start()

    def unblacklist_slave(self, agent_id):
        if agent_id not in self.blacklisted_slaves:
            return

        log.debug("Unblacklisting slave: %s" % agent_id)
        with self.blacklisted_slaves_lock:
            self.blacklisted_slaves.discard(agent_id)


class DrainTask(object):
    def __init__(self, id):
        self.id = id


def find_existing_id_if_exists_or_gen_new(name):
    for framework in mesos_tools.get_all_frameworks(active_only=True):
        if framework.name == name:
            return framework.id
    else:
        return uuid.uuid4().hex


def create_driver(
    framework_name,
    scheduler,
    system_paasta_config,
    implicit_acks=False
):
    master_uri = '{}:{}'.format(
        mesos_tools.get_mesos_leader(), mesos_tools.MESOS_MASTER_PORT
    )

    framework = Dict(
        user=getpass.getuser(),
        name=framework_name,
        failover_timeout=604800,
        id=Dict(value=find_existing_id_if_exists_or_gen_new(framework_name)),
        checkpoint=True,
        principal=system_paasta_config.get_paasta_native_config()['principal']
    )

    driver = MesosSchedulerDriver(
        sched=scheduler,
        framework=framework,
        master_uri=master_uri,
        use_addict=True,
        implicit_acknowledgements=implicit_acks,
        principal=system_paasta_config.get_paasta_native_config()['principal'],
        secret=system_paasta_config.get_paasta_native_config()['secret'],
    )
    return driver


def get_paasta_native_jobs_for_cluster(cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """A paasta_native-specific wrapper around utils.get_services_for_cluster

    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (service, job_name)"""
    return get_services_for_cluster(cluster, 'paasta_native', soa_dir)
