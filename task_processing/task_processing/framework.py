from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import logging
import random
import threading
import time
import uuid
from threading import Timer

import mesos.interface
import service_configuration_lib
from mesos.interface import mesos_pb2

from paasta_tools.frameworks.constraints import check_offer_constraints
from paasta_tools.frameworks.constraints import update_constraint_state
from paasta_tools.frameworks.native_service_config import load_paasta_native_job_config
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import paasta_print

log = logging.getLogger(__name__)

MESOS_TASK_SPACER = '.'

# Bring these into local scope for shorter lines of code.
TASK_STAGING = mesos_pb2.TASK_STAGING
TASK_STARTING = mesos_pb2.TASK_STARTING
TASK_RUNNING = mesos_pb2.TASK_RUNNING

TASK_KILLING = mesos_pb2.TASK_KILLING
TASK_FINISHED = mesos_pb2.TASK_FINISHED
TASK_FAILED = mesos_pb2.TASK_FAILED
TASK_KILLED = mesos_pb2.TASK_KILLED
TASK_LOST = mesos_pb2.TASK_LOST
TASK_ERROR = mesos_pb2.TASK_ERROR

LIVE_TASK_STATES = (TASK_STAGING, TASK_STARTING, TASK_RUNNING)


log = logging.getLogger(__name__)


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


class NativeScheduler(mesos.interface.Scheduler):
    def __init__(self, service_name, instance_name, cluster,
                 system_paasta_config, staging_timeout, soa_dir=DEFAULT_SOA_DIR,
                 service_config=None, reconcile_backoff=30,
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

        if service_config is not None:
            self.service_config = service_config
            self.service_config.config_dict.update(self.service_config_overrides)
        else:
            self.load_config()

    def need_to_stop(self):
        # Is used to decide whether to stop the driver or try to start more tasks.
        for task, params in self.tasks_with_flags.items():
            if params.mesos_task_state not in LIVE_TASK_STATES:
                return True
        return False

    def statusUpdate(self, driver, update):
        if self.frozen:
            return

        # update tasks
        task_id = update.task_id.value
        state = update.state
        paasta_print("Task %s is in state %s" %
                     (task_id, mesos_pb2.TaskState.Name(state)))

        task_params = self.tasks_with_flags.setdefault(task_id, MesosTaskParameters(health=None))
        task_params.mesos_task_state = state

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
        # Stop if task ran and finished
        if self.need_to_stop():
            driver.stop()

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

    def resourceOffers(self, driver, offers):
        if self.frozen:
            return

        if self.within_reconcile_backoff():
            paasta_print("Declining all offers since we started reconciliation too recently")
            for offer in offers:
                driver.declineOffer(offer.id)
        else:
            for idx, offer in enumerate(offers):
                if offer.slave_id.value in self.blacklisted_slaves:
                    log.critical("Ignoring offer %s from blacklisted slave %s" %
                                 (offer.id.value, offer.slave_id.value))
                    driver.declineOffer(offer.id)
                    del offers[idx]

            if len(offers) == 0:
                return

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
                        operation = mesos_pb2.Offer.Operation()
                        operation.type = mesos_pb2.Offer.Operation.LAUNCH
                        operation.launch.task_infos.extend(tasks)
                        driver.acceptOffers([offer.id], [operation])

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
                    filters = mesos_pb2.Filters()
                    filters.refuse_seconds = 60
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
        self.blacklist_slave(self.tasks_with_flags[task_id].offer.slave_id.value)
        self.kill_task(driver, task_id)

    def staging_timer_for_task(self, timeout_value, driver, taskId):
        return Timer(timeout_value, lambda: self.log_and_kill(driver, taskId))

    def tasks_and_state_for_offer(self, driver, offer, state):
        """Returns collection of tasks that can fit inside an offer."""
        if self.dry_run or self.need_to_stop():
            if self.dry_run:
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
                base_task.slave_id.value = offer.slave_id.value

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

                    t = mesos_pb2.TaskInfo()
                    t.MergeFrom(base_task)
                    tid = "%s.%s" % (t.name, uuid.uuid4().hex)
                    t.task_id.value = tid

                    t.container.docker.port_mappings[0].host_port = task_port
                    for resource in t.resources:
                        if resource.name == "ports":
                            resource.ranges.range[0].begin = task_port
                            resource.ranges.range[0].end = task_port

                    tasks.append(t)

                    remainingCpus -= task_cpus
                    remainingMem -= task_mem
                    remainingPorts -= {task_port}

                    update_constraint_state(offer, self.constraints, new_constraint_state)

                # raise constraint error but only if no other tasks fit/fail the offer
                if total > 0 and failed_constraints == total:
                    raise ConstraintFailAllTasksError

                paasta_print("Would have launched: ", tasks)
            driver.stop()
            return [], state
        return tasks, state

    def offer_matches_pool(self, offer):
        for attribute in offer.attributes:
            if attribute.name == "pool":
                return attribute.text.value == self.service_config.get_pool()
        # we didn't find a pool attribute on this slave, so assume it's not in our pool.
        return False

    def within_reconcile_backoff(self):
        return time.time() - self.reconcile_backoff < self.reconcile_start_time

    def kill_task(self, driver, task):
        tid = mesos_pb2.TaskID()
        tid.value = task
        driver.killTask(tid)
        self.tasks_with_flags[task].mesos_task_state = TASK_KILLING

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

    def blacklist_slave(self, slave_id):
        if slave_id in self.blacklisted_slaves:
            return

        log.debug("Blacklisting slave: %s" % slave_id)
        with self.blacklisted_slaves_lock:
            self.blacklisted_slaves.add(slave_id)
            Timer(self.blacklist_timeout, lambda: self.unblacklist_slave(slave_id)).start()

    def unblacklist_slave(self, slave_id):
        if slave_id not in self.blacklisted_slaves:
            return

        log.debug("Unblacklisting slave: %s" % slave_id)
        with self.blacklisted_slaves_lock:
            self.blacklisted_slaves.discard(slave_id)
