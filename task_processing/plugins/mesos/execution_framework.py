# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import threading
import time
from threading import Timer

import mesos.interface
import mesos.native
from mesos.interface import mesos_pb2

from paasta_tools.utils import paasta_print
from task_processing.plugins.mesos.translator import mesos_status_to_event

try:
    from Queue import Queue
except ImportError:
    from queue import Queue

MESOS_TASK_SPACER = '.'

# Bring these into local scope for shorter lines of code.
TASK_STAGING = mesos_pb2.TASK_STAGING
TASK_STARTING = mesos_pb2.TASK_STARTING
TASK_RUNNING = mesos_pb2.TASK_RUNNING

TASK_FINISHED = mesos_pb2.TASK_FINISHED
TASK_FAILED = mesos_pb2.TASK_FAILED
TASK_KILLED = mesos_pb2.TASK_KILLED
TASK_LOST = mesos_pb2.TASK_LOST
TASK_ERROR = mesos_pb2.TASK_ERROR

LIVE_TASK_STATES = (TASK_STAGING, TASK_STARTING, TASK_RUNNING)

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)
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


class ExecutionFramework(mesos.interface.Scheduler):
    def __init__(
        self,
        name,
        staging_timeout,
        reconcile_backoff=1,
        reconcile_start_time=float('inf'),
        dry_run=False,
        translator=mesos_status_to_event,
    ):
        self.translator = translator
        self.queue = Queue(1000)
        self.name = name
        self.tasks_with_flags = {}
        self.constraint_state = {}
        self.constraint_state_lock = threading.Lock()
        self.frozen = False
        self.framework_info = self.build_framework_info()
        self.tasks_waiting = Queue(1000)
        self.dry_run = dry_run

        # don't accept resources until we reconcile.
        self.reconcile_start_time = reconcile_start_time

        # wait this long after starting a reconcile before accepting offers.
        self.reconcile_backoff = 1

        # wait this long for a task to launch.
        self.staging_timeout = staging_timeout

        # Gets set when registered() is called
        self.framework_id = None

        self.blacklisted_slaves = set()
        self.blacklisted_slaves_lock = threading.Lock()
        self.blacklist_timeout = 10

    def shutdown(self, driver):
        # TODO: this is naive, as it does nothing to stop on-going calls
        #       to statusUpdate or resourceOffers.
        paasta_print("Freezing the scheduler. Further status updates and resource offers are ignored.")
        self.frozen = True
        paasta_print("Killing any remaining live tasks.")
        for task, parameters in self.tasks_with_flags.items():
            if parameters.mesos_task_state in LIVE_TASK_STATES:
                self.kill_task(driver, task)

    def need_to_stop(self):
        return False

    def launch_tasks_for_offers(self, driver, offers):
        """For each offer tries to launch all tasks that can fit in there.
        Declines offer if no fitting tasks found."""
        launched_tasks = []

        for offer in offers:
            with self.constraint_state_lock:
                try:
                    if self.dry_run:
                        tasks, new_state = self.tasks_and_state_for_offer(
                            driver, offer, self.constraint_state)
                        tasks, _ = self.tasks_and_state_for_offer(driver, offer, self.constraint_state)
                        print("Would have launched: ", tasks)
                        self.driver.stop()
                    else:
                        tasks, new_state = self.tasks_and_state_for_offer(
                            driver=driver,
                            offer=offer,
                            state=self.constraint_state
                        )
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
                                    )
                                )
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

        lost_state = mesos_pb2.TaskStatus()
        lost_state.task_id.value = task_id
        lost_state.state = TASK_LOST
        lost_state.data = 'startup timer expired'.encode('ascii', 'ignore')
        self.statusUpdate(driver, lost_state)

        self.blacklist_slave(self.tasks_with_flags[task_id].offer.slave_id.value)
        self.kill_task(driver, task_id)

    def staging_timer_for_task(self, timeout_value, driver, task_id):
        return Timer(timeout_value, lambda: self.log_and_kill(driver, task_id))

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

        # don't mutate existing state
        # new_constraint_state = copy.deepcopy(state)
        # total = 0
        # failed_constraints = 0
        while not self.tasks_waiting.empty():
            task_config = self.tasks_waiting.get()
            mesos_task = self.create_new_docker_task(offer, task_config)
            mesos_task.slave_id.value = offer.slave_id.value

            # total += 1

            if not(remainingCpus >= task_config.cpus and
                   remainingMem >= task_config.mem and
                   self.offer_matches_pool(offer) and
                   len(remainingPorts) >= 1):
                # re-queue the config we've just pulled
                self.tasks_waiting.put(task_config)
                break

            # if not(check_offer_constraints(offer, task.constraints,
                # new_constraint_state)):
                # failed_constraints += 1
                # break

            # task_port = random.choice(list(remainingPorts))

            # t.container.docker.port_mappings[0].host_port = task_port
            # for resource in t.resources:
            # if resource.name == "ports":
            # resource.ranges.range[0].begin = task_port
            # resource.ranges.range[0].end = task_port

            tasks.append(mesos_task)

            remainingCpus -= task_config.cpus
            remainingMem -= task_config.mem
            # remainingPorts -= {task_port}

            # update_constraint_state(offer, self.constraints, new_constraint_state)

        # raise constraint error but only if no other tasks fit/fail the offer
        # if total > 0 and failed_constraints == total:
        #     raise ConstraintFailAllTasksError

        return tasks, state

    def offer_matches_pool(self, offer):
        # for attribute in offer.attributes:
            # if attribute.name == "pool":
                # return attribute.text.value == self.service_config.get_pool()
        # # we didn't find a pool attribute on this slave, so assume it's not in our pool.
        # return False
        return True

    def within_reconcile_backoff(self):
        return time.time() - self.reconcile_backoff < self.reconcile_start_time

    def kill_task(self, driver, task):
        tid = mesos_pb2.TaskID()
        tid.value = task
        driver.killTask(tid)

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

    def enqueue(self, task_info):
        self.tasks_waiting.put(task_info)

    def wait_for(self, task_id):
        result = Queue()
        self.on_success(task_id, lambda: result.put(0))
        self.on_failure(task_id, lambda: result.put(1))
        return result.get()

    def on_success(self, task_id, callback):
        pass

    def on_failure(self, task_id, callback):
        pass

    def on_status(self, task_id, func):
        pass

    def build_framework_info(self):
        framework = mesos_pb2.FrameworkInfo()
        framework.user = ""  # Have Mesos fill in the current user.
        framework.name = self.name
        framework.checkpoint = True
        return framework

    def build_decline_offer_filter(self):
        f = mesos_pb2.Filters()
        f.refuse_seconds = self.offer_backoff
        return f

    def get_available_ports(self, resource):
        i = 0
        ports = []
        while True:
            try:
                ports = ports + range(
                    resource.ranges.range[i].begin,
                    resource.ranges.range[i].end
                )
                i += 1
            except Exception:
                break
        return ports

    def is_offer_valid(self, offer, task_config):
        offer_cpus = 0
        offer_mem = 0
        offer_disk = 0
        for resource in offer.resources:
            if resource.name == "cpus":
                offer_cpus += resource.scalar.value
            elif resource.name == "mem":
                offer_mem += resource.scalar.value
            elif resource.name == "disk":
                offer_disk += resource.scalar.value
            elif resource.name == "ports":
                # TODO: Validate if the ports available > ports require
                self.get_available_ports(resource)

        log.info(
            "Received offer {id} with cpus: {cpu} and mem: {mem}".format(
                id=offer.id.value,
                cpu=offer_cpus,
                mem=offer_mem
            )
        )

        if ((offer_cpus >= task_config.cpus and
             offer_mem >= task_config.mem and
             offer_disk >= task_config.disk)):
            return True

        return False

    def create_new_docker_task(self, offer, task_config):
        task = mesos_pb2.TaskInfo()

        container = mesos_pb2.ContainerInfo()
        container.type = 1  # mesos_pb2.ContainerInfo.Type.DOCKER

        command = mesos_pb2.CommandInfo()
        command.value = task_config.cmd

        task.command.MergeFrom(command)
        task.task_id.value = task_config.task_id
        task.slave_id.value = offer.slave_id.value
        task.name = task_config.name

        # CPUs
        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = task_config.cpus

        # mem
        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = task_config.mem

        # disk
        disk = task.resources.add()
        disk.name = "disk"
        disk.type = mesos_pb2.Value.SCALAR
        disk.scalar.value = task_config.disk

        # Volumes
        for mode in task_config.volumes:
            for container_path, host_path in task_config.volumes[mode]:
                volume = container.volumes.add()
                volume.container_path = container_path
                volume.host_path = host_path
                """
                volume.mode = 1 # mesos_pb2.Volume.Mode.RW
                volume.mode = 2 # mesos_pb2.Volume.Mode.RO
                """
                volume.mode = 1 if mode == "RW" else 2

        # Container info
        docker = mesos_pb2.ContainerInfo.DockerInfo()
        docker.image = task_config.image
        docker.network = 2  # mesos_pb2.ContainerInfo.DockerInfo.Network.BRIDGE
        docker.force_pull_image = True

        # TODO: who wants to implement support for ports?

        # available_ports = []
        # for resource in offer.resources:
        #     if resource.name == "ports":
        #         available_ports = self.get_available_ports(resource)

        # port_to_use = available_ports[0]

        # mesos_ports = task.resources.add()
        # mesos_ports.name = "ports"
        # mesos_ports.type = mesos_pb2.Value.RANGES
        # port_range = mesos_ports.ranges.range.add()

        # port_range.begin = port_to_use
        # port_range.end = port_to_use
        # docker_port = docker.port_mappings.add()
        # docker_port.host_port = port_to_use
        # docker_port.container_port = 8888

        # Set docker info in container.docker
        container.docker.MergeFrom(docker)
        # Set docker container in task.container
        task.container.MergeFrom(container)

        return task

    def stop(self):
        pass

    ####################################################################
    #                   Mesos driver hooks go here                     #
    ####################################################################

    def slaveLost(self, drive, slaveId):
        log.error("Slave lost: {id}".format(id=str(slaveId)))

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

    def statusUpdate(self, driver, update):
        if self.queue:
            self.queue.put(self.translator(update))
        if self.frozen:
            return

        # update tasks
        task_id = update.task_id.value
        state = update.state
        task_params = self.tasks_with_flags.setdefault(task_id, MesosTaskParameters(health=None))
        task_params.mesos_task_state = state

        for task, params in list(self.tasks_with_flags.items()):
            if params.marked_for_gc:
                self.tasks_with_flags.pop(task)

        if task_params.mesos_task_state is not TASK_STAGING:
            if self.tasks_with_flags[task_id].staging_timer:
                self.tasks_with_flags[task_id].staging_timer.cancel()
                self.tasks_with_flags[task_id].staging_timer = None

        # if task_params.mesos_task_state not in LIVE_TASK_STATES:
            # task_params.marked_for_gc = True
            # with self.constraint_state_lock:
                # update_constraint_state(task_params.offer, self.constraints,
                # self.constraint_state, step=-1)

        driver.acknowledgeStatusUpdate(update)


def mesos_task_for_task_config(task_config):
    task = mesos_pb2.TaskInfo()
    task.container.type = mesos_pb2.ContainerInfo.MESOS
    task.container.docker.image = task_config.image

    for param in task_config.docker_parameters:
        p = task.container.docker.parameters.add()
        p.key = param['key']
        p.value = param['value']

    # parameters.extend(task_config.ulimit)
    # parameters.extend(task_config.cap_add)

    for volume in task_config.volumes:
        v = task.container.volumes.add()
        v.mode = getattr(mesos_pb2.Volume, volume['mode'].upper())
        v.container_path = volume['containerPath']
        v.host_path = volume['hostPath']

    task.command.value = task_config.cmd
    cpus = task.resources.add()
    cpus.name = "cpus"
    cpus.type = mesos_pb2.Value.SCALAR
    cpus.scalar.value = task_config.cpus
    mem = task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = task_config.mem

    task.task_id.value = task_config.task_id
    task.name = task_config.name
    return task
