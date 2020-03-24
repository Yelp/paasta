#!/usr/bin/env python
import asyncio
import copy
import getpass
import logging
import random
import threading
import time
import uuid
from typing import Collection
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple

import a_sync
import service_configuration_lib
from pymesos import MesosSchedulerDriver
from pymesos.interface import Scheduler

from paasta_tools import bounce_lib
from paasta_tools import drain_lib
from paasta_tools import mesos_tools
from paasta_tools.frameworks.constraints import check_offer_constraints
from paasta_tools.frameworks.constraints import ConstraintState
from paasta_tools.frameworks.constraints import update_constraint_state
from paasta_tools.frameworks.native_service_config import load_paasta_native_job_config
from paasta_tools.frameworks.native_service_config import NativeServiceConfig
from paasta_tools.frameworks.native_service_config import NativeServiceConfigDict
from paasta_tools.frameworks.native_service_config import TaskInfo
from paasta_tools.frameworks.task_store import MesosTaskParameters
from paasta_tools.frameworks.task_store import TaskStore
from paasta_tools.frameworks.task_store import ZKTaskStore
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_LOGLEVEL
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import SystemPaastaConfig

log = logging.getLogger(__name__)

MESOS_TASK_SPACER = "."

# Bring these into local scope for shorter lines of code.
TASK_STAGING = "TASK_STAGING"
TASK_STARTING = "TASK_STARTING"
TASK_RUNNING = "TASK_RUNNING"

TASK_KILLING = "TASK_KILLING"
TASK_FINISHED = "TASK_FINISHED"
TASK_FAILED = "TASK_FAILED"
TASK_KILLED = "TASK_KILLED"
TASK_LOST = "TASK_LOST"
TASK_ERROR = "TASK_ERROR"

LIVE_TASK_STATES = (TASK_STAGING, TASK_STARTING, TASK_RUNNING)


class ConstraintFailAllTasksError(Exception):
    pass


class NativeScheduler(Scheduler):
    task_store: TaskStore

    def __init__(
        self,
        service_name: str,
        instance_name: str,
        cluster: str,
        system_paasta_config: SystemPaastaConfig,
        staging_timeout: float,
        soa_dir: str = DEFAULT_SOA_DIR,
        service_config: Optional[NativeServiceConfig] = None,
        reconcile_backoff: float = 30,
        instance_type: str = "paasta_native",
        service_config_overrides: Optional[NativeServiceConfigDict] = None,
        reconcile_start_time: float = float("inf"),
        task_store_type=ZKTaskStore,
    ) -> None:
        self.service_name = service_name
        self.instance_name = instance_name
        self.instance_type = instance_type
        self.cluster = cluster
        self.system_paasta_config = system_paasta_config
        self.soa_dir = soa_dir

        # This will be initialized in registered().
        self.task_store = None
        self.task_store_type = task_store_type

        self.service_config_overrides = service_config_overrides or {}
        self.constraint_state: ConstraintState = {}
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

        # agent_id -> unix timestamp of when we blacklisted it
        self.blacklisted_slaves: Dict[str, float] = {}
        self.blacklist_timeout = 3600

        if service_config is not None:
            self.service_config = service_config
            self.service_config.config_dict.update(  # type: ignore
                self.service_config_overrides
            )
            self.recreate_drain_method()
            self.reload_constraints()
            self.validate_config()
        else:
            self.load_config()

    def log(self, line, level=DEFAULT_LOGLEVEL):
        _log(
            service=self.service_name,
            instance=self.instance_name,
            component="deploy",
            line=line,
            level=level,
        )

    def shutdown(self, driver: MesosSchedulerDriver):
        # TODO: this is naive, as it does nothing to stop on-going calls
        #       to statusUpdate or resourceOffers.
        self.log(
            "Freezing the scheduler. Further status updates and resource offers are ignored."
        )
        self.frozen = True
        self.log("Killing any remaining live tasks.")
        for task, parameters in self.task_store.get_all_tasks().items():
            if parameters.mesos_task_state in LIVE_TASK_STATES:
                self.kill_task(driver, task)
        self.task_store.close()

    def registered(self, driver: MesosSchedulerDriver, frameworkId, masterInfo):
        self.framework_id = frameworkId["value"]
        self.log("Registered with framework ID %s" % frameworkId["value"])

        self.task_store = self.task_store_type(
            service_name=self.service_name,
            instance_name=self.instance_name,
            framework_id=self.framework_id,
            system_paasta_config=self.system_paasta_config,
        )

        self.reconcile_start_time = time.time()
        driver.reconcileTasks([])

    def reregistered(self, driver: MesosSchedulerDriver, masterInfo):
        self.registered(driver, {"value": driver.framework_id}, masterInfo)

    def resourceOffers(self, driver: MesosSchedulerDriver, offers):
        if self.frozen:
            return

        if self.within_reconcile_backoff():
            self.log(
                "Declining all offers since we started reconciliation too recently"
            )
            for offer in offers:
                driver.declineOffer(offer.id)
        else:
            for idx, offer in enumerate(offers):
                if offer.agent_id.value in self.blacklisted_slaves:
                    log.critical(
                        "Ignoring offer %s from blacklisted slave %s"
                        % (offer.id.value, offer.agent_id.value)
                    )
                    filters = {"refuse_seconds": self.blacklist_timeout}
                    driver.declineOffer(offer.id, filters)
                    del offers[idx]

            self.launch_tasks_for_offers(driver, offers)

    def launch_tasks_for_offers(
        self, driver: MesosSchedulerDriver, offers
    ) -> List[TaskInfo]:
        """For each offer tries to launch all tasks that can fit in there.
        Declines offer if no fitting tasks found."""
        launched_tasks: List[TaskInfo] = []

        for offer in offers:
            with self.constraint_state_lock:
                try:
                    tasks, new_state = self.tasks_and_state_for_offer(
                        driver, offer, self.constraint_state
                    )

                    if tasks is not None and len(tasks) > 0:
                        driver.launchTasks([offer.id], tasks)

                        for task in tasks:
                            self.task_store.add_task_if_doesnt_exist(
                                task["task_id"]["value"],
                                health=None,
                                mesos_task_state=TASK_STAGING,
                                offer=offer,
                                resources=task["resources"],
                            )
                        launched_tasks.extend(tasks)
                        self.constraint_state = new_state
                    else:
                        driver.declineOffer(offer.id)
                except ConstraintFailAllTasksError:
                    self.log("Offer failed constraints for every task, rejecting 60s")
                    filters = {"refuse_seconds": 60}
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
            if self.is_task_new(name, task) and (
                parameters.mesos_task_state in LIVE_TASK_STATES
            ):
                num_have += 1

        for task in scheduledTasks:
            if task["name"] == name:
                num_have += 1

        return num_have < self.service_config.get_desired_instances()

    def get_new_tasks(self, name, tasks_with_params: Dict[str, MesosTaskParameters]):
        return {
            tid: params
            for tid, params in tasks_with_params.items()
            if (
                self.is_task_new(name, tid)
                and (params.mesos_task_state in LIVE_TASK_STATES)
            )
        }

    def get_old_tasks(self, name, tasks_with_params: Dict[str, MesosTaskParameters]):
        return {
            tid: params
            for tid, params in tasks_with_params.items()
            if (
                (not self.is_task_new(name, tid))
                and (params.mesos_task_state in LIVE_TASK_STATES)
            )
        }

    def is_task_new(self, name, tid):
        return tid.startswith("%s." % name)

    def log_and_kill(self, driver: MesosSchedulerDriver, task_id):
        log.critical(
            "Task stuck launching for %ss, assuming to have failed. Killing task."
            % self.staging_timeout
        )
        self.blacklist_slave(self.task_store.get_task(task_id).offer.agent_id.value)
        self.kill_task(driver, task_id)

    def tasks_and_state_for_offer(
        self, driver: MesosSchedulerDriver, offer, state: ConstraintState
    ) -> Tuple[List[TaskInfo], ConstraintState]:
        """Returns collection of tasks that can fit inside an offer."""
        tasks: List[TaskInfo] = []
        offerCpus = 0.0
        offerMem = 0.0
        offerPorts: List[int] = []
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
        base_task["agent_id"]["value"] = offer["agent_id"]["value"]

        task_mem = self.service_config.get_mem()
        task_cpus = self.service_config.get_cpus()

        # don't mutate existing state
        new_constraint_state = copy.deepcopy(state)
        total = 0
        failed_constraints = 0
        while self.need_more_tasks(
            base_task["name"], self.task_store.get_all_tasks(), tasks
        ):
            total += 1

            if not (
                remainingCpus >= task_cpus
                and remainingMem >= task_mem
                and self.offer_matches_pool(offer)
                and len(remainingPorts) >= 1
            ):
                break

            if not (
                check_offer_constraints(offer, self.constraints, new_constraint_state)
            ):
                failed_constraints += 1
                break

            task_port = random.choice(list(remainingPorts))

            task = copy.deepcopy(base_task)
            task["task_id"] = {"value": "{}.{}".format(task["name"], uuid.uuid4().hex)}

            task["container"]["docker"]["port_mappings"][0]["host_port"] = task_port
            for resource in task["resources"]:
                if resource["name"] == "ports":
                    resource["ranges"]["range"][0]["begin"] = task_port
                    resource["ranges"]["range"][0]["end"] = task_port

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

    def periodic(self, driver: MesosSchedulerDriver):
        if self.frozen:
            return

        self.periodic_was_called = True  # Used for testing.
        if not self.within_reconcile_backoff():
            driver.reviveOffers()

        self.load_config()
        self.kill_tasks_if_necessary(driver)
        self.check_blacklisted_slaves_for_timeout()

    def statusUpdate(self, driver: MesosSchedulerDriver, update: Dict):
        if self.frozen:
            return

        # update tasks
        task_id = update["task_id"]["value"]
        self.log("Task {} is in state {}".format(task_id, update["state"]))

        task_params = self.task_store.update_task(
            task_id, mesos_task_state=update["state"]
        )

        if task_params.mesos_task_state not in LIVE_TASK_STATES:
            with self.constraint_state_lock:
                update_constraint_state(
                    task_params.offer, self.constraints, self.constraint_state, step=-1
                )

        driver.acknowledgeStatusUpdate(update)
        self.kill_tasks_if_necessary(driver)

    def make_healthiness_sorter(
        self, base_task_name: str, all_tasks_with_params: Dict[str, MesosTaskParameters]
    ):
        def healthiness_score(task_id):
            """Return a tuple that can be used as a key for sorting, that expresses our desire to keep this task around.
            Higher values (things that sort later) are more desirable."""
            params = all_tasks_with_params[task_id]

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
            return (
                params.is_healthy,
                state_score,
                self.is_task_new(base_task_name, task_id),
            )

        return healthiness_score

    def kill_tasks_if_necessary(self, driver: MesosSchedulerDriver):
        base_task = self.service_config.base_task(self.system_paasta_config)

        all_tasks_with_params = self.task_store.get_all_tasks()

        new_tasks_with_params = self.get_new_tasks(
            base_task["name"], all_tasks_with_params
        )
        happy_new_tasks_with_params = self.get_happy_tasks(new_tasks_with_params)

        desired_instances = self.service_config.get_desired_instances()
        # this puts the most-desired tasks first. I would have left them in order of bad->good and used
        # new_tasks_by_desirability[:-desired_instances] instead, but list[:-0] is an empty list, rather than the full
        # list.
        new_task_ids_by_desirability = sorted(
            list(new_tasks_with_params.keys()),
            key=self.make_healthiness_sorter(base_task["name"], all_tasks_with_params),
            reverse=True,
        )
        new_task_ids_to_kill = new_task_ids_by_desirability[desired_instances:]

        old_tasks_with_params = self.get_old_tasks(
            base_task["name"], all_tasks_with_params
        )
        old_draining_tasks_with_params = self.get_draining_tasks(old_tasks_with_params)
        old_non_draining_tasks = sorted(
            list(
                set(old_tasks_with_params.keys()) - set(old_draining_tasks_with_params)
            ),
            key=self.make_healthiness_sorter(base_task["name"], all_tasks_with_params),
            reverse=True,
        )

        actions = bounce_lib.crossover_bounce(
            new_config={"instances": desired_instances},
            new_app_running=True,
            happy_new_tasks=happy_new_tasks_with_params.keys(),
            old_non_draining_tasks=new_task_ids_to_kill + old_non_draining_tasks,
        )

        with a_sync.idle_event_loop():
            futures = []
            for task in set(new_tasks_with_params.keys()) - set(
                actions["tasks_to_drain"]
            ):
                futures.append(asyncio.ensure_future(self.undrain_task(task)))
            for task in actions["tasks_to_drain"]:
                futures.append(asyncio.ensure_future(self.drain_task(task)))

            if futures:
                a_sync.block(asyncio.wait, futures)

            async def kill_if_safe_to_kill(task_id: str):
                if await self.drain_method.is_safe_to_kill(
                    self.make_drain_task(task_id)
                ):
                    self.kill_task(driver, task_id)

            futures = []
            for task, parameters in all_tasks_with_params.items():
                if (
                    parameters.is_draining
                    and parameters.mesos_task_state in LIVE_TASK_STATES
                ):
                    futures.append(asyncio.ensure_future(kill_if_safe_to_kill(task)))
            if futures:
                a_sync.block(asyncio.wait, futures)

    def get_happy_tasks(self, tasks_with_params: Dict[str, MesosTaskParameters]):
        """Filter a dictionary of tasks->params to those that are running and not draining."""
        happy_tasks = {}
        for tid, params in tasks_with_params.items():
            if params.mesos_task_state == TASK_RUNNING and not params.is_draining:
                happy_tasks[tid] = params
        return happy_tasks

    def get_draining_tasks(self, tasks_with_params: Dict[str, MesosTaskParameters]):
        """Filter a dictionary of tasks->params to those that are draining."""
        return {t: p for t, p in tasks_with_params.items() if p.is_draining}

    def make_drain_task(self, task_id: str):
        """Return a DrainTask object, which is suitable for passing to drain methods."""

        ports = []

        params = self.task_store.get_task(task_id)
        for resource in params.resources:
            if resource["name"] == "ports":
                for rg in resource["ranges"]["range"]:
                    for port in range(rg["begin"], rg["end"] + 1):
                        ports.append(port)

        return DrainTask(
            id=task_id, host=params.offer["agent_id"]["value"], ports=ports
        )

    async def undrain_task(self, task_id: str):
        self.log("Undraining task %s" % task_id)
        await self.drain_method.stop_draining(self.make_drain_task(task_id))
        self.task_store.update_task(task_id, is_draining=False)

    async def drain_task(self, task_id: str):
        self.log("Draining task %s" % task_id)
        await self.drain_method.drain(self.make_drain_task(task_id))
        self.task_store.update_task(task_id, is_draining=True)

    def kill_task(self, driver: MesosSchedulerDriver, task_id: str):
        self.log("Killing task %s" % task_id)
        driver.killTask({"value": task_id})
        self.task_store.update_task(task_id, mesos_task_state=TASK_KILLING)

    def group_tasks_by_version(
        self, task_ids: Collection[str]
    ) -> Mapping[str, Collection[str]]:
        d: Dict[str, List[str]] = {}
        for task_id in task_ids:
            version = task_id.rsplit(".", 1)[0]
            d.setdefault(version, []).append(task_id)
        return d

    def load_config(self) -> None:
        service_configuration_lib._yaml_cache = {}
        self.service_config = load_paasta_native_job_config(
            service=self.service_name,
            instance=self.instance_name,
            instance_type=self.instance_type,
            cluster=self.cluster,
            soa_dir=self.soa_dir,
            config_overrides=self.service_config_overrides,
        )
        self.recreate_drain_method()
        self.reload_constraints()
        self.validate_config()

    def validate_config(self) -> None:
        pass

    def recreate_drain_method(self) -> None:
        """Re-instantiate self.drain_method. Should be called after self.service_config changes."""
        self.drain_method = drain_lib.get_drain_method(
            name=self.service_config.get_drain_method(
                self.service_config.service_namespace_config
            ),
            service=self.service_name,
            instance=self.instance_name,
            registrations=self.service_config.get_registrations(),
            **self.service_config.get_drain_method_params(
                self.service_config.service_namespace_config
            ),
        )

    def reload_constraints(self):
        self.constraints = self.service_config.get_constraints() or []

    def blacklist_slave(self, agent_id: str):
        log.debug("Blacklisting slave: %s" % agent_id)
        self.blacklisted_slaves.setdefault(agent_id, time.time())

    def unblacklist_slave(self, agent_id: str):
        if agent_id not in self.blacklisted_slaves:
            return

        log.debug("Unblacklisting slave: %s" % agent_id)
        with self.blacklisted_slaves_lock:
            del self.blacklisted_slaves[agent_id]

    def check_blacklisted_slaves_for_timeout(self):
        for agent_id, blacklist_time in self.blacklisted_slaves.items():
            if (blacklist_time + self.blacklist_timeout) < time.time():
                self.unblacklist_slave(agent_id)


class DrainTask:
    def __init__(self, id, host, ports):
        self.id = id
        self.host = host
        self.ports = ports


def find_existing_id_if_exists_or_gen_new(name):
    for framework in mesos_tools.get_all_frameworks(active_only=True):
        if framework.name == name:
            return framework.id
    else:
        return uuid.uuid4().hex


def create_driver(framework_name, scheduler, system_paasta_config, implicit_acks=False):
    master_uri = "{}:{}".format(
        mesos_tools.get_mesos_leader(), mesos_tools.MESOS_MASTER_PORT
    )

    framework = {
        "user": getpass.getuser(),
        "name": framework_name,
        "failover_timeout": 604800,
        "id": {"value": find_existing_id_if_exists_or_gen_new(framework_name)},
        "checkpoint": True,
        "principal": system_paasta_config.get_paasta_native_config()["principal"],
    }

    driver = MesosSchedulerDriver(
        sched=scheduler,
        framework=framework,
        master_uri=master_uri,
        use_addict=True,
        implicit_acknowledgements=implicit_acks,
        principal=system_paasta_config.get_paasta_native_config()["principal"],
        secret=system_paasta_config.get_paasta_native_config()["secret"],
    )
    return driver


def get_paasta_native_jobs_for_cluster(cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """A paasta_native-specific wrapper around utils.get_services_for_cluster

    :param cluster: The cluster to read the configuration for
    :param soa_dir: The SOA config directory to read from
    :returns: A list of tuples of (service, job_name)"""
    return get_services_for_cluster(cluster, "paasta_native", soa_dir)
