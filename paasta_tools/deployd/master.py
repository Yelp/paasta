#!/usr/bin/env python
import inspect
import logging
import socket
import sys
import time
from queue import Empty
from typing import Any
from typing import List
from typing import Type

import service_configuration_lib
from kazoo.client import KazooClient

from paasta_tools.deployd import watchers
from paasta_tools.deployd.common import DelayDeadlineQueue
from paasta_tools.deployd.common import DelayDeadlineQueueProtocol
from paasta_tools.deployd.common import get_marathon_clients_from_config
from paasta_tools.deployd.common import PaastaQueue
from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.deployd.leader import PaastaLeaderElection
from paasta_tools.deployd.metrics import QueueAndWorkerMetrics
from paasta_tools.deployd.queue import ZKDelayDeadlineQueue
from paasta_tools.deployd.workers import PaastaDeployWorker
from paasta_tools.list_marathon_service_instances import (
    get_service_instances_that_need_bouncing,
)
from paasta_tools.marathon_tools import DEFAULT_SOA_DIR
from paasta_tools.metrics.metrics_lib import get_metrics_interface
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import ZookeeperPool

# Broken out into a constant so that we don't get drift between this and the code in paasta_deployd_steps.py that
# searches for this message.
DEAD_DEPLOYD_WORKER_MESSAGE = "Detected a dead worker, starting a replacement thread"


class AddHostnameFilter(logging.Filter):
    def __init__(self) -> None:
        super().__init__()
        self.hostname = socket.gethostname()

    def filter(self, record: Any) -> bool:
        record.hostname = self.hostname
        return True


class DeployDaemon(PaastaThread):
    def __init__(self) -> None:
        super().__init__()
        self.started = False
        self.daemon = True
        service_configuration_lib.disable_yaml_cache()
        self.config = load_system_paasta_config()
        self.setup_logging()
        self.metrics = get_metrics_interface("paasta.deployd")
        self.setup_instances_to_bounce()
        self.control = PaastaQueue("ControlQueue")
        self.marathon_clients = get_marathon_clients_from_config()

    def setup_instances_to_bounce(self) -> None:
        if self.config.get_deployd_use_zk_queue():
            zk_client = KazooClient(hosts=self.config.get_zk_hosts())
            zk_client.start()
            self.instances_to_bounce: DelayDeadlineQueueProtocol = ZKDelayDeadlineQueue(
                client=zk_client
            )
        else:
            self.instances_to_bounce = DelayDeadlineQueue()

    def setup_logging(self) -> None:
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, self.config.get_deployd_log_level()))
        handler = logging.StreamHandler()
        handler.addFilter(AddHostnameFilter())
        root_logger.addHandler(handler)
        logging.getLogger("kazoo").setLevel(logging.CRITICAL)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s:%(hostname)s:%(levelname)s:%(name)s:%(message)s"
            )
        )

    def run(self) -> None:
        self.log.info("paasta-deployd starting up...")
        startup_counter = self.metrics.create_counter(
            "process_started", paasta_cluster=self.config.get_cluster()
        )
        startup_counter.count()
        with ZookeeperPool() as self.zk:
            self.election = PaastaLeaderElection(
                self.zk,
                "/paasta-deployd-leader",
                socket.getfqdn(),
                control=self.control,
            )
            self.is_leader = False
            self.log.info("Waiting to become leader")
            self.election.run(self.startup)
            self.log.info("Leadership given up, exiting...")

    @property
    def watcher_threads_enabled(self) -> List[Type[watchers.PaastaWatcher]]:
        disabled_watchers = self.config.get_disabled_watchers()
        watcher_classes = [
            obj[1]
            for obj in inspect.getmembers(watchers)
            if inspect.isclass(obj[1]) and obj[1].__bases__[0] == watchers.PaastaWatcher
        ]
        enabled_watchers = [
            x for x in watcher_classes if x.__name__ not in disabled_watchers
        ]
        return enabled_watchers

    def startup(self) -> None:
        self.is_leader = True
        self.log.info("This node is elected as leader {}".format(socket.getfqdn()))
        leader_counter = self.metrics.create_counter(
            "leader_elections", paasta_cluster=self.config.get_cluster()
        )
        leader_counter.count()
        self.log.info("Starting all watcher threads")
        self.start_watchers()
        self.log.info(
            "All watchers started, now adding all services for initial bounce"
        )

        # Fill the queue if we are not using the persistent ZK queue
        if not self.config.get_deployd_use_zk_queue():
            self.add_all_services()
        self.log.info("Prioritising services that we know need a bounce...")
        if self.config.get_deployd_startup_oracle_enabled():
            self.prioritise_bouncing_services()
        self.log.info("Starting worker threads")
        self.start_workers()
        QueueAndWorkerMetrics(
            queue=self.instances_to_bounce,
            workers=self.workers,
            cluster=self.config.get_cluster(),
            metrics_provider=self.metrics,
        ).start()
        self.started = True
        self.log.info("Startup finished!")
        self.main_loop()

    def main_loop(self) -> None:
        while True:
            try:
                message = self.control.get(block=False)
            except Empty:
                message = None
            if message == "ABORT":
                self.log.info("Got ABORT message, main_loop exiting...")
                break
            if not self.all_watchers_running():
                self.log.error("One or more watcher died, committing suicide!")
                sys.exit(1)
            if self.all_workers_dead():
                self.log.error("All workers have died, committing suicide!")
                sys.exit(1)
            self.check_and_start_workers()
            time.sleep(0.1)

    def all_watchers_running(self) -> bool:
        return all([watcher.is_alive() for watcher in self.watcher_threads])

    def all_workers_dead(self) -> bool:
        return all([not worker.is_alive() for worker in self.workers])

    def check_and_start_workers(self) -> None:
        live_workers = len([worker for worker in self.workers if worker.is_alive()])
        number_of_dead_workers = self.config.get_deployd_number_workers() - live_workers
        for i in range(number_of_dead_workers):
            self.log.error(DEAD_DEPLOYD_WORKER_MESSAGE)
            worker_no = len(self.workers) + 1
            worker = PaastaDeployWorker(
                worker_no, self.instances_to_bounce, self.config, self.metrics
            )
            worker.start()
            self.workers.append(worker)

    def stop(self) -> None:
        self.control.put("ABORT")

    def start_workers(self) -> None:
        self.workers: List[PaastaDeployWorker] = []
        for i in range(self.config.get_deployd_number_workers()):
            worker = PaastaDeployWorker(
                i, self.instances_to_bounce, self.config, self.metrics
            )
            worker.start()
            self.workers.append(worker)

    def add_all_services(self) -> None:
        instances = get_services_for_cluster(
            cluster=self.config.get_cluster(),
            instance_type="marathon",
            soa_dir=DEFAULT_SOA_DIR,
        )
        for service, instance in instances:
            self.instances_to_bounce.put(
                ServiceInstance(
                    service=service,
                    instance=instance,
                    watcher="daemon_start",
                    bounce_by=time.time()
                    + self.config.get_deployd_startup_bounce_deadline(),
                    wait_until=time.time(),
                    failures=0,
                    bounce_start_time=time.time(),
                    enqueue_time=time.time(),
                )
            )

    def prioritise_bouncing_services(self) -> None:
        service_instances = get_service_instances_that_need_bouncing(
            self.marathon_clients, DEFAULT_SOA_DIR
        )

        now = time.time()

        for service_instance in service_instances:
            self.log.info(f"Prioritising {service_instance} to be bounced immediately")
            service, instance = service_instance.split(".")
            self.instances_to_bounce.put(
                ServiceInstance(
                    service=service,
                    instance=instance,
                    watcher=type(self).__name__,
                    bounce_by=now,
                    wait_until=now,
                    failures=0,
                    bounce_start_time=time.time(),
                    enqueue_time=time.time(),
                )
            )

    def start_watchers(self) -> None:
        """ should block until all threads happy"""
        self.watcher_threads = [
            watcher(
                instances_to_bounce=self.instances_to_bounce,
                cluster=self.config.get_cluster(),
                zookeeper_client=self.zk,
                config=self.config,
            )
            for watcher in self.watcher_threads_enabled
        ]

        self.log.info(f"Starting the following watchers {self.watcher_threads}")
        for watcher in self.watcher_threads:
            watcher.start()
        self.log.info("Waiting for all watchers to start")
        attempts = 0
        while attempts < 120:
            if all([watcher.is_ready for watcher in self.watcher_threads]):
                return
            self.log.info("Sleeping and waiting for watchers to all start")
            self.log.info(
                "Waiting on: {}".format(
                    [
                        watcher.__class__.__name__
                        for watcher in self.watcher_threads
                        if not watcher.is_ready
                    ]
                )
            )
            time.sleep(1)
            attempts += 1
        self.log.error("Failed to start all the watchers, exiting...")
        sys.exit(1)


def main() -> None:
    dd = DeployDaemon()
    dd.start()
    while dd.is_alive():
        time.sleep(0.1)


if __name__ == "__main__":
    main()
