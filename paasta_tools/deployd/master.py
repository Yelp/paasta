#!/usr/bin/env python
import inspect
import logging
import socket
import sys
import time
from queue import Empty

import service_configuration_lib

from paasta_tools.deployd import watchers
from paasta_tools.deployd.common import get_marathon_clients_from_config
from paasta_tools.deployd.common import PaastaPriorityQueue
from paasta_tools.deployd.common import PaastaQueue
from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.common import rate_limit_instances
from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.deployd.leader import PaastaLeaderElection
from paasta_tools.deployd.metrics import QueueMetrics
from paasta_tools.deployd.workers import PaastaDeployWorker
from paasta_tools.list_marathon_service_instances import get_service_instances_that_need_bouncing
from paasta_tools.marathon_tools import DEFAULT_SOA_DIR
from paasta_tools.metrics.metrics_lib import get_metrics_interface
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import ZookeeperPool


class DedupedPriorityQueue(PaastaPriorityQueue):
    """This class extends the python Queue class so that the Queue is
    deduplicated. i.e. there can be only one copy of each service instance
    in the queue at any one time
    """

    def __init__(self, name, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.bouncing = set()

    def put(self, priority, service_instance, *args, **kwargs):
        service_instance_key = "{}.{}".format(
            service_instance.service,
            service_instance.instance,
        )
        if service_instance_key not in self.bouncing:
            self.bouncing.add(service_instance_key)
            super().put(priority, service_instance, *args, **kwargs)
        else:
            self.log.debug(f"{service_instance_key} already present in {self.name}, dropping extra message")

    def get(self, *args, **kwargs):
        service_instance = super().get(*args, **kwargs)
        service_instance_key = "{}.{}".format(
            service_instance.service,
            service_instance.instance,
        )
        self.bouncing.remove(service_instance_key)
        return service_instance


class Inbox(PaastaThread):
    def __init__(self, inbox_q, bounce_q):
        super().__init__()
        self.daemon = True
        self.name = "Inbox"
        self.inbox_q = inbox_q
        self.bounce_q = bounce_q
        self.to_bounce = {}

    def run(self):
        while True:
            self.process_inbox()

    def process_inbox(self):
        try:
            service_instance = self.inbox_q.get(block=False)
        except Empty:
            service_instance = None
        if service_instance:
            self.log.debug("Processing {}.{} to see if we need to add it "
                           "to bounce queue".format(
                               service_instance.service,
                               service_instance.instance,
                           ))
            self.process_service_instance(service_instance)
        if self.inbox_q.empty() and self.to_bounce:
            self.process_to_bounce()
        time.sleep(0.1)

    def process_service_instance(self, service_instance):
        service_instance_key = f"{service_instance.service}.{service_instance.instance}"
        if self.should_add_to_bounce(service_instance, service_instance_key):
            self.log.info(f"Enqueuing {service_instance} to be bounced in the future")
            self.to_bounce[service_instance_key] = service_instance

    def should_add_to_bounce(self, service_instance, service_instance_key):
        if service_instance_key in self.to_bounce:
            if service_instance.bounce_by > self.to_bounce[service_instance_key].bounce_by:
                self.log.debug(f"{service_instance} already in bounce queue with higher priority")
                return False
        return True

    def process_to_bounce(self):
        bounced = []
        self.log.debug("Processing %d bounce queue entries..." % len(self.to_bounce.keys()))
        for service_instance_key in self.to_bounce.keys():
            if self.to_bounce[service_instance_key].bounce_by < int(time.time()):
                service_instance = self.to_bounce[service_instance_key]
                bounced.append(service_instance_key)
                self.bounce_q.put(service_instance.priority, service_instance)
        for service_instance_key in bounced:
            self.to_bounce.pop(service_instance_key)
        # TODO: if the bounceq is empty we could probably start adding SIs from
        # self.to_bounce to make sure the workers always have something to do.


class AddHostnameFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.hostname = socket.gethostname()

    def filter(self, record):
        record.hostname = self.hostname
        return True


class DeployDaemon(PaastaThread):
    def __init__(self):
        super().__init__()
        self.started = False
        self.daemon = True
        service_configuration_lib.disable_yaml_cache()
        self.config = load_system_paasta_config()
        self.setup_logging()
        self.bounce_q = DedupedPriorityQueue("BounceQueue")
        self.inbox_q = PaastaQueue("InboxQueue")
        self.control = PaastaQueue("ControlQueue")
        self.inbox = Inbox(self.inbox_q, self.bounce_q)
        self.marathon_clients = get_marathon_clients_from_config()

    def setup_logging(self):
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, self.config.get_deployd_log_level()))
        handler = logging.StreamHandler()
        handler.addFilter(AddHostnameFilter())
        root_logger.addHandler(handler)
        logging.getLogger("kazoo").setLevel(logging.CRITICAL)
        handler.setFormatter(logging.Formatter('%(asctime)s:%(hostname)s:%(levelname)s:%(name)s:%(message)s'))

    def run(self):
        self.log.info("paasta-deployd starting up...")
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

    def bounce(self, service_instance):
        self.inbox_q.put(service_instance)

    @property
    def watcher_threads_enabled(self):
        disabled_watchers = self.config.get_disabled_watchers()
        watcher_classes = [
            obj[1] for obj in inspect.getmembers(watchers) if inspect.isclass(obj[1]) and
            obj[1].__bases__[0] == watchers.PaastaWatcher
        ]
        enabled_watchers = [x for x in watcher_classes if x.__name__ not in disabled_watchers]
        return enabled_watchers

    def startup(self):
        self.is_leader = True
        self.log.info("This node is elected as leader {}".format(socket.getfqdn()))
        self.metrics = get_metrics_interface('paasta.deployd')
        leader_counter = self.metrics.create_counter("leader_elections", paasta_cluster=self.config.get_cluster())
        leader_counter.count()
        QueueMetrics(self.inbox, self.bounce_q, self.config.get_cluster(), self.metrics).start()
        self.inbox.start()
        self.log.info("Starting all watcher threads")
        self.start_watchers()
        self.log.info("All watchers started, now adding all services for initial bounce")
        self.add_all_services()
        self.log.info("Prioritising services that we know need a bounce...")
        if self.config.get_deployd_startup_oracle_enabled():
            self.prioritise_bouncing_services()
        self.log.info("Starting worker threads")
        self.start_workers()
        self.started = True
        self.log.info("Startup finished!")
        self.main_loop()

    def main_loop(self):
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

    def all_watchers_running(self):
        return all([watcher.is_alive() for watcher in self.watcher_threads])

    def all_workers_dead(self):
        return all([not worker.is_alive() for worker in self.workers])

    def check_and_start_workers(self):
        live_workers = len([worker for worker in self.workers if worker.is_alive()])
        number_of_dead_workers = self.config.get_deployd_number_workers() - live_workers
        for i in range(number_of_dead_workers):
            self.log.error("Detected a dead worker, starting a replacement thread")
            worker_no = len(self.workers) + 1
            worker = PaastaDeployWorker(worker_no, self.inbox_q, self.bounce_q, self.config, self.metrics)
            worker.start()
            self.workers.append(worker)

    def stop(self):
        self.control.put("ABORT")

    def start_workers(self):
        self.workers = []
        for i in range(self.config.get_deployd_number_workers()):
            worker = PaastaDeployWorker(i, self.inbox_q, self.bounce_q, self.config, self.metrics)
            worker.start()
            self.workers.append(worker)

    def add_all_services(self):
        instances = get_services_for_cluster(
            cluster=self.config.get_cluster(),
            instance_type='marathon',
            soa_dir=DEFAULT_SOA_DIR,
        )
        instances_to_add = rate_limit_instances(
            instances=instances,
            cluster=self.config.get_cluster(),
            number_per_minute=self.config.get_deployd_startup_bounce_rate(),
            watcher_name='daemon_start',
            priority=99,
        )
        for service_instance in instances_to_add:
            self.inbox_q.put(service_instance)

    def prioritise_bouncing_services(self):
        service_instances = get_service_instances_that_need_bouncing(
            self.marathon_clients,
            DEFAULT_SOA_DIR,
        )
        for service_instance in service_instances:
            self.log.info(f"Prioritising {service_instance} to be bounced immediately")
            service, instance = service_instance.split('.')
            self.inbox_q.put(ServiceInstance(
                service=service,
                instance=instance,
                cluster=self.config.get_cluster(),
                watcher=type(self).__name__,
                bounce_by=int(time.time()),
                bounce_timers=None,
                failures=0,
            ))

    def start_watchers(self):
        """ should block until all threads happy"""
        self.watcher_threads = [
            watcher(
                inbox_q=self.inbox_q,
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
            self.log.info("Waiting on: {}".format(
                [watcher.__class__.__name__ for watcher in self.watcher_threads if not watcher.is_ready],
            ))
            time.sleep(1)
            attempts += 1
        self.log.error("Failed to start all the watchers, exiting...")
        sys.exit(1)


def main():
    dd = DeployDaemon()
    dd.start()
    while dd.is_alive():
        time.sleep(0.1)


if __name__ == '__main__':
    main()
