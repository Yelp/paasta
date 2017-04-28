#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import unicode_literals

import inspect
import logging
import socket
import time

from six.moves.queue import Empty

from paasta_tools.deployd import watchers
from paasta_tools.deployd.common import PaastaQueue
from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.deployd.leader import PaastaLeaderElection
from paasta_tools.deployd.metrics import get_metrics_interface
from paasta_tools.deployd.metrics import QueueMetrics
from paasta_tools.deployd.workers import PaastaDeployWorker
from paasta_tools.marathon_tools import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import ZookeeperPool


class Inbox(PaastaThread):
    def __init__(self, inbox_q, bounce_q):
        super(Inbox, self).__init__()
        self.daemon = True
        self.name = "Inbox"
        self.inbox_q = inbox_q
        self.bounce_q = bounce_q
        self.to_bounce = {}

    def run(self):
        while True:
            self.process_inbox()
        pass

    def process_inbox(self):
        try:
            service_instance = self.inbox_q.get(block=False)
        except Empty:
            service_instance = None
        if service_instance:
            self.log.debug("Processing {}.{} to see if we need to add it "
                           "to bounce queue".format(service_instance.service,
                                                    service_instance.instance))
            self.process_service_instance(service_instance)
        if self.inbox_q.empty() and self.to_bounce:
            self.process_to_bounce()
        time.sleep(1)

    def process_service_instance(self, service_instance):
        service_instance_key = "{}.{}".format(service_instance.service, service_instance.instance)
        if self.should_add_to_bounce(service_instance, service_instance_key):
            self.log.debug("Adding {} to be bounced in the future".format(service_instance))
            self.to_bounce[service_instance_key] = service_instance

    def should_add_to_bounce(self, service_instance, service_instance_key):
        if service_instance_key in self.to_bounce:
            if service_instance.bounce_by > self.to_bounce[service_instance_key].bounce_by:
                self.log.debug("{} already in bounce queue with higher priority".format(service_instance))
                return False
        return True

    def process_to_bounce(self):
        for service_instance_key in self.to_bounce.keys():
            if self.to_bounce[service_instance_key].bounce_by < int(time.time()):
                service_instance = self.to_bounce.pop(service_instance_key)
                self.bounce_q.put(service_instance)


class DeployDaemon(PaastaThread):
    def __init__(self):
        super(DeployDaemon, self).__init__()
        self.started = False
        self.daemon = True
        self.bounce_q = PaastaQueue("BounceQueue")
        self.inbox_q = PaastaQueue("InboxQueue")
        self.control = PaastaQueue("ControlQueue")
        self.inbox = Inbox(self.inbox_q, self.bounce_q)
        self.config = load_system_paasta_config()
        logging.basicConfig(level=getattr(logging, self.config.get_deployd_log_level()))

    def run(self):
        self.log.info("paasta-deployd starting up...")
        with ZookeeperPool() as self.zk:
            self.log.info("Waiting to become leader")
            self.election = PaastaLeaderElection(self.zk,
                                                 "/paasta-deployd-leader",
                                                 socket.getfqdn(),
                                                 control=self.control)
            self.is_leader = False
            self.election.run(self.startup)

    def bounce(self, service_instance):
        self.inbox_q.put(service_instance)

    def startup(self):
        self.is_leader = True
        self.log.debug("This node is elected as leader {}".format(socket.getfqdn()))
        self.metrics = get_metrics_interface(self.config.get_deployd_metrics_provider())
        QueueMetrics(self.inbox_q, self.bounce_q, self.metrics).start()
        self.inbox.start()
        self.log.info("Starting all watcher threads")
        self.start_watchers()
        self.log.info("All watchers started, now adding all services for initial bounce")
        self.add_all_services()
        self.log.info("Starting worker threads")
        self.start_workers()
        self.started = True
        self.main_loop()

    def main_loop(self):
        while True:
            try:
                message = self.control.get(block=False)
            except Empty:
                message = None
            if message == "ABORT":
                break
            time.sleep(0.1)

    def stop(self):
        self.control.put("ABORT")

    def start_workers(self):
        for i in range(self.config.get_deployd_number_workers()):
            worker = PaastaDeployWorker(i, self.inbox_q, self.bounce_q, self.metrics)
            worker.start()

    def add_all_services(self):
        instances = get_services_for_cluster(cluster=self.config.get_cluster(),
                                             instance_type='marathon',
                                             soa_dir=DEFAULT_SOA_DIR)
        instances_to_add = rate_limit_instances(instances=instances,
                                                number_per_minute=self.config.get_deployd_big_bounce_rate(),
                                                watcher_name='daemon_start')
        for service_instance in instances_to_add:
            self.inbox_q.put(service_instance)

    def start_watchers(self):
        """ should block until all threads happy"""
        watcher_classes = [obj[1] for obj in inspect.getmembers(watchers) if inspect.isclass(obj[1]) and
                           obj[1].__bases__[0] == watchers.PaastaWatcher]
        watcher_threads = [watcher(inbox_q=self.inbox_q,
                                   cluster=self.config.get_cluster(),
                                   zookeeper_client=self.zk)
                           for watcher in watcher_classes]
        self.log.info("Starting the following watchers {}".format(watcher_threads))
        for watcher in watcher_threads:
            watcher.start()
        self.log.info("Waiting for all watchers to start")
        while not all([watcher.is_ready for watcher in watcher_threads]):
            self.log.debug("Sleeping and waiting for watchers to all start")
            time.sleep(1)


def rate_limit_instances(instances, number_per_minute, watcher_name):
    service_instances = []
    if not instances:
        return []
    time_now = int(time.time())
    time_step = int(60 / number_per_minute)
    bounce_time = time_now
    for service, instance in instances:
        service_instances.append(ServiceInstance(service=service,
                                                 instance=instance,
                                                 watcher=watcher_name,
                                                 bounce_by=bounce_time,
                                                 bounce_timers=None))
        bounce_time += time_step
    return service_instances


def main():
    dd = DeployDaemon()
    dd.start()
    while dd.is_alive():
        time.sleep(0.1)


if __name__ == '__main__':
    main()
