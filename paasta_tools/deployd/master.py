#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import unicode_literals

import inspect
import logging
import socket
import sys
import time

from six.moves.queue import Empty

from paasta_tools.deployd import watchers
from paasta_tools.deployd.common import PaastaQueue
from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.deployd.leader import PaastaLeaderElection
from paasta_tools.deployd.workers import PaastaDeployWorker
from paasta_tools.marathon_tools import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import ZookeeperPool

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class Inbox(PaastaThread):
    def __init__(self, inbox_q, bounce_q):
        PaastaThread.__init__(self)
        PaastaThread.daemon = True
        PaastaThread.name = "Inbox"
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
            self.log.debug("Processing {} to decide if we need to add it to bounce q".format(service_instance))
            self.process_service_instance(service_instance)
        if self.inbox_q.empty() and self.to_bounce:
            self.log.debug("Cleared inbox, checking if any services are ready to bounce")
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
                self.log.debug("{} already in bounce q with higher priority".format(service_instance))
                return False
        return True

    def process_to_bounce(self):
        for service_instance_key in self.to_bounce.keys():
            if self.to_bounce[service_instance_key].bounce_by < int(time.time()):
                service_instance = self.to_bounce.pop(service_instance_key)
                self.bounce_q.put(service_instance)


class DeployDaemon(object):
    def __init__(self):
        self.log.info("paasta-deployd starting up...")
        self.bounce_q = PaastaQueue("BounceQueue")
        self.inbox_q = PaastaQueue("InboxQueue")
        self.control = PaastaQueue("ControlQueue")
        self.inbox = Inbox(self.inbox_q, self.bounce_q)
        self.config = load_system_paasta_config()
        with ZookeeperPool() as zk:
            self.log.info("Waiting to become leader")
            self.election = PaastaLeaderElection(zk, "/paasta-deployd-leader", socket.getfqdn(), control=self.control)
            self.election.run(self.start)

    @property
    def log(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)

    def bounce(self, service_instance):
        self.inbox_q.put(service_instance)

    def start(self):
        self.log.debug("This node is elected as leader {}".format(socket.getfqdn()))
        self.inbox.start()
        self.log.info("Starting all watcher threads")
        self.start_watchers()
        self.log.info("All watchers started, now adding all services for initial bounce")
        self.add_all_services()
        self.log.info("Starting worker threads")
        self.start_workers()
        while True:
            try:
                message = self.control.get(block=False)
            except Empty:
                message = None
            self.handle_control_message(message)
            time.sleep(1)

    def start_workers(self):
        for i in range(5):
            worker = PaastaDeployWorker(i, self.inbox_q, self.bounce_q)
            worker.start()

    def handle_control_message(self, message):
        if message == "ABORT":
            self.log.debug("Quitting!")
            sys.exit(2)

    def add_all_services(self):
        instances = get_services_for_cluster(cluster=self.config.get_cluster(),
                                             instance_type='marathon',
                                             soa_dir=DEFAULT_SOA_DIR)
        instances_to_add = splay_instances(instances=instances,
                                           splay_minutes=30,
                                           watcher_name='daemon_start')
        for service_instance in instances_to_add:
            self.inbox_q.put(service_instance)

    def start_watchers(self):
        """ should block until all threads happy"""
        watcher_classes = [obj[1] for obj in inspect.getmembers(watchers) if inspect.isclass(obj[1]) and
                           obj[1].__bases__[0] == watchers.PaastaWatcher]
        self.watchers = [watcher(inbox_q=self.inbox_q,
                                 cluster=self.config.get_cluster())
                         for watcher in watcher_classes]
        self.log.info("Starting the following watchers {}".format(self.watchers))
        [watcher.start() for watcher in self.watchers]
        self.log.info("Waiting for all watchers to start")
        while not all([watcher.is_ready for watcher in self.watchers]):
            self.log.debug("Sleeping and waiting for watchers to all start")
            time.sleep(1)


def splay_instances(instances, splay_minutes, watcher_name):
    service_instances = []
    time_now = int(time.time())
    time_step = int((splay_minutes * 60) / len(instances))
    bounce_time = time_now
    for service, instance in instances:
        service_instances.append(ServiceInstance(service=service,
                                                 instance=instance,
                                                 watcher=watcher_name,
                                                 bounce_by=bounce_time))
        bounce_time += time_step
    return service_instances


if __name__ == '__main__':
    DeployDaemon()
