from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import time
from functools import reduce

import pyinotify
from kazoo.protocol.states import EventType
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.recipe.watchers import DataWatch

from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.long_running_service_tools import AUTOSCALING_ZK_ROOT
from paasta_tools.marathon_tools import DEFAULT_SOA_DIR
from paasta_tools.utils import list_all_instances_for_service


class PaastaWatcher(PaastaThread):

    def __init__(self, inbox_q, cluster):
        super(PaastaWatcher, self).__init__()
        self.daemon = True
        self.inbox_q = inbox_q
        self.cluster = cluster
        self.is_ready = False


class AutoscalerWatcher(PaastaWatcher):

    def __init__(self, inbox_q, cluster, **kwargs):
        super(AutoscalerWatcher, self).__init__(inbox_q, cluster)
        self.zk = kwargs.pop('zookeeper_client')
        self.watchers = {}

    def watch_folder(self, path):
        """recursive nonsense"""
        if "autoscaling.lock" in path:
            return
        self.log.debug("Adding watch on {}".format(path))
        watcher = ChildrenWatch(self.zk, path, func=self.process_folder_event, send_event=True)
        self.watchers[path] = watcher
        children = watcher._prior_children
        if children and ('instances' in children):
            self.watch_node("{}/instances".format(path))
        elif children:
            for child in children:
                self.watch_folder("{}/{}".format(path, child))

    def watch_node(self, path):
        self.log.debug("Adding watch on {}".format(path))
        DataWatch(self.zk, path, func=self.process_node_event, send_event=True)

    def process_node_event(self, data, stat, event):
        self.log.debug("Node change: {}".format(event))
        if event and (event.type == EventType.CREATED or event.type == EventType.CHANGED):
            service, instance = event.path.split('/')[-3:-1]
            self.log.info("Number of instances changed or autoscaling enabled for first time"
                          " for {}.{}".format(service, instance))
            service_instance = ServiceInstance(service=service,
                                               instance=instance,
                                               bounce_by=int(time.time()),
                                               bounce_timers=None,
                                               watcher=self.__class__.__name__)
            self.inbox_q.put(service_instance)

    def process_folder_event(self, children, event):
        self.log.debug("Folder change: {}".format(event))
        if event and (event.type == EventType.CHILD):
            fq_children = ["{}/{}".format(event.path, child) for child in children]
            for child in fq_children:
                if child not in self.watchers:
                    self.watch_folder(child)

    def run(self):
        self.watchers[AUTOSCALING_ZK_ROOT] = self.watch_folder(AUTOSCALING_ZK_ROOT)
        self.is_ready = True
        while True:
            time.sleep(0.1)


class FileWatcher(PaastaWatcher):

    def __init__(self, inbox_q, cluster, **kwargs):
        super(FileWatcher, self).__init__(inbox_q, cluster)
        self.wm = pyinotify.WatchManager()
        self.wm.add_watch(DEFAULT_SOA_DIR, self.mask, rec=True)
        self.notifier = pyinotify.Notifier(watch_manager=self.wm,
                                           default_proc_fun=YelpSoaEventHandler(filewatcher=self))

    @property
    def mask(self):
        boring_flags = ['IN_CLOSE_NOWRITE', 'IN_OPEN', 'IN_ACCESS', 'IN_ATTRIB']
        return reduce(lambda x, y: x | y,
                      [v for k, v in pyinotify.EventsCodes.OP_FLAGS.items() if k not in boring_flags])

    def run(self):
        self.notifier.loop(callback=self.startup_checker)

    def startup_checker(self, obj):
        self.is_ready = True


class YelpSoaEventHandler(pyinotify.ProcessEvent):

    def my_init(self, filewatcher):
        self.filewatcher = filewatcher

    @property
    def log(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)

    def filter_event(self, event):
        starts_with = ['marathon-', 'deployments.json']
        if any([event.name.startswith(x) for x in starts_with]):
            return event

    def watch_new_folder(self, event):
        if event.maskname == 'IN_CREATE|IN_ISDIR':
            self.filewatcher.wm.add_watch(event.pathname, self.filewatcher.mask, rec=True)

    def process_default(self, event):
        self.log.debug(event)
        self.watch_new_folder(event)
        event = self.filter_event(event)
        if event:
            self.log.debug("Change of {} in {}".format(event.name, event.path))
            service_name = event.path.split('/')[-1]
            self.log.debug("Adding all instances for service {} to the bounce inbox "
                           "with immediate priority".format(service_name))
            instances = list_all_instances_for_service(service=service_name,
                                                       clusters=[self.filewatcher.cluster],
                                                       instance_type='marathon')
            self.log.debug(instances)
            service_instances = [ServiceInstance(service=service_name,
                                                 instance=instance,
                                                 bounce_by=int(time.time()),
                                                 watcher=self.__class__.__name__,
                                                 bounce_timers=None)
                                 for instance in instances]
            for service_instance in service_instances:
                self.filewatcher.inbox_q.put(service_instance)
