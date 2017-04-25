from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import time

import pyinotify

from paasta_tools.deployd.common import PaastaThread
from paasta_tools.deployd.common import ServiceInstance
from paasta_tools.marathon_tools import DEFAULT_SOA_DIR
from paasta_tools.utils import list_all_instances_for_service


class PaastaWatcher(PaastaThread):

    def __init__(self, inbox_q, cluster):
        super(PaastaWatcher, self).__init__()
        self.daemon = True
        self.inbox_q = inbox_q
        self.cluster = cluster
        self.is_ready = False


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
