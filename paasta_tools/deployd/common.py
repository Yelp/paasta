from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import time
from collections import namedtuple
from threading import Thread

from six.moves.queue import Queue

BounceTimers = namedtuple('BounceTimers', ['processed_by_worker', 'setup_marathon', 'bounce_length'])
ServiceInstance = namedtuple('ServiceInstance', ['service', 'instance', 'bounce_by', 'watcher', 'bounce_timers'])


class PaastaThread(Thread):

    @property
    def log(self):
        name = '.'.join([self.__class__.__module__, self.__class__.__name__])
        return logging.getLogger(name)


class PaastaQueue(Queue):

    def __init__(self, name, *args, **kwargs):
        self.name = name
        Queue.__init__(self, *args, **kwargs)

    @property
    def log(self):
        name = '.'.join([self.__class__.__module__, self.__class__.__name__])
        return logging.getLogger(name)

    def put(self, item, *args, **kwargs):
        self.log.debug("Adding {} to {} queue".format(item, self.name))
        Queue.put(self, item, *args, **kwargs)


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
