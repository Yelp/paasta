from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from collections import namedtuple
from threading import Thread

from six.moves.queue import Queue

BounceTimers = namedtuple('BounceTimers', ['processed_by_worker', 'setup_marathon', 'bounce_length'])
ServiceInstance = namedtuple('ServiceInstance', ['service', 'instance', 'bounce_by', 'watcher', 'bounce_timers'])


class PaastaThread(Thread):

    @property
    def log(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)


class PaastaQueue(Queue):

    def __init__(self, name, *args, **kwargs):
        self.name = name
        Queue.__init__(self, *args, **kwargs)

    @property
    def log(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)

    def put(self, item, *args, **kwargs):
        self.log.debug("Adding {} to {} queue".format(item, self.name))
        Queue.put(self, item, *args, **kwargs)
