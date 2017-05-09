from __future__ import absolute_import
from __future__ import unicode_literals

from task_processing.events.event import mesos_status_to_event


class EventProcessor():
    def __init__(self, publish_queue):
        self.publish_queue = publish_queue

    def publish_event(self, status):
        translated = mesos_status_to_event(status)
        self.publish_queue.put(translated, timeout=1)
