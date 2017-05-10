from __future__ import absolute_import
from __future__ import unicode_literals


class EventBase(object):
    def __init__(self, original_event):
        self.original_event = original_event
        self.task_id = original_event.task_id.value


class EventStarting(EventBase):
    pass


class EventRunning(EventBase):
    pass


class EventFinished(EventBase):
    pass


class EventFailed(EventBase):
    pass


class EventKilled(EventBase):
    pass


class EventLost(EventBase):
    pass


class EventStaging(EventBase):
    pass
