from __future__ import absolute_import
from __future__ import unicode_literals


class EventBase(object):
    def __init__(self, original_event):
        self.original_event = original_event
        self.task_id = original_event.task_id.value


class EventTerminal(EventBase):
    pass


class EventStarting(EventBase):
    pass


class EventRunning(EventBase):
    pass


class EventFinished(EventTerminal):
    pass


class EventFailed(EventTerminal):
    pass


class EventKilled(EventTerminal):
    pass


class EventLost(EventTerminal):
    pass


class EventStaging(EventBase):
    pass
