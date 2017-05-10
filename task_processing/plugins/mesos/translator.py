from __future__ import absolute_import
from __future__ import unicode_literals

from task_processing.events.event import EventFailed
from task_processing.events.event import EventFinished
from task_processing.events.event import EventKilled
from task_processing.events.event import EventLost
from task_processing.events.event import EventRunning
from task_processing.events.event import EventStaging
from task_processing.events.event import EventStarting

MESOS_STATUS_TO_EVENT = {
    0: EventStarting,
    1: EventRunning,
    2: EventFinished,
    3: EventFailed,
    4: EventKilled,
    5: EventLost,
    6: EventStaging,
}


def mesos_status_to_event(mesos_status):
    return MESOS_STATUS_TO_EVENT[mesos_status.state](mesos_status)
