from __future__ import absolute_import
from __future__ import unicode_literals

from task_processing.events import event

# https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto
MESOS_TASK_STATUS_TO_EVENT = {
    0: event.EventStarting,
    1: event.EventRunning,
    2: event.EventFinished,
    3: event.EventFailed,
    4: event.EventKilled,
    5: event.EventLost,
    6: event.EventStaging,
    7: event.EventError,
    8: event.EventKilling,
    9: event.EventDropped,
    10: event.EventUnreachable,
    11: event.EventGone,
    12: event.EventGoneByOperator,
    13: event.EventUnknown
}


def mesos_status_to_event(mesos_status):
    return MESOS_TASK_STATUS_TO_EVENT[mesos_status.state](mesos_status)
