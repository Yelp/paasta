from __future__ import absolute_import
from __future__ import unicode_literals


class TaskProcessingEvent(object):
    def __init__(self, original_event):
        self._original_event = original_event


class TaskRunningEvent(TaskProcessingEvent):
    def __init__(self, *args, **kwargs):
        super(TaskRunningEvent, self).__init__(*args, **kwargs)


class TaskFinishedEvent(TaskProcessingEvent):
    def __init__(self, *args, **kwargs):
        super(TaskFinishedEvent, self).__init__(*args, **kwargs)


class TaskFailedEvent(TaskProcessingEvent):
    def __init__(self, *args, **kwargs):
        super(TaskFailedEvent, self).__init__(*args, **kwargs)


class TaskKilledEvent(TaskProcessingEvent):
    def __init__(self, *args, **kwargs):
        super(TaskKilledEvent, self).__init__(*args, **kwargs)


def mesos_status_to_event(mesos_status):
    # DRIVER_NOT_STARTED = 1
    # DRIVER_RUNNING = 2
    # DRIVER_ABORTED = 3
    # DRIVER_STOPPED = 4
    # TASK_STAGING = 6
    # TASK_STARTING = 0
    # TASK_RUNNING = 1
    # TASK_FINISHED = 2
    # TASK_FAILED = 3
    # TASK_KILLED = 4
    # TASK_LOST = 5
    translation = {
        1: TaskRunningEvent,
        2: TaskFinishedEvent,
        3: TaskFailedEvent,
        4: TaskKilledEvent
    }
    match = translation.get(mesos_status.state, TaskProcessingEvent)
    return match(original_event=mesos_status)
