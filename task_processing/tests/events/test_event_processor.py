from __future__ import absolute_import
from __future__ import unicode_literals

from mesos.interface import mesos_pb2
from six.moves import queue

from task_processing.events.event_processor import EventProcessor


def test_event_processor_e2e():
    test_queue = queue.Queue(10)
    processor = EventProcessor(test_queue)
    fake_status_update = mesos_pb2.TaskStatus()
    fake_status_update.state = mesos_pb2.TASK_RUNNING
    processor.publish_event(fake_status_update)
    event = test_queue.get(block=False, timeout=0)
    test_queue.task_done()
    assert event is not None
    test_queue.join()
