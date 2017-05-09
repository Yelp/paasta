# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from six.moves.queue import Queue

from task_processing.runners.runner import Runner


class Sync(Runner):
    def __init__(self, executor):
        self.executor = executor
        self.queue = Queue.queue()

    def run(self, task_config):
        task_id = self.executor.run(task_config)
        event_queue = self.executor.get_event_queue()

        while True:
            event = event_queue.get()
            if event.task_id == task_id and event.is_finished():
                return
