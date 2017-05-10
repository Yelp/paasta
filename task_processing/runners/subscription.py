# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from threading import Thread

from task_processing.runners.runner import Runner


class Subscription(Runner):
    def __init__(self, executor, queue):
        self.executor = executor
        self.queue = queue

        producer_t = Thread(target=self.event_producer)
        producer_t.daemon = True
        producer_t.start()

    def event_producer(self):
        event_queue = self.executor.get_event_queue()
        while True:
            self.queue.put(event_queue.get(), False)

    def run(self, task_config):
        return self.executor.run(task_config)

    def kill(self, task_id):
        return self.executor.kill(task_id)
