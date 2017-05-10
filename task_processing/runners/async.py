# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from threading import Thread

from task_processing.runners.runner import Runner


class AsyncError(Exception):
    pass


class Async(Runner):
    def __init__(self, executor, callbacks=None):
        if not callbacks:
            raise AsyncError("must provide at least one callback")

        self.callbacks = callbacks
        self.executor = executor

        callback_t = Thread(target=self._callback_loop)
        callback_t.daemon = True
        callback_t.start()

    def run(self, task_config):
        return self.executor.run(task_config)

    def _callback_loop(self):
        event_queue = self.executor.get_event_queue()
        while True:
            event = event_queue.get()
            for (cl, fn) in self.callbacks:
                if isinstance(event, cl):
                    fn(event)
