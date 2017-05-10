# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from threading import Thread

from task_processing.runners.runner import Runner


class AsyncError(Exception):
    pass


class Async(Runner):
    def __init__(self, executor, on_success=None, on_failure=None, on_all=None):
        if on_success is None and on_failure is None and on_all is None:
            raise AsyncError("must provide at least one callback")

        self.on_success = on_success
        self.on_failure = on_failure
        self.on_all = on_all

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
            if self.on_success and event.is_success():
                self.on_success(event)
            if self.on_failure and event.is_failure():
                self.on_failure(event)
            if self.on_all:
                self.on_all(event)
