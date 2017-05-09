# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from threading import Thread

from six.moves.queue import Queue


class Runner(object):
    def run(self, task_config):
        pass

    def kill(self, task_id=None):
        """ Stop the instance of the task """
        pass

    def subscribe(self, queue):
        """Subscribe to TaskProcessingEvent updates.

        :param queue: a threading.Queue object, onto which events will be pushed
        """
        self.subscribe_queue = queue

    def status(self, task_id=None):
        """ Get the status of this instance of the task.
            What return type here?? we do not know yet
        """
        pass

    def cleanup(self):
        """ Cleanup any state before shutting down, can internally call kill """
        pass

    def supported_extra_methods(self):
        """
        :return [list of strings] of extensions that this executor provides

        For example, if this implementation supports stdout or stdin
        """

    def wait_until_done(self):
        pass
        # self.driver_thread.join()


class Subscription(Runner):
    def __init__(self, executor, queue):
        self.executor = executor
        self.queue = queue

        producer_t = Thread(target=self.event_producer)
        producer_t.daemon = True
        producer_t.start()

    def event_producer(self):
        while True:
            self.queue.put(self.executor.get_event_queue().get())

    def run(self, task_config):
        return self.executor.run(task_config)


class Sync(Runner):
    def __init__(self, execution_framework):
        self.execution_framework = execution_framework
        self.queue = Queue.queue()

    def run(self, task_config):
        self.execution_framework.enqueue(task_config)
        # while True:
        #     event = event_queue.pop()
        #     if event is TASK_FINISHED:
        #         return
