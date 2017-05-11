# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import logging
import os

from six.moves.queue import Empty
from six.moves.queue import Queue

from task_processing.events.event import EventTerminal
from task_processing.interfaces.task_executor import make_task_config
from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.runners.subscription import Subscription

logging.basicConfig()


def main():
    credentials = {'principal': 'mesos', 'secret': 'very'}
    mesos_address = os.environ['MESOS']
    executor = MesosExecutor(credentials=credentials,
                             mesos_address=mesos_address)
    queue = Queue(100)
    runner = Subscription(executor, queue)

    tasks = set()
    for _ in range(1, 100):
        task_config = make_task_config(image="ubuntu:14.04", cmd="/bin/sleep 10")
        tasks.add(task_config.task_id)
        runner.run(task_config)

    print("Running {} tasks: {}".format(len(tasks), tasks))
    while len(tasks) > 0:
        try:
            event = queue.get(True, 10)
        except Empty:
            event = None

        if event is None:
            print("Timeout on subscription queue, still waiting for {}".format(tasks))
        else:
            print("{} {}".format(event.task_id, type(event)))
            if isinstance(event, EventTerminal):
                tasks.discard(event.task_id)

    runner.stop()


if __name__ == "__main__":
    exit(main())
