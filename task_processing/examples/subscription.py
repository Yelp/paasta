# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import logging

from six.moves.queue import Queue

from task_processing.executors.mesos_executor import MesosExecutor
from task_processing.executors.task_executor import make_task_config
from task_processing.runners.subscription import Subscription
logging.basicConfig()


def main():
    credentials = {'principal': 'mesos', 'secret': 'very'}

    queue = Queue(100)
    executor = MesosExecutor(credentials=credentials)  # (framework, driver, translator)
    task_config = make_task_config(image="ubuntu:14.04", cmd="/bin/sleep 10")
    runner = Subscription(executor, queue)

    configs = [task_config] * 100
    for config in configs:
        runner.run(config)

    while True:
        event = queue.get()
        queue.task_done()
        print(event)


if __name__ == "__main__":
    exit(main())
