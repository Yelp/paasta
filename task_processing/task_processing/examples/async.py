# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import logging
import os
import time

from task_processing.events.event import EventTerminal
from task_processing.interfaces.task_executor import make_task_config
from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.runners.async import Async

logging.basicConfig()


class Counter(object):
    def __init__(self):
        self.terminated = 0

    def process_event(self, event):
        print('{} {}'.format(event.task_id, type(event)))
        self.terminated += 1


def main():
    mesos_address = os.environ['MESOS']
    executor = MesosExecutor(
        credential_secret_file="/src/task_processing/examples/cluster/secret",
        mesos_address=mesos_address
    )

    counter = Counter()
    runner = Async(executor, [(EventTerminal, counter.process_event)])

    for _ in range(100):
        task_config = make_task_config(image="ubuntu:14.04",
                                       cmd="/bin/sleep 10")
        runner.run(task_config)

    while True:
        print('terminated {} tasks'.format(counter.terminated))
        if counter.terminated >= 100:
            return
        time.sleep(10)

    runner.stop()


if __name__ == "__main__":
    exit(main())
