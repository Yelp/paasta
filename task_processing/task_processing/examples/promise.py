# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import logging
import os

from task_processing.executors.mesos_executor import MesosExecutor
from task_processing.executors.task_executor import make_task_config
from task_processing.runners.promise import Promise


logging.basicConfig()


def main():
    credentials = {'principal': 'mesos', 'secret': 'very'}
    mesos_address = os.environ['MESOS']
    executor = MesosExecutor(credentials=credentials,
                             mesos_address=mesos_address)
    task_config = make_task_config(image="ubuntu:14.04", cmd="/bin/sleep 10")
    runner = Promise(executor)
    future = runner.run(task_config)
    print(future.get())


if __name__ == "__main__":
    exit(main())
