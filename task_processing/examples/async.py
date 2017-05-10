# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from task_processing.events.event import EventBase
from task_processing.interfaces.task_executor import make_task_config
from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.runners.async import Async


def main():
    credentials = {'principal': 'mesos', 'secret': 'very'}
    task_config = make_task_config(image="ubuntu:14.04", cmd="/bin/sleep 10")
    executor = MesosExecutor(credentials=credentials)
    runner = Async(executor, [(EventBase, lambda ev: print(ev))])

    configs = [task_config] * 100
    for config in configs:
        runner.run(config)


if __name__ == "__main__":
    exit(main())
