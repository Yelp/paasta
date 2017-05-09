# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from six.moves.queue import Queue

from task_processing.executors.mesos_executor import MesosExecutor
from task_processing.executors.task_executor import TaskConfig
from task_processing.runners.subscription import Subscription


def main():
    credentials = {'principal': 'mesos', 'secret': 'very'}

    # task blueprint
    task_config = TaskConfig(
        image="ubuntu:14.04",
        cmd="/bin/sleep 10",
        cpus=0.1,
        mem=32,
        disk=1000,
        volumes={},
        ports=[],
        cap_add=[],
        ulimit=[],
        docker_parameters=[]
    )

    queue = Queue(10)
    executor = MesosExecutor(credentials=credentials)  # (framework, driver, translator)
    runner = Subscription(executor, queue)

    configs = [task_config] * 100
    for config in configs:
        runner.run(config)

    while True:
        event = queue.get()
        print(event)


if __name__ == "__main__":
    exit(main())
