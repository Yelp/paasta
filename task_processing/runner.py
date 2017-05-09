# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from task_processing.executors.mesos_executor import MesosExecutor
from task_processing.executors.task_executor import TaskConfig


def main():
    credentials = {'principal': 'mesos', 'secret': 'very'}

    task_executor = MesosExecutor(
        credentials=credentials,
        max_tasks=5,
    )

    # task blueprint
    task_config = TaskConfig(
        image="ubuntu:14.04",
        cmd="/bin/sleep 120",
        cpus=1,
        mem=32,
        disk=1000,
        volumes={},
        ports=[],
        cap_add=[],
        ulimit=[],
        docker_parameters=[]
    )

    # run and wait for result
    task_id, status = task_executor.run(task_config)
    if status.is_success:
        print("success")
    else:
        print("failure")

    # promise = task_executor.run_promise(task_config)
    # # do stuff ...
    # status = promise()

    # # run and provide callbacks
    # task_executor.run_async(
        # task_config,
        # success=lambda: print("success"),
        # failure=lambda: print("failure")
    # )

    # run until completion of pending tasks
    task_executor.wait_until_done()


if __name__ == "__main__":
    exit(main())
