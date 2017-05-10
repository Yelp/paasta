# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from task_processing.task_config import TaskConfig
from task_processing.task_executor import TaskExecutor


def main():
    credentials = {'principal': 'mesos', 'secret': 'very'}

    task_executor = TaskExecutor(
        credentials=credentials,
        parallelization=5,
    )

    # task blueprint
    task_config = TaskConfig(
        image="ubuntu:14.04",
        cmd="/bin/sleep 120",
        cpus=1,
        mem=32,
        disk=1000,
        volumes={
            "RO": [("/nail/etc/", "/nail/etc")],
            "RW": [("/tmp", "/nail/tmp")]
        },
        ports=[]
    )

    # run and wait for result
    status = task_executor.run(task_config)
    if status.is_success:
        print("success")
    else:
        print("failure")

    promise = task_executor.run_promise(task_config)
    # do stuff ...
    status = promise()

    # run and provide callbacks
    task_executor.run_async(
        task_config,
        success=lambda: print("success"),
        failure=lambda: print("failure")
    )

    # run until completion of pending tasks
    task_executor.wait_until_done()


if __name__ == "__main__":
    exit(main())
