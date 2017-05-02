# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time

from docker_task_configs import DockerTaskConfigs
from mesos_task_execution import MesosTaskExecution


def main():
    d = DockerTaskConfigs(
        image="ubuntu:14.04",
        cmd="/bin/sleep 120",
        cpus=1,
        mem=10,
        disk=1000,
        volumes={
            "RO": [("/nail/etc/", "/nail/etc")],
            "RW": [("/tmp", "/nail/tmp")]
        },
        ports=[]
    )

    mesos_execution = MesosTaskExecution(docker_configs=d)
    mesos_execution.run()
    while True:
        time.sleep(10)


if __name__ == "__main__":
    exit(main())
