# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import logging
import os

from task_processing.interfaces.task_executor import make_task_config
from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.runners.sync import Sync

logging.basicConfig()


def main():
    mesos_address = os.environ.get('MESOS', '127.0.0.1:5050')
    executor = MesosExecutor(
        credential_secret_file="/src/task_processing/examples/cluster/secret",
        mesos_address=mesos_address
    )
    task_config = make_task_config(image="ubuntu:14.04", cmd="/bin/sleep 10")
    runner = Sync(executor)
    result = runner.run(task_config)
    print(result)
    print(result.original_event)
    runner.stop()


if __name__ == "__main__":
    exit(main())
