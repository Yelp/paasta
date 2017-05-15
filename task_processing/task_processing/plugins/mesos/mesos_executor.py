from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
import threading

import mesos.native
from mesos.interface import mesos_pb2

from task_processing.interfaces.task_executor import TaskExecutor
from task_processing.plugins.mesos.execution_framework import ExecutionFramework
from task_processing.plugins.mesos.translator import mesos_status_to_event
FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)
log = logging.getLogger(__name__)


class MesosExecutor(TaskExecutor):
    def __init__(self,
                 authentication_principal='taskproc',
                 credential_secret_file=None,
                 mesos_address='127.0.0.1:5050',
                 translator=mesos_status_to_event):
        """
        Constructs the instance of a task execution, encapsulating all state
        required to run, monitor and stop the job.

        :param dict credentials: Mesos principal and secret.
        """

        self.logger = logging.getLogger(__name__)

        credential = mesos_pb2.Credential()
        credential.principal = authentication_principal
        if credential_secret_file:
            if not os.path.exists(credential_secret_file):
                self.logger.fatal("credential secret file does not exist")
            else:
                with open(credential_secret_file) as f:
                    credential.secret = f.read().strip()

        self.execution_framework = ExecutionFramework(
            name="test",
            staging_timeout=10,
            translator=translator,
        )

        # TODO: Get mesos master ips from smartstack
        self.driver = mesos.native.MesosSchedulerDriver(
            self.execution_framework,
            self.execution_framework.framework_info,
            mesos_address,
            False,
            credential
        )

        # start driver thread immediately
        self.driver_thread = threading.Thread(target=self.driver.run, args=())
        self.driver_thread.daemon = True
        self.driver_thread.start()

    def run(self, task_config):
        self.execution_framework.enqueue(task_config)

    def kill(self, task_id):
        print("Killing")

    def stop(self):
        self.execution_framework.stop()
        self.driver.stop()
        self.driver.join()

    def get_event_queue(self):
        return self.execution_framework.queue
